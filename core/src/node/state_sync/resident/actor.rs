//! The task that owns a resident: it applies the resident's maintenance,
//! answers its leases' commands and delivers its live routes.

use super::{
    bootstrap::{ensure_catalog_unchanged, fold_candidate, StorageRead, TreeUpdate, MAX_READ_ATTEMPTS},
    identity::{ensure_filter_unchanged, Access, Coverage, Identity},
    lease::{Command, Resident, ResidentSnapshot},
    route::{current_states, Affected, LiveRead, Refill, Route, RouteMembers},
    Lru,
};
use crate::{
    node::{
        state_sync::{
            tree::{DigestRequest, DigestRequests, TreeEntry, ID_ROW_BYTES},
            QueryTree, Snapshot,
        },
        Node, WeakNode,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::{MaintenanceSubscription, SourceBatch, SourceError, MAX_PENDING_BYTES},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_proto::{self as proto, state_sync::StateFrameBody};
use ankurah_signals::Wait;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::Ordering, Arc, Mutex, Weak},
};
use tokio::sync::{mpsc, oneshot, Notify};

/// A resident's state, owned by its actor task: what it admits, every
/// candidate its predicate matches, its tree, its maintenance subscription and
/// the live routes it delivers to.
pub(super) struct ResidentActor<CD: ContextData> {
    pub(super) identity: Identity<CD>,
    // IDs only: denied candidates must also be rechecked when opaque policy changes.
    pub(super) candidates: BTreeSet<proto::EntityId>,
    pub(super) tree: QueryTree,
    pub(super) cursor: MaintenanceSubscription<TreeUpdate>,
    pub(super) revision: u64,
    pub(super) routes: Vec<Route<CD>>,
    /// How the snapshots this actor hands out ask it for the tree's digests,
    /// once the actor runs.
    pub(super) digest_requests: Option<DigestRequests>,
}

impl<CD: ContextData> ResidentActor<CD> {
    /// Fail with `PolicyChanged` unless the tree holds exactly the candidates
    /// the resident's access admits now: every candidate for an unrestricted
    /// resident, and for a restricted one those its admitting credentials may
    /// read.
    pub(super) async fn check_authorization<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.identity.credentials);
        let entries = self.tree.entries();
        if self.identity.access == Access::Unrestricted || policy.allows_unrestricted_state_reads() {
            // A new grant must also admit candidates previously denied by policy.
            // Returning the old subset would silently lose newly readable rows.
            anyhow::ensure!(entries.keys().eq(self.candidates.iter()), SourceError::PolicyChanged);
            return Ok(());
        }
        let states = current_states(node, self.candidates.iter().copied()).await?;
        for id in &self.candidates {
            let state = states.get(id).ok_or(SourceError::Changed)?;
            anyhow::ensure!(policy.check_read(id, &state.state).is_ok() == entries.contains_key(id), SourceError::PolicyChanged);
        }
        Ok(())
    }

    /// The bytes this resident is charged against the LRU's idle allowance.
    pub(super) fn accounted_bytes(&self) -> usize { self.tree.accounted_bytes().saturating_add(self.candidates.len() * ID_ROW_BYTES) }

    /// Fold `changes` into the candidates and the tree. With `delivery`, return
    /// each entity the changes moved that was or is a member, for live routes.
    pub(super) async fn apply(&mut self, changes: Vec<TreeUpdate>, delivery: bool) -> anyhow::Result<Vec<Affected>> {
        for change in &changes {
            fold_candidate(&mut self.candidates, change);
        }
        let (mut affected, latest) = group_affected(changes, |id| self.tree.entries().get(id).cloned());
        if delivery {
            for change in &mut affected {
                change.after = latest[&change.entity_id].clone();
            }
            affected.retain(|change| change.before.is_some() || change.after.is_some());
        } else {
            affected.clear();
        }
        self.tree.apply(latest).await?;
        Ok(affected)
    }

    /// Deliver `affected` to every live route: each route's membership changes
    /// under its own principal's checks, one read of the states and events
    /// they deliver, and each stream's items built from that read. `refill` is
    /// a limited resident's new cut, when a storage read moved it.
    async fn deliver<SE, PA>(&mut self, node: &Node<SE, PA>, revision: u64, affected: Vec<Affected>, refill: Option<Refill>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut publications = Vec::with_capacity(self.routes.len());
        for mut route in std::mem::take(&mut self.routes) {
            if route.owner.strong_count() == 0 {
                continue;
            }
            let changes = async {
                // Each route reads for its own principal, checked afresh.
                let policy = ContextPolicy::from_credentials(&node.policy_agent, &route.credentials);
                ensure_filter_unchanged(&policy, &route.selection, &route.filtered)?;
                self.identity.ensure_holder_admitted(&policy)?;
                route.changes(&self.identity.coverage, &self.tree, &node.catalog, &affected, refill.as_ref()).await
            }
            .await;
            match changes {
                Ok(changes) => publications.push((route, changes)),
                Err(error) => fail_route(node, &route, &error),
            }
        }
        let entities = publications.iter().flat_map(|(_, changes)| changes.iter().map(|change| change.entity_id)).collect();
        let read = match LiveRead::of(node, entities, &affected).await {
            Ok(read) => read,
            Err(error) => {
                for (route, _) in &publications {
                    fail_route(node, route, &error);
                }
                return;
            }
        };
        for (route, changes) in publications {
            if changes.is_empty()
                || crate::node::state_sync::queue_live_changes(node, route.peer, route.stream, revision, changes, &read).is_ok()
            {
                self.routes.push(route);
            }
        }
    }

    /// Bring the resident up to `batch`, the next committed changes, and
    /// deliver them to its live routes. A complete resident applies the batch
    /// as it stands, without a fence that would wait for a commit in flight.
    pub(super) async fn publish<SE, PA>(&mut self, node: &Node<SE, PA>, batch: SourceBatch<TreeUpdate>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        ensure_catalog_unchanged(&self.identity, &batch)?;
        self.routes.retain(|route| route.owner.strong_count() != 0);
        let revision = batch.revision.0;
        match self.identity.coverage {
            Coverage::Complete => self.advance_complete(node, vec![batch], revision).await,
            Coverage::Limited(_) if self.identity.filtered.limit != Some(0) && self.relevant(&batch) => {
                self.refill(node, vec![batch]).await
            }
            Coverage::Limited(_) => {
                self.revision = revision;
                Ok(())
            }
        }
    }

    /// Apply a finite metadata window before publishing its net membership.
    /// Repeated changes keep their event identities but update the index once.
    async fn advance_complete<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        pending: Vec<SourceBatch<TreeUpdate>>,
        revision: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut bytes = 0usize;
        let mut changes = Vec::new();
        for batch in pending {
            ensure_catalog_unchanged(&self.identity, &batch)?;
            anyhow::ensure!(batch.bytes <= MAX_PENDING_BYTES.saturating_sub(bytes), SourceError::Lagged);
            bytes += batch.bytes;
            changes.extend(batch.changes);
        }
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if !changes.is_empty() {
            let affected = self.apply(changes, !self.routes.is_empty()).await?;
            if !affected.is_empty() {
                self.deliver(node, revision, affected, None).await;
            }
        }
        self.revision = revision;
        Ok(())
    }

    fn relevant(&self, batch: &SourceBatch<TreeUpdate>) -> bool {
        batch.changes.iter().any(|change| change.matches || self.candidates.contains(&change.entity_id))
    }

    /// Read a limited resident's cut again after `pending` touched it, fence
    /// the read, replace the tree's entries and deliver the new cut.
    async fn refill<SE, PA>(&mut self, node: &Node<SE, PA>, mut pending: Vec<SourceBatch<TreeUpdate>>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut bytes = pending.iter().map(|batch| batch.bytes).sum::<usize>();
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.identity.credentials);
        let mut attempts = 0;
        let (read, revision) = loop {
            attempts += 1;
            anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
            let window = self.cursor.fence().await?;
            if let Some(batch) = window.batch {
                ensure_catalog_unchanged(&self.identity, &batch)?;
                anyhow::ensure!(batch.bytes <= MAX_PENDING_BYTES.saturating_sub(bytes), SourceError::Lagged);
                bytes += batch.bytes;
                pending.push(batch);
            }
            let read = StorageRead::of(node, &self.identity, &policy).await?;
            match self.cursor.fence_unchanged_since(window.revision).await {
                Err(SourceError::Changed) => continue,
                result => {
                    let revision = result?.revision;
                    // Keep bounded residents' explicit entity watches bounded
                    // too; predicate routing still observes future entrants.
                    match self
                        .cursor
                        .finish_bootstrap(self.identity.filtered.predicate.clone(), read.candidates.iter().copied(), revision)
                        .await
                    {
                        Err(SourceError::Changed) => continue,
                        result => {
                            result?;
                            break (read, revision.0);
                        }
                    }
                }
            }
        };
        self.candidates = read.candidates;
        let (mut affected, _) =
            group_affected(pending.into_iter().flat_map(|batch| batch.changes).collect(), |id| self.tree.entries().get(id).cloned());
        self.tree.replace(read.rows).await?;
        for change in &mut affected {
            change.after = self.tree.entries().get(&change.entity_id).cloned();
        }
        let refill = Refill { members: Arc::new(self.tree.snapshot().query_members()), ordered: read.ordered };
        self.deliver(node, revision, affected, Some(refill)).await;
        self.revision = revision;
        Ok(())
    }

    /// Apply every commit published so far, and snapshot the tree.
    pub(super) async fn refresh<SE, PA>(&mut self, node: &Node<SE, PA>) -> anyhow::Result<Snapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let window = self.cursor.fence().await?;
        if matches!(self.identity.coverage, Coverage::Limited(_)) {
            let mut relevant = false;
            if let Some(batch) = &window.batch {
                ensure_catalog_unchanged(&self.identity, batch)?;
                relevant = self.identity.filtered.limit != Some(0) && self.relevant(batch);
            }
            if relevant {
                self.refill(node, window.batch.into_iter().collect()).await?;
            } else {
                self.revision = window.revision.0;
            }
        } else {
            self.advance_complete(node, window.batch.into_iter().collect(), window.revision.0).await?;
        }
        Ok(self.snapshot())
    }

    /// A snapshot of the tree whose first digest read asks this actor for the
    /// tree's digests.
    fn snapshot(&self) -> Snapshot {
        let snapshot = self.tree.snapshot();
        match &self.digest_requests {
            Some(requests) => snapshot.asking(requests),
            None => snapshot,
        }
    }

    /// Answer commands and apply published batches until the resident's last
    /// handle drops, charging the LRU for the resident after each.
    async fn run<SE, PA>(
        &mut self,
        weak: WeakNode<SE, PA>,
        mut commands: mpsc::Receiver<Command<CD>>,
        mut digest_requests: mpsc::Receiver<DigestRequest>,
        cleanup: Arc<Notify>,
        lru: &Weak<Mutex<Lru<CD>>>,
        id: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        loop {
            self.routes.retain(|route| route.owner.strong_count() != 0);
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else { return Ok(()); };
                    let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    self.handle_command(&node, command, lru, id).await?;
                }
                request = digest_requests.recv() => {
                    if let Some(request) = request {
                        self.tree.answer(request).await?;
                    }
                }
                batch = self.cursor.next() => {
                    let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    node.system.check_not_halted()?;
                    self.identity.ensure_policy_unchanged(&node)?;
                    self.publish(&node, batch?).await?;
                }
                _ = cleanup.notified() => { self.routes.retain(|route| route.owner.strong_count() != 0); }
            }
            let lru = lru.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            lru.lock().unwrap().update_bytes(id, self.accounted_bytes())?;
        }
    }

    /// Answer one lease's command from the resident brought up to date. A
    /// failure to bring the resident up to date stops it; a failure of the
    /// command's own view fails only that command.
    async fn handle_command<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        command: Command<CD>,
        lru: &Weak<Mutex<Lru<CD>>>,
        id: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let refreshed = async {
            node.system.check_not_halted()?;
            self.identity.ensure_policy_unchanged(node)?;
            let snapshot = self.refresh(node).await?;
            self.check_authorization(node).await?;
            let lru = lru.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            lru.lock().unwrap().update_bytes(id, self.accounted_bytes())?;
            Ok::<_, anyhow::Error>(snapshot)
        }
        .await;
        let snapshot = match refreshed {
            Ok(snapshot) => snapshot,
            Err(error) => {
                // A retry must not reacquire the failed source before
                // the actor's outer cleanup has removed its entry.
                if let Some(lru) = lru.upgrade() {
                    lru.lock().unwrap().slots.retain(|slot| slot.id != id);
                }
                command.reject(&error);
                return Err(error);
            }
        };
        let selected = async {
            let view = command.view();
            let policy = ContextPolicy::from_credentials(&node.policy_agent, &view.credentials);
            ensure_filter_unchanged(&policy, &view.selection, &view.filtered)?;
            self.identity.ensure_holder_admitted(&policy)?;
            self.identity.coverage.select(snapshot, &view.selection, &node.catalog).await?.with_spec(
                view.spec.clone(),
                &view.selection,
                &node.catalog,
            )
        }
        .await;
        let snapshot = match selected {
            Ok(snapshot) => snapshot,
            Err(error) => {
                // An invalid view does not invalidate the source used by
                // other query owners.
                command.reject(&error);
                return Ok(());
            }
        };
        match command {
            Command::Snapshot { response, .. } => {
                let _ = response.send(Ok(ResidentSnapshot { revision: self.revision, snapshot }));
            }
            Command::Subscribe { view, peer, request, fingerprint, query, version, catalog, known, owner, response } => {
                if response.is_closed() || owner.strong_count() == 0 {
                    return Ok(());
                }
                let members = RouteMembers::of(&snapshot, &view.selection);
                let result = crate::node::state_sync::prepare_state_stream(
                    node,
                    peer,
                    request,
                    fingerprint,
                    view.credentials.clone(),
                    Some((query, version)),
                    catalog,
                    self.revision,
                    snapshot,
                    known,
                );
                if let Ok(pending) = &result {
                    self.routes.push(Route {
                        peer,
                        stream: pending.id(),
                        owner,
                        selection: view.selection.clone(),
                        filtered: view.filtered.clone(),
                        credentials: view.credentials.clone(),
                        members,
                    });
                }
                let _ = response.send(result);
            }
            Command::Refresh { peer, stream, number, known, response, .. } => {
                let result =
                    if self.routes.iter().any(|route| route.peer == peer && route.stream == stream && route.owner.strong_count() != 0) {
                        crate::node::state_sync::queue_round(node, peer, stream, number, self.revision, snapshot, known)
                    } else {
                        Err(anyhow::anyhow!("query stream unavailable"))
                    };
                let _ = response.send(result);
            }
        }
        Ok(())
    }

    /// Run this actor until its resident is dropped or the node stops, then
    /// remove its slot and invalidate the routes it still serves.
    pub(super) fn spawn<SE, PA>(
        mut self,
        node: &Node<SE, PA>,
        receiver: mpsc::Receiver<Command<CD>>,
        canceled: oneshot::Receiver<()>,
        cleanup: Arc<Notify>,
        lru: Weak<Mutex<Lru<CD>>>,
        resident: Weak<Resident<CD>>,
        id: u64,
    ) where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let weak = node.weak();
        let run = node.run.clone();
        let (requests, digest_requests) = mpsc::channel(8);
        self.digest_requests = Some(DigestRequests::new(&requests));
        let actor = self;
        crate::task::spawn(async move {
            // Holding the sender keeps the snapshots' requests answerable while this task runs.
            let _requests = requests;
            let mut actor = actor;
            let result = tokio::select! {
                _ = canceled => Ok(()),
                _ = run.wait_value(false) => Ok(()),
                result = actor.run(weak.clone(), receiver, digest_requests, cleanup, &lru, id) => result,
            };
            if let Some(resident) = resident.upgrade() {
                resident.alive.store(false, Ordering::Release);
            }
            if let Some(lru) = lru.upgrade() {
                lru.lock().unwrap().slots.retain(|slot| slot.id != id);
            }
            // Removal is immediate; transport cleanup may wait for its handler.
            if let Err(error) = result {
                tracing::debug!(%error, "resident tree stopped");
                let unavailable = matches!(error.downcast_ref::<SourceError>(), Some(SourceError::Unavailable));
                for route in actor.routes {
                    let weak = weak.clone();
                    // A peer waiting on history or its own handler must not
                    // delay invalidation of the tree's other subscribers.
                    crate::task::spawn(async move {
                        if let Some(node) = weak.upgrade() {
                            let reason = if unavailable { StateFrameBody::SourceUnavailable } else { StateFrameBody::RestartRequired };
                            let _ = crate::node::state_sync::fail_state_stream(&node, route.peer, route.stream, reason).await;
                        }
                    });
                }
            }
        });
    }
}

/// `changes` grouped by entity in commit order: each entity once, with the
/// entry `before` gives for it and the events it committed, and the entry its
/// last change leaves it, by entity.
fn group_affected(
    changes: Vec<TreeUpdate>,
    before: impl Fn(&proto::EntityId) -> Option<TreeEntry>,
) -> (Vec<Affected>, BTreeMap<proto::EntityId, Option<TreeEntry>>) {
    let mut affected: Vec<Affected> = Vec::new();
    let mut positions = BTreeMap::new();
    let mut latest = BTreeMap::new();
    for change in changes {
        let position = *positions.entry(change.entity_id).or_insert_with(|| {
            affected.push(Affected { entity_id: change.entity_id, before: before(&change.entity_id), after: None, events: Vec::new() });
            affected.len() - 1
        });
        affected[position].events.extend(change.events);
        latest.insert(change.entity_id, change.entry);
    }
    (affected, latest)
}

/// Make `route`'s peer restart its stream after a live delivery failed.
fn fail_route<SE, PA, CD>(node: &Node<SE, PA>, route: &Route<CD>, error: &anyhow::Error)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    tracing::debug!(%error, "resident tree live delivery failed");
    let weak = node.weak();
    let (peer, stream) = (route.peer, route.stream);
    // A peer waiting on its own handler must not delay this resident.
    crate::task::spawn(async move {
        if let Some(node) = weak.upgrade() {
            let _ = crate::node::state_sync::fail_state_stream(&node, peer, stream, StateFrameBody::RestartRequired).await;
        }
    });
}
