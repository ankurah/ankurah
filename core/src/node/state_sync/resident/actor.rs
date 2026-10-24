//! The task that owns a resident: it applies the resident's maintenance,
//! answers its leases' commands and delivers its live routes.

use super::{
    bootstrap::{ensure_catalog_unchanged, fold_candidate, StorageRead, TreeUpdate, MAX_READ_ATTEMPTS},
    identity::{Coverage, Identity},
    lease::{Command, Resident, ResidentSnapshot},
    route::{current_state, Route},
    Lru,
};
use crate::{
    node::{
        state_sync::{tree::ID_ROW_BYTES, QueryTree, Snapshot},
        Node, WeakNode,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::{MaintenanceSubscription, SourceBatch, SourceError, MAX_PENDING_BYTES},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_proto as proto;
use ankurah_signals::Wait;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::Ordering, Arc, Mutex, Weak},
};
use tokio::sync::{mpsc, oneshot, Notify};

type RouteMembership = (BTreeMap<proto::EntityId, proto::QueryMember>, Vec<proto::EntityId>);

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
    pub(super) routes: Vec<Route>,
}

impl<CD: ContextData> ResidentActor<CD> {
    /// Fail with `Changed` unless the tree holds exactly the candidates the
    /// admitting credentials may read now.
    pub(super) async fn check_authorization<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.identity.credentials);
        let entries = self.tree.entries();
        if policy.allows_unrestricted_state_reads() {
            // A new grant must also admit candidates previously denied by policy.
            // Returning the old subset would silently lose newly readable rows.
            anyhow::ensure!(entries.keys().eq(self.candidates.iter()), SourceError::Changed);
            return Ok(());
        }
        for id in &self.candidates {
            let state = current_state(node, *id).await?.ok_or(SourceError::Changed)?;
            anyhow::ensure!(policy.check_read(id, &state.state).is_ok() == entries.contains_key(id), SourceError::Changed);
        }
        Ok(())
    }

    /// The bytes this resident is charged against the LRU's idle allowance.
    pub(super) fn accounted_bytes(&self) -> usize { self.tree.accounted_bytes().saturating_add(self.candidates.len() * ID_ROW_BYTES) }

    /// Fold `changes` into the candidates and the tree. With `delivery`, return
    /// the changes to entities that were or now are members, for live routes.
    pub(super) async fn apply(&mut self, changes: Vec<TreeUpdate>, delivery: bool) -> anyhow::Result<Vec<TreeUpdate>> {
        let mut affected = BTreeSet::new();
        let mut latest = BTreeMap::new();
        for change in &changes {
            fold_candidate(&mut self.candidates, change);
            latest.insert(change.entity_id, change.entry.clone());
            if delivery && (self.tree.entries().contains_key(&change.entity_id) || change.entry.is_some()) {
                affected.insert(change.entity_id);
            }
        }
        self.tree.apply(latest).await?;
        Ok(changes.into_iter().filter(|change| affected.contains(&change.entity_id)).collect())
    }

    async fn deliver<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        revision: u64,
        sources: Vec<TreeUpdate>,
        prepared: Option<Vec<anyhow::Result<RouteMembership>>>,
    ) where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.identity.credentials);
        let mut retained = Vec::with_capacity(self.routes.len());
        let mut prepared = prepared.map(Vec::into_iter);
        for mut route in std::mem::take(&mut self.routes) {
            let membership = prepared.as_mut().map(|prepared| prepared.next().expect("one prepared membership per route"));
            if route.owner.strong_count() == 0 {
                continue;
            }
            let result = async {
                anyhow::ensure!(
                    policy.filter_predicate(route.selection.predicate.clone())? == route.filtered.predicate,
                    SourceError::Changed
                );
                if let Some(membership) = membership {
                    let (next, ordered) = membership?;
                    route.changes_for_members(node, &policy, &sources, next, ordered).await
                } else {
                    route.changes(node, &self.identity.coverage, &self.tree, &policy, &sources).await
                }
            }
            .await;
            match result {
                Ok(changes) => {
                    if changes.is_empty() || crate::node::state_sync::queue_live(node, route.peer, route.stream, revision, changes).is_ok()
                    {
                        retained.push(route);
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, "resident tree live delivery failed");
                    let weak = node.weak();
                    crate::task::spawn(async move {
                        if let Some(node) = weak.upgrade() {
                            let _ = crate::node::state_sync::expire_state_stream(&node, route.peer, route.stream).await;
                        }
                    });
                }
            }
        }
        self.routes = retained;
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
            let sources = self.apply(changes, !self.routes.is_empty()).await?;
            if !sources.is_empty() {
                self.deliver(node, revision, sources, None).await;
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
        self.tree.replace(read.rows).await?;
        let sources = pending.into_iter().flat_map(|batch| batch.changes).collect();
        let members = self.tree.snapshot().query_members();
        let prepared = self.routes.iter().map(|_| Ok((members.clone(), read.ordered.clone()))).collect();
        self.deliver(node, revision, sources, Some(prepared)).await;
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
        Ok(self.tree.snapshot())
    }

    /// Run this actor until its resident is dropped or the node stops, then
    /// remove its slot and invalidate the routes it still serves.
    pub(super) fn spawn<SE, PA>(
        self,
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
        let actor = self;
        crate::task::spawn(async move {
            let mut actor = actor;
            let result = tokio::select! {
                _ = canceled => Ok(()),
                _ = run.wait_value(false) => Ok(()),
                result = run_resident(&mut actor, weak.clone(), receiver, cleanup, &lru, id) => result,
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
                            if unavailable {
                                let _ = crate::node::state_sync::source_unavailable(&node, route.peer, route.stream).await;
                            } else {
                                let _ = crate::node::state_sync::expire_state_stream(&node, route.peer, route.stream).await;
                            }
                        }
                    });
                }
            }
        });
    }
}

async fn run_resident<SE, PA>(
    actor: &mut ResidentActor<PA::ContextData>,
    weak: WeakNode<SE, PA>,
    mut commands: mpsc::Receiver<Command<PA::ContextData>>,
    cleanup: Arc<Notify>,
    lru: &Weak<Mutex<Lru<PA::ContextData>>>,
    id: u64,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    loop {
        actor.routes.retain(|route| route.owner.strong_count() != 0);
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return Ok(()); };
                let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                let refreshed = async {
                    node.system.check_not_halted()?;
                    actor.identity.check(&node)?;
                    let snapshot = actor.refresh(&node).await?;
                    actor.check_authorization(&node).await?;
                    let lru = lru.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    lru.lock().unwrap().update_bytes(id, actor.accounted_bytes())?;
                    Ok::<_, anyhow::Error>(snapshot)
                }.await;
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
                    let policy = ContextPolicy::from_credentials(&node.policy_agent, &actor.identity.credentials);
                    anyhow::ensure!(policy.filter_predicate(view.selection.predicate.clone())? == view.filtered.predicate, SourceError::Changed);
                    actor.identity.coverage.select(snapshot, &view.selection, &node.catalog).await?
                        .with_spec(view.spec.clone(), &view.selection, &node.catalog)
                }.await;
                let snapshot = match selected {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        command.reject(&error);
                        // An invalid view does not invalidate the source used
                        // by other query owners.
                        continue;
                    }
                };
                match command {
                    Command::Snapshot { response, .. } => { let _ = response.send(Ok(ResidentSnapshot { revision: actor.revision, snapshot })); }
                    Command::Subscribe { view, peer, request, fingerprint, query, version, catalog, known, owner, response } => {
                        if response.is_closed() || owner.strong_count() == 0 { continue; }
                        let members = snapshot.query_members();
                        let result = crate::node::state_sync::prepare_state_stream(&node, peer, request, fingerprint, actor.identity.credentials.clone(), Some((query, version)), catalog, actor.revision, snapshot, known);
                        if let Ok(pending) = &result {
                            actor.routes.push(Route { peer, stream: pending.id(), owner, selection: view.selection.clone(), filtered: view.filtered.clone(), members });
                        }
                        let _ = response.send(result);
                    }
                    Command::Refresh { peer, stream, number, known, response, .. } => {
                        let result = if actor.routes.iter().any(|route| route.peer == peer && route.stream == stream && route.owner.strong_count() != 0) {
                            crate::node::state_sync::queue_round(&node, peer, stream, number, actor.revision, snapshot, known)
                        } else { Err(anyhow::anyhow!("query stream unavailable")) };
                        let _ = response.send(result);
                    }
                }
            }
            batch = actor.cursor.next() => {
                let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                node.system.check_not_halted()?;
                actor.identity.check(&node)?;
                actor.publish(&node, batch?).await?;
            }
            _ = cleanup.notified() => { actor.routes.retain(|route| route.owner.strong_count() != 0); }
        }
        let lru = lru.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
        lru.lock().unwrap().update_bytes(id, actor.accounted_bytes())?;
    }
}
