//! The task that owns a resident's tree: it applies maintenance, answers commands and delivers live routes.

use super::{
    bootstrap::{CapturedQuery, QueryProjection, TreeUpdate, MAX_READ_ATTEMPTS},
    identity::{Coverage, Identity},
    lease::{Capture, Command, Resident},
    route::{current_state, Route},
    Entries,
};
use crate::{
    node::{state_sync::Snapshot, Node, WeakNode},
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::{MaintenanceSubscription, SourceBatch, SourceError, MAX_PENDING_BYTES},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_proto as proto;
use ankurah_signals::Wait;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
};
use tokio::sync::{mpsc, oneshot, Notify};

type RouteMembership = (BTreeMap<proto::EntityId, proto::QueryMember>, Vec<proto::EntityId>);

pub(crate) struct QuerySource<CD: ContextData> {
    credentials: Vec<CD>,
    pub(super) projection: QueryProjection,
    pub(super) cursor: MaintenanceSubscription<TreeUpdate>,
    pub(super) revision: u64,
    pub(super) routes: Vec<Route>,
}
impl<CD: ContextData> QuerySource<CD> {
    pub(super) fn credentials(&self) -> &[CD] { &self.credentials }
    pub(super) fn from_capture(credentials: Vec<CD>, capture: CapturedQuery) -> Self {
        Self { credentials, projection: capture.projection, cursor: capture.cursor, revision: capture.revision, routes: Vec::new() }
    }
    pub(super) async fn check_authorization<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let snapshot = self.projection.tree.current_snapshot().await?;
        if policy.allows_unrestricted_state_reads() {
            // A new grant must also admit candidates previously denied by policy.
            // Returning the old subset would silently lose newly readable rows.
            anyhow::ensure!(snapshot.entries.keys().eq(self.projection.candidates.iter()), SourceError::Changed);
            return Ok(());
        }
        for id in &self.projection.candidates {
            let state = current_state(node, *id).await?.ok_or(SourceError::Changed)?;
            anyhow::ensure!(policy.check_read(id, &state.state).is_ok() == snapshot.entries.contains_key(id), SourceError::Changed);
        }
        Ok(())
    }
    pub(super) fn accounted_bytes(&self) -> usize { self.projection.bytes.saturating_add(self.projection.candidates.len() * 64) }
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
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
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
                    route.changes(node, &self.projection, &policy, &sources).await
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
    pub(super) async fn publish<SE, PA>(&mut self, node: &Node<SE, PA>, batch: Arc<SourceBatch<TreeUpdate>>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        self.projection.check_catalog(&batch)?;
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if matches!(self.projection.coverage, Coverage::Limited(_)) {
            if self.projection.selection.limit != Some(0) && self.relevant(&batch) {
                return self.refill(node, vec![batch]).await;
            }
        } else {
            let window = self.cursor.fence().await?;
            let pending = std::iter::once(batch).chain(window.batches).collect();
            return self.advance_complete(node, pending, window.revision.0).await;
        }
        self.revision = batch.revision.0;
        Ok(())
    }

    /// Apply a finite metadata window before publishing its net membership.
    /// Repeated changes keep their event identities but update the index once.
    async fn advance_complete<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        pending: Vec<Arc<SourceBatch<TreeUpdate>>>,
        revision: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut bytes = 0usize;
        let mut changes = Vec::new();
        for batch in pending {
            self.projection.check_catalog(&batch)?;
            anyhow::ensure!(batch.bytes <= MAX_PENDING_BYTES.saturating_sub(bytes), SourceError::Lagged);
            bytes += batch.bytes;
            changes.extend(batch.changes.iter().cloned());
        }
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if !changes.is_empty() {
            let batch =
                SourceBatch { revision: crate::reactor::maintenance::SourceRevision(revision), changes, catalog_changed: false, bytes };
            let sources = self.projection.apply(&batch, !self.routes.is_empty()).await?;
            if !sources.is_empty() {
                self.deliver(node, revision, sources, None).await;
            }
        }
        self.revision = revision;
        Ok(())
    }
    fn relevant(&self, batch: &SourceBatch<TreeUpdate>) -> bool {
        batch.changes.iter().any(|change| change.matches || self.projection.candidates.contains(&change.entity_id))
    }
    async fn refill<SE, PA>(&mut self, node: &Node<SE, PA>, mut pending: Vec<Arc<SourceBatch<TreeUpdate>>>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut bytes = pending.iter().map(|batch| batch.bytes).sum::<usize>();
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let mut attempts = 0;
        let (rows, candidates, ordered, revision) = loop {
            attempts += 1;
            anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
            let window = self.cursor.fence().await?;
            for batch in window.batches {
                self.projection.check_catalog(&batch)?;
                anyhow::ensure!(batch.bytes <= MAX_PENDING_BYTES.saturating_sub(bytes), SourceError::Lagged);
                bytes += batch.bytes;
                pending.push(batch);
            }
            let (rows, candidates, ordered) = self.projection.read(node, &policy).await?;
            match self.cursor.validate_limited_read(window.revision).await {
                Err(SourceError::Changed) => continue,
                result => {
                    let revision = result?.revision;
                    // Keep bounded residents' explicit entity watches bounded
                    // too; predicate routing still observes future entrants.
                    match self
                        .cursor
                        .finish_bootstrap(self.projection.selection.predicate.clone(), candidates.iter().copied(), revision)
                        .await
                    {
                        Err(SourceError::Changed) => continue,
                        result => {
                            result?;
                            break (rows, candidates, ordered, revision.0);
                        }
                    }
                }
            }
        };
        self.projection.candidates = candidates;
        let snapshot = self.projection.tree.refresh_entries(self.projection.spec.clone(), rows.into_values().collect()).await?;
        self.projection.bytes = snapshot.accounted_bytes();
        let sources = pending.into_iter().flat_map(|batch| batch.changes.clone()).collect();
        let members = snapshot.query_members();
        let prepared = self.routes.iter().map(|_| Ok((members.clone(), ordered.clone()))).collect();
        self.deliver(node, revision, sources, Some(prepared)).await;
        self.revision = revision;
        Ok(())
    }
    pub(super) async fn refresh<SE, PA>(&mut self, node: &Node<SE, PA>) -> anyhow::Result<Snapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let window = self.cursor.fence().await?;
        if matches!(self.projection.coverage, Coverage::Limited(_)) {
            let mut relevant = false;
            for batch in &window.batches {
                self.projection.check_catalog(batch)?;
                relevant |= self.projection.selection.limit != Some(0) && self.relevant(batch);
            }
            if relevant {
                self.refill(node, window.batches).await?;
            } else {
                self.revision = window.revision.0;
            }
        } else {
            self.advance_complete(node, window.batches, window.revision.0).await?;
        }
        self.projection.tree.current_snapshot().await
    }
}

impl<CD: ContextData> QuerySource<CD> {
    /// Run this actor until its resident is dropped or the node stops, then
    /// remove its entry and invalidate the routes it still serves.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn<SE, PA>(
        self,
        node: &Node<SE, PA>,
        receiver: mpsc::Receiver<Command<CD>>,
        canceled: oneshot::Receiver<()>,
        cleanup: Arc<Notify>,
        entries: Weak<Mutex<Entries<CD>>>,
        resident: Weak<Resident<CD>>,
        id: u64,
        scope: Identity<CD>,
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
                result = run_resident(&mut actor, weak.clone(), receiver, cleanup, &entries, id, &scope) => result,
            };
            if let Some(resident) = resident.upgrade() {
                resident.alive.store(false, Ordering::Release);
            }
            if let Some(entries) = entries.upgrade() {
                entries.lock().unwrap().entries.retain(|entry| entry.id != id);
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
    actor: &mut QuerySource<PA::ContextData>,
    weak: WeakNode<SE, PA>,
    mut commands: mpsc::Receiver<Command<PA::ContextData>>,
    cleanup: Arc<Notify>,
    entries: &Weak<Mutex<Entries<PA::ContextData>>>,
    id: u64,
    scope: &Identity<PA::ContextData>,
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
                let captured = async {
                    node.system.check_not_halted()?;
                    scope.check(&node)?;
                    let snapshot = actor.refresh(&node).await?;
                    actor.check_authorization(&node).await?;
                    let entries = entries.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    entries.lock().unwrap().update_bytes(id, actor.accounted_bytes())?;
                    Ok::<_, anyhow::Error>(snapshot)
                }.await;
                let snapshot = match captured {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        // A retry must not reacquire the failed source before
                        // the actor's outer cleanup has removed its entry.
                        if let Some(entries) = entries.upgrade() {
                            entries.lock().unwrap().entries.retain(|entry| entry.id != id);
                        }
                        command.reject(&error);
                        return Err(error);
                    }
                };
                let selected = async {
                    let view = command.view();
                    let policy = ContextPolicy::from_credentials(&node.policy_agent, &actor.credentials().to_vec());
                    anyhow::ensure!(policy.filter_predicate(view.selection.predicate.clone())? == view.filtered.predicate, SourceError::Changed);
                    actor.projection.coverage.select(snapshot, &view.selection, &node.catalog).await?
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
                    Command::Snapshot { response, .. } => { let _ = response.send(Ok(Capture { revision: actor.revision, snapshot })); }
                    Command::Subscribe { view, peer, request, fingerprint, query, version, catalog, known, owner, response } => {
                        if response.is_closed() || owner.strong_count() == 0 { continue; }
                        let members = snapshot.query_members();
                        let result = crate::node::state_sync::prepare_state_stream(&node, peer, request, fingerprint, actor.credentials().to_vec(), Some((query, version)), catalog, actor.revision, snapshot, known);
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
                scope.check(&node)?;
                actor.publish(&node, batch?).await?;
            }
            _ = cleanup.notified() => { actor.routes.retain(|route| route.owner.strong_count() != 0); }
        }
        let entries = entries.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
        entries.lock().unwrap().update_bytes(id, actor.accounted_bytes())?;
    }
}
