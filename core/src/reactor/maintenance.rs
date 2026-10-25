//! Reactor subscriptions for indexes that retain metadata rather than entities.
//!
//! Projection happens while the committed entity is available. Only the projected
//! values enter each subscription's bounded queue; no entity handles are retained.
use super::{watcherset::WatcherOp, Reactor, ReactorSubscriptionId, WatcherChange, WatcherSet};
use crate::entity::Entity;
use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{Attested, EntityId, Event, QueryId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard};

/// The projected bytes one reader may hold unconsumed; a reader past it fails as lagged.
pub(crate) const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

/// How many commits that changed entities the source has published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceRevision(pub(crate) u64);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SourceError {
    #[error("reactor maintenance stopped with its node")]
    Closed,
    #[error("a commit failed inside the commit gate, so the index may have missed its outcome; admit it again")]
    CommitInDoubt,
    #[error("index maintenance fell behind its bounded change window")]
    Lagged,
    #[error("source changed while the index was being read")]
    Changed,
    #[error("query catalog changed; reopen the query generation")]
    CatalogChanged,
    #[error("access policy changed for the index's readers; reopen the query")]
    PolicyChanged,
}

impl SourceError {
    /// Whether `error` is a source error that admitting the index again can
    /// recover from, unlike maintenance that stopped with its node.
    pub(crate) fn is_retryable(error: &anyhow::Error) -> bool {
        matches!(
            error.downcast_ref::<SourceError>(),
            Some(
                SourceError::CommitInDoubt
                    | SourceError::Changed
                    | SourceError::Lagged
                    | SourceError::CatalogChanged
                    | SourceError::PolicyChanged
            )
        )
    }
}

/// Compact result of inspecting one committed entity. `member` controls the
/// entity watch needed to observe its later departure from the predicate.
pub(crate) struct ProjectedChange<T> {
    pub(crate) value: T,
    pub(crate) member: bool,
    pub(crate) bytes: usize,
}

/// One reader's projections of the commits it has not consumed, coalesced.
#[derive(Debug)]
pub(crate) struct SourceBatch<T> {
    pub(crate) changes: Vec<T>,
    pub(crate) catalog_changed: bool,
    pub(crate) bytes: usize,
}

/// What a fence hands its reader: the source's revision at the fence, and the
/// reader's unconsumed changes up to it, coalesced into at most one batch.
pub(crate) struct SourceWindow<T> {
    pub(crate) revision: SourceRevision,
    pub(crate) batch: Option<SourceBatch<T>>,
}

/// The commit gate, the source's revision, and the readers each commit publishes to.
#[derive(Clone)]
pub(super) struct Maintenance(Arc<MaintenanceInner>);

struct MaintenanceInner {
    gate: Arc<AsyncMutex<()>>,
    state: Mutex<SourceState>,
    /// The watchers that route each committed entity to the readers it may
    /// concern, apart from the reactor's own.
    watchers: Mutex<WatcherSet>,
}

#[derive(Default)]
struct SourceState {
    revision: u64,
    /// Whether the reactor closed, after which maintenance admits no reader.
    closed: bool,
    readers: BTreeMap<ReactorSubscriptionId, Weak<dyn MaintenanceReader>>,
}

trait MaintenanceReader: Send + Sync {
    fn project(&self, entities: &[&Entity], catalog_changed: bool) -> Vec<WatcherChange>;
    fn fail(&self, error: SourceError);
}

/// A reader's projection of one publication's committed entities, one result
/// per entity in order, computed while the entities are available.
type Projector<T> = dyn Fn(&[&Entity]) -> anyhow::Result<Vec<ProjectedChange<T>>> + Send + Sync;

struct Reader<T> {
    id: ReactorSubscriptionId,
    query: QueryId,
    predicate: Mutex<Predicate<Resolved>>,
    project: Box<Projector<T>>,
    state: Mutex<ReaderState<T>>,
    notify: Notify,
}

struct ReaderState<T> {
    pending: Option<SourceBatch<T>>,
    error: Option<SourceError>,
}

impl<T> Default for ReaderState<T> {
    fn default() -> Self { Self { pending: None, error: None } }
}

/// A reader's RAII registration, which lives as long as the index it feeds, such as a resident tree.
pub(crate) struct MaintenanceSubscription<T> {
    maintenance: Maintenance,
    reader: Arc<Reader<T>>,
}

/// Serializes persistence, the resident entities' advance and index projection. A commit
/// holds it from its storage commit until it has registered or updated the
/// resident entity and published the entities it changed; otherwise a competing
/// writer could retry from stale in-memory state after storage advances. A
/// commit that fails or stops after taking it fails every reader that exists
/// then, any of which may have missed its outcome.
pub(crate) struct SourceCommit {
    maintenance: Maintenance,
    _gate: OwnedMutexGuard<()>,
    finished: bool,
}

impl Maintenance {
    pub(super) fn new() -> Self {
        Self(Arc::new(MaintenanceInner {
            gate: Arc::new(AsyncMutex::new(())),
            state: Mutex::default(),
            watchers: Mutex::new(WatcherSet::new()),
        }))
    }

    pub(super) async fn begin_commit(&self) -> SourceCommit {
        SourceCommit { maintenance: self.clone(), _gate: self.0.gate.clone().lock_owned().await, finished: false }
    }

    pub(super) async fn subscribe<T: Send + Sync + 'static>(
        &self,
        project: impl Fn(&[&Entity]) -> anyhow::Result<Vec<ProjectedChange<T>>> + Send + Sync + 'static,
    ) -> Result<(SourceRevision, MaintenanceSubscription<T>), SourceError> {
        let _gate = self.0.gate.lock().await;
        let mut source = self.0.state.lock().unwrap();
        if source.closed {
            return Err(SourceError::Closed);
        }
        let reader = Arc::new(Reader {
            id: ReactorSubscriptionId::new(),
            query: QueryId::new(),
            predicate: Mutex::new(Predicate::True),
            project: Box::new(project),
            state: Mutex::default(),
            notify: Notify::new(),
        });
        // Before the scan establishes membership, even a change whose new value
        // no longer matches may remove a row the scan already observed.
        self.0.watchers.lock().unwrap().recurse_predicate_watchers(&Predicate::True, (reader.id, reader.query), WatcherOp::Add);
        source.readers.insert(reader.id, Arc::downgrade(&(reader.clone() as Arc<dyn MaintenanceReader>)));
        Ok((SourceRevision(source.revision), MaintenanceSubscription { maintenance: self.clone(), reader }))
    }

    /// Fail every reader and admit none again: the reactor closed with its node.
    pub(super) fn close(&self) {
        let mut source = self.0.state.lock().unwrap();
        source.closed = true;
        for reader in source.readers.values().filter_map(Weak::upgrade) {
            reader.fail(SourceError::Closed);
        }
    }

    /// A commit failed after taking the gate, for `cause`, and published
    /// nothing, so storage may hold its outcome or not. Fail every reader that
    /// exists now, any of which may have missed the outcome, and log it once.
    /// A reader admitted later reads the outcome from storage, which holds it
    /// once the gate is released. One risk remains: a write that storage
    /// reported failed but that lands later leaves such a reader stale for its
    /// entity until that entity's next change.
    fn commit_in_doubt(&self, cause: &dyn std::fmt::Display) {
        tracing::error!(%cause, "a commit failed inside the commit gate; the resident trees that existed are admitted again from storage");
        let source = self.0.state.lock().unwrap();
        for reader in source.readers.values().filter_map(Weak::upgrade) {
            reader.fail(SourceError::CommitInDoubt);
        }
    }
}

impl Reactor<Entity, Attested<Event>> {
    /// Take the commit gate for one storage commit; the returned [`SourceCommit`] publishes the
    /// entities the commit changed to maintenance.
    pub(crate) async fn begin_commit(&self) -> SourceCommit { self.0.maintenance.begin_commit().await }

    /// Subscribe an index before reading its initial contents. Projection runs
    /// before public event filtering and may retain only the index's metadata.
    pub(crate) async fn subscribe_maintenance<T: Send + Sync + 'static>(
        &self,
        project: impl Fn(&[&Entity]) -> anyhow::Result<Vec<ProjectedChange<T>>> + Send + Sync + 'static,
    ) -> Result<(SourceRevision, MaintenanceSubscription<T>), SourceError> {
        self.0.maintenance.subscribe(project).await
    }
}

impl<T: Send + Sync> MaintenanceReader for Reader<T> {
    fn project(&self, entities: &[&Entity], catalog_changed: bool) -> Vec<WatcherChange> {
        let mut state = self.state.lock().unwrap();
        if state.error.is_some() || (entities.is_empty() && !catalog_changed) {
            return Vec::new();
        }
        let mut values = Vec::with_capacity(entities.len());
        let mut watchers = Vec::with_capacity(entities.len());
        let mut bytes = 0usize;
        let result = (|| {
            let projected = if entities.is_empty() { Vec::new() } else { (self.project)(entities)? };
            anyhow::ensure!(projected.len() == entities.len(), "a projector returns one projection per entity");
            for (entity, projected) in entities.iter().zip(projected) {
                bytes = bytes.checked_add(projected.bytes).ok_or_else(|| anyhow::anyhow!("index change size overflow"))?;
                let entity_id = entity.id();
                watchers.push(if projected.member {
                    WatcherChange::Add { entity_id, subscription_id: self.id, query_id: self.query }
                } else {
                    WatcherChange::Remove { entity_id, subscription_id: self.id, query_id: self.query }
                });
                values.push(projected.value);
            }
            Ok::<_, anyhow::Error>(())
        })();
        let error = match result {
            Err(error) => {
                tracing::debug!(%error, "reactor index projection failed");
                Some(SourceError::Changed)
            }
            Ok(()) if bytes > MAX_PENDING_BYTES.saturating_sub(state.pending.as_ref().map_or(0, |batch| batch.bytes)) => {
                Some(SourceError::Lagged)
            }
            Ok(()) => None,
        };
        if let Some(error) = error {
            state.pending = None;
            state.error = Some(error);
            watchers.clear();
        } else {
            // Commit boundaries do not change index work. Coalesce only the
            // unconsumed window; next()/fence() detach it before later commits.
            if let Some(pending) = &mut state.pending {
                pending.changes.extend(values);
                pending.catalog_changed |= catalog_changed;
                pending.bytes += bytes;
            } else {
                state.pending = Some(SourceBatch { changes: values, catalog_changed, bytes });
            }
        }
        self.notify.notify_one();
        watchers
    }

    fn fail(&self, error: SourceError) {
        let mut state = self.state.lock().unwrap();
        state.pending = None;
        state.error = Some(error);
        self.notify.notify_one();
    }
}

impl SourceCommit {
    pub(crate) fn record_storage_outcome(
        &mut self,
        result: Result<crate::storage::StorageCommitOutcome, crate::error::MutationError>,
    ) -> Result<crate::storage::StorageCommitOutcome, crate::error::MutationError> {
        if matches!(&result, Ok(crate::storage::StorageCommitOutcome::Conflict { .. }) | Err(crate::error::MutationError::WriteConflict)) {
            self.finished = true;
        }
        result
    }

    /// Publish the entities this commit changed, as they stand committed.
    pub(crate) fn publish<'a>(mut self, entities: impl IntoIterator<Item = &'a Entity>) {
        let mut source = self.maintenance.0.state.lock().unwrap();
        let entities = Arc::new(entities.into_iter().collect::<Vec<_>>());
        if source.closed || entities.is_empty() {
            self.finished = true;
            return;
        }
        source.revision += 1;
        source.readers.retain(|_, reader| reader.strong_count() != 0);
        let catalog_changed = entities.iter().any(|entity| entity.memberships().iter().any(crate::schema::is_catalog_model));
        let mut candidates = BTreeMap::new();
        {
            let watchers = self.maintenance.0.watchers.lock().unwrap();
            for (offset, entity) in entities.iter().enumerate() {
                watchers.accumulate_interested_watchers(*entity, offset, &entities, &mut candidates);
            }
        }
        let mut watcher_changes = Vec::new();
        for (id, reader) in &source.readers {
            let Some(reader) = reader.upgrade() else { continue };
            let mut interested = Vec::new();
            if let Some(candidates) = candidates.get(id) {
                for query in candidates.query_iter() {
                    interested.extend(query.iter().copied());
                }
            }
            watcher_changes.extend(reader.project(&interested, catalog_changed));
        }
        let mut watchers = self.maintenance.0.watchers.lock().unwrap();
        for change in watcher_changes {
            watchers.apply_watcher_change(change);
        }
        self.finished = true;
    }

    /// End this commit after `error`, publishing nothing. Unless storage
    /// refused the commit, which changed nothing, its outcome is in doubt.
    pub(crate) fn fail(mut self, error: &crate::error::MutationError) {
        if !std::mem::replace(&mut self.finished, true) {
            self.maintenance.commit_in_doubt(error);
        }
    }
}

impl Drop for SourceCommit {
    fn drop(&mut self) {
        if !self.finished {
            self.maintenance.commit_in_doubt(&"the commit stopped before it published");
        }
    }
}

impl<T> MaintenanceSubscription<T> {
    pub(crate) async fn fence(&mut self) -> Result<SourceWindow<T>, SourceError> { self.fence_inner(None).await }

    pub(crate) async fn fence_unchanged_since(&mut self, started: SourceRevision) -> Result<SourceWindow<T>, SourceError> {
        self.fence_inner(Some(started)).await
    }

    async fn fence_inner(&mut self, started: Option<SourceRevision>) -> Result<SourceWindow<T>, SourceError> {
        let _gate = self.maintenance.0.gate.lock().await;
        let source = self.maintenance.0.state.lock().unwrap();
        let mut reader = self.reader.state.lock().unwrap();
        if let Some(error) = &reader.error {
            return Err(error.clone());
        }
        let revision = SourceRevision(source.revision);
        if started.is_some_and(|started| started != revision) {
            return Err(SourceError::Changed);
        }
        Ok(SourceWindow { revision, batch: reader.pending.take() })
    }

    /// Narrow the bootstrap watch only when the supplied membership includes
    /// every committed change. On Changed, drain/apply another fence and retry.
    pub(crate) async fn finish_bootstrap(
        &mut self,
        predicate: Predicate<Resolved>,
        members: impl IntoIterator<Item = EntityId>,
        revision: SourceRevision,
    ) -> Result<(), SourceError> {
        let _gate = self.maintenance.0.gate.lock().await;
        let source = self.maintenance.0.state.lock().unwrap();
        if let Some(error) = &self.reader.state.lock().unwrap().error {
            return Err(error.clone());
        }
        if source.revision != revision.0 {
            return Err(SourceError::Changed);
        }
        let predicate = watch_predicate(predicate);
        let mut old = self.reader.predicate.lock().unwrap();
        let mut watchers = self.maintenance.0.watchers.lock().unwrap();
        watchers.recurse_predicate_watchers(&old, (self.reader.id, self.reader.query), WatcherOp::Remove);
        watchers.remove_subscription_entity_watchers(self.reader.id);
        watchers.recurse_predicate_watchers(&predicate, (self.reader.id, self.reader.query), WatcherOp::Add);
        watchers.add_predicate_entity_watchers(self.reader.id, self.reader.query, members);
        *old = predicate;
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> Result<SourceBatch<T>, SourceError> {
        loop {
            let notified = self.reader.notify.notified();
            {
                let mut reader = self.reader.state.lock().unwrap();
                if let Some(error) = &reader.error {
                    return Err(error.clone());
                }
                if let Some(batch) = reader.pending.take() {
                    return Ok(batch);
                }
            }
            notified.await;
        }
    }
}

impl<T> Drop for MaintenanceSubscription<T> {
    fn drop(&mut self) {
        let mut source = self.maintenance.0.state.lock().unwrap();
        source.readers.remove(&self.reader.id);
        let predicate = self.reader.predicate.lock().unwrap();
        let mut watchers = self.maintenance.0.watchers.lock().unwrap();
        watchers.recurse_predicate_watchers(&predicate, (self.reader.id, self.reader.query), WatcherOp::Remove);
        watchers.remove_subscription_entity_watchers(self.reader.id);
    }
}

/// Property indexes do not reproduce every evaluator cast or expression. A
/// membership-only superset gives maintenance a conservative candidate route;
/// the projector still evaluates the entire predicate against the entity.
fn watch_predicate(predicate: Predicate<Resolved>) -> Predicate<Resolved> {
    match predicate {
        Predicate::And(left, right) => match (watch_predicate(*left), watch_predicate(*right)) {
            (Predicate::False, _) | (_, Predicate::False) => Predicate::False,
            (Predicate::True, predicate) | (predicate, Predicate::True) => predicate,
            (left, right) => Predicate::And(Box::new(left), Box::new(right)),
        },
        Predicate::Or(left, right) => match (watch_predicate(*left), watch_predicate(*right)) {
            (Predicate::True, _) | (_, Predicate::True) => Predicate::True,
            (Predicate::False, predicate) | (predicate, Predicate::False) => predicate,
            (left, right) => Predicate::Or(Box::new(left), Box::new(right)),
        },
        predicate @ (Predicate::MemberOf(_) | Predicate::True | Predicate::False) => predicate,
        _ => Predicate::True,
    }
}

#[cfg(test)]
mod tests;
