//! Reactor subscriptions for indexes that retain metadata rather than entities.
//!
//! Projection happens while the committed entity is available. Only the projected
//! values enter each subscription's bounded queue; no entity handles are retained.
use super::{watcherset::WatcherOp, ChangeNotification, ReactorSubscriptionId, WatcherChange, WatcherSet};
use crate::changes::EntityChange;
use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{EntityId, QueryId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard};

pub(crate) const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceRevision(pub(crate) u64);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SourceError {
    #[error("reactor maintenance is unavailable after an incomplete commit; reconstruct the node")]
    Unavailable,
    #[error("index maintenance fell behind its bounded change window")]
    Lagged,
    #[error("source changed while the index was being read")]
    Changed,
    #[error("query catalog changed; reopen the query generation")]
    CatalogChanged,
}

/// Compact result of inspecting one committed entity. `member` controls the
/// entity watch needed to observe its later departure from the predicate.
pub(crate) struct ProjectedChange<T> {
    pub(crate) value: T,
    pub(crate) member: bool,
    pub(crate) bytes: usize,
}

#[derive(Debug)]
pub(crate) struct SourceBatch<T> {
    pub(crate) revision: SourceRevision,
    pub(crate) changes: Vec<T>,
    pub(crate) catalog_changed: bool,
    pub(crate) bytes: usize,
}

pub(crate) struct SourceWindow<T> {
    pub(crate) revision: SourceRevision,
    pub(crate) batches: Vec<Arc<SourceBatch<T>>>,
}

#[derive(Clone)]
pub(super) struct Maintenance(Arc<MaintenanceInner>);

struct MaintenanceInner {
    gate: Arc<AsyncMutex<()>>,
    state: Mutex<Publications>,
    watchers: Arc<Mutex<WatcherSet>>,
}

#[derive(Default)]
struct Publications {
    revision: u64,
    unavailable: bool,
    readers: BTreeMap<ReactorSubscriptionId, Weak<dyn MaintenanceReader>>,
}

trait MaintenanceReader: Send + Sync {
    fn project(&self, revision: SourceRevision, changes: &[&EntityChange], live_events: bool, catalog_changed: bool) -> Vec<WatcherChange>;
    fn fail(&self, error: SourceError);
}

type Projector<T> = dyn Fn(&EntityChange, bool) -> anyhow::Result<ProjectedChange<T>> + Send + Sync;

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

/// RAII registration; its lifetime is the lifetime of the resident index.
pub(crate) struct MaintenanceSubscription<T> {
    maintenance: Maintenance,
    reader: Arc<Reader<T>>,
}

/// Serializes persistence, resident publication and index projection. An
/// uncertain or interrupted commit must not leave a successful read barrier.
pub(crate) struct SourceCommit {
    maintenance: Maintenance,
    _gate: OwnedMutexGuard<()>,
    finished: bool,
}

impl Maintenance {
    pub(super) fn new(watchers: Arc<Mutex<WatcherSet>>) -> Self {
        Self(Arc::new(MaintenanceInner { gate: Arc::new(AsyncMutex::new(())), state: Mutex::default(), watchers }))
    }

    pub(super) async fn begin_commit(&self) -> SourceCommit {
        SourceCommit { maintenance: self.clone(), _gate: self.0.gate.clone().lock_owned().await, finished: false }
    }

    pub(super) async fn subscribe<T: Send + Sync + 'static>(
        &self,
        project: impl Fn(&EntityChange, bool) -> anyhow::Result<ProjectedChange<T>> + Send + Sync + 'static,
    ) -> Result<(SourceRevision, MaintenanceSubscription<T>), SourceError> {
        let _gate = self.0.gate.lock().await;
        let mut source = self.0.state.lock().unwrap();
        if source.unavailable {
            return Err(SourceError::Unavailable);
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

    pub(super) fn invalidate(&self) {
        let mut source = self.0.state.lock().unwrap();
        source.unavailable = true;
        for reader in source.readers.values().filter_map(Weak::upgrade) {
            reader.fail(SourceError::Unavailable);
        }
    }
}

impl<T: Send + Sync> MaintenanceReader for Reader<T> {
    fn project(&self, revision: SourceRevision, changes: &[&EntityChange], live_events: bool, catalog_changed: bool) -> Vec<WatcherChange> {
        let mut state = self.state.lock().unwrap();
        if state.error.is_some() || (changes.is_empty() && !catalog_changed) {
            return Vec::new();
        }
        let mut values = Vec::with_capacity(changes.len());
        let mut watchers = Vec::with_capacity(changes.len());
        let mut bytes = 0usize;
        let result = (|| {
            for change in changes {
                let projected = (self.project)(change, live_events)?;
                bytes = bytes.checked_add(projected.bytes).ok_or_else(|| anyhow::anyhow!("index change size overflow"))?;
                let entity_id = change.entity().id();
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
                pending.revision = revision;
                pending.changes.extend(values);
                pending.catalog_changed |= catalog_changed;
                pending.bytes += bytes;
            } else {
                state.pending = Some(SourceBatch { revision, changes: values, catalog_changed, bytes });
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
    pub(crate) fn storage_result(
        &mut self,
        result: Result<crate::storage::StorageCommitOutcome, crate::error::MutationError>,
    ) -> Result<crate::storage::StorageCommitOutcome, crate::error::MutationError> {
        if matches!(&result, Ok(crate::storage::StorageCommitOutcome::Conflict { .. }) | Err(crate::error::MutationError::WriteConflict)) {
            self.finished = true;
        }
        result
    }

    pub(crate) fn publish(mut self, changes: &[EntityChange], live_events: bool) {
        let mut source = self.maintenance.0.state.lock().unwrap();
        if source.unavailable || changes.is_empty() {
            self.finished = true;
            return;
        }
        let Some(revision) = source.revision.checked_add(1) else { return };
        source.revision = revision;
        source.readers.retain(|_, reader| reader.strong_count() != 0);
        let catalog_changed = changes.iter().any(|change| change.entity().memberships().iter().any(crate::schema::is_catalog_model));
        let changes = Arc::new(changes.iter().collect::<Vec<_>>());
        let mut candidates = BTreeMap::new();
        {
            let watchers = self.maintenance.0.watchers.lock().unwrap();
            for (offset, change) in changes.iter().enumerate() {
                watchers.accumulate_interested_watchers(change.entity(), offset, &changes, &mut candidates);
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
            watcher_changes.extend(reader.project(SourceRevision(revision), &interested, live_events, catalog_changed));
        }
        let mut watchers = self.maintenance.0.watchers.lock().unwrap();
        for change in watcher_changes {
            watchers.apply_watcher_change(change);
        }
        self.finished = true;
    }
}

impl Drop for SourceCommit {
    fn drop(&mut self) {
        if !self.finished {
            self.maintenance.invalidate();
        }
    }
}

impl<T> MaintenanceSubscription<T> {
    pub(crate) async fn fence(&mut self) -> Result<SourceWindow<T>, SourceError> { self.fence_inner(None).await }

    pub(crate) async fn validate_limited_read(&mut self, started: SourceRevision) -> Result<SourceWindow<T>, SourceError> {
        self.fence_inner(Some(started)).await
    }

    async fn fence_inner(&mut self, started: Option<SourceRevision>) -> Result<SourceWindow<T>, SourceError> {
        let _gate = self.maintenance.0.gate.lock().await;
        let source = self.maintenance.0.state.lock().unwrap();
        if source.unavailable {
            return Err(SourceError::Unavailable);
        }
        let mut reader = self.reader.state.lock().unwrap();
        if let Some(error) = &reader.error {
            return Err(error.clone());
        }
        let revision = SourceRevision(source.revision);
        if started.is_some_and(|started| started != revision) {
            return Err(SourceError::Changed);
        }
        Ok(SourceWindow { revision, batches: reader.pending.take().map(Arc::new).into_iter().collect() })
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
        if source.unavailable {
            return Err(SourceError::Unavailable);
        }
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

    pub(crate) async fn next(&mut self) -> Result<Arc<SourceBatch<T>>, SourceError> {
        loop {
            let notified = self.reader.notify.notified();
            {
                let mut reader = self.reader.state.lock().unwrap();
                if let Some(error) = &reader.error {
                    return Err(error.clone());
                }
                if let Some(batch) = reader.pending.take() {
                    return Ok(Arc::new(batch));
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
