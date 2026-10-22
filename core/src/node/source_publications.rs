//! Immutable memory publications between a query's initial read and its standing feed.
//!
//! This is a source revision, not an atomic database snapshot. A query subscribes
//! before reading, then reconciles its captured rows with the complete window up
//! to a fence. Complete residents fold that window. Limited residents validate
//! an unchanged revision around their bounded read so unchanged rank replacements
//! cannot be missed.
use crate::entity::CapturedEntity;
use crate::{changes::EntityChange, reactor::ChangeNotification};
use ankurah_proto::{Attested, Event};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard};

pub(crate) const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_PENDING_BATCHES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceRevision(pub(crate) u64);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SourceError {
    #[error("source publication is unavailable after an incomplete commit; reconstruct the node")]
    Unavailable,
    #[error("source reader fell behind its bounded change window")]
    Lagged,
    #[error("source changed while the query was being read")]
    Changed,
    #[error("query catalog changed; reopen the query generation")]
    CatalogChanged,
}

/// State and published events from one admitted entity revision. Neither is
/// read back through a live entity when a delayed reader consumes this record.
#[derive(Debug, Clone)]
pub(crate) struct SourceChange {
    pub(crate) state: Arc<CapturedEntity>,
    pub(crate) events: Vec<Attested<Event>>,
}

#[derive(Debug)]
pub(crate) struct SourceBatch {
    pub(crate) revision: SourceRevision,
    pub(crate) changes: Vec<SourceChange>,
    pub(crate) bytes: usize,
}

pub(crate) struct SourceWindow {
    pub(crate) revision: SourceRevision,
    pub(crate) batches: Vec<Arc<SourceBatch>>,
}

/// Owns the short commit/publication gate. Scans and policy evaluation never
/// hold this gate. Each reader retains its own bounded window independently.
#[derive(Clone, Default)]
pub(crate) struct SourcePublications(Arc<SourceInner>);

#[derive(Default)]
struct SourceInner {
    gate: Arc<AsyncMutex<()>>,
    state: Mutex<Publications>,
}

#[derive(Default)]
struct Publications {
    revision: u64,
    unavailable: bool,
    readers: Vec<Weak<Reader>>,
}

struct Reader {
    state: Mutex<ReaderState>,
    notify: Notify,
}

#[derive(Default)]
struct ReaderState {
    queue: VecDeque<(Arc<SourceBatch>, usize)>,
    bytes: usize,
    error: Option<SourceError>,
}

pub(crate) struct SourceCursor {
    source: SourcePublications,
    reader: Arc<Reader>,
}

/// A dropped commit future cannot leave a successful-looking source fence.
/// Once storage may have committed, uncertainty invalidates this source until
/// the node is reconstructed; merely retrying a query cannot repair a resident
/// that the interrupted writer might not have published.
pub(crate) struct SourceCommit {
    source: SourcePublications,
    _gate: OwnedMutexGuard<()>,
    finished: bool,
}

impl SourcePublications {
    pub(crate) async fn begin_commit(&self) -> SourceCommit {
        SourceCommit { source: self.clone(), _gate: self.0.gate.clone().lock_owned().await, finished: false }
    }

    pub(crate) async fn subscribe(&self) -> Result<(SourceRevision, SourceCursor), SourceError> {
        let _gate = self.0.gate.lock().await;
        let mut source = self.0.state.lock().unwrap();
        if source.unavailable {
            return Err(SourceError::Unavailable);
        }
        let reader = Arc::new(Reader { state: Mutex::new(ReaderState::default()), notify: Notify::new() });
        source.readers.retain(|reader| reader.strong_count() != 0);
        source.readers.push(Arc::downgrade(&reader));
        Ok((SourceRevision(source.revision), SourceCursor { source: self.clone(), reader }))
    }

    fn invalidate(&self) {
        let mut source = self.0.state.lock().unwrap();
        source.unavailable = true;
        source.readers.retain(|reader| {
            let Some(reader) = reader.upgrade() else {
                return false;
            };
            let mut state = reader.state.lock().unwrap();
            state.queue.clear();
            state.bytes = 0;
            state.error = Some(SourceError::Unavailable);
            reader.notify.notify_one();
            true
        });
    }
}

impl SourceCommit {
    /// Only an explicit conflict establishes that storage published nothing.
    /// Other errors and cancellation leave commit outcome uncertain; the guard
    /// stays armed until resident publication and immutable admission finish.
    pub(crate) fn storage_result(
        &mut self,
        result: Result<crate::storage::StorageCommitOutcome, crate::error::MutationError>,
    ) -> Result<crate::storage::StorageCommitOutcome, crate::error::MutationError> {
        if matches!(&result, Ok(crate::storage::StorageCommitOutcome::Conflict { .. }) | Err(crate::error::MutationError::WriteConflict)) {
            self.finished = true;
        }
        result
    }

    /// Admit the whole committed batch before releasing the publication gate.
    /// Initial transfer admits state but no live events, just as reactor delivery does.
    pub(crate) fn publish(mut self, changes: &[EntityChange], live_events: bool) {
        let mut source = self.source.0.state.lock().unwrap();
        source.readers.retain(|reader| reader.strong_count() != 0);
        if source.unavailable || changes.is_empty() {
            self.finished = true;
            return;
        }
        let Some(revision) = source.revision.checked_add(1) else {
            drop(source);
            return;
        };
        source.revision = revision;
        // No reader can need an immutable row before it registered its window.
        // Avoid serialization and an additional state cache when there are none.
        if source.readers.is_empty() {
            self.finished = true;
            return;
        }
        let captured: Result<Vec<_>, _> = changes
            .iter()
            .map(|change| {
                Ok(SourceChange {
                    state: Arc::new(change.entity().capture()?),
                    events: if live_events { change.events().to_vec() } else { Vec::new() },
                })
            })
            .collect::<Result<_, crate::error::StateError>>();
        let Ok(changes) = captured else {
            drop(source);
            return;
        };
        let bytes = changes.iter().try_fold(0usize, |total, change| {
            let size = bincode::serialized_size(&(&*change.state, &change.events)).ok()?;
            total.checked_add(usize::try_from(size).ok()?)?.checked_add(change.state.lookup_bytes())
        });
        let Some(bytes) = bytes else {
            drop(source);
            return;
        };
        let batch = Arc::new(SourceBatch { revision: SourceRevision(revision), changes, bytes });
        for reader in source.readers.iter().filter_map(Weak::upgrade) {
            let mut state = reader.state.lock().unwrap();
            if state.error.is_some() {
                continue;
            }
            if state.queue.len() >= MAX_PENDING_BATCHES || bytes > MAX_PENDING_BYTES.saturating_sub(state.bytes) {
                state.queue.clear();
                state.bytes = 0;
                state.error = Some(SourceError::Lagged);
            } else {
                state.bytes += bytes;
                state.queue.push_back((batch.clone(), bytes));
            }
            reader.notify.notify_one();
        }
        self.finished = true;
    }
}

impl Drop for SourceCommit {
    fn drop(&mut self) {
        if !self.finished {
            self.source.invalidate();
        }
    }
}

impl SourceCursor {
    /// Close a finite read window. Publications after this fence remain queued.
    pub(crate) async fn fence(&mut self) -> Result<SourceWindow, SourceError> { self.fence_inner(None).await }

    /// A bounded read cannot reconstruct omitted rank replacements from changes
    /// alone. Reject it if a commit crossed the read, leaving its window queued.
    pub(crate) async fn validate_limited_read(&mut self, started: SourceRevision) -> Result<SourceWindow, SourceError> {
        self.fence_inner(Some(started)).await
    }

    async fn fence_inner(&mut self, started: Option<SourceRevision>) -> Result<SourceWindow, SourceError> {
        let _gate = self.source.0.gate.lock().await;
        let source = self.source.0.state.lock().unwrap();
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
        reader.bytes = 0;
        Ok(SourceWindow { revision, batches: reader.queue.drain(..).map(|(batch, _)| batch).collect() })
    }

    /// Wait for another complete publication; callers still choose their finite
    /// completion fence rather than chasing this feed indefinitely.
    pub(crate) async fn next(&mut self) -> Result<Arc<SourceBatch>, SourceError> {
        loop {
            let notified = self.reader.notify.notified();
            {
                let mut reader = self.reader.state.lock().unwrap();
                if let Some(error) = &reader.error {
                    return Err(error.clone());
                }
                if let Some((batch, bytes)) = reader.queue.pop_front() {
                    reader.bytes -= bytes;
                    return Ok(batch);
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests;
