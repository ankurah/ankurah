//! A read of the IndexedDB engine's maps apart from any storage transaction.

use ankurah_core::error::RetrievalError;
use ankurah_core::storage::{LockName, MapRead, MapRows, MapSnapshot};
use async_trait::async_trait;

use super::reading::Reading;
use crate::database::Database;

/// A snapshot of the IndexedDB engine's maps: its reads see one committed
/// state, the one its first read saw, for the writes under the names it
/// reads under.
///
/// A read that finds a noted version moved fails as a write conflict, and
/// its caller retries with a fresh snapshot. Sustained writes under the same
/// names can prevent a snapshot from completing.
pub struct IndexedDbMapSnapshot {
    database: Database,

    /// The names it reads under, until its first read notes their versions.
    locks: Vec<LockName>,

    reading: Reading,
}

impl IndexedDbMapSnapshot {
    /// A snapshot on `database` under `locks`, which reads nothing until its
    /// first read.
    pub(in crate::engine) fn new(database: Database, locks: &[LockName]) -> Self {
        Self { database, locks: locks.to_vec(), reading: Reading::default() }
    }
}

#[async_trait]
impl MapSnapshot for IndexedDbMapSnapshot {
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError> {
        let locks = match self.reading.has_noted() {
            true => Vec::new(),
            false => self.locks.clone(),
        };

        Ok(self.reading.read(&self.database, &locks, &reads).await?)
    }
}
