//! A read of the maps that sees one committed state under the lock names it
//! reads by: while it is open, no commit under those names lands.

use ankurah_core::error::RetrievalError;
use ankurah_core::storage::{LockName, MapRead, MapRows, MapSnapshot};
use async_trait::async_trait;
use tokio::sync::OwnedRwLockReadGuard;

use super::locks::Reading;
use super::read;
use crate::SledStorageEngine;

/// A snapshot of the sled engine's maps, begun by
/// [`StorageEngine::map_snapshot`](ankurah_core::storage::StorageEngine::map_snapshot).
///
/// It reads what sled holds as it reads, and holds the readers' lock of each
/// name it was begun under, so that no commit under those names lands until
/// it is dropped: its reads of what those names guard see the maps as the
/// last commit before it left them. It shares the reset lock, so that no
/// reset empties the maps while it is open.
pub struct SledMapSnapshot {
    /// The maps' sled tree.
    maps: sled::Tree,

    _reading: Vec<Reading>,
    _reset: OwnedRwLockReadGuard<()>,
}

impl SledMapSnapshot {
    /// A snapshot of `engine`'s maps under `locks`, once no reset is under
    /// way and no commit under those names is landing.
    pub(crate) async fn begin(engine: &SledStorageEngine, locks: &[LockName]) -> Self {
        let reset = engine.map_locks.share_reset().await;
        let reading = engine.map_locks.read_under(locks).await;
        let maps = engine.database.lock().unwrap().maps_tree.clone();

        Self { maps, _reading: reading, _reset: reset }
    }
}

#[async_trait]
impl MapSnapshot for SledMapSnapshot {
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError> { read::stored(&self.maps, reads).await }
}
