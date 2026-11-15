//! The reads a digest tree begins itself, each in a snapshot of the engine's
//! maps.

use std::ops::Bound;

use ankurah_digest_tree::{DigestGroup, Encode, ReadTrx, Row, RowKey, StoreError};

use super::by_key::ByKey;
use super::rows::{decode_entry, encode_bound, ranged};
use super::EngineAdapter;
use crate::storage::{MapRead, MapSnapshot, ScanDirection, StorageEngine};
use crate::sync::error::snapshot_store_failure;

/// Adapts an engine's snapshot to [`ReadTrx`] for one tree. After opening the
/// snapshot, its first read checks that the tree's incarnation is current
/// and reads its generation. Reads by key can use cached rows only at that
/// generation.
pub struct StorageEngineSnapshot<SE: StorageEngine, K, L, G> {
    store: EngineAdapter<SE, K, L, G>,
    snapshot: SE::Snapshot,

    /// The generation the snapshot sees the tree's rows at.
    generation: u64,
}

impl<SE: StorageEngine, K, L, G> StorageEngineSnapshot<SE, K, L, G> {
    pub(super) fn new(store: EngineAdapter<SE, K, L, G>, snapshot: SE::Snapshot, generation: u64) -> Self {
        Self { store, snapshot, generation }
    }
}

impl<SE, K, L, G> ReadTrx<RowKey, Row<K, L, G>> for StorageEngineSnapshot<SE, K, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    async fn get(&mut self, key: &RowKey) -> Result<Option<Row<K, L, G>>, StoreError> {
        let mut rows = self.get_many(std::slice::from_ref(key)).await?;
        Ok(rows.pop().flatten())
    }

    /// The rows under `keys`: those the cache keeps at the snapshot's
    /// generation from the cache, and the rest in one read of the snapshot,
    /// which the cache then keeps.
    async fn get_many(&mut self, keys: &[RowKey]) -> Result<Vec<Option<Row<K, L, G>>>, StoreError> {
        let tree = self.store.inner();
        let mut read = ByKey::new(keys);
        read.fill_from_cache(tree, self.generation);

        if let Some(unread) = read.unread(tree) {
            let answers = self.snapshot.map_read(vec![unread]).await.map_err(snapshot_store_failure)?;
            read.fill_from_storage(tree, answers, Some(self.generation))?;
        }

        Ok(read.rows())
    }

    async fn range(
        &mut self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, Row<K, L, G>)>, StoreError> {
        let tree = self.store.inner();
        let read = MapRead::Range { map: tree.rows.clone(), start: encode_bound(start), end: encode_bound(end), direction, limit };

        let entries = ranged(self.snapshot.map_read(vec![read]).await.map_err(snapshot_store_failure)?)?;
        entries.into_iter().map(|entry| decode_entry(&tree.id, entry)).collect()
    }
}
