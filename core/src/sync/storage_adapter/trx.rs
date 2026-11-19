//! One digest-tree transaction over locked storage or a private managed version.

use std::collections::BTreeMap;
use std::ops::Bound;

use ankurah_digest_tree::{DigestGroup, Encode, ReadTrx, Row, RowKey, StoreError, StoreTrx, WriteItem};

use super::by_key::ByKey;
use super::rows::{decode_entry, encode_bound, ranged};
use super::{EngineAdapter, TreeLanding};
use crate::storage::{MapRead, MapWrite, ScanDirection, StorageEngine, StorageTransaction};
use crate::sync::error::transaction_store_failure;
use crate::sync::storage_adapter::cache::admits;
use crate::sync::storage_adapter::layout::entry::{Status, TreeEntry};
use crate::sync::storage_adapter::layout::log::{self, Changed};
use crate::sync::storage_adapter::layout::names::trees_map;
use crate::sync::{Error, TreePart};

/// Adapts the owner's storage transaction to [`StoreTrx`] for one tree's
/// creation or commit. Direct writes hold the tree's storage lock; managed
/// writes modify a private version whose durable base is checked again at
/// flush. Both read their own writes over one consistent starting version.
pub struct StorageEngineTrx<'a, SE: StorageEngine, K, L, G> {
    store: EngineAdapter<SE, K, L, G>,
    trx: Option<&'a mut (dyn StorageTransaction + 'a)>,
    buffered: Option<super::StorageEngineSnapshot<SE, K, L, G>>,

    /// The generation the tree's rows were at when the transaction took the
    /// tree's lock, or `None` for a tree it creates.
    locked: Option<u64>,

    /// The generation the transaction's writes have brought the tree's rows
    /// to: `locked` until it writes them.
    generation: Option<u64>,

    /// The leaves a commit checks, as a read with the tree's lock found them:
    /// answered again without another request while the transaction hasn't
    /// written the tree's rows.
    leaves: BTreeMap<RowKey, Option<Vec<u8>>>,
}

impl<'a, SE: StorageEngine, K, L, G> StorageEngineTrx<'a, SE, K, L, G> {
    /// `store`'s tree, read and written through `trx`, which holds the
    /// tree's lock: the tree's rows were at generation `locked` when it took
    /// it, or `None` for a tree it creates, and `leaves` are what it read of
    /// them with the lock.
    pub(in crate::sync) fn new(
        store: EngineAdapter<SE, K, L, G>,
        trx: &'a mut (dyn StorageTransaction + 'a),
        locked: Option<u64>,
        leaves: BTreeMap<RowKey, Option<Vec<u8>>>,
    ) -> Self {
        Self { store, trx: Some(trx), buffered: None, locked, generation: locked, leaves }
    }

    pub(super) fn buffered(store: EngineAdapter<SE, K, L, G>, read: super::StorageEngineSnapshot<SE, K, L, G>) -> Self {
        Self { store, trx: None, buffered: Some(read), locked: None, generation: None, leaves: BTreeMap::new() }
    }

    /// The store the transaction is lent as.
    pub(super) fn store(&self) -> &EngineAdapter<SE, K, L, G> { &self.store }

    /// The generation the transaction's reads see the tree at, if it is the
    /// one the last landed commit left: until the transaction writes the
    /// tree's rows, what it read with the lock still holds, and the cache's
    /// rows at that generation do.
    fn landed_generation(&self) -> Option<u64> { self.locked.filter(|_| self.generation == self.locked) }
}

impl<SE, K, L, G> ReadTrx<RowKey, Row<K, L, G>> for StorageEngineTrx<'_, SE, K, L, G>
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

    /// The rows under `keys`: while the transaction hasn't written, the
    /// leaves read with the lock from what that read found, and the rows the
    /// cache keeps at the locked generation from the cache; the rest in one
    /// read through the transaction, which the cache keeps if the transaction
    /// hasn't written.
    async fn get_many(&mut self, keys: &[RowKey]) -> Result<Vec<Option<Row<K, L, G>>>, StoreError> {
        if let Some(read) = &mut self.buffered {
            return read.get_many(keys).await;
        }
        let tree = self.store.inner();
        let landed = self.landed_generation();
        let mut read = ByKey::new(keys, tree.rows.clone());

        if let Some(generation) = landed {
            read.fill_from_prefetch(&tree.id, &self.leaves)?;
            read.fill_from_cache(tree, generation);
        }

        if let Some(unread) = read.unread() {
            let answers =
                self.trx.as_mut().expect("storage transaction").map_read(vec![unread]).await.map_err(transaction_store_failure)?;
            read.fill_from_storage(tree, answers, landed)?;
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
        if let Some(read) = &mut self.buffered {
            return read.range(start, end, direction, limit).await;
        }
        let read =
            MapRead::Range { map: self.store.inner().rows.clone(), start: encode_bound(start), end: encode_bound(end), direction, limit };
        let entries =
            ranged(self.trx.as_mut().expect("storage transaction").map_read(vec![read]).await.map_err(transaction_store_failure)?)?;

        let id = &self.store.inner().id;
        entries.into_iter().map(|entry| decode_entry(id, entry)).collect()
    }
}

impl<SE, K, L, G> StoreTrx<RowKey, Row<K, L, G>> for StorageEngineTrx<'_, SE, K, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    type Pending = TreeLanding;

    /// One map write holding the crate's batch, into the tree's rows; the
    /// tree's entry at the next generation, generation 0 for a tree being
    /// created; and, for a tree that was there before, the next generation's
    /// change-log entry, and the deletion of the entry the log no longer
    /// reaches back to. An empty batch writes nothing, and the generation
    /// stays.
    async fn write(&mut self, batch: Vec<WriteItem<RowKey, Row<K, L, G>>>) -> Result<TreeLanding, StoreError> {
        if batch.is_empty() {
            return Ok(TreeLanding::nothing());
        }

        if let Some(read) = &mut self.buffered {
            let writes = super::buffered::encode_batch(read.rows_map(), batch);
            read.writes.add(writes.clone());
            return Ok(TreeLanding::buffered(writes));
        }
        let tree = self.store.inner();
        let generation = match self.generation {
            None => 0,

            // Only a damaged entry holds the last generation there is.
            Some(generation) => generation.checked_add(1).ok_or_else(|| Error::damaged(&tree.id, TreePart::Entry, None))?,
        };

        let mut changed: Changed = Vec::new();
        let mut writes: Vec<MapWrite> = Vec::with_capacity(batch.len() + 3);
        for item in batch {
            let (key, row) = match item {
                WriteItem::Put(key, row) => (key, Some(row.encode())),
                WriteItem::Delete(key) => (key, None),
            };

            let (cached, key) = (admits(&key), key.encode());
            if cached {
                changed.push((key.clone(), row.clone()));
            }

            writes.push(match row {
                Some(value) => MapWrite::Put { map: tree.rows.clone(), key, value },
                None => MapWrite::Delete { map: tree.rows.clone(), key },
            });
        }

        let entry = TreeEntry { incarnation: tree.incarnation, generation, status: Status::Ready };
        writes.push(MapWrite::Put { map: trees_map(), key: tree.id.entry_key(), value: entry.encode() });

        if self.generation.is_some() {
            writes.push(MapWrite::Put { map: tree.log.clone(), key: log::key(generation), value: log::entry(&changed) });

            if let Some(unreached) = generation.checked_sub(log::LENGTH) {
                writes.push(MapWrite::Delete { map: tree.log.clone(), key: log::key(unreached) });
            }
        }

        self.trx.as_mut().expect("storage transaction").map_write(writes).await.map_err(transaction_store_failure)?;
        self.generation = Some(generation);

        Ok(TreeLanding::of(generation, changed))
    }
}
