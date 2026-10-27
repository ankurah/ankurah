//! Stores with quirks a real store may have, and every row a store holds, as
//! a test compares them.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use ankurah_digest_tree::{DigestTree, Encode, MemoryStore, Ristretto255, Row, RowKey, ScanDirection, Store, StoreError, WriteItem};

use super::members::{Id, Item, Spec, Tag};

/// Every row `store` holds, encoded, in key order.
pub async fn stored_rows<V: Encode + Clone + Send + Sync + 'static>(store: &impl Store<RowKey, V>) -> Vec<(RowKey, Vec<u8>)> {
    let rows = store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await.unwrap();
    rows.into_iter().map(|(key, row)| (key, row.encode())).collect()
}

/// A memory store that counts the batches written to it, so a test can tell
/// how many writes an operation made, not only what its rows ended up as.
/// Clones share the rows and the count.
#[derive(Clone)]
pub struct CountingStore<V> {
    rows: MemoryStore<RowKey, V>,
    writes: Arc<AtomicUsize>,
}

impl<V> CountingStore<V> {
    pub fn new() -> Self { Self { rows: MemoryStore::new(), writes: Arc::new(AtomicUsize::new(0)) } }

    /// How many batches have been written.
    pub fn writes(&self) -> usize { self.writes.load(Ordering::Relaxed) }
}

impl<V: Clone + Send + Sync + 'static> Store<RowKey, V> for CountingStore<V> {
    async fn get(&self, key: &RowKey) -> Result<Option<V>, StoreError> { self.rows.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, StoreError> {
        self.rows.range(start, end, direction, limit).await
    }

    async fn write(&self, items: Vec<WriteItem<RowKey, V>>) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.rows.write(items).await
    }
}

/// The rows of a tree of items, over a store that counts its writes.
pub type CountedStore = CountingStore<Row<Spec, Tag, Ristretto255>>;

/// A tree of items over a store that counts its writes.
pub type CountedTree = DigestTree<Spec, Id, Tag, Ristretto255, CountedStore>;

/// A counted tree holding exactly `members`, and its store.
pub async fn counted(key_spec: Spec, members: impl IntoIterator<Item = Item>) -> (CountedTree, CountedStore) {
    let store = CountingStore::new();
    (DigestTree::build(store.clone(), key_spec, members).await.unwrap(), store)
}

/// A memory store whose writes fail while a test says so, as a real store's
/// may when its disk or its connection goes. Clones share the rows and the
/// switch.
#[derive(Clone)]
pub struct FailingWrites<V> {
    rows: MemoryStore<RowKey, V>,
    failing: Arc<AtomicBool>,
}

impl<V> FailingWrites<V> {
    pub fn new() -> Self { Self { rows: MemoryStore::new(), failing: Arc::new(AtomicBool::new(false)) } }

    /// Make every write from now on fail, as a failure trying again may fix,
    /// or succeed again.
    pub fn fail_writes(&self, failing: bool) { self.failing.store(failing, Ordering::Relaxed) }
}

impl<V: Clone + Send + Sync + 'static> Store<RowKey, V> for FailingWrites<V> {
    async fn get(&self, key: &RowKey) -> Result<Option<V>, StoreError> { self.rows.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, StoreError> {
        self.rows.range(start, end, direction, limit).await
    }

    async fn write(&self, items: Vec<WriteItem<RowKey, V>>) -> Result<(), StoreError> {
        if self.failing.load(Ordering::Relaxed) {
            return Err(StoreError::retryable("the store's disk went away"));
        }

        self.rows.write(items).await
    }
}

/// The rows of a tree of items, over a store whose writes can fail.
pub type FailingStore = FailingWrites<Row<Spec, Tag, Ristretto255>>;

/// A tree of items over a store whose writes can fail.
pub type FailingTree = DigestTree<Spec, Id, Tag, Ristretto255, FailingStore>;

/// A memory store that returns at most three rows per read, as a store that
/// pages its reads may: the tree must read on.
#[derive(Clone)]
pub struct ShortPages<V>(pub MemoryStore<RowKey, V>);

impl<V: Clone + Send + Sync + 'static> Store<RowKey, V> for ShortPages<V> {
    async fn get(&self, key: &RowKey) -> Result<Option<V>, StoreError> { self.0.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, StoreError> {
        self.0.range(start, end, direction, limit.min(3)).await
    }

    async fn write(&self, items: Vec<WriteItem<RowKey, V>>) -> Result<(), StoreError> { self.0.write(items).await }
}
