//! Maps with quirks a real store's rows may have, each kept behind a
//! reference store's locks, and every row a store holds, as a test compares
//! them.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_digest_tree::{
    DigestTree, Encode, Locked, MemoryMap, OrderedMap, ReadTrx, Ristretto255, Row, RowKey, ScanDirection, Store, StoreError, WriteItem,
};

use super::members::{Id, Item, Property, Tag};

/// Every row `store` holds, encoded, in key order, through one read.
pub async fn stored_rows<V: Encode>(store: &impl Store<RowKey, V>) -> Vec<(RowKey, Vec<u8>)> {
    let mut read = store.begin_read().await.unwrap();
    let rows = read.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await.unwrap();
    rows.into_iter().map(|(key, row)| (key, row.encode())).collect()
}

/// What a counting map has been asked: the batches applied to it, its reads,
/// the rows they returned and how many of those are node rows, and its range
/// reads over node rows. Clones share the counts.
#[derive(Clone, Default)]
pub struct Counts {
    writes: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    rows_read: Arc<AtomicUsize>,
    nodes_read: Arc<AtomicUsize>,
    node_ranges: Arc<AtomicUsize>,
}

impl Counts {
    /// How many batches have been applied.
    pub fn writes(&self) -> usize { self.writes.load(Ordering::Relaxed) }

    /// How many reads the map has answered, whatever they returned.
    pub fn reads(&self) -> usize { self.reads.load(Ordering::Relaxed) }

    /// How many rows reads have returned.
    pub fn rows_read(&self) -> usize { self.rows_read.load(Ordering::Relaxed) }

    /// How many of the rows reads have returned are node rows.
    pub fn nodes_read(&self) -> usize { self.nodes_read.load(Ordering::Relaxed) }

    /// How many range reads have reached over node rows' keys, which sort
    /// after the metadata row's and before every leaf's, whatever they
    /// returned.
    pub fn node_ranges(&self) -> usize { self.node_ranges.load(Ordering::Relaxed) }

    fn count_read<'k>(&self, keys: impl IntoIterator<Item = &'k RowKey>) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        for key in keys {
            self.rows_read.fetch_add(1, Ordering::Relaxed);
            self.nodes_read.fetch_add(usize::from(matches!(key, RowKey::Node(_))), Ordering::Relaxed);
        }
    }
}

/// A memory map that counts what it is asked, so a test can tell how much an
/// operation wrote and read, not only what its rows ended up as.
pub struct Counting<V> {
    rows: MemoryMap<RowKey, V>,
    counts: Counts,
}

impl<V: Clone + Send + Sync + 'static> OrderedMap for Counting<V> {
    type Key = RowKey;
    type Value = V;

    async fn get(&self, key: &RowKey) -> Result<Option<V>, StoreError> {
        let row = self.rows.get(key).await?;
        self.counts.count_read(row.is_some().then_some(key));

        Ok(row)
    }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, StoreError> {
        let rows = self.rows.range(start, end, direction, limit).await?;
        self.counts.count_read(rows.iter().map(|(key, _)| key));

        let starts_before_the_leaves = !matches!(start, Bound::Included(RowKey::Leaf(_)) | Bound::Excluded(RowKey::Leaf(_)));
        let ends_after_the_metadata = !matches!(end, Bound::Included(RowKey::Meta) | Bound::Excluded(RowKey::Meta));
        if starts_before_the_leaves && ends_after_the_metadata {
            self.counts.node_ranges.fetch_add(1, Ordering::Relaxed);
        }

        Ok(rows)
    }

    async fn apply(&self, batch: Vec<WriteItem<RowKey, V>>) -> Result<(), StoreError> {
        self.counts.writes.fetch_add(1, Ordering::Relaxed);
        self.rows.apply(batch).await
    }
}

/// A store of a tree of items over a map that counts what it is asked.
pub type CountedStore = Locked<Counting<Row<Property, Tag, Ristretto255>>>;

/// A tree of items over a map that counts what it is asked.
pub type CountedTree = DigestTree<Property, Id, Tag, Ristretto255, CountedStore>;

/// An empty store over a map that counts what it is asked, and its counts.
pub fn counted_store() -> (CountedStore, Counts) {
    let counts = Counts::default();
    (Locked::over(Counting { rows: MemoryMap::new(), counts: counts.clone() }), counts)
}

/// A counted tree holding exactly `members`, and its map's counts.
pub async fn counted(key_spec: KeySpec<Property>, members: impl IntoIterator<Item = Item>) -> (CountedTree, Counts) {
    let (store, counts) = counted_store();
    (DigestTree::create(store, key_spec, members).await.unwrap(), counts)
}

/// Switches that make a failing map's reads or writes fail, as a real
/// store's may when its disk or its connection goes. Clones share the
/// switches.
#[derive(Clone, Default)]
pub struct Faults {
    reads: Arc<AtomicBool>,
    writes: Arc<AtomicBool>,
}

impl Faults {
    /// Make every read from now on fail, as a failure trying again may fix,
    /// or succeed again.
    pub fn fail_reads(&self, failing: bool) { self.reads.store(failing, Ordering::Relaxed) }

    /// Make every batch applied from now on fail, as a failure trying again
    /// may fix, or succeed again.
    pub fn fail_writes(&self, failing: bool) { self.writes.store(failing, Ordering::Relaxed) }
}

/// Refuse an operation while its switch says it fails.
fn refuse_while(failing: &AtomicBool) -> Result<(), StoreError> {
    match failing.load(Ordering::Relaxed) {
        true => Err(StoreError::retryable("the store's disk went away")),
        false => Ok(()),
    }
}

/// A memory map whose reads or writes fail while its faults say so.
pub struct Failing<V> {
    rows: MemoryMap<RowKey, V>,
    faults: Faults,
}

impl<V: Clone + Send + Sync + 'static> OrderedMap for Failing<V> {
    type Key = RowKey;
    type Value = V;

    async fn get(&self, key: &RowKey) -> Result<Option<V>, StoreError> {
        refuse_while(&self.faults.reads)?;
        self.rows.get(key).await
    }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, StoreError> {
        refuse_while(&self.faults.reads)?;
        self.rows.range(start, end, direction, limit).await
    }

    async fn apply(&self, batch: Vec<WriteItem<RowKey, V>>) -> Result<(), StoreError> {
        refuse_while(&self.faults.writes)?;
        self.rows.apply(batch).await
    }
}

/// A store of a tree of items over a map whose reads or writes can fail.
pub type FailingStore = Locked<Failing<Row<Property, Tag, Ristretto255>>>;

/// A tree of items over a map whose reads or writes can fail.
pub type FailingTree = DigestTree<Property, Id, Tag, Ristretto255, FailingStore>;

/// An empty store over a map whose reads or writes fail while its faults say
/// so, and the faults, none of them on yet.
pub fn failing_store() -> (FailingStore, Faults) {
    let faults = Faults::default();
    (Locked::over(Failing { rows: MemoryMap::new(), faults: faults.clone() }), faults)
}
