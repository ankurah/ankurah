use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::executor::block_on;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

use super::*;
use crate::{Locked, MemoryMap, MemoryStore, OrderedMap, RowKey, Store, StoreTrx};

fn key(n: u8) -> RowKey { RowKey::Leaf(vec![n]) }

fn byte_of(key: &RowKey) -> u8 {
    match key {
        RowKey::Leaf(n) => n[0],
        other => panic!("a leaf key, not {other:?}"),
    }
}

fn bound(kind: u8, n: u8) -> Bound<RowKey> {
    match kind {
        0 => Bound::Unbounded,
        1 => Bound::Included(key(n)),
        _ => Bound::Excluded(key(n)),
    }
}

/// A store over `map`, holding `stored`.
fn holding<M: OrderedMap<Key = RowKey, Value = u8>>(map: M, stored: impl IntoIterator<Item = (u8, u8)>) -> Locked<M> {
    let store = Locked::over(map);
    let mut trx = block_on(store.begin());
    block_on(trx.write(stored.into_iter().map(|(n, held)| WriteItem::Put(key(n), held)).collect())).unwrap();
    block_on(trx.commit()).unwrap();

    store
}

/// Read a range through an overlay of `writes` over `store`, and check it
/// against a model that applies the writes to a copy of `stored`.
fn range_matches_the_model<M: OrderedMap<Key = RowKey, Value = u8>>(
    map: M,
    stored: &BTreeMap<u8, u8>,
    writes: &[(u8, Option<u8>)],
    (start, end): (Bound<RowKey>, Bound<RowKey>),
    direction: ScanDirection,
    limit: usize,
) -> Result<(), TestCaseError> {
    let store = holding(map, stored.iter().map(|(&n, &held)| (n, held)));
    let mut overlay = Overlay::new();
    let mut model = stored.clone();

    for (n, write) in writes {
        match write {
            Some(held) => {
                overlay.put(key(*n), *held);
                model.insert(*n, *held);
            }

            None => {
                overlay.delete(key(*n));
                model.remove(n);
            }
        }
    }

    let mut rows = block_on(store.begin_read()).unwrap();
    let read = block_on(overlay.range(&mut rows, start.as_ref(), end.as_ref(), direction, limit)).unwrap();
    let bounds = (start.as_ref().map(byte_of), end.as_ref().map(byte_of));

    let mut expected: Vec<(RowKey, u8)> = model.iter().filter(|(n, _)| bounds.contains(*n)).map(|(&n, &held)| (key(n), held)).collect();

    if direction == ScanDirection::Reverse {
        expected.reverse();
    }

    expected.truncate(limit);
    prop_assert_eq!(read, expected);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, rng_seed: RngSeed::Fixed(0x2085_0001), ..ProptestConfig::default() })]

    /// A range read sees the rows beneath under the writes laid over them, in
    /// either direction and up to any limit, as a model that applies the
    /// writes to a copy of the rows does, however the pages of the rows
    /// beneath fall against the writes.
    #[test]
    fn a_range_read_merges_the_writes_into_the_rows_beneath(
        stored in proptest::collection::btree_map(0..40u8, any::<u8>(), 0..30),
        writes in proptest::collection::vec((0..40u8, proptest::option::of(any::<u8>())), 0..30),
        (start, end) in ((0..3u8, 0..40u8), (0..3u8, 0..40u8)),
        reverse in any::<bool>(),
        limit in 0..12usize,
    ) {
        let bounds = (bound(start.0, start.1), bound(end.0, end.1));
        let direction = if reverse { ScanDirection::Reverse } else { ScanDirection::Forward };
        range_matches_the_model(MemoryMap::new(), &stored, &writes, bounds, direction, limit)?;
    }
}

/// A map that returns every row, whatever bounds it is asked for.
struct Boundless(MemoryMap<RowKey, u8>);

impl OrderedMap for Boundless {
    type Key = RowKey;
    type Value = u8;

    async fn get(&self, key: &RowKey) -> Result<Option<u8>, StoreError> { self.0.get(key).await }

    async fn range(
        &self,
        _: Bound<&RowKey>,
        _: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, u8)>, StoreError> {
        self.0.range(Bound::Unbounded, Bound::Unbounded, direction, limit).await
    }

    async fn apply(&self, batch: Vec<WriteItem<RowKey, u8>>) -> Result<(), StoreError> { self.0.apply(batch).await }
}

/// Rows outside the bounds they were asked for would send the paging that
/// follows them round in circles, so they are a permanent store failure.
#[test]
fn rows_beneath_outside_the_range_fail_the_read() {
    let store = holding(Boundless(MemoryMap::new()), (0..5).map(|n| (n, n)));
    let mut rows = block_on(store.begin_read()).unwrap();
    let overlay = Overlay::<RowKey, u8>::new();

    let failure = block_on(overlay.range(&mut rows, Bound::Included(&key(2)), Bound::Unbounded, ScanDirection::Forward, 10)).unwrap_err();
    assert!(!failure.is_retryable());
}

/// A map that returns every row between the bounds, whatever limit it is
/// asked for.
struct Limitless(MemoryMap<RowKey, u8>);

impl OrderedMap for Limitless {
    type Key = RowKey;
    type Value = u8;

    async fn get(&self, key: &RowKey) -> Result<Option<u8>, StoreError> { self.0.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        _: usize,
    ) -> Result<Vec<(RowKey, u8)>, StoreError> {
        self.0.range(start, end, direction, usize::MAX).await
    }

    async fn apply(&self, batch: Vec<WriteItem<RowKey, u8>>) -> Result<(), StoreError> { self.0.apply(batch).await }
}

/// More rows than were asked for break the limit every read is sized by, so
/// they are a permanent store failure, as rows outside the range are.
#[test]
fn more_rows_beneath_than_asked_fail_the_read() {
    let store = holding(Limitless(MemoryMap::new()), (0..5).map(|n| (n, n)));
    let mut rows = block_on(store.begin_read()).unwrap();
    let overlay = Overlay::<RowKey, u8>::new();

    let failure = block_on(overlay.range(&mut rows, Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 2)).unwrap_err();
    assert!(!failure.is_retryable());
}

/// A map that counts the range reads it answers.
struct RangesCounted {
    rows: MemoryMap<RowKey, u8>,
    ranges: Arc<AtomicUsize>,
}

impl OrderedMap for RangesCounted {
    type Key = RowKey;
    type Value = u8;

    async fn get(&self, key: &RowKey) -> Result<Option<u8>, StoreError> { self.rows.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, u8)>, StoreError> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        self.rows.range(start, end, direction, limit).await
    }

    async fn apply(&self, batch: Vec<WriteItem<RowKey, u8>>) -> Result<(), StoreError> { self.rows.apply(batch).await }
}

/// A range that ends before the limit is read in one read of the rows
/// beneath, either way and whatever writes lie over it: a page shorter than
/// asked is the end of the range.
#[test]
fn a_range_that_ends_before_the_limit_is_one_read_beneath() {
    let ranges = Arc::new(AtomicUsize::new(0));
    let store = holding(RangesCounted { rows: MemoryMap::new(), ranges: Arc::clone(&ranges) }, (0..5).map(|n| (n, n)));
    let mut rows = block_on(store.begin_read()).unwrap();

    let mut overlay = Overlay::new();
    overlay.delete(key(1));
    overlay.put(key(7), 7);

    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        ranges.store(0, Ordering::Relaxed);
        let read = block_on(overlay.range(&mut rows, Bound::Unbounded, Bound::Unbounded, direction, 10)).unwrap();

        assert_eq!(read.len(), 5, "rows 0, 2, 3 and 4 beneath, and 7 written");
        assert_eq!(ranges.load(Ordering::Relaxed), 1, "{direction:?}");
    }
}

/// A row that counts every clone of it, in a count its clones share.
#[derive(Debug)]
struct Cloned(Arc<AtomicUsize>);

impl Clone for Cloned {
    fn clone(&self) -> Self {
        self.0.fetch_add(1, Ordering::Relaxed);
        Cloned(Arc::clone(&self.0))
    }
}

/// A range read through many writes clones only the rows it returns: a read
/// of one row over a hundred written ones clones one, either way.
#[test]
fn a_range_read_through_writes_clones_only_the_rows_it_returns() {
    let clones = Arc::new(AtomicUsize::new(0));
    let store = MemoryStore::<RowKey, Cloned>::new();
    let mut rows = block_on(store.begin_read()).unwrap();

    let mut overlay = Overlay::new();
    for n in 0..100 {
        overlay.put(key(n), Cloned(Arc::clone(&clones)));
    }

    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        clones.store(0, Ordering::Relaxed);
        let read = block_on(overlay.range(&mut rows, Bound::Unbounded, Bound::Unbounded, direction, 1)).unwrap();

        assert_eq!(read.len(), 1);
        assert_eq!(clones.load(Ordering::Relaxed), 1, "{direction:?}");
    }
}

/// Every write, as one batch, in key order, the last write under a key
/// replacing the ones before it.
#[test]
fn the_writes_become_one_batch_in_key_order() {
    let mut overlay = Overlay::new();
    overlay.put(key(2), 2);
    overlay.delete(key(1));
    overlay.extend(vec![WriteItem::Put(key(2), 3), WriteItem::Put(key(0), 0)]);

    let writes: Vec<String> = overlay.into_writes().iter().map(|write| format!("{write:?}")).collect();
    let expected = [WriteItem::Put(key(0), 0), WriteItem::Delete(key(1)), WriteItem::Put(key(2), 3)];

    assert_eq!(writes, expected.iter().map(|write| format!("{write:?}")).collect::<Vec<_>>());
}

/// A read of one key sees the writes laid over it, and the row beneath where
/// none was.
#[test]
fn a_read_of_one_key_sees_the_writes_over_it() {
    let store: MemoryStore<RowKey, u8> = holding(MemoryMap::new(), [(1, 1), (2, 2)]);
    let mut rows = block_on(store.begin_read()).unwrap();

    let mut overlay = Overlay::new();
    overlay.put(key(1), 10);
    overlay.delete(key(2));
    overlay.put(key(3), 30);

    let read: Vec<Option<u8>> = (1..=4).map(|n| block_on(overlay.get(&mut rows, &key(n))).unwrap()).collect();
    assert_eq!(read, [Some(10), None, Some(30), None]);
}

/// Rows beneath an overlay that note each read of many they answer, and
/// answer it one row short when told to, as a broken store might.
struct Beneath<R> {
    rows: R,
    asked: Vec<Vec<RowKey>>,
    one_short: bool,
}

impl<R: ReadTrx<RowKey, u8>> ReadTrx<RowKey, u8> for Beneath<R> {
    async fn get(&mut self, key: &RowKey) -> Result<Option<u8>, StoreError> { self.rows.get(key).await }

    async fn get_many(&mut self, keys: &[RowKey]) -> Result<Vec<Option<u8>>, StoreError> {
        self.asked.push(keys.to_vec());

        let mut found = self.rows.get_many(keys).await?;
        if self.one_short {
            found.pop();
        }

        Ok(found)
    }

    async fn range(
        &mut self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, u8)>, StoreError> {
        self.rows.range(start, end, direction, limit).await
    }
}

/// A read of several keys sees the writes laid over them, and asks the rows
/// beneath for the others alone, all in one read.
#[test]
fn a_read_of_several_keys_sees_the_writes_and_asks_beneath_for_the_rest_at_once() {
    let store: MemoryStore<RowKey, u8> = holding(MemoryMap::new(), [(1, 1), (2, 2), (4, 4)]);
    let mut rows = Beneath { rows: block_on(store.begin_read()).unwrap(), asked: Vec::new(), one_short: false };

    let mut overlay = Overlay::new();
    overlay.put(key(1), 10);
    overlay.delete(key(2));

    let read = block_on(overlay.get_many(&mut rows, &[key(1), key(2), key(3), key(4)])).unwrap();
    assert_eq!(read, [Some(10), None, None, Some(4)]);
    assert_eq!(rows.asked, [vec![key(3), key(4)]]);

    block_on(overlay.get_many(&mut rows, &[key(2), key(1)])).unwrap();
    assert_eq!(rows.asked.len(), 1, "keys the writes all hold ask nothing beneath");
}

/// Rows beneath that answer a read of several keys with another number of
/// rows can't be paired with the keys, so they are a permanent store failure.
#[test]
fn another_number_of_rows_beneath_than_keys_fails_the_read() {
    let store: MemoryStore<RowKey, u8> = holding(MemoryMap::new(), [(1, 1), (2, 2)]);
    let mut rows = Beneath { rows: block_on(store.begin_read()).unwrap(), asked: Vec::new(), one_short: true };

    let failure = block_on(Overlay::new().get_many(&mut rows, &[key(1), key(2)])).unwrap_err();
    assert!(!failure.is_retryable());
}
