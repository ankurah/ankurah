use std::collections::BTreeMap;

use super::*;
use crate::fixtures::{block_on, KeySpec, Tag};
use crate::tree::fixtures::ShortPages;
use crate::{MemoryStore, Ristretto255};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

type TestRow = Row<KeySpec, Tag, Ristretto255>;

/// A metadata row holding `n`, so rows tell apart: staging never looks at a
/// row's kind.
fn row(n: u8) -> TestRow { Row::Meta(Meta { key_spec: KeySpec::new(vec![]), group: n }) }

fn key(n: u8) -> RowKey { RowKey::Leaf(vec![n]) }

fn holds(rows: &[(RowKey, TestRow)]) -> Vec<(RowKey, Vec<u8>)> {
    rows.iter()
        .map(|(key, row)| match row {
            Row::Meta(meta) => (key.clone(), vec![meta.group]),
            other => panic!("a metadata row, not {other:?}"),
        })
        .collect()
}

fn bound(kind: u8, n: u8) -> Bound<RowKey> {
    match kind {
        0 => Bound::Unbounded,
        1 => Bound::Included(key(n)),
        _ => Bound::Excluded(key(n)),
    }
}

/// Read a range through rows over `store`, with `changes` pending, and check
/// it against a model that applies the changes to a copy of `stored`.
fn range_matches_the_model<S: Store<RowKey, TestRow>>(
    store: S,
    stored: &BTreeMap<u8, u8>,
    changes: &[(u8, Option<u8>)],
    (start, end): (Bound<RowKey>, Bound<RowKey>),
    direction: ScanDirection,
    limit: usize,
) -> Result<(), TestCaseError> {
    block_on(store.write(stored.iter().map(|(&n, &held)| WriteItem::Put(key(n), row(held))).collect())).unwrap();
    let mut rows = StagedRows::new(&store);
    let mut model = stored.clone();

    for (n, change) in changes {
        match change {
            Some(held) => {
                rows.put(key(*n), row(*held));
                model.insert(*n, *held);
            }

            None => {
                rows.delete(key(*n));
                model.remove(n);
            }
        }
    }

    let read = block_on(rows.range(start.as_ref(), end.as_ref(), direction, limit)).unwrap();
    let bounds = (start.as_ref().map(byte_of), end.as_ref().map(byte_of));

    let mut expected: Vec<(RowKey, Vec<u8>)> =
        model.iter().filter(|(n, _)| bounds.contains(*n)).map(|(&n, &held)| (key(n), vec![held])).collect();

    if direction == ScanDirection::Reverse {
        expected.reverse();
    }

    expected.truncate(limit);
    prop_assert_eq!(holds(&read), expected);
    Ok(())
}

fn byte_of(key: &RowKey) -> u8 {
    match key {
        RowKey::Leaf(n) => n[0],
        other => panic!("a leaf key, not {other:?}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, rng_seed: RngSeed::Fixed(0x2085_0001), ..ProptestConfig::default() })]

    /// A range read sees the store's rows under the pending changes, in
    /// either direction and up to any limit, as a model that applies the
    /// changes to a copy of the rows does, however the store's pages fall
    /// against the changes, and however short a store keeps its pages.
    #[test]
    fn a_range_read_merges_pending_changes_into_the_stored_rows(
        stored in proptest::collection::btree_map(0..40u8, any::<u8>(), 0..30),
        changes in proptest::collection::vec((0..40u8, proptest::option::of(any::<u8>())), 0..30),
        (start, end) in ((0..3u8, 0..40u8), (0..3u8, 0..40u8)),
        reverse in any::<bool>(),
        limit in 0..12usize,
    ) {
        let bounds = (bound(start.0, start.1), bound(end.0, end.1));
        let direction = if reverse { ScanDirection::Reverse } else { ScanDirection::Forward };
        range_matches_the_model(MemoryStore::new(), &stored, &changes, bounds.clone(), direction, limit)?;
        range_matches_the_model(ShortPages(MemoryStore::new()), &stored, &changes, bounds, direction, limit)?;
    }
}

/// A store that returns every row, whatever bounds it is asked for.
#[derive(Clone)]
struct Boundless(MemoryStore<RowKey, TestRow>);

impl Store<RowKey, TestRow> for Boundless {
    async fn get(&self, key: &RowKey) -> Result<Option<TestRow>, StoreError> { self.0.get(key).await }

    async fn range(
        &self,
        _: Bound<&RowKey>,
        _: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, TestRow)>, StoreError> {
        self.0.range(Bound::Unbounded, Bound::Unbounded, direction, limit).await
    }

    async fn write(&self, items: Vec<WriteItem<RowKey, TestRow>>) -> Result<(), StoreError> { self.0.write(items).await }
}

/// Rows outside the bounds a store was asked for would send the paging that
/// follows them round in circles, so they are a permanent store failure.
#[test]
fn a_store_that_returns_rows_outside_the_range_fails_the_read() {
    let store = Boundless(MemoryStore::new());
    block_on(store.write((0..5).map(|n| WriteItem::Put(key(n), row(n))).collect())).unwrap();
    let rows = StagedRows::new(&store);

    match block_on(rows.range(Bound::Included(&key(2)), Bound::Unbounded, ScanDirection::Forward, 10)) {
        Err(Error::Store(failure)) => assert!(!failure.is_retryable()),
        other => panic!("expected a store failure, got {other:?}"),
    }
}

/// Every change, as one batch, in key order.
#[test]
fn pending_changes_become_one_batch() {
    let store = MemoryStore::<RowKey, TestRow>::new();
    let mut rows = StagedRows::new(&store);
    rows.put(key(2), row(2));
    rows.delete(key(1));
    rows.put(key(2), row(3));

    let writes: Vec<String> = rows.into_writes().iter().map(|write| format!("{write:?}")).collect();
    assert_eq!(writes, [format!("{:?}", WriteItem::<RowKey, TestRow>::Delete(key(1))), format!("{:?}", WriteItem::Put(key(2), row(3)))]);
}

/// A row of another kind than its key names is damage, which reads report.
#[test]
fn a_row_of_another_kind_than_its_key_is_inconsistent() {
    let store = MemoryStore::<RowKey, TestRow>::new();
    block_on(store.write(vec![WriteItem::Put(RowKey::Node(NodePrefix::root()), row(1))])).unwrap();

    let rows = StagedRows::new(&store);
    assert!(matches!(block_on(rows.node(&NodePrefix::root())), Err(Error::Inconsistent(_))));
    assert!(matches!(block_on(crate::tree::trie::Trie::new(&rows).root()), Err(Error::Inconsistent(_))));
}
