//! Reads that race commits on a store with no other owner: each read sees
//! the tree before a commit or after it, never between, and so does each
//! batch of a range read.

mod common;

use std::ops::Bound;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{
    leaf_address, DigestTree, Locked, MemoryMap, OrderedMap, Ristretto255, Row, RowKey, ScanDirection, Span, StoreError, WriteItem,
};
use common::{asc, item, leaf_at, poll_once, tree_of, Id, ItemTree, Property, Tag};
use futures::StreamExt;

/// The rows of a tree of items.
type Rows = Row<Property, Tag, Ristretto255>;

/// Gates a test holds to keep a gated map's batches, or its next range read
/// once the test asks for it, waiting as long as the test likes, while every
/// other read passes. Clones share the gates.
#[derive(Clone, Default)]
struct Gates {
    /// Set to make the next range read wait on `reads`.
    hold_next_range: Arc<AtomicBool>,
    reads: Arc<tokio::sync::RwLock<()>>,
    writes: Arc<tokio::sync::RwLock<()>>,
}

/// A memory map whose batches, and whose next range read when asked, wait on
/// its gates.
struct Gated {
    rows: MemoryMap<RowKey, Rows>,
    gates: Gates,
}

impl OrderedMap for Gated {
    type Key = RowKey;
    type Value = Rows;

    async fn get(&self, key: &RowKey) -> Result<Option<Rows>, StoreError> { self.rows.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, Rows)>, StoreError> {
        let _open = match self.gates.hold_next_range.swap(false, Ordering::SeqCst) {
            true => Some(self.gates.reads.read().await),
            false => None,
        };

        self.rows.range(start, end, direction, limit).await
    }

    async fn apply(&self, batch: Vec<WriteItem<RowKey, Rows>>) -> Result<(), StoreError> {
        let _open = self.gates.writes.read().await;
        self.rows.apply(batch).await
    }
}

/// An empty store over a gated map, and its gates, none of them held.
fn gated() -> (Locked<Gated>, Gates) {
    let gates = Gates::default();
    (Locked::over(Gated { rows: MemoryMap::new(), gates: gates.clone() }), gates)
}

/// Each read holds a read of the store of its own, which a commit's batch
/// waits out, so it sees the tree as it was before a commit or after it,
/// never between: a read waits out a batch being applied, and a commit's
/// batch waits out a read under way.
#[tokio::test]
async fn a_read_racing_a_commit_sees_the_tree_before_or_after_it() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let (store, gated) = gated();
    let tree = DigestTree::<Property, Id, Tag, Ristretto255, _>::create(store, spec.clone(), (0..100).map(|n| item!(n, n as i64, b"")))
        .await
        .unwrap();

    let view = tree.view();
    let digest = || async { view.span_digest(&Span::all()).await.unwrap().digest };

    let with_100: ItemTree = tree_of!(spec, (0..101).map(|n| item!(n, n as i64, b"")));
    let after_first = with_100.view().span_digest(&Span::all()).await.unwrap().digest;

    // The commit's batch waits on the gate as the map applies it.
    let writes_closed = gated.writes.write().await;
    let mut trx = tree.transaction();
    trx.insert(item!(100, 100, b"")).unwrap();

    let mut committing = pin!(trx.commit());
    assert!(poll_once(committing.as_mut()).is_pending());

    let mut reading = pin!(digest());
    assert!(poll_once(reading.as_mut()).is_pending(), "the read waits for the commit");

    drop(writes_closed);
    committing.await.unwrap();
    assert_eq!(reading.await, after_first, "the read sees the tree after the commit");

    // The read holds its read of the store while its batch waits on the map.
    // The commit removes member 50, but only the read's own map read is
    // held, so the commit can wait on nothing but the read.
    let reads_closed = gated.reads.write().await;
    gated.hold_next_range.store(true, Ordering::SeqCst);

    let mut reading = pin!(leaf_at(&view, [50i64], &Id(50)));
    assert!(poll_once(reading.as_mut()).is_pending());

    let mut trx = tree.transaction();
    trx.remove(item!(50, 50, b"")).unwrap();
    trx.remove(item!(100, 100, b"")).unwrap();

    let mut committing = pin!(trx.commit());
    assert!(poll_once(committing.as_mut()).is_pending(), "the commit waits for the read");

    drop(reads_closed);
    assert!(reading.await.is_some(), "the read sees the tree before the commit");

    committing.await.unwrap();
    assert_eq!(digest().await.count(), 99);
    assert!(leaf_at(&view, [50i64], &Id(50)).await.is_none(), "the commit removed member 50");
}

/// A range read holds a read of the store only while it reads a batch, so a
/// commit completes at once while the read is half consumed. The read then
/// yields every member the commit left in place, once and in order.
#[tokio::test]
async fn a_commit_completes_while_a_range_read_is_half_consumed() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..300).map(|n| item!(n, n as i64, b"")));
    let mut scan = tree.view().range(..).unwrap();

    // Ten members yielded, of the 64 the first batch read before reading one
    // past them.
    let mut read = Vec::new();
    for _ in 0..10 {
        read.push(scan.next().await.unwrap().unwrap());
    }

    // Members removed behind the scan, within its first batch, at the one it
    // read past that batch, and ahead of it; one moved from behind the scan
    // to ahead of it, one moved the other way, and one inserted ahead.
    let mut trx = tree.transaction();
    for n in [5, 30, 64, 200] {
        trx.remove(item!(n, n as i64, b"")).unwrap();
    }

    trx.update(item!(3, 3, b""), item!(3, 250, b"")).unwrap();
    trx.update(item!(250, 250, b""), item!(250, -1, b"")).unwrap();
    trx.insert(item!(1000, 100, b"")).unwrap();

    let mut committing = pin!(trx.commit());
    match poll_once(committing.as_mut()) {
        Poll::Ready(committed) => committed.unwrap(),
        Poll::Pending => panic!("the commit waited: the scan held a read between batches"),
    }

    while let Some(member) = scan.next().await {
        read.push(member.unwrap());
    }

    let addresses: Vec<Vec<u8>> = read.iter().map(|leaf| leaf_address(&leaf.key, &leaf.id)).collect();
    assert!(addresses.windows(2).all(|pair| pair[0] < pair[1]), "in address order, so no address twice");

    let changed = [3, 5, 30, 64, 200, 250, 1000];
    let kept: Vec<u64> = read.iter().map(|leaf| leaf.id.0).filter(|id| !changed.contains(id)).collect();

    assert_eq!(kept, (0..300).filter(|n| !changed.contains(n)).collect::<Vec<_>>(), "every member left in place, once and in order");
}

/// A range read's batch under way keeps its read of the store open while the
/// map holds its read, even once its caller gives up on `next()`, so a commit
/// to the tree waits; dropping the stream drops the batch, and the commit
/// goes ahead.
#[tokio::test]
async fn a_batch_under_way_holds_off_commits_until_its_stream_is_dropped() {
    let (store, gated) = gated();
    let tree = DigestTree::<Property, Id, Tag, Ristretto255, _>::create(
        store,
        KeySpec::new(vec![asc!(Property::A, ValueType::I64)]),
        (0..100).map(|n| item!(n, n as i64, b"")),
    )
    .await
    .unwrap();

    // The first batch's read waits on the map; the caller's `next()` is
    // dropped while it waits, as a timeout would drop it.
    let reads_closed = gated.reads.write().await;
    gated.hold_next_range.store(true, Ordering::SeqCst);

    let mut scan = tree.view().range(..).unwrap();
    assert!(poll_once(pin!(scan.next())).is_pending(), "the first batch waits on the map");

    let mut trx = tree.transaction();
    trx.remove(item!(5, 5, b"")).unwrap();

    let mut committing = pin!(trx.commit());
    assert!(poll_once(committing.as_mut()).is_pending(), "the commit waits for the batch under way");

    drop(scan);

    match poll_once(committing.as_mut()) {
        Poll::Ready(committed) => committed.unwrap(),
        Poll::Pending => panic!("the commit still waited once the stream was dropped"),
    }

    drop(reads_closed);
}
