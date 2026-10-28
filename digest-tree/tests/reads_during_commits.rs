//! Reads that race commits: each read sees the tree before a commit or after
//! it, never between, and so does each batch of a range read.

mod common;

use std::ops::Bound;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::test_support::member_key;
use ankurah_digest_tree::{
    leaf_address, DigestTree, MemoryStore, Ristretto255, Row, RowKey, ScanDirection, Span, Store, StoreError, WriteItem,
};
use common::{asc, item, poll_once, tree_of, Id, ItemTree, Property, Tag};
use futures::StreamExt;

/// A store whose writes wait while a test holds one gate, and whose next read
/// of one row, or next read of a range of rows, once the test asks for it,
/// waits while the test holds another: that read keeps the tree's read lock
/// as long as the test likes, while every other read passes.
#[derive(Clone)]
struct Gated {
    rows: MemoryStore<RowKey, Row<Property, Tag, Ristretto255>>,

    /// The row, if any, whose next read waits on `reads` while `hold_next`
    /// is set.
    gated_row: Option<RowKey>,
    hold_next: Arc<AtomicBool>,

    /// Set to make the next range read wait on `reads`.
    hold_next_range: Arc<AtomicBool>,
    reads: Arc<async_lock::RwLock<()>>,
    writes: Arc<async_lock::RwLock<()>>,
}

impl Gated {
    /// A store over no rows, holding none of its reads.
    fn new() -> Self {
        Self {
            rows: MemoryStore::new(),
            gated_row: None,
            hold_next: Arc::default(),
            hold_next_range: Arc::default(),
            reads: Arc::default(),
            writes: Arc::default(),
        }
    }

    /// The same store, whose next read of `row` a test can hold.
    fn gating_row(self, row: RowKey) -> Self { Self { gated_row: Some(row), ..self } }
}

impl Store<RowKey, Row<Property, Tag, Ristretto255>> for Gated {
    async fn get(&self, key: &RowKey) -> Result<Option<Row<Property, Tag, Ristretto255>>, StoreError> {
        let _open = match self.gated_row.as_ref() == Some(key) && self.hold_next.swap(false, Ordering::SeqCst) {
            true => Some(self.reads.read().await),
            false => None,
        };

        self.rows.get(key).await
    }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, Row<Property, Tag, Ristretto255>)>, StoreError> {
        let _open = match self.hold_next_range.swap(false, Ordering::SeqCst) {
            true => Some(self.reads.read().await),
            false => None,
        };

        self.rows.range(start, end, direction, limit).await
    }

    async fn write(&self, items: Vec<WriteItem<RowKey, Row<Property, Tag, Ristretto255>>>) -> Result<(), StoreError> {
        let _open = self.writes.read().await;
        self.rows.write(items).await
    }
}

/// Each read takes the tree's read lock for itself alone, so it sees the
/// tree as it was before a commit or after it, never between: a read waits
/// out a commit that is writing, and a commit waits out a read under way.
#[tokio::test]
async fn a_read_racing_a_commit_sees_the_tree_before_or_after_it() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let fifty = item!(50, 50, b"");
    let gated = Gated::new().gating_row(RowKey::Leaf(leaf_address(&member_key(&spec, &fifty).unwrap(), &fifty.id)));

    let tree =
        DigestTree::<Property, Id, Tag, Ristretto255, Gated>::build(gated.clone(), spec.clone(), (0..100).map(|n| item!(n, n as i64, b"")))
            .await
            .unwrap();

    let view = tree.view();
    let digest = || async { view.span_digest(&Span::all()).await.unwrap().digest };

    let with_100: ItemTree = tree_of!(spec, (0..101).map(|n| item!(n, n as i64, b"")));
    let after_first = with_100.view().span_digest(&Span::all()).await.unwrap().digest;

    // The commit holds the tree's write lock while its write waits.
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

    // The read keeps the tree's read lock while its read of member 50's leaf
    // waits. The commit removes member 50, but only the read's own read of
    // that leaf is held, so the commit can wait on nothing but the lock.
    let reads_closed = gated.reads.write().await;
    gated.hold_next.store(true, Ordering::SeqCst);

    let mut reading = pin!(view.member([50i64], &Id(50)));
    assert!(poll_once(reading.as_mut()).is_pending());

    let mut trx = tree.transaction();
    trx.remove(item!(50, 50, b"")).unwrap();
    trx.remove(item!(100, 100, b"")).unwrap();

    let mut committing = pin!(trx.commit());
    assert!(poll_once(committing.as_mut()).is_pending(), "the commit waits for the read");

    drop(reads_closed);
    assert!(reading.await.unwrap().is_some(), "the read sees the tree before the commit");

    committing.await.unwrap();
    assert_eq!(digest().await.count(), 99);
    assert!(view.member([50i64], &Id(50)).await.unwrap().is_none(), "the commit removed member 50");
}

/// A range read holds the tree's lock only while it reads a batch, so a
/// commit completes at once while the read is half consumed. The read then
/// yields every member the commit left in place, once and in order.
#[tokio::test]
async fn a_commit_completes_while_a_range_read_is_half_consumed() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..300).map(|n| item!(n, n as i64, b"")));
    let mut scan = tree.view().range(..);

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
        Poll::Pending => panic!("the commit waited: the scan held the tree's lock between batches"),
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

/// A range read's batch under way keeps the tree's read lock while the store
/// holds its read, even once its caller gives up on `next()`, so a commit to
/// the tree waits; dropping the stream drops the batch, and the commit goes
/// ahead.
#[tokio::test]
async fn a_batch_under_way_keeps_the_lock_until_its_stream_is_dropped() {
    let gated = Gated::new();
    let tree = DigestTree::<Property, Id, Tag, Ristretto255, Gated>::build(
        gated.clone(),
        KeySpec::new(vec![asc!(Property::A, ValueType::I64)]),
        (0..100).map(|n| item!(n, n as i64, b"")),
    )
    .await
    .unwrap();

    // The first batch's read waits on the store; the caller's `next()` is
    // dropped while it waits, as a timeout would drop it.
    let reads_closed = gated.reads.write().await;
    gated.hold_next_range.store(true, Ordering::SeqCst);

    let mut scan = tree.view().range(..);
    assert!(poll_once(pin!(scan.next())).is_pending(), "the first batch waits on the store");

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
