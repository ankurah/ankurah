//! Reads that race commits: each read sees the tree before a commit or after
//! it, never between, and so does each batch of a range read.

mod common;

use std::ops::Bound;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;

use ankurah_digest_tree::{
    leaf_address, DigestTree, MemoryStore, Ristretto255, Row, RowKey, ScanDirection, ScanRange, Span, Store, StoreError, WriteItem,
};
use common::{built, integer_key, integer_spec, item, poll_once, Id, Spec, Tag};
use futures::StreamExt;

/// A store whose writes wait while a test holds one gate, and whose next read
/// of one row, once the test asks for it, waits while the test holds
/// another: that read keeps the tree's read lock as long as the test likes,
/// while every other read passes.
#[derive(Clone)]
struct Gated {
    rows: MemoryStore<RowKey, Row<Spec, Tag, Ristretto255>>,

    /// The row whose next read waits on `reads` while `hold_next` is set.
    gated_row: RowKey,
    hold_next: Arc<AtomicBool>,
    reads: Arc<async_lock::RwLock<()>>,
    writes: Arc<async_lock::RwLock<()>>,
}

impl Store<RowKey, Row<Spec, Tag, Ristretto255>> for Gated {
    async fn get(&self, key: &RowKey) -> Result<Option<Row<Spec, Tag, Ristretto255>>, StoreError> {
        let _open = match *key == self.gated_row && self.hold_next.swap(false, Ordering::SeqCst) {
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
    ) -> Result<Vec<(RowKey, Row<Spec, Tag, Ristretto255>)>, StoreError> {
        self.rows.range(start, end, direction, limit).await
    }

    async fn write(&self, items: Vec<WriteItem<RowKey, Row<Spec, Tag, Ristretto255>>>) -> Result<(), StoreError> {
        let _open = self.writes.read().await;
        self.rows.write(items).await
    }
}

/// Each read takes the tree's read lock for itself alone, so it sees the
/// tree as it was before a commit or after it, never between: a read waits
/// out a commit that is writing, and a commit waits out a read under way.
#[tokio::test]
async fn a_read_racing_a_commit_sees_the_tree_before_or_after_it() {
    let (key, id) = (integer_key(Some(50)), Id(50));
    let gated = Gated {
        rows: MemoryStore::new(),
        gated_row: RowKey::Leaf(leaf_address(&key, &id)),
        hold_next: Arc::default(),
        reads: Arc::default(),
        writes: Arc::default(),
    };

    let tree = DigestTree::<Spec, Id, Tag, Ristretto255, Gated>::build(
        gated.clone(),
        integer_spec(),
        (0..100).map(|n| item(n, Some(n as i64), b"")),
    )
    .await
    .unwrap();

    let view = tree.view();
    let digest = || async { view.span_digest(&Span::all()).await.unwrap().digest };

    let after_first = built::<Ristretto255>(integer_spec(), (0..101).map(|n| item(n, Some(n as i64), b"")))
        .await
        .view()
        .span_digest(&Span::all())
        .await
        .unwrap()
        .digest;

    // The commit holds the tree's write lock while its write waits.
    let writes_closed = gated.writes.write().await;
    let mut trx = tree.transaction();
    trx.insert(item(100, Some(100), b"")).unwrap();

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

    let mut reading = pin!(view.member(&key, &id));
    assert!(poll_once(reading.as_mut()).is_pending());

    let mut trx = tree.transaction();
    trx.remove(item(50, Some(50), b"")).unwrap();
    trx.remove(item(100, Some(100), b"")).unwrap();

    let mut committing = pin!(trx.commit());
    assert!(poll_once(committing.as_mut()).is_pending(), "the commit waits for the read");

    drop(reads_closed);
    assert!(reading.await.unwrap().is_some(), "the read sees the tree before the commit");

    committing.await.unwrap();
    assert_eq!(digest().await.count(), 99);
    assert!(view.member(&key, &id).await.unwrap().is_none(), "the commit removed member 50");
}

/// A range read holds the tree's lock only while it reads a batch, so a
/// commit completes at once while the read is half consumed. The read then
/// yields every member the commit left in place, once and in order.
#[tokio::test]
async fn a_commit_completes_while_a_range_read_is_half_consumed() {
    let tree = built::<Ristretto255>(integer_spec(), (0..300).map(|n| item(n, Some(n as i64), b""))).await;
    let mut scan = tree.view().range(ScanRange::forward(Span::all()));

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
        trx.remove(item(n, Some(n as i64), b"")).unwrap();
    }

    trx.update(item(3, Some(3), b""), item(3, Some(250), b"")).unwrap();
    trx.update(item(250, Some(250), b""), item(250, Some(-1), b"")).unwrap();
    trx.insert(item(1000, Some(100), b"")).unwrap();

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
