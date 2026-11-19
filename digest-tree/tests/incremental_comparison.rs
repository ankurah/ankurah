//! Bounded comparison, including a peer missing an entire large tree,
//! cancellation, and the distinction between address observations and a
//! globally paired result.

mod common;

use std::future::{pending, poll_fn};
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

use ankurah_core_types::{key_spec::KeySpec, ValueType};
use ankurah_digest_tree::{
    ComparisonError, Digest, DigestGroup, DigestTree, Error, Locked, LtHash16, MemoryMap, OrderedMap, Page, PageGetter, Ristretto255, Row,
    RowKey, ScanDirection, Span, SpanDigest, StoreError, WriteItem,
};
use common::{asc, counted, failing_store, item, no_items, poll_once, tree_of, FailingTree, Id, Item, ItemTree, ItemView, Property, Tag};

fn keyspec() -> KeySpec<Property> { KeySpec::new(vec![asc!(Property::A, ValueType::I64)]) }
fn size(n: usize) -> NonZeroUsize { NonZeroUsize::new(n).unwrap() }
fn items(n: u64) -> impl Iterator<Item = Item> { (0..n).map(|n| item!(n, n as i64, b"old")) }

struct Recording<G: DigestGroup = Ristretto255> {
    view: ItemView<G>,
    requests: Vec<Vec<Span>>,
}

impl<G: DigestGroup> PageGetter<Id, Tag, G> for Recording<G> {
    type Error = Error;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Tag, G>>, Error> {
        self.requests.push(spans.clone());
        self.view.pages(spans).await
    }
}

#[tokio::test]
async fn an_empty_peer_does_not_materialize_all_our_members_before_yielding() {
    let (tree, counts) = counted(keyspec(), items(4096)).await;
    let empty: ItemTree = tree_of!(keyspec(), no_items!());
    let mut getter = Recording { view: empty.view(), requests: Vec::new() };
    let before = counts.rows_read();
    let mut comparison = tree.view().compare(&mut getter, size(7));
    assert_eq!(counts.rows_read(), before, "constructing a comparison reads nothing");

    let first = comparison.next_batch().await.unwrap().unwrap();
    assert_eq!(first.len(), 7);
    let read = counts.rows_read() - before;
    assert!(read < 100, "only a bounded first range, not all 4096 members: {read} rows");

    let mut found: Vec<Id> = first.into_iter().map(|difference| difference.ours.unwrap().id).collect();
    while let Some(batch) = comparison.next_batch().await.unwrap() {
        assert!(batch.len() <= 7);
        for difference in batch {
            assert!(difference.theirs.is_none());
            found.push(difference.ours.unwrap().id);
        }
    }
    assert_eq!(found, (0..4096).map(Id).collect::<Vec<_>>());
    assert!(comparison.next_batch().await.unwrap().is_none());
    drop(comparison);
    assert_eq!(getter.requests, [vec![Span::all()]], "no more remote information is needed");
}

#[tokio::test]
async fn zero_remote_digests_yield_large_local_spans_without_further_exchanges() {
    struct EmptySubdivision(Vec<SpanDigest<Ristretto255>>, usize);
    impl PageGetter<Id, Tag, Ristretto255> for EmptySubdivision {
        type Error = Error;
        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Tag, Ristretto255>>, Error> {
            self.1 += 1;
            assert_eq!(spans, [Span::all()]);
            Ok(vec![Page::Digests(self.0.clone())])
        }
    }

    let (tree, counts) = counted(keyspec(), items(4096)).await;
    let Bound::Included(cut) = tree.span_for(2048i64..).unwrap().start else { panic!("an inclusive lower bound") };
    let spans = [Span::new(Bound::Unbounded, Bound::Excluded(cut.clone())), Span::new(Bound::Included(cut), Bound::Unbounded)];
    let mut getter = EmptySubdivision(spans.into_iter().map(|bounds| SpanDigest { bounds, digest: Digest::empty() }).collect(), 0);
    let before = counts.rows_read();
    let mut comparison = tree.view().compare(&mut getter, size(7));
    let first = comparison.next_batch().await.unwrap().unwrap();
    assert_eq!(first.len(), 7);
    assert!(counts.rows_read() - before < 150, "digests do not trigger a full local scan");
    let mut ids: Vec<_> = first.into_iter().map(|d| d.ours.unwrap().id).collect();
    while let Some(batch) = comparison.next_batch().await.unwrap() {
        ids.extend(batch.into_iter().map(|d| d.ours.unwrap().id));
    }
    assert_eq!(ids, (0..4096).map(Id).collect::<Vec<_>>());
    drop(comparison);
    assert_eq!(getter.1, 1);
}

#[tokio::test]
async fn remote_descent_batches_pages_without_gathering_the_whole_level() {
    let empty: ItemTree<LtHash16> = tree_of!(keyspec(), no_items!());
    let theirs: ItemTree<LtHash16> = tree_of!(keyspec(), items(4096));
    let mut getter = Recording { view: theirs.view(), requests: Vec::new() };
    let mut comparison = empty.view().compare(&mut getter, size(8));
    let mut ids = Vec::new();
    while let Some(batch) = comparison.next_batch().await.unwrap() {
        assert!(batch.len() <= 8);
        ids.extend(batch.into_iter().map(|difference| {
            assert!(difference.ours.is_none());
            difference.theirs.unwrap().id
        }));
    }
    drop(comparison);
    ids.sort();
    assert_eq!(ids, (0..4096).map(Id).collect::<Vec<_>>());
    assert!(getter.requests.iter().all(|spans| spans.len() <= 8));
    assert!(getter.requests.iter().any(|spans| spans.len() > 1));
    assert!(getter.requests.len() < 40, "batches of pages, not one request per leaf: {}", getter.requests.len());
}

#[tokio::test]
async fn a_move_is_two_address_observations_but_one_collected_difference() {
    let ours: ItemTree = tree_of!(keyspec(), items(300));
    let theirs: ItemTree = tree_of!(keyspec(), items(300));
    let mut trx = theirs.transaction();
    trx.update(item!(20, 20, b"old"), item!(20, 400, b"new")).unwrap();
    trx.commit().await.unwrap();

    let mut comparison = ours.view().compare(theirs.view(), size(1));
    let mut observations = Vec::new();
    while let Some(batch) = comparison.next_batch().await.unwrap() {
        observations.extend(batch);
    }
    assert_eq!(observations.len(), 2);
    assert!(observations.iter().any(|d| d.ours.as_ref().is_some_and(|leaf| leaf.id == Id(20)) && d.theirs.is_none()));
    assert!(observations.iter().any(|d| d.theirs.as_ref().is_some_and(|leaf| leaf.id == Id(20)) && d.ours.is_none()));

    let paired = ours.view().differences(theirs.view()).await.unwrap();
    assert_eq!(paired.len(), 1);
    assert_eq!(paired[0].ours.as_ref().unwrap().id, Id(20));
    assert_eq!(paired[0].theirs.as_ref().unwrap().id, Id(20));
}

#[tokio::test]
async fn a_window_reports_only_its_differences_and_equal_trees_finish_empty() {
    let ours: ItemTree = tree_of!(keyspec(), items(300));
    let theirs: ItemTree = tree_of!(keyspec(), items(300));
    assert!(ours.view().compare(theirs.view(), size(7)).next_batch().await.unwrap().is_none());
    let mut trx = theirs.transaction();
    for n in [20, 200] {
        trx.update(item!(n, n as i64, b"old"), item!(n, n as i64, b"new")).unwrap();
    }
    trx.commit().await.unwrap();
    let mut comparison = ours.select(..100i64).unwrap().compare(theirs.view(), size(7));
    let batch = comparison.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].ours.as_ref().unwrap().id, Id(20));
    assert_eq!(batch[0].theirs.as_ref().unwrap().payload, Tag(b"new".to_vec()));
    assert!(comparison.next_batch().await.unwrap().is_none());
}

struct PauseOnce {
    view: ItemView,
    requests: Vec<Vec<Span>>,
    pause: bool,
}

impl PageGetter<Id, Tag, Ristretto255> for PauseOnce {
    type Error = Error;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Tag, Ristretto255>>, Error> {
        self.requests.push(spans.clone());
        if std::mem::take(&mut self.pause) {
            pending::<()>().await;
        }
        self.view.pages(spans).await
    }
}

#[tokio::test]
async fn canceling_a_getter_call_retries_its_spans_without_losing_work() {
    let ours: ItemTree = tree_of!(keyspec(), no_items!());
    let theirs: ItemTree = tree_of!(keyspec(), items(10));
    let mut getter = PauseOnce { view: theirs.view(), requests: Vec::new(), pause: true };
    let mut comparison = ours.view().compare(&mut getter, size(20));
    let mut call = Box::pin(comparison.next_batch());
    assert!(poll_once(call.as_mut()).is_pending());
    drop(call);
    let differences = comparison.next_batch().await.unwrap().unwrap();
    assert_eq!(differences.into_iter().map(|d| d.theirs.unwrap().id).collect::<Vec<_>>(), (0..10).map(Id).collect::<Vec<_>>());
    assert!(comparison.next_batch().await.unwrap().is_none());
    drop(comparison);
    assert_eq!(getter.requests, [vec![Span::all()], vec![Span::all()]]);
}

/// Pause a chosen range read, after earlier reads have supplied differences.
/// The waiting guard also reveals whether dropping the cursor releases it.
#[derive(Default)]
struct Gate {
    ranges: AtomicUsize,
    block_at: AtomicUsize,
    waiting: AtomicBool,
}

struct Waiting<'a>(&'a AtomicBool);
impl Drop for Waiting<'_> {
    fn drop(&mut self) { self.0.store(false, Ordering::Relaxed); }
}

struct PausingMap<V> {
    rows: MemoryMap<RowKey, V>,
    gate: Arc<Gate>,
}

impl<V: Clone + Send + Sync + 'static> OrderedMap for PausingMap<V> {
    type Key = RowKey;
    type Value = V;

    async fn get(&self, key: &RowKey) -> Result<Option<V>, StoreError> { self.rows.get(key).await }

    async fn range(
        &self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, V)>, StoreError> {
        let read = self.gate.ranges.fetch_add(1, Ordering::Relaxed) + 1;
        if self.gate.block_at.load(Ordering::Relaxed) == read {
            self.gate.waiting.store(true, Ordering::Relaxed);
            let _waiting = Waiting(&self.gate.waiting);
            poll_fn(|_| if self.gate.block_at.load(Ordering::Relaxed) == read { Poll::Pending } else { Poll::Ready(()) }).await;
        }
        self.rows.range(start, end, direction, limit).await
    }

    async fn apply(&self, batch: Vec<WriteItem<RowKey, V>>) -> Result<(), StoreError> { self.rows.apply(batch).await }
}

type PausingTree = DigestTree<Property, Id, Tag, Ristretto255, Locked<PausingMap<Row<Property, Tag, Ristretto255>>>>;

#[tokio::test]
async fn canceled_local_read_retains_the_partly_filled_output_and_releases_on_drop() {
    let gate = Arc::new(Gate::default());
    let store = Locked::over(PausingMap { rows: MemoryMap::new(), gate: gate.clone() });
    let ours = PausingTree::create(store, keyspec(), items(300)).await.unwrap();
    let theirs: ItemTree = tree_of!(keyspec(), no_items!());
    gate.block_at.store(gate.ranges.load(Ordering::Relaxed) + 2, Ordering::Relaxed);
    let mut comparison = ours.view().compare(theirs.view(), size(200));
    let mut call = Box::pin(comparison.next_batch());
    assert!(poll_once(call.as_mut()).is_pending());
    assert!(gate.waiting.load(Ordering::Relaxed));
    drop(call);
    assert!(gate.waiting.load(Ordering::Relaxed), "the cursor owns the pending local read");
    gate.block_at.store(0, Ordering::Relaxed);
    let first = comparison.next_batch().await.unwrap().unwrap();
    assert_eq!(first.len(), 200);
    let mut trx = ours.transaction();
    trx.update(item!(0, 0, b"old"), item!(0, 0, b"new")).unwrap();
    let mut commit = Box::pin(trx.commit());
    assert!(matches!(poll_once(commit.as_mut()), Poll::Ready(Ok(()))), "a returned batch holds no read lock");
    let second = comparison.next_batch().await.unwrap().unwrap();
    let ids: Vec<_> = first.into_iter().chain(second).map(|d| d.ours.unwrap().id).collect();
    assert_eq!(ids, (0..300).map(Id).collect::<Vec<_>>());
    assert!(comparison.next_batch().await.unwrap().is_none());
    assert!(!gate.waiting.load(Ordering::Relaxed));

    gate.block_at.store(gate.ranges.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    let mut comparison = ours.view().compare(theirs.view(), size(10));
    let mut call = Box::pin(comparison.next_batch());
    assert!(poll_once(call.as_mut()).is_pending());
    drop(call);
    assert!(gate.waiting.load(Ordering::Relaxed));
    drop(comparison);
    assert!(!gate.waiting.load(Ordering::Relaxed));
    // A writer can commit even while the gate remains closed: no read lock
    // leaked from the discarded cursor.
    let mut trx = ours.transaction();
    trx.insert(item!(400, 400, b"new")).unwrap();
    let mut commit = Box::pin(trx.commit());
    assert!(matches!(poll_once(commit.as_mut()), Poll::Ready(Ok(()))));
}

#[derive(Debug, PartialEq, Eq)]
struct Disconnected;

struct Disconnecting {
    view: ItemView,
    disconnected: Arc<AtomicBool>,
}

impl PageGetter<Id, Tag, Ristretto255> for Disconnecting {
    type Error = Disconnected;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Tag, Ristretto255>>, Disconnected> {
        if self.disconnected.load(Ordering::Relaxed) {
            return Err(Disconnected);
        }
        Ok(self.view.pages(spans).await.unwrap())
    }
}

#[tokio::test]
async fn a_later_getter_error_is_preserved_and_ends_the_cursor() {
    let ours: ItemTree = tree_of!(keyspec(), no_items!());
    let theirs: ItemTree = tree_of!(keyspec(), items(300));
    let disconnected = Arc::new(AtomicBool::new(false));
    let getter = Disconnecting { view: theirs.view(), disconnected: disconnected.clone() };
    let mut comparison = ours.view().compare(getter, size(7));
    assert_eq!(comparison.next_batch().await.unwrap().unwrap().len(), 7);
    disconnected.store(true, Ordering::Relaxed);
    loop {
        match comparison.next_batch().await {
            Ok(Some(_)) => {}
            Err(ComparisonError::Getter(Disconnected)) => break,
            other => panic!("expected the getter's error: {other:?}"),
        }
    }
    assert!(comparison.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn a_later_local_read_error_is_preserved_and_ends_the_cursor() {
    let (store, faults) = failing_store();
    let ours = FailingTree::create(store, keyspec(), items(300)).await.unwrap();
    let theirs: ItemTree = tree_of!(keyspec(), no_items!());
    let mut comparison = ours.view().compare(theirs.view(), size(7));
    assert_eq!(comparison.next_batch().await.unwrap().unwrap().len(), 7);
    faults.fail_reads(true);
    loop {
        match comparison.next_batch().await {
            Ok(Some(_)) => {}
            Err(ComparisonError::View(Error::Store(_))) => break,
            other => panic!("expected a local store error: {other:?}"),
        }
    }
    assert!(comparison.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn the_cursor_and_its_next_batch_can_be_sent() {
    fn require_send<T: Send>(_: &T) {}
    let tree: ItemTree = tree_of!(keyspec(), no_items!());
    let mut comparison = tree.view().compare(tree.view(), size(7));
    require_send(&comparison);
    require_send(&comparison.next_batch());
}
