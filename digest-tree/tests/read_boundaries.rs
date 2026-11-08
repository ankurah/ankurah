//! Where a tree's store calls go: each read of a tree through a read of the
//! store of its own, a range read through one per batch, and a commit through
//! the transaction its owner lends it, its checked leaves in one read.

mod common;

use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{
    DigestTree, LockedRead, LockedTrx, MemoryMap, MemoryStore, ReadTrx, Ristretto255, Row, RowKey, ScanDirection, Span, Store, StoreError,
    StoreTrx, WriteItem,
};
use common::{asc, item, Id, Property, Tag};
use futures::TryStreamExt;

/// The rows of a tree of items.
type Rows = Row<Property, Tag, Ristretto255>;

/// Which read or transaction a store call went through, numbered in the order
/// they were begun.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Handle {
    Read(usize),
    Trx(usize),
}

/// A store over a memory store that records which read or transaction each of
/// its calls went through, the keys of each read of many, and the leaves
/// read one at a time. Clones share the rows and the record.
#[derive(Clone)]
struct Recording {
    rows: MemoryStore<RowKey, Rows>,
    begun: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<Handle>>>,
    many_read: Arc<Mutex<Vec<Vec<RowKey>>>>,
    leaves_read_alone: Arc<Mutex<Vec<RowKey>>>,
}

impl Recording {
    fn new() -> Self {
        Self {
            rows: MemoryStore::new(),
            begun: Arc::default(),
            calls: Arc::default(),
            many_read: Arc::default(),
            leaves_read_alone: Arc::default(),
        }
    }

    /// The handles the calls since the last take went through, each once, in
    /// the order they were first used.
    fn take_handles(&self) -> Vec<Handle> {
        let mut handles = Vec::new();
        for call in std::mem::take(&mut *self.calls.lock().unwrap()) {
            if !handles.contains(&call) {
                handles.push(call);
            }
        }

        handles
    }

    /// The next handle of `kind`, numbered after every handle begun before it.
    fn next(&self, kind: fn(usize) -> Handle) -> Handle { kind(self.begun.fetch_add(1, Ordering::SeqCst) + 1) }

    /// The owner's transaction `trx` on the memory store beneath, lent to a
    /// tree as this store's.
    fn lend<'a>(&self, trx: &'a mut LockedTrx<MemoryMap<RowKey, Rows>>) -> Recorded<&'a mut LockedTrx<MemoryMap<RowKey, Rows>>> {
        Recorded { recording: self.clone(), handle: self.next(Handle::Trx), rows: trx }
    }
}

/// A read or a transaction on a recording store, whose every call the store
/// records under its handle.
struct Recorded<T> {
    recording: Recording,
    handle: Handle,
    rows: T,
}

impl<T> Recorded<T> {
    fn record(&self) { self.recording.calls.lock().unwrap().push(self.handle) }
}

impl<T: ReadTrx<RowKey, Rows>> ReadTrx<RowKey, Rows> for Recorded<T> {
    async fn get(&mut self, key: &RowKey) -> Result<Option<Rows>, StoreError> {
        self.record();

        if matches!(key, RowKey::Leaf(_)) {
            self.recording.leaves_read_alone.lock().unwrap().push(key.clone());
        }

        self.rows.get(key).await
    }

    async fn get_many(&mut self, keys: &[RowKey]) -> Result<Vec<Option<Rows>>, StoreError> {
        self.record();
        self.recording.many_read.lock().unwrap().push(keys.to_vec());

        self.rows.get_many(keys).await
    }

    async fn range(
        &mut self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, Rows)>, StoreError> {
        self.record();
        self.rows.range(start, end, direction, limit).await
    }
}

impl<T: StoreTrx<RowKey, Rows>> StoreTrx<RowKey, Rows> for Recorded<T> {
    type Pending = T::Pending;

    async fn write(&mut self, batch: Vec<WriteItem<RowKey, Rows>>) -> Result<T::Pending, StoreError> {
        self.record();
        self.rows.write(batch).await
    }
}

impl Store<RowKey, Rows> for Recording {
    type Read = Recorded<LockedRead<MemoryMap<RowKey, Rows>>>;
    type Trx<'a> = Recorded<&'a mut LockedTrx<MemoryMap<RowKey, Rows>>>;
    type Pending = ();

    async fn begin_read(&self) -> Result<Self::Read, StoreError> {
        Ok(Recorded { recording: self.clone(), handle: self.next(Handle::Read), rows: self.rows.begin_read().await? })
    }

    fn owns(&self, trx: &Self::Trx<'_>) -> bool { self.rows.owns(&trx.rows) }

    fn landed(&self, pending: ()) { self.rows.landed(pending) }
}

type RecordedTree = DigestTree<Property, Id, Tag, Ristretto255, Recording>;

/// A tree of 200 members, three buckets' worth, over a recording store whose
/// record starts empty.
async fn recorded_tree() -> (RecordedTree, Recording) {
    let store = Recording::new();
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);

    let mut owner_trx = store.rows.begin().await;
    let members = (0..200).map(|n| item!(n, n as i64, b""));
    let created = RecordedTree::create_in(store.clone(), store.lend(&mut owner_trx), spec, members).await.unwrap();
    owner_trx.commit().await.unwrap();

    let tree = created.landed();
    store.take_handles();
    store.begun.store(0, Ordering::SeqCst);

    (tree, store)
}

/// Each read of a view, and an audit, begins a read of its own and makes
/// every store call through it.
#[tokio::test]
async fn each_read_runs_through_one_read_of_its_own() {
    let (tree, store) = recorded_tree().await;
    let view = tree.view();

    view.count(..).await.unwrap();
    assert_eq!(store.take_handles(), [Handle::Read(1)]);

    view.span_digest(&Span::all()).await.unwrap();
    assert_eq!(store.take_handles(), [Handle::Read(2)]);

    view.pages(vec![Span::all(), Span::all()]).await.unwrap();
    assert_eq!(store.take_handles(), [Handle::Read(3)]);

    tree.audit().await.unwrap();
    assert_eq!(store.take_handles(), [Handle::Read(4)]);
}

/// A range read begins a read of its own for each batch, so that it holds no
/// read open while its caller pauses between batches: here batches of 64
/// members, 128 and the last 8.
#[tokio::test]
async fn a_range_read_begins_a_read_for_each_batch() {
    let (tree, store) = recorded_tree().await;
    let mut scan = tree.view().range(..).unwrap();

    // The first batch's 64 members.
    for _ in 0..64 {
        scan.try_next().await.unwrap();
    }

    assert_eq!(store.take_handles(), [Handle::Read(1)]);

    let rest: Vec<_> = scan.try_collect().await.unwrap();

    assert_eq!(rest.len(), 136);
    assert_eq!(store.take_handles(), [Handle::Read(2), Handle::Read(3)]);
}

/// A commit reads and writes through the transaction its owner lends it, and
/// through nothing else. It names every leaf its checks read, each once and
/// in key order: the insert's slot, the updated member's leaf, which its new
/// version keeps, and the removed member's. It reads them in one read, and
/// none on its own.
#[tokio::test]
async fn a_commit_reads_every_leaf_its_checks_name_in_one_read() {
    let (tree, store) = recorded_tree().await;

    let mut trx = tree.transaction();
    trx.insert(item!(500, 500, b"")).unwrap();
    trx.update(item!(7, 7, b""), item!(7, 7, b"retitled")).unwrap();
    trx.remove(item!(3, 3, b"")).unwrap();
    let prepared = trx.prepare().await;

    let named = prepared.leaves_checked().to_vec();
    assert_eq!(named.len(), 3, "the insert's slot, the updated leaf and the removed leaf");
    assert!(named.windows(2).all(|pair| pair[0] < pair[1]), "in key order");

    let mut owner_trx = store.rows.begin().await;
    let applied = prepared.apply(store.lend(&mut owner_trx)).await.unwrap();
    owner_trx.commit().await.unwrap();
    applied.landed();

    assert_eq!(store.take_handles(), [Handle::Trx(1)]);
    assert_eq!(*store.many_read.lock().unwrap(), [named]);
    assert_eq!(*store.leaves_read_alone.lock().unwrap(), []);
}
