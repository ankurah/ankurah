//! A fake outer transaction with canonical records and two trees. All records
//! share one MemoryTransaction; committing separate MemoryMaps would not test
//! atomicity. Its topology fence is deliberately global, as in the memory
//! reference adapter, rather than a proposed database locking strategy.
use std::borrow::BorrowMut;
use std::ops::Bound::{Excluded, Included, Unbounded};

use ankql::ast::Predicate;
use ankurah_core_types::{EntityId, KeySpec};
use ankurah_digest_tree::{
    ByteBounds, HeadHash, LeafChange, LeafInput, MapAccess, MapName, MapTransaction, MemoryMaps, MemoryTransaction, NodePrefix,
    Ristretto255, TreeError, TreeSpan, TreeSpec, TreeTransaction, BUCKET_CAPACITY,
};
use async_trait::async_trait;

const FIRST: u8 = 1;
const SECOND: u8 = 2;

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Default)]
struct Store(MemoryMaps);
struct Transaction {
    maps: MemoryTransaction,
    fault: Option<Fault>,
}
#[derive(Clone, Copy, Debug)]
enum Fault {
    Error,
    Suspend,
}

impl Store {
    fn begin(&self) -> Transaction { Transaction { maps: self.0.begin(), fault: None } }

    async fn rows(&self) -> [Rows; 3] {
        let mut tx = self.0.begin();
        let mut rows = [Rows::new(), Rows::new(), Rows::new()];
        for (i, map) in [MapName::Leaves, MapName::Lookup, MapName::Nodes].into_iter().enumerate() {
            rows[i] = tx.scan(map, (Unbounded, Unbounded), usize::MAX).await.unwrap();
        }
        rows
    }
}

impl Transaction {
    /// A view of the tree `id` that borrows this transaction.
    fn tree(&mut self, id: u8, spec: &TreeSpec) -> TreeTransaction<Ristretto255, TreeMaps<&mut Transaction>> {
        assert!(id == FIRST || id == SECOND);
        TreeTransaction::bind(spec, TreeMaps { owner: self, id })
    }

    /// A view of the tree `id` that owns this transaction, and so commits it.
    fn into_tree(self, id: u8, spec: &TreeSpec) -> TreeTransaction<Ristretto255, TreeMaps<Transaction>> {
        assert!(id == FIRST || id == SECOND);
        TreeTransaction::bind(spec, TreeMaps { owner: self, id })
    }

    async fn set_state(&mut self, expected: Option<HeadHash>, leaf: &LeafInput) -> Result<(), TreeError> {
        let key = [b"\0".as_slice(), &leaf.entity_id.to_bytes()].concat();
        self.maps.put_if(MapName::Leaves, &key, expected.as_ref().map(|head| head.as_bytes().as_slice()), leaf.head.as_bytes()).await
    }

    async fn state(&mut self, id: EntityId) -> Option<Vec<u8>> {
        self.maps.get(MapName::Leaves, &[b"\0".as_slice(), &id.to_bytes()].concat()).await.unwrap()
    }

    async fn commit(self) -> Result<(), TreeError> { self.maps.commit().await }

    async fn after_write(&mut self) -> Result<(), TreeError> {
        match self.fault.take() {
            Some(Fault::Error) => Err(TreeError::Audit("injected write failure")),
            Some(Fault::Suspend) => std::future::pending().await,
            None => Ok(()),
        }
    }
}

// Each tree has its own key namespace. An adapter that borrows its owner
// implements only map access: it cannot commit the outer transaction. One
// that owns it commits it.
struct TreeMaps<O> {
    owner: O,
    id: u8,
}

impl<O: BorrowMut<Transaction> + Send> TreeMaps<O> {
    fn key(&self, key: &[u8]) -> Vec<u8> { [&[self.id], key].concat() }

    async fn scan_page(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize, reverse: bool) -> Result<Rows, TreeError> {
        let start = match bounds.0 {
            Unbounded => Included(vec![self.id]),
            bound => bound.map(|key| self.key(key)),
        };
        let end = match bounds.1 {
            Unbounded => Excluded(vec![self.id + 1]),
            bound => bound.map(|key| self.key(key)),
        };
        let bounds = (start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice));
        let maps = &mut self.owner.borrow_mut().maps;
        let rows = if reverse { maps.scan_reverse(map, bounds, limit).await? } else { maps.scan(map, bounds, limit).await? };
        Ok(rows.into_iter().map(|(key, value)| (key[1..].to_vec(), value)).collect())
    }
}

#[async_trait]
impl<O: BorrowMut<Transaction> + Send> MapAccess for TreeMaps<O> {
    fn poison(&mut self) { self.owner.borrow_mut().maps.poison(); }
    fn watch_structure(&mut self) { self.owner.borrow_mut().maps.watch_structure(); }
    fn change_structure(&mut self) { self.owner.borrow_mut().maps.change_structure(); }

    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> {
        let key = self.key(key);
        self.owner.borrow_mut().maps.get(map, &key).await
    }

    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Rows, TreeError> {
        self.scan_page(map, bounds, limit, false).await
    }

    async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Rows, TreeError> {
        self.scan_page(map, bounds, limit, true).await
    }

    async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        let key = self.key(key);
        let owner = self.owner.borrow_mut();
        owner.maps.insert(map, &key, value).await?;
        owner.after_write().await
    }

    async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError> {
        let key = self.key(key);
        let owner = self.owner.borrow_mut();
        owner.maps.put_if(map, &key, expected, value).await?;
        owner.after_write().await
    }

    async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError> {
        let key = self.key(key);
        let owner = self.owner.borrow_mut();
        owner.maps.delete_if(map, &key, expected).await?;
        owner.after_write().await
    }

    async fn append(&mut self, map: MapName, key_prefix: &[u8], value: &[u8]) -> Result<(), TreeError> {
        let key_prefix = self.key(key_prefix);
        let owner = self.owner.borrow_mut();
        owner.maps.append(map, &key_prefix, value).await?;
        owner.after_write().await
    }
}

#[async_trait]
impl MapTransaction for TreeMaps<Transaction> {
    async fn commit(self) -> Result<(), TreeError> { self.owner.commit().await }
}

fn spec() -> TreeSpec { TreeSpec::new(KeySpec::new(vec![]), Predicate::True) }

fn leaf(n: u32, version: u8) -> LeafInput {
    let mut id = [0; 32];
    id[..4].copy_from_slice(&n.to_be_bytes());
    LeafInput { key: vec![], entity_id: EntityId::from_bytes(id), head: HeadHash::of([[version; 32]]) }
}

async fn update(tx: &mut Transaction, spec: &TreeSpec, before: Option<LeafInput>, after: LeafInput) {
    tx.set_state(before.as_ref().map(|leaf| leaf.head), &after).await.unwrap();
    for id in [FIRST, SECOND] {
        tx.tree(id, spec).apply_change(LeafChange { before: before.clone(), after: Some(after.clone()) }).await.unwrap();
    }
}

async fn populated(count: usize) -> (Store, TreeSpec) {
    let (store, spec) = (Store::default(), spec());
    let mut tx = store.begin();
    let leaves: Vec<_> = (0..count as u32).map(|n| leaf(n, 0)).collect();
    for leaf in &leaves {
        tx.set_state(None, leaf).await.unwrap();
    }
    for id in [FIRST, SECOND] {
        tx.tree(id, &spec).build(leaves.clone()).await.unwrap();
    }
    tx.commit().await.unwrap();
    (store, spec)
}

async fn assert_tree(tx: &mut Transaction, id: u8, spec: &TreeSpec, expected: &[LeafInput]) {
    let mut tree = tx.tree(id, spec);
    let rows = tree.leaves(&TreeSpan::all(), usize::MAX).await.unwrap();
    let actual: Vec<_> = rows.into_iter().map(|row| LeafInput { key: row.key, entity_id: row.entity_id, head: row.head }).collect();
    assert_eq!(actual, expected);
    assert_eq!(tree.audit().await.unwrap().leaves, expected.len());
}

#[tokio::test]
async fn views_share_pending_writes_and_commit_with_entity_records() {
    let (store, spec) = populated(1).await;
    let mut observer = store.begin();
    let mut tx = store.begin();
    update(&mut tx, &spec, Some(leaf(0, 0)), leaf(0, 1)).await;
    // Release and reopen both views, interleaving another entity write.
    update(&mut tx, &spec, Some(leaf(0, 1)), leaf(0, 2)).await;
    update(&mut tx, &spec, None, leaf(1, 0)).await;
    for id in [FIRST, SECOND] {
        assert_tree(&mut tx, id, &spec, &[leaf(0, 2), leaf(1, 0)]).await;
        assert_tree(&mut observer, id, &spec, &[leaf(0, 0)]).await;
    }
    assert_eq!(tx.state(leaf(0, 0).entity_id).await, Some(leaf(0, 2).head.as_bytes().to_vec()));
    assert_eq!(store.begin().state(leaf(0, 0).entity_id).await, Some(leaf(0, 0).head.as_bytes().to_vec()));
    // A different membership in the second tree must not change the first.
    tx.tree(SECOND, &spec).apply_change(LeafChange { before: Some(leaf(1, 0)), after: None }).await.unwrap();
    tx.commit().await.unwrap();
    let mut current = store.begin();
    assert_tree(&mut current, FIRST, &spec, &[leaf(0, 2), leaf(1, 0)]).await;
    assert_tree(&mut current, SECOND, &spec, &[leaf(0, 2)]).await;
    assert_eq!(current.state(leaf(1, 0).entity_id).await, Some(leaf(1, 0).head.as_bytes().to_vec()));
    assert_eq!(current.state(leaf(0, 0).entity_id).await, Some(leaf(0, 2).head.as_bytes().to_vec()));
    // A preexisting reader stays on its original snapshot across commit.
    for id in [FIRST, SECOND] {
        assert_tree(&mut observer, id, &spec, &[leaf(0, 0)]).await;
    }
    assert_eq!(observer.state(leaf(0, 0).entity_id).await, Some(leaf(0, 0).head.as_bytes().to_vec()));
}

#[tokio::test]
async fn dropping_the_owner_rolls_back_successful_tree_and_entity_writes() {
    let (store, spec) = populated(1).await;
    let before = store.rows().await;
    let mut tx = store.begin();
    update(&mut tx, &spec, Some(leaf(0, 0)), leaf(0, 1)).await;
    drop(tx);
    assert_eq!(store.rows().await, before);
}

#[tokio::test]
async fn scopes_cannot_cross_reopened_views_or_tree_namespaces() {
    let (store, spec) = populated(1).await;
    let mut tx = store.begin();
    let old = tx.tree(FIRST, &spec).scope_spans([TreeSpan::all()]);
    for id in [FIRST, SECOND] {
        let mut tree = tx.tree(id, &spec);
        assert!(matches!(tree.count(&old).await, Err(TreeError::StaleScope)));
        let scope = tree.scope_spans([TreeSpan::all()]);
        assert_eq!(tree.count(&scope).await.unwrap(), 1);
    }
    tx.commit().await.unwrap();
}

fn cancel_after_yield(future: impl std::future::Future) {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(future.as_mut().poll(&mut context).is_pending());
}

/// Run `operation` on `tree` with its owner's fault armed, so that it fails at
/// its first write or is cancelled there.
async fn fail<T: MapAccess>(tree: &mut TreeTransaction<Ristretto255, T>, operation: &str, fault: Fault) {
    let work = async {
        match operation {
            "change" => tree.apply_change(LeafChange { before: Some(leaf(0, 1)), after: Some(leaf(0, 2)) }).await,
            "build" => tree.build([leaf(0, 2)]).await,
            "compact" => tree.compact_node(&NodePrefix::root()).await.map(|_| ()),
            _ => unreachable!(),
        }
    };
    match fault {
        Fault::Error => assert!(matches!(work.await, Err(TreeError::Audit("injected write failure")))),
        Fault::Suspend => cancel_after_yield(work),
    }
}

#[tokio::test]
async fn failure_and_cancellation_poison_the_owner_whether_the_view_borrows_or_owns_it() {
    for owned in [false, true] {
        for fault in [Fault::Error, Fault::Suspend] {
            for operation in ["change", "build", "compact"] {
                let (store, spec) = populated(1).await;
                let mut setup = store.begin();
                update(&mut setup, &spec, Some(leaf(0, 0)), leaf(0, 1)).await;
                setup.commit().await.unwrap();
                let before = store.rows().await;
                let mut tx = store.begin();
                tx.set_state(Some(leaf(0, 1).head), &leaf(0, 2)).await.unwrap();
                tx.tree(SECOND, &spec).apply_change(LeafChange { before: Some(leaf(0, 1)), after: Some(leaf(0, 2)) }).await.unwrap();
                tx.fault = Some(fault);
                // A successful no-op afterwards cannot clear the poison, nor,
                // for a borrowed view, can opening a new view.
                let committed = if owned {
                    let mut tree = tx.into_tree(FIRST, &spec);
                    fail(&mut tree, operation, fault).await;
                    tree.apply_change(LeafChange { before: None, after: None }).await.unwrap();
                    tree.commit().await
                } else {
                    fail(&mut tx.tree(FIRST, &spec), operation, fault).await;
                    tx.tree(FIRST, &spec).apply_change(LeafChange { before: None, after: None }).await.unwrap();
                    tx.commit().await
                };
                assert!(matches!(committed, Err(TreeError::Conflict)), "owned {owned}, {fault:?}: {operation}");
                assert_eq!(store.rows().await, before, "owned {owned}, {fault:?}: {operation}");
            }
        }
    }
}

#[tokio::test]
async fn semantic_tree_failure_also_poisons_a_borrowed_memory_owner() {
    let maps = MemoryMaps::new();
    let spec = spec();
    let mut owner = maps.begin();
    {
        let mut tree = TreeTransaction::<Ristretto255, _>::bind(&spec, &mut owner);
        tree.build([leaf(0, 0)]).await.unwrap();
        // This fails in the tree before any conditional map write can poison it.
        assert!(matches!(tree.apply_change(LeafChange { before: Some(leaf(0, 1)), after: None }).await, Err(TreeError::Conflict)));
    }
    TreeTransaction::<Ristretto255, _>::bind(&spec, &mut owner).apply_change(LeafChange { before: None, after: None }).await.unwrap();
    assert!(matches!(owner.commit().await, Err(TreeError::Conflict)));
    assert!(maps.begin().scan(MapName::Leaves, (Unbounded, Unbounded), 1).await.unwrap().is_empty());
}

#[tokio::test]
async fn structural_and_ordinary_writers_retry_the_entire_outer_transaction() {
    for structure_first in [true, false] {
        let (store, spec) = populated(BUCKET_CAPACITY).await;
        let mut structure = store.begin();
        let mut ordinary = store.begin();
        let added = leaf(BUCKET_CAPACITY as u32, 0);
        update(&mut structure, &spec, None, added.clone()).await;
        update(&mut ordinary, &spec, Some(leaf(0, 0)), leaf(0, 1)).await;
        if structure_first {
            structure.commit().await.unwrap();
            assert!(matches!(ordinary.commit().await, Err(TreeError::Conflict)));
            assert_eq!(store.begin().state(leaf(0, 0).entity_id).await, Some(leaf(0, 0).head.as_bytes().to_vec()));
            let mut retry = store.begin();
            update(&mut retry, &spec, Some(leaf(0, 0)), leaf(0, 1)).await;
            retry.commit().await.unwrap();
        } else {
            ordinary.commit().await.unwrap();
            assert!(matches!(structure.commit().await, Err(TreeError::Conflict)));
            assert_eq!(store.begin().state(added.entity_id).await, None);
            let mut retry = store.begin();
            update(&mut retry, &spec, None, added).await;
            retry.commit().await.unwrap();
        }
        let mut expected: Vec<_> = (0..=BUCKET_CAPACITY as u32).map(|n| leaf(n, 0)).collect();
        expected[0] = leaf(0, 1);
        for id in [FIRST, SECOND] {
            assert_tree(&mut store.begin(), id, &spec, &expected).await;
        }
    }
}

#[tokio::test]
async fn independent_head_changes_can_both_commit() {
    let (store, spec) = populated(2).await;
    let (mut a, mut b) = (store.begin(), store.begin());
    update(&mut a, &spec, Some(leaf(0, 0)), leaf(0, 1)).await;
    update(&mut b, &spec, Some(leaf(1, 0)), leaf(1, 1)).await;
    a.commit().await.unwrap();
    b.commit().await.unwrap();
    for id in [FIRST, SECOND] {
        assert_tree(&mut store.begin(), id, &spec, &[leaf(0, 1), leaf(1, 1)]).await;
    }
}

#[tokio::test]
async fn canonical_state_conflict_rolls_back_other_entity_and_tree_writes() {
    let (store, spec) = populated(2).await;
    let (mut winner, mut loser) = (store.begin(), store.begin());
    update(&mut loser, &spec, Some(leaf(1, 0)), leaf(1, 1)).await;
    // Only the canonical row overlaps here. The loser has not touched entity
    // zero's tree rows, so map-write or topology conflicts cannot mask it.
    loser.set_state(Some(leaf(0, 0).head), &leaf(0, 2)).await.unwrap();
    update(&mut winner, &spec, Some(leaf(0, 0)), leaf(0, 1)).await;
    winner.commit().await.unwrap();
    let committed = store.rows().await;
    assert!(matches!(loser.commit().await, Err(TreeError::Conflict)));
    assert_eq!(store.rows().await, committed);
    for id in [FIRST, SECOND] {
        assert_tree(&mut store.begin(), id, &spec, &[leaf(0, 1), leaf(1, 0)]).await;
    }
}
