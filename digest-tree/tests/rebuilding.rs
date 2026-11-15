//! A complete rebuild keeps the store's identity and existing handles, and
//! expresses every removed row through the ordinary transaction batch.

mod common;

use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{
    DigestGroup, DigestTree, Encode, Error, LockedTrx, LtHash16, MemoryMap, ReadTrx, Ristretto255, Row, RowKey, ScanDirection, Store,
    StoreError, StoreTrx, WriteItem,
};
use common::{
    asc, counted, desc, failing_store, item, no_items, stored_rows, ten_items, tree_of, CountedTree, FailingTree, Id, Item, ItemStore,
    ItemTree, Property, Tag,
};

fn spec() -> KeySpec<Property> { KeySpec::new(vec![asc!(Property::A, ValueType::I64)]) }

async fn preserves_handles<G: DigestGroup>() {
    let old: Vec<Item> = (0..300).map(|n| item!(n, n as i64, b"old")).collect();
    let tree: ItemTree<G> = tree_of!(spec(), old);
    let view = tree.view();
    let before = stored_rows(tree.store()).await;
    let members = [item!(1, 1, b"old"), item!(500, -10, b"new")];

    let mut owner_trx = tree.store().begin().await;
    let rebuilt = ItemTree::rebuild_in(tree.store().clone(), &mut owner_trx, spec(), members.clone()).await.unwrap();
    assert_eq!(stored_rows(tree.store()).await, before, "the rebuild is not visible before commit");
    assert_eq!(view.count(..).await.unwrap(), 300);
    owner_trx.commit().await.unwrap();
    let rebuilt = rebuilt.landed();

    let expected: ItemTree<G> = tree_of!(spec(), members);
    assert_eq!(stored_rows(tree.store()).await, stored_rows(expected.store()).await, "obsolete rows are gone");
    assert_eq!(view.span_digest(..).await.unwrap(), expected.view().span_digest(..).await.unwrap(), "an existing view remains usable");
    assert_eq!(tree.audit().await.unwrap().members, 2);
    assert_eq!(rebuilt.key_spec(), tree.key_spec());

    // A transaction made through the old handle still updates the same store.
    let mut changes = tree.transaction();
    changes.insert(item!(501, 20, b"later")).unwrap();
    changes.commit().await.unwrap();
    assert_eq!(rebuilt.view().count(..).await.unwrap(), 3);
}

#[tokio::test]
async fn rebuilding_preserves_existing_handles_in_ristretto() { preserves_handles::<Ristretto255>().await }

#[tokio::test]
async fn rebuilding_preserves_existing_handles_in_lthash() { preserves_handles::<LtHash16>().await }

#[tokio::test]
async fn rebuilding_empty_members_keeps_an_empty_tree_but_absent_metadata_is_refused() {
    let tree: ItemTree = tree_of!(spec(), ten_items());
    let mut owner_trx = tree.store().begin().await;
    let rebuilt = ItemTree::rebuild_in(tree.store().clone(), &mut owner_trx, spec(), no_items!()).await.unwrap();
    owner_trx.commit().await.unwrap();
    rebuilt.landed();
    assert_eq!(tree.view().count(..).await.unwrap(), 0);
    assert_eq!(tree.audit().await.unwrap().members, 0);
    assert_eq!(ItemTree::<Ristretto255>::open(tree.store().clone()).await.unwrap().key_spec(), &spec());

    let empty = ItemStore::<Ristretto255>::new();
    let mut owner_trx = empty.begin().await;
    assert!(matches!(ItemTree::rebuild_in(empty.clone(), &mut owner_trx, spec(), ten_items()).await, Err(Error::NoTree)));
    owner_trx.commit().await.unwrap();
    assert!(stored_rows(&empty).await.is_empty());
}

#[tokio::test]
async fn incompatible_spec_or_invalid_members_write_nothing() {
    let (tree, counts) = counted(spec(), ten_items()).await;
    let before = stored_rows(tree.store()).await;
    let writes = counts.writes();

    let mut owner_trx = tree.store().begin().await;
    let other_spec = KeySpec::new(vec![desc!(Property::A, ValueType::I64)]);
    assert!(matches!(
        CountedTree::rebuild_in(tree.store().clone(), &mut owner_trx, other_spec, ten_items()).await,
        Err(Error::KeySpecMismatch)
    ));
    owner_trx.commit().await.unwrap();

    let mut owner_trx = tree.store().begin().await;
    assert!(matches!(
        CountedTree::rebuild_in(tree.store().clone(), &mut owner_trx, spec(), [item!(1, [], b"no key")]).await,
        Err(Error::Key { .. })
    ));
    owner_trx.commit().await.unwrap();

    assert_eq!(counts.writes(), writes, "refusals submitted no write, even when their owner committed");
    assert_eq!(stored_rows(tree.store()).await, before);
}

#[tokio::test]
async fn rebuilding_refuses_another_digest_group_without_changing_its_rows() {
    let source: ItemTree = tree_of!(spec(), ten_items());
    let (_, meta) = stored_rows(source.store()).await.into_iter().find(|(key, _)| *key == RowKey::Meta).unwrap();
    let store = ItemStore::<LtHash16>::new();
    let mut owner_trx = store.begin().await;
    owner_trx.write(vec![WriteItem::Put(RowKey::Meta, Row::decode(&meta).unwrap())]).await.unwrap();
    owner_trx.commit().await.unwrap();
    let before = stored_rows(&store).await;

    let mut owner_trx = store.begin().await;
    assert!(matches!(ItemTree::rebuild_in(store.clone(), &mut owner_trx, spec(), ten_items()).await, Err(Error::WrongGroup)));
    owner_trx.commit().await.unwrap();
    assert_eq!(stored_rows(&store).await, before);
}

#[tokio::test]
async fn rebuilding_refuses_another_stores_transaction() {
    let tree: ItemTree = tree_of!(spec(), ten_items());
    let other: ItemTree = tree_of!(spec(), [item!(500, 500, b"other")]);
    let before = stored_rows(tree.store()).await;
    let other_before = stored_rows(other.store()).await;
    let mut owner_trx = other.store().begin().await;
    assert!(matches!(ItemTree::rebuild_in(tree.store().clone(), &mut owner_trx, spec(), no_items!()).await, Err(Error::OtherTree)));
    owner_trx.commit().await.unwrap();
    assert_eq!(stored_rows(tree.store()).await, before);
    assert_eq!(stored_rows(other.store()).await, other_before);
}

#[tokio::test]
async fn discarding_or_failing_the_owners_commit_leaves_the_old_tree_intact() {
    let (store, faults) = failing_store();
    let tree = FailingTree::create(store.clone(), spec(), ten_items()).await.unwrap();
    let before = stored_rows(&store).await;

    let mut owner_trx = store.begin().await;
    let rebuilt = FailingTree::rebuild_in(store.clone(), &mut owner_trx, spec(), no_items!()).await.unwrap();
    drop((rebuilt, owner_trx));
    assert_eq!(stored_rows(&store).await, before);

    let mut owner_trx = store.begin().await;
    let rebuilt = FailingTree::rebuild_in(store.clone(), &mut owner_trx, spec(), [item!(20, 20, b"new")]).await.unwrap();
    faults.fail_writes(true);
    assert!(owner_trx.commit().await.is_err());
    drop(rebuilt);
    faults.fail_writes(false);
    assert_eq!(stored_rows(&store).await, before);
    assert_eq!(tree.audit().await.unwrap().members, 10);
}

#[tokio::test]
async fn prepared_changes_recheck_the_rebuilt_members() {
    let before = item!(1, 1, b"before");
    let after = item!(1, 2, b"after");
    let tree: ItemTree = tree_of!(spec(), [before.clone()]);
    let mut change = tree.transaction();
    change.update(&before, &after).unwrap();
    let prepared = change.prepare().await;

    let mut owner_trx = tree.store().begin().await;
    let rebuilt =
        ItemTree::rebuild_in(tree.store().clone(), &mut owner_trx, spec(), [before.clone(), item!(2, 3, b"unrelated")]).await.unwrap();
    owner_trx.commit().await.unwrap();
    rebuilt.landed();
    let mut owner_trx = tree.store().begin().await;
    let applied = prepared.apply(&mut owner_trx).await.unwrap();
    owner_trx.commit().await.unwrap();
    applied.landed();
    let expected: ItemTree = tree_of!(spec(), [after.clone(), item!(2, 3, b"unrelated")]);
    assert_eq!(stored_rows(tree.store()).await, stored_rows(expected.store()).await);

    let later = item!(1, 4, b"later");
    let mut stale = tree.transaction();
    stale.update(&after, &later).unwrap();
    let prepared = stale.prepare().await;
    let mut owner_trx = tree.store().begin().await;
    let rebuilt = ItemTree::rebuild_in(tree.store().clone(), &mut owner_trx, spec(), [later, item!(2, 3, b"unrelated")]).await.unwrap();
    owner_trx.commit().await.unwrap();
    rebuilt.landed();
    let before_refusal = stored_rows(tree.store()).await;
    let mut owner_trx = tree.store().begin().await;
    assert!(matches!(prepared.apply(&mut owner_trx).await, Err(Error::NotHeld { .. })));
    owner_trx.commit().await.unwrap();
    assert_eq!(stored_rows(tree.store()).await, before_refusal, "already-applied input remains a strict refusal");
}

type ItemRow = Row<Property, Tag, Ristretto255>;
type MemoryTrx = LockedTrx<MemoryMap<RowKey, ItemRow>>;

/// Record the actual StoreTrx batches, before the reference transaction can
/// combine them or collapse repeated keys.
#[derive(Clone)]
struct RecordingStore {
    store: ItemStore,
    batches: Arc<Mutex<Vec<Vec<(RowKey, bool)>>>>,
}

struct RecordingTrx<'a> {
    trx: &'a mut MemoryTrx,
    batches: &'a Mutex<Vec<Vec<(RowKey, bool)>>>,
}

impl Store<RowKey, ItemRow> for RecordingStore {
    type Read = <ItemStore as Store<RowKey, ItemRow>>::Read;
    type Trx<'a> = RecordingTrx<'a>;
    type Pending = ();

    async fn begin_read(&self) -> Result<Self::Read, StoreError> { self.store.begin_read().await }

    fn owns(&self, trx: &RecordingTrx<'_>) -> bool { self.store.owns(&trx.trx) }

    fn landed(&self, _: ()) {}
}

impl ReadTrx<RowKey, ItemRow> for RecordingTrx<'_> {
    async fn get(&mut self, key: &RowKey) -> Result<Option<ItemRow>, StoreError> { self.trx.get(key).await }

    async fn get_many(&mut self, keys: &[RowKey]) -> Result<Vec<Option<ItemRow>>, StoreError> { self.trx.get_many(keys).await }

    async fn range(
        &mut self,
        start: Bound<&RowKey>,
        end: Bound<&RowKey>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(RowKey, ItemRow)>, StoreError> {
        self.trx.range(start, end, direction, limit).await
    }
}

impl StoreTrx<RowKey, ItemRow> for RecordingTrx<'_> {
    type Pending = ();

    async fn write(&mut self, batch: Vec<WriteItem<RowKey, ItemRow>>) -> Result<(), StoreError> {
        self.batches.lock().unwrap().push(
            batch
                .iter()
                .map(|item| match item {
                    WriteItem::Put(key, _) => (key.clone(), true),
                    WriteItem::Delete(key) => (key.clone(), false),
                })
                .collect(),
        );
        self.trx.write(batch).await
    }
}

#[tokio::test]
async fn a_rebuild_writes_one_batch_including_every_obsolete_node_and_leaf() {
    let tree: ItemTree = tree_of!(spec(), (0..300).map(|n| item!(n, n as i64, b"old")));
    let expected: ItemTree = tree_of!(spec(), [item!(500, 500, b"new")]);
    let old_keys: BTreeSet<_> = stored_rows(tree.store()).await.into_iter().map(|(key, _)| key).collect();
    let new_keys: BTreeSet<_> = stored_rows(expected.store()).await.into_iter().map(|(key, _)| key).collect();
    let obsolete: BTreeSet<_> = old_keys.difference(&new_keys).cloned().collect();
    assert!(obsolete.iter().any(|key| matches!(key, RowKey::Node(_))));
    assert!(obsolete.iter().any(|key| matches!(key, RowKey::Leaf(_))));

    let recording = RecordingStore { store: tree.store().clone(), batches: Arc::default() };
    let mut owner_trx = tree.store().begin().await;
    let lent = RecordingTrx { trx: &mut owner_trx, batches: &recording.batches };
    let rebuilt = DigestTree::<Property, Id, Tag, Ristretto255, _>::rebuild_in(recording.clone(), lent, spec(), [item!(500, 500, b"new")])
        .await
        .unwrap();
    {
        let batches = recording.batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.iter().map(|(key, _)| key).collect::<BTreeSet<_>>().len(), batch.len(), "each key is written once");
        assert_eq!(batch.iter().filter(|(_, put)| !put).map(|(key, _)| key.clone()).collect::<BTreeSet<_>>(), obsolete);
        assert_eq!(batch.iter().filter(|(_, put)| *put).map(|(key, _)| key.clone()).collect::<BTreeSet<_>>(), new_keys);
    }
    owner_trx.commit().await.unwrap();
    rebuilt.landed();
    assert_eq!(stored_rows(tree.store()).await, stored_rows(expected.store()).await);
}
