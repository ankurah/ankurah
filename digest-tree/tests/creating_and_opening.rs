//! Making a tree: creating one, on a store of its own or in an owner's
//! transaction, and opening the one a store holds, with the stores and inputs
//! each refuses.

mod common;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::test_support::bucket_row;
use ankurah_digest_tree::{
    Applied, Created, Digest, DigestGroup, Encode, Error, LtHash16, MemberId, Prepared, Ristretto255, Row, RowKey, Span, StoreTrx,
    WriteItem,
};
use common::{asc, item, leaf_at, no_items, stored_rows, ten_items, tree_of, Id, Item, ItemStore, ItemTree, Property, Tag};

#[tokio::test]
async fn a_tree_created_empty_opens_with_its_spec() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let store = ItemStore::<Ristretto255>::new();
    let created = ItemTree::create(store.clone(), spec.clone(), no_items!()).await.unwrap();
    assert_eq!(created.view().count(..).await.unwrap(), 0);

    let opened = ItemTree::<Ristretto255>::open(store).await.unwrap();
    assert_eq!(opened.key_spec(), &spec);
    opened.audit().await.unwrap();
}

/// Weak references follow the tree's existing ownership, including derived
/// reads and writes, without extending it themselves.
#[tokio::test]
async fn weak_references_follow_the_shared_tree_lifetime() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), ten_items());
    let weak = tree.downgrade();
    let another_weak = weak.clone();
    let cloned = tree.clone();
    drop(tree);
    assert!(weak.upgrade().is_some());

    let view = cloned.view();
    drop(cloned);
    assert!(weak.upgrade().is_some());
    let cloned_view = view.clone();
    drop(view);
    assert!(weak.upgrade().is_some());

    let stream = cloned_view.range(..).unwrap();
    drop(cloned_view);
    let transaction = weak.upgrade().unwrap().transaction();
    drop(stream);
    assert!(weak.upgrade().is_some());

    let prepared = transaction.prepare().await;
    assert!(weak.upgrade().is_some());
    let store = prepared.tree().store().clone();
    let mut owner_trx = store.begin().await;
    let applied = prepared.apply(&mut owner_trx).await.unwrap();
    assert!(weak.upgrade().is_some());
    owner_trx.commit().await.unwrap();
    applied.landed();
    assert!(weak.upgrade().is_none());
    assert!(another_weak.upgrade().is_none());
}

/// An owner that keeps a tree's rows beside its own records creates the tree
/// in its own transaction: nothing of it is there until the owner commits,
/// and then the tree has the rows a creation on a store of its own writes.
#[tokio::test]
async fn a_tree_created_in_an_owners_transaction_exists_once_it_commits() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let store = ItemStore::<Ristretto255>::new();

    let mut owner_trx = store.begin().await;
    let created = ItemTree::create_in(store.clone(), &mut owner_trx, spec.clone(), ten_items()).await.unwrap();
    assert!(matches!(ItemTree::<Ristretto255>::open(store.clone()).await, Err(Error::NoTree)), "nothing before the commit");

    owner_trx.commit().await.unwrap();
    let tree = created.landed();

    let by_create: ItemTree = tree_of!(spec.clone(), ten_items());
    assert_eq!(stored_rows(&store).await, stored_rows(by_create.store()).await);

    assert_eq!(tree.view().count(..).await.unwrap(), 10);
    assert_eq!(ItemTree::<Ristretto255>::open(store).await.unwrap().key_spec(), &spec);
}

/// A creation dropped with its owner's transaction, as when the owner's
/// records fail to commit, leaves the store empty, and a tree can be created
/// there afterwards.
#[tokio::test]
async fn a_creation_whose_transaction_is_dropped_leaves_the_store_empty() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let store = ItemStore::<Ristretto255>::new();

    let mut owner_trx = store.begin().await;
    let created = ItemTree::create_in(store.clone(), &mut owner_trx, spec.clone(), ten_items()).await.unwrap();
    drop((created, owner_trx));

    assert!(stored_rows(&store).await.is_empty());
    ItemTree::create(store, spec, ten_items()).await.unwrap();
}

/// A creation given another store's transaction would write its rows beside
/// another tree's, so it is refused, and writes nothing to either store.
#[tokio::test]
async fn a_creation_refuses_a_transaction_on_another_store() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let (store, other) = (ItemStore::<Ristretto255>::new(), ItemStore::<Ristretto255>::new());

    let mut others_trx = other.begin().await;
    let refused = ItemTree::create_in(store.clone(), &mut others_trx, spec, ten_items()).await;
    assert!(matches!(refused, Err(Error::OtherTree)));

    others_trx.commit().await.unwrap();
    assert!(stored_rows(&store).await.is_empty() && stored_rows(&other).await.is_empty());
}

#[tokio::test]
async fn create_refuses_a_store_that_holds_a_tree() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let store = ItemStore::<Ristretto255>::new();
    ItemTree::create(store.clone(), spec.clone(), no_items!()).await.unwrap();

    assert!(matches!(ItemTree::create(store.clone(), spec.clone(), no_items!()).await, Err(Error::StoreNotEmpty)));
    assert!(matches!(ItemTree::create(store, spec, [item!(1, 1, b"")]).await, Err(Error::StoreNotEmpty)));
}

/// Rows without a tree, from whatever wrote them, would mix with a new
/// tree's.
#[tokio::test]
async fn create_refuses_a_store_that_holds_stray_rows() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let store = ItemStore::<Ristretto255>::new();

    let mut trx = store.begin().await;
    trx.write(vec![WriteItem::Put(RowKey::Leaf(vec![1]), bucket_row(Digest::empty()))]).await.unwrap();
    trx.commit().await.unwrap();

    assert!(matches!(ItemTree::create(store.clone(), spec.clone(), no_items!()).await, Err(Error::StoreNotEmpty)));
    assert!(matches!(ItemTree::create(store, spec, [item!(1, 1, b"")]).await, Err(Error::StoreNotEmpty)));
}

#[tokio::test]
async fn open_refuses_a_store_that_holds_no_tree() {
    assert!(matches!(ItemTree::<Ristretto255>::open(ItemStore::new()).await, Err(Error::NoTree)));
}

/// A store that keeps bytes holds a tree's rows whatever group summed them,
/// so a tree summed in another group is told by the group its metadata
/// records: here a tree's metadata row, decoded from its bytes into a store
/// of the other group's rows.
#[tokio::test]
async fn open_refuses_a_tree_summed_in_another_group() {
    let summed: ItemTree<Ristretto255> = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), [item!(1, 1, b"")]);
    let (_, meta) = stored_rows(summed.store()).await.into_iter().find(|(key, _)| *key == RowKey::Meta).expect("the metadata row");

    let store = ItemStore::<LtHash16>::new();
    let mut trx = store.begin().await;
    trx.write(vec![WriteItem::Put(RowKey::Meta, Row::decode(&meta).unwrap())]).await.unwrap();
    trx.commit().await.unwrap();

    assert!(matches!(ItemTree::<LtHash16>::open(store).await, Err(Error::WrongGroup)));
}

#[tokio::test]
async fn create_refuses_the_same_id_twice() {
    let members = [item!(1, 1, b"a"), item!(2, 2, b"b"), item!(1, 3, b"c")];
    assert!(matches!(
        ItemTree::<Ristretto255>::create(ItemStore::new(), KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), members).await,
        Err(Error::DuplicateMember { member }) if member == Id(1).to_bytes().into_owned()
    ));
}

#[tokio::test]
async fn create_refuses_a_member_without_a_key() {
    let members = [item!(1, 1, b""), item!(2, [], b"")];
    match ItemTree::<Ristretto255>::create(ItemStore::new(), KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), members).await {
        Err(Error::Key { member, .. }) => assert_eq!(member, Id(2).to_bytes().into_owned()),
        other => panic!("expected a member without a key, got {:?}", other.map(|_| ())),
    }
}

/// A creation lays out several chunks of members, which it hashes apart.
async fn a_large_creation_holds_every_member<G: DigestGroup>() {
    let members: Vec<Item> = (0..5000).map(|n| item!(n, (n % 97) as i64, &n.to_le_bytes())).collect();
    let tree: ItemTree<G> = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), members.clone());

    assert_eq!(tree.audit().await.unwrap().members, 5000);
    let view = tree.view();

    for member in members.iter().step_by(499) {
        assert_eq!(leaf_at(&view, [member.values[0].unwrap()], &member.id).await.map(|leaf| leaf.payload), Some(member.payload.clone()));
    }
}

#[tokio::test]
async fn a_large_ristretto255_creation_holds_every_member() { a_large_creation_holds_every_member::<Ristretto255>().await }

#[tokio::test]
async fn a_large_lthash16_creation_holds_every_member() { a_large_creation_holds_every_member::<LtHash16>().await }

/// Every operation's future can cross threads when the members can, so a
/// caller can spawn it on a multithreaded runtime. Not awaited: compiling is
/// the test.
#[tokio::test]
async fn every_operations_future_can_cross_threads() {
    fn send<T: Send>(_: T) {}

    fn is_send<T: Send>() {}

    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let store = ItemStore::<Ristretto255>::new();
    send(ItemTree::create(store.clone(), spec.clone(), vec![item!(1, 1, b"")]));
    send(ItemTree::open(store.clone()));

    let mut owner_trx = store.begin().await;
    send(ItemTree::create_in(store.clone(), &mut owner_trx, spec.clone(), vec![item!(1, 1, b"")]));
    is_send::<Created<Property, Id, Tag, Ristretto255, ItemStore>>();

    let tree: ItemTree = tree_of!(spec, [item!(1, 1, b"")]);
    let mut trx = tree.transaction();
    trx.insert(item!(2, 2, b"")).unwrap();
    send(trx.commit());

    // A commit in an owner's transaction, too, which its owner holds across
    // its own awaits.
    let mut trx = tree.transaction();
    trx.insert(item!(2, 2, b"")).unwrap();
    send(trx.prepare());
    is_send::<Prepared<Property, Id, Tag, Ristretto255, ItemStore>>();

    let mut trx = tree.transaction();
    trx.insert(item!(2, 2, b"")).unwrap();
    let prepared = trx.prepare().await;
    let mut owner_trx = tree.store().begin().await;
    send(prepared.apply(&mut owner_trx));
    is_send::<Applied<Property, Id, Tag, Ristretto255, ItemStore>>();

    send(tree.audit());

    let view = tree.view();

    send(view.count(..));
    send(view.count(&Span::all()));
    send(view.span_digest(&Span::all()));
    send(view.pages(vec![Span::all()]));
    send(view.differences(tree.view()));

    // A range read is a stream, not a future, which its caller holds across
    // its own awaits.
    send(view.range(..).unwrap());
}
