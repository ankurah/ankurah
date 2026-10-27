//! Making a tree: creating one empty, building one whole, and opening the one
//! a store holds, with the stores and inputs each refuses.

mod common;

use ankurah_digest_tree::{
    Digest, DigestGroup, Error, LtHash16, MemberId, Ristretto255, Row, RowKey, Span, StagedCommit, Store, WriteItem,
};
use common::{built, integer_spec, item, stored_rows, Id, Item, ItemStore, ItemTree, KeySpec, Tag};

#[tokio::test]
async fn a_created_tree_is_empty_and_opens_with_its_spec() {
    let store = ItemStore::<Ristretto255>::new();
    let created = ItemTree::create(store.clone(), integer_spec()).await.unwrap();
    assert_eq!(created.view().len().await.unwrap(), 0);

    let opened = ItemTree::<Ristretto255>::open(store).await.unwrap();
    assert_eq!(opened.key_spec(), &integer_spec());
    opened.audit().await.unwrap();
}

/// An empty tree has one set of rows however it was made.
#[tokio::test]
async fn creating_a_tree_and_building_one_of_no_members_give_the_same_rows() {
    let (by_create, by_build) = (ItemStore::<Ristretto255>::new(), ItemStore::<Ristretto255>::new());
    ItemTree::create(by_create.clone(), integer_spec()).await.unwrap();
    ItemTree::build(by_build.clone(), integer_spec(), Vec::<Item>::new()).await.unwrap();

    assert_eq!(stored_rows(&by_create).await, stored_rows(&by_build).await);
}

#[tokio::test]
async fn create_and_build_refuse_a_store_that_holds_a_tree() {
    let store = ItemStore::<Ristretto255>::new();
    ItemTree::create(store.clone(), integer_spec()).await.unwrap();

    assert!(matches!(ItemTree::create(store.clone(), integer_spec()).await, Err(Error::StoreNotEmpty)));
    assert!(matches!(ItemTree::build(store, integer_spec(), [item!(1, Some(1), b"")]).await, Err(Error::StoreNotEmpty)));
}

/// Rows without a tree, from whatever wrote them, would mix with a new
/// tree's.
#[tokio::test]
async fn create_and_build_refuse_a_store_that_holds_stray_rows() {
    let store = ItemStore::<Ristretto255>::new();
    store.write(vec![WriteItem::Put(RowKey::Leaf(vec![1]), Row::Node(Digest::empty()))]).await.unwrap();

    assert!(matches!(ItemTree::create(store.clone(), integer_spec()).await, Err(Error::StoreNotEmpty)));
    assert!(matches!(ItemTree::build(store, integer_spec(), [item!(1, Some(1), b"")]).await, Err(Error::StoreNotEmpty)));
}

#[tokio::test]
async fn open_refuses_a_store_that_holds_no_tree() {
    assert!(matches!(ItemTree::<Ristretto255>::open(ItemStore::new()).await, Err(Error::NoTree)));
}

/// A store that keeps bytes holds a tree's rows whatever group summed them,
/// so a tree summed in another group is told by the group its metadata
/// records.
#[cfg(feature = "sled")]
#[tokio::test]
async fn open_refuses_a_tree_summed_in_another_group() {
    use ankurah_digest_tree::{DigestTree, SledStore};
    use common::{KeySpec, Tag};

    type SledTree<G> = DigestTree<KeySpec, Id, Tag, G, SledStore<Row<KeySpec, Tag, G>>>;

    let rows = sled::Config::new().temporary(true).open().unwrap().open_tree("rows").unwrap();
    SledTree::<Ristretto255>::build(SledStore::new(rows.clone()), integer_spec(), [item!(1, Some(1), b"")]).await.unwrap();

    assert!(matches!(SledTree::<LtHash16>::open(SledStore::new(rows)).await, Err(Error::WrongGroup)));
}

#[tokio::test]
async fn build_refuses_the_same_id_twice() {
    let members = [item!(1, Some(1), b"a"), item!(2, Some(2), b"b"), item!(1, Some(3), b"c")];
    assert!(matches!(
        ItemTree::<Ristretto255>::build(ItemStore::new(), integer_spec(), members).await,
        Err(Error::DuplicateMember { member }) if member == Id(1).to_bytes().into_owned()
    ));
}

#[tokio::test]
async fn build_refuses_a_member_without_a_key() {
    let members = [item!(1, Some(1), b""), Item::new(2, [], b"")];
    match ItemTree::<Ristretto255>::build(ItemStore::new(), integer_spec(), members).await {
        Err(Error::Key { member, .. }) => assert_eq!(member, Id(2).to_bytes().into_owned()),
        other => panic!("expected a member without a key, got {:?}", other.map(|_| ())),
    }
}

/// A build lays out several chunks of members, which it hashes apart.
async fn a_large_build_holds_every_member<G: DigestGroup>() {
    let members: Vec<Item> = (0..5000).map(|n| item!(n, Some((n % 97) as i64), &n.to_le_bytes())).collect();
    let tree = built::<G>(integer_spec(), members.clone()).await;

    assert_eq!(tree.audit().await.unwrap().members, 5000);
    let view = tree.view();

    for member in members.iter().step_by(499) {
        assert_eq!(
            view.member(&integer_spec().collate([member.values[0]]), &member.id).await.unwrap().map(|leaf| leaf.payload),
            Some(member.payload.clone())
        );
    }
}

#[tokio::test]
async fn a_large_ristretto255_build_holds_every_member() { a_large_build_holds_every_member::<Ristretto255>().await }

#[tokio::test]
async fn a_large_lthash16_build_holds_every_member() { a_large_build_holds_every_member::<LtHash16>().await }

/// Every operation's future can cross threads when the members can, so a
/// caller can spawn it on a multithreaded runtime. Not awaited: compiling is
/// the test.
#[tokio::test]
async fn every_operations_future_can_cross_threads() {
    fn send<T: Send>(_: T) {}

    fn is_send<T: Send>() {}

    let store = ItemStore::<Ristretto255>::new();
    send(ItemTree::create(store.clone(), integer_spec()));
    send(ItemTree::open(store.clone()));
    send(ItemTree::build(store, integer_spec(), vec![item!(1, Some(1), b"")]));

    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"")]).await;
    let mut trx = tree.transaction();
    trx.insert(item!(2, Some(2), b"")).unwrap();
    send(trx.commit());

    // A staged commit, too, which its owner holds across its own storage
    // transaction.
    let mut trx = tree.transaction();
    trx.insert(item!(2, Some(2), b"")).unwrap();
    send(trx.stage());
    is_send::<StagedCommit<KeySpec, Tag, Ristretto255>>();

    send(tree.audit());

    let view = tree.view();
    let root = view.span_digest(&Span::all()).await.unwrap();

    send(view.len());
    let key = integer_spec().collate([1]);
    send(view.member(&key, &Id(1)));
    send(view.count(&Span::all()));
    send(view.span_digest(&Span::all()));
    send(view.refine(&root));

    // A range read is a stream, not a future, which its caller holds across
    // its own awaits.
    send(view.range(..));
}
