//! The trie's shape as members come and go: splits, merges and inserted
//! branches.

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;

use crate::common::{asc, item, tree_of, Item, ItemTree, Property};
use crate::fixtures::leaf_element;
use crate::tree::fixtures::assert_same_tree;
use crate::{Error, NodePrefix, Ristretto255, Row, RowKey, Store, WriteItem};

/// Members with ids from `ids` and no values, for a tree keyed by id alone.
fn by_id(ids: impl IntoIterator<Item = u64>) -> Vec<Item> { ids.into_iter().map(|id| item!(id, [], b"")).collect() }

async fn by_id_tree(ids: impl IntoIterator<Item = u64>) -> ItemTree { tree_of!(KeySpec::new(vec![]), by_id(ids)) }

/// Commit one transaction to a tree keyed by id alone: insert the members
/// with ids `inserted` and remove those with ids `removed`, each as the tree
/// holds it.
async fn change(tree: &ItemTree, inserted: impl IntoIterator<Item = u64>, removed: impl IntoIterator<Item = u64>) -> Result<(), Error> {
    let mut trx = tree.transaction();
    for member in by_id(removed) {
        trx.remove(member)?;
    }

    for member in by_id(inserted) {
        trx.insert(member)?;
    }

    trx.commit().await
}

#[tokio::test]
async fn a_bucket_splits_past_its_capacity_into_the_nodes_a_build_lays_out() {
    let tree = by_id_tree(0..64).await;
    assert_eq!(tree.node_prefixes().await.len(), 1, "64 members fit one bucket");
    change(&tree, [64], []).await.unwrap();

    let rebuilt = by_id_tree(0..65).await;
    assert_eq!(tree.node_prefixes().await.len(), 3);
    assert_eq!(tree.stored_rows().await, rebuilt.stored_rows().await);
}

/// A subtree with children that a removal leaves holding no more than a
/// bucket holds merges into one bucket, though both its sides still hold
/// members: the tree holds a build's very rows.
#[tokio::test]
async fn a_subtree_shrunk_to_a_buckets_capacity_merges_into_one_bucket() {
    // 65 members split into ids 0 to 63 and id 64; removing id 1 leaves 64,
    // with both sides still holding members.
    let tree = by_id_tree(0..65).await;
    change(&tree, [], [1]).await.unwrap();

    let rebuilt = by_id_tree((0..65).filter(|&id| id != 1)).await;
    assert_eq!(tree.node_prefixes().await.len(), 1, "64 members fit one bucket");
    assert_eq!(tree.stored_rows().await, rebuilt.stored_rows().await);
}

/// A removal that empties one side of a branch leaves the branch parting
/// nothing, so the branch gives way to the other side: a bucket's members
/// become the branch's own, and a subtree's children its children. Either
/// way the tree holds a build's very rows.
#[tokio::test]
async fn a_branch_whose_side_empties_gives_way_to_the_other_side() {
    // 65 members split into ids 0 to 63 and id 64; removing id 64 leaves the
    // first side's bucket, which becomes the root.
    let bucket_side = by_id_tree(0..65).await;
    change(&bucket_side, [], [64]).await.unwrap();
    assert_eq!(bucket_side.stored_rows().await, by_id_tree(0..64).await.stored_rows().await);

    // Ids 0 to 199 lie on the root's first side, which has children of its
    // own, and an id with its top bit set alone on the second; removing that
    // id promotes the first side's children to the root's.
    let lone = 1 << 63;
    let subtree_side = by_id_tree((0..200).chain([lone])).await;
    let first_side_children = subtree_side.node_prefixes().await.len() - 3;

    change(&subtree_side, [], [lone]).await.unwrap();
    assert_eq!(subtree_side.node_prefixes().await.len(), first_side_children + 1, "the root and the first side's subtree");
    assert_eq!(subtree_side.stored_rows().await, by_id_tree(0..200).await.stored_rows().await);
}

/// A branch whose one side a transaction empties while it fills the other, a
/// full bucket, past its capacity gives way to that bucket, which then splits.
#[tokio::test]
async fn a_branch_that_collapses_onto_an_overfull_bucket_splits_it() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), (0..64).map(|n| item!(n, 0, b"")).chain([item!(100, 1, b"")]));
    assert_eq!(tree.node_prefixes().await.len(), 3, "64 members on one side, one on the other");

    let mut trx = tree.transaction();
    trx.remove(item!(100, 1, b"")).unwrap();
    trx.insert(item!(200, 0, b"")).unwrap();
    trx.commit().await.unwrap();

    assert_same_tree(&tree, &tree_of!(spec, (0..64).chain([200]).map(|n| item!(n, 0, b"")))).await;
}

/// A branch whose one side a transaction empties while it empties a side of
/// the other side's subtree gives way to that subtree, and then to its other
/// side in turn.
#[tokio::test]
async fn a_branch_that_collapses_onto_a_subtree_with_an_empty_side_collapses_again() {
    // Key 1 keeps more members than a bucket holds, so the root keeps its
    // children once the transaction is done and must give way twice.
    let members = (0..10).map(|n| item!(n, 0, b"")).chain((100..180).map(|n| item!(n, 1, b""))).chain([item!(300, 2, b"")]);

    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), members);
    assert_eq!(tree.node_prefixes().await.len(), 7, "keys 0 and 1 on one side, parted twice beneath it, and key 2 on the other");

    let mut trx = tree.transaction();
    trx.remove(item!(300, 2, b"")).unwrap();

    for n in 0..10 {
        trx.remove(item!(n, 0, b"")).unwrap();
    }

    trx.commit().await.unwrap();
    assert_same_tree(&tree, &tree_of!(spec, (100..180).map(|n| item!(n, 1, b"")))).await;
}

/// A member beneath a node with children but outside both gets a branch of
/// its own above them, and the children keep their rows.
#[tokio::test]
async fn a_member_outside_a_nodes_children_gets_a_branch_of_its_own() {
    // Keys 0 to 99: the root's children part the keys deep in their bytes,
    // and a negative key lies outside both.
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), (0..100).map(|n| item!(n, n as i64, b"")));
    let before = tree.node_prefixes().await;

    let mut trx = tree.transaction();
    trx.insert(item!(100, -1, b"")).unwrap();
    trx.commit().await.unwrap();

    let after = tree.node_prefixes().await;
    assert_eq!(after.len(), before.len() + 2, "a branch above the old children, and a bucket for the new member");
    assert!(before.iter().all(|prefix| after.contains(prefix)), "the old nodes stay");
    assert_same_tree(&tree, &tree_of!(spec, (0..100).map(|n| item!(n, n as i64, b"")).chain([item!(100, -1, b"")]))).await;
}

/// A split sums the bucket's members, and a bucket whose digest disagrees
/// with them is damage the split reports rather than repairs.
#[tokio::test]
async fn a_split_reports_a_bucket_whose_digest_disagrees_with_its_members() {
    let tree = by_id_tree(0..64).await;
    let root = RowKey::Node(NodePrefix::root());
    let Some(Row::Node(mut damaged)) = tree.store().get(&root).await.unwrap() else { panic!("a root node") };

    // Another sum with the same count, so that the 65th member splits it.
    damaged.add_element(&leaf_element::<Ristretto255>(b"", b"stray", b"")).unwrap();
    damaged.sub_element(&leaf_element::<Ristretto255>(b"", b"other", b"")).unwrap();
    tree.store().write(vec![WriteItem::Put(root, Row::Node(damaged))]).await.unwrap();

    assert!(matches!(change(&tree, [64], []).await, Err(Error::Inconsistent(_))));
}
