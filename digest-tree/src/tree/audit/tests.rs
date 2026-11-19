//! The audit catches each kind of damage to a tree's rows.

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;

use super::*;
use crate::common::{asc, item, tree_of, Id, Item, ItemTree, Property, Tag};
use crate::fixtures::leaf_element;
use crate::store::{LeafRow, Meta, NodeRow};
use crate::tree::fixtures::key_of;
use crate::NodePrefix;
use crate::{leaf_address, Ristretto255, WriteItem};

type Write = WriteItem<RowKey, Row<Property, Tag, Ristretto255>>;

/// 100 members under keys 0 to 99: a root and two buckets.
async fn hundred() -> ItemTree {
    tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..100).map(|n| item!(n, n as i64, b"payload")))
}

/// The fault the audit names in `tree` after `damage` is written to its
/// store.
async fn fault_after(tree: ItemTree, damage: Vec<Write>) -> &'static str {
    tree.damage(damage).await;

    match tree.audit().await {
        Err(Error::Inconsistent(fault)) => fault,
        other => panic!("expected the audit to find a fault, got {other:?}"),
    }
}

async fn row(tree: &ItemTree, key: RowKey) -> Row<Property, Tag, Ristretto255> { tree.stored_row(&key).await.expect("a stored row") }

/// The address of the member with id `id` in a tree of [`hundred`], whose
/// key is its id.
fn address(id: u64) -> Vec<u8> { leaf_address(&key_of(id as i64), &Id(id)) }

async fn node_row(tree: &ItemTree, prefix: &NodePrefix) -> NodeRow<Ristretto255> {
    match row(tree, RowKey::Node(prefix.clone())).await {
        Row::Node(node) => node,
        other => panic!("a node row, not {other:?}"),
    }
}

async fn digest_of(tree: &ItemTree, prefix: &NodePrefix) -> Digest<Ristretto255> { node_row(tree, prefix).await.digest }

#[tokio::test]
async fn a_consistent_tree_passes_with_its_counts() {
    assert_eq!(hundred().await.audit().await.unwrap(), Audit { members: 100, nodes: 3 });
}

#[tokio::test]
async fn another_key_encoding_requires_an_explicit_rebuild() {
    for version in [1, u8::MAX] {
        let tree = hundred().await;
        let Row::Meta(mut meta) = row(&tree, RowKey::Meta).await else { panic!("metadata") };
        meta.key_encoding = version;
        tree.damage(vec![WriteItem::Put(RowKey::Meta, Row::Meta(meta))]).await;

        assert!(matches!(ItemTree::<Ristretto255>::open(tree.store().clone()).await,
            Err(Error::WrongKeyEncoding { found }) if found == version));
        assert!(tree.audit().await.is_err());

        let mut trx = tree.store().begin().await;
        let result =
            ItemTree::rebuild_in(tree.store().clone(), &mut trx, tree.key_spec().clone(), (0..100).map(|n| item!(n, n as i64, b"payload")))
                .await;
        if version != 1 {
            assert!(matches!(result, Err(Error::WrongKeyEncoding { found }) if found == version));
            trx.commit().await.unwrap();
            let Row::Meta(meta) = row(&tree, RowKey::Meta).await else { panic!("metadata") };
            assert_eq!(meta.key_encoding, version, "a future encoding is left unchanged");
            continue;
        }
        let rebuilt = result.unwrap();
        trx.commit().await.unwrap();
        let rebuilt = rebuilt.landed();
        assert_eq!(rebuilt.audit().await.unwrap().members, 100);
        assert_eq!(ItemTree::<Ristretto255>::open(tree.store().clone()).await.unwrap().key_spec(), tree.key_spec());
    }
}

#[tokio::test]
async fn the_audit_catches_a_node_whose_digest_is_not_its_members_sum() {
    let tree = hundred().await;
    let nodes = tree.node_prefixes().await;
    let mut damaged = node_row(&tree, &nodes[1]).await;
    damaged.digest = digest_of(&tree, &nodes[2]).await;

    assert_eq!(
        fault_after(tree, vec![WriteItem::Put(RowKey::Node(nodes[1].clone()), Row::Node(damaged))]).await,
        "a node's digest is not the sum of the members beneath it"
    );
}

/// A payload changed without its element: the sums still agree, the leaf
/// doesn't.
#[tokio::test]
async fn the_audit_catches_a_member_whose_element_is_not_its_leafs() {
    let tree = hundred().await;
    let address = address(7);
    let Row::Leaf(leaf) = row(&tree, RowKey::Leaf(address.clone())).await else { panic!("a leaf row") };
    let damaged = Row::Leaf(LeafRow { payload: Tag(b"other".to_vec()), element: leaf.element });

    assert_eq!(fault_after(tree, vec![WriteItem::Put(RowKey::Leaf(address), damaged)]).await, "a member's element is not its leaf's");
}

/// A member filed again under another key, beside member 80 in the bucket of
/// keys 64 to 99, with its element right and every digest above it counting
/// it: what an insert of a member the tree already holds elsewhere leaves,
/// which no change can see.
#[tokio::test]
async fn the_audit_catches_a_member_held_under_two_keys() {
    let tree = hundred().await;
    let key = key_of(80);
    let address = leaf_address(&key, &Id(7));
    let element = leaf_element::<Ristretto255>(&key, &Id(7).to_bytes(), b"payload");

    let mut damage = vec![WriteItem::Put(RowKey::Leaf(address.clone()), Row::Leaf(LeafRow { payload: Tag(b"payload".to_vec()), element }))];
    for prefix in tree.node_prefixes().await.into_iter().filter(|prefix| prefix.contains_address(&address)) {
        let mut node = node_row(&tree, &prefix).await;
        node.digest.add_element(&element).unwrap();
        damage.push(WriteItem::Put(RowKey::Node(prefix), Row::Node(node)));
    }

    assert_eq!(fault_after(tree, damage).await, "a member is held under two keys");
}

#[tokio::test]
async fn the_audit_catches_a_branch_that_lost_a_child() {
    let tree = hundred().await;
    let child = tree.node_prefixes().await[2].clone();

    assert_eq!(fault_after(tree, vec![WriteItem::Delete(RowKey::Node(child))]).await, "a node's children are not two siblings");
}

/// A member beneath the root but beneath neither of its children, with every
/// digest above it counting it.
#[tokio::test]
async fn the_audit_catches_a_member_beneath_no_bucket() {
    let tree = hundred().await;
    let key = key_of(-1);
    let address = leaf_address(&key, &Id(500));

    let stray_element = leaf_element::<Ristretto255>(&key, &Id(500).to_bytes(), b"payload");
    let mut root = node_row(&tree, &NodePrefix::root()).await;
    root.digest.add_element(&stray_element).unwrap();

    let damage = vec![
        WriteItem::Put(RowKey::Leaf(address), Row::Leaf(LeafRow { payload: Tag(b"payload".to_vec()), element: stray_element })),
        WriteItem::Put(RowKey::Node(NodePrefix::root()), Row::Node(root)),
    ];

    assert_eq!(fault_after(tree, damage).await, "a member lies beneath no bucket");
}

/// A bucket over its capacity, and a node with children holding no more than
/// a bucket holds, with every digest right.
#[tokio::test]
async fn the_audit_catches_nodes_whose_counts_their_shape_does_not_allow() {
    let tree = hundred().await;
    let nodes = tree.node_prefixes().await;
    let mut root = node_row(&tree, &nodes[0]).await;
    root.branches_at = None;

    let mut damage: Vec<Write> = nodes[1..].iter().cloned().map(|child| WriteItem::Delete(RowKey::Node(child))).collect();
    damage.push(WriteItem::Put(RowKey::Node(nodes[0].clone()), Row::Node(root)));

    let overfull = fault_after(tree, damage).await;
    assert_eq!(overfull, "a node holds a number of members its shape doesn't allow");

    // Keyed by id alone, 65 members split into ids 0 to 63 and id 64; leaving
    // 63 members beneath the first child and id 64 beneath the second leaves
    // the root's children holding 64, which one bucket holds.
    let members = |ids: &mut dyn Iterator<Item = u64>| ids.map(|id| item!(id, [], b"")).collect::<Vec<Item>>();
    let tree: ItemTree = tree_of!(KeySpec::new(vec![]), members(&mut (0..65)));

    let first: ItemTree = tree_of!(KeySpec::new(vec![]), members(&mut (0..63)));
    let second: ItemTree = tree_of!(KeySpec::new(vec![]), members(&mut std::iter::once(64)));

    let both: ItemTree = tree_of!(KeySpec::new(vec![]), members(&mut (0..63).chain([64])));
    let damage = shrink_to(&tree, 63..64, [&both, &first, &second]).await;

    assert_eq!(fault_after(tree, damage).await, "a node holds a number of members its shape doesn't allow");
}

/// A branch with an empty side, which a removal must collapse: every digest
/// right, every count one its node's shape allows, yet the branch parts
/// nothing.
#[tokio::test]
async fn the_audit_catches_a_branch_with_an_empty_side() {
    // Keyed by id alone, 65 members split into ids 0 to 63 and id 64;
    // removing id 64 alone, without collapsing the branch, leaves the second
    // side empty.
    let members = |ids: &mut dyn Iterator<Item = u64>| ids.map(|id| item!(id, [], b"")).collect::<Vec<Item>>();
    let tree: ItemTree = tree_of!(KeySpec::new(vec![]), members(&mut (0..65)));

    let kept: ItemTree = tree_of!(KeySpec::new(vec![]), members(&mut (0..64)));
    let empty: ItemTree = tree_of!(KeySpec::new(vec![]), members(&mut std::iter::empty()));

    let damage = shrink_to(&tree, 64..65, [&kept, &kept, &empty]).await;
    assert_eq!(fault_after(tree, damage).await, "a branch has an empty side");
}

/// The damage that removes the members with ids in `removed` from `tree`,
/// keyed by id alone with three nodes, a root and two children, and gives
/// each node the digest of the matching tree's root.
async fn shrink_to(tree: &ItemTree, removed: std::ops::Range<u64>, digests_from: [&ItemTree; 3]) -> Vec<Write> {
    let mut damage: Vec<Write> = removed.map(|id| WriteItem::Delete(RowKey::Leaf(leaf_address(&[], &Id(id))))).collect();

    for (prefix, digests_from) in tree.node_prefixes().await.into_iter().zip(digests_from) {
        let mut node = node_row(tree, &prefix).await;
        node.digest = digest_of(digests_from, &NodePrefix::root()).await;
        damage.push(WriteItem::Put(RowKey::Node(prefix), Row::Node(node)));
    }

    damage
}

/// A branch that records none, a branch that records another prefix than
/// its children share, and a bucket that records a branch, each with every
/// digest and count right: what a record left stale by a change leaves, which
/// reads would follow to the wrong rows.
#[tokio::test]
async fn the_audit_catches_a_node_whose_record_is_not_where_it_branches() {
    let nodes = hundred().await.node_prefixes().await;
    let (root, bucket) = (&nodes[0], &nodes[1]);

    for (prefix, branches_at) in [(root, None), (root, Some(NodePrefix::root())), (bucket, Some(bucket.clone()))] {
        let tree = hundred().await;
        let mut node = node_row(&tree, prefix).await;
        node.branches_at = branches_at;

        assert_eq!(
            fault_after(tree, vec![WriteItem::Put(RowKey::Node(prefix.clone()), Row::Node(node))]).await,
            "a node's record of where it branches is not where its children do"
        );
    }
}

#[tokio::test]
async fn the_audit_catches_metadata_of_another_group_or_spec() {
    fn identifier<G: DigestGroup>() -> u8 { G::ROW_VERSION }

    for meta in [
        Meta {
            key_spec: KeySpec::new(vec![asc!(Property::A, ValueType::I64)]),
            group: identifier::<crate::LtHash16>(),
            key_encoding: ankurah_core_types::ordering::KEY_ENCODING_VERSION,
        },
        Meta {
            key_spec: KeySpec::new(vec![]),
            group: identifier::<Ristretto255>(),
            key_encoding: ankurah_core_types::ordering::KEY_ENCODING_VERSION,
        },
    ] {
        let fault = fault_after(hundred().await, vec![WriteItem::Put(RowKey::Meta, Row::Meta(meta))]).await;
        assert_eq!(fault, "the metadata row names another group, key spec or key encoding");
    }
}
