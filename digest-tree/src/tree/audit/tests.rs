//! The audit catches each kind of damage to a tree's rows.

use super::*;
use crate::fixtures::{leaf_element, Id, Item, Spec, Tag};
use crate::store::{LeafRow, Meta};
use crate::tree::fixtures::{built, by_id_spec, integer_key, integer_spec, item, Tree};
use crate::NodePrefix;
use crate::{leaf_address, Member, Ristretto255, WriteItem};

type Write = WriteItem<RowKey, Row<Spec, Tag, Ristretto255>>;

/// 100 members under keys 0 to 99: a root and two buckets.
async fn hundred() -> Tree { built(integer_spec(), (0..100).map(|n| item(n, Some(i64::from(n)), b"payload"))).await }

/// The fault the audit names in `tree` after `damage` is written to its
/// store.
async fn fault_after(tree: Tree, damage: Vec<Write>) -> &'static str {
    tree.store().write(damage).await.unwrap();

    match tree.audit().await {
        Err(Error::Inconsistent(fault)) => fault,
        other => panic!("expected the audit to find a fault, got {other:?}"),
    }
}

async fn row(tree: &Tree, key: RowKey) -> Row<Spec, Tag, Ristretto255> { tree.store().get(&key).await.unwrap().expect("a stored row") }

/// The address of the member with id `id` in a tree of [`hundred`], whose
/// key is its id.
fn address(id: u32) -> Vec<u8> { leaf_address(&integer_key(Some(i64::from(id))), &Id(id)) }

async fn digest_of(tree: &Tree, prefix: &NodePrefix) -> Digest<Ristretto255> {
    match row(tree, RowKey::Node(prefix.clone())).await {
        Row::Node(digest) => digest,
        other => panic!("a node row, not {other:?}"),
    }
}

#[tokio::test]
async fn a_consistent_tree_passes_with_its_counts() {
    assert_eq!(hundred().await.audit().await.unwrap(), Audit { members: 100, nodes: 3 });
}

#[tokio::test]
async fn the_audit_catches_a_node_whose_digest_is_not_its_members_sum() {
    let tree = hundred().await;
    let nodes = tree.node_prefixes().await;
    let other = digest_of(&tree, &nodes[2]).await;

    assert_eq!(
        fault_after(tree, vec![WriteItem::Put(RowKey::Node(nodes[1].clone()), Row::Node(other))]).await,
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
    let again = item(7, Some(80), b"payload");
    let key = again.key(&integer_spec()).unwrap();
    let address = leaf_address(&key, &Id(7));
    let element = leaf_element::<Ristretto255>(&key, &Id(7).to_bytes(), b"payload");

    let mut damage = vec![WriteItem::Put(RowKey::Leaf(address.clone()), Row::Leaf(LeafRow { payload: Tag(b"payload".to_vec()), element }))];
    for node in tree.node_prefixes().await.into_iter().filter(|node| node.contains_address(&address)) {
        let mut digest = digest_of(&tree, &node).await;
        digest.add_element(&element).unwrap();
        damage.push(WriteItem::Put(RowKey::Node(node), Row::Node(digest)));
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
    let stray = item(500, Some(-1), b"payload");
    let key = stray.key(&integer_spec()).unwrap();
    let address = leaf_address(&key, &Id(500));

    let stray_element = leaf_element::<Ristretto255>(&key, &Id(500).to_bytes(), b"payload");
    let mut root = digest_of(&tree, &NodePrefix::root()).await;
    root.add_element(&stray_element).unwrap();

    let damage = vec![
        WriteItem::Put(RowKey::Leaf(address), Row::Leaf(LeafRow { payload: Tag(b"payload".to_vec()), element: stray_element })),
        WriteItem::Put(RowKey::Node(NodePrefix::root()), Row::Node(root)),
    ];

    assert_eq!(fault_after(tree, damage).await, "a member lies beneath no bucket");
}

/// A bucket over its capacity, and a node with children at the merge
/// threshold, with every digest right.
#[tokio::test]
async fn the_audit_catches_nodes_whose_counts_their_shape_does_not_allow() {
    let tree = hundred().await;
    let children: Vec<RowKey> = tree.node_prefixes().await[1..].iter().cloned().map(RowKey::Node).collect();
    let overfull = fault_after(tree, children.into_iter().map(WriteItem::Delete).collect()).await;
    assert_eq!(overfull, "a node holds a number of members its shape doesn't allow");

    // Keyed by id alone, 65 members split into ids 0 to 63 and id 64; leaving
    // 31 members beneath the first child and id 64 beneath the second leaves
    // the root's children holding 32.
    let members = |ids: &mut dyn Iterator<Item = u32>| ids.map(|id| Item::new(id, [], b"")).collect::<Vec<_>>();
    let tree: Tree = built(by_id_spec(), members(&mut (0..65))).await;

    let first: Tree = built(by_id_spec(), members(&mut (0..31))).await;
    let second: Tree = built(by_id_spec(), members(&mut std::iter::once(64))).await;

    let both: Tree = built(by_id_spec(), members(&mut (0..31).chain([64]))).await;
    let nodes = tree.node_prefixes().await;

    assert_eq!(
        fault_after(tree, shrink_to(31..64, &nodes, [&both, &first, &second]).await).await,
        "a node holds a number of members its shape doesn't allow"
    );
}

/// A branch with an empty side, which a removal must collapse: every digest
/// right, every count one its node's shape allows, yet the branch parts
/// nothing.
#[tokio::test]
async fn the_audit_catches_a_branch_with_an_empty_side() {
    // Keyed by id alone, 65 members split into ids 0 to 63 and id 64;
    // removing id 64 alone, without collapsing the branch, leaves the second
    // side empty.
    let members = |ids: &mut dyn Iterator<Item = u32>| ids.map(|id| Item::new(id, [], b"")).collect::<Vec<_>>();
    let tree: Tree = built(by_id_spec(), members(&mut (0..65))).await;

    let kept: Tree = built(by_id_spec(), members(&mut (0..64))).await;
    let empty: Tree = built(by_id_spec(), members(&mut std::iter::empty())).await;

    let nodes = tree.node_prefixes().await;
    assert_eq!(fault_after(tree, shrink_to(64..65, &nodes, [&kept, &kept, &empty]).await).await, "a branch has an empty side");
}

/// The damage that removes the members with ids in `removed` from a tree
/// keyed by id alone whose three nodes are `nodes`, and gives each node the
/// digest of the matching tree's root: a root and two children.
async fn shrink_to(removed: std::ops::Range<u32>, nodes: &[NodePrefix], digests_from: [&Tree; 3]) -> Vec<Write> {
    let mut damage: Vec<Write> = removed.map(|id| WriteItem::Delete(RowKey::Leaf(leaf_address(&[], &Id(id))))).collect();

    for (node, tree) in nodes.iter().zip(digests_from) {
        damage.push(WriteItem::Put(RowKey::Node(node.clone()), Row::Node(digest_of(tree, &NodePrefix::root()).await)));
    }

    damage
}

#[tokio::test]
async fn the_audit_catches_metadata_of_another_group_or_spec() {
    fn identifier<G: DigestGroup>() -> u8 { G::ROW_VERSION }

    for meta in [
        Meta { key_spec: integer_spec(), group: identifier::<crate::LtHash16>() },
        Meta { key_spec: by_id_spec(), group: identifier::<Ristretto255>() },
    ] {
        let fault = fault_after(hundred().await, vec![WriteItem::Put(RowKey::Meta, Row::Meta(meta))]).await;
        assert_eq!(fault, "the metadata row names another group or key spec");
    }
}
