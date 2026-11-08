use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use sha2::{Digest as _, Sha256};

use super::*;
use crate::common::{asc, item, stored_rows, tree_of, Id, ItemTree, Property};
use crate::fixtures::{leaf_element, to_hex};
use crate::span::address_of;
use crate::{MemberId, Ristretto255, Store};

/// The SHA-256 of the node rows of a creation of `n` members keyed by id alone,
/// ids 0 to `n` as eight big-endian bytes, with empty payloads: of each
/// node's prefix encoding, behind its length as an unsigned 32-bit big-endian
/// integer, and the part of its stored row `stored` picks out, in prefix
/// order. And how many nodes the creation has.
async fn node_rows_hash(n: u64, stored: impl Fn(&[u8]) -> &[u8]) -> (usize, String) {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![]), (0..n).map(|id| item!(id, [], b"")));
    let mut rows = Vec::new();
    let mut count = 0;
    for (key, row) in stored_rows(tree.store()).await {
        if let RowKey::Node(prefix) = key {
            let prefix = prefix.to_bytes();
            rows.extend_from_slice(&(prefix.len() as u32).to_be_bytes());
            rows.extend_from_slice(&prefix);
            rows.extend_from_slice(stored(&row));
            count += 1;
        }
    }

    (count, to_hex(&Sha256::digest(&rows)))
}

/// A node's stored row after its kind's tag: its stored digest, then where it
/// branches.
fn after_the_tag(row: &[u8]) -> &[u8] { &row[1..] }

/// A node's stored digest alone, which follows its kind's tag.
fn stored_digest(row: &[u8]) -> &[u8] { &row[1..1 + Digest::<Ristretto255>::STORED_LEN] }

/// The node rows of creations, pinned by their prefixes and stored digests
/// alone. The answers were computed outside this crate, from the address
/// encoding, the layout rules, the prefix encoding and RFC 9496's arithmetic
/// and encoding, so they pin the trie's shape and every digest rather than
/// echo the code.
#[tokio::test]
async fn creations_lay_out_the_nodes_an_independent_implementation_does() {
    for (n, nodes, answer) in [
        (0, 1, "1c3cf3f1ec3b92921f6afcfa42bf08cb4f665ae5e3244a8ef56e50a562798ef1"),
        (65, 3, "42f1daa95dfb49f497f843b864b869ad7a76739cce1ead3179808febb870f056"),
        (4096, 127, "e801b25c5e314fb9ab1f304603374ccaafe409bc4af62f25c827874a02d366b4"),
    ] {
        assert_eq!(node_rows_hash(n, stored_digest).await, (nodes, answer.to_string()), "{n} members");
    }
}

/// A creation's node rows pinned whole: each node's prefix, its stored digest,
/// and after the digest a byte for bucket (0) or branch (1), and for a branch
/// the encoding of the prefix its children share. The answer was computed
/// outside this crate, as the answers above were, so it pins where every node
/// branches and how its row records it.
#[tokio::test]
async fn creations_record_where_their_nodes_branch_as_an_independent_implementation_does() {
    let answer = "da9c80afe71709688d86fa7718710515e6fadcd52b39e93eb024a1d0dcf1d584";
    assert_eq!(node_rows_hash(4096, after_the_tag).await, (127, answer.to_string()));
}

/// Leaves of these ids under empty keys, with their elements, in address
/// order.
fn leaves(ids: impl IntoIterator<Item = u64>) -> Vec<(Vec<u8>, <Ristretto255 as DigestGroup>::Element)> {
    let mut leaves: Vec<_> = ids
        .into_iter()
        .map(|id| {
            let id = Id(id).to_bytes().into_owned();
            (address_of(&[], &id), leaf_element::<Ristretto255>(b"", &id, b""))
        })
        .collect();

    leaves.sort_by(|a, b| a.0.cmp(&b.0));
    leaves
}

fn placed(leaves: &[(Vec<u8>, <Ristretto255 as DigestGroup>::Element)]) -> Vec<(&[u8], &<Ristretto255 as DigestGroup>::Element)> {
    leaves.iter().map(|(address, element)| (address.as_slice(), element)).collect()
}

/// A bucket holds every member up to its capacity; one more splits it where
/// the first and last addresses first differ, and the split's sides sum to
/// their parent.
#[test]
fn a_layout_splits_past_the_capacity_where_the_addresses_first_differ() {
    let full = leaves(0..64);
    assert_eq!(lay_out::<Ristretto255>(&NodePrefix::root(), &placed(&full)).unwrap().len(), 1);

    let leaves = leaves(0..65);
    let nodes = lay_out::<Ristretto255>(&NodePrefix::root(), &placed(&leaves)).unwrap();

    // Ids 0 and 64 first differ at bit 129 of their addresses: past the empty
    // key's end and seven escaped zero bytes, id 0's last byte is zero and id
    // 64's is 0x40.
    let (left, right) = (NodePrefix::of(&leaves[0].0, 130), NodePrefix::of(&leaves[64].0, 130));
    let prefixes: Vec<&NodePrefix> = nodes.iter().map(|node| &node.prefix).collect();
    assert_eq!(prefixes, [&left, &right, &NodePrefix::root()], "the children, then their parent");

    let (left_digest, right_digest, root_digest) = (nodes[0].row.digest, nodes[1].row.digest, nodes[2].row.digest);
    assert_eq!((left_digest.count(), right_digest.count(), root_digest.count()), (64, 1, 65));

    let mut sum = left_digest;
    sum.add(&right_digest).unwrap();

    assert_eq!(sum, root_digest);

    let records: Vec<&Option<NodePrefix>> = nodes.iter().map(|node| &node.row.branches_at).collect();
    assert_eq!(records, [&None, &None, &Some(NodePrefix::of(&leaves[0].0, 129))], "the parent records where its children branch");
}

/// An address that begins another can't be held apart from it by any bit.
#[test]
fn a_layout_refuses_addresses_that_are_prefixes_of_others() {
    let element = leaf_element::<Ristretto255>(b"", b"leaf", b"");
    let mut addresses: Vec<Vec<u8>> = (0..65u8).map(|n| vec![7, n]).collect();
    addresses.insert(0, vec![7]);

    let placed: Vec<(&[u8], &<Ristretto255 as DigestGroup>::Element)> =
        addresses.iter().map(|address| (address.as_slice(), &element)).collect();

    assert!(matches!(lay_out::<Ristretto255>(&NodePrefix::root(), &placed), Err(Error::Inconsistent(_))));
}

/// A branch's children are the nodes at the prefix it records followed by 0
/// and by 1. Asked together, a span that reaches both of the root's children
/// has those children beneath its smallest containing subtree, and a span
/// within a bucket has nothing beneath it.
#[tokio::test]
async fn children_and_the_children_beneath_a_spans_smallest_subtree_are_found() {
    // Keys 0 to 99, with one id each: the root's children branch where the
    // keys' encodings first differ, deep in their bytes.
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..100).map(|n| item!(n, n as i64, b"")));
    let mut rows = TreeRows::new(tree.store().begin_read().await.unwrap());
    let mut trie = Trie::new(&mut rows);
    let root = trie.root().await.unwrap();
    let [first, second] = trie.children(&root).await.unwrap().expect("a root of 100 members has children");
    assert_eq!(first.prefix.sibling(), Some(second.prefix.clone()));

    let mut sum = first.row.digest;
    sum.add(&second.row.digest).unwrap();
    assert_eq!(sum, root.row.digest);

    assert!(trie.children(&first).await.unwrap().is_none(), "a bucket of fewer than 65 members");

    let found = trie.children_of_smallest_containing(&[Span::all(), first.prefix.span()]).await.unwrap();
    assert_eq!(found, [Some([first.clone(), second.clone()]), None]);
}
