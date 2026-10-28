use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use sha2::{Digest as _, Sha256};

use super::*;
use crate::common::{asc, item, tree_of, Id, ItemTree, Property};
use crate::fixtures::{leaf_element, to_hex};
use crate::span::address_of;
use crate::{MemberId, Ristretto255};

/// The node rows of a build of `n` members keyed by id alone, ids 0 to `n` as
/// eight big-endian bytes, with empty payloads, pinned by the SHA-256 of each node's prefix encoding,
/// behind its length as an unsigned 32-bit big-endian integer, and its stored
/// digest, in prefix order. The answers were computed outside this crate,
/// from the address encoding, the layout rules, the prefix encoding and RFC
/// 9496's arithmetic and encoding, so they pin the trie's shape and every
/// digest rather than echo the code.
#[tokio::test]
async fn builds_lay_out_the_nodes_an_independent_implementation_does() {
    for (n, nodes, answer) in [
        (0, 1, "1c3cf3f1ec3b92921f6afcfa42bf08cb4f665ae5e3244a8ef56e50a562798ef1"),
        (65, 3, "42f1daa95dfb49f497f843b864b869ad7a76739cce1ead3179808febb870f056"),
        (4096, 127, "e801b25c5e314fb9ab1f304603374ccaafe409bc4af62f25c827874a02d366b4"),
    ] {
        let tree: ItemTree = tree_of!(KeySpec::new(vec![]), (0..n).map(|id| item!(id, [], b"")));
        let mut rows = Vec::new();
        let mut count = 0;
        for (key, row) in tree.stored_rows().await {
            if let RowKey::Node(prefix) = key {
                let prefix = prefix.to_bytes();
                rows.extend_from_slice(&(prefix.len() as u32).to_be_bytes());
                rows.extend_from_slice(&prefix);

                // A node row is its kind's tag, then the stored digest.
                rows.extend_from_slice(&row[1..]);
                count += 1;
            }
        }
        assert_eq!((count, to_hex(&Sha256::digest(&rows))), (nodes, answer.to_string()), "{n} members");
    }
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

    let (left_digest, right_digest, root_digest) = (nodes[0].digest, nodes[1].digest, nodes[2].digest);
    assert_eq!((left_digest.count(), right_digest.count(), root_digest.count()), (64, 1, 65));

    let mut sum = left_digest;
    sum.add(&right_digest).unwrap();

    assert_eq!(sum, root_digest);
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

/// A branch's children are found by its first child alone, and the nearest
/// nodes beneath a prefix on a compressed path are the branch's children.
#[tokio::test]
async fn children_and_the_nearest_nodes_beneath_a_prefix_are_found_in_prefix_order() {
    // Keys 0 to 99, with one id each: the root's children branch where the
    // keys' encodings first differ, deep in their bytes.
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..100).map(|n| item!(n, n as i64, b"")));
    let rows = StagedRows::new(tree.store());
    let trie = Trie::new(&rows);
    let [first, second] = trie.children(&NodePrefix::root()).await.unwrap().expect("a root of 100 members has children");
    assert_eq!(first.prefix.sibling(), Some(second.prefix.clone()));

    let mut sum = first.digest;
    sum.add(&second.digest).unwrap();
    assert_eq!(sum, trie.root().await.unwrap().digest);

    // Every prefix on the path between the root and its children reaches
    // both children; a prefix beneath a bucket reaches nothing.
    let shared = NodePrefix::of(&start_bytes(&first.prefix), first.prefix.bit_len() - 1);
    for bits in 0..=shared.bit_len() {
        let on_path = NodePrefix::of(&start_bytes(&first.prefix), bits);
        let nearest = trie.nearest_beneath(&on_path).await.unwrap();

        assert_eq!(nearest, [first.clone(), second.clone()], "{on_path:?}");
    }

    assert!(trie.children(&first.prefix).await.unwrap().is_none(), "a bucket of fewer than 65 members");
    assert!(trie.nearest_beneath(&first.prefix.child(true)).await.unwrap().is_empty());
}

/// The bytes a node's span starts at: its prefix's bits, zero-padded.
fn start_bytes(prefix: &NodePrefix) -> Vec<u8> {
    match prefix.span().start {
        std::ops::Bound::Included(start) => start,
        other => panic!("a node's span starts at an included bound, not {other:?}"),
    }
}
