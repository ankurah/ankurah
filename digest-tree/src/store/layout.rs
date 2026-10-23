//! The byte layouts of every key and value the tree's maps hold, in one
//! place, so that the maps hold bytes and nothing else.
//!
//! Map by map:
//!
//! - leaves: the key is the [leaf address](crate::leaf_address), the key the
//!   entity is filed under followed by its 32-byte entity id; the value is
//!   the head hash (32 bytes) and the leaf element in the row codec
//!   ([`LeafElement::ROW_LEN`] bytes in the tree's group), [`leaf_value_len`]
//!   bytes in all.
//! - lookup: the key is the entity id (32 bytes); the value is every key the
//!   entity is filed under, each preceded by its length as an unsigned 32-bit
//!   big-endian integer, in ascending order and each once.
//! - nodes: a tree node's base row is keyed by its prefix's encoding
//!   ([`NodePrefix::to_bytes`]) and holds the digest in the row codec
//!   ([`Digest::ROW_LEN`] bytes); each of its delta rows is keyed by the
//!   prefix's encoding, the byte [`DELTA_SEPARATOR`] and a suffix unique to the
//!   append ([`MapAccess::append`](crate::MapAccess::append)), and
//!   holds a digest delta in the row codec. The encoding is prefix-ordered, so
//!   a subtree is one range scan, but a descendant's encoding may extend an
//!   ancestor's bytes, so a suffix alone after the prefix could make a deeper
//!   tree node's base key look like a delta key. No prefix encoding contains
//!   the separator (its bytes stop at 253), so a tree node's deltas fill a key
//!   range of their own, from the encoding and the separator up to the
//!   encoding with its last byte raised by one, that no other tree node's rows
//!   enter. The tree never reads a suffix.
//!
//! A key or value in any other shape is reported as
//! [`TreeError::Unreadable`], naming which.

use std::collections::BTreeSet;

use ankurah_core_types::EntityId;

use super::rows::LeafRow;
use crate::address::NodePrefix;
use crate::digest::{exact, DecodeError, Digest, DigestGroup, HeadHash, LeafElement};
use crate::error::TreeError;

const HEAD_LEN: usize = 32;
const KEY_LEN_LEN: usize = 4;
const COUNT_LEN: usize = 8;

/// The empty lookup key holds a big-endian u64 count of entities with more
/// than one leaf. All other lookup keys are 32-byte entity identities.
pub(crate) const MULTIPLE_KEYS: &[u8] = b"";

/// The length of a leaf row's value in the group `G`: the head hash and the
/// leaf element's row bytes.
pub const fn leaf_value_len<G: DigestGroup>() -> usize { HEAD_LEN + LeafElement::<G>::ROW_LEN }

/// The byte between a prefix's encoding and the suffix in a delta key.
pub const DELTA_SEPARATOR: u8 = 0xff;

/// The key and the entity id an address is made of.
fn split_address<'a>(what: &'static str, address: &'a [u8]) -> Result<(&'a [u8], EntityId), TreeError> {
    if address.len() < EntityId::BYTE_LEN {
        return Err(TreeError::unreadable(what, DecodeError::Length { expected: EntityId::BYTE_LEN, found: address.len() }));
    }
    let (key, id) = address.split_at(address.len() - EntityId::BYTE_LEN);
    Ok((key, EntityId::from_bytes(id.try_into().expect("32 bytes"))))
}

pub(crate) fn leaf_value<G: DigestGroup>(row: &LeafRow<G>) -> Vec<u8> {
    let mut value = Vec::with_capacity(leaf_value_len::<G>());
    value.extend_from_slice(row.head.as_bytes());
    value.extend_from_slice(&row.element.to_row_bytes());
    value
}

/// The leaf row filed at `address` with `value`.
pub(crate) fn leaf_row<G: DigestGroup>(address: &[u8], value: &[u8]) -> Result<LeafRow<G>, TreeError> {
    let (key, entity_id) = split_address("leaf address", address)?;
    let value = exact(value, leaf_value_len::<G>()).map_err(|source| TreeError::unreadable("leaf value", source))?;
    let (head, element) = value.split_at(HEAD_LEN);
    Ok(LeafRow {
        key: key.to_vec(),
        entity_id,
        head: HeadHash::from_bytes(head.try_into().expect("32 bytes")),
        element: LeafElement::from_row_bytes(element).map_err(|source| TreeError::unreadable("leaf value", source))?,
    })
}

/// The value of a tree node's base row or of a delta row: the digest in the
/// row codec.
pub(crate) fn digest_value<G: DigestGroup>(digest: &Digest<G>) -> Vec<u8> { digest.to_row_bytes() }

/// The digest a base or delta value holds; `what` names which for the error.
pub(crate) fn digest<G: DigestGroup>(what: &'static str, value: &[u8]) -> Result<Digest<G>, TreeError> {
    Digest::from_row_bytes(value).map_err(|source| TreeError::unreadable(what, source))
}

/// The first key a delta of the prefix encoded as `encoding` can have, which
/// its deltas are appended under, and the first key past them all (`None`
/// when nothing follows, as for the root): the range its deltas fill, which
/// no other tree node's rows enter.
pub(crate) fn delta_key_bounds(encoding: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let first = [encoding, &[DELTA_SEPARATOR]].concat();
    let mut encoding = encoding.to_vec();
    // Every key beginning with the encoding and the separator lies before the
    // encoding with its last byte raised by one; encoding bytes stop at 253.
    let past = encoding.pop().map(|last| {
        encoding.push(last + 1);
        encoding
    });
    (first, past)
}

/// What a key of the nodes map names: the base row of the tree node at a
/// prefix, or one of its delta rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeKey {
    /// The key is the prefix's encoding.
    Base(NodePrefix),
    /// The key is the prefix's encoding, the separator and a suffix.
    Delta(NodePrefix),
}

impl NodeKey {
    /// The prefix of the tree node whose row the key names.
    pub(crate) fn into_prefix(self) -> NodePrefix {
        match self {
            NodeKey::Base(prefix) | NodeKey::Delta(prefix) => prefix,
        }
    }
}

/// What the key `key` of the nodes map names.
pub(crate) fn node_key(key: &[u8]) -> Result<NodeKey, TreeError> {
    let separator = key.iter().position(|byte| *byte == DELTA_SEPARATOR);
    let encoding = separator.map_or(key, |separator| &key[..separator]);
    let prefix = NodePrefix::from_bytes(encoding).map_err(|source| TreeError::unreadable("node key", source))?;
    Ok(if separator.is_some() { NodeKey::Delta(prefix) } else { NodeKey::Base(prefix) })
}

/// The value of the multi-key count row: the count of entities filed under
/// more than one key.
pub(crate) fn multiple_keys_value(count: u64) -> Vec<u8> { count.to_be_bytes().to_vec() }

/// The count the multi-key count row holds; zero where there is no row.
pub(crate) fn multiple_keys_count(value: Option<&[u8]>) -> Result<u64, TreeError> {
    let Some(value) = value else { return Ok(0) };
    let count = exact(value, COUNT_LEN).map_err(|source| TreeError::unreadable("multi-key count", source))?;
    Ok(u64::from_be_bytes(count.try_into().expect("eight bytes")))
}

pub(crate) fn lookup_value(keys: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    let mut value = Vec::with_capacity(keys.iter().map(|key| KEY_LEN_LEN + key.len()).sum());
    for key in keys {
        value.extend_from_slice(&(key.len() as u32).to_be_bytes());
        value.extend_from_slice(key);
    }
    value
}

/// The keys a lookup value lists.
pub(crate) fn lookup_keys(value: &[u8]) -> Result<BTreeSet<Vec<u8>>, TreeError> {
    let truncated = || TreeError::Unreadable { what: "lookup value", source: None };
    let mut keys = BTreeSet::new();
    let mut rest = value;
    while !rest.is_empty() {
        let (len, after) = rest.split_at_checked(KEY_LEN_LEN).ok_or_else(truncated)?;
        let len = u32::from_be_bytes(len.try_into().expect("four bytes")) as usize;
        let (key, after) = after.split_at_checked(len).ok_or_else(truncated)?;
        keys.insert(key.to_vec());
        rest = after;
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::{Leaf, LtHash16, Ristretto255};
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    fn entity(n: u8) -> EntityId { EntityId::from_bytes([n; 32]) }

    fn random_prefix(rng: &mut SmallRng, from: &NodePrefix, more_bits: u32) -> NodePrefix {
        (0..more_bits).fold(from.clone(), |prefix, _| prefix.child(rng.gen()))
    }

    /// A delta key of `prefix` with a suffix of one to sixteen random bytes.
    fn random_delta_key(rng: &mut SmallRng, prefix: &NodePrefix) -> Vec<u8> {
        let (mut key, _) = delta_key_bounds(&prefix.to_bytes());
        key.extend((0..rng.gen_range(1..=16)).map(|_| rng.gen::<u8>()));
        key
    }

    /// Whether `key` lies within the range of `prefix`'s delta keys.
    fn among_deltas_of(prefix: &NodePrefix, key: &[u8]) -> bool {
        let (first, past) = delta_key_bounds(&prefix.to_bytes());
        first.as_slice() <= key && past.is_none_or(|past| key < past.as_slice())
    }

    /// Whether `key` lies within the range of `prefix`'s subtree.
    fn within_subtree_of(prefix: &NodePrefix, key: &[u8]) -> bool {
        prefix.to_bytes().as_slice() <= key && prefix.after_subtree().is_none_or(|after| key < after.to_bytes().as_slice())
    }

    /// A tree node's keys, base and deltas, lie within its subtree's range, its
    /// deltas within their own range whatever their suffixes, and a
    /// descendant's keys within the subtree's range but never among the
    /// deltas: so a subtree is one scan, and a delta is never read as a
    /// tree node, or a tree node as a delta.
    #[test]
    fn delta_keys_fill_a_range_of_their_own_within_the_subtree() {
        let mut rng = SmallRng::seed_from_u64(19);
        for _ in 0..500 {
            let (depth, deeper) = (rng.gen_range(0..=20), rng.gen_range(1..=60));
            let prefix = random_prefix(&mut rng, &NodePrefix::root(), depth);
            let descendant = random_prefix(&mut rng, &prefix, deeper);
            let own_delta = random_delta_key(&mut rng, &prefix);
            let descendant_delta = random_delta_key(&mut rng, &descendant);

            assert!(among_deltas_of(&prefix, &own_delta), "{prefix:?}");
            assert!(within_subtree_of(&prefix, &prefix.to_bytes()) && within_subtree_of(&prefix, &own_delta), "{prefix:?}");
            for key in [descendant.to_bytes(), descendant_delta.clone()] {
                assert!(within_subtree_of(&prefix, &key), "{prefix:?} above {descendant:?}");
                assert!(!among_deltas_of(&prefix, &key), "{prefix:?} against {descendant:?}");
            }
            assert!(!among_deltas_of(&descendant, &own_delta), "{descendant:?} against {prefix:?}");

            assert_eq!(node_key(&prefix.to_bytes()).unwrap(), NodeKey::Base(prefix.clone()));
            assert_eq!(node_key(&own_delta).unwrap(), NodeKey::Delta(prefix));
            assert_eq!(node_key(&descendant_delta).unwrap(), NodeKey::Delta(descendant));
        }
        assert_eq!(delta_key_bounds(&NodePrefix::root().to_bytes()), (vec![DELTA_SEPARATOR], None), "the root's deltas run to the end");
    }

    /// In the group `G`, a leaf row's value and a digest's round-trip, and
    /// other lengths and an unknown version are refused.
    fn leaf_and_digest_values_round_trip<G: DigestGroup>() {
        let head = HeadHash::of([[1; 32]]);
        let row: LeafRow<G> =
            LeafRow { key: b"k".to_vec(), entity_id: entity(2), head, element: Leaf::new(b"k", entity(2), head).element() };
        let value = leaf_value(&row);
        assert_eq!(value.len(), leaf_value_len::<G>());
        assert_eq!(leaf_row(&row.address(), &value).unwrap(), row);
        assert!(matches!(leaf_row::<G>(&row.address(), &value[1..]), Err(TreeError::Unreadable { what: "leaf value", .. })));
        assert!(matches!(leaf_row::<G>(&[1; 31], &value), Err(TreeError::Unreadable { what: "leaf address", .. })));

        let removal = -Digest::from(row.element);
        assert_eq!(digest("delta value", &digest_value(&removal)).unwrap(), removal, "a negative count survives");
        assert!(
            matches!(digest::<G>("node value", &vec![0; Digest::<G>::ROW_LEN]), Err(TreeError::Unreadable { what: "node value", .. })),
            "codec version 0"
        );
        assert!(matches!(digest::<G>("delta value", &digest_value(&removal)[1..]), Err(TreeError::Unreadable { what: "delta value", .. })));
    }

    #[test]
    fn values_round_trip_and_other_lengths_are_refused() {
        leaf_and_digest_values_round_trip::<Ristretto255>();
        leaf_and_digest_values_round_trip::<LtHash16>();

        let keys: BTreeSet<Vec<u8>> = [b"".to_vec(), b"ab".to_vec(), b"a".to_vec()].into_iter().collect();
        let value = lookup_value(&keys);
        assert_eq!(value, [&[0, 0, 0, 0][..], &[0, 0, 0, 1, b'a'], &[0, 0, 0, 2, b'a', b'b']].concat(), "ascending, length-prefixed");
        assert_eq!(lookup_keys(&value).unwrap(), keys);
        assert_eq!(lookup_keys(&[]).unwrap(), BTreeSet::new());
        for truncated in [&value[..3], &value[..value.len() - 1]] {
            assert!(matches!(lookup_keys(truncated), Err(TreeError::Unreadable { what: "lookup value", .. })));
        }
    }
}
