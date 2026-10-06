//! Properties a digest tree relies on: a set of leaves has one digest however
//! the leaves are grouped and whatever order they arrive in, a commit's
//! subtract-and-add update agrees with recomputation whether it changes an
//! entity's head or moves the entity to another key, a leaf's canonical
//! encoding is injective and binds its key, and the byte layouts round-trip.

use ankurah_core_types::EntityId;
use ankurah_digest::{Digest, HeadHash, Leaf, LeafPoint, DIGEST_WIRE_LEN};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;

/// A leaf's parts, owned so proptest can generate and shrink them.
#[derive(Clone, Debug)]
struct LeafParts {
    key: Vec<u8>,
    entity_id: [u8; 32],
    head: Vec<[u8; 32]>,
}

impl LeafParts {
    fn point(&self) -> LeafPoint {
        Leaf::new(&self.key, EntityId::from_bytes(self.entity_id), HeadHash::of(self.head.iter().copied())).point()
    }
}

fn leaf_parts() -> impl Strategy<Value = LeafParts> {
    (index_key(), any::<[u8; 32]>(), head()).prop_map(|(key, entity_id, head)| LeafParts { key, entity_id, head })
}

/// Index keys: mostly short, some of 250 to 261 bytes, either side of 255,
/// the largest length one byte can hold, and with zero bytes common, since the
/// key encoder writes them as terminators and escapes.
fn index_key() -> impl Strategy<Value = Vec<u8>> {
    let byte = prop_oneof![Just(0u8), any::<u8>()];
    prop_oneof![3 => vec(byte.clone(), 0..12), 1 => vec(byte, 250..262)]
}

/// A head of up to three event ids, empty included.
fn head() -> impl Strategy<Value = Vec<[u8; 32]>> { vec(any::<[u8; 32]>(), 0..4) }

/// A second key for a leaf filed under `key`: a prefix of it, it followed by
/// more bytes, or any key.
fn second_key(key: Vec<u8>) -> impl Strategy<Value = Vec<u8>> {
    let extended = key.clone();
    prop_oneof![
        any::<Index>().prop_map(move |cut| key[..cut.index(key.len() + 1)].to_vec()),
        vec(any::<u8>(), 1..4).prop_map(move |more| [&extended[..], &more].concat()),
        index_key(),
    ]
}

fn leaf_points(max: usize) -> impl Strategy<Value = Vec<LeafPoint>> {
    vec(leaf_parts(), 0..max).prop_map(|leaves| leaves.iter().map(LeafParts::point).collect())
}

fn digests(points: &[LeafPoint]) -> Vec<Digest> { points.iter().map(|point| Digest::from(*point)).collect() }

/// The first part of a canonical leaf encoding, read through the eight
/// big-endian bytes of its length, and the bytes after it.
fn split_part(bytes: &[u8]) -> (&[u8], &[u8]) {
    let (length, rest) = bytes.split_at(8);
    rest.split_at(u64::from_be_bytes(length.try_into().unwrap()) as usize)
}

/// Sum consecutive runs of `digests`, cutting before each position in `cuts`:
/// one level of a tree whose nodes hold consecutive leaves.
fn group(digests: &[Digest], cuts: &[Index]) -> Vec<Digest> {
    let mut at: Vec<usize> = cuts.iter().map(|cut| cut.index(digests.len() + 1)).collect();
    at.push(0);
    at.push(digests.len());
    at.sort_unstable();
    at.dedup();
    at.windows(2).map(|run| digests[run[0]..run[1]].iter().sum()).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Two members that group the same leaves into different trees, with
    /// different bucket boundaries and different depths, hold the same digest
    /// at the root, and it equals the sum of the leaves taken one by one.
    #[test]
    fn any_grouping_gives_the_same_digest(
        points in leaf_points(24),
        first in (vec(any::<Index>(), 0..6), vec(any::<Index>(), 0..3)),
        second in vec(any::<Index>(), 0..8),
    ) {
        let leaves = digests(&points);
        let flat: Digest = leaves.iter().sum();
        let two_levels: Digest = group(&group(&leaves, &first.0), &first.1).iter().sum();
        let one_level: Digest = group(&leaves, &second).iter().sum();
        prop_assert_eq!(two_levels, flat);
        prop_assert_eq!(one_level, flat);
        prop_assert_eq!(flat.count(), points.len() as i64);
    }

    /// Leaves folded in any order give the same digest.
    #[test]
    fn insertion_order_never_matters((points, reordered) in leaf_points(24).prop_flat_map(|points| (Just(points.clone()), Just(points).prop_shuffle()))) {
        let mut folded = Digest::empty();
        for point in &reordered {
            folded += Digest::from(*point);
        }
        prop_assert_eq!(folded, digests(&points).iter().sum::<Digest>());
    }

    /// A commit that changes one entity's head updates a digest by subtracting
    /// the old leaf's point and adding the new one, and the result equals the
    /// digest recomputed from scratch; the count does not change.
    #[test]
    fn subtracting_the_old_leaf_and_adding_the_new_equals_recomputation(
        leaves in vec(leaf_parts(), 1..16),
        changed in any::<Index>(),
        new_head in head(),
    ) {
        let i = changed.index(leaves.len());
        let before: Digest = leaves.iter().map(|leaf| Digest::from(leaf.point())).sum();
        let mut after = leaves.clone();
        after[i].head = new_head;
        let updated = before - Digest::from(leaves[i].point()) + Digest::from(after[i].point());
        let recomputed: Digest = after.iter().map(|leaf| Digest::from(leaf.point())).sum();
        prop_assert_eq!(updated, recomputed);
        prop_assert_eq!(updated.count(), leaves.len() as i64);
    }

    /// A commit that moves one entity to another key, its head unchanged,
    /// updates a digest the same way: subtracting the old leaf's point and
    /// adding the new one gives the digest recomputed from scratch, a digest
    /// other than the one before, with the same count.
    #[test]
    fn moving_an_entity_to_another_key_equals_recomputation(
        leaves in vec(leaf_parts(), 1..16),
        moved in any::<Index>(),
        new_key in index_key(),
    ) {
        let i = moved.index(leaves.len());
        prop_assume!(new_key != leaves[i].key);
        let before: Digest = leaves.iter().map(|leaf| Digest::from(leaf.point())).sum();
        let mut after = leaves.clone();
        after[i].key = new_key;
        let updated = before - Digest::from(leaves[i].point()) + Digest::from(after[i].point());
        let recomputed: Digest = after.iter().map(|leaf| Digest::from(leaf.point())).sum();
        prop_assert_eq!(updated, recomputed);
        prop_assert_ne!(updated, before);
        prop_assert_eq!(updated.count(), leaves.len() as i64);
    }

    /// The canonical encoding is injective: it parses back into exactly the
    /// key, entity id and head hash it was made from, with nothing left over,
    /// so no two distinct leaves share an encoding.
    #[test]
    fn the_canonical_encoding_is_injective(leaf in leaf_parts()) {
        let head = HeadHash::of(leaf.head.iter().copied());
        let encoded = Leaf::new(&leaf.key, EntityId::from_bytes(leaf.entity_id), head).encode();
        let (key, rest) = split_part(&encoded);
        let (entity_id, rest) = split_part(rest);
        let (head_hash, rest) = split_part(rest);
        prop_assert_eq!(key, &leaf.key[..]);
        prop_assert_eq!(entity_id, &leaf.entity_id[..]);
        prop_assert_eq!(head_hash, &head.as_bytes()[..]);
        prop_assert!(rest.is_empty());
    }

    /// The same entity with the same head under two keys is two distinct
    /// leaves: different points, a digest that counts both, and removing one
    /// leaves exactly the other.
    #[test]
    fn a_leaf_under_two_keys_is_two_distinct_leaves((leaf, other_key) in leaf_parts().prop_flat_map(|leaf| (Just(leaf.clone()), second_key(leaf.key)))) {
        prop_assume!(other_key != leaf.key);
        let filed_twice = LeafParts { key: other_key, ..leaf.clone() };
        let (a, b) = (leaf.point(), filed_twice.point());
        prop_assert_ne!(a, b);
        let both = Digest::from(a) + Digest::from(b);
        prop_assert_eq!(both.count(), 2);
        prop_assert_ne!(both, Digest::from(a) + Digest::from(a));
        prop_assert_eq!(both - Digest::from(b), Digest::from(a));
    }

    /// A head's hash does not depend on the order or repetition of its event
    /// ids, so neither does its leaf's point.
    #[test]
    fn a_head_is_hashed_as_a_set((head, reordered) in vec(any::<[u8; 32]>(), 1..5).prop_flat_map(|head| {
        let repeated: Vec<[u8; 32]> = head.iter().chain(head.iter().take(1)).copied().collect();
        (Just(head), Just(repeated).prop_shuffle())
    })) {
        prop_assert_eq!(HeadHash::of(head.iter().copied()), HeadHash::of(reordered.iter().copied()));
    }

    /// Rows and the wire form decode to the digest or leaf point that was
    /// encoded, including differences with negative counts, and any count
    /// survives the wire form unchanged.
    #[test]
    fn the_codecs_round_trip(added in leaf_points(6), removed in leaf_points(6), count in any::<i64>()) {
        let digest: Digest = digests(&added).iter().sum::<Digest>() - digests(&removed).iter().sum::<Digest>();
        prop_assert_eq!(Digest::from_row_bytes(&digest.to_row_bytes()), Ok(digest));
        prop_assert_eq!(Digest::from_wire_bytes(&digest.to_wire_bytes()), Ok(digest));
        for point in &added {
            prop_assert_eq!(LeafPoint::from_row_bytes(&point.to_row_bytes()), Ok(*point));
        }
        let mut wire: [u8; DIGEST_WIRE_LEN] = digest.to_wire_bytes();
        wire[32..].copy_from_slice(&count.to_be_bytes());
        let decoded = Digest::from_wire_bytes(&wire).expect("a canonical point and any count");
        prop_assert_eq!(decoded.count(), count);
        prop_assert_eq!(decoded.to_wire_bytes(), wire);
    }
}
