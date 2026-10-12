//! The rows a tree keeps: one leaf row per leaf, filed by leaf address, and
//! one tree node row per stored prefix.
//!
//! Rows carry typed values: the head hash, the leaf element and the digest as
//! the [`digest`](crate::digest) module defines them, in the group the tree
//! sums in (ristretto255 unless the tree names another). The maps hold their
//! bytes in the layouts of [`layout`](crate::layout), which the tree encodes
//! and decodes, so the maps hold bytes and nothing else.

use std::ops::Bound;

use ankurah_core_types::EntityId;
use serde::{Deserialize, Serialize};

use crate::digest::{Digest, DigestGroup, HeadHash, LeafElement, Ristretto255};

use crate::map::ByteBounds;
use crate::prefix::NodePrefix;

/// The tree's record of one leaf, so that the next change to the entity
/// knows what to subtract: the inputs of the leaf and its element.
///
/// Addition counts, so a tree is correct only if every change applies to it
/// exactly once, with the old leaf subtracted and the new one added. The
/// stored element saves hashing the old leaf again, and a change replaces the
/// row only if it still holds what the change read, so a change applied twice
/// finds the row already replaced and is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafRow<G: DigestGroup = Ristretto255> {
    /// The canonical key the leaf is filed under: empty where the entity id
    /// alone is the key.
    pub key: Vec<u8>,
    /// The entity the leaf names.
    pub entity_id: EntityId,
    /// The hash of the entity's head, which the leaf's element was hashed
    /// from.
    pub head: HeadHash,
    /// The leaf's element, which the digests along its path hold.
    pub element: LeafElement<G>,
}

impl<G: DigestGroup> LeafRow<G> {
    /// Where the row is filed: see [`leaf_address`].
    pub fn address(&self) -> Vec<u8> { leaf_address(&self.key, self.entity_id) }
}

/// A tree node's summary of the leaves beneath it, so that one comparison can
/// stand for them all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRow<G: DigestGroup = Ristretto255> {
    /// The sum of the leaf elements beneath the tree node, with their count.
    pub digest: Digest<G>,
}

/// Where a tree files a leaf: the canonical key followed by the entity id.
/// Leaf rows are ordered by it, and a tree node's prefix is a run of its
/// leading bits.
///
/// [`CanonicalCodec`](crate::CanonicalCodec) makes complete keys prefix-free
/// for a given spec, including strings containing NUL. Thus each key's leaves
/// occupy a contiguous interval and tuple bounds extend to address bounds
/// without admitting rows of a neighboring key.
pub fn leaf_address(key: &[u8], entity_id: EntityId) -> Vec<u8> {
    let mut address = Vec::with_capacity(key.len() + EntityId::BYTE_LEN);
    address.extend_from_slice(key);
    address.extend_from_slice(&entity_id.to_bytes());
    address
}

/// A range of leaf addresses: the rows a read asks for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddressRange {
    /// The lower bound.
    pub start: Bound<Vec<u8>>,
    /// The upper bound.
    pub end: Bound<Vec<u8>>,
}

impl AddressRange {
    /// The range between two bounds.
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self { Self { start, end } }

    /// Every address.
    pub fn all() -> Self { Self::new(Bound::Unbounded, Bound::Unbounded) }

    /// Every address beneath `prefix`: the leaves of a bucket, or of any tree
    /// node.
    pub fn under(prefix: &NodePrefix) -> Self {
        let (start, end) = prefix.address_bounds();
        Self::new(Bound::Included(start), end.map_or(Bound::Unbounded, Bound::Excluded))
    }

    /// The rest of this range after `address`, to read the next page.
    pub fn after(&self, address: &[u8]) -> Self { Self::new(Bound::Excluded(address.to_vec()), self.end.clone()) }

    /// The bounds as a range scan takes them.
    pub fn bounds(&self) -> ByteBounds<'_> { (self.start.as_ref().map(Vec::as_slice), self.end.as_ref().map(Vec::as_slice)) }

    /// Whether `address` lies within the range.
    pub fn contains(&self, address: &[u8]) -> bool {
        let above_start = match &self.start {
            Bound::Included(start) => address >= start.as_slice(),
            Bound::Excluded(start) => address > start.as_slice(),
            Bound::Unbounded => true,
        };
        let below_end = match &self.end {
            Bound::Included(end) => address <= end.as_slice(),
            Bound::Excluded(end) => address < end.as_slice(),
            Bound::Unbounded => true,
        };
        above_start && below_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    fn entity(n: u8) -> EntityId { EntityId::from_bytes([n; 32]) }

    #[test]
    fn an_address_is_the_key_then_the_entity_id() {
        assert_eq!(leaf_address(b"blue\0", entity(9)), [&b"blue\0"[..], &[9; 32]].concat());
        assert_eq!(leaf_address(b"", entity(9)), [9; 32], "the entity-id key is empty");
        let head = HeadHash::of([[1; 32]]);
        let row: LeafRow = LeafRow {
            key: b"blue\0".to_vec(),
            entity_id: entity(9),
            head,
            element: crate::digest::Leaf::new(b"blue\0", entity(9), head).element(),
        };
        assert_eq!(row.address(), leaf_address(b"blue\0", entity(9)));
    }

    #[test]
    fn contains_honours_each_kind_of_bound() {
        let included = AddressRange::new(Bound::Included(vec![2]), Bound::Included(vec![4]));
        let excluded = AddressRange::new(Bound::Excluded(vec![2]), Bound::Excluded(vec![4]));
        for address in [vec![1], vec![2], vec![3], vec![4], vec![4, 0], vec![5]] {
            let inside = address >= vec![2] && address <= vec![4];
            assert_eq!(included.contains(&address), inside, "{address:?} within the closed range");
            let inside = address > vec![2] && address < vec![4];
            assert_eq!(excluded.contains(&address), inside, "{address:?} within the open range");
            assert!(AddressRange::all().contains(&address));
        }
    }

    #[test]
    fn the_next_page_starts_past_the_last_address_read_and_keeps_the_end() {
        let range = AddressRange::new(Bound::Included(vec![1]), Bound::Excluded(vec![9]));
        let next = range.after(&[3]);
        assert_eq!(next, AddressRange::new(Bound::Excluded(vec![3]), Bound::Excluded(vec![9])));
        assert_eq!(next.bounds(), (Bound::Excluded(&[3][..]), Bound::Excluded(&[9][..])));
        assert!(!next.contains(&[3]) && next.contains(&[3, 0]) && !next.contains(&[9]));
    }

    /// The range under a prefix holds exactly the addresses that begin with
    /// its bits, judged by the prefix itself.
    #[test]
    fn the_range_under_a_prefix_holds_exactly_the_addresses_it_contains() {
        let mut rng = SmallRng::seed_from_u64(17);
        for _ in 0..200 {
            let len = rng.gen_range(0..=40);
            let from: Vec<u8> = (0..5).map(|_| rng.gen::<u8>() & 0xC3).collect();
            let prefix = NodePrefix::of(&from, len);
            let range = AddressRange::under(&prefix);
            for _ in 0..50 {
                let address: Vec<u8> = (0..rng.gen_range(0..=6)).map(|_| rng.gen::<u8>() & 0xC3).collect();
                assert_eq!(range.contains(&address), prefix.contains_address(&address), "{prefix:?} against {address:?}");
            }
        }
        assert_eq!(AddressRange::under(&NodePrefix::root()), AddressRange::new(Bound::Included(vec![]), Bound::Unbounded));
    }
}
