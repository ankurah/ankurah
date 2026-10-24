//! Where a tree files each leaf, and how runs of leaves are named: the
//! canonical key codec, the leaf address, the prefixes that name tree nodes,
//! and the spans of leaf addresses.

mod key;
mod prefix;
pub(crate) mod span;

use ankurah_core_types::EntityId;

pub use key::CanonicalCodec;
pub(crate) use key::{encode_part, validate_part};
pub use prefix::{NodePrefix, NodePrefixError};
pub use span::{TreeSpan, TreeSpanDigest};

/// Where a tree files a leaf: the canonical key followed by the entity id.
/// Leaf rows are ordered by it, and a tree node's prefix is a run of its
/// leading bits.
///
/// [`CanonicalCodec`] makes complete keys prefix-free for a given spec,
/// strings containing NUL included, so each key's leaves occupy a contiguous
/// interval, and tuple bounds extend to address bounds without admitting rows
/// of a neighboring key.
pub fn leaf_address(key: &[u8], entity_id: EntityId) -> Vec<u8> {
    let mut address = Vec::with_capacity(key.len() + EntityId::BYTE_LEN);
    address.extend_from_slice(key);
    address.extend_from_slice(&entity_id.to_bytes());
    address
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::HeadHash;
    use crate::store::LeafRow;

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
}
