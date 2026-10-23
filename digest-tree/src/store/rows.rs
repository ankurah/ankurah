//! The rows a tree keeps: one leaf row per leaf, filed by leaf address, and
//! one tree node row per stored prefix.
//!
//! Rows carry typed values: the head hash, the leaf element and the digest as
//! the [`digest`](crate::digest) module defines them, in the group the tree
//! sums in (ristretto255 unless the tree names another). The maps hold their
//! bytes in the layouts of the `layout` module, which the tree encodes
//! and decodes, so the maps hold bytes and nothing else.

use ankurah_core_types::EntityId;

use super::layout;
use crate::address::leaf_address;
use crate::digest::{DigestGroup, HeadHash, LeafElement, Ristretto255};
use crate::error::TreeError;

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

/// A leaf row with the address it is filed at and the bytes of its value,
/// each made once: the bytes a write stores, or the bytes a read found, which
/// a conditional write expects to find unchanged. The bytes are only ever
/// made from the row or the row from the bytes, so the two agree.
pub(crate) struct EncodedLeaf<G: DigestGroup> {
    address: Vec<u8>,
    row: LeafRow<G>,
    value: Vec<u8>,
}

impl<G: DigestGroup> EncodedLeaf<G> {
    /// `row`, filed at `address`, its leaf address, with its value encoded.
    pub(crate) fn new(address: Vec<u8>, row: LeafRow<G>) -> Self {
        debug_assert_eq!(address, row.address(), "a leaf is filed at its own address");
        let value = layout::leaf_value(&row);
        Self { address, row, value }
    }

    /// The leaf row stored at `address` as `value`.
    pub(crate) fn decode(address: Vec<u8>, value: Vec<u8>) -> Result<Self, TreeError> {
        let row = layout::leaf_row(&address, &value)?;
        Ok(Self { address, row, value })
    }

    pub(crate) fn address(&self) -> &[u8] { &self.address }

    pub(crate) fn row(&self) -> &LeafRow<G> { &self.row }

    pub(crate) fn value(&self) -> &[u8] { &self.value }
}
