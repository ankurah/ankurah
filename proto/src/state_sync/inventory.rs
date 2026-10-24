//! Inventories of the entity heads within a span, packed into one byte buffer that a
//! peer validates once and then reads without copying. Binary encodings carry the
//! buffer itself; human-readable ones list the keyed entity heads.

use std::collections::HashSet;

use ankurah_digest_tree::{CanonicalCodec, TreeError};
use bytes::Bytes;
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};

use crate::{Clock, EntityId, TreeSpan, TreeSpec};

mod codec;
mod cursor;
pub use codec::InventoryError;
pub use cursor::{ClockRef, EntityHeadRef, HeadCursor, KeyedEntityHeadRef};

/// Every keyed entity head within exactly `bounds`, so a gap between heads is known
/// to be empty.
///
/// A responder splits a large span into inventories in address order; each describes
/// its own bounds completely, and the rest of the span stays outstanding. Unlike
/// digest-tree's `LeafPage` and `LeafCursor`, which page through a local tree, an
/// inventory travels between peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeSpanInventory {
    pub bounds: TreeSpan,
    /// Strictly increasing by canonical key bytes followed by entity ID.
    pub heads: PackedHeads,
}

impl TreeSpanInventory {
    /// Checks canonical keys, nonempty bounds, strictly increasing addresses within
    /// those bounds, and each entity at most once. Reads borrowed heads without
    /// rebuilding their clocks. Message limits and correspondence to a requested
    /// span belong to the caller.
    pub fn validate(&self, spec: &TreeSpec) -> Result<(), InventoryError> {
        if self.bounds.is_empty() {
            return Err(InventoryError::EmptyBounds);
        }
        let mut previous = Vec::new();
        let mut address = Vec::new();
        let mut entities = HashSet::with_capacity(self.heads.len());
        let mut cursor = self.heads.cursor();
        while let Some(head) = cursor.next() {
            check_key(spec, head.key)?;
            if !entities.insert(head.entity.entity_id) {
                return Err(InventoryError::DuplicateEntity(head.entity.entity_id));
            }
            address.clear();
            address.extend_from_slice(head.key);
            address.extend_from_slice(&head.entity.entity_id.to_bytes());
            if !self.bounds.contains(&address) {
                return Err(InventoryError::OutsideBounds);
            }
            if address <= previous {
                return Err(InventoryError::Unordered);
            }
            std::mem::swap(&mut previous, &mut address);
        }
        Ok(())
    }
}

/// Checks that `key` is canonical under the spec's key parts.
pub(crate) fn check_key(spec: &TreeSpec, key: &[u8]) -> Result<(), TreeError> { CanonicalCodec::check(spec.key_spec(), key) }

/// An entity's identity and causal frontier.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityHead {
    pub entity_id: EntityId,
    pub head: Clock,
}

/// One entity head at its address in a tree: its key under the tree's key spec, and
/// the entity's identity and head.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyedEntityHead {
    /// Canonical property values selected by the TreeSpec's KeySpec. The full
    /// address appends the entity ID; this key is empty for entity-ID-only trees.
    /// It belongs to this tree: another KeySpec can place the same EntityHead
    /// under a different key.
    pub key: Vec<u8>,
    /// The entity's whole head, so a responder can build an event bridge from it
    /// without another round trip.
    pub entity: EntityHead,
}

/// Immutable keyed heads in one shared byte buffer. Clones share the allocation.
/// Keys are plain or relative to the preceding key, whichever occupies fewer bytes.
#[derive(Clone)]
pub struct PackedHeads {
    bytes: Bytes,
    len: usize,
    unshared_bytes: usize,
}

impl std::fmt::Debug for PackedHeads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackedHeads").field("len", &self.len).field("encoded_bytes", &self.bytes.len()).finish()
    }
}

impl Default for PackedHeads {
    fn default() -> Self { Self::encode(std::iter::empty()).expect("an empty inventory is within every limit") }
}

impl TryFrom<Vec<KeyedEntityHead>> for PackedHeads {
    type Error = InventoryError;
    fn try_from(heads: Vec<KeyedEntityHead>) -> Result<Self, InventoryError> {
        Self::encode(heads.iter().map(|head| (head.key.as_slice(), head.entity.entity_id, &head.entity.head)))
    }
}

impl PackedHeads {
    /// Encode borrowed keyed entity heads directly, without cloning their keys or clocks.
    /// Refuses more, or larger, heads than one inventory may carry.
    pub fn encode<'a>(heads: impl Iterator<Item = (&'a [u8], EntityId, &'a Clock)> + Clone) -> Result<Self, InventoryError> {
        codec::write(heads)
    }

    pub fn len(&self) -> usize { self.len }
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// The bytes these heads would occupy with every key written whole: the work
    /// they describe, whatever prefix sharing saved on the wire.
    pub fn unshared_bytes(&self) -> usize { self.unshared_bytes }

    /// The bytes prefix sharing saved on the wire.
    pub(crate) fn shared_prefix_bytes(&self) -> u64 { (self.unshared_bytes - self.bytes.len()) as u64 }

    /// The work one head adds to an inventory, for filling a page head by head.
    pub fn head_work_bytes(key_len: usize, head: &Clock) -> usize { codec::plain_head_size(key_len, head) }

    /// Materialize owned heads, the inverse of encoding them.
    pub fn to_vec(&self) -> Vec<KeyedEntityHead> {
        let mut heads = Vec::with_capacity(self.len);
        let mut cursor = self.cursor();
        while let Some(head) = cursor.next() {
            heads.push(head.to_owned());
        }
        heads
    }

    /// A lending cursor: a reconstructed key remains valid until the next advance.
    /// The cursor shares the packed buffer and can be retained across work batches.
    pub fn cursor(&self) -> HeadCursor { HeadCursor::new(self.bytes.clone()) }

    fn decode(bytes: &[u8]) -> Result<Self, InventoryError> {
        let validated = codec::validate(bytes)?;
        Ok(Self { bytes: Bytes::copy_from_slice(bytes), len: validated.heads, unshared_bytes: validated.unshared_bytes })
    }
}

impl Serialize for PackedHeads {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            let mut seq = serializer.serialize_seq(Some(self.len))?;
            let mut cursor = self.cursor();
            while let Some(head) = cursor.next() {
                seq.serialize_element(&head)?;
            }
            seq.end()
        } else {
            serializer.serialize_bytes(&self.bytes)
        }
    }
}

impl<'de> Deserialize<'de> for PackedHeads {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let heads = Vec::<KeyedEntityHead>::deserialize(deserializer)?;
            Self::try_from(heads).map_err(serde::de::Error::custom)
        } else {
            deserializer.deserialize_bytes(HeadsVisitor)
        }
    }
}

struct HeadsVisitor;
impl Visitor<'_> for HeadsVisitor {
    type Value = PackedHeads;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("a bounded packed entity-head inventory") }
    fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> { PackedHeads::decode(bytes).map_err(E::custom) }
}

#[cfg(test)]
mod tests;
