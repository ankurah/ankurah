//! Packed, independently decodable inventories. Binary serde retains the validated
//! byte buffer; JSON keeps the readable list of keyed entity heads.

use ankurah_digest_tree::{CanonicalCodec, TreeError};
use bytes::Bytes;
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS},
    Clock, EntityId, TreeSpan, TreeSpec,
};

mod codec;
mod cursor;
pub use cursor::{ClockRef, EntityHeadRef, HeadCursor, InventoryClock, KeyedEntityHeadRef};

/// All keyed entity heads within exactly `bounds`, including empty gaps.
///
/// A large requested span can be split into disjoint inventories in address order.
/// Each inventory completely describes its bounds; the remaining bounds stay pending.
/// This is a wire inventory, separate from the tree crate's local scan page and cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeSpanInventory {
    pub bounds: TreeSpan,
    /// Strictly increasing by canonical key bytes followed by entity ID.
    pub heads: PackedHeads,
}

impl TreeSpanInventory {
    /// Check canonical keys, nonempty bounds and strictly increasing addresses
    /// within those bounds. Reads borrowed heads without rebuilding their clocks.
    /// Message budgets and correspondence to a requested span belong to the caller.
    pub fn validate(&self, spec: &TreeSpec) -> Result<(), TreeError> {
        if self.bounds.is_empty() {
            return Err(TreeError::InvalidKey("empty inventory bounds"));
        }
        let mut previous = Vec::new();
        let mut address = Vec::new();
        let mut cursor = self.heads.cursor();
        while let Some(head) = cursor.next() {
            CanonicalCodec.split(spec.key_spec(), head.key)?;
            address.clear();
            address.extend_from_slice(head.key);
            address.extend_from_slice(&head.entity.entity_id.to_bytes());
            if !self.bounds.contains(&address) || address <= previous {
                return Err(TreeError::InvalidKey("invalid inventory address order or bounds"));
            }
            std::mem::swap(&mut previous, &mut address);
        }
        Ok(())
    }
}

/// An entity's identity and causal frontier.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityHead {
    pub entity_id: EntityId,
    pub head: Clock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyedEntityHead {
    /// Canonical property values selected by the TreeSpec's KeySpec. The full
    /// address appends the entity ID; this key is empty for entity-ID-only trees.
    /// It belongs to this tree: another KeySpec can place the same EntityHead
    /// under a different key.
    pub key: Vec<u8>,
    /// The actual clock avoids another round trip when constructing an EventBridge.
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
    fn default() -> Self { Self::encode(std::iter::empty()) }
}

impl From<Vec<KeyedEntityHead>> for PackedHeads {
    fn from(heads: Vec<KeyedEntityHead>) -> Self {
        Self::encode(heads.iter().map(|head| (head.key.as_slice(), head.entity.entity_id, &head.entity.head)))
    }
}

impl PackedHeads {
    /// Encode borrowed keyed entity heads directly, without cloning their keys or clocks.
    /// The repeatable iterator is scanned for exact size before one buffer is built.
    pub fn encode<'a>(heads: impl Iterator<Item = (&'a [u8], EntityId, &'a Clock)> + Clone) -> Self { codec::write(heads) }

    pub fn len(&self) -> usize { self.len }
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// The bytes these heads would occupy with every key written whole: the work
    /// they describe, whatever prefix sharing saved on the wire.
    pub fn unshared_bytes(&self) -> usize { self.unshared_bytes }

    /// The bytes prefix sharing saved on the wire.
    pub(crate) fn shared_prefix_bytes(&self) -> u64 { (self.unshared_bytes - self.bytes.len()) as u64 }

    /// The work one head adds to an inventory, for filling a page head by head.
    pub fn head_work_bytes(key_len: usize, head: &Clock) -> usize { codec::plain_head_size(key_len, head) }

    /// Explicitly materialize owned heads when a caller needs independent values.
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

    fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        let validated = codec::validate(bytes)?;
        if validated.canonical {
            return Ok(Self { bytes: Bytes::copy_from_slice(bytes), len: validated.heads, unshared_bytes: validated.unshared_bytes });
        }
        // Rare noncanonical clocks retain Clock's sorting/deduplication semantics.
        // Validate the entire bounded payload before normalizing it once, so all
        // later cursors see canonical clocks and never allocate per-head clocks.
        let mut reader = codec::Reader::new(bytes);
        let encoding = reader.key_encoding()?;
        reader.uint()?;
        let mut key = Vec::new();
        let mut heads = Vec::with_capacity(validated.heads);
        for _ in 0..validated.heads {
            heads.push(KeyedEntityHeadRef::from_encoded(reader.head(encoding, &mut key)?).to_owned());
        }
        Ok(heads.into())
    }
}

impl Serialize for PackedHeads {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.bytes.len() > MAX_OFFER_BYTES || self.unshared_bytes > MAX_OFFER_BYTES || self.len > MAX_OFFER_HEADS {
            return Err(serde::ser::Error::custom("inventory exceeds codec limits"));
        }
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
            let packed = Self::from(heads);
            codec::validate(&packed.bytes).map_err(serde::de::Error::custom)?;
            Ok(packed)
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
