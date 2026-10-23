//! Packed, independently decodable inventories. Binary serde retains the validated
//! byte buffer; JSON keeps the readable list of keyed entity heads.

use ankurah_digest_tree::{CanonicalCodec, TreeError};
use bytes::Bytes;
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};

use crate::{Clock, EntityHead, EntityId, EventId, TreeSpan, TreeSpec};

mod codec;
use codec::{Reader, PLAIN, PLAIN_FIXED, PREFIX, PREFIX_FIXED};

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
        while let Some(leaf) = cursor.next() {
            CanonicalCodec::split(spec.key_spec(), leaf.key)?;
            address.clear();
            address.extend_from_slice(leaf.key);
            address.extend_from_slice(&leaf.entity.entity_id.to_bytes());
            if !self.bounds.contains(&address) || address <= previous {
                return Err(TreeError::InvalidKey("invalid inventory address order or bounds"));
            }
            std::mem::swap(&mut previous, &mut address);
        }
        Ok(())
    }

    /// Uncompressed logical size, including fixed-width lengths and generations.
    /// Compression must not bypass the comparison's decoded-work budget.
    pub fn expanded_size(&self) -> u64 { bincode::serialized_size(&self.bounds).expect("bounds serialize") + self.heads.expanded_size() }
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

// A smaller message budget and row count still apply to follow-up pages in core.
const MAX_BYTES: usize = 256 * 1024;
const MAX_ROWS: usize = 2048;

/// Immutable keyed heads in one shared byte buffer. Clones share the allocation.
/// Keys are plain or relative to the preceding key, whichever occupies fewer bytes.
#[derive(Clone)]
pub struct PackedHeads {
    bytes: Bytes,
    len: usize,
    expanded_size: u64,
    plain_size: usize,
}

impl std::fmt::Debug for PackedHeads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackedHeads").field("len", &self.len).field("encoded_bytes", &self.bytes.len()).finish()
    }
}

impl Default for PackedHeads {
    fn default() -> Self { Self::from_rows(std::iter::empty()) }
}

impl From<Vec<KeyedEntityHead>> for PackedHeads {
    fn from(heads: Vec<KeyedEntityHead>) -> Self {
        Self::from_rows(heads.iter().map(|leaf| (leaf.key.as_slice(), leaf.entity.entity_id, &leaf.entity.head)))
    }
}

impl PackedHeads {
    /// Encode borrowed source rows directly, without cloning their keys or clocks.
    /// The repeatable iterator is scanned for exact size before one buffer is built.
    pub fn from_rows<'a>(rows: impl Iterator<Item = (&'a [u8], EntityId, &'a Clock)> + Clone) -> Self {
        let mut count = 0;
        let mut plain = 0;
        let mut compressed = 0;
        let mut expanded_size = 8;
        let mut generations = 0;
        let mut tips = 0;
        let mut previous = &[][..];
        for (key, _, head) in rows.clone() {
            count += 1;
            let shared = codec::common_prefix(previous, key);
            let generation_size = head.entries().iter().map(|(g, _)| codec::uint_size(*g as usize)).sum::<usize>();
            generations += generation_size;
            tips += head.len();
            let clock_size = codec::uint_size(head.len()) + head.len() * 32 + generation_size;
            plain += codec::uint_size(key.len()) + key.len() + 32 + clock_size;
            compressed += codec::uint_size(shared) + codec::uint_size(key.len() - shared) + key.len() - shared + 32 + clock_size;
            expanded_size += 8 + key.len() as u64 + 32 + 8 + 36 * head.len() as u64;
            previous = key;
        }
        let fixed = generations > tips * 4;
        if fixed {
            plain -= generations - tips * 4;
            compressed -= generations - tips * 4;
        }
        let prefix = compressed < plain;
        let mut bytes = Vec::with_capacity(1 + codec::uint_size(count) + if prefix { compressed } else { plain });
        bytes.push(match (prefix, fixed) {
            (false, false) => PLAIN,
            (true, false) => PREFIX,
            (false, true) => PLAIN_FIXED,
            (true, true) => PREFIX_FIXED,
        });
        codec::put_uint(&mut bytes, count);
        previous = &[];
        for (key, id, head) in rows {
            let shared = if prefix { codec::common_prefix(previous, key) } else { 0 };
            if prefix {
                codec::put_uint(&mut bytes, shared);
            }
            codec::put_uint(&mut bytes, key.len() - shared);
            bytes.extend_from_slice(&key[shared..]);
            bytes.extend_from_slice(&id.to_bytes());
            codec::put_uint(&mut bytes, head.len());
            for (generation, id) in head.entries() {
                if fixed {
                    bytes.extend_from_slice(&generation.to_le_bytes());
                } else {
                    codec::put_uint(&mut bytes, *generation as usize);
                }
                bytes.extend_from_slice(id.as_bytes());
            }
            previous = key;
        }
        Self { bytes: bytes.into(), len: count, expanded_size, plain_size: 1 + codec::uint_size(count) + plain }
    }

    pub fn len(&self) -> usize { self.len }
    pub fn is_empty(&self) -> bool { self.len == 0 }
    pub fn expanded_size(&self) -> u64 { self.expanded_size }
    /// Key bytes saved by prefix coding. Counting these again bounds traversal
    /// work independently of the compressed wire size, including across pages.
    pub fn prefix_savings(&self) -> usize { self.plain_size.saturating_sub(self.bytes.len()) }

    /// Explicitly materialize owned rows when a caller needs independent values.
    pub fn to_vec(&self) -> Vec<KeyedEntityHead> {
        let mut rows = Vec::with_capacity(self.len);
        let mut cursor = self.cursor();
        while let Some(row) = cursor.next() {
            rows.push(row.to_owned());
        }
        rows
    }

    /// A lending cursor: a reconstructed key remains valid until the next advance.
    /// The cursor shares the packed buffer and can be retained across work batches.
    pub fn cursor(&self) -> HeadCursor {
        let mut reader = Reader::new(&self.bytes);
        let (prefix, fixed) = reader.mode().expect("constructed or validated inventory");
        let remaining = reader.uint().expect("constructed or validated inventory");
        HeadCursor { bytes: self.bytes.clone(), offset: reader.offset, remaining, prefix, fixed, key: Vec::new() }
    }

    fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        let (len, expanded_size, plain_size, canonical) = codec::validate(bytes)?;
        if canonical {
            return Ok(Self { bytes: Bytes::copy_from_slice(bytes), len, expanded_size, plain_size });
        }
        // Rare noncanonical clocks retain Clock's sorting/deduplication semantics.
        // Validate the entire bounded payload before normalizing it once, so all
        // later cursors see canonical clocks and never allocate per-head clocks.
        let mut reader = Reader::new(bytes);
        let (prefix, fixed) = reader.mode()?;
        reader.uint()?;
        let mut key = Vec::new();
        let mut rows = Vec::with_capacity(len);
        for _ in 0..len {
            rows.push(reader.row(prefix, fixed, &mut key)?.to_owned());
        }
        Ok(rows.into())
    }
}

/// Sequential traversal with one reusable key buffer; no per-head allocations.
/// This intentionally is not Iterator: the next key may overwrite the current view.
pub struct HeadCursor {
    bytes: Bytes,
    offset: usize,
    remaining: usize,
    prefix: bool,
    fixed: bool,
    key: Vec<u8>,
}

impl HeadCursor {
    pub fn len(&self) -> usize { self.remaining }
    pub fn is_empty(&self) -> bool { self.remaining == 0 }
    #[allow(clippy::should_implement_trait)] // Lending result borrows the cursor's reusable key.
    pub fn next(&mut self) -> Option<KeyedEntityHeadRef<'_>> {
        if self.remaining == 0 {
            return None;
        }
        let mut reader = Reader { bytes: &self.bytes, offset: self.offset };
        let row = reader.row(self.prefix, self.fixed, &mut self.key).expect("constructed or validated inventory");
        self.offset = reader.offset;
        self.remaining -= 1;
        Some(row)
    }

    /// Retain a clock for later repair selection without allocating its entries.
    /// The returned slice shares this inventory's backing buffer.
    pub fn next_clock(&mut self) -> Option<(EntityId, InventoryClock)> {
        let bytes = self.bytes.clone();
        let row = self.next()?;
        Some((
            row.entity.entity_id,
            InventoryClock { bytes: bytes.slice_ref(row.entity.head.bytes), count: row.entity.head.count, fixed: row.entity.head.fixed },
        ))
    }
}

/// A captured inventory clock whose bytes can outlive the traversal cursor.
#[derive(Debug, Clone)]
pub struct InventoryClock {
    bytes: Bytes,
    count: usize,
    fixed: bool,
}

impl InventoryClock {
    pub fn to_owned(&self) -> Clock { ClockRef { bytes: &self.bytes, count: self.count, fixed: self.fixed }.to_owned() }
}

#[derive(Debug, Serialize)]
pub struct KeyedEntityHeadRef<'a> {
    pub key: &'a [u8],
    pub entity: EntityHeadRef<'a>,
}

impl KeyedEntityHeadRef<'_> {
    pub fn to_owned(&self) -> KeyedEntityHead { KeyedEntityHead { key: self.key.to_vec(), entity: self.entity.to_owned() } }
}

#[derive(Debug, Serialize)]
pub struct EntityHeadRef<'a> {
    pub entity_id: EntityId,
    pub head: ClockRef<'a>,
}

impl EntityHeadRef<'_> {
    pub fn to_owned(&self) -> EntityHead { EntityHead { entity_id: self.entity_id, head: self.head.to_owned() } }
}

#[derive(Debug, Clone, Copy)]
pub struct ClockRef<'a> {
    bytes: &'a [u8],
    count: usize,
    fixed: bool,
}

impl ClockRef<'_> {
    pub fn len(&self) -> usize { self.count }
    pub fn is_empty(&self) -> bool { self.count == 0 }
    pub fn entries(&self) -> impl Iterator<Item = (u32, EventId)> + '_ {
        let mut reader = Reader::new(self.bytes);
        (0..self.count).map(move |_| {
            let generation = reader.generation(self.fixed).expect("validated generation");
            let id = reader.take(32).expect("validated event id");
            (generation, EventId::from_bytes(id.try_into().unwrap()))
        })
    }
    pub fn to_owned(&self) -> Clock { Clock::new(self.entries().collect::<Vec<_>>()).expect("validated clock") }
    pub fn equals(&self, clock: &Clock) -> bool {
        self.count == clock.len()
            && self.entries().zip(clock.entries()).all(|((g, id), (other_g, other_id))| g == *other_g && id == *other_id)
    }
}

impl Serialize for ClockRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.count))?;
        for entry in self.entries() {
            seq.serialize_element(&entry)?;
        }
        seq.end()
    }
}

impl Serialize for PackedHeads {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.bytes.len() > MAX_BYTES || self.plain_size > MAX_BYTES || self.len > MAX_ROWS {
            return Err(serde::ser::Error::custom("inventory exceeds codec limits"));
        }
        if serializer.is_human_readable() {
            let mut seq = serializer.serialize_seq(Some(self.len))?;
            let mut cursor = self.cursor();
            while let Some(row) = cursor.next() {
                seq.serialize_element(&row)?;
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
            let rows = Vec::<KeyedEntityHead>::deserialize(deserializer)?;
            let packed = Self::from(rows);
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
