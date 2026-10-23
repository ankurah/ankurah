//! Payload: encoding tag, head count, then key, entity ID and clock for each head.
//! Counts, lengths and clock generations use unsigned little-endian bincode varints.
//! The prefix encoding adds the shared-byte count before each key suffix. IDs are
//! always 32 bytes.

use ankurah_digest_tree::TreeError;

use super::PackedHeads;
use crate::{
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS},
    Clock, EntityId, EventId,
};

pub(super) const PLAIN: u8 = 1;
pub(super) const PREFIX: u8 = 2;
type Result<T> = std::result::Result<T, InventoryError>;

/// Why a packed inventory, or an inventory's heads within its bounds, is invalid.
#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    /// The bytes are not a well-formed packed inventory.
    #[error("malformed packed inventory: {0}")]
    Malformed(&'static str),
    /// The heads are more, or larger, than one inventory may carry.
    #[error("packed inventory exceeds its {0}")]
    ExceedsLimit(&'static str),
    /// The inventory's bounds contain no address.
    #[error("empty inventory bounds")]
    EmptyBounds,
    /// A key is not canonical under the tree's key spec.
    #[error("inventory key is not canonical: {0}")]
    InvalidKey(#[from] TreeError),
    /// A head's address lies outside the inventory's bounds.
    #[error("inventory head outside its bounds")]
    OutsideBounds,
    /// A head's address does not follow the preceding head's.
    #[error("inventory heads out of address order")]
    Unordered,
}

/// How the payload writes each key: whole, or as a suffix of the preceding key.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyEncoding {
    Plain,
    Prefix,
}

pub(super) fn uint_size(value: usize) -> usize {
    match value {
        0..=250 => 1,
        251..=65535 => 3,
        65536..=4294967295 => 5,
        _ => 9,
    }
}

pub(super) fn put_uint(out: &mut Vec<u8>, value: usize) {
    match uint_size(value) {
        1 => out.push(value as u8),
        3 => {
            out.push(251);
            out.extend_from_slice(&(value as u16).to_le_bytes());
        }
        5 => {
            out.push(252);
            out.extend_from_slice(&(value as u32).to_le_bytes());
        }
        _ => {
            out.push(253);
            out.extend_from_slice(&(value as u64).to_le_bytes());
        }
    }
}

pub(super) fn common_prefix(a: &[u8], b: &[u8]) -> usize { a.iter().zip(b).take_while(|(a, b)| a == b).count() }

/// The bytes one clock occupies: its tip count, then each tip's generation and event ID.
fn clock_size(head: &Clock) -> usize {
    uint_size(head.len()) + head.entries().iter().map(|(g, _)| uint_size(*g as usize) + 32).sum::<usize>()
}

/// The bytes one head occupies with its key written whole.
pub(super) fn plain_head_size(key_len: usize, head: &Clock) -> usize { uint_size(key_len) + key_len + 32 + clock_size(head) }

/// Encode borrowed keyed entity heads directly, without cloning their keys or clocks.
/// The repeatable iterator is scanned for exact size, and checked against one
/// inventory's limits, before one buffer is built.
pub(super) fn write<'a>(heads: impl Iterator<Item = (&'a [u8], EntityId, &'a Clock)> + Clone) -> Result<PackedHeads> {
    let mut count = 0;
    let mut plain = 0;
    let mut compressed = 0;
    let mut previous = &[][..];
    for (key, _, head) in heads.clone() {
        count += 1;
        let shared = common_prefix(previous, key);
        plain += plain_head_size(key.len(), head);
        compressed += uint_size(shared) + uint_size(key.len() - shared) + key.len() - shared + 32 + clock_size(head);
        previous = key;
    }
    let unshared_bytes = 1 + uint_size(count) + plain;
    if count > MAX_OFFER_HEADS {
        return Err(InventoryError::ExceedsLimit("head limit"));
    }
    if unshared_bytes > MAX_OFFER_BYTES {
        return Err(InventoryError::ExceedsLimit("byte limit"));
    }
    let encoding = if compressed < plain { KeyEncoding::Prefix } else { KeyEncoding::Plain };
    let mut bytes = Vec::with_capacity(1 + uint_size(count) + if encoding == KeyEncoding::Prefix { compressed } else { plain });
    bytes.push(match encoding {
        KeyEncoding::Plain => PLAIN,
        KeyEncoding::Prefix => PREFIX,
    });
    put_uint(&mut bytes, count);
    previous = &[];
    for (key, id, head) in heads {
        let shared = match encoding {
            KeyEncoding::Plain => 0,
            KeyEncoding::Prefix => {
                let shared = common_prefix(previous, key);
                put_uint(&mut bytes, shared);
                shared
            }
        };
        put_uint(&mut bytes, key.len() - shared);
        bytes.extend_from_slice(&key[shared..]);
        bytes.extend_from_slice(&id.to_bytes());
        put_uint(&mut bytes, head.len());
        for (generation, id) in head.entries() {
            put_uint(&mut bytes, *generation as usize);
            bytes.extend_from_slice(id.as_bytes());
        }
        previous = key;
    }
    Ok(PackedHeads { bytes: bytes.into(), len: count, unshared_bytes })
}

/// One keyed entity head as it lies in the payload: its reconstructed key, its
/// entity, and the bytes and tip count of its clock.
pub(super) struct EncodedHead<'k, 'a> {
    pub key: &'k [u8],
    pub entity_id: EntityId,
    pub clock: &'a [u8],
    pub tips: usize,
}

pub(super) struct Reader<'a> {
    pub bytes: &'a [u8],
    pub offset: usize,
}
impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self { Self { bytes, offset: 0 } }
    pub fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or(InventoryError::Malformed("length overflow"))?;
        let bytes = self.bytes.get(self.offset..end).ok_or(InventoryError::Malformed("truncated inventory"))?;
        self.offset = end;
        Ok(bytes)
    }
    pub fn uint(&mut self) -> Result<usize> {
        let value = match self.take(1)?[0] {
            n @ 0..=250 => n as u64,
            251 => u16::from_le_bytes(self.take(2)?.try_into().unwrap()) as u64,
            252 => u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as u64,
            253 => u64::from_le_bytes(self.take(8)?.try_into().unwrap()),
            _ => return Err(InventoryError::Malformed("invalid inventory integer")),
        };
        usize::try_from(value).map_err(|_| InventoryError::Malformed("inventory integer overflow"))
    }
    pub fn generation(&mut self) -> Result<u32> {
        let value = u32::try_from(self.uint()?).map_err(|_| InventoryError::Malformed("invalid clock generation"))?;
        if value == 0 {
            return Err(InventoryError::Malformed("invalid clock generation"));
        }
        Ok(value)
    }
    pub fn key_encoding(&mut self) -> Result<KeyEncoding> {
        match self.take(1)?[0] {
            PLAIN => Ok(KeyEncoding::Plain),
            PREFIX => Ok(KeyEncoding::Prefix),
            _ => Err(InventoryError::Malformed("unsupported inventory encoding")),
        }
    }
    pub fn head<'k>(&mut self, encoding: KeyEncoding, key: &'k mut Vec<u8>) -> Result<EncodedHead<'k, 'a>>
    where 'a: 'k {
        let shared = match encoding {
            KeyEncoding::Plain => 0,
            KeyEncoding::Prefix => self.uint()?,
        };
        if shared > key.len() {
            return Err(InventoryError::Malformed("prefix exceeds preceding key"));
        }
        let length = self.uint()?;
        let suffix = self.take(length)?;
        shared.checked_add(length).ok_or(InventoryError::Malformed("key length overflow"))?;
        let full_key = match encoding {
            KeyEncoding::Plain => suffix,
            KeyEncoding::Prefix => {
                key.truncate(shared);
                key.extend_from_slice(suffix);
                key.as_slice()
            }
        };
        let entity_id = EntityId::from_bytes(self.take(32)?.try_into().unwrap());
        let tips = self.uint()?;
        if tips > (self.bytes.len() - self.offset) / 33 {
            return Err(InventoryError::Malformed("truncated clock"));
        }
        let start = self.offset;
        for _ in 0..tips {
            self.generation()?;
            self.take(32)?;
        }
        Ok(EncodedHead { key: full_key, entity_id, clock: &self.bytes[start..self.offset], tips })
    }
}

/// The (generation, event) tips of a clock that `Reader::head` already validated.
pub(super) fn tips(clock: &[u8], count: usize) -> impl Iterator<Item = (u32, EventId)> + '_ {
    let mut reader = Reader::new(clock);
    (0..count).map(move |_| {
        let generation = reader.generation().expect("validated generation");
        let id = reader.take(32).expect("validated event id");
        (generation, EventId::from_bytes(id.try_into().unwrap()))
    })
}

/// What validating a payload learned about it.
pub(super) struct Validated {
    pub heads: usize,
    pub unshared_bytes: usize,
}

/// Check all fields before retaining or exposing a payload. Cap the unshared size
/// as well as the wire bytes, so repeated prefixes cannot amplify work. Each clock
/// must list its tips in ascending event order, as the writer does.
pub(super) fn validate(bytes: &[u8]) -> Result<Validated> {
    if bytes.len() > MAX_OFFER_BYTES {
        return Err(InventoryError::ExceedsLimit("byte limit"));
    }
    let mut reader = Reader::new(bytes);
    let encoding = reader.key_encoding()?;
    let heads = reader.uint()?;
    if heads > MAX_OFFER_HEADS {
        return Err(InventoryError::ExceedsLimit("head limit"));
    }
    let mut unshared_bytes = 1 + uint_size(heads);
    let mut key = Vec::new();
    for _ in 0..heads {
        let head = reader.head(encoding, &mut key)?;
        unshared_bytes += uint_size(head.key.len()) + head.key.len() + 32 + uint_size(head.tips) + head.clock.len();
        if unshared_bytes > MAX_OFFER_BYTES {
            return Err(InventoryError::ExceedsLimit("byte limit"));
        }
        if !tips(head.clock, head.tips).map(|(_, id)| id).is_sorted_by(|a, b| a < b) {
            return Err(InventoryError::Malformed("clock tips out of ascending event order"));
        }
    }
    if reader.offset != bytes.len() {
        return Err(InventoryError::Malformed("trailing inventory bytes"));
    }
    Ok(Validated { heads, unshared_bytes })
}
