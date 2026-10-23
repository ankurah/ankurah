//! Payload: encoding tag, head count, then key, entity ID and clock for each head.
//! Counts/lengths use unsigned little-endian bincode varints. Prefix modes add
//! the shared-byte count before each key suffix. Fixed modes store generations
//! as four little-endian bytes; other modes use varints. IDs are always 32 bytes.

use super::PackedHeads;
use crate::{
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS},
    Clock, EntityId, EventId,
};

pub(super) const PLAIN: u8 = 1;
pub(super) const PREFIX: u8 = 2;
pub(super) const PLAIN_FIXED: u8 = 3;
pub(super) const PREFIX_FIXED: u8 = 4;
type Result<T> = std::result::Result<T, &'static str>;

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

/// Encode borrowed keyed entity heads directly, without cloning their keys or clocks.
/// The repeatable iterator is scanned for exact size before one buffer is built.
pub(super) fn write<'a>(heads: impl Iterator<Item = (&'a [u8], EntityId, &'a Clock)> + Clone) -> PackedHeads {
    let mut count = 0;
    let mut plain = 0;
    let mut compressed = 0;
    let mut expanded_size = 8;
    let mut generations = 0;
    let mut tips = 0;
    let mut previous = &[][..];
    for (key, _, head) in heads.clone() {
        count += 1;
        let shared = common_prefix(previous, key);
        let generation_size = head.entries().iter().map(|(g, _)| uint_size(*g as usize)).sum::<usize>();
        generations += generation_size;
        tips += head.len();
        let clock_size = uint_size(head.len()) + head.len() * 32 + generation_size;
        plain += uint_size(key.len()) + key.len() + 32 + clock_size;
        compressed += uint_size(shared) + uint_size(key.len() - shared) + key.len() - shared + 32 + clock_size;
        expanded_size += 8 + key.len() as u64 + 32 + 8 + 36 * head.len() as u64;
        previous = key;
    }
    let fixed = generations > tips * 4;
    if fixed {
        plain -= generations - tips * 4;
        compressed -= generations - tips * 4;
    }
    let prefix = compressed < plain;
    let mut bytes = Vec::with_capacity(1 + uint_size(count) + if prefix { compressed } else { plain });
    bytes.push(match (prefix, fixed) {
        (false, false) => PLAIN,
        (true, false) => PREFIX,
        (false, true) => PLAIN_FIXED,
        (true, true) => PREFIX_FIXED,
    });
    put_uint(&mut bytes, count);
    previous = &[];
    for (key, id, head) in heads {
        let shared = if prefix { common_prefix(previous, key) } else { 0 };
        if prefix {
            put_uint(&mut bytes, shared);
        }
        put_uint(&mut bytes, key.len() - shared);
        bytes.extend_from_slice(&key[shared..]);
        bytes.extend_from_slice(&id.to_bytes());
        put_uint(&mut bytes, head.len());
        for (generation, id) in head.entries() {
            if fixed {
                bytes.extend_from_slice(&generation.to_le_bytes());
            } else {
                put_uint(&mut bytes, *generation as usize);
            }
            bytes.extend_from_slice(id.as_bytes());
        }
        previous = key;
    }
    PackedHeads { bytes: bytes.into(), len: count, expanded_size, plain_size: 1 + uint_size(count) + plain }
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
        let end = self.offset.checked_add(len).ok_or("length overflow")?;
        let bytes = self.bytes.get(self.offset..end).ok_or("truncated inventory")?;
        self.offset = end;
        Ok(bytes)
    }
    pub fn uint(&mut self) -> Result<usize> {
        let value = match self.take(1)?[0] {
            n @ 0..=250 => n as u64,
            251 => u16::from_le_bytes(self.take(2)?.try_into().unwrap()) as u64,
            252 => u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as u64,
            253 => u64::from_le_bytes(self.take(8)?.try_into().unwrap()),
            _ => return Err("invalid inventory integer"),
        };
        usize::try_from(value).map_err(|_| "inventory integer overflow")
    }
    pub fn generation(&mut self, fixed: bool) -> Result<u32> {
        let value = if fixed {
            u32::from_le_bytes(self.take(4)?.try_into().unwrap())
        } else {
            u32::try_from(self.uint()?).map_err(|_| "invalid clock generation")?
        };
        if value == 0 {
            return Err("invalid clock generation");
        }
        Ok(value)
    }
    pub fn mode(&mut self) -> Result<(bool, bool)> {
        match self.take(1)?[0] {
            PLAIN => Ok((false, false)),
            PREFIX => Ok((true, false)),
            PLAIN_FIXED => Ok((false, true)),
            PREFIX_FIXED => Ok((true, true)),
            _ => Err("unsupported inventory encoding"),
        }
    }
    pub fn head<'k>(&mut self, prefix: bool, fixed: bool, key: &'k mut Vec<u8>) -> Result<EncodedHead<'k, 'a>>
    where 'a: 'k {
        let shared = if prefix { self.uint()? } else { 0 };
        if shared > key.len() {
            return Err("prefix exceeds preceding key");
        }
        let length = self.uint()?;
        let suffix = self.take(length)?;
        shared.checked_add(length).ok_or("expanded key length overflow")?;
        let full_key = if prefix {
            key.truncate(shared);
            key.extend_from_slice(suffix);
            key.as_slice()
        } else {
            suffix
        };
        let entity_id = EntityId::from_bytes(self.take(32)?.try_into().unwrap());
        let tips = self.uint()?;
        if tips > (self.bytes.len() - self.offset) / 33 {
            return Err("truncated clock");
        }
        let start = self.offset;
        for _ in 0..tips {
            self.generation(fixed)?;
            self.take(32)?;
        }
        Ok(EncodedHead { key: full_key, entity_id, clock: &self.bytes[start..self.offset], tips })
    }
}

/// The (generation, event) tips of a clock that `Reader::head` already validated.
pub(super) fn tips(clock: &[u8], count: usize, fixed: bool) -> impl Iterator<Item = (u32, EventId)> + '_ {
    let mut reader = Reader::new(clock);
    (0..count).map(move |_| {
        let generation = reader.generation(fixed).expect("validated generation");
        let id = reader.take(32).expect("validated event id");
        (generation, EventId::from_bytes(id.try_into().unwrap()))
    })
}

/// Check all fields before retaining or exposing a payload. Cap the expanded plain
/// payload as well as its wire bytes so repeated prefixes cannot amplify work.
pub(super) fn validate(bytes: &[u8]) -> Result<(usize, u64, usize, bool)> {
    if bytes.len() > MAX_OFFER_BYTES {
        return Err("packed inventory exceeds byte limit");
    }
    let mut reader = Reader::new(bytes);
    let (prefix, fixed) = reader.mode()?;
    let count = reader.uint()?;
    if count > MAX_OFFER_HEADS {
        return Err("packed inventory exceeds head limit");
    }
    let mut plain_size = 1 + uint_size(count);
    let mut expanded_size = 8;
    let mut canonical = true;
    let mut key = Vec::new();
    for _ in 0..count {
        let head = reader.head(prefix, fixed, &mut key)?;
        plain_size += uint_size(head.key.len()) + head.key.len() + 32 + uint_size(head.tips) + head.clock.len();
        if plain_size > MAX_OFFER_BYTES {
            return Err("expanded inventory exceeds byte limit");
        }
        expanded_size += 8 + head.key.len() as u64 + 32 + 8 + 36 * head.tips as u64;
        let ordered = tips(head.clock, head.tips, fixed).map(|(_, id)| id).is_sorted_by(|a, b| a < b);
        if !ordered {
            Clock::new(tips(head.clock, head.tips, fixed).collect::<Vec<_>>()).map_err(|_| "invalid clock annotations")?;
            canonical = false;
        }
    }
    if reader.offset != bytes.len() {
        return Err("trailing inventory bytes");
    }
    Ok((count, expanded_size, plain_size, canonical))
}
