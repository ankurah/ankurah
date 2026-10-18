//! Payload: encoding tag, row count, then key, entity ID and clock for each row.
//! Counts/lengths use unsigned little-endian bincode varints. Prefix modes add
//! the shared-byte count before each key suffix. Fixed modes store generations
//! as four little-endian bytes; other modes use varints. IDs are always 32 bytes.

use super::{ClockRef, EntityHeadRef, KeyedEntityHeadRef, MAX_BYTES, MAX_ROWS};
use crate::{Clock, EntityId};

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
    pub fn row<'k>(&mut self, prefix: bool, fixed: bool, key: &'k mut Vec<u8>) -> Result<KeyedEntityHeadRef<'k>>
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
        let count = self.uint()?;
        if count > (self.bytes.len() - self.offset) / 33 {
            return Err("truncated clock");
        }
        let start = self.offset;
        for _ in 0..count {
            self.generation(fixed)?;
            self.take(32)?;
        }
        Ok(KeyedEntityHeadRef {
            key: full_key,
            entity: EntityHeadRef { entity_id, head: ClockRef { bytes: &self.bytes[start..self.offset], count, fixed } },
        })
    }
}

/// Check all fields before retaining or exposing a payload. Cap the expanded plain
/// payload as well as its wire bytes so repeated prefixes cannot amplify work.
pub(super) fn validate(bytes: &[u8]) -> Result<(usize, u64, usize, bool)> {
    if bytes.len() > MAX_BYTES {
        return Err("packed inventory exceeds byte limit");
    }
    let mut reader = Reader::new(bytes);
    let (prefix, fixed) = reader.mode()?;
    let count = reader.uint()?;
    if count > MAX_ROWS {
        return Err("packed inventory exceeds row limit");
    }
    let mut plain_size = 1 + uint_size(count);
    let mut expanded_size = 8;
    let mut canonical = true;
    let mut key = Vec::new();
    for _ in 0..count {
        let row = reader.row(prefix, fixed, &mut key)?;
        let clock = row.entity.head;
        plain_size += uint_size(row.key.len()) + row.key.len() + 32 + uint_size(clock.count) + clock.bytes.len();
        if plain_size > MAX_BYTES {
            return Err("expanded inventory exceeds byte limit");
        }
        expanded_size += 8 + row.key.len() as u64 + 32 + 8 + 36 * clock.count as u64;
        let ordered = clock.entries().map(|(_, id)| id).is_sorted_by(|a, b| a < b);
        if !ordered {
            Clock::new(clock.entries().collect::<Vec<_>>()).map_err(|_| "invalid clock annotations")?;
            canonical = false;
        }
    }
    if reader.offset != bytes.len() {
        return Err("trailing inventory bytes");
    }
    Ok((count, expanded_size, plain_size, canonical))
}
