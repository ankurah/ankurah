//! A key spec's byte encoding, which a tree's metadata row holds.
//!
//! A key spec is its parts one after another. A part is its property's
//! encoding as a length-prefixed field, then a byte each for its direction,
//! its value type and its nulls order, then its sub-path and its collation:
//! each a byte saying whether it is there, and then its strings as
//! length-prefixed fields, the sub-path's behind their count.

use ankurah_core_types::key_spec::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder};
use ankurah_core_types::ValueType;

use crate::bytes::{write_field, ByteReader};
use crate::{Encode, StoreError};

impl<K: Encode> Encode for KeySpec<K> {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for part in &self.keyparts {
            write_field(&mut bytes, &part.key.encode());
            bytes.extend([direction_byte(part.direction), value_type_byte(part.value_type), nulls_byte(part.nulls)]);

            match &part.sub_path {
                None => bytes.push(0),

                Some(path) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&u32::try_from(path.len()).expect("a sub-path of fewer than 2^32 steps").to_be_bytes());
                    path.iter().for_each(|step| write_field(&mut bytes, step.as_bytes()));
                }
            }

            match &part.collation {
                None => bytes.push(0),

                Some(name) => {
                    bytes.push(1);
                    write_field(&mut bytes, name.as_bytes());
                }
            }
        }

        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        let mut reader = ByteReader::new(bytes);
        let mut keyparts = Vec::new();

        while !reader.rest().is_empty() {
            let key = K::decode(reader.field().ok_or_else(truncated)?)?;
            let direction = direction_of(reader.byte().ok_or_else(truncated)?)?;
            let value_type = value_type_of(reader.byte().ok_or_else(truncated)?)?;
            let nulls = nulls_of(reader.byte().ok_or_else(truncated)?)?;

            let sub_path = match present(&mut reader)? {
                false => None,

                true => {
                    let steps = reader.u32_be().ok_or_else(truncated)?;
                    Some((0..steps).map(|_| string(&mut reader)).collect::<Result<_, _>>()?)
                }
            };

            let collation = match present(&mut reader)? {
                false => None,
                true => Some(string(&mut reader)?),
            };

            keyparts.push(IndexKeyPart { key, sub_path, direction, value_type, nulls, collation });
        }

        Ok(KeySpec::new(keyparts))
    }
}

fn direction_byte(direction: IndexDirection) -> u8 {
    match direction {
        IndexDirection::Asc => 0,
        IndexDirection::Desc => 1,
    }
}

fn direction_of(byte: u8) -> Result<IndexDirection, StoreError> {
    match byte {
        0 => Ok(IndexDirection::Asc),
        1 => Ok(IndexDirection::Desc),
        _ => Err(StoreError::permanent("a key part's direction is 0 or 1")),
    }
}

/// Each value type's byte, which a stored key spec keeps, so these never
/// change.
fn value_type_byte(value_type: ValueType) -> u8 {
    match value_type {
        ValueType::I16 => 0,
        ValueType::I32 => 1,
        ValueType::I64 => 2,
        ValueType::F64 => 3,
        ValueType::Bool => 4,
        ValueType::String => 5,
        ValueType::EntityId => 6,
        ValueType::Object => 7,
        ValueType::Binary => 8,
        ValueType::Json => 9,
    }
}

fn value_type_of(byte: u8) -> Result<ValueType, StoreError> {
    Ok(match byte {
        0 => ValueType::I16,
        1 => ValueType::I32,
        2 => ValueType::I64,
        3 => ValueType::F64,
        4 => ValueType::Bool,
        5 => ValueType::String,
        6 => ValueType::EntityId,
        7 => ValueType::Object,
        8 => ValueType::Binary,
        9 => ValueType::Json,
        _ => return Err(StoreError::permanent("a key part's value type is unknown")),
    })
}

fn nulls_byte(nulls: Option<NullsOrder>) -> u8 {
    match nulls {
        None => 0,
        Some(NullsOrder::First) => 1,
        Some(NullsOrder::Last) => 2,
    }
}

fn nulls_of(byte: u8) -> Result<Option<NullsOrder>, StoreError> {
    match byte {
        0 => Ok(None),
        1 => Ok(Some(NullsOrder::First)),
        2 => Ok(Some(NullsOrder::Last)),
        _ => Err(StoreError::permanent("a key part's nulls order is 0, 1 or 2")),
    }
}

/// Whether the field that follows is there.
fn present(reader: &mut ByteReader<'_>) -> Result<bool, StoreError> {
    match reader.byte().ok_or_else(truncated)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(StoreError::permanent("a key part's presence byte is 0 or 1")),
    }
}

fn string(reader: &mut ByteReader<'_>) -> Result<String, StoreError> {
    let bytes = reader.field().ok_or_else(truncated)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| StoreError::permanent("a key part's string is not UTF-8"))
}

fn truncated() -> StoreError { StoreError::permanent("a truncated key spec") }
