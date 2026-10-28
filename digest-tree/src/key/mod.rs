//! Keys: the bytes a tree orders its members by, which the tree makes from
//! each member's values under its key spec.
//!
//! A key is its parts' encodings one after another. Each encoding is a tag,
//! saying whether the part holds a value, then the value's sortable bytes
//! ([`Collatable`]): as they are for a type whose every value takes the same
//! number of bytes, and framed as an address frames its fields for any other.
//! So keys sort part by part, each part in its values' order, and the encoding
//! of a key's leading parts begins exactly the keys that hold those values
//! there, which is what turns a range of values into a span of addresses. A
//! descending part has every byte of its encoding inverted, which reverses its
//! order and keeps any encoding from beginning another.
//!
//! A part without a value is tagged to sort before or after every value, as
//! the part's nulls order says, and otherwise where PostgreSQL puts it: last
//! in an ascending part and first in a descending one (ankurah/ankurah#482).

mod range;

pub(crate) use range::span_for;
pub use range::KeyRange;

use ankurah_core_types::collation::Collatable;
use ankurah_core_types::key_spec::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder};
use ankurah_core_types::{Value, ValueType};

use crate::span::frame_into;
use crate::{Error, Member, MemberId};

/// The tag of a part that holds a value.
const PRESENT: u8 = 0x01;

/// The tag of a part without a value that sorts before every value, before
/// the part's direction is applied.
const MISSING_BEFORE: u8 = 0x00;

/// The tag of a part without a value that sorts after every value, before the
/// part's direction is applied.
const MISSING_AFTER: u8 = 0x02;

/// Refuse a key spec whose part names a collation: the tree implements none,
/// and ignoring one would order members otherwise than the spec says.
pub(crate) fn check_spec<K>(spec: &KeySpec<K>) -> Result<(), Error> {
    match spec.keyparts.iter().position(|part| part.collation.is_some()) {
        Some(part) => Err(Error::Collation { part }),
        None => Ok(()),
    }
}

/// The key the tree files `member` under: its value for each part of `spec`,
/// checked against the part's type and encoded.
pub(crate) fn member_key<K, M: Member<K>>(spec: &KeySpec<K>, member: &M) -> Result<Vec<u8>, Error> {
    let id = || member.id().to_bytes().into_owned();
    let mut key = Vec::new();

    for (index, part) in spec.keyparts.iter().enumerate() {
        let value = member.value(part).map_err(|source| Error::Key { member: id(), source: Box::new(source) })?;

        if let Some(value) = &value {
            let found = value.value_type();
            if found != part.value_type {
                return Err(Error::WrongValueType { member: id(), part: index, expected: part.value_type, found });
            }
        }

        encode_part(part, value.as_ref(), &mut key);
    }

    Ok(key)
}

/// The key of a member whose values are `values`, one for each part of
/// `spec`, first part first: the key a lookup names.
pub(crate) fn lookup_key<K>(spec: &KeySpec<K>, values: &[Value]) -> Result<Vec<u8>, Error> {
    if values.len() != spec.keyparts.len() {
        return Err(Error::KeyLength { parts: spec.keyparts.len(), given: values.len() });
    }

    let mut key = Vec::new();
    for (index, (part, value)) in spec.keyparts.iter().zip(values).enumerate() {
        check_value(part, index, value)?;
        encode_part(part, Some(value), &mut key);
    }

    Ok(key)
}

/// Refuse a value for the part at `index`, given by a lookup or a key range,
/// whose type isn't the part's.
fn check_value<K>(part: &IndexKeyPart<K>, index: usize, value: &Value) -> Result<(), Error> {
    let found = value.value_type();
    match found == part.value_type {
        true => Ok(()),
        false => Err(Error::KeyValueType { part: index, expected: part.value_type, found }),
    }
}

/// Append to `key` the encoding of `part` holding `value`, or no value.
fn encode_part<K>(part: &IndexKeyPart<K>, value: Option<&Value>, key: &mut Vec<u8>) {
    let start = key.len();

    match value {
        None => key.push(missing_tag(part)),

        Some(value) => {
            key.push(PRESENT);

            let bytes = Collatable::to_bytes(value);
            match is_fixed_width(part.value_type) {
                true => key.extend_from_slice(&bytes),
                false => frame_into(key, &bytes),
            }
        }
    }

    if part.direction.is_desc() {
        key[start..].iter_mut().for_each(|byte| *byte = !*byte);
    }
}

/// The byte every encoding of `part` holding a value begins with, its
/// direction applied: what the span of its values begins with.
fn present_tag<K>(part: &IndexKeyPart<K>) -> u8 {
    match part.direction {
        IndexDirection::Asc => PRESENT,
        IndexDirection::Desc => !PRESENT,
    }
}

/// The tag of `part` holding no value, before its direction is applied. A
/// descending part's inversion turns the order over, so a missing value that
/// sorts after every value there takes the tag that sorts before them.
fn missing_tag<K>(part: &IndexKeyPart<K>) -> u8 {
    match (part.direction, nulls_order(part)) {
        (IndexDirection::Asc, NullsOrder::Last) | (IndexDirection::Desc, NullsOrder::First) => MISSING_AFTER,
        (IndexDirection::Asc, NullsOrder::First) | (IndexDirection::Desc, NullsOrder::Last) => MISSING_BEFORE,
    }
}

/// Where `part` puts members without a value: where the part says, or else
/// where PostgreSQL puts them, last ascending and first descending.
fn nulls_order<K>(part: &IndexKeyPart<K>) -> NullsOrder {
    part.nulls.unwrap_or(match part.direction {
        IndexDirection::Asc => NullsOrder::Last,
        IndexDirection::Desc => NullsOrder::First,
    })
}

/// Whether every value of `value_type` takes the same number of sortable
/// bytes, so that they end themselves without framing.
fn is_fixed_width(value_type: ValueType) -> bool {
    match value_type {
        ValueType::I16 | ValueType::I32 | ValueType::I64 | ValueType::F64 | ValueType::Bool | ValueType::EntityId => true,
        ValueType::String | ValueType::Object | ValueType::Binary | ValueType::Json => false,
    }
}

#[cfg(test)]
mod tests;
