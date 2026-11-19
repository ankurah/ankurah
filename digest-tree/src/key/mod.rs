//! Keys: the bytes a tree orders its members by, which the tree makes from
//! each member's values under its key spec.
//!
//! A key is its parts' encodings one after another. Each encoding is a tag,
//! saying whether the part holds a value, then the value's sortable bytes
//! (the shared canonical ordering): as they are for a type whose every value takes the same
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
pub use range::{IntoSpan, IntoSpans, KeyRange};

use ankurah_core_types::key_spec::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core_types::ordering::supports_collation;
use ankurah_core_types::Value;

use crate::{Error, Member, MemberId};

use ankurah_core_types::ordering::append_key_part as encode_part;

/// The tag of a part that holds a value.
const PRESENT: u8 = 0x01;

/// Refuse a collation other than the shared C/POSIX byte order.
pub(crate) fn check_spec<K>(spec: &KeySpec<K>) -> Result<(), Error> {
    match spec.keyparts.iter().position(|part| !supports_collation(part.collation.as_deref())) {
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

/// Refuse a value for the part at `index`, given by a key range,
/// whose type isn't the part's.
fn check_value<K>(part: &IndexKeyPart<K>, index: usize, value: &Value) -> Result<(), Error> {
    let found = value.value_type();
    match found == part.value_type {
        true => Ok(()),
        false => Err(Error::KeyValueType { part: index, expected: part.value_type, found }),
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

#[cfg(test)]
mod tests;
