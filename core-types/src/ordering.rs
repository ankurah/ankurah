//! Canonical query ordering and ordered-key bytes.
//!
//! Strings use UTF-8 byte order (PostgreSQL's C/POSIX collation). JSON follows
//! PostgreSQL JSONB comparison in a database with that default collation,
//! including decimal numeric equality and container ordering. This is separate
//! from predicate comparisons through [`Value::partial_cmp`].
//! Numbers are compared as serialized from the accepted [`serde_json::Value`];
//! this does not change JSON parsing or recover precision lost before storage.

use std::cmp::Ordering;

use crate::collation::Collatable;
use crate::key_spec::{IndexDirection, IndexKeyPart, NullsOrder};
use crate::{Value, ValueType};

mod json;

/// Version of the ordered-key encoding. Version 1 used JSON text ordering.
/// Persisted keys must be rebuilt when this version changes.
pub const KEY_ENCODING_VERSION: u8 = 2;

/// The default placement of missing values: last ascending, first descending.
pub fn default_nulls(direction: IndexDirection) -> NullsOrder {
    match direction {
        IndexDirection::Asc => NullsOrder::Last,
        IndexDirection::Desc => NullsOrder::First,
    }
}

/// Whether a collation names the supported UTF-8 byte ordering.
pub fn supports_collation(name: Option<&str>) -> bool { matches!(name, None | Some("C" | "POSIX")) }

/// Ascending bytes for one present value, before tuple framing or direction.
/// Equal numbers in JSON have identical bytes, independent of decimal spelling.
pub fn value_bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::Json(value) => json::bytes(value),
        value => Collatable::to_bytes(value),
    }
}

/// Total query ordering for present values. Float NaNs follow infinity and
/// compare equal; both signed zeros compare equal. Integer widths share an
/// order; otherwise differing scalar types have a fixed type order. A JSON
/// scalar remains distinct from a scalar outside JSON.
pub fn compare_values(left: &Value, right: &Value) -> Ordering {
    type_order(left).cmp(&type_order(right)).then_with(|| value_bytes(left).cmp(&value_bytes(right)))
}

fn type_order(value: &Value) -> u8 {
    match value {
        Value::I16(_) | Value::I32(_) | Value::I64(_) => 0,
        Value::F64(_) => 1,
        Value::Bool(_) => 2,
        Value::String(_) => 3,
        Value::EntityId(_) => 4,
        Value::Object(_) => 5,
        Value::Binary(_) => 6,
        Value::Json(_) => 7,
    }
}

/// Append a key component whose bytes sort in the part's direction and NULL
/// placement. Components are prefix-free, including after descending inversion.
/// The caller must validate the value type and [`supports_collation`] first.
pub fn append_key_part<K>(part: &IndexKeyPart<K>, value: Option<&Value>, key: &mut Vec<u8>) {
    let start = key.len();
    match value {
        None => {
            let nulls = part.nulls.unwrap_or_else(|| default_nulls(part.direction));
            key.push(match (part.direction, nulls) {
                (IndexDirection::Asc, NullsOrder::First) | (IndexDirection::Desc, NullsOrder::Last) => 0,
                (IndexDirection::Asc, NullsOrder::Last) | (IndexDirection::Desc, NullsOrder::First) => 2,
            });
        }
        Some(value) => {
            key.push(1);
            let bytes = value_bytes(value);
            match part.value_type {
                ValueType::I16 | ValueType::I32 | ValueType::I64 | ValueType::F64 | ValueType::Bool | ValueType::EntityId => {
                    key.extend_from_slice(&bytes);
                }
                ValueType::String | ValueType::Object | ValueType::Binary | ValueType::Json => frame(key, &bytes),
            }
        }
    }
    if part.direction.is_desc() {
        key[start..].iter_mut().for_each(|byte| *byte = !*byte);
    }
}

fn frame(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        out.push(byte);
        if byte == 0 {
            out.push(255);
        }
    }
    out.extend([0, 0]);
}

#[cfg(test)]
mod tests;
