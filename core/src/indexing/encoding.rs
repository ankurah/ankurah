use super::key_spec::KeySpec;
use crate::collation::Collatable;
use crate::value::{Value, ValueType};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Type mismatch: expected {0:?}, got {1:?}")]
    TypeMismatch(ValueType, ValueType),
}

// Type tags for JSON encoding.
// These are chosen to provide sensible sort order: null < bool < int < float < string
// Each type uses fixed-width encoding where possible to avoid sentinel issues.
const JSON_TAG_NULL: u8 = 0x00;
const JSON_TAG_BOOL: u8 = 0x10;
const JSON_TAG_INT: u8 = 0x20; // i64: fixed 8 bytes, no sentinel needed
const JSON_TAG_FLOAT: u8 = 0x30; // f64: fixed 8 bytes, no sentinel needed
const JSON_TAG_STRING: u8 = 0x40; // variable length, uses 0x00 sentinel with 0x00→0x00 0xFF escaping

/// Encode a single component (no NULL handling for now - TODO: add NULL support later)
pub fn encode_component_typed(value: &Value, expected_type: ValueType, descending: bool) -> Result<Vec<u8>, IndexError> {
    // Cast value to expected type (short-circuits if types already match)
    let value = value.cast_to(expected_type).map_err(|_| IndexError::TypeMismatch(expected_type, ValueType::of(value)))?;

    encode_value_component(&value, expected_type, descending)
}

/// Encode a non-NULL value component
fn encode_value_component(value: &Value, expected_type: ValueType, descending: bool) -> Result<Vec<u8>, IndexError> {
    match (value, expected_type) {
        (Value::String(s), ValueType::String) => {
            if !descending {
                // ASC: [escaped UTF-8][0x00] - no type tag needed
                let mut out = Vec::with_capacity(s.len() + 1);
                for &b in s.as_bytes() {
                    if b == 0x00 {
                        out.push(0x00);
                        out.push(0xFF);
                    } else {
                        out.push(b);
                    }
                }
                out.push(0x00);
                Ok(out)
            } else {
                // DESC: [inv(payload) with 0xFF escaped as 0xFF 0x00][0xFF 0xFF]
                let mut out = Vec::with_capacity(s.len() + 2);
                for &b in s.as_bytes() {
                    let inv = 0xFFu8.wrapping_sub(b);
                    if inv == 0xFF {
                        out.push(0xFF);
                        out.push(0x00);
                    } else {
                        out.push(inv);
                    }
                }
                out.push(0xFF);
                out.push(0xFF);
                Ok(out)
            }
        }
        (Value::I16(_) | Value::I32(_) | Value::I64(_), ValueType::I16 | ValueType::I32 | ValueType::I64) => {
            // Integers are encoded big-endian (order-preserving). DESC: invert payload bytes.
            let bytes = value.to_bytes();
            if !descending {
                Ok(bytes)
            } else {
                Ok(bytes.into_iter().map(|b| 0xFFu8.wrapping_sub(b)).collect())
            }
        }
        (Value::F64(_), ValueType::F64) => {
            // F64 uses collation ordering (NaN sorts last, proper IEEE 754 ordering). DESC: invert payload bytes.
            let bytes = value.to_bytes();
            if !descending {
                Ok(bytes)
            } else {
                Ok(bytes.into_iter().map(|b| 0xFFu8.wrapping_sub(b)).collect())
            }
        }
        (Value::Bool(_), ValueType::Bool) => {
            // ASC: false(0) < true(1). DESC: invert payload to flip order.
            let b = value.to_bytes()[0];
            Ok(vec![if !descending { b } else { 0xFFu8.wrapping_sub(b) }])
        }
        (Value::EntityId(entity_id), ValueType::EntityId) => {
            // Fixed-width EntityId: no terminator needed
            let bytes = entity_id.to_bytes();
            if !descending {
                Ok(bytes.to_vec())
            } else {
                Ok(bytes.into_iter().map(|b| 0xFFu8.wrapping_sub(b)).collect())
            }
        }
        (Value::Object(bytes) | Value::Binary(bytes), ValueType::Binary | ValueType::Object) => {
            if !descending {
                // ASC: [escaped bytes][0x00] - terminator needed for variable-width
                let mut out = Vec::with_capacity(bytes.len() + 1);
                for &b in bytes.iter() {
                    if b == 0x00 {
                        out.push(0x00);
                        out.push(0xFF);
                    } else {
                        out.push(b);
                    }
                }
                out.push(0x00);
                Ok(out)
            } else {
                // DESC: [inv(bytes) with 0xFF escaped as 0xFF 0x00][0xFF 0xFF]
                let mut out = Vec::with_capacity(bytes.len() + 2);
                for &b in bytes.iter() {
                    let inv = 0xFFu8.wrapping_sub(b);
                    if inv == 0xFF {
                        out.push(0xFF);
                        out.push(0x00);
                    } else {
                        out.push(inv);
                    }
                }
                out.push(0xFF);
                out.push(0xFF);
                Ok(out)
            }
        }
        // JSON: type-tagged encoding preserving original type (no cross-type casting)
        (Value::Json(json), ValueType::Json) => Ok(encode_json_value(json, descending)),
        _ => Err(IndexError::TypeMismatch(expected_type, ValueType::of(value))),
    }
}

/// Encode JSON value with type tag prefix.
/// Different types get different prefixes, so "9" (string) != 9 (int).
fn encode_json_value(json: &serde_json::Value, descending: bool) -> Vec<u8> {
    let (tag, payload) = match json {
        serde_json::Value::Null => (JSON_TAG_NULL, vec![]),
        serde_json::Value::Bool(b) => (JSON_TAG_BOOL, vec![if *b { 1 } else { 0 }]),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // i64: fixed 8 bytes big-endian with sign flip for proper ordering
                (JSON_TAG_INT, Value::I64(i).to_bytes())
            } else if let Some(f) = n.as_f64() {
                // f64: fixed 8 bytes with IEEE 754 ordering
                (JSON_TAG_FLOAT, Value::F64(f).to_bytes())
            } else {
                // Fallback for very large numbers
                (JSON_TAG_NULL, vec![])
            }
        }
        serde_json::Value::String(s) => {
            // Variable length: escape 0x00 bytes and add 0x00 terminator
            let mut payload = Vec::with_capacity(s.len() + 1);
            for &b in s.as_bytes() {
                if b == 0x00 {
                    payload.push(0x00);
                    payload.push(0xFF);
                } else {
                    payload.push(b);
                }
            }
            payload.push(0x00); // terminator
            (JSON_TAG_STRING, payload)
        }
        // Objects and arrays are unsortable - encode as null
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => (JSON_TAG_NULL, vec![]),
    };

    if !descending {
        let mut out = Vec::with_capacity(1 + payload.len());
        out.push(tag);
        out.extend(payload);
        out
    } else {
        // DESC: invert tag and all payload bytes
        let mut out = Vec::with_capacity(1 + payload.len());
        out.push(0xFFu8.wrapping_sub(tag));
        out.extend(payload.into_iter().map(|b| 0xFFu8.wrapping_sub(b)));
        out
    }
}

/// Type-aware encoding using KeySpec for validation and optimization
/// TODO: Add NULL handling later
pub fn encode_tuple_values_with_key_spec(values: &[Value], key_spec: &KeySpec) -> Result<Vec<u8>, IndexError> {
    let mut out = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i >= key_spec.keyparts.len() {
            break; // Don't encode more values than key spec defines
        }
        let keypart = &key_spec.keyparts[i];

        // Use type-aware encoding without type tags
        let bytes = encode_component_typed(v, keypart.value_type, keypart.direction.is_desc())?;
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_desc_ordering() {
        let a = encode_component_typed(&Value::String("a".to_string()), ValueType::String, true).unwrap();
        let b = encode_component_typed(&Value::String("b".to_string()), ValueType::String, true).unwrap();

        // DESC: "a" should sort after "b" (reversed)
        assert!(a > b);
    }

    #[test]
    fn test_asc_ordering() {
        let a = encode_component_typed(&Value::String("a".to_string()), ValueType::String, false).unwrap();
        let b = encode_component_typed(&Value::String("b".to_string()), ValueType::String, false).unwrap();

        // ASC: "a" should sort before "b"
        assert!(a < b);
    }
}
