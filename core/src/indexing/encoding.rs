use super::key_spec::KeySpec;
use crate::collation::Collatable;
use crate::value::{Value, ValueType};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Type mismatch: expected {0:?}, got {1:?}")]
    TypeMismatch(ValueType, ValueType),
}

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
        _ => Err(IndexError::TypeMismatch(expected_type, ValueType::of(value))),
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
