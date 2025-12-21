use crate::collation::Collatable;
use crate::value::Value;

// Collation for Value (single value). Tuple framing (type tags/lengths) is handled by higher-level encoders.
impl Collatable for Value {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::String(s) => s.as_bytes().to_vec(),
            // Use fixed-width big-endian encoding to preserve numeric order across widths
            Value::I16(x) => (*x as i64).to_be_bytes().to_vec(),
            Value::I32(x) => (*x as i64).to_be_bytes().to_vec(),
            Value::I64(x) => x.to_be_bytes().to_vec(),
            Value::F64(f) => {
                let bits = if f.is_nan() {
                    u64::MAX // NaN sorts last
                } else {
                    let bits = f.to_bits();
                    if *f >= 0.0 {
                        bits ^ (1 << 63) // Flip sign bit for positive numbers
                    } else {
                        !bits // Flip all bits for negative numbers
                    }
                };
                bits.to_be_bytes().to_vec()
            }
            Value::Bool(b) => vec![*b as u8],
            Value::EntityId(entity_id) => entity_id.to_bytes().to_vec(),
            // For binary/object, return raw bytes; tuple framing will add type-tag/len for cross-type ordering
            Value::Object(bytes) | Value::Binary(bytes) => bytes.clone(),
            // For JSON, serialize to bytes
            Value::Json(json) => serde_json::to_vec(json).unwrap_or_default(),
        }
    }

    fn successor_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Value::String(s) => {
                let mut bytes = s.as_bytes().to_vec();
                bytes.push(0);
                Some(bytes)
            }
            Value::I16(x) => {
                if *x == i16::MAX {
                    None
                } else {
                    Some(((*x as i64) + 1).to_be_bytes().to_vec())
                }
            }
            Value::I32(x) => {
                if *x == i32::MAX {
                    None
                } else {
                    Some(((*x as i64) + 1).to_be_bytes().to_vec())
                }
            }
            Value::I64(x) => {
                if *x == i64::MAX {
                    None
                } else {
                    Some((x + 1).to_be_bytes().to_vec())
                }
            }
            Value::F64(f) => {
                if f.is_nan() || (f.is_infinite() && *f > 0.0) {
                    None
                } else {
                    let bits = if *f >= 0.0 { f.to_bits() ^ (1 << 63) } else { !f.to_bits() };
                    let next_bits = bits + 1;
                    Some(next_bits.to_be_bytes().to_vec())
                }
            }
            Value::Bool(b) => {
                if *b {
                    None
                } else {
                    Some(vec![1])
                }
            }
            Value::EntityId(entity_id) => {
                let mut bytes = entity_id.to_bytes();
                // Increment the byte array (big-endian arithmetic)
                for i in (0..16).rev() {
                    if bytes[i] == 0xFF {
                        bytes[i] = 0;
                    } else {
                        bytes[i] += 1;
                        return Some(bytes.to_vec());
                    }
                }
                None // Overflow - already at maximum
            }
            Value::Object(_) | Value::Binary(_) | Value::Json(_) => None,
        }
    }

    fn predecessor_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Value::String(s) => {
                let bytes = s.as_bytes();
                if bytes.is_empty() {
                    None
                } else {
                    Some(bytes[..bytes.len() - 1].to_vec())
                }
            }
            Value::I16(x) => {
                if *x == i16::MIN {
                    None
                } else {
                    Some(((*x as i64) - 1).to_be_bytes().to_vec())
                }
            }
            Value::I32(x) => {
                if *x == i32::MIN {
                    None
                } else {
                    Some(((*x as i64) - 1).to_be_bytes().to_vec())
                }
            }
            Value::I64(x) => {
                if *x == i64::MIN {
                    None
                } else {
                    Some((x - 1).to_be_bytes().to_vec())
                }
            }
            Value::F64(f) => {
                if f.is_nan() || (f.is_infinite() && *f < 0.0) {
                    None
                } else {
                    let bits = if *f >= 0.0 { f.to_bits() ^ (1 << 63) } else { !f.to_bits() };
                    let prev_bits = bits - 1;
                    Some(prev_bits.to_be_bytes().to_vec())
                }
            }
            Value::Bool(b) => {
                if *b {
                    Some(vec![0])
                } else {
                    None
                }
            }
            Value::EntityId(entity_id) => {
                let mut bytes = entity_id.to_bytes();
                if bytes == [0u8; 16] {
                    None // Already at minimum
                } else {
                    // Decrement the byte array (big-endian arithmetic)
                    for i in (0..16).rev() {
                        if bytes[i] == 0 {
                            bytes[i] = 0xFF;
                        } else {
                            bytes[i] -= 1;
                            return Some(bytes.to_vec());
                        }
                    }
                    None // Should never reach here since we checked for zero above
                }
            }
            Value::Object(_) | Value::Binary(_) | Value::Json(_) => None,
        }
    }

    fn is_minimum(&self) -> bool {
        match self {
            Value::String(s) => s.is_empty(),
            Value::I16(x) => *x == i16::MIN,
            Value::I32(x) => *x == i32::MIN,
            Value::I64(x) => *x == i64::MIN,
            Value::F64(f) => *f == f64::NEG_INFINITY,
            Value::Bool(b) => !b,
            Value::EntityId(entity_id) => entity_id.to_bytes() == [0u8; 16],
            Value::Object(_) | Value::Binary(_) | Value::Json(_) => false,
        }
    }

    fn is_maximum(&self) -> bool {
        match self {
            Value::String(_) => false, // Strings have no theoretical maximum
            Value::I16(x) => *x == i16::MAX,
            Value::I32(x) => *x == i32::MAX,
            Value::I64(x) => *x == i64::MAX,
            Value::F64(f) => *f == f64::INFINITY,
            Value::Bool(b) => *b,
            Value::EntityId(entity_id) => entity_id.to_bytes() == [0xFFu8; 16],
            Value::Object(_) | Value::Binary(_) | Value::Json(_) => false,
        }
    }
}
