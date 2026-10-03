use crate::collation::Collatable;
use crate::value::Value;
use ankurah_core_types::EntityId;

// Collation for Value (single value). Tuple framing (type tags/lengths) is handled by higher-level encoders.
impl Collatable for Value {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::String(s) => s.as_bytes().to_vec(),
            // Use fixed-width big-endian encoding to preserve numeric order across widths
            Value::I16(x) => (*x as i64).to_bytes(),
            Value::I32(x) => (*x as i64).to_bytes(),
            Value::I64(x) => x.to_bytes(),
            Value::F64(f) => f.to_bytes(),
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
                    Some(((*x as i64) + 1).to_bytes())
                }
            }
            Value::I32(x) => {
                if *x == i32::MAX {
                    None
                } else {
                    Some(((*x as i64) + 1).to_bytes())
                }
            }
            Value::I64(x) => x.successor_bytes(),
            Value::F64(f) => f.successor_bytes(),
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
                for i in (0..bytes.len()).rev() {
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
                    Some(((*x as i64) - 1).to_bytes())
                }
            }
            Value::I32(x) => {
                if *x == i32::MIN {
                    None
                } else {
                    Some(((*x as i64) - 1).to_bytes())
                }
            }
            Value::I64(x) => x.predecessor_bytes(),
            Value::F64(f) => f.predecessor_bytes(),
            Value::Bool(b) => {
                if *b {
                    Some(vec![0])
                } else {
                    None
                }
            }
            Value::EntityId(entity_id) => {
                let mut bytes = entity_id.to_bytes();
                if bytes == [0u8; EntityId::BYTE_LEN] {
                    None // Already at minimum
                } else {
                    // Decrement the byte array (big-endian arithmetic)
                    for i in (0..bytes.len()).rev() {
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
            Value::EntityId(entity_id) => entity_id.to_bytes() == [0u8; EntityId::BYTE_LEN],
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
            Value::EntityId(entity_id) => entity_id.to_bytes() == [0xFFu8; EntityId::BYTE_LEN],
            Value::Object(_) | Value::Binary(_) | Value::Json(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn narrow_integers_collate_at_the_full_i64_width() {
        for (value, max, min) in
            [(Value::I16(100), Value::I16(i16::MAX), Value::I16(i16::MIN)), (Value::I32(1000), Value::I32(i32::MAX), Value::I32(i32::MIN))]
        {
            assert_eq!(value.to_bytes().len(), 8);
            assert!(value.successor_bytes().unwrap() > value.to_bytes());
            assert!(value.predecessor_bytes().unwrap() < value.to_bytes());
            assert!(!value.is_minimum());
            assert!(!value.is_maximum());

            assert!(max.successor_bytes().is_none());
            assert!(min.predecessor_bytes().is_none());
            assert!(max.is_maximum());
            assert!(min.is_minimum());
        }
    }

    #[test]
    fn entity_ids_bound_at_the_all_zero_and_all_ones_ids() {
        assert!(!Value::EntityId(EntityId::random()).is_minimum());
        assert!(!Value::EntityId(EntityId::random()).is_maximum());

        let min = Value::EntityId(EntityId::from_bytes([0; EntityId::BYTE_LEN]));
        assert!(min.is_minimum());
        assert!(min.predecessor_bytes().is_none());

        let max = Value::EntityId(EntityId::from_bytes([255; EntityId::BYTE_LEN]));
        assert!(max.is_maximum());
        assert!(max.successor_bytes().is_none());
    }

    #[test]
    fn integer_keys_follow_numeric_order_across_zero() {
        let widths: [(fn(i64) -> Value, i64, i64); 3] = [
            (|n| Value::I16(n as i16), i16::MIN.into(), i16::MAX.into()),
            (|n| Value::I32(n as i32), i32::MIN.into(), i32::MAX.into()),
            (Value::I64, i64::MIN, i64::MAX),
        ];
        for (value, min, max) in widths {
            let ascending = [min, min + 1, -300, -256, -255, -2, -1, 0, 1, 2, 255, 256, 300, max - 1, max];
            let keys: Vec<_> = ascending.iter().map(|&n| value(n).to_bytes()).collect();
            assert!(keys.windows(2).all(|pair| pair[0] < pair[1]), "keys must ascend with {ascending:?}");
            for n in ascending {
                let (key, successor, predecessor) = (value(n).to_bytes(), value(n).successor_bytes(), value(n).predecessor_bytes());
                assert_eq!(successor, (n < max).then(|| value(n + 1).to_bytes()), "successor of {n}");
                assert_eq!(predecessor, (n > min).then(|| value(n - 1).to_bytes()), "predecessor of {n}");
                assert!(successor.is_none_or(|s| s > key) && predecessor.is_none_or(|p| p < key), "neighbours of {n}");
            }
        }
    }

    #[test]
    fn primitive_and_value_numeric_keys_agree() {
        fn check<T: Collatable>(primitive: T, value: Value) {
            assert_eq!(primitive.to_bytes(), value.to_bytes());
            assert_eq!(primitive.predecessor_bytes(), value.predecessor_bytes());
            assert_eq!(primitive.successor_bytes(), value.successor_bytes());
        }
        for n in [i64::MIN, -256, -1, 0, 1, 256, i64::MAX] {
            check(n, Value::I64(n));
        }
        for n in [-256i16, -1, 0, 1, 256] {
            check(n as i64, Value::I16(n));
            check(n as i64, Value::I32(n as i32));
        }
        let nearest = f64::from_bits(1);
        for f in [f64::NEG_INFINITY, -1.0, -nearest, -0.0, 0.0, nearest, 1.0, f64::INFINITY, f64::NAN] {
            check(f, Value::F64(f));
        }
    }

    #[test]
    fn both_zeros_share_one_key_between_the_nearest_negative_and_positive() {
        let nearest = f64::from_bits(1); // the smallest positive subnormal
        let [below, negative_zero, zero, above] = [-nearest, -0.0, 0.0, nearest].map(|f| Value::F64(f).to_bytes());
        assert_eq!(negative_zero, zero);
        for z in [Value::F64(-0.0), Value::F64(0.0)] {
            let (predecessor, successor) = (z.predecessor_bytes().unwrap(), z.successor_bytes().unwrap());
            assert!(below <= predecessor && predecessor < zero, "x >= {z:?} must admit zero and no negative");
            assert!(zero < successor && successor <= above, "x <= {z:?} must admit zero and no positive");
        }
    }
}
