// Refactored planner integration - cleaner and more cohesive

use ankurah_core::value::ValueType;
use ankurah_core::{collation::Collatable, value::Value};
use ankurah_storage_common::{Endpoint, IndexDirection, KeyBounds, KeyDatum, KeySpec};

use crate::error::IndexError;

/// Represents the result of converting logical bounds to physical Sled ranges
#[derive(Debug)]
pub struct SledRangeBounds {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub upper_open_ended: bool,
    pub eq_prefix_guard: Vec<u8>,
}

/// Convert IndexBounds directly to Sled byte ranges for a specific index
///
/// This is the main entry point that combines:
/// 1. Extracting logical bounds from the query
/// 2. Converting to physical byte ranges based on the index specification
///
/// # Arguments
/// * `bounds` - The logical query bounds (e.g., "year >= 1969")
/// * `key_spec` - The key specification including column order and directions
pub fn key_bounds_to_sled_range(bounds: &KeyBounds, key_spec: &KeySpec) -> Result<SledRangeBounds, IndexError> {
    // Process the bounds
    let mut lower_tuple = Vec::new();
    let mut upper_tuple = Vec::new();
    let mut lower_open = false;
    let mut upper_open = false;
    let mut eq_prefix_len = 0;
    let mut eq_prefix_values = Vec::new();

    // Build logical bounds from IndexBounds (copied from original normalize logic)
    for bound in &bounds.keyparts {
        // Equality check (low==high, both inclusive)
        if let (Endpoint::Value { datum: low_datum, inclusive: low_incl }, Endpoint::Value { datum: high_datum, inclusive: high_incl }) =
            (&bound.low, &bound.high)
        {
            if let (KeyDatum::Val(low_val), KeyDatum::Val(high_val)) = (low_datum, high_datum) {
                if low_val == high_val && *low_incl && *high_incl {
                    lower_tuple.push(low_val.clone());
                    upper_tuple.push(high_val.clone());
                    eq_prefix_values.push(low_val.clone());
                    eq_prefix_len += 1;
                    continue;
                }
            }
        }

        // Lower side
        if let Endpoint::Value { datum: KeyDatum::Val(val), inclusive } = &bound.low {
            lower_tuple.push(val.clone());
            lower_open = !inclusive;
        }

        // Upper side
        if let Endpoint::Value { datum: KeyDatum::Val(val), inclusive } = &bound.high {
            upper_tuple.push(val.clone());
            upper_open = !inclusive;
        }
        break; // Only process first bound with actual constraints
    }

    // Now convert to physical bounds based on key spec
    convert_to_physical_bounds(lower_tuple, upper_tuple, lower_open, upper_open, eq_prefix_len, eq_prefix_values, key_spec)
}

fn convert_to_physical_bounds(
    lower_tuple: Vec<Value>,
    upper_tuple: Vec<Value>,
    lower_open: bool,
    upper_open: bool,
    eq_prefix_len: usize,
    eq_prefix_values: Vec<Value>,
    key_spec: &KeySpec,
) -> Result<SledRangeBounds, IndexError> {
    // Case 1: Equality bounds
    if eq_prefix_len > 0 && lower_tuple == upper_tuple && lower_tuple.len() == eq_prefix_len {
        let encoded_prefix = encode_tuple_values_with_key_spec(&eq_prefix_values[..eq_prefix_len], key_spec)?;

        if key_spec.keyparts.len() > eq_prefix_len {
            // Multi-key partial equality: use prefix guard
            let mut start = encoded_prefix.clone();
            start.push(0); // tuple terminator

            return Ok(SledRangeBounds {
                start,
                end: None,
                upper_open_ended: true,
                eq_prefix_guard: encoded_prefix, // No terminator in guard
            });
        } else {
            // Single-key or full equality: use tight range
            let mut start = encoded_prefix.clone();
            start.push(0);

            let end = lex_successor(start.clone());

            return Ok(SledRangeBounds {
                start,
                end,
                upper_open_ended: false,
                eq_prefix_guard: Vec::new(), // No guard needed
            });
        }
    }

    // Case 2: Single-component DESC inequality
    if let Some(first_keypart) = key_spec.keyparts.first() {
        if first_keypart.direction.is_desc() {
            let is_single = (lower_tuple.len() <= 1) && (upper_tuple.len() <= 1);
            if is_single {
                return handle_desc_inequality(lower_tuple, upper_tuple, lower_open, upper_open, eq_prefix_len, eq_prefix_values, key_spec);
            }
        }
    }

    // Case 3: General bounds (ASC or multi-component)
    handle_general_bounds(lower_tuple, upper_tuple, lower_open, upper_open, eq_prefix_len, eq_prefix_values, key_spec)
}

fn handle_desc_inequality(
    lower_tuple: Vec<Value>,
    upper_tuple: Vec<Value>,
    lower_open: bool,
    upper_open: bool,
    eq_prefix_len: usize,
    eq_prefix_values: Vec<Value>,
    key_spec: &KeySpec,
) -> Result<SledRangeBounds, IndexError> {
    // DESC swaps logical lower/upper to physical upper/lower
    // x > 5 on DESC becomes scan from start to enc(5) (exclusive)
    // x < 5 on DESC becomes scan from enc(5) to end

    let (start_bytes, end_bytes_opt) = match (!lower_tuple.is_empty(), !upper_tuple.is_empty()) {
        // x > L or x >= L
        (true, false) => {
            let start = vec![0x00]; // Start from beginning
            let mut end = encode_tuple_values_with_key_spec(&lower_tuple, key_spec)?;
            if !lower_open {
                // >= L → end = succ(enc(L))
                if let Some(s) = lex_successor(end.clone()) {
                    end = s;
                }
            }
            end.push(0);
            (start, Some(end))
        }
        // x < U or x <= U
        (false, true) => {
            let mut start = encode_tuple_values_with_key_spec(&upper_tuple, key_spec)?;
            if upper_open {
                // < U → start = succ(enc(U))
                if let Some(s) = lex_successor(start.clone()) {
                    start = s;
                } else {
                    start.clear();
                }
            }
            start.push(0);
            (start, None)
        }
        // L <= x <= U
        (true, true) => {
            let mut start = encode_tuple_values_with_key_spec(&upper_tuple, key_spec)?;
            if upper_open {
                if let Some(s) = lex_successor(start.clone()) {
                    start = s;
                }
            }
            start.push(0);

            let mut end = encode_tuple_values_with_key_spec(&lower_tuple, key_spec)?;
            if !lower_open {
                if let Some(s) = lex_successor(end.clone()) {
                    end = s;
                }
            }
            end.push(0);
            (start, Some(end))
        }
        // No bounds
        _ => (vec![0x00], None),
    };

    let eq_guard =
        if eq_prefix_len > 0 { encode_tuple_values_with_key_spec(&eq_prefix_values[..eq_prefix_len], key_spec)? } else { Vec::new() };

    let upper_open_ended = end_bytes_opt.is_none();
    Ok(SledRangeBounds { start: start_bytes, end: end_bytes_opt, upper_open_ended, eq_prefix_guard: eq_guard })
}

fn handle_general_bounds(
    lower_tuple: Vec<Value>,
    upper_tuple: Vec<Value>,
    lower_open: bool,
    upper_open: bool,
    eq_prefix_len: usize,
    eq_prefix_values: Vec<Value>,
    key_spec: &KeySpec,
) -> Result<SledRangeBounds, IndexError> {
    // Standard ASC bounds or multi-component bounds
    let start = if !lower_tuple.is_empty() {
        let mut start = encode_tuple_values_with_key_spec(&lower_tuple, key_spec)?;
        if lower_open {
            start = lex_successor(start).unwrap_or_default();
        }
        start.push(0);
        start
    } else {
        vec![0x00]
    };

    let (end, upper_open_ended) = if !upper_tuple.is_empty() {
        let mut end = encode_tuple_values_with_key_spec(&upper_tuple, key_spec)?;
        if !upper_open {
            if let Some(s) = lex_successor(end) {
                end = s;
            } else {
                return Ok(SledRangeBounds {
                    start,
                    end: None,
                    upper_open_ended: true,
                    eq_prefix_guard: if eq_prefix_len > 0 {
                        encode_tuple_values_with_key_spec(&eq_prefix_values, key_spec)?
                    } else {
                        Vec::new()
                    },
                });
            }
        }
        end.push(0);
        (Some(end), false)
    } else {
        (None, true)
    };

    let eq_guard = if eq_prefix_len > 0 { encode_tuple_values_with_key_spec(&eq_prefix_values, key_spec)? } else { Vec::new() };

    Ok(SledRangeBounds { start, end, upper_open_ended, eq_prefix_guard: eq_guard })
}

/// Encode a single component for Sled storage.
pub fn encode_component_for_sled(value: &Value, descending: bool) -> Vec<u8> {
    match value {
        Value::String(s) => {
            if !descending {
                // ASC: [0x10][escaped UTF-8][0x00]
                let mut out = Vec::with_capacity(1 + s.len() + 1);
                out.push(0x10u8);
                for &b in s.as_bytes() {
                    if b == 0x00 {
                        out.push(0x00);
                        out.push(0xFF);
                    } else {
                        out.push(b);
                    }
                }
                out.push(0x00);
                out
            } else {
                // DESC: [0x10][inv(payload) with 0xFF escaped as 0xFF 0x00][0xFF 0xFF]
                let mut out = Vec::with_capacity(1 + s.len() + 1);
                out.push(0x10u8);
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
                out
            }
        }
        Value::I16(_) | Value::I32(_) | Value::I64(_) => {
            // Integers are encoded big-endian (order-preserving). DESC: invert payload bytes.
            let mut out = Vec::with_capacity(1 + 8);
            out.push(0x20u8);
            let bytes = value.to_bytes();
            if !descending {
                out.extend_from_slice(&bytes);
            } else {
                out.extend(bytes.into_iter().map(|b| 0xFFu8.wrapping_sub(b)));
            }
            out
        }
        Value::F64(_) => {
            // F64 uses collation ordering (NaN sorts last, proper IEEE 754 ordering). DESC: invert payload bytes.
            let mut out = Vec::with_capacity(1 + 8);
            out.push(0x30u8);
            let bytes = value.to_bytes();
            if !descending {
                out.extend_from_slice(&bytes);
            } else {
                out.extend(bytes.into_iter().map(|b| 0xFFu8.wrapping_sub(b)));
            }
            out
        }
        Value::Bool(_) => {
            // ASC: false(0) < true(1). DESC: invert payload to flip order.
            let mut out = Vec::with_capacity(1 + 1);
            out.push(0x40u8);
            let b = value.to_bytes()[0];
            out.push(if !descending { b } else { 0xFFu8.wrapping_sub(b) });
            out
        }
        Value::EntityId(entity_id) => {
            // EntityId uses binary collation of the ULID bytes with prefix 0x35
            let bytes = entity_id.to_bytes();
            let mut out = Vec::with_capacity(1 + 16);
            out.push(0x35u8);
            if !descending {
                out.extend_from_slice(&bytes);
            } else {
                out.extend(bytes.into_iter().map(|b| 0xFFu8.wrapping_sub(b)));
            }
            out
        }
        Value::Object(bytes) | Value::Binary(bytes) => {
            if !descending {
                // ASC: [0x50][escaped bytes][0x00]
                let mut out = Vec::with_capacity(1 + bytes.len() + 1);
                out.push(0x50u8);
                for &b in bytes {
                    if b == 0x00 {
                        out.push(0x00);
                        out.push(0xFF);
                    } else {
                        out.push(b);
                    }
                }
                out.push(0x00);
                out
            } else {
                // DESC: [0x50][inv(bytes) with 0xFF escaped as 0xFF 0x00][0xFF 0xFF]
                let mut out = Vec::with_capacity(1 + bytes.len() + 1);
                out.push(0x50u8);
                for &b in bytes {
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
                out
            }
        }
    }
}

/// Type-aware component encoding without type tags - requires KeySpec for type validation
pub fn encode_component_typed(value: &Value, expected_type: ValueType, descending: bool) -> Result<Vec<u8>, IndexError> {
    // Validate that the value matches the expected type
    if ValueType::of(value) != expected_type {
        return Err(IndexError::TypeMismatch(expected_type, ValueType::of(value)));
    }

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
        (Value::Bool(_), ValueType::Bool) => {
            // ASC: false(0) < true(1). DESC: invert payload to flip order.
            let b = value.to_bytes()[0];
            Ok(vec![if !descending { b } else { 0xFFu8.wrapping_sub(b) }])
        }
        (Value::Object(bytes) | Value::Binary(bytes), ValueType::Binary) => {
            if !descending {
                // ASC: [escaped bytes][0x00] - no type tag needed
                let mut out = Vec::with_capacity(bytes.len() + 1);
                for &b in bytes {
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
                for &b in bytes {
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

/// Compute the lexicographic successor of a composite tuple encoding
///
/// This is different from `Collatable::successor_bytes()` in several important ways:
///
/// 1. **Input**: Works on encoded composite keys (multiple fields encoded together with type tags)
///    vs. individual logical values
/// 2. **Algorithm**: Pure bytewise arithmetic (increment with carry) vs. type-aware logic
/// 3. **Purpose**: Creates tight byte ranges for Sled storage layer vs. query planning logic
///
/// Example: For encoded key `[0x10, 0x41, 0x6C, 0x69, 0x63, 0x65, 0x00]` (String "Alice")
/// - This function: `[0x10, 0x41, 0x6C, 0x69, 0x63, 0x65, 0x01]` (increment last byte)
/// - Collatable: Would work on "Alice" logically → "Alice\0"
///
/// This function is essential for creating exclusive upper bounds in Sled ranges, allowing
/// us to convert inclusive bounds to exclusive ones for proper range scanning.
pub fn lex_successor(mut key: Vec<u8>) -> Option<Vec<u8>> {
    // True bytewise successor: increment with carry; if overflow (all 0xFF), no successor
    for i in (0..key.len()).rev() {
        if key[i] != 0xFF {
            key[i] += 1;
            for j in (i + 1)..key.len() {
                key[j] = 0x00;
            }
            return Some(key);
        }
    }
    None
}

/// Type-aware encoding using KeySpec for validation and optimization
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
mod order_inversion_tests {
    use super::*;

    fn cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering { a.cmp(b) }

    #[test]
    fn string_inversion_reverses_order() {
        let a = Value::String("Album".into());
        let b = Value::String("Album-with-dashes".into());
        let asc_a = encode_component_for_sled(&a, false);
        let asc_b = encode_component_for_sled(&b, false);
        assert!(cmp(&asc_a, &asc_b) == std::cmp::Ordering::Less);

        let desc_a = encode_component_for_sled(&a, true);
        let desc_b = encode_component_for_sled(&b, true);
        assert!(cmp(&desc_a, &desc_b) == std::cmp::Ordering::Greater);
    }

    #[test]
    fn string_with_nulls_roundtrips_and_orders() {
        let a = Value::String("A\0B".into());
        let b = Value::String("A\0B\0C".into());
        let asc_a = encode_component_for_sled(&a, false);
        let asc_b = encode_component_for_sled(&b, false);
        assert!(cmp(&asc_a, &asc_b) == std::cmp::Ordering::Less);

        let desc_a = encode_component_for_sled(&a, true);
        let desc_b = encode_component_for_sled(&b, true);
        assert!(cmp(&desc_a, &desc_b) == std::cmp::Ordering::Greater);
    }

    #[test]
    fn integers_inversion_reverses_order() {
        let a = Value::I64(42);
        let b = Value::I64(100);
        let asc_a = encode_component_for_sled(&a, false);
        let asc_b = encode_component_for_sled(&b, false);
        assert!(cmp(&asc_a, &asc_b) == std::cmp::Ordering::Less);

        let desc_a = encode_component_for_sled(&a, true);
        let desc_b = encode_component_for_sled(&b, true);
        assert!(cmp(&desc_a, &desc_b) == std::cmp::Ordering::Greater);
    }

    #[test]
    fn binary_inversion_reverses_order() {
        let a = Value::Binary(vec![0x00, 0x10, 0x20]);
        let b = Value::Binary(vec![0x00, 0x10, 0x21]);
        let asc_a = encode_component_for_sled(&a, false);
        let asc_b = encode_component_for_sled(&b, false);
        assert!(cmp(&asc_a, &asc_b) == std::cmp::Ordering::Less);

        let desc_a = encode_component_for_sled(&a, true);
        let desc_b = encode_component_for_sled(&b, true);
        assert!(cmp(&desc_a, &desc_b) == std::cmp::Ordering::Greater);
    }
}
