// Refactored planner integration - cleaner and more cohesive

use ankurah_core::indexing::KeySpec;
use ankurah_core::value::ValueType;
use ankurah_core::value::Value;
use ankurah_storage_common::{Endpoint, KeyBounds, KeyDatum};

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
            let start = encoded_prefix.clone();

            return Ok(SledRangeBounds { start, end: None, upper_open_ended: true, eq_prefix_guard: encoded_prefix });
        } else {
            // Single-key or full equality: use tight range
            let start = encoded_prefix.clone();
            let end = lex_successor(start.clone());

            return Ok(SledRangeBounds {
                start,
                end,
                upper_open_ended: false,
                eq_prefix_guard: Vec::new(), // No guard needed
            });
        }
    }

    // =====================================================================================
    // Case 2: DESC inequality - requires bound inversion
    // =====================================================================================
    //
    // WHY DESC NEEDS SPECIAL HANDLING:
    //
    // DESC columns invert each byte during encoding (0xFF - b), which reverses the
    // lexicographic order. This means logical bounds must be SWAPPED to produce the
    // correct physical scan range.
    //
    // Example with 2-byte integers (simplified):
    //
    //   Logical values:    5 < 10
    //   ASC encoding:      [0x00, 0x05] < [0x00, 0x0A]  ✓ same order
    //   DESC encoding:     [0xFF, 0xFA] > [0xFF, 0xF5]  ✗ REVERSED order
    //
    // So for a query like `timestamp > 5` on a DESC column:
    //
    //   Logical meaning:   "values greater than 5" → {6, 7, 8, 9, 10, ...}
    //   ASC scan range:    enc(5) to end            → [0x00,0x05] to [0xFF,0xFF]
    //   DESC scan range:   start to enc(5)          → [0x00,0x00] to [0xFF,0xFA]
    //                      ↑ INVERTED! larger values have SMALLER bytes in DESC
    //
    // Similarly for `timestamp <= 5` on DESC:
    //
    //   Logical meaning:   "values ≤ 5" → {..., 3, 4, 5}
    //   ASC scan range:    start to succ(enc(5))   → [0x00,0x00] to [0x00,0x06]
    //   DESC scan range:   enc(5) to end           → [0xFF,0xFA] to [0xFF,0xFF]
    //                      ↑ INVERTED! smaller values have LARGER bytes in DESC
    //
    // ASC columns don't need this because logical order matches byte order.
    //
    // ---------------------------------------------------------------------------------
    // PR #212 BUG: The inequality column index matters!
    // ---------------------------------------------------------------------------------
    //
    // For composite indexes with equality prefix + DESC inequality:
    //   Index:  [room ASC, deleted ASC, timestamp DESC]
    //   Query:  room = 'abc' AND deleted = false AND timestamp <= 1000
    //
    // The inequality column (timestamp) is at index 2, NOT index 0.
    //
    //   eq_prefix_len = 2  (room and deleted are equality predicates)
    //   inequality column = keyparts[2] = timestamp (DESC)
    //
    // OLD BUG: checked keyparts[0].is_desc() → room is ASC → FALSE → wrong path!
    // FIX:     check keyparts[eq_prefix_len].is_desc() → timestamp is DESC → TRUE
    // =====================================================================================

    let ineq_lower_len = lower_tuple.len().saturating_sub(eq_prefix_len);
    let ineq_upper_len = upper_tuple.len().saturating_sub(eq_prefix_len);

    if let Some(ineq_keypart) = key_spec.keyparts.get(eq_prefix_len) {
        if ineq_keypart.direction.is_desc() {
            // Single-component inequality (0 or 1 values beyond the equality prefix)
            let is_single = ineq_lower_len <= 1 && ineq_upper_len <= 1;
            if is_single {
                return handle_desc_inequality(lower_tuple, upper_tuple, lower_open, upper_open, eq_prefix_len, eq_prefix_values, key_spec);
            }
        }
    }

    // Case 3: General bounds (ASC inequality or multi-component)
    // ASC columns don't need bound inversion - logical order matches byte order
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

            let mut end = encode_tuple_values_with_key_spec(&lower_tuple, key_spec)?;
            if !lower_open {
                if let Some(s) = lex_successor(end.clone()) {
                    end = s;
                }
            }
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
        (Some(end), false)
    } else {
        (None, true)
    };

    let eq_guard = if eq_prefix_len > 0 { encode_tuple_values_with_key_spec(&eq_prefix_values, key_spec)? } else { Vec::new() };

    Ok(SledRangeBounds { start, end, upper_open_ended, eq_prefix_guard: eq_guard })
}

/// Type-aware component encoding without type tags - requires KeySpec for type validation
/// Delegates to core encoding for consistency
pub fn encode_component_typed(value: &Value, expected_type: ValueType, descending: bool) -> Result<Vec<u8>, IndexError> {
    match ankurah_core::indexing::encode_component_typed(value, expected_type, descending) {
        Ok(bytes) => Ok(bytes),
        Err(core_err) => {
            // Convert core IndexError to sled IndexError
            match core_err {
                ankurah_core::indexing::IndexError::TypeMismatch(expected, got) => Err(IndexError::TypeMismatch(expected, got)),
            }
        }
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
/// Delegates to core encoding for consistency
pub fn encode_tuple_values_with_key_spec(values: &[Value], key_spec: &KeySpec) -> Result<Vec<u8>, IndexError> {
    match ankurah_core::indexing::encode_tuple_values_with_key_spec(values, key_spec) {
        Ok(bytes) => Ok(bytes),
        Err(core_err) => match core_err {
            ankurah_core::indexing::IndexError::TypeMismatch(expected, got) => Err(IndexError::TypeMismatch(expected, got)),
        },
    }
}
