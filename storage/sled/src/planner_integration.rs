//! Converting the planner's logical key bounds into sled byte ranges.
//!
//! An index tree is keyed by the canonical tuple encoding followed by the
//! entity id (see `index.rs`). Every scan range here is built from the two
//! ends of a prefix's key range: because parts encode prefix-free, the keys of
//! the tuples whose leading parts equal `T` are exactly the keys that begin
//! with `encode(T)`, the range `[encode(T), prefix_range_end(encode(T)))`.

use ankurah_core::indexing::{prefix_range_end, KeySpec};
use ankurah_core::value::Value;
use ankurah_core::value::ValueType;
use ankurah_storage_common::{Endpoint, KeyBoundComponent, KeyBounds, KeyDatum};

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
/// The bounds hold equalities on leading parts of the key and at most one
/// inequality on the part after them (the planner bounds no later part). The
/// equalities encode to a prefix; the inequality, when present, narrows that
/// prefix's key range.
///
/// # Arguments
/// * `bounds` - The logical query bounds (e.g., "year >= 1969")
/// * `key_spec` - The key specification including column order and directions
pub fn key_bounds_to_sled_range(bounds: &KeyBounds, key_spec: &KeySpec<String>) -> Result<SledRangeBounds, IndexError> {
    let mut equal = Vec::new();
    let mut inequality = None;
    for bound in &bounds.keyparts {
        match equality_value(bound) {
            Some(value) => equal.push(value.clone()),
            None => {
                inequality = Some(bound);
                break;
            }
        }
    }
    let prefix = encode_tuple_values_with_key_spec(&equal, key_spec)?;

    // The inequality is on the part after the equalities, not on the first
    // part (PR #212). A bound the index has no part for cannot narrow the scan.
    let (Some(bound), Some(part)) = (inequality, key_spec.keyparts.get(equal.len())) else {
        // Equalities on leading parts only: scan the prefix's range open-ended
        // behind a prefix guard. Equalities on the whole key: the tight range.
        return Ok(if key_spec.keyparts.len() > equal.len() {
            SledRangeBounds { start: prefix.clone(), end: None, upper_open_ended: true, eq_prefix_guard: prefix }
        } else {
            let end = prefix_range_end(&prefix);
            SledRangeBounds { start: prefix, upper_open_ended: end.is_none(), end, eq_prefix_guard: Vec::new() }
        });
    };

    // A descending part reverses byte order, so its logical ends swap sides.
    let (low, high) = (endpoint_value(&bound.low), endpoint_value(&bound.high));
    let (byte_low, byte_high) = if part.direction.is_desc() { (high, low) } else { (low, high) };
    let encode = |value: &Value| -> Result<Vec<u8>, IndexError> {
        let mut values = equal.clone();
        values.push(value.clone());
        encode_tuple_values_with_key_spec(&values, key_spec)
    };

    let start = match byte_low {
        None => prefix.clone(),
        Some((value, true)) => encode(value)?,
        Some((value, false)) => match prefix_range_end(&encode(value)?) {
            Some(after) => after,
            // Nothing sorts above an all-0xFF key: no key qualifies.
            None => {
                return Ok(SledRangeBounds {
                    start: prefix.clone(),
                    end: Some(prefix),
                    upper_open_ended: false,
                    eq_prefix_guard: Vec::new(),
                })
            }
        },
    };
    let (end, upper_open_ended) = match byte_high {
        None => (None, true),
        Some((value, false)) => (Some(encode(value)?), false),
        Some((value, true)) => match prefix_range_end(&encode(value)?) {
            Some(after) => (Some(after), false),
            None => (None, true),
        },
    };
    Ok(SledRangeBounds { start, end, upper_open_ended, eq_prefix_guard: prefix })
}

/// The value a bound pins its part to: both ends inclusive on the same value.
fn equality_value(bound: &KeyBoundComponent) -> Option<&Value> {
    match (endpoint_value(&bound.low), endpoint_value(&bound.high)) {
        (Some((low, true)), Some((high, true))) if low == high => Some(low),
        _ => None,
    }
}

/// A finite endpoint's value and whether the bound includes it.
fn endpoint_value(endpoint: &Endpoint) -> Option<(&Value, bool)> {
    match endpoint {
        Endpoint::Value { datum: KeyDatum::Val(value), inclusive } => Some((value, *inclusive)),
        _ => None,
    }
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

/// Type-aware encoding using KeySpec for validation and optimization
/// Delegates to core encoding for consistency
pub fn encode_tuple_values_with_key_spec(values: &[Value], key_spec: &KeySpec<String>) -> Result<Vec<u8>, IndexError> {
    match ankurah_core::indexing::encode_tuple_values_with_key_spec(values, key_spec) {
        Ok(bytes) => Ok(bytes),
        Err(core_err) => match core_err {
            ankurah_core::indexing::IndexError::TypeMismatch(expected, got) => Err(IndexError::TypeMismatch(expected, got)),
        },
    }
}
