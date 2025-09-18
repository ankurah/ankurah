use ankurah_core::property::PropertyValue;
use ankurah_storage_common::{CanonicalRange, Endpoint, KeyBounds, KeyDatum, ScanDirection};
use anyhow::Result;
use wasm_bindgen::JsValue;

/// Normalize IndexBounds to CanonicalRange following the playbook algorithm
/// Returns (CanonicalRange, eq_prefix_len, eq_prefix_values)
pub fn normalize(bounds: &KeyBounds) -> (CanonicalRange, usize, Vec<PropertyValue>) {
    let mut lower_tuple = Vec::new();
    let mut upper_tuple = Vec::new();
    let mut lower_open = false;
    let mut upper_open = false;
    let mut eq_prefix_len = 0;
    let mut eq_prefix_values = Vec::new();

    // Step 1: Accumulate equality prefix
    for bound in &bounds.keyparts {
        // Check if this is an equality constraint (low == high and both inclusive)
        if let (Endpoint::Value { datum: low_datum, inclusive: low_incl }, Endpoint::Value { datum: high_datum, inclusive: high_incl }) =
            (&bound.low, &bound.high)
        {
            if let (KeyDatum::Val(low_val), KeyDatum::Val(high_val)) = (low_datum, high_datum) {
                if low_val == high_val && *low_incl && *high_incl {
                    // This is an equality constraint
                    lower_tuple.push(low_val.clone());
                    upper_tuple.push(high_val.clone());
                    eq_prefix_len += 1;
                    eq_prefix_values.push(low_val.clone());
                    continue;
                }
            }
        }

        // Step 2: At first non-equality column, process lower and upper sides

        // Process lower side
        match &bound.low {
            Endpoint::UnboundedLow(_) => {
                // Keep lower at equality prefix; do not break. We'll process upper next.
            }
            Endpoint::Value { datum: KeyDatum::Val(val), inclusive } => {
                lower_tuple.push(val.clone());
                lower_open = !inclusive;
            }
            _ => break, // Other cases like infinity values
        }

        // Process upper side
        match &bound.high {
            Endpoint::UnboundedHigh(_) => {
                // upper = None (open-ended)
                return (CanonicalRange { lower: Some((lower_tuple, lower_open)), upper: None }, eq_prefix_len, eq_prefix_values);
            }
            Endpoint::Value { datum: KeyDatum::Val(val), inclusive } => {
                upper_tuple.push(val.clone());
                upper_open = !inclusive;
            }
            _ => {
                // Other cases - treat as open-ended
                return (CanonicalRange { lower: Some((lower_tuple, lower_open)), upper: None }, eq_prefix_len, eq_prefix_values);
            }
        }

        // Stop after processing first inequality column
        break;
    }

    // Equality-only bounds: if we have equality on exactly one leading column
    // and no inequality processed, prefer a prefix-open scan to cover full suffix.
    // If there are multiple equality columns present, keep exact closed range.
    if eq_prefix_len == bounds.keyparts.len() && eq_prefix_len == 1 {
        return (CanonicalRange { lower: Some((lower_tuple, lower_open)), upper: None }, eq_prefix_len, eq_prefix_values);
    }

    // Build final CanonicalRange for inequality cases
    let canonical_range = CanonicalRange {
        lower: if lower_tuple.is_empty() { None } else { Some((lower_tuple, lower_open)) },
        upper: if upper_tuple.is_empty() { None } else { Some((upper_tuple, upper_open)) },
    };

    (canonical_range, eq_prefix_len, eq_prefix_values)
}

/// Convert CanonicalRange to IdbKeyRange following the playbook
/// Returns (IdbKeyRange, upper_open_ended_flag)
///
/// TODO: Implement micro-optimizations from playbook section 6:
/// - Use IDBKeyRange.only() for exact equality matches
/// - Support openKeyCursor() for key-only scans
/// - Implement reverse scan early stop optimization
pub fn to_idb_keyrange(canonical_range: &CanonicalRange) -> Result<(web_sys::IdbKeyRange, bool)> {
    match (&canonical_range.lower, &canonical_range.upper) {
        // Upper = None (open-ended)
        (Some((lower_tuple, lower_open)), None) => {
            let lower_js = idb_key_tuple(lower_tuple)?;
            let range = web_sys::IdbKeyRange::lower_bound_with_open(&lower_js, *lower_open)
                .map_err(|e| anyhow::anyhow!("Failed to create lower bound IdbKeyRange: {:?}", e))?;
            Ok((range, true)) // upper_open_ended = true
        }

        // Finite lower & upper
        (Some((lower_tuple, lower_open)), Some((upper_tuple, upper_open))) => {
            let lower_js = idb_key_tuple(lower_tuple)?;
            let upper_js = idb_key_tuple(upper_tuple)?;
            let range = web_sys::IdbKeyRange::bound_with_lower_open_and_upper_open(&lower_js, &upper_js, *lower_open, *upper_open)
                .map_err(|e| anyhow::anyhow!("Failed to create bound IdbKeyRange: {:?}", e))?;
            Ok((range, false)) // upper_open_ended = false
        }

        // Only upper bound
        (None, Some((upper_tuple, upper_open))) => {
            let upper_js = idb_key_tuple(upper_tuple)?;
            let range = web_sys::IdbKeyRange::upper_bound_with_open(&upper_js, *upper_open)
                .map_err(|e| anyhow::anyhow!("Failed to create upper bound IdbKeyRange: {:?}", e))?;
            Ok((range, false)) // upper_open_ended = false
        }

        // Completely unbounded - no range needed
        (None, None) => {
            // This case should probably be handled at a higher level
            Err(anyhow::anyhow!("Cannot create IdbKeyRange for completely unbounded range"))
        }
    }
}

/// Build JS key tuples from PropertyValue arrays (section 2.1 of playbook)
fn idb_key_tuple(parts: &[PropertyValue]) -> Result<JsValue> {
    let arr = js_sys::Array::new();
    for p in parts {
        let js_val = match p {
            PropertyValue::I16(x) => JsValue::from_f64(*x as f64),
            PropertyValue::I32(x) => JsValue::from_f64(*x as f64),
            PropertyValue::I64(x) => encode_i64_for_idb(*x)?,
            PropertyValue::Bool(b) => JsValue::from_f64(if *b { 1.0 } else { 0.0 }),
            PropertyValue::String(s) => JsValue::from_str(s),
            PropertyValue::Object(bytes) | PropertyValue::Binary(bytes) => {
                let u8_array = unsafe { js_sys::Uint8Array::view(bytes) };
                u8_array.buffer().into()
            }
        };
        arr.push(&js_val);
    }
    Ok(arr.into())
}

/// Encode i64 for IndexedDB as order-preserving string (section 4.2 of playbook)
fn encode_i64_for_idb(x: i64) -> Result<JsValue> {
    const BIAS: i128 = (i64::MAX as i128) - (i64::MIN as i128);
    let u = (x as i128) - (i64::MIN as i128); // [0 .. 2^64-1] as i128
    Ok(JsValue::from_str(&format!("{:020}", u))) // zero-padded => lex order == numeric
}

/// Convert Plan bounds to IndexedDB IdbKeyRange using the new IR pipeline
/// Returns (IdbKeyRange, upper_open_ended_flag, eq_prefix_len, eq_prefix_values)
pub fn plan_bounds_to_idb_range(bounds: &KeyBounds) -> Result<(web_sys::IdbKeyRange, bool, usize, Vec<PropertyValue>)> {
    // Step 1: Normalize IR to CanonicalRange
    let (canonical_range, eq_prefix_len, eq_prefix_values) = normalize(bounds);

    // Step 2: Convert CanonicalRange to IdbKeyRange
    let (idb_range, upper_open_ended) = to_idb_keyrange(&canonical_range)?;

    Ok((idb_range, upper_open_ended, eq_prefix_len, eq_prefix_values))
}

/// Convert scan direction to IndexedDB cursor direction
pub fn scan_direction_to_cursor_direction(scan_direction: &ScanDirection) -> web_sys::IdbCursorDirection {
    match scan_direction {
        ScanDirection::Forward => web_sys::IdbCursorDirection::Next,
        ScanDirection::Reverse => web_sys::IdbCursorDirection::Prev,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::Predicate;
    use ankurah_storage_common::{IndexKeyPart, KeyBoundComponent, KeySpec, Plan, ValueType};

    #[test]
    fn test_plan_index_spec_name() {
        let plan = Plan::Index {
            index_spec: KeySpec::new(vec![
                IndexKeyPart::asc("__collection", ValueType::String),
                IndexKeyPart::asc("age", ValueType::I32),
                IndexKeyPart::asc("score", ValueType::I32),
            ]),
            scan_direction: ScanDirection::Forward,
            bounds: KeyBounds::new(vec![]), // Empty bounds for this test
            remaining_predicate: Predicate::True,
            order_by_spill: vec![],
        };

        if let Plan::Index { index_spec, .. } = plan {
            let index_name = index_spec.name_with("", "__");
            assert_eq!(index_name, "__collection asc__age asc__score asc");
        }
    }

    #[test]
    fn test_scan_direction_to_cursor_direction() {
        let asc_direction = scan_direction_to_cursor_direction(&ScanDirection::Forward);
        let desc_direction = scan_direction_to_cursor_direction(&ScanDirection::Reverse);

        // Verify the directions are different and correct
        assert_ne!(asc_direction as u32, desc_direction as u32);
        assert_eq!(asc_direction as u32, web_sys::IdbCursorDirection::Next as u32);
        assert_eq!(desc_direction as u32, web_sys::IdbCursorDirection::Prev as u32);
    }

    #[test]
    fn test_normalize_equality_only() {
        // Test normalization of equality-only bounds: __collection = 'album' AND age = 30
        let bounds = KeyBounds::new(vec![
            KeyBoundComponent {
                column: "__collection".to_string(),
                low: Endpoint::incl(PropertyValue::String("album".to_string())),
                high: Endpoint::incl(PropertyValue::String("album".to_string())),
            },
            KeyBoundComponent {
                column: "age".to_string(),
                low: Endpoint::incl(PropertyValue::I32(30)),
                high: Endpoint::incl(PropertyValue::I32(30)),
            },
        ]);

        let (canonical_range, eq_prefix_len, eq_prefix_values) = normalize(&bounds);

        // Should have both values in equality prefix
        assert_eq!(eq_prefix_len, 2);
        assert_eq!(eq_prefix_values, vec![PropertyValue::String("album".to_string()), PropertyValue::I32(30)]);

        // Both lower and upper should be the same (exact match)
        assert_eq!(canonical_range.lower, Some((vec![PropertyValue::String("album".to_string()), PropertyValue::I32(30)], false))); // closed bounds
        assert_eq!(canonical_range.upper, Some((vec![PropertyValue::String("album".to_string()), PropertyValue::I32(30)], false)));
        // closed bounds
    }

    #[test]
    fn test_normalize_with_inequality() {
        // Test normalization with inequality: __collection = 'album' AND age > 25
        let bounds = KeyBounds::new(vec![
            KeyBoundComponent {
                column: "__collection".to_string(),
                low: Endpoint::incl(PropertyValue::String("album".to_string())),
                high: Endpoint::incl(PropertyValue::String("album".to_string())),
            },
            KeyBoundComponent {
                column: "age".to_string(),
                low: Endpoint::excl(PropertyValue::I32(25)),
                high: Endpoint::UnboundedHigh(ValueType::I32),
            },
        ]);

        let (canonical_range, eq_prefix_len, eq_prefix_values) = normalize(&bounds);

        // Should have one equality in prefix
        assert_eq!(eq_prefix_len, 1);
        assert_eq!(eq_prefix_values, vec![PropertyValue::String("album".to_string())]);

        // Lower bound should include equality + inequality
        assert_eq!(canonical_range.lower, Some((vec![PropertyValue::String("album".to_string()), PropertyValue::I32(25)], true))); // open because > 25

        // Upper bound should be None (open-ended)
        assert_eq!(canonical_range.upper, None);
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn test_plan_bounds_to_idb_range() {
        // Test the full pipeline: IndexBounds → CanonicalRange → IdbKeyRange
        let bounds = KeyBounds::new(vec![KeyBoundComponent {
            column: "__collection".to_string(),
            low: Endpoint::incl(PropertyValue::String("album".to_string())),
            high: Endpoint::incl(PropertyValue::String("album".to_string())),
        }]);

        let result = plan_bounds_to_idb_range(&bounds);
        assert!(result.is_ok());

        let (_idb_range, upper_open_ended, eq_prefix_len, eq_prefix_values) = result.unwrap();

        // Should be exact match (not open-ended)
        assert!(!upper_open_ended);
        assert_eq!(eq_prefix_len, 1);
        assert_eq!(eq_prefix_values, vec![PropertyValue::String("album".to_string())]);
    }
}
