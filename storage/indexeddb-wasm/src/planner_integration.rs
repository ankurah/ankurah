use ankurah_core::value::Value;
use ankurah_storage_common::{CanonicalRange, Endpoint, KeyBounds, KeyDatum, ScanDirection};
use anyhow::Result;
use wasm_bindgen::JsValue;

/// Normalize IndexBounds to CanonicalRange following the playbook algorithm
/// Returns (CanonicalRange, eq_prefix_len, eq_prefix_values)
pub fn normalize(bounds: &KeyBounds) -> (CanonicalRange, usize, Vec<Value>) {
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
/// Uses canonical IdbValue encoding for symmetric handling across storage, ranges, and guards
fn idb_key_tuple(parts: &[Value]) -> Result<JsValue> {
    let arr = js_sys::Array::new();
    for p in parts {
        let js_val: JsValue = crate::idb_value::IdbValue::from(p).into();
        arr.push(&js_val);
    }
    Ok(arr.into())
}

/// Convert Plan bounds to IndexedDB IdbKeyRange using the new IR pipeline
/// Returns (IdbKeyRange, upper_open_ended_flag, eq_prefix_len, eq_prefix_values)
pub fn plan_bounds_to_idb_range(bounds: &KeyBounds) -> Result<(web_sys::IdbKeyRange, bool, usize, Vec<Value>)> {
    // Step 1: Normalize IR to CanonicalRange
    let (canonical_range, eq_prefix_len, eq_prefix_values) = normalize(bounds);

    // Step 2: Convert CanonicalRange to IdbKeyRange
    let (idb_range, upper_open_ended) = to_idb_keyrange(&canonical_range)?;

    Ok((idb_range, upper_open_ended, eq_prefix_len, eq_prefix_values))
}

#[allow(unused)]
pub fn plan_bounds_to_idb_range_syntax(bounds: &KeyBounds) -> Result<String> {
    use std::fmt::Write;

    // Get the normalized range and encoding info
    let (canonical_range, eq_prefix_len, eq_prefix_values) = normalize(bounds);

    // For debugging, let's trace the input bounds
    tracing::info!("plan_bounds_to_idb_range_syntax input: {:?}", bounds);
    tracing::info!("Normalized canonical_range: {:?}", canonical_range);
    tracing::info!("eq_prefix_len: {}, eq_prefix_values: {:?}", eq_prefix_len, eq_prefix_values);

    let mut js_code = String::new();

    match (&canonical_range.lower, &canonical_range.upper) {
        (Some((lower_tuple, lower_open)), Some((upper_tuple, upper_open))) => {
            // Both bounds exist - use bound()
            write!(js_code, "IDBKeyRange.bound(")?;
            write!(js_code, "{}", values_to_js_array(lower_tuple)?)?;
            write!(js_code, ", ")?;
            write!(js_code, "{}", values_to_js_array(upper_tuple)?)?;
            write!(js_code, ", {}, {}", lower_open, upper_open)?;
            write!(js_code, ")")?;
        }
        (Some((lower_tuple, lower_open)), None) => {
            // Only lower bound - use lowerBound()
            write!(js_code, "IDBKeyRange.lowerBound(")?;
            write!(js_code, "{}", values_to_js_array(lower_tuple)?)?;
            write!(js_code, ", {})", lower_open)?;
        }
        (None, Some((upper_tuple, upper_open))) => {
            // Only upper bound - use upperBound()
            write!(js_code, "IDBKeyRange.upperBound(")?;
            write!(js_code, "{}", values_to_js_array(upper_tuple)?)?;
            write!(js_code, ", {})", upper_open)?;
        }
        (None, None) => {
            return Err(anyhow::anyhow!("Cannot generate syntax for completely unbounded range"));
        }
    }

    tracing::info!("Generated IDBKeyRange syntax: {}", js_code);

    Ok(js_code)
}

/// Convert Value array to JavaScript array syntax
/// This replicates the encoding logic from idb_key_tuple() but generates string syntax
/// Note: Uses IndexedDB key encoding (i64 as raw numbers, bool as 0/1)
fn values_to_js_array(values: &[Value]) -> Result<String> {
    let mut result = String::from("[");

    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            result.push_str(", ");
        }

        match value {
            Value::String(s) => {
                // Same as: JsValue::from_str(s)
                result.push('"');
                result.push_str(&s.replace("\\", "\\\\").replace("\"", "\\\""));
                result.push('"');
            }
            Value::I64(x) => {
                // Same as: JsValue::from_f64(*x as f64) - use raw number
                result.push_str(&x.to_string());
            }
            Value::I32(x) => {
                // Same as: JsValue::from_f64(*x as f64)
                result.push_str(&x.to_string());
            }
            Value::I16(x) => {
                // Same as: JsValue::from_f64(*x as f64)
                result.push_str(&x.to_string());
            }
            Value::F64(x) => {
                // Same as: JsValue::from_f64(*x)
                if x.fract() == 0.0 {
                    result.push_str(&format!("{:.0}", x));
                } else {
                    result.push_str(&x.to_string());
                }
            }
            Value::Bool(b) => {
                // Same as: JsValue::from_f64(if *b { 1.0 } else { 0.0 })
                // IndexedDB keys don't support boolean, use 0/1
                result.push_str(if *b { "1" } else { "0" });
            }
            Value::EntityId(entity_id) => {
                // Same as: JsValue::from_str(&entity_id.to_base64())
                result.push('"');
                result.push_str(&entity_id.to_base64());
                result.push('"');
            }
            Value::Object(_) | Value::Binary(_) => {
                // Same as idb_key_tuple: converts to ArrayBuffer
                // For syntax generation, we can't easily represent this
                return Err(anyhow::anyhow!("Object and Binary values not supported in key syntax generation: {:?}", value));
            }
        }
    }

    result.push(']');
    Ok(result)
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
    use ankurah_core::indexing::{IndexKeyPart, KeySpec};
    use ankurah_core::value::ValueType;
    use ankurah_storage_common::{KeyBoundComponent, Plan};

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
                low: Endpoint::incl(Value::String("album".to_string())),
                high: Endpoint::incl(Value::String("album".to_string())),
            },
            KeyBoundComponent { column: "age".to_string(), low: Endpoint::incl(Value::I32(30)), high: Endpoint::incl(Value::I32(30)) },
        ]);

        let (canonical_range, eq_prefix_len, eq_prefix_values) = normalize(&bounds);

        // Should have both values in equality prefix
        assert_eq!(eq_prefix_len, 2);
        assert_eq!(eq_prefix_values, vec![Value::String("album".to_string()), Value::I32(30)]);

        // Both lower and upper should be the same (exact match)
        assert_eq!(canonical_range.lower, Some((vec![Value::String("album".to_string()), Value::I32(30)], false))); // closed bounds
        assert_eq!(canonical_range.upper, Some((vec![Value::String("album".to_string()), Value::I32(30)], false)));
        // closed bounds
    }

    #[test]
    fn test_normalize_with_inequality() {
        // Test normalization with inequality: __collection = 'album' AND age > 25
        let bounds = KeyBounds::new(vec![
            KeyBoundComponent {
                column: "__collection".to_string(),
                low: Endpoint::incl(Value::String("album".to_string())),
                high: Endpoint::incl(Value::String("album".to_string())),
            },
            KeyBoundComponent {
                column: "age".to_string(),
                low: Endpoint::excl(Value::I32(25)),
                high: Endpoint::UnboundedHigh(ValueType::I32),
            },
        ]);

        let (canonical_range, eq_prefix_len, eq_prefix_values) = normalize(&bounds);

        // Should have one equality in prefix
        assert_eq!(eq_prefix_len, 1);
        assert_eq!(eq_prefix_values, vec![Value::String("album".to_string())]);

        // Lower bound should include equality + inequality
        assert_eq!(canonical_range.lower, Some((vec![Value::String("album".to_string()), Value::I32(25)], true))); // open because > 25

        // Upper bound should be None (open-ended)
        assert_eq!(canonical_range.upper, None);
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn test_plan_bounds_to_idb_range() {
        // Test the full pipeline: IndexBounds → CanonicalRange → IdbKeyRange
        let bounds = KeyBounds::new(vec![KeyBoundComponent {
            column: "__collection".to_string(),
            low: Endpoint::incl(Value::String("album".to_string())),
            high: Endpoint::incl(Value::String("album".to_string())),
        }]);

        let result = plan_bounds_to_idb_range(&bounds);
        assert!(result.is_ok());

        let (_idb_range, upper_open_ended, eq_prefix_len, eq_prefix_values) = result.unwrap();

        // Should be exact match (not open-ended)
        assert!(!upper_open_ended);
        assert_eq!(eq_prefix_len, 1);
        assert_eq!(eq_prefix_values, vec![Value::String("album".to_string())]);
    }

    #[test]
    fn test_plan_bounds_to_idb_range_syntax() {
        // Test the syntax generation for your exact bounds from the debug print
        let bounds = KeyBounds::new(vec![
            KeyBoundComponent {
                column: "__collection".to_string(),
                low: Endpoint::incl(Value::String("connectionevent".to_string())),
                high: Endpoint::incl(Value::String("connectionevent".to_string())),
            },
            KeyBoundComponent {
                column: "user_id".to_string(),
                low: Endpoint::incl(Value::String("AZoegTHj_4vcBoJ5FfY-Xw".to_string())),
                high: Endpoint::incl(Value::String("AZoegTHj_4vcBoJ5FfY-Xw".to_string())),
            },
            KeyBoundComponent {
                column: "timestamp".to_string(),
                low: Endpoint::excl(Value::I64(1761455267792)),
                high: Endpoint::excl(Value::I64(1761456167793)),
            },
        ]);

        let result = plan_bounds_to_idb_range_syntax(&bounds);
        assert!(result.is_ok());

        let js_syntax = result.unwrap();
        println!("Generated JavaScript syntax: {}", js_syntax);

        // Should generate IDBKeyRange.bound with raw i64 numbers (matches From<&Value>)
        assert!(js_syntax.contains("IDBKeyRange.bound"));
        assert!(js_syntax.contains("\"connectionevent\""));
        assert!(js_syntax.contains("\"AZoegTHj_4vcBoJ5FfY-Xw\""));
        // i64 values as raw numbers (not encoded strings)
        assert!(js_syntax.contains("1761455267792")); // Lower bound as raw number
        assert!(js_syntax.contains("1761456167793")); // Upper bound as raw number
        assert!(js_syntax.contains("true, true")); // Both bounds are exclusive
    }

    #[test]
    fn test_plan_bounds_to_idb_range_syntax_equality_only() {
        // Test with equality-only bounds on a single column
        // Per normalize() line 71-72: single equality becomes open-ended upper bound
        let bounds = KeyBounds::new(vec![KeyBoundComponent {
            column: "__collection".to_string(),
            low: Endpoint::incl(Value::String("album".to_string())),
            high: Endpoint::incl(Value::String("album".to_string())),
        }]);

        let result = plan_bounds_to_idb_range_syntax(&bounds);
        assert!(result.is_ok());

        let js_syntax = result.unwrap();
        println!("Generated JavaScript syntax for single equality: {}", js_syntax);

        // Single equality becomes lowerBound (open-ended) per normalize() logic
        assert!(js_syntax.contains("IDBKeyRange.lowerBound"));
        assert!(js_syntax.contains("\"album\""));
        assert!(js_syntax.contains("false)")); // Inclusive lower bound
    }

    #[test]
    fn test_plan_bounds_to_idb_range_syntax_multi_equality() {
        // Test with equality on multiple columns
        // Per normalize() line 71-72: multiple equalities use exact closed range
        let bounds = KeyBounds::new(vec![
            KeyBoundComponent {
                column: "__collection".to_string(),
                low: Endpoint::incl(Value::String("album".to_string())),
                high: Endpoint::incl(Value::String("album".to_string())),
            },
            KeyBoundComponent {
                column: "year".to_string(),
                low: Endpoint::incl(Value::String("2000".to_string())),
                high: Endpoint::incl(Value::String("2000".to_string())),
            },
        ]);

        let result = plan_bounds_to_idb_range_syntax(&bounds);
        assert!(result.is_ok());

        let js_syntax = result.unwrap();
        println!("Generated JavaScript syntax for multi-equality: {}", js_syntax);

        // Multiple equalities use bound() with exact match
        assert!(js_syntax.contains("IDBKeyRange.bound"));
        assert!(js_syntax.contains("\"album\""));
        assert!(js_syntax.contains("\"2000\""));
        assert!(js_syntax.contains("false, false")); // Both bounds inclusive (closed)
    }
}
