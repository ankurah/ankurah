use ankurah_core::property::PropertyValue;
use ankurah_storage_common::planner::{Bound, Range};
use anyhow::Result;
use wasm_bindgen::JsValue;

fn bound_both(lower: &[PropertyValue], upper: &[PropertyValue], lower_open: bool, upper_open: bool) -> Result<web_sys::IdbKeyRange> {
    let lower_array = property_values_to_js_array(lower);
    let upper_array = property_values_to_js_array(upper);
    web_sys::IdbKeyRange::bound_with_lower_open_and_upper_open(&lower_array, &upper_array, lower_open, upper_open)
        .map_err(|e| anyhow::anyhow!("Failed to create IdbKeyRange: {:?}", e))
}

fn bound_lower(lower: &[PropertyValue], open: bool) -> Result<web_sys::IdbKeyRange> {
    let lower_array = property_values_to_js_array(lower);
    web_sys::IdbKeyRange::lower_bound_with_open(&lower_array, open).map_err(|e| anyhow::anyhow!("Failed to create IdbKeyRange: {:?}", e))
}

fn bound_upper(upper: &[PropertyValue], open: bool) -> Result<web_sys::IdbKeyRange> {
    let upper_array = property_values_to_js_array(upper);
    web_sys::IdbKeyRange::upper_bound_with_open(&upper_array, open).map_err(|e| anyhow::anyhow!("Failed to create IdbKeyRange: {:?}", e))
}

/// Compare two PropertyValues for ordering (basic implementation for range checking)
fn compare_property_values(a: &PropertyValue, b: &PropertyValue) -> Option<std::cmp::Ordering> {
    use PropertyValue::*;
    match (a, b) {
        (String(a), String(b)) => Some(a.cmp(b)),
        (I16(a), I16(b)) => Some(a.cmp(b)),
        (I32(a), I32(b)) => Some(a.cmp(b)),
        (I64(a), I64(b)) => Some(a.cmp(b)),
        (Bool(a), Bool(b)) => Some(a.cmp(b)),
        // Cross-type integer comparisons
        (I16(a), I32(b)) => Some((*a as i32).cmp(b)),
        (I16(a), I64(b)) => Some((*a as i64).cmp(b)),
        (I32(a), I16(b)) => Some(a.cmp(&(*b as i32))),
        (I32(a), I64(b)) => Some((*a as i64).cmp(b)),
        (I64(a), I16(b)) => Some(a.cmp(&(*b as i64))),
        (I64(a), I32(b)) => Some(a.cmp(&(*b as i64))),
        // Different types can't be compared
        _ => None,
    }
}

/// Check if a range is impossible (lower bound > upper bound)
/// FIXME: This needs to work for different length bounds
pub fn is_impossible_range(range: &Range) -> bool {
    use Bound::*;
    match (&range.from, &range.to) {
        (Inclusive(lower), Inclusive(upper))
        | (Inclusive(lower), Exclusive(upper))
        | (Exclusive(lower), Inclusive(upper))
        | (Exclusive(lower), Exclusive(upper)) => {
            // Compare the values element by element
            for (l, u) in lower.iter().zip(upper.iter()) {
                match compare_property_values(l, u) {
                    Some(std::cmp::Ordering::Greater) => return true,
                    Some(std::cmp::Ordering::Less) => return false,
                    Some(std::cmp::Ordering::Equal) => continue,
                    None => return false, // Can't compare, assume possible
                }
            }
            // If all elements are equal, check if bounds are exclusive
            match (&range.from, &range.to) {
                (Exclusive(_), _) | (_, Exclusive(_)) => lower == upper,
                _ => false,
            }
        }
        _ => false, // Unbounded ranges are never impossible
    }
}

/// Convert Plan range to IndexedDB IdbKeyRange
pub fn plan_range_to_idb_range(range: &Range) -> Result<Option<web_sys::IdbKeyRange>> {
    use Bound::*;
    Ok(Some(match (&range.from, &range.to) {
        (Unbounded, Unbounded) => return Ok(None),
        (Inclusive(lower), Inclusive(upper)) => bound_both(lower, upper, false, false)?,
        (Inclusive(lower), Exclusive(upper)) => bound_both(lower, upper, false, true)?,
        (Exclusive(lower), Inclusive(upper)) => bound_both(lower, upper, true, false)?,
        (Exclusive(lower), Exclusive(upper)) => bound_both(lower, upper, true, true)?,
        (Inclusive(lower), Unbounded) => bound_lower(lower, false)?,
        (Exclusive(lower), Unbounded) => bound_lower(lower, true)?,
        (Unbounded, Inclusive(upper)) => bound_upper(upper, false)?,
        (Unbounded, Exclusive(upper)) => bound_upper(upper, true)?,
    }))
}

/// Convert PropertyValue array to JavaScript array for IndexedDB
fn property_values_to_js_array(values: &[PropertyValue]) -> js_sys::Array {
    let array = js_sys::Array::new();
    for value in values {
        array.push(&JsValue::from(value));
    }
    array
}

/// Convert scan direction to IndexedDB cursor direction
pub fn scan_direction_to_cursor_direction(scan_direction: &ankurah_storage_common::planner::ScanDirection) -> web_sys::IdbCursorDirection {
    match scan_direction {
        ankurah_storage_common::planner::ScanDirection::Forward => web_sys::IdbCursorDirection::Next,
        ankurah_storage_common::planner::ScanDirection::Reverse => web_sys::IdbCursorDirection::Prev,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::Predicate;
    use ankurah_storage_common::planner::Plan;

    #[test]
    fn test_plan_index_spec_name() {
        use ankurah_storage_common::index_spec::{IndexDirection, IndexField, IndexSpec};

        let plan = Plan {
            index_spec: IndexSpec::new(vec![
                IndexField::new("__collection", IndexDirection::Asc),
                IndexField::new("age", IndexDirection::Asc),
                IndexField::new("score", IndexDirection::Asc),
            ]),
            scan_direction: ankurah_storage_common::planner::ScanDirection::Forward,
            range: Range::new(Bound::Unbounded, Bound::Unbounded),
            remaining_predicate: Predicate::True,
            sort_fields: vec![],
        };

        let index_name = plan.index_spec.name("", "__");
        assert_eq!(index_name, "__collection__age__score");
    }

    #[test]
    fn test_scan_direction_to_cursor_direction() {
        let asc_direction = scan_direction_to_cursor_direction(&ankurah_storage_common::planner::ScanDirection::Forward);
        let desc_direction = scan_direction_to_cursor_direction(&ankurah_storage_common::planner::ScanDirection::Reverse);

        // Verify the directions are different and correct
        assert_ne!(asc_direction as u32, desc_direction as u32);
        assert_eq!(asc_direction as u32, web_sys::IdbCursorDirection::Next as u32);
        assert_eq!(desc_direction as u32, web_sys::IdbCursorDirection::Prev as u32);
    }

    #[test]
    fn test_impossible_range_detection() {
        use ankurah_core::property::PropertyValue;

        // Test impossible range: lower > upper
        let impossible_range = Range::new(
            Bound::Exclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2010".to_string())]),
            Bound::Exclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2005".to_string())]),
        );

        // Test the detection logic directly
        assert!(is_impossible_range(&impossible_range));

        // Test possible range: lower < upper
        let possible_range = Range::new(
            Bound::Exclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2005".to_string())]),
            Bound::Exclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2010".to_string())]),
        );

        // Should not be detected as impossible
        assert!(!is_impossible_range(&possible_range));

        // Test equal bounds with exclusive ranges (should be impossible)
        let equal_exclusive_range = Range::new(
            Bound::Exclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2005".to_string())]),
            Bound::Exclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2005".to_string())]),
        );

        assert!(is_impossible_range(&equal_exclusive_range));

        // Test equal bounds with inclusive ranges (should be possible)
        let equal_inclusive_range = Range::new(
            Bound::Inclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2005".to_string())]),
            Bound::Inclusive(vec![PropertyValue::String("album".to_string()), PropertyValue::String("2005".to_string())]),
        );

        assert!(!is_impossible_range(&equal_inclusive_range));
    }
}
