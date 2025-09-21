use ankurah::{Value, ValueType};
use ankurah_storage_common::{Endpoint, IndexDirection, IndexKeyPart, KeyBoundComponent, KeyBounds, KeySpec};
use ankurah_storage_sled::{error::IndexError, planner_integration::key_bounds_to_sled_range};

#[test]
fn equality_bounds_use_prefix_guard_for_multi_key() -> Result<(), IndexError> {
    // name = "Alice" on a (name, age) index should use prefix guard
    let bounds = KeyBounds::new(vec![KeyBoundComponent {
        column: "name".to_string(),
        low: Endpoint::incl(Value::String("Alice".to_string())),
        high: Endpoint::incl(Value::String("Alice".to_string())),
    }]);

    let key_spec = KeySpec {
        keyparts: vec![
            IndexKeyPart {
                column: "name".to_string(),
                direction: IndexDirection::Asc,
                value_type: ValueType::String,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "age".to_string(),
                direction: IndexDirection::Asc,
                value_type: ValueType::I32,
                nulls: None,
                collation: None,
            },
        ],
    };

    let result = key_bounds_to_sled_range(&bounds, &key_spec)?;

    // Should use unbounded upper with prefix guard for multi-key partial equality
    assert!(result.end.is_none(), "Multi-key equality should have unbounded upper");
    assert!(result.upper_open_ended, "Should be open-ended");
    assert!(!result.eq_prefix_guard.is_empty(), "Should have prefix guard");
    Ok(())
}

#[test]
fn equality_bounds_use_tight_range_for_single_key() -> Result<(), IndexError> {
    // name = "Alice" on a single-key (name) index should use tight range
    let bounds = KeyBounds::new(vec![KeyBoundComponent {
        column: "name".to_string(),
        low: Endpoint::incl(Value::String("Alice".to_string())),
        high: Endpoint::incl(Value::String("Alice".to_string())),
    }]);

    let key_spec = KeySpec {
        keyparts: vec![IndexKeyPart {
            column: "name".to_string(),
            direction: IndexDirection::Asc,
            value_type: ValueType::String,
            nulls: None,
            collation: None,
        }],
    };

    let result = key_bounds_to_sled_range(&bounds, &key_spec)?;

    // Should use tight range for single-key equality
    assert!(result.end.is_some(), "Single-key equality should have bounded upper");
    assert!(!result.upper_open_ended, "Should not be open-ended");
    assert!(result.eq_prefix_guard.is_empty(), "Should not need prefix guard");
    Ok(())
}

#[test]
fn inequality_bounds_handle_desc_correctly() -> Result<(), IndexError> {
    // age > 25 on a DESC index should swap the bounds
    let bounds = KeyBounds::new(vec![KeyBoundComponent {
        column: "age".to_string(),
        low: Endpoint::excl(Value::I32(25)),
        high: Endpoint::UnboundedHigh(ValueType::I32),
    }]);

    let key_spec = KeySpec {
        keyparts: vec![IndexKeyPart {
            column: "age".to_string(),
            direction: IndexDirection::Desc,
            value_type: ValueType::I32,
            nulls: None,
            collation: None,
        }],
    };

    let result = key_bounds_to_sled_range(&bounds, &key_spec)?;

    // For DESC, age > 25 should map to a bounded upper range (scan from start to enc(25))
    assert_eq!(result.start, vec![0x00], "DESC inequality should start from beginning");
    assert!(result.end.is_some(), "DESC inequality should have bounded upper");
    Ok(())
}
