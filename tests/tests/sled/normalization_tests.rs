use ankurah::core::indexing::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah::{Value, ValueType};
use ankurah_storage_common::{Endpoint, KeyBoundComponent, KeyBounds};
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
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::String,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "age".to_string(),
                sub_path: None,
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
            sub_path: None,
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
            sub_path: None,
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

/// PR #212 regression test: DESC inequality with ASC equality prefix
///
/// This is THE bug scenario where the planner was checking if the FIRST column
/// was DESC instead of checking if the INEQUALITY column was DESC.
///
/// Index: [room ASC, deleted ASC, timestamp DESC]
/// Query: room = X AND deleted = false AND timestamp <= Y
///
/// Old (buggy) code: checked keyparts.first().is_desc() → room is ASC → FALSE
/// Fixed code: checks keyparts.get(eq_prefix_len).is_desc() → timestamp is DESC → TRUE
#[test]
fn desc_inequality_with_asc_equality_prefix() -> Result<(), IndexError> {
    // Simulate: room = "room123" AND deleted = false AND timestamp <= 1700000000000
    // This creates bounds with two equality components + one upper bound inequality
    let bounds = KeyBounds::new(vec![
        // room = "room123" (equality)
        KeyBoundComponent {
            column: "room".to_string(),
            low: Endpoint::incl(Value::String("room123".to_string())),
            high: Endpoint::incl(Value::String("room123".to_string())),
        },
        // deleted = false (equality)
        KeyBoundComponent {
            column: "deleted".to_string(),
            low: Endpoint::incl(Value::Bool(false)),
            high: Endpoint::incl(Value::Bool(false)),
        },
        // timestamp <= 1700000000000 (inequality on DESC column)
        KeyBoundComponent {
            column: "timestamp".to_string(),
            low: Endpoint::UnboundedLow(ValueType::I64),
            high: Endpoint::incl(Value::I64(1700000000000)),
        },
    ]);

    // Index: [room ASC, deleted ASC, timestamp DESC]
    let key_spec = KeySpec {
        keyparts: vec![
            IndexKeyPart {
                column: "room".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::String,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "deleted".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::Bool,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "timestamp".to_string(),
                sub_path: None,
                direction: IndexDirection::Desc,
                value_type: ValueType::I64,
                nulls: None,
                collation: None,
            },
        ],
    };

    let result = key_bounds_to_sled_range(&bounds, &key_spec)?;

    // The fix should recognize that timestamp (at index 2 = eq_prefix_len) is DESC
    // and invoke handle_desc_inequality, which should:
    // 1. Set start = enc(room, deleted, timestamp) - the upper bound becomes start
    // 2. Set end = succ(enc(room, deleted)) - scan to end of equality prefix
    // 3. Have a non-empty eq_prefix_guard

    // For DESC with upper bound only (timestamp <= Y):
    // - start should be enc(room123, false, 1700000000000)
    // - end should be succ(enc(room123, false))
    assert!(result.start.len() > 10, "Start should be encoded key with all three components, got {} bytes", result.start.len());
    assert!(result.end.is_some(), "Should have bounded end");
    assert!(!result.eq_prefix_guard.is_empty(), "Should have equality prefix guard");

    // The start should NOT be 0x00 - that would indicate the fix didn't apply
    assert_ne!(
        result.start,
        vec![0x00],
        "PR #212 BUG: Start is 0x00, meaning DESC handling was not applied. \
         The code is checking the first column (room ASC) instead of the \
         inequality column (timestamp DESC)"
    );

    Ok(())
}

/// Test DESC inequality with single equality prefix (eq_prefix_len = 1)
/// Index: [category ASC, score DESC]
/// Query: category = 'A' AND score <= 50
#[test]
fn desc_inequality_with_single_asc_prefix() -> Result<(), IndexError> {
    let bounds = KeyBounds::new(vec![
        // category = "A" (equality)
        KeyBoundComponent {
            column: "category".to_string(),
            low: Endpoint::incl(Value::String("A".to_string())),
            high: Endpoint::incl(Value::String("A".to_string())),
        },
        // score <= 50 (inequality on DESC column)
        KeyBoundComponent {
            column: "score".to_string(),
            low: Endpoint::UnboundedLow(ValueType::I64),
            high: Endpoint::incl(Value::I64(50)),
        },
    ]);

    let key_spec = KeySpec {
        keyparts: vec![
            IndexKeyPart {
                column: "category".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::String,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "score".to_string(),
                sub_path: None,
                direction: IndexDirection::Desc,
                value_type: ValueType::I64,
                nulls: None,
                collation: None,
            },
        ],
    };

    let result = key_bounds_to_sled_range(&bounds, &key_spec)?;

    // Should apply DESC handling for the score column
    assert!(result.start.len() > 5, "Start should include encoded category + score");
    assert!(result.end.is_some(), "Should have bounded end");
    assert!(!result.eq_prefix_guard.is_empty(), "Should have equality prefix guard");
    assert_ne!(result.start, vec![0x00], "Should not start at 0x00 for upper-bound-only DESC");

    Ok(())
}

/// Test DESC inequality with >= operator (lower bound only)
/// Index: [room ASC, deleted ASC, timestamp DESC]
/// Query: room = X AND deleted = false AND timestamp >= Y
#[test]
fn desc_lower_bound_inequality_with_asc_prefix() -> Result<(), IndexError> {
    let bounds = KeyBounds::new(vec![
        KeyBoundComponent {
            column: "room".to_string(),
            low: Endpoint::incl(Value::String("room123".to_string())),
            high: Endpoint::incl(Value::String("room123".to_string())),
        },
        KeyBoundComponent {
            column: "deleted".to_string(),
            low: Endpoint::incl(Value::Bool(false)),
            high: Endpoint::incl(Value::Bool(false)),
        },
        // timestamp >= 1700000000000 (lower bound on DESC column)
        KeyBoundComponent {
            column: "timestamp".to_string(),
            low: Endpoint::incl(Value::I64(1700000000000)),
            high: Endpoint::UnboundedHigh(ValueType::I64),
        },
    ]);

    let key_spec = KeySpec {
        keyparts: vec![
            IndexKeyPart {
                column: "room".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::String,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "deleted".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::Bool,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "timestamp".to_string(),
                sub_path: None,
                direction: IndexDirection::Desc,
                value_type: ValueType::I64,
                nulls: None,
                collation: None,
            },
        ],
    };

    let result = key_bounds_to_sled_range(&bounds, &key_spec)?;

    // For DESC with lower bound only (timestamp >= Y) WITH equality prefix:
    // The bounds include the equality prefix in both lower_tuple and upper_tuple.
    // - lower_tuple = [room, deleted, Y] (equality + lower bound)
    // - upper_tuple = [room, deleted] (equality only)
    // So both are non-empty, triggering the (true, true) branch which:
    // - start = enc(upper_tuple) = enc(room, deleted) - just the prefix
    // - end = succ(enc(lower_tuple)) = succ(enc(room, deleted, Y))
    //
    // This correctly scans from the start of the equality prefix up to (not including)
    // the successor of the inequality value, capturing all timestamps >= Y.
    assert!(result.start.len() > 5, "Start should include encoded equality prefix");
    assert!(result.end.is_some(), "Should have bounded end");
    assert!(!result.eq_prefix_guard.is_empty(), "Should have equality prefix guard");

    Ok(())
}

/// Test DESC range query (both lower and upper bounds)
/// Index: [room ASC, deleted ASC, timestamp DESC]
/// Query: room = X AND deleted = false AND timestamp >= A AND timestamp <= B
#[test]
fn desc_range_inequality_with_asc_prefix() -> Result<(), IndexError> {
    let bounds = KeyBounds::new(vec![
        KeyBoundComponent {
            column: "room".to_string(),
            low: Endpoint::incl(Value::String("room123".to_string())),
            high: Endpoint::incl(Value::String("room123".to_string())),
        },
        KeyBoundComponent {
            column: "deleted".to_string(),
            low: Endpoint::incl(Value::Bool(false)),
            high: Endpoint::incl(Value::Bool(false)),
        },
        // timestamp >= 1700000000000 AND timestamp <= 1700000005000 (range on DESC column)
        KeyBoundComponent {
            column: "timestamp".to_string(),
            low: Endpoint::incl(Value::I64(1700000000000)),
            high: Endpoint::incl(Value::I64(1700000005000)),
        },
    ]);

    let key_spec = KeySpec {
        keyparts: vec![
            IndexKeyPart {
                column: "room".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::String,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "deleted".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,
                value_type: ValueType::Bool,
                nulls: None,
                collation: None,
            },
            IndexKeyPart {
                column: "timestamp".to_string(),
                sub_path: None,
                direction: IndexDirection::Desc,
                value_type: ValueType::I64,
                nulls: None,
                collation: None,
            },
        ],
    };

    let result = key_bounds_to_sled_range(&bounds, &key_spec)?;

    // For DESC with both bounds (A <= timestamp <= B):
    // - start should be enc(room, deleted, B) - upper logical becomes lower physical
    // - end should be succ(enc(room, deleted, A)) - lower logical becomes upper physical
    assert!(result.start.len() > 10, "Start should include all three components");
    assert!(result.end.is_some(), "Should have bounded end");
    assert!(!result.eq_prefix_guard.is_empty(), "Should have equality prefix guard");

    Ok(())
}
