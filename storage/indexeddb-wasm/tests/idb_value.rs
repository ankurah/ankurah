//! Unit tests for IdbValue conversions
//!
//! These tests verify the hybrid i64 encoding strategy where:
//! - Positive values within safe range (0 to MAX_SAFE_INTEGER) → f64 numbers
//! - Positive values beyond safe range (> MAX_SAFE_INTEGER) → zero-padded strings
//! - Negative values → f64 numbers (with precision loss warning if beyond MIN_SAFE_INTEGER)
mod common;
use ankurah_core::value::{Value, ValueType};
use ankurah_storage_indexeddb_wasm::idb_value::{IdbValue, MAX_SAFE_INTEGER, MIN_SAFE_INTEGER};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;
#[wasm_bindgen_test]
fn test_i64_positive_safe_range_as_number() {
    // Positive values within safe range should be stored as f64
    let small = Value::I64(100);
    let js_val: JsValue = IdbValue::from(small).into();
    assert!(js_val.as_f64().is_some());
    assert_eq!(js_val.as_f64().unwrap(), 100.0);

    // At the boundary
    let at_max = Value::I64(MAX_SAFE_INTEGER);
    let js_val: JsValue = IdbValue::from(at_max).into();
    assert!(js_val.as_f64().is_some());
    assert_eq!(js_val.as_f64().unwrap(), MAX_SAFE_INTEGER as f64);
}

#[wasm_bindgen_test]
fn test_i64_positive_beyond_safe_as_string() {
    // Positive values beyond safe range should be stored as zero-padded strings
    let beyond = Value::I64(MAX_SAFE_INTEGER + 1);
    let js_val: JsValue = IdbValue::from(beyond).into();
    assert!(js_val.as_string().is_some());
    assert_eq!(js_val.as_string().unwrap(), "00009007199254740992");

    // Large value
    let large = Value::I64(9_223_372_036_854_775_807); // i64::MAX
    let js_val: JsValue = IdbValue::from(large).into();
    assert!(js_val.as_string().is_some());
    assert_eq!(js_val.as_string().unwrap(), "09223372036854775807");
}

#[wasm_bindgen_test]
fn test_i64_negative_always_number() {
    // Negative values are always stored as f64
    let small_neg = Value::I64(-100);
    let js_val: JsValue = IdbValue::from(small_neg).into();
    assert!(js_val.as_f64().is_some());
    assert_eq!(js_val.as_f64().unwrap(), -100.0);

    // At safe boundary
    let at_min = Value::I64(MIN_SAFE_INTEGER);
    let js_val: JsValue = IdbValue::from(at_min).into();
    assert!(js_val.as_f64().is_some());
    assert_eq!(js_val.as_f64().unwrap(), MIN_SAFE_INTEGER as f64);

    // Beyond safe range (accepts truncation)
    let beyond_neg = Value::I64(MIN_SAFE_INTEGER - 1);
    let js_val: JsValue = IdbValue::from(beyond_neg).into();
    assert!(js_val.as_f64().is_some());
}

#[wasm_bindgen_test]
fn test_i64_string_roundtrip() {
    // Large positive i64 should roundtrip through string encoding + casting
    let original = Value::I64(9_223_372_036_854_775_000);
    let js_val: JsValue = IdbValue::from(original.clone()).into();

    // Should be stored as string
    assert!(js_val.as_string().is_some());

    // Comes back as String, use casting to recover i64
    let idb_val = IdbValue::try_from(js_val).unwrap();
    let recovered = idb_val.into_value();

    // Should be a String initially
    assert!(matches!(recovered, Value::String(_)));

    // Cast back to I64
    let cast_back = recovered.cast_to(ValueType::I64).unwrap();
    assert_eq!(cast_back, original);
}

#[wasm_bindgen_test]
fn test_i64_ordering_across_threshold() {
    // Values should maintain ordering across the number/string threshold
    let before = Value::I64(MAX_SAFE_INTEGER);
    let after = Value::I64(MAX_SAFE_INTEGER + 1);

    let js_before: JsValue = IdbValue::from(before).into();
    let js_after: JsValue = IdbValue::from(after).into();

    // Before is a number
    assert!(js_before.as_f64().is_some());
    // After is a string
    assert!(js_after.as_string().is_some());

    // IndexedDB guarantees: all numbers < all strings
    // So this maintains correct ordering: before < after
}
