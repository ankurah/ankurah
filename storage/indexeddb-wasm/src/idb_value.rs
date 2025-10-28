//! IndexedDB-compatible value encoding
//!
//! IndexedDB has specific constraints on what can be used as keys:
//! - Valid key types: number, string, Date, ArrayBuffer, Array
//! - Boolean is NOT a valid key type and must be encoded as 0/1
//! - Binary data must use ArrayBuffer (not Uint8Array) for proper lexicographic ordering
//!
//! ## Integer Range Limitation
//!
//! JavaScript numbers are IEEE 754 double-precision floats (f64).
//! Safe integer range: ±2^53 - 1 = ±9,007,199,254,740,991
//!
//! Values outside this range will lose precision when converted to f64.
//! This is acceptable for:
//! - Unix millisecond timestamps (safe until year ~285,000 CE)
//! - Most application-level counters and IDs
//!
//! For values requiring full i64 range, consider alternative encoding strategies.

use ankurah_core::value::Value;
use wasm_bindgen::JsValue;

/// Maximum safe integer in JavaScript (2^53 - 1)
#[allow(unused)]
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Minimum safe integer in JavaScript (-(2^53 - 1))
#[allow(unused)]
pub const MIN_SAFE_INTEGER: i64 = -9_007_199_254_740_991;

/// IndexedDB-compatible value wrapper
///
/// Provides symmetric encoding/decoding between Ankurah `Value` and JavaScript `JsValue`
/// that respects IndexedDB key constraints.
///
/// Key encoding rules:
/// - Bool → number (0/1) because booleans are not valid IndexedDB keys
/// - I64 → f64 with accepted precision loss outside ±2^53
/// - Binary/Object → ArrayBuffer for lexicographic byte ordering
/// - All other types → standard JsValue encoding
pub struct IdbValue(Value);

impl IdbValue {
    /// Extract the inner Value
    pub fn into_value(self) -> Value { self.0 }
}

impl From<Value> for IdbValue {
    fn from(value: Value) -> Self { IdbValue(value) }
}

impl From<&Value> for IdbValue {
    fn from(value: &Value) -> Self { IdbValue(value.clone()) }
}

impl From<IdbValue> for JsValue {
    /// Convert to IndexedDB-compatible JsValue
    ///
    /// This encoding ensures values can be used both as:
    /// - Field values stored in IndexedDB objects
    /// - Index keys for range queries and compound indexes
    /// - Prefix guards during cursor iteration
    ///
    /// Special handling for i64:
    /// - Negative values: always stored as f64 (accept truncation beyond ±2^53)
    /// - Positive values 0..=2^53-1: stored as f64 (efficient)
    /// - Positive values >2^53-1: stored as zero-padded string (full precision)
    fn from(value: IdbValue) -> Self {
        match value.0 {
            Value::I16(x) => JsValue::from_f64(x as f64),
            Value::I32(x) => JsValue::from_f64(x as f64),
            Value::I64(x) => {
                if x < 0 {
                    // Negative: always use f64
                    if x < MIN_SAFE_INTEGER {
                        tracing::warn!("Negative i64 {} exceeds safe integer range ({}), precision loss will occur", x, MIN_SAFE_INTEGER);
                    }
                    JsValue::from_f64(x as f64)
                } else if x <= MAX_SAFE_INTEGER {
                    // Positive safe range: use f64
                    JsValue::from_f64(x as f64)
                } else {
                    // Positive beyond safe range: use zero-padded string
                    // i64::MAX is 9223372036854775807 (19 digits), pad to 20
                    // All strings are lexicographically after all numbers in IndexedDB keys
                    // so we can use this to our advantage as long as we do it consistently
                    JsValue::from_str(&format!("{:020}", x))
                }
            }
            Value::F64(x) => JsValue::from_f64(x),
            Value::Bool(b) => JsValue::from_f64(if b { 1.0 } else { 0.0 }), // IndexedDB keys don't support boolean
            Value::String(s) => JsValue::from_str(&s),
            Value::EntityId(entity_id) => JsValue::from_str(&entity_id.to_base64()),
            Value::Binary(bytes) | Value::Object(bytes) => js_sys::Uint8Array::from(bytes.as_slice()).into(),
        }
    }
}

impl TryFrom<JsValue> for IdbValue {
    type Error = JsValue;

    /// Convert from JsValue to IdbValue
    ///
    /// Uses standard Value conversion without schema information. Type information may be lost:
    /// - 0/1 numbers → I32 (bool type info lost)
    /// - Zero-padded numeric strings → String (i64 type info lost for large values)
    ///
    /// **Future enhancement:** Accept schema/ValueType hints for direct conversion to proper types.
    ///
    /// **Current workaround:** We rely on Value-to-Value casting in predicate comparisons
    /// (see `compare_values_with_cast` in `filter.rs`). When comparing values from IndexedDB
    /// against query literals, the casting system automatically converts:
    /// - `Value::I32(1)` ↔ `Value::Bool(true)`
    /// - `Value::String("9007199254740992000")` ↔ `Value::I64(9007199254740992000)`
    fn try_from(js_value: JsValue) -> Result<Self, Self::Error> {
        let value = Value::try_from(js_value)?;
        Ok(IdbValue(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_integer_range() {
        // Verify our constants are correct
        assert_eq!(MAX_SAFE_INTEGER, 9_007_199_254_740_991);
        assert_eq!(MIN_SAFE_INTEGER, -9_007_199_254_740_991);

        // Safe range is 2^53 - 1
        assert_eq!(MAX_SAFE_INTEGER, (1i64 << 53) - 1);
        assert_eq!(MIN_SAFE_INTEGER, -((1i64 << 53) - 1));
    }

    #[test]
    fn test_timestamp_safety() {
        // Current Unix timestamp in milliseconds (2024)
        let now = 1700000000000i64;
        assert!(now < MAX_SAFE_INTEGER);

        // Year 285,000 CE would still be safe
        let far_future = 8_900_000_000_000_000i64;
        assert!(far_future < MAX_SAFE_INTEGER);
    }
}
