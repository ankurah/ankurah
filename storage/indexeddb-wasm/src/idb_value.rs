//! IndexedDB-compatible value encoding
//!
//! IndexedDB has specific constraints on what can be used as keys:
//! - Valid key types: number, string, Date, ArrayBuffer, Array
//! - Boolean is NOT a valid key type and must be encoded as 0/1
//! - Binary data is encoded as Uint8Array for lexicographic byte ordering
//!
//! ## Integer Range Limitation
//!
//! JavaScript numbers are IEEE 754 double-precision floats (f64).
//! Safe integer range: ±2^53 - 1 = ±9,007,199,254,740,991
//!
//! Positive i64 values beyond this range use zero-padded strings to retain precision.
//! Negative i64 values remain numbers and may lose precision below the safe range.

use ankurah_core::value::{Value, ValueType};
use wasm_bindgen::{JsCast, JsValue};

/// Convert boolean values to 0/1 numbers recursively in a JSON structure.
/// IndexedDB doesn't support boolean keys, so we must encode bools as numbers
/// for subpath indexing to work (e.g., `data.enabled = true` → `data.enabled = 1`).
fn convert_json_bools_to_numbers(json: &serde_json::Value) -> serde_json::Value {
    match json {
        serde_json::Value::Bool(b) => serde_json::Value::Number(if *b { 1.into() } else { 0.into() }),
        serde_json::Value::Array(arr) => serde_json::Value::Array(arr.iter().map(convert_json_bools_to_numbers).collect()),
        serde_json::Value::Object(obj) => {
            serde_json::Value::Object(obj.iter().map(|(k, v)| (k.clone(), convert_json_bools_to_numbers(v))).collect())
        }
        other => other.clone(),
    }
}

/// Maximum safe integer in JavaScript (2^53 - 1)
#[allow(unused)]
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Minimum safe integer in JavaScript (-(2^53 - 1))
#[allow(unused)]
pub const MIN_SAFE_INTEGER: i64 = -9_007_199_254_740_991;

/// IndexedDB-compatible value wrapper
///
/// Encodes index keys and decodes projections with their durable property type.
///
/// Key encoding rules:
/// - Bool → number (0/1) because booleans are not valid IndexedDB keys
/// - I64 → number, or zero-padded text for positive values above 2^53 - 1
/// - Binary/Object → Uint8Array for lexicographic byte ordering
/// - All other types → standard JsValue encoding
pub struct IdbValue(Value);

impl IdbValue {
    /// Extract the inner Value.
    pub fn into_value(self) -> Value { self.0 }

    /// Decode a projection using its durable property type, never its apparent JS type.
    pub fn from_js(value: JsValue, value_type: ValueType) -> Result<Self, JsValue> {
        fn as_integer(value: &JsValue) -> Option<f64> { value.as_f64().filter(|n| n.fract() == 0.0) }

        let decoded = match value_type {
            ValueType::I16 => as_integer(&value).filter(|n| *n >= i16::MIN as f64 && *n <= i16::MAX as f64).map(|n| Value::I16(n as i16)),
            ValueType::I32 => as_integer(&value).filter(|n| *n >= i32::MIN as f64 && *n <= i32::MAX as f64).map(|n| Value::I32(n as i32)),
            ValueType::I64 => match value.as_string() {
                Some(text) => text.parse::<i64>().ok().map(Value::I64),
                None => as_integer(&value).filter(|n| *n >= i64::MIN as f64 && *n < -(i64::MIN as f64)).map(|n| Value::I64(n as i64)),
            },
            ValueType::F64 => value.as_f64().map(Value::F64),
            ValueType::Bool => value.as_f64().filter(|n| *n == 0.0 || *n == 1.0).map(|n| Value::Bool(n == 1.0)),
            ValueType::String => value.as_string().map(Value::String),
            ValueType::EntityId => value.as_string().and_then(|text| text.parse().ok()).map(Value::EntityId),
            ValueType::Binary | ValueType::Object => {
                if value.is_instance_of::<js_sys::Uint8Array>() || value.is_instance_of::<js_sys::ArrayBuffer>() {
                    let bytes = js_sys::Uint8Array::new(&value).to_vec();
                    Some(if value_type == ValueType::Binary { Value::Binary(bytes) } else { Value::Object(bytes) })
                } else {
                    None
                }
            }
            ValueType::Json => serde_wasm_bindgen::from_value(value.clone()).ok().map(Value::Json),
        };
        decoded.map(Self).ok_or(value)
    }
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
            // Json is stored as a parsed JS object to enable IndexedDB's native nested property indexing.
            // IMPORTANT: We must use serialize_maps_as_objects(true) to create plain JS objects,
            // not ES2015 Maps. IndexedDB keyPath traversal only works with plain objects.
            // NOTE: Booleans must be converted to 0/1 because IDB doesn't support boolean keys.
            Value::Json(json) => {
                use serde::Serialize;
                let converted = convert_json_bools_to_numbers(&json);
                let serializer = serde_wasm_bindgen::Serializer::json_compatible();
                converted.serialize(&serializer).unwrap_or(JsValue::NULL)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn projections_decode_to_their_recorded_type() {
        for value in [
            Value::I16(-123),
            Value::I32(123),
            Value::I64(-123),
            Value::I64(MAX_SAFE_INTEGER),
            Value::I64(MAX_SAFE_INTEGER + 1),
            Value::I64(i64::MAX),
            Value::F64(9_007_199_254_740_992.0),
            Value::Bool(true),
            Value::Bool(false),
            Value::String("00009007199254740992".into()),
            Value::EntityId(ankurah_proto::EntityId::from_bytes([1; 32])),
            Value::Binary(vec![0, 1, 255]),
            Value::Object(vec![0, 1, 255]),
            Value::Json(serde_json::json!({"name": "123", "count": 7})),
            Value::Json(serde_json::json!("123")),
        ] {
            let encoded: JsValue = IdbValue::from(&value).into();
            assert_eq!(IdbValue::from_js(encoded, value.value_type()).unwrap().into_value(), value);
        }
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn invalid_projections_are_not_coerced() {
        assert!(IdbValue::from_js(JsValue::from_f64(1.5), ValueType::I32).is_err());
        assert!(IdbValue::from_js(JsValue::from_f64(32768.0), ValueType::I16).is_err());
        assert!(IdbValue::from_js(JsValue::from_f64(2.0), ValueType::Bool).is_err());
        assert!(IdbValue::from_js(JsValue::from_str("12"), ValueType::F64).is_err());
        assert!(IdbValue::from_js(JsValue::from_f64(12.0), ValueType::String).is_err());
        assert!(IdbValue::from_js(JsValue::from_str("9223372036854775808"), ValueType::I64).is_err());
    }

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
