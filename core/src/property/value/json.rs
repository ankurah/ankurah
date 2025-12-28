//! JSON property type for storing structured data.
//!
//! The `Json` type wraps `serde_json::Value` and stores it as binary data using LWW semantics.
//! This enables querying nested JSON fields using dot-path syntax in AnkQL:
//!
//! ```rust,ignore
//! #[derive(Model)]
//! pub struct Track {
//!     pub name: String,
//!     pub licensing: Json,
//! }
//!
//! // Query nested fields
//! ctx.fetch::<TrackView>("licensing.territory = ?", "US")
//! ctx.fetch::<TrackView>("licensing.rights.holder = ?", "Label")
//! ```

use serde::{Deserialize, Serialize};
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

use crate::property::{traits::PropertyError, Property};
use crate::value::Value;

/// A JSON property type for storing structured/nested data.
///
/// Stores data as serialized JSON bytes using LWW (last-writer-wins) semantics.
/// The inner `serde_json::Value` can represent any JSON structure: objects, arrays,
/// strings, numbers, booleans, or null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Json(pub serde_json::Value);

impl Json {
    /// Create a new Json from a serde_json::Value.
    pub fn new(value: serde_json::Value) -> Self { Json(value) }

    /// Create a Json null value.
    pub fn null() -> Self { Json(serde_json::Value::Null) }

    /// Create a Json object from key-value pairs.
    pub fn object(pairs: impl IntoIterator<Item = (impl Into<String>, serde_json::Value)>) -> Self {
        let map: serde_json::Map<String, serde_json::Value> = pairs.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Json(serde_json::Value::Object(map))
    }

    /// Create a Json array.
    pub fn array(items: impl IntoIterator<Item = serde_json::Value>) -> Self { Json(serde_json::Value::Array(items.into_iter().collect())) }

    /// Get the inner serde_json::Value.
    pub fn inner(&self) -> &serde_json::Value { &self.0 }

    /// Get a mutable reference to the inner value.
    pub fn inner_mut(&mut self) -> &mut serde_json::Value { &mut self.0 }

    /// Consume self and return the inner value.
    pub fn into_inner(self) -> serde_json::Value { self.0 }

    /// Get a nested value by path (e.g., "licensing.territory").
    ///
    /// Returns None if the path doesn't exist or any intermediate value is not an object.
    pub fn get_path(&self, path: &[&str]) -> Option<&serde_json::Value> {
        let mut current = &self.0;
        for step in path {
            current = current.get(*step)?;
        }
        Some(current)
    }

    /// Check if this Json is null.
    pub fn is_null(&self) -> bool { self.0.is_null() }

    /// Check if this Json is an object.
    pub fn is_object(&self) -> bool { self.0.is_object() }

    /// Check if this Json is an array.
    pub fn is_array(&self) -> bool { self.0.is_array() }
}

impl Default for Json {
    fn default() -> Self { Json::null() }
}

impl From<serde_json::Value> for Json {
    fn from(value: serde_json::Value) -> Self { Json(value) }
}

impl From<Json> for serde_json::Value {
    fn from(json: Json) -> Self { json.0 }
}

impl std::ops::Deref for Json {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::DerefMut for Json {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

// WASM bindings for Json type

// TypeScript type definition for Json - represents any valid JSON value
#[cfg(feature = "wasm")]
#[wasm_bindgen::prelude::wasm_bindgen(typescript_custom_section)]
const TS_JSON_TYPE: &'static str = r#"
/** A JSON value - can be any valid JSON: object, array, string, number, boolean, or null */
export type Json = any;
"#;

#[cfg(feature = "wasm")]
impl From<Json> for JsValue {
    fn from(json: Json) -> Self {
        // Convert serde_json::Value to JsValue using serde-wasm-bindgen
        // Use serialize_maps_as_objects to get POJOs instead of Map instances
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        json.0.serialize(&serializer).unwrap_or(JsValue::NULL)
    }
}

#[cfg(feature = "wasm")]
impl wasm_bindgen::describe::WasmDescribe for Json {
    fn describe() { JsValue::describe() }
}

#[cfg(feature = "wasm")]
impl wasm_bindgen::convert::IntoWasmAbi for Json {
    type Abi = <JsValue as wasm_bindgen::convert::IntoWasmAbi>::Abi;

    fn into_abi(self) -> Self::Abi { JsValue::from(self).into_abi() }
}

#[cfg(feature = "wasm")]
impl wasm_bindgen::convert::FromWasmAbi for Json {
    type Abi = <JsValue as wasm_bindgen::convert::FromWasmAbi>::Abi;

    unsafe fn from_abi(js: Self::Abi) -> Self {
        let js_value = JsValue::from_abi(js);
        // Convert JsValue to serde_json::Value using serde-wasm-bindgen
        let value: serde_json::Value = serde_wasm_bindgen::from_value(js_value).unwrap_or(serde_json::Value::Null);
        Json(value)
    }
}

impl Property for Json {
    fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::Json(self.0.clone()))) }

    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match value {
            Some(Value::Json(json)) => Ok(Json(json)),
            Some(Value::Binary(bytes)) => {
                // Accept Binary for backwards compatibility
                let json_value: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(|e| PropertyError::DeserializeError(Box::new(e)))?;
                Ok(Json(json_value))
            }
            Some(other) => Err(PropertyError::InvalidVariant { given: other, ty: "Json".to_string() }),
            None => Err(PropertyError::Missing),
        }
    }
}

/// Macro for creating Json objects with a more ergonomic syntax.
///
/// # Example
/// ```rust,ignore
/// use ankurah_core::json;
///
/// let licensing = json!({
///     "territory": "US",
///     "rights": {
///         "holder": "Label",
///         "type": "exclusive"
///     }
/// });
/// ```
#[macro_export]
macro_rules! json {
    ($($json:tt)+) => {
        $crate::property::value::json::Json::new(serde_json::json!($($json)+))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_roundtrip() {
        let original = Json::object([
            ("name".to_string(), serde_json::json!("test")),
            ("count".to_string(), serde_json::json!(42)),
            (
                "nested".to_string(),
                serde_json::json!({
                    "inner": "value"
                }),
            ),
        ]);

        // Convert to Value and back
        let value = original.into_value().unwrap().unwrap();
        let recovered = Json::from_value(Some(value)).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_json_get_path() {
        let json = Json::new(serde_json::json!({
            "licensing": {
                "territory": "US",
                "rights": {
                    "holder": "Label"
                }
            }
        }));

        assert_eq!(json.get_path(&["licensing", "territory"]), Some(&serde_json::json!("US")));
        assert_eq!(json.get_path(&["licensing", "rights", "holder"]), Some(&serde_json::json!("Label")));
        assert_eq!(json.get_path(&["licensing", "nonexistent"]), None);
        assert_eq!(json.get_path(&["nonexistent"]), None);
    }

    #[test]
    fn test_json_null() {
        let json = Json::null();
        assert!(json.is_null());

        let value = json.into_value().unwrap().unwrap();
        let recovered = Json::from_value(Some(value)).unwrap();
        assert!(recovered.is_null());
    }

    #[test]
    fn test_json_missing() {
        let result = Json::from_value(None);
        assert!(matches!(result, Err(PropertyError::Missing)));
    }

    #[test]
    fn test_json_invalid_variant() {
        let result = Json::from_value(Some(Value::String("not json bytes".to_string())));
        assert!(matches!(result, Err(PropertyError::InvalidVariant { .. })));
    }

    #[test]
    fn test_json_deref() {
        let json = Json::new(serde_json::json!({"key": "value"}));

        // Can use serde_json::Value methods directly via Deref
        assert!(json.is_object());
        assert_eq!(json.get("key"), Some(&serde_json::json!("value")));
    }
}
