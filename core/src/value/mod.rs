mod cast;
pub mod cast_predicate;
mod collatable;
#[cfg(feature = "wasm")]
mod wasm;

pub use cast::CastError;

use ankurah_proto as proto;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Custom serialization for serde_json::Value that stores as bytes.
/// This is needed because bincode doesn't support deserialize_any.
mod json_as_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let bytes = serde_json::to_vec(value).map_err(serde::ser::Error::custom)?;
        bytes.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
    where D: Deserializer<'de> {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        serde_json::from_slice(&bytes).map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Value {
    // Numbers
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),

    Bool(bool),
    String(String),
    EntityId(proto::EntityId),
    Object(Vec<u8>),
    Binary(Vec<u8>),
    /// JSON value - stored as jsonb in PostgreSQL for proper query support.
    /// Serialized as bytes for bincode compatibility.
    #[serde(with = "json_as_bytes")]
    Json(serde_json::Value),
}

impl Value {
    /// Create a Json value from any serializable type.
    pub fn json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> { Ok(Value::Json(serde_json::to_value(value)?)) }

    /// Parse this value as JSON into the target type.
    /// Works for Json, Object, Binary (as bytes) and String variants.
    /// Returns InvalidVariant error for numeric, bool, and EntityId types.
    pub fn parse_as_json<T: serde::de::DeserializeOwned>(&self) -> Result<T, crate::property::PropertyError> {
        match self {
            Value::Json(json) => Ok(serde_json::from_value(json.clone())?),
            Value::Object(bytes) | Value::Binary(bytes) => Ok(serde_json::from_slice(bytes)?),
            Value::String(s) => Ok(serde_json::from_str(s)?),
            other => {
                Err(crate::property::PropertyError::InvalidVariant { given: other.clone(), ty: std::any::type_name::<T>().to_string() })
            }
        }
    }

    /// Parse this value as a string using FromStr.
    /// Only works for Value::String variant.
    /// Returns InvalidVariant error for other types.
    pub fn parse_as_string<T: std::str::FromStr>(&self) -> Result<T, crate::property::PropertyError> {
        match self {
            Value::String(s) => s
                .parse()
                .map_err(|_| crate::property::PropertyError::InvalidValue { value: s.clone(), ty: std::any::type_name::<T>().to_string() }),
            other => {
                Err(crate::property::PropertyError::InvalidVariant { given: other.clone(), ty: std::any::type_name::<T>().to_string() })
            }
        }
    }

    /// Extract value at a sub-path within structured data.
    /// Returns None if the path doesn't exist (missing - distinct from null).
    /// For empty path, returns self unchanged.
    /// Supports Json, Binary, and String (permissive for backward compat).
    pub fn extract_at_path(&self, path: &[String]) -> Option<Value> {
        if path.is_empty() {
            return Some(self.clone());
        }

        match self {
            Value::Json(json) => {
                let mut current = json;
                for key in path {
                    current = current.get(key)?;
                }
                Some(json_value_to_value(current))
            }
            Value::Binary(bytes) => {
                let json: serde_json::Value = serde_json::from_slice(bytes).ok()?;
                let mut current = &json;
                for key in path {
                    current = current.get(key)?;
                }
                Some(json_value_to_value(current))
            }
            Value::String(s) => {
                let json: serde_json::Value = serde_json::from_str(s).ok()?;
                let mut current = &json;
                for key in path {
                    current = current.get(key)?;
                }
                Some(json_value_to_value(current))
            }
            _ => None,
        }
    }
}

/// Convert serde_json::Value to ankurah Value
fn json_value_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Json(serde_json::Value::Null),
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::I64(i)
            } else if let Some(f) = n.as_f64() {
                Value::F64(f)
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        // Arrays and objects remain as Json
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(json.clone()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    I16,
    I32,
    I64,
    F64,
    Bool,
    String,
    EntityId,
    Object,
    Binary,
    Json,
}

impl ValueType {
    pub fn of(v: &Value) -> Self {
        match v {
            Value::I16(_) => ValueType::I16,
            Value::I32(_) => ValueType::I32,
            Value::I64(_) => ValueType::I64,
            Value::F64(_) => ValueType::F64,
            Value::Bool(_) => ValueType::Bool,
            Value::String(_) => ValueType::String,
            Value::EntityId(_) => ValueType::EntityId,
            Value::Object(_) => ValueType::Object,
            Value::Binary(_) => ValueType::Binary,
            Value::Json(_) => ValueType::Json,
        }
    }
}

// Conversions between core::ValueType and proto::sys::ValueType
impl From<proto::sys::ValueType> for ValueType {
    fn from(v: proto::sys::ValueType) -> Self {
        match v {
            proto::sys::ValueType::I16 => ValueType::I16,
            proto::sys::ValueType::I32 => ValueType::I32,
            proto::sys::ValueType::I64 => ValueType::I64,
            proto::sys::ValueType::F64 => ValueType::F64,
            proto::sys::ValueType::Bool => ValueType::Bool,
            proto::sys::ValueType::String => ValueType::String,
            proto::sys::ValueType::EntityId => ValueType::EntityId,
            proto::sys::ValueType::Object => ValueType::Object,
            proto::sys::ValueType::Binary => ValueType::Binary,
            proto::sys::ValueType::Json => ValueType::Json,
        }
    }
}

impl From<ValueType> for proto::sys::ValueType {
    fn from(v: ValueType) -> Self {
        match v {
            ValueType::I16 => proto::sys::ValueType::I16,
            ValueType::I32 => proto::sys::ValueType::I32,
            ValueType::I64 => proto::sys::ValueType::I64,
            ValueType::F64 => proto::sys::ValueType::F64,
            ValueType::Bool => proto::sys::ValueType::Bool,
            ValueType::String => proto::sys::ValueType::String,
            ValueType::EntityId => proto::sys::ValueType::EntityId,
            ValueType::Object => proto::sys::ValueType::Object,
            ValueType::Binary => proto::sys::ValueType::Binary,
            ValueType::Json => proto::sys::ValueType::Json,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            // Same types - compare directly
            (Value::I16(a), Value::I16(b)) => a.partial_cmp(b),
            (Value::I32(a), Value::I32(b)) => a.partial_cmp(b),
            (Value::I64(a), Value::I64(b)) => a.partial_cmp(b),
            (Value::F64(a), Value::F64(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::EntityId(a), Value::EntityId(b)) => a.to_bytes().partial_cmp(&b.to_bytes()),
            (Value::Object(a), Value::Object(b)) => a.partial_cmp(b),
            (Value::Binary(a), Value::Binary(b)) => a.partial_cmp(b),
            // JSON values: compare by serialized form (not ideal but works for basic cases)
            (Value::Json(a), Value::Json(b)) => a.to_string().partial_cmp(&b.to_string()),
            // Cross-type comparison: different types are not comparable
            _ => None,
        }
    }
}

// Comparison operators for Value (used in filter.rs)
impl Value {
    pub fn gt(&self, other: &Self) -> bool { self.partial_cmp(other) == Some(std::cmp::Ordering::Greater) }

    pub fn ge(&self, other: &Self) -> bool {
        matches!(self.partial_cmp(other), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
    }

    pub fn lt(&self, other: &Self) -> bool { self.partial_cmp(other) == Some(std::cmp::Ordering::Less) }

    pub fn le(&self, other: &Self) -> bool { matches!(self.partial_cmp(other), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)) }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::I16(int) => write!(f, "{:?}", int),
            Value::I32(int) => write!(f, "{:?}", int),
            Value::I64(int) => write!(f, "{:?}", int),
            Value::F64(float) => write!(f, "{:?}", float),
            Value::Bool(bool) => write!(f, "{:?}", bool),
            Value::String(string) => write!(f, "{:?}", string),
            Value::EntityId(entity_id) => write!(f, "{}", entity_id),
            Value::Object(object) => write!(f, "{:?}", object),
            Value::Binary(binary) => write!(f, "{:?}", binary),
            Value::Json(json) => write!(f, "{}", json),
        }
    }
}

impl From<ankql::ast::Literal> for Value {
    fn from(literal: ankql::ast::Literal) -> Self {
        match literal {
            ankql::ast::Literal::I16(i) => Value::I16(i),
            ankql::ast::Literal::I32(i) => Value::I32(i),
            ankql::ast::Literal::I64(i) => Value::I64(i),
            ankql::ast::Literal::F64(f) => Value::F64(f),
            ankql::ast::Literal::Bool(b) => Value::Bool(b),
            ankql::ast::Literal::String(s) => Value::String(s),
            ankql::ast::Literal::EntityId(ulid) => Value::EntityId(proto::EntityId::from_ulid(ulid)),
            ankql::ast::Literal::Object(object) => Value::Object(object),
            ankql::ast::Literal::Binary(binary) => Value::Binary(binary),
            ankql::ast::Literal::Json(json) => Value::Json(json),
        }
    }
}

impl From<&ankql::ast::Literal> for Value {
    fn from(literal: &ankql::ast::Literal) -> Self {
        match literal {
            ankql::ast::Literal::I16(i) => Value::I16(*i),
            ankql::ast::Literal::I32(i) => Value::I32(*i),
            ankql::ast::Literal::I64(i) => Value::I64(*i),
            ankql::ast::Literal::F64(f) => Value::F64(*f),
            ankql::ast::Literal::Bool(b) => Value::Bool(*b),
            ankql::ast::Literal::String(s) => Value::String(s.clone()),
            ankql::ast::Literal::EntityId(ulid) => Value::EntityId(proto::EntityId::from_ulid(*ulid)),
            ankql::ast::Literal::Object(object) => Value::Object(object.clone()),
            ankql::ast::Literal::Binary(binary) => Value::Binary(binary.clone()),
            ankql::ast::Literal::Json(json) => Value::Json(json.clone()),
        }
    }
}

impl From<Value> for ankql::ast::Literal {
    fn from(value: Value) -> Self {
        match value {
            Value::I16(i) => ankql::ast::Literal::I16(i),
            Value::I32(i) => ankql::ast::Literal::I32(i),
            Value::I64(i) => ankql::ast::Literal::I64(i),
            Value::F64(f) => ankql::ast::Literal::F64(f),
            Value::Bool(b) => ankql::ast::Literal::Bool(b),
            Value::String(s) => ankql::ast::Literal::String(s),
            Value::EntityId(entity_id) => ankql::ast::Literal::EntityId(entity_id.to_ulid()),
            Value::Object(bytes) => ankql::ast::Literal::String(String::from_utf8_lossy(&bytes).to_string()),
            Value::Binary(bytes) => ankql::ast::Literal::String(String::from_utf8_lossy(&bytes).to_string()),
            Value::Json(json) => ankql::ast::Literal::Json(json),
        }
    }
}

impl From<&Value> for ankql::ast::Literal {
    fn from(value: &Value) -> Self {
        match value {
            Value::I16(i) => ankql::ast::Literal::I16(*i),
            Value::I32(i) => ankql::ast::Literal::I32(*i),
            Value::I64(i) => ankql::ast::Literal::I64(*i),
            Value::F64(f) => ankql::ast::Literal::F64(*f),
            Value::Bool(b) => ankql::ast::Literal::Bool(*b),
            Value::String(s) => ankql::ast::Literal::String(s.clone()),
            Value::EntityId(entity_id) => ankql::ast::Literal::EntityId(entity_id.to_ulid()),
            Value::Object(bytes) => ankql::ast::Literal::String(String::from_utf8_lossy(bytes).to_string()),
            Value::Binary(bytes) => ankql::ast::Literal::String(String::from_utf8_lossy(bytes).to_string()),
            Value::Json(json) => ankql::ast::Literal::Json(json.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_at_path_empty() {
        let value = Value::String("hello".to_string());
        let result = value.extract_at_path(&[]);
        assert_eq!(result, Some(Value::String("hello".to_string())));
    }

    #[test]
    fn test_extract_at_path_json_string() {
        let json = serde_json::json!({ "session_id": "sess123" });
        let value = Value::Json(json);

        let result = value.extract_at_path(&["session_id".to_string()]);
        assert_eq!(result, Some(Value::String("sess123".to_string())));
    }

    #[test]
    fn test_extract_at_path_json_number() {
        let json = serde_json::json!({ "count": 42 });
        let value = Value::Json(json);

        let result = value.extract_at_path(&["count".to_string()]);
        assert_eq!(result, Some(Value::I64(42)));
    }

    #[test]
    fn test_extract_at_path_json_nested() {
        let json = serde_json::json!({ "context": { "user": { "name": "Alice" } } });
        let value = Value::Json(json);

        let result = value.extract_at_path(&["context".to_string(), "user".to_string(), "name".to_string()]);
        assert_eq!(result, Some(Value::String("Alice".to_string())));
    }

    #[test]
    fn test_extract_at_path_missing() {
        let json = serde_json::json!({ "session_id": "sess123" });
        let value = Value::Json(json);

        let result = value.extract_at_path(&["nonexistent".to_string()]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_at_path_non_json() {
        let value = Value::String("not json".to_string());

        // Non-empty path on non-JSON returns None
        let result = value.extract_at_path(&["field".to_string()]);
        assert_eq!(result, None);
    }
}
