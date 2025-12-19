mod cast;
pub mod cast_predicate;
mod collatable;
#[cfg(feature = "wasm")]
mod wasm;

pub use cast::CastError;

use ankurah_proto as proto;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd)]
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
    /// JSON value - stored as jsonb in PostgreSQL for proper query support
    Json(Vec<u8>),
}

impl Value {
    /// Create a Json value from any serializable type.
    pub fn json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> { Ok(Value::Json(serde_json::to_vec(value)?)) }

    /// Parse this value as JSON into the target type.
    /// Works for Json, Object, Binary (as bytes) and String variants.
    /// Returns InvalidVariant error for numeric, bool, and EntityId types.
    pub fn parse_as_json<T: serde::de::DeserializeOwned>(&self) -> Result<T, crate::property::PropertyError> {
        match self {
            Value::Json(bytes) | Value::Object(bytes) | Value::Binary(bytes) => Ok(serde_json::from_slice(bytes)?),
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
            Value::Json(bytes) | Value::Binary(bytes) => {
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
        serde_json::Value::Null => Value::String("null".to_string()), // TODO: proper null handling
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
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(serde_json::to_vec(json).unwrap_or_default()),
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
            Value::Json(json) => write!(f, "{}", String::from_utf8_lossy(json)),
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
            Value::Json(bytes) => ankql::ast::Literal::String(String::from_utf8_lossy(&bytes).to_string()),
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
            Value::Json(bytes) => ankql::ast::Literal::String(String::from_utf8_lossy(bytes).to_string()),
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
        let value = Value::Json(serde_json::to_vec(&json).unwrap());

        let result = value.extract_at_path(&["session_id".to_string()]);
        assert_eq!(result, Some(Value::String("sess123".to_string())));
    }

    #[test]
    fn test_extract_at_path_json_number() {
        let json = serde_json::json!({ "count": 42 });
        let value = Value::Json(serde_json::to_vec(&json).unwrap());

        let result = value.extract_at_path(&["count".to_string()]);
        assert_eq!(result, Some(Value::I64(42)));
    }

    #[test]
    fn test_extract_at_path_json_nested() {
        let json = serde_json::json!({ "context": { "user": { "name": "Alice" } } });
        let value = Value::Json(serde_json::to_vec(&json).unwrap());

        let result = value.extract_at_path(&["context".to_string(), "user".to_string(), "name".to_string()]);
        assert_eq!(result, Some(Value::String("Alice".to_string())));
    }

    #[test]
    fn test_extract_at_path_missing() {
        let json = serde_json::json!({ "session_id": "sess123" });
        let value = Value::Json(serde_json::to_vec(&json).unwrap());

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
