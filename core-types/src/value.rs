use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

use crate::{EntityId, ValueType};

mod json_as_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serde_json::to_vec(value).map_err(serde::ser::Error::custom)?.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
    where D: Deserializer<'de> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        serde_json::from_slice(&bytes).map_err(serde::de::Error::custom)
    }
}

/// A typed scalar or structured value carried by properties and query ASTs.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Value {
    /// A signed 16-bit integer.
    I16(i16),
    /// A signed 32-bit integer.
    I32(i32),
    /// A signed 64-bit integer.
    I64(i64),
    /// A 64-bit IEEE-754 floating-point number.
    F64(f64),
    /// A Boolean value.
    Bool(bool),
    /// A UTF-8 string.
    String(String),
    /// A durable entity identity.
    EntityId(EntityId),
    /// Opaque backend-defined object bytes.
    Object(Vec<u8>),
    /// Arbitrary binary bytes.
    Binary(Vec<u8>),
    /// A structured JSON value.
    #[serde(with = "json_as_bytes")]
    Json(serde_json::Value),
}

impl From<&Value> for Value {
    fn from(value: &Value) -> Self { value.clone() }
}

macro_rules! from_value_variant {
    ($type:ty, $variant:ident) => {
        impl From<$type> for Value {
            fn from(value: $type) -> Self { Self::$variant(value) }
        }
    };
}

from_value_variant!(String, String);
from_value_variant!(i16, I16);
from_value_variant!(i32, I32);
from_value_variant!(i64, I64);
from_value_variant!(f64, F64);
from_value_variant!(bool, Bool);
from_value_variant!(Vec<u8>, Binary);
from_value_variant!(EntityId, EntityId);

impl From<&str> for Value {
    fn from(value: &str) -> Self { Self::String(value.to_owned()) }
}

/// A failure to convert a [`Value`] to a requested [`ValueType`].
#[derive(Debug, Clone, PartialEq, Error)]
pub enum CastError {
    /// No cast is defined between the source and target types.
    #[error("cannot cast from {from:?} to {to:?}")]
    IncompatibleTypes {
        /// The value's current type.
        from: ValueType,
        /// The requested type.
        to: ValueType,
    },
    /// A textual value could not be parsed as the requested type.
    #[error("invalid format '{value}' for type {target_type:?}")]
    InvalidFormat {
        /// The invalid textual representation.
        value: String,
        /// The requested type.
        target_type: ValueType,
    },
    /// A numeric value lies outside the requested type's range.
    #[error("numeric overflow: '{value}' cannot fit in {target_type:?}")]
    NumericOverflow {
        /// The value that overflowed.
        value: String,
        /// The requested numeric type.
        target_type: ValueType,
    },
}

/// A failure to deserialize the contents of a [`Value`] through one of its
/// convenience parsing helpers.
#[derive(Debug, Error)]
pub enum ValueParseError {
    /// JSON text or bytes could not be deserialized as the requested type.
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
    /// The value variant cannot carry the requested representation.
    #[error("invalid variant `{given}` for `{ty}`")]
    InvalidVariant {
        /// The value whose variant was unsupported.
        given: Value,
        /// The requested Rust type.
        ty: String,
    },
    /// A string could not be parsed as the requested Rust type.
    #[error("invalid value `{value}` for `{ty}`")]
    InvalidValue {
        /// The invalid string.
        value: String,
        /// The requested Rust type.
        ty: String,
    },
}

impl ValueType {
    /// Return the logical type of `value`.
    pub fn of(value: &Value) -> Self { value.value_type() }
}

impl Value {
    /// Serialize a Rust value into the JSON variant.
    pub fn json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> { Ok(Self::Json(serde_json::to_value(value)?)) }

    /// Parse this value's JSON representation as `T`.
    ///
    /// [`Value::Json`], [`Value::Object`], [`Value::Binary`], and
    /// [`Value::String`] carry JSON-compatible data. Other variants return
    /// [`ValueParseError::InvalidVariant`].
    pub fn parse_as_json<T: serde::de::DeserializeOwned>(&self) -> Result<T, ValueParseError> {
        match self {
            Self::Json(json) => Ok(serde_json::from_value(json.clone())?),
            Self::Object(bytes) | Self::Binary(bytes) => Ok(serde_json::from_slice(bytes)?),
            Self::String(string) => Ok(serde_json::from_str(string)?),
            other => Err(ValueParseError::InvalidVariant { given: other.clone(), ty: std::any::type_name::<T>().to_owned() }),
        }
    }

    /// Parse the contents of a string value as `T`.
    ///
    /// Other value variants return [`ValueParseError::InvalidVariant`].
    pub fn parse_as_string<T: std::str::FromStr>(&self) -> Result<T, ValueParseError> {
        match self {
            Self::String(string) => string
                .parse()
                .map_err(|_| ValueParseError::InvalidValue { value: string.clone(), ty: std::any::type_name::<T>().to_owned() }),
            other => Err(ValueParseError::InvalidVariant { given: other.clone(), ty: std::any::type_name::<T>().to_owned() }),
        }
    }

    /// Return this value's logical type.
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::I16(_) => ValueType::I16,
            Self::I32(_) => ValueType::I32,
            Self::I64(_) => ValueType::I64,
            Self::F64(_) => ValueType::F64,
            Self::Bool(_) => ValueType::Bool,
            Self::String(_) => ValueType::String,
            Self::EntityId(_) => ValueType::EntityId,
            Self::Object(_) => ValueType::Object,
            Self::Binary(_) => ValueType::Binary,
            Self::Json(_) => ValueType::Json,
        }
    }

    /// Convert this value to `target_type`.
    ///
    /// Numeric narrowing checks bounds. String conversions can additionally
    /// fail on invalid syntax; see [`ValueType::castable_to`] for whether a
    /// conversion path exists independent of a particular value.
    pub fn cast_to(&self, target_type: ValueType) -> Result<Self, CastError> {
        let source_type = self.value_type();
        if source_type == target_type {
            return Ok(self.clone());
        }

        match (self, target_type) {
            (Self::String(s), ValueType::EntityId) => {
                EntityId::from_base64(s).map(Self::EntityId).map_err(|_| CastError::InvalidFormat { value: s.clone(), target_type })
            }
            (Self::EntityId(id), ValueType::String) => Ok(Self::String(id.to_base64())),

            (Self::I16(n), ValueType::I32) => Ok(Self::I32(*n as i32)),
            (Self::I16(n), ValueType::I64) => Ok(Self::I64(*n as i64)),
            (Self::I16(n), ValueType::F64) => Ok(Self::F64(*n as f64)),
            (Self::I32(n), ValueType::I16) => i16::try_from(*n).map(Self::I16).map_err(|_| overflow(*n, target_type)),
            (Self::I32(n), ValueType::I64) => Ok(Self::I64(*n as i64)),
            (Self::I32(n), ValueType::F64) => Ok(Self::F64(*n as f64)),
            (Self::I64(n), ValueType::I16) => i16::try_from(*n).map(Self::I16).map_err(|_| overflow(*n, target_type)),
            (Self::I64(n), ValueType::I32) => i32::try_from(*n).map(Self::I32).map_err(|_| overflow(*n, target_type)),
            (Self::I64(n), ValueType::F64) => Ok(Self::F64(*n as f64)),
            (Self::F64(n), ValueType::I16) if n.is_finite() && *n >= i16::MIN as f64 && *n <= i16::MAX as f64 => Ok(Self::I16(*n as i16)),
            (Self::F64(n), ValueType::I32) if n.is_finite() && *n >= i32::MIN as f64 && *n <= i32::MAX as f64 => Ok(Self::I32(*n as i32)),
            (Self::F64(n), ValueType::I64) if n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 => Ok(Self::I64(*n as i64)),
            (Self::F64(n), ValueType::I16 | ValueType::I32 | ValueType::I64) => Err(overflow(*n, target_type)),

            (Self::String(s), ValueType::I16) => parse(s, target_type, Self::I16),
            (Self::String(s), ValueType::I32) => parse(s, target_type, Self::I32),
            (Self::String(s), ValueType::I64) => parse(s, target_type, Self::I64),
            (Self::String(s), ValueType::F64) => parse(s, target_type, Self::F64),
            (Self::String(s), ValueType::Bool) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Ok(Self::Bool(true)),
                "false" | "0" | "no" | "off" => Ok(Self::Bool(false)),
                _ => Err(invalid(s, target_type)),
            },

            (Self::I16(n), ValueType::String) => Ok(Self::String(n.to_string())),
            (Self::I32(n), ValueType::String) => Ok(Self::String(n.to_string())),
            (Self::I64(n), ValueType::String) => Ok(Self::String(n.to_string())),
            (Self::F64(n), ValueType::String) => Ok(Self::String(n.to_string())),
            (Self::Bool(b), ValueType::String) => Ok(Self::String(b.to_string())),

            (Self::Bool(b), ValueType::I16) => Ok(Self::I16(if *b { 1 } else { 0 })),
            (Self::Bool(b), ValueType::I32) => Ok(Self::I32(if *b { 1 } else { 0 })),
            (Self::Bool(b), ValueType::I64) => Ok(Self::I64(if *b { 1 } else { 0 })),
            (Self::Bool(b), ValueType::F64) => Ok(Self::F64(if *b { 1.0 } else { 0.0 })),
            (Self::I16(n), ValueType::Bool) => Ok(Self::Bool(*n != 0)),
            (Self::I32(n), ValueType::Bool) => Ok(Self::Bool(*n != 0)),
            (Self::I64(n), ValueType::Bool) => Ok(Self::Bool(*n != 0)),
            (Self::F64(n), ValueType::Bool) => Ok(Self::Bool(*n != 0.0)),

            (Self::String(s), ValueType::Json) => Ok(Self::Json(serde_json::Value::String(s.clone()))),
            (Self::I16(n), ValueType::Json) => Ok(Self::Json(serde_json::json!(*n as i64))),
            (Self::I32(n), ValueType::Json) => Ok(Self::Json(serde_json::json!(*n as i64))),
            (Self::I64(n), ValueType::Json) => Ok(Self::Json(serde_json::json!(*n))),
            (Self::F64(n), ValueType::Json) => Ok(Self::Json(serde_json::json!(*n))),
            (Self::Bool(b), ValueType::Json) => Ok(Self::Json(serde_json::Value::Bool(*b))),

            (Self::Json(serde_json::Value::String(s)), ValueType::String) => Ok(Self::String(s.clone())),
            (Self::Json(serde_json::Value::Number(n)), ValueType::I64) if n.is_i64() => Ok(Self::I64(n.as_i64().unwrap())),
            (Self::Json(serde_json::Value::Number(n)), ValueType::I32) if n.is_i64() => {
                i32::try_from(n.as_i64().unwrap()).map(Self::I32).map_err(|_| overflow(n, target_type))
            }
            (Self::Json(serde_json::Value::Number(n)), ValueType::I16) if n.is_i64() => {
                i16::try_from(n.as_i64().unwrap()).map(Self::I16).map_err(|_| overflow(n, target_type))
            }
            (Self::Json(serde_json::Value::Number(n)), ValueType::F64) => Ok(Self::F64(n.as_f64().unwrap_or(0.0))),
            (Self::Json(serde_json::Value::Bool(b)), ValueType::Bool) => Ok(Self::Bool(*b)),

            _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
        }
    }

    /// Attempt a cast, returning `None` for any conversion failure.
    pub fn try_cast_to(&self, target_type: ValueType) -> Option<Self> { self.cast_to(target_type).ok() }

    /// Extract a nested value by object-key path.
    ///
    /// JSON values are traversed directly. Binary, object, and string values
    /// are first interpreted as JSON. An empty path returns this value.
    pub fn extract_at_path(&self, path: &[String]) -> Option<Self> {
        if path.is_empty() {
            return Some(self.clone());
        }
        let json = match self {
            Self::Json(json) => json.clone(),
            Self::Binary(bytes) | Self::Object(bytes) => serde_json::from_slice(bytes).ok()?,
            Self::String(string) => serde_json::from_str(string).ok()?,
            _ => return None,
        };
        let mut current = &json;
        for key in path {
            current = current.get(key)?;
        }
        Some(from_json_scalar(current))
    }

    /// Return whether this value compares greater than `other`.
    pub fn gt(&self, other: &Self) -> bool { self.partial_cmp(other) == Some(std::cmp::Ordering::Greater) }
    /// Return whether this value compares greater than or equal to `other`.
    pub fn ge(&self, other: &Self) -> bool {
        matches!(self.partial_cmp(other), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
    }
    /// Return whether this value compares less than `other`.
    pub fn lt(&self, other: &Self) -> bool { self.partial_cmp(other) == Some(std::cmp::Ordering::Less) }
    /// Return whether this value compares less than or equal to `other`.
    pub fn le(&self, other: &Self) -> bool { matches!(self.partial_cmp(other), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)) }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::I16(a), Self::I16(b)) => a.partial_cmp(b),
            (Self::I32(a), Self::I32(b)) => a.partial_cmp(b),
            (Self::I64(a), Self::I64(b)) => a.partial_cmp(b),
            (Self::F64(a), Self::F64(b)) => a.partial_cmp(b),
            (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
            (Self::String(a), Self::String(b)) => a.partial_cmp(b),
            (Self::EntityId(a), Self::EntityId(b)) => a.to_bytes().partial_cmp(&b.to_bytes()),
            (Self::Object(a), Self::Object(b)) | (Self::Binary(a), Self::Binary(b)) => a.partial_cmp(b),
            (Self::Json(a), Self::Json(b)) => a.to_string().partial_cmp(&b.to_string()),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::I16(v) => write!(f, "{v:?}"),
            Self::I32(v) => write!(f, "{v:?}"),
            Self::I64(v) => write!(f, "{v:?}"),
            Self::F64(v) => write!(f, "{v:?}"),
            Self::Bool(v) => write!(f, "{v:?}"),
            Self::String(v) => write!(f, "{v:?}"),
            Self::EntityId(v) => fmt::Display::fmt(v, f),
            Self::Object(v) | Self::Binary(v) => write!(f, "{v:?}"),
            Self::Json(v) => fmt::Display::fmt(v, f),
        }
    }
}

fn parse<T>(source: &str, target_type: ValueType, wrap: impl FnOnce(T) -> Value) -> Result<Value, CastError>
where T: std::str::FromStr {
    source.parse().map(wrap).map_err(|_| invalid(source, target_type))
}

fn invalid(value: impl ToString, target_type: ValueType) -> CastError { CastError::InvalidFormat { value: value.to_string(), target_type } }

fn overflow(value: impl ToString, target_type: ValueType) -> CastError {
    CastError::NumericOverflow { value: value.to_string(), target_type }
}

fn from_json_scalar(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Json(serde_json::Value::Null),
        serde_json::Value::Bool(value) => Value::Bool(*value),
        serde_json::Value::Number(value) => {
            value.as_i64().map(Value::I64).or_else(|| value.as_f64().map(Value::F64)).unwrap_or_else(|| Value::String(value.to_string()))
        }
        serde_json::Value::String(value) => Value::String(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(json.clone()),
    }
}

#[cfg(feature = "wasm")]
mod wasm {
    use super::Value;
    use wasm_bindgen::{JsCast, JsValue};

    impl From<Value> for JsValue {
        fn from(value: Value) -> Self { Self::from(&value) }
    }

    impl From<&Value> for JsValue {
        fn from(value: &Value) -> Self {
            match value {
                Value::String(v) => JsValue::from_str(v),
                Value::I16(v) => JsValue::from_f64(*v as f64),
                Value::I32(v) => JsValue::from_f64(*v as f64),
                Value::I64(v) => JsValue::from_f64(*v as f64),
                Value::F64(v) => JsValue::from_f64(*v),
                Value::Bool(v) => JsValue::from_bool(*v),
                Value::EntityId(v) => JsValue::from_str(&v.to_base64()),
                Value::Object(v) | Value::Binary(v) => js_sys::Uint8Array::from(v.as_slice()).into(),
                Value::Json(v) => serde_wasm_bindgen::to_value(v).unwrap_or(JsValue::NULL),
            }
        }
    }

    impl TryFrom<JsValue> for Value {
        type Error = JsValue;

        fn try_from(value: JsValue) -> Result<Self, Self::Error> {
            if value.is_null() || value.is_undefined() {
                return Err(value);
            }
            if let Some(v) = value.as_string() {
                return Ok(Self::String(v));
            }
            if let Some(v) = value.as_bool() {
                return Ok(Self::Bool(v));
            }
            if let Some(v) = value.as_f64() {
                return Ok(if v.fract() == 0.0 {
                    let integer = v as i64;
                    i32::try_from(integer).map(Self::I32).unwrap_or(Self::I64(integer))
                } else {
                    Self::F64(v)
                });
            }
            if value.is_instance_of::<js_sys::Uint8Array>() {
                let array: js_sys::Uint8Array = value.unchecked_into();
                let mut bytes = vec![0; array.length() as usize];
                array.copy_to(&mut bytes);
                return Ok(Self::Binary(bytes));
            }
            Err(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn castability_and_cast_implementation_stay_aligned() {
        use ValueType::*;
        let representatives = vec![
            Value::I16(1),
            Value::I32(1),
            Value::I64(1),
            Value::F64(1.0),
            Value::Bool(true),
            Value::String("1".into()),
            Value::EntityId(crate::EntityId::new()),
            Value::Object(vec![]),
            Value::Binary(vec![1]),
            Value::Json(serde_json::json!(1)),
            Value::Json(serde_json::json!("s")),
            Value::Json(serde_json::json!(true)),
        ];
        let types = [I16, I32, I64, F64, Bool, String, EntityId, Object, Binary, Json];
        for source in types {
            let values: Vec<_> = representatives.iter().filter(|value| value.value_type() == source).collect();
            for target in types {
                let any = values.iter().any(|value| !matches!(value.cast_to(target), Err(CastError::IncompatibleTypes { .. })));
                assert_eq!(source.castable_to(target), any, "{source:?} -> {target:?}");
            }
        }
    }

    #[test]
    fn public_parse_helpers_accept_their_documented_variants() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Example {
            answer: i32,
        }

        assert_eq!(Value::Json(serde_json::json!({"answer": 42})).parse_as_json::<Example>().unwrap(), Example { answer: 42 });
        assert_eq!(Value::Binary(br#"{"answer":42}"#.to_vec()).parse_as_json::<Example>().unwrap(), Example { answer: 42 });
        assert_eq!(Value::String("42".to_owned()).parse_as_string::<i32>().unwrap(), 42);
    }

    #[test]
    fn public_parse_helpers_reject_wrong_variants_and_values() {
        assert!(matches!(Value::Bool(true).parse_as_json::<bool>(), Err(ValueParseError::InvalidVariant { given: Value::Bool(true), .. })));
        assert!(matches!(Value::String("not a number".to_owned()).parse_as_string::<i32>(), Err(ValueParseError::InvalidValue { .. })));
    }
}
