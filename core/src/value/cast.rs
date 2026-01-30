use crate::{
    property::PropertyError,
    value::{Value, ValueType},
};
use ankurah_proto::EntityId;

#[derive(Debug, Clone, PartialEq)]
pub enum CastError {
    /// Cannot cast from source type to target type
    IncompatibleTypes { from: ValueType, to: ValueType },
    /// Invalid format for the target type (e.g., invalid EntityId string)
    InvalidFormat { value: String, target_type: ValueType },
    /// Numeric overflow when casting between numeric types
    NumericOverflow { value: String, target_type: ValueType },
}

impl std::fmt::Display for CastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CastError::IncompatibleTypes { from, to } => {
                write!(f, "Cannot cast from {:?} to {:?}", from, to)
            }
            CastError::InvalidFormat { value, target_type } => {
                write!(f, "Invalid format '{}' for type {:?}", value, target_type)
            }
            CastError::NumericOverflow { value, target_type } => {
                write!(f, "Numeric overflow: '{}' cannot fit in {:?}", value, target_type)
            }
        }
    }
}

impl From<CastError> for PropertyError {
    fn from(err: CastError) -> Self { PropertyError::CastError(err) }
}

impl std::error::Error for CastError {}

impl Value {
    /// Cast this value to the specified target type
    pub fn cast_to(&self, target_type: ValueType) -> Result<Value, CastErrorChangeMe> {
        let source_type = ValueType::of(self);

        // If already the target type, return clone
        if source_type == target_type {
            return Ok(self.clone());
        }

        match (self, target_type) {
            // String to EntityId conversion
            (Value::String(s), ValueType::EntityId) => match EntityId::from_base64(s) {
                Ok(entity_id) => Ok(Value::EntityId(entity_id)),
                Err(_) => Err(CastError::InvalidFormat { value: s.clone(), target_type: ValueType::EntityId }),
            },

            // EntityId to String conversion
            (Value::EntityId(entity_id), ValueType::String) => Ok(Value::String(entity_id.to_base64())),

            // Numeric conversions
            (Value::I16(n), ValueType::I32) => Ok(Value::I32(*n as i32)),
            (Value::I16(n), ValueType::I64) => Ok(Value::I64(*n as i64)),
            (Value::I16(n), ValueType::F64) => Ok(Value::F64(*n as f64)),

            (Value::I32(n), ValueType::I16) => {
                if *n >= i16::MIN as i32 && *n <= i16::MAX as i32 {
                    Ok(Value::I16(*n as i16))
                } else {
                    Err(CastError::NumericOverflow { value: n.to_string(), target_type: ValueType::I16 })
                }
            }
            (Value::I32(n), ValueType::I64) => Ok(Value::I64(*n as i64)),
            (Value::I32(n), ValueType::F64) => Ok(Value::F64(*n as f64)),

            (Value::I64(n), ValueType::I16) => {
                if *n >= i16::MIN as i64 && *n <= i16::MAX as i64 {
                    Ok(Value::I16(*n as i16))
                } else {
                    Err(CastError::NumericOverflow { value: n.to_string(), target_type: ValueType::I16 })
                }
            }
            (Value::I64(n), ValueType::I32) => {
                if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    Ok(Value::I32(*n as i32))
                } else {
                    Err(CastError::NumericOverflow { value: n.to_string(), target_type: ValueType::I32 })
                }
            }
            (Value::I64(n), ValueType::F64) => Ok(Value::F64(*n as f64)),

            (Value::F64(n), ValueType::I16) => {
                if n.is_finite() && *n >= i16::MIN as f64 && *n <= i16::MAX as f64 {
                    Ok(Value::I16(*n as i16))
                } else {
                    Err(CastError::NumericOverflow { value: n.to_string(), target_type: ValueType::I16 })
                }
            }
            (Value::F64(n), ValueType::I32) => {
                if n.is_finite() && *n >= i32::MIN as f64 && *n <= i32::MAX as f64 {
                    Ok(Value::I32(*n as i32))
                } else {
                    Err(CastError::NumericOverflow { value: n.to_string(), target_type: ValueType::I32 })
                }
            }
            (Value::F64(n), ValueType::I64) => {
                if n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                    Ok(Value::I64(*n as i64))
                } else {
                    Err(CastError::NumericOverflow { value: n.to_string(), target_type: ValueType::I64 })
                }
            }

            // String to numeric conversions
            (Value::String(s), ValueType::I16) => match s.parse::<i16>() {
                Ok(n) => Ok(Value::I16(n)),
                Err(_) => Err(CastError::InvalidFormat { value: s.clone(), target_type: ValueType::I16 }),
            },
            (Value::String(s), ValueType::I32) => match s.parse::<i32>() {
                Ok(n) => Ok(Value::I32(n)),
                Err(_) => Err(CastError::InvalidFormat { value: s.clone(), target_type: ValueType::I32 }),
            },
            (Value::String(s), ValueType::I64) => match s.parse::<i64>() {
                Ok(n) => Ok(Value::I64(n)),
                Err(_) => Err(CastError::InvalidFormat { value: s.clone(), target_type: ValueType::I64 }),
            },
            (Value::String(s), ValueType::F64) => match s.parse::<f64>() {
                Ok(n) => Ok(Value::F64(n)),
                Err(_) => Err(CastError::InvalidFormat { value: s.clone(), target_type: ValueType::F64 }),
            },
            (Value::String(s), ValueType::Bool) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Ok(Value::Bool(true)),
                "false" | "0" | "no" | "off" => Ok(Value::Bool(false)),
                _ => Err(CastError::InvalidFormat { value: s.clone(), target_type: ValueType::Bool }),
            },

            // Numeric to string conversions
            (Value::I16(n), ValueType::String) => Ok(Value::String(n.to_string())),
            (Value::I32(n), ValueType::String) => Ok(Value::String(n.to_string())),
            (Value::I64(n), ValueType::String) => Ok(Value::String(n.to_string())),
            (Value::F64(n), ValueType::String) => Ok(Value::String(n.to_string())),
            (Value::Bool(b), ValueType::String) => Ok(Value::String(b.to_string())),

            // Bool to numeric conversions (for IndexedDB compatibility where bool stored as 0/1)
            (Value::Bool(b), ValueType::I16) => Ok(Value::I16(if *b { 1 } else { 0 })),
            (Value::Bool(b), ValueType::I32) => Ok(Value::I32(if *b { 1 } else { 0 })),
            (Value::Bool(b), ValueType::I64) => Ok(Value::I64(if *b { 1 } else { 0 })),
            (Value::Bool(b), ValueType::F64) => Ok(Value::F64(if *b { 1.0 } else { 0.0 })),

            // Numeric to bool conversions (0 = false, non-zero = true)
            (Value::I16(n), ValueType::Bool) => Ok(Value::Bool(*n != 0)),
            (Value::I32(n), ValueType::Bool) => Ok(Value::Bool(*n != 0)),
            (Value::I64(n), ValueType::Bool) => Ok(Value::Bool(*n != 0)),
            (Value::F64(f), ValueType::Bool) => Ok(Value::Bool(*f != 0.0)),

            // Cast TO Json: wrap scalar values as JSON, preserving their original type tag
            // This is used for JSON subfield indexing where type must be preserved
            (Value::String(s), ValueType::Json) => Ok(Value::Json(serde_json::Value::String(s.clone()))),
            (Value::I64(n), ValueType::Json) => Ok(Value::Json(serde_json::json!(*n))),
            (Value::I32(n), ValueType::Json) => Ok(Value::Json(serde_json::json!(*n as i64))),
            (Value::I16(n), ValueType::Json) => Ok(Value::Json(serde_json::json!(*n as i64))),
            (Value::F64(n), ValueType::Json) => Ok(Value::Json(serde_json::json!(*n))),
            (Value::Bool(b), ValueType::Json) => Ok(Value::Json(serde_json::Value::Bool(*b))),

            // Cast FROM Json: extract value if types match
            (Value::Json(json), ValueType::String) => match json {
                serde_json::Value::String(s) => Ok(Value::String(s.clone())),
                _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
            },
            (Value::Json(json), ValueType::I64) => match json {
                serde_json::Value::Number(n) if n.is_i64() => Ok(Value::I64(n.as_i64().unwrap())),
                _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
            },
            (Value::Json(json), ValueType::I32) => match json {
                serde_json::Value::Number(n) if n.is_i64() => {
                    let i = n.as_i64().unwrap();
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        Ok(Value::I32(i as i32))
                    } else {
                        Err(CastError::NumericOverflow { value: i.to_string(), target_type: ValueType::I32 })
                    }
                }
                _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
            },
            (Value::Json(json), ValueType::I16) => match json {
                serde_json::Value::Number(n) if n.is_i64() => {
                    let i = n.as_i64().unwrap();
                    if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                        Ok(Value::I16(i as i16))
                    } else {
                        Err(CastError::NumericOverflow { value: i.to_string(), target_type: ValueType::I16 })
                    }
                }
                _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
            },
            (Value::Json(json), ValueType::F64) => match json {
                serde_json::Value::Number(n) => Ok(Value::F64(n.as_f64().unwrap_or(0.0))),
                _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
            },
            (Value::Json(json), ValueType::Bool) => match json {
                serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
                _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
            },

            // All other combinations are incompatible
            _ => Err(CastError::IncompatibleTypes { from: source_type, to: target_type }),
        }
    }

    /// Try to cast this value to the specified target type, returning None if the cast fails
    pub fn try_cast_to(&self, target_type: ValueType) -> Option<Value> { self.cast_to(target_type).ok() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_to_entity_id() {
        let entity_id = EntityId::new();
        let base64_str = entity_id.to_base64();
        let value = Value::String(base64_str.clone());

        let result = value.cast_to(ValueType::EntityId).unwrap();
        match result {
            Value::EntityId(parsed_id) => assert_eq!(parsed_id, entity_id),
            _ => panic!("Expected EntityId variant"),
        }
    }

    #[test]
    fn test_entity_id_to_string() {
        let entity_id = EntityId::new();
        let value = Value::EntityId(entity_id.clone());

        let result = value.cast_to(ValueType::String).unwrap();
        match result {
            Value::String(s) => assert_eq!(s, entity_id.to_base64()),
            _ => panic!("Expected String variant"),
        }
    }

    #[test]
    fn test_invalid_entity_id_string() {
        let value = Value::String("invalid-entity-id".to_string());
        let result = value.cast_to(ValueType::EntityId);

        assert!(matches!(result, Err(CastError::InvalidFormat { .. })));
    }

    #[test]
    fn test_numeric_upcasting() {
        let value = Value::I16(42);

        assert_eq!(value.cast_to(ValueType::I32).unwrap(), Value::I32(42));
        assert_eq!(value.cast_to(ValueType::I64).unwrap(), Value::I64(42));
        assert_eq!(value.cast_to(ValueType::F64).unwrap(), Value::F64(42.0));
    }

    #[test]
    fn test_numeric_downcasting() {
        let value = Value::I32(42);
        assert_eq!(value.cast_to(ValueType::I16).unwrap(), Value::I16(42));

        let large_value = Value::I32(100000);
        assert!(matches!(large_value.cast_to(ValueType::I16), Err(CastError::NumericOverflow { .. })));
    }

    #[test]
    fn test_string_to_numeric() {
        let value = Value::String("42".to_string());

        assert_eq!(value.cast_to(ValueType::I16).unwrap(), Value::I16(42));
        assert_eq!(value.cast_to(ValueType::I32).unwrap(), Value::I32(42));
        assert_eq!(value.cast_to(ValueType::I64).unwrap(), Value::I64(42));
        assert_eq!(value.cast_to(ValueType::F64).unwrap(), Value::F64(42.0));
    }

    #[test]
    fn test_string_to_bool() {
        assert_eq!(Value::String("true".to_string()).cast_to(ValueType::Bool).unwrap(), Value::Bool(true));
        assert_eq!(Value::String("false".to_string()).cast_to(ValueType::Bool).unwrap(), Value::Bool(false));
        assert_eq!(Value::String("1".to_string()).cast_to(ValueType::Bool).unwrap(), Value::Bool(true));
        assert_eq!(Value::String("0".to_string()).cast_to(ValueType::Bool).unwrap(), Value::Bool(false));

        assert!(matches!(Value::String("maybe".to_string()).cast_to(ValueType::Bool), Err(CastError::InvalidFormat { .. })));
    }

    #[test]
    fn test_incompatible_types() {
        // Binary to I32 is truly incompatible
        let value = Value::Binary(vec![1, 2, 3]);
        let result = value.cast_to(ValueType::I32);

        assert!(matches!(result, Err(CastError::IncompatibleTypes { .. })));
    }

    #[test]
    fn test_same_type_cast() {
        let value = Value::I32(42);
        let result = value.cast_to(ValueType::I32).unwrap();

        assert_eq!(result, value);
    }
}
