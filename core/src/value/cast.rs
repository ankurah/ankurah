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
    fn from(err: CastError) -> Self { PropertyError::NonCastable(err) }
}

impl std::error::Error for CastError {}

impl Value {
    /// Cast this value to the specified target type
    pub fn cast_to(&self, target_type: ValueType) -> Result<Value, CastError> {
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

impl ValueType {
    /// The catalog's string form of a value type: the lowercased variant name
    /// as declared by [`crate::property::Property::VALUE_TYPE`] and stored in
    /// `_ankurah_property.value_type`. `None` for a string this build does not
    /// know (a newer fleet's type), which callers must treat as
    /// incompatible-unless-equal.
    pub fn from_property_str(s: &str) -> Option<ValueType> {
        Some(match s {
            "string" => ValueType::String,
            "i16" => ValueType::I16,
            "i32" => ValueType::I32,
            "i64" => ValueType::I64,
            "f64" => ValueType::F64,
            "bool" => ValueType::Bool,
            "entityid" => ValueType::EntityId,
            "object" => ValueType::Object,
            "binary" => ValueType::Binary,
            "json" => ValueType::Json,
            _ => return None,
        })
    }

    /// The type-level projection of [`Value::cast_to`]: whether a cast arrow
    /// exists from `self` to `target`. A `true` here still admits per-value
    /// failure (`InvalidFormat` / `NumericOverflow`); a `false` means every
    /// value of `self` returns `IncompatibleTypes`. Kept in lockstep with the
    /// `cast_to` match by the `castable_to_agrees_with_cast_to` test below.
    pub fn castable_to(self, target: ValueType) -> bool {
        use ValueType::*;
        if self == target {
            return true;
        }
        matches!(
            (self, target),
            // String <-> EntityId
            (String, EntityId) | (EntityId, String)
            // numeric <-> numeric
            | (I16, I32) | (I16, I64) | (I16, F64)
            | (I32, I16) | (I32, I64) | (I32, F64)
            | (I64, I16) | (I64, I32) | (I64, F64)
            | (F64, I16) | (F64, I32) | (F64, I64)
            // String <-> numeric / bool
            | (String, I16) | (String, I32) | (String, I64) | (String, F64) | (String, Bool)
            | (I16, String) | (I32, String) | (I64, String) | (F64, String) | (Bool, String)
            // Bool <-> numeric
            | (Bool, I16) | (Bool, I32) | (Bool, I64) | (Bool, F64)
            | (I16, Bool) | (I32, Bool) | (I64, Bool) | (F64, Bool)
            // scalar <-> Json
            | (String, Json) | (I16, Json) | (I32, Json) | (I64, Json) | (F64, Json) | (Bool, Json)
            | (Json, String) | (Json, I16) | (Json, I32) | (Json, I64) | (Json, F64) | (Json, Bool)
        )
    }

    /// Both cast arrows exist: a binary compiled with one of the pair can
    /// write through the other (compiled -> canonical) and read back
    /// (canonical -> compiled). The registration compatibility gate
    /// (rfc.md 5.6 as amended 2026-07-10) admits a drifted compiled type on
    /// exactly this condition.
    pub fn mutually_castable(a: ValueType, b: ValueType) -> bool { a.castable_to(b) && b.castable_to(a) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_to_entity_id() {
        let entity_id = EntityId::from_bytes([0x11; 32]);
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
        let entity_id = EntityId::from_bytes([0x22; 32]);
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

    /// `ValueType::castable_to` is the type-level projection of the
    /// `cast_to` match and must stay in lockstep with it: an arrow exists
    /// for a type pair exactly when SOME value of the source type casts
    /// without `IncompatibleTypes` (per-value `InvalidFormat` /
    /// `NumericOverflow` still count as an arrow). Json needs several
    /// representatives because its `cast_to` arms report a per-value SHAPE
    /// mismatch as `IncompatibleTypes`.
    #[test]
    fn castable_to_agrees_with_cast_to() {
        use ValueType::*;
        let representatives: Vec<Value> = vec![
            Value::I16(1),
            Value::I32(1),
            Value::I64(1),
            Value::F64(1.0),
            Value::Bool(true),
            Value::String("1".into()),
            Value::EntityId(ankurah_proto::EntityId::from_bytes([0x33; 32])),
            Value::Object(vec![]),
            Value::Binary(vec![1]),
            Value::Json(serde_json::json!(1)),
            Value::Json(serde_json::json!("s")),
            Value::Json(serde_json::json!(true)),
        ];
        let types = [I16, I32, I64, F64, Bool, String, EntityId, Object, Binary, Json];
        for source in types {
            let reps: Vec<&Value> = representatives.iter().filter(|v| ValueType::of(v) == source).collect();
            assert!(!reps.is_empty(), "no representative value for {source:?}");
            for target in types {
                let any_arrow = reps.iter().any(|v| !matches!(v.cast_to(target), Err(CastError::IncompatibleTypes { .. })));
                assert_eq!(source.castable_to(target), any_arrow, "castable_to({source:?}, {target:?}) disagrees with cast_to");
            }
        }
    }

    #[test]
    fn mutually_castable_pairs() {
        use ValueType::*;
        // The registration gate's admissions: both arrows must exist.
        assert!(ValueType::mutually_castable(String, I64));
        assert!(ValueType::mutually_castable(I32, I64));
        assert!(ValueType::mutually_castable(Bool, String));
        assert!(ValueType::mutually_castable(String, EntityId));
        // One-way or no-way pairs refuse.
        assert!(!ValueType::mutually_castable(String, Binary));
        assert!(!ValueType::mutually_castable(EntityId, I64));
        assert!(!ValueType::mutually_castable(Object, Json));
        assert!(!ValueType::mutually_castable(EntityId, Json));
    }
}
