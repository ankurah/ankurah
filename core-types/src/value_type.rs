use serde::{Deserialize, Serialize};

/// The durable logical type of a property value.
///
/// This lives beside the identity types so query ASTs, catalog resolvers, and
/// storage consumers can agree on a type without depending on `ankurah-core`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    /// A signed 16-bit integer.
    I16,
    /// A signed 32-bit integer.
    I32,
    /// A signed 64-bit integer.
    I64,
    /// A 64-bit IEEE-754 floating-point number.
    F64,
    /// A Boolean value.
    Bool,
    /// A UTF-8 string.
    String,
    /// A durable entity identity.
    EntityId,
    /// Opaque backend-defined object bytes.
    Object,
    /// Arbitrary binary bytes.
    Binary,
    /// A structured JSON value.
    Json,
}

impl ValueType {
    /// Parse the catalog spelling emitted by property declarations.
    pub fn from_property_str(s: &str) -> Option<Self> {
        Some(match s {
            "string" => Self::String,
            "i16" => Self::I16,
            "i32" => Self::I32,
            "i64" => Self::I64,
            "f64" => Self::F64,
            "bool" => Self::Bool,
            "entityid" => Self::EntityId,
            "object" => Self::Object,
            "binary" => Self::Binary,
            "json" => Self::Json,
            _ => return None,
        })
    }

    /// Whether this type has a value-level cast path to `target`.
    /// Individual values may still fail because of format or overflow.
    pub fn castable_to(self, target: Self) -> bool {
        use ValueType::*;
        if self == target {
            return true;
        }
        matches!(
            (self, target),
            (String, EntityId)
                | (EntityId, String)
                | (I16, I32)
                | (I16, I64)
                | (I16, F64)
                | (I32, I16)
                | (I32, I64)
                | (I32, F64)
                | (I64, I16)
                | (I64, I32)
                | (I64, F64)
                | (F64, I16)
                | (F64, I32)
                | (F64, I64)
                | (String, I16)
                | (String, I32)
                | (String, I64)
                | (String, F64)
                | (String, Bool)
                | (I16, String)
                | (I32, String)
                | (I64, String)
                | (F64, String)
                | (Bool, String)
                | (Bool, I16)
                | (Bool, I32)
                | (Bool, I64)
                | (Bool, F64)
                | (I16, Bool)
                | (I32, Bool)
                | (I64, Bool)
                | (F64, Bool)
                | (String, Json)
                | (I16, Json)
                | (I32, Json)
                | (I64, Json)
                | (F64, Json)
                | (Bool, Json)
                | (Json, String)
                | (Json, I16)
                | (Json, I32)
                | (Json, I64)
                | (Json, F64)
                | (Json, Bool)
        )
    }

    /// Whether values can be converted in both directions between two types.
    pub fn mutually_castable(a: Self, b: Self) -> bool { a.castable_to(b) && b.castable_to(a) }
}

#[cfg(test)]
mod tests {
    use super::ValueType;

    #[test]
    fn mutually_castable_pairs() {
        use ValueType::*;
        assert!(ValueType::mutually_castable(String, I64));
        assert!(ValueType::mutually_castable(I32, I64));
        assert!(ValueType::mutually_castable(Bool, String));
        assert!(ValueType::mutually_castable(String, EntityId));
        assert!(!ValueType::mutually_castable(String, Binary));
        assert!(!ValueType::mutually_castable(EntityId, I64));
        assert!(!ValueType::mutually_castable(Object, Json));
        assert!(!ValueType::mutually_castable(EntityId, Json));
    }
}
