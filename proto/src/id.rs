use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

use crate::error::DecodeError;
// TODO - split out the different id types. Presently there's a lot of not-entities that are using this type for their ID
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct EntityId(pub(crate) Ulid);

impl EntityId {
    pub fn new() -> Self { EntityId(Ulid::new()) }

    pub fn from_bytes(bytes: [u8; 16]) -> Self { EntityId(Ulid::from_bytes(bytes)) }

    pub fn to_bytes(&self) -> [u8; 16] { self.0.to_bytes() }

    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(DecodeError::InvalidBase64)?;
        let bytes: [u8; 16] = decoded[..].try_into().map_err(|_| DecodeError::InvalidLength)?;

        Ok(EntityId(Ulid::from_bytes(bytes)))
    }

    pub fn to_base64(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0.to_bytes()) }

    pub fn to_base64_short(&self) -> String {
        // take the last 6 characters of the base64 encoded string
        let value = self.to_base64();
        value[value.len() - 6..].to_string()
    }

    pub fn to_ulid(&self) -> Ulid { self.0 }
    pub fn from_ulid(ulid: Ulid) -> Self { EntityId(ulid) }
}

// Methods exported to both WASM and UniFFI
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", uniffi::export)]
impl EntityId {
    #[cfg_attr(feature = "wasm", wasm_bindgen(js_name = toString))]
    pub fn to_string(&self) -> String { self.to_base64() }
}

// WASM-only methods
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl EntityId {
    #[wasm_bindgen(js_name = to_base64)]
    pub fn to_base64_js(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0.to_bytes()) }

    #[wasm_bindgen(js_name = from_base64)]
    pub fn from_base64_js(s: &str) -> Result<Self, JsValue> { Self::from_base64(s).map_err(|e| JsValue::from_str(&e.to_string())) }

    #[wasm_bindgen]
    pub fn equals(&self, other: &EntityId) -> bool { self.0 == other.0 }
}

// UniFFI-only methods
#[cfg(feature = "uniffi")]
#[uniffi::export]
impl EntityId {
    /// Parse an EntityId from a base64 string
    #[uniffi::constructor(name = "fromBase64")]
    pub fn from_base64_uniffi(s: String) -> Result<Self, IdParseError> { Self::from_base64(s).map_err(|e| e.into()) }

    /// Compare two EntityIds for equality
    #[uniffi::method(name = "equals")]
    pub fn equals_uniffi(&self, other: &EntityId) -> bool { self.0 == other.0 }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if f.alternate() {
            write!(f, "{}", self.to_base64_short())
        } else {
            write!(f, "{}", self.to_base64())
        }
    }
}
impl std::fmt::Debug for EntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.to_base64()) }
}

impl TryFrom<&str> for EntityId {
    type Error = DecodeError;
    fn try_from(id: &str) -> Result<Self, Self::Error> { Self::from_base64(id) }
}

impl TryFrom<String> for EntityId {
    type Error = DecodeError;
    fn try_from(id: String) -> Result<Self, Self::Error> { Self::try_from(id.as_str()) }
}

impl TryFrom<&String> for EntityId {
    type Error = DecodeError;
    fn try_from(id: &String) -> Result<Self, Self::Error> { Self::try_from(id.as_str()) }
}

impl std::str::FromStr for EntityId {
    type Err = DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> { Self::from_base64(s) }
}

impl From<EntityId> for String {
    fn from(id: EntityId) -> String { id.to_base64() }
}

impl From<&EntityId> for String {
    fn from(id: &EntityId) -> String { id.to_base64() }
}

impl TryInto<EntityId> for Vec<u8> {
    type Error = DecodeError;
    fn try_into(self) -> Result<EntityId, Self::Error> {
        let bytes: [u8; 16] = self.try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(EntityId(Ulid::from_bytes(bytes)))
    }
}

impl From<EntityId> for Ulid {
    fn from(id: EntityId) -> Self { id.0 }
}

impl Default for EntityId {
    fn default() -> Self { Self::new() }
}

impl Serialize for EntityId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            // Use base64 for human-readable formats like JSON
            serializer.serialize_str(&self.to_base64())
        } else {
            // Use raw bytes as a fixed-size array for binary formats like bincode
            self.to_bytes().serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EntityId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            // Deserialize from base64 string for human-readable formats
            let s = String::deserialize(deserializer)?;
            EntityId::from_base64(s).map_err(serde::de::Error::custom)
        } else {
            // Deserialize from raw bytes as a fixed-size array for binary formats
            let bytes = <[u8; 16]>::deserialize(deserializer)?;
            Ok(EntityId::from_bytes(bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_id_json_serialization() {
        let id = EntityId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"AQIDBAUGBwgJCgsMDQ4PEA\"");
        assert_eq!(id, serde_json::from_str(&json).unwrap());
    }

    #[test]
    fn test_entity_id_bincode_serialization() {
        let id = EntityId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let bytes = bincode::serialize(&id).unwrap();
        assert_eq!(bytes, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(id, bincode::deserialize(&bytes).unwrap());
    }
}

// EntityId support for predicates

impl From<EntityId> for ankql::ast::Expr {
    fn from(id: EntityId) -> ankql::ast::Expr { ankql::ast::Expr::Literal(ankql::ast::Literal::EntityId(id.to_ulid())) }
}

impl From<&EntityId> for ankql::ast::Expr {
    fn from(id: &EntityId) -> ankql::ast::Expr { ankql::ast::Expr::Literal(ankql::ast::Literal::EntityId(id.to_ulid())) }
}
