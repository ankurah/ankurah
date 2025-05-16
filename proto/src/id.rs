use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

use wasm_bindgen::prelude::*;

use crate::error::DecodeError;
// TODO - split out the different id types. Presently there's a lot of not-entities that are using this type for their ID
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize)]
#[wasm_bindgen]
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
}

#[cfg_attr(feature = "wasm", wasm_bindgen)]
impl EntityId {
    pub fn as_string(&self) -> String { self.to_base64() }

    #[wasm_bindgen(js_name = to_base64)]
    pub fn to_base64_js(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0.to_bytes()) }

    #[cfg(feature = "wasm")]
    #[wasm_bindgen(js_name = from_base64)]
    pub fn from_base64_js(s: &str) -> Result<Self, JsValue> { Self::from_base64(s).map_err(|e| JsValue::from_str(&e.to_string())) }
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

impl TryInto<EntityId> for Vec<u8> {
    type Error = DecodeError;
    fn try_into(self) -> Result<EntityId, Self::Error> {
        let bytes: [u8; 16] = self.try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(EntityId(Ulid::from_bytes(bytes)))
    }
}

impl std::fmt::Debug for EntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0.to_string()) }
}

impl From<EntityId> for Ulid {
    fn from(id: EntityId) -> Self { id.0 }
}

impl Default for EntityId {
    fn default() -> Self { Self::new() }
}
