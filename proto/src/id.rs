use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

use wasm_bindgen::prelude::*;

use crate::error::DecodeError;
// TODO - split out the different id types. Presently there's a lot of not-entities that are using this type for their ID
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize)]
#[wasm_bindgen]
pub struct EntityID(Ulid);
// TODO - get rid of ID in favor of EntityId and other discrete ID types

impl EntityID {
    pub fn new() -> Self { EntityID(Ulid::new()) }

    pub fn from_ulid(ulid: Ulid) -> Self { EntityID(ulid) }

    pub fn to_bytes(&self) -> [u8; 16] { self.0.to_bytes() }

    pub fn from_base64(base64_string: &str) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(base64_string).map_err(|e| DecodeError::InvalidBase64(e))?;
        let bytes: [u8; 16] = decoded[..].try_into().map_err(|_| DecodeError::InvalidLength)?;

        Ok(EntityID(Ulid::from_bytes(bytes)))
    }

    pub fn to_base64(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0.to_bytes()) }

    pub fn to_base64_short(&self) -> String {
        // take the last 6 characters of the base64 encoded string
        let value = self.to_base64();
        value[value.len() - 6..].to_string()
    }
}

#[wasm_bindgen]
impl EntityID {
    pub fn as_string(&self) -> String { self.to_base64() }

    #[wasm_bindgen(js_name = to_base64)]
    pub fn to_base64_js(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0.to_bytes()) }

    #[wasm_bindgen(js_name = from_base64)]
    pub fn from_base64_js(s: &str) -> Result<Self, JsValue> { Self::from_base64(s).map_err(|e| JsValue::from_str(&e.to_string())) }
}

impl fmt::Display for EntityID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> { write!(f, "{}", self.to_base64_short()) }
}

impl TryFrom<&str> for EntityID {
    type Error = DecodeError;
    fn try_from(id: &str) -> Result<Self, Self::Error> {
        match Self::from_base64(id) {
            Ok(id) => Ok(id),
            Err(DecodeError::InvalidLength) => {
                // fall back to ulid (base32) for compatibility with old ids
                // REMOVE THIS ONCE ALL OLD IDS ARE CONVERTED TO BASE64
                let ulid = Ulid::from_string(id).map_err(|_| DecodeError::InvalidUlid).map_err(|_| DecodeError::InvalidFallback)?;
                Ok(EntityID::from_ulid(ulid))
            }
            Err(e) => Err(e),
        }
    }
}

impl TryFrom<String> for EntityID {
    type Error = DecodeError;
    fn try_from(id: String) -> Result<Self, Self::Error> { Self::try_from(id.as_str()) }
}

impl TryFrom<&String> for EntityID {
    type Error = DecodeError;
    fn try_from(id: &String) -> Result<Self, Self::Error> { Self::try_from(id.as_str()) }
}

impl TryFrom<JsValue> for EntityID {
    type Error = DecodeError;
    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let id: String = value.as_string().ok_or(DecodeError::NotStringValue)?;
        id.try_into()
    }
}

impl std::fmt::Debug for EntityID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0.to_string()) }
}

impl From<EntityID> for Ulid {
    fn from(id: EntityID) -> Self { id.0 }
}

impl Default for EntityID {
    fn default() -> Self { Self::new() }
}
