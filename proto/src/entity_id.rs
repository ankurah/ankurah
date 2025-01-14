use anyhow::anyhow;
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

use wasm_bindgen::prelude::*;

use crate::DecodeError;
// TODO - split out the different id types. Presently there's a lot of not-entities that are using this type for their ID
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize)]
#[wasm_bindgen]
pub struct ID(Ulid);
impl ID {
    pub fn new() -> Self { ID(Ulid::new()) }

    pub fn from_ulid(ulid: Ulid) -> Self { ID(ulid) }

    pub fn to_bytes(&self) -> [u8; 16] { self.0.to_bytes() }

    pub fn from_base64(base64_string: &str) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(base64_string).map_err(|e| DecodeError::InvalidBase64(e))?;
        let bytes: [u8; 16] = decoded[..].try_into().map_err(|_| DecodeError::InvalidLength)?;

        Ok(ID(Ulid::from_bytes(bytes)))
    }

    pub fn to_base64(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0.to_bytes()) }
}

#[wasm_bindgen]
impl ID {
    pub fn as_string(&self) -> String { self.to_base64() }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> { write!(f, "{}", self.to_base64()) }
}
impl Into<String> for ID {
    fn into(self) -> String { self.to_base64() }
}
impl TryFrom<&str> for ID {
    type Error = DecodeError;
    fn try_from(id: &str) -> Result<Self, Self::Error> {
        match Self::from_base64(id) {
            Ok(id) => Ok(id),
            Err(DecodeError::InvalidLength) => {
                // fall back to ulid (base32) for compatibility with old ids
                // REMOVE THIS ONCE ALL OLD IDS ARE CONVERTED TO BASE64
                let ulid = Ulid::from_string(id).map_err(|_| DecodeError::InvalidUlid).map_err(|_| DecodeError::InvalidFallback)?;
                Ok(ID::from_ulid(ulid))
            }
            Err(e) => Err(e),
        }
    }
}

impl TryFrom<String> for ID {
    type Error = DecodeError;
    fn try_from(id: String) -> Result<Self, Self::Error> { Self::try_from(id.as_str()) }
}

impl TryFrom<&String> for ID {
    type Error = DecodeError;
    fn try_from(id: &String) -> Result<Self, Self::Error> { Self::try_from(id.as_str()) }
}

impl TryFrom<JsValue> for ID {
    type Error = DecodeError;
    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let id: String = value.as_string().ok_or(DecodeError::NotStringValue)?;
        id.try_into()
    }
}

impl std::fmt::Debug for ID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0.to_string()) }
}

impl From<ID> for Ulid {
    fn from(id: ID) -> Self { id.0 }
}

impl Default for ID {
    fn default() -> Self { Self::new() }
}
