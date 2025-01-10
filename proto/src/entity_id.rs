use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

use wasm_bindgen::prelude::*;
// TODO - split out the different id types. Presently there's a lot of not-entities that are using this type for their ID
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize)]
#[wasm_bindgen]
pub struct ID(Ulid);

impl std::fmt::Debug for ID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0.to_string()) }
}

impl From<ID> for Ulid {
    fn from(id: ID) -> Self { id.0 }
}

impl Default for ID {
    fn default() -> Self { Self::new() }
}

impl TryFrom<String> for ID {
    type Error = ulid::DecodeError;
    fn try_from(id: String) -> Result<Self, Self::Error> { Ok(Self::from_ulid(Ulid::from_string(&id)?)) }
}

impl ID {
    pub fn new() -> Self { ID(Ulid::new()) }

    pub fn from_ulid(ulid: Ulid) -> Self { ID(ulid) }

    pub fn to_bytes(&self) -> [u8; 16] { self.0.to_bytes() }
}

#[wasm_bindgen]
impl ID {
    pub fn as_string(&self) -> String { self.0.to_string() }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> { write!(f, "I{}", &self.0.to_string()[20..]) }
}

impl AsRef<ID> for ID {
    fn as_ref(&self) -> &ID { self }
}
