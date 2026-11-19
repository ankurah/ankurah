use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

use crate::DecodeError;
#[cfg(feature = "uniffi")]
use crate::IdParseError;

/// A node id identifies a node by its public key and is distinct from an [`EntityId`](crate::EntityId).
/// For Ed25519, the node id is the public key's 32 bytes.
/// Human-readable serializers use unpadded URL-safe base64; binary serializers use the 32 bytes without a length prefix.
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
#[cfg_attr(feature = "uniffi", uniffi::export(Display, Eq, Hash))]
pub struct NodeId([u8; 32]);

impl NodeId {
    /// Constructs a node id from its 32 bytes without validating a public key.
    pub fn from_bytes(bytes: [u8; 32]) -> Self { Self(bytes) }

    /// Returns the node id's 32 bytes.
    pub fn to_bytes(&self) -> [u8; 32] { self.0 }

    /// Decodes a node id from unpadded URL-safe base64 and rejects any decoded length other than 32 bytes.
    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(DecodeError::InvalidBase64)?;
        let bytes: [u8; 32] = decoded.as_slice().try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Self(bytes))
    }

    /// Encodes the node id as unpadded URL-safe base64.
    pub fn to_base64(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0) }

    /// Returns the first six base64 characters for abbreviated display.
    pub fn to_base64_short(&self) -> String {
        let value = self.to_base64();
        value[..6].to_owned()
    }
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl NodeId {
    #[wasm_bindgen(js_name = to_base64)]
    /// Exposes the node id's unpadded URL-safe base64 representation to JavaScript.
    pub fn to_base64_js(&self) -> String { self.to_base64() }
}

#[cfg(feature = "uniffi")]
#[uniffi::export]
impl NodeId {
    #[uniffi::constructor(name = "fromBase64")]
    /// Decodes a node id from unpadded URL-safe base64 text through UniFFI.
    pub fn from_base64_uniffi(value: String) -> Result<Self, IdParseError> { Self::from_base64(value).map_err(Into::into) }

    #[uniffi::method(name = "equals")]
    /// Compares two node ids through UniFFI.
    pub fn equals_uniffi(&self, other: &NodeId) -> bool { self == other }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.write_str(&self.to_base64_short())
        } else {
            f.write_str(&self.to_base64())
        }
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.to_base64()) }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            self.to_bytes().serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            Self::from_base64(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
        } else {
            Ok(Self::from_bytes(<[u8; 32]>::deserialize(deserializer)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_uses_base64_and_bincode_uses_32_raw_bytes() {
        let id = NodeId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ]);
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA\"");
        assert_eq!(bincode::serialize(&id).unwrap(), id.to_bytes());
        assert_eq!(serde_json::from_str::<NodeId>(&serde_json::to_string(&id).unwrap()).unwrap(), id);
        assert_eq!(bincode::deserialize::<NodeId>(&bincode::serialize(&id).unwrap()).unwrap(), id);
    }
}
