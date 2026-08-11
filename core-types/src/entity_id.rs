use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

use crate::DecodeError;
#[cfg(feature = "uniffi")]
use crate::IdParseError;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
/// A durable, globally unique entity identity: the 32 bytes of the entity's
/// genesis event id.
///
/// Because the id IS the hash over the genesis content, an entity's identity
/// cannot be chosen ahead of the content it names; the creation path derives
/// it (`ankurah_proto::Event::genesis`).
///
/// The human-readable representation is an unpadded URL-safe base64 encoding
/// of those 32 bytes. Binary serializers receive the raw bytes.
pub struct EntityId([u8; 32]);

impl EntityId {
    /// The exact byte width of an entity id.
    pub const BYTE_LEN: usize = 32;

    /// Draw 32 random bytes as an identity.
    ///
    /// This value names no genesis event, so no entity can ever carry it. It
    /// exists for the two identities that are not entities: a node's own id
    /// and test ids that deliberately name nothing. Creating an entity derives
    /// its id instead.
    ///
    /// TODO make this test-only. It cannot be `#[cfg(test)]` yet because a
    /// node draws its own id from here in production. That use migrates when
    /// the deferred node-identity work gives nodes their own key-derived type,
    /// and this becomes a test affordance and nothing else.
    pub fn random() -> Self {
        let mut bytes = [0u8; Self::BYTE_LEN];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
        Self(bytes)
    }

    /// Construct an identity from its exact 32-byte representation.
    pub fn from_bytes(bytes: [u8; 32]) -> Self { Self(bytes) }

    /// Return the exact 32-byte representation.
    pub fn to_bytes(&self) -> [u8; 32] { self.0 }

    /// Decode an identity from unpadded URL-safe base64.
    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(DecodeError::InvalidBase64)?;
        let bytes: [u8; 32] = decoded.as_slice().try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Self(bytes))
    }

    /// Encode this identity as unpadded URL-safe base64.
    pub fn to_base64(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0) }

    /// Return the first six characters of the base64 representation.
    pub fn to_base64_short(&self) -> String {
        let value = self.to_base64();
        value[..6].to_owned()
    }
}

#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", uniffi::export)]
impl EntityId {
    #[cfg_attr(feature = "wasm", wasm_bindgen(js_name = toString))]
    /// Return the canonical base64 representation.
    pub fn to_string(&self) -> String { self.to_base64() }
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl EntityId {
    #[wasm_bindgen(js_name = to_base64)]
    /// Return the canonical base64 representation to JavaScript.
    pub fn to_base64_js(&self) -> String { self.to_base64() }

    #[wasm_bindgen(js_name = from_base64)]
    /// Decode an identity from base64 in JavaScript.
    pub fn from_base64_js(value: &str) -> Result<Self, JsValue> {
        Self::from_base64(value).map_err(|error| JsValue::from_str(&error.to_string()))
    }

    #[wasm_bindgen]
    /// Compare two identities in JavaScript.
    pub fn equals(&self, other: &EntityId) -> bool { self == other }
}

#[cfg(feature = "uniffi")]
#[uniffi::export]
impl EntityId {
    #[uniffi::constructor(name = "fromBase64")]
    /// Decode an identity from base64 through UniFFI.
    pub fn from_base64_uniffi(value: String) -> Result<Self, IdParseError> { Self::from_base64(value).map_err(Into::into) }

    #[uniffi::method(name = "equals")]
    /// Compare two identities through UniFFI.
    pub fn equals_uniffi(&self, other: &EntityId) -> bool { self == other }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.write_str(&self.to_base64_short())
        } else {
            f.write_str(&self.to_base64())
        }
    }
}

impl fmt::Debug for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.to_base64()) }
}

impl TryFrom<&str> for EntityId {
    type Error = DecodeError;
    fn try_from(value: &str) -> Result<Self, Self::Error> { Self::from_base64(value) }
}

impl TryFrom<String> for EntityId {
    type Error = DecodeError;
    fn try_from(value: String) -> Result<Self, Self::Error> { Self::try_from(value.as_str()) }
}

impl TryFrom<&String> for EntityId {
    type Error = DecodeError;
    fn try_from(value: &String) -> Result<Self, Self::Error> { Self::try_from(value.as_str()) }
}

impl std::str::FromStr for EntityId {
    type Err = DecodeError;
    fn from_str(value: &str) -> Result<Self, Self::Err> { Self::from_base64(value) }
}

impl From<EntityId> for String {
    fn from(id: EntityId) -> Self { id.to_base64() }
}

impl From<&EntityId> for String {
    fn from(id: &EntityId) -> Self { id.to_base64() }
}

impl TryFrom<Vec<u8>> for EntityId {
    type Error = DecodeError;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let bytes: [u8; 32] = value.try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Self::from_bytes(bytes))
    }
}

impl Serialize for EntityId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            self.to_bytes().serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EntityId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            Self::from_base64(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
        } else {
            Ok(Self::from_bytes(<[u8; 32]>::deserialize(deserializer)?))
        }
    }
}

#[cfg(feature = "wasm")]
impl TryFrom<wasm_bindgen::JsValue> for EntityId {
    type Error = DecodeError;

    fn try_from(value: wasm_bindgen::JsValue) -> Result<Self, Self::Error> {
        Self::from_base64(value.as_string().ok_or(DecodeError::NotStringValue)?)
    }
}

#[cfg(feature = "wasm")]
impl From<&EntityId> for wasm_bindgen::JsValue {
    fn from(id: &EntityId) -> Self { id.to_base64().into() }
}

#[cfg(feature = "postgres")]
impl postgres_types::ToSql for EntityId {
    fn to_sql(
        &self,
        _: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use base64::write::EncoderWriter;
        use bytes::BufMut;
        use std::io::Write;

        let mut encoder = EncoderWriter::new(out.writer(), &general_purpose::URL_SAFE_NO_PAD);
        encoder.write_all(&self.to_bytes())?;
        encoder.finish()?;
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &postgres_types::Type) -> bool { matches!(ty.name(), "character" | "bpchar") }

    postgres_types::to_sql_checked!();
}

#[cfg(feature = "postgres")]
impl<'a> postgres_types::FromSql<'a> for EntityId {
    fn from_sql(_: &postgres_types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self::from_base64(raw)?)
    }

    fn accepts(ty: &postgres_types::Type) -> bool { matches!(ty.name(), "character" | "bpchar") }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_and_bincode_encodings_are_pinned() {
        let id = EntityId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ]);
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA\"");
        assert_eq!(bincode::serialize(&id).unwrap(), id.to_bytes());
        assert_eq!(serde_json::from_str::<EntityId>(&serde_json::to_string(&id).unwrap()).unwrap(), id);
        assert_eq!(bincode::deserialize::<EntityId>(&bincode::serialize(&id).unwrap()).unwrap(), id);
    }

    /// The base64 form is exactly 43 characters; a 22-character (16-byte)
    /// string does not decode.
    #[test]
    fn only_the_full_width_decodes() {
        let id = EntityId::from_bytes([0xAB; 32]);
        assert_eq!(id.to_base64().len(), 43);
        assert_eq!(EntityId::from_base64(id.to_base64()).unwrap(), id);
        assert!(matches!(EntityId::from_base64("AQIDBAUGBwgJCgsMDQ4PEA"), Err(DecodeError::InvalidLength)));
    }
}
