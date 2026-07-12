//! Cryptographic node identity (RFC specs/identity-attestation/spec.md I.1).
//!
//! Every node holds an ed25519 keypair; the public (verifying) key IS the
//! node identity. `NodeId` replaces the historical use of `EntityId` for node
//! identity everywhere (Presence, peer maps, request/update routing,
//! subscription bookkeeping), completing the node-identity third of the
//! id-type split.

use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

use crate::error::DecodeError;
#[cfg(feature = "uniffi")]
use crate::error::IdParseError;

/// A node's identity: its ed25519 verifying-key bytes.
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct NodeId(pub(crate) [u8; 32]);

impl NodeId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self { NodeId(bytes) }

    pub fn to_bytes(&self) -> [u8; 32] { self.0 }

    pub fn as_bytes(&self) -> &[u8] { &self.0 }

    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(DecodeError::InvalidBase64)?;
        let bytes: [u8; 32] = decoded[..].try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(NodeId(bytes))
    }

    pub fn to_base64(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0) }

    pub fn to_base64_short(&self) -> String {
        // take the last 6 characters of the base64 encoded string
        let value = self.to_base64();
        value[value.len() - 6..].to_string()
    }

    /// The ed25519 verifying key these bytes claim to be. Fails on bytes
    /// that are not a valid curve point (a `NodeId` deserialized from the
    /// wire is untrusted input).
    pub fn verifying_key(&self) -> Result<ed25519_dalek::VerifyingKey, ed25519_dalek::SignatureError> {
        ed25519_dalek::VerifyingKey::from_bytes(&self.0)
    }

    /// Verify `signature` over `message` under this node's key.
    pub fn verify(&self, message: &[u8], signature: &crate::Signature) -> bool {
        match self.verifying_key() {
            // `verify_strict` rejects both weak public keys and small-order R
            // components. Keep the explicit key check as a local invariant:
            // a deserialized NodeId must never authenticate as a low-order
            // curve point, even if dalek's verification internals change.
            Ok(key) if !key.is_weak() => key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(&signature.0)).is_ok(),
            Err(_) => false,
            Ok(_) => false,
        }
    }
}

impl From<ed25519_dalek::VerifyingKey> for NodeId {
    fn from(key: ed25519_dalek::VerifyingKey) -> Self { NodeId(key.to_bytes()) }
}

impl From<&ed25519_dalek::VerifyingKey> for NodeId {
    fn from(key: &ed25519_dalek::VerifyingKey) -> Self { NodeId(key.to_bytes()) }
}

// Methods exported to both WASM and UniFFI
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", uniffi::export)]
impl NodeId {
    #[cfg_attr(feature = "wasm", wasm_bindgen(js_name = toString))]
    #[cfg_attr(feature = "uniffi", uniffi::method(name = "toString"))]
    pub fn to_string_exported(&self) -> String { self.to_base64() }
}

// WASM-only methods
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl NodeId {
    #[wasm_bindgen(js_name = to_base64)]
    pub fn to_base64_js(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0) }

    #[wasm_bindgen(js_name = from_base64)]
    pub fn from_base64_js(s: &str) -> Result<Self, JsValue> { Self::from_base64(s).map_err(|e| JsValue::from_str(&e.to_string())) }

    #[wasm_bindgen]
    pub fn equals(&self, other: &NodeId) -> bool { self.0 == other.0 }
}

// UniFFI-only methods
#[cfg(feature = "uniffi")]
#[uniffi::export]
impl NodeId {
    /// Parse a NodeId from a base64 string
    #[uniffi::constructor(name = "fromBase64")]
    pub fn from_base64_uniffi(s: String) -> Result<Self, IdParseError> { Self::from_base64(s).map_err(|e| e.into()) }

    /// Compare two NodeIds for equality
    #[uniffi::method(name = "equals")]
    pub fn equals_uniffi(&self, other: &NodeId) -> bool { self.0 == other.0 }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if f.alternate() {
            write!(f, "{}", self.to_base64_short())
        } else {
            write!(f, "{}", self.to_base64())
        }
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "NodeId({})", self.to_base64()) }
}

impl TryFrom<&str> for NodeId {
    type Error = DecodeError;
    fn try_from(id: &str) -> Result<Self, Self::Error> { Self::from_base64(id) }
}

impl TryFrom<String> for NodeId {
    type Error = DecodeError;
    fn try_from(id: String) -> Result<Self, Self::Error> { Self::try_from(id.as_str()) }
}

impl std::str::FromStr for NodeId {
    type Err = DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> { Self::from_base64(s) }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> String { id.to_base64() }
}

impl From<&NodeId> for String {
    fn from(id: &NodeId) -> String { id.to_base64() }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            NodeId::from_base64(s).map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; 32]>::deserialize(deserializer)?;
            Ok(NodeId::from_bytes(bytes))
        }
    }
}

/// An ed25519 signature (64 bytes). Signed and verified against domain-tagged
/// preimages only; the tags live next to the types they cover
/// ([`crate::peering::PRESENCE_TAG`], [`crate::auth::ATTESTATION_TAG`]).
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Signature(pub(crate) [u8; 64]);

impl Signature {
    pub fn from_bytes(bytes: [u8; 64]) -> Self { Signature(bytes) }

    pub fn to_bytes(&self) -> [u8; 64] { self.0 }

    pub fn as_bytes(&self) -> &[u8] { &self.0 }

    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(DecodeError::InvalidBase64)?;
        let bytes: [u8; 64] = decoded[..].try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Signature(bytes))
    }

    pub fn to_base64(&self) -> String { general_purpose::URL_SAFE_NO_PAD.encode(self.0) }
}

impl From<ed25519_dalek::Signature> for Signature {
    fn from(sig: ed25519_dalek::Signature) -> Self { Signature(sig.to_bytes()) }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> { write!(f, "{}", self.to_base64()) }
}

impl std::fmt::Debug for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Signature({})", self.to_base64()) }
}

// serde does not implement the array traits beyond 32 elements, so the 64-byte
// signature serializes as two 32-byte halves in binary formats (bincode
// concatenates fixed arrays, so the encoding is exactly the 64 raw bytes) and
// as base64 in human-readable formats.
impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            let first: [u8; 32] = self.0[..32].try_into().expect("fixed split");
            let second: [u8; 32] = self.0[32..].try_into().expect("fixed split");
            (first, second).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            Signature::from_base64(s).map_err(serde::de::Error::custom)
        } else {
            let (first, second) = <([u8; 32], [u8; 32])>::deserialize(deserializer)?;
            let mut bytes = [0u8; 64];
            bytes[..32].copy_from_slice(&first);
            bytes[32..].copy_from_slice(&second);
            Ok(Signature(bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_round_trips() {
        let id = NodeId::from_bytes([7u8; 32]);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(id, serde_json::from_str(&json).unwrap());
        let bytes = bincode::serialize(&id).unwrap();
        assert_eq!(bytes, [7u8; 32]);
        assert_eq!(id, bincode::deserialize(&bytes).unwrap());
    }

    #[test]
    fn signature_bincode_is_the_64_raw_bytes() {
        let mut raw = [0u8; 64];
        for (i, b) in raw.iter_mut().enumerate() {
            *b = i as u8;
        }
        let sig = Signature::from_bytes(raw);
        let bytes = bincode::serialize(&sig).unwrap();
        assert_eq!(bytes, raw);
        assert_eq!(sig, bincode::deserialize::<Signature>(&bytes).unwrap());

        let json = serde_json::to_string(&sig).unwrap();
        assert_eq!(sig, serde_json::from_str(&json).unwrap());
    }

    #[test]
    fn verify_rejects_wrong_key_and_wrong_message() {
        use ed25519_dalek::Signer;
        let key = ed25519_dalek::SigningKey::from_bytes(&[3u8; 32]);
        let node_id = NodeId::from(key.verifying_key());
        let sig: Signature = key.sign(b"hello").into();

        assert!(node_id.verify(b"hello", &sig));
        assert!(!node_id.verify(b"other message", &sig));

        let other = ed25519_dalek::SigningKey::from_bytes(&[4u8; 32]);
        assert!(!NodeId::from(other.verifying_key()).verify(b"hello", &sig));

        // Bytes that are not a curve point verify nothing.
        assert!(!NodeId::from_bytes([0xFFu8; 32]).verify(b"hello", &sig));
    }

    #[test]
    fn strict_verification_rejects_cross_message_small_order_forgery() {
        use ed25519_dalek::Verifier;

        // Deterministic low-order forgery: A is the Edwards identity, S=1,
        // and R=B. The legacy/cofactored verification equation accepts this
        // same signature for every message because [k]A is always zero.
        let mut weak_key_bytes = [0u8; 32];
        weak_key_bytes[0] = 1;
        let weak_key = ed25519_dalek::VerifyingKey::from_bytes(&weak_key_bytes).expect("identity point is encoded canonically");
        assert!(weak_key.is_weak());

        let mut forged_signature_bytes = [0x66u8; 64];
        forged_signature_bytes[0] = 0x58; // compressed Edwards basepoint R
        forged_signature_bytes[32..].fill(0);
        forged_signature_bytes[32] = 1; // canonical scalar S=1
        let forged_dalek_signature = ed25519_dalek::Signature::from_bytes(&forged_signature_bytes);

        let first = b"transfer 1 unit";
        let second = b"transfer 1000000 units";
        assert!(weak_key.verify(first, &forged_dalek_signature).is_ok(), "vector must demonstrate the legacy forgery");
        assert!(weak_key.verify(second, &forged_dalek_signature).is_ok(), "one signature must forge a second message");

        let node_id = NodeId::from_bytes(weak_key_bytes);
        let forged_signature = Signature::from_bytes(forged_signature_bytes);
        assert!(!node_id.verify(first, &forged_signature));
        assert!(!node_id.verify(second, &forged_signature));

        // Strict verification continues to accept ordinary deterministic
        // keys and signatures.
        use ed25519_dalek::Signer;
        let normal_key = ed25519_dalek::SigningKey::from_bytes(&[0xA7; 32]);
        let normal_node = NodeId::from(normal_key.verifying_key());
        let normal_signature: Signature = normal_key.sign(first).into();
        assert!(!normal_key.verifying_key().is_weak());
        assert!(normal_node.verify(first, &normal_signature));
        assert!(!normal_node.verify(second, &normal_signature));
    }
}
