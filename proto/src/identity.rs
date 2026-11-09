use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::DecodeError;

/// A cryptographic identity: a public verifying key. It names who minted a
/// genesis and who signed a presence or an attestation. Core records the key
/// and nothing about what it denotes.
///
/// The variant names the signature scheme. Binary encodings write the variant
/// index before the key bytes, and every genesis hashes its minter, so the
/// index is part of every entity id: variants append and never reorder.
///
/// The human-readable form is the unpadded URL-safe base64 of the key bytes,
/// as for `EntityId`; binary serializers receive the variant index, then the
/// bytes.
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
pub enum Identity {
    /// An ed25519 verifying key in its 32-byte compressed encoding.
    Ed25519([u8; 32]),
}

/// A signature by an [`Identity`], in the identity's scheme.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum Signature {
    /// A 64-byte ed25519 signature.
    Ed25519([u8; 64]),
}

/// The peer layer's name for an [`Identity`] that is a node: the key a node
/// signs its presence with. A type of its own so a publisher's key cannot be
/// passed where a peer is meant.
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(Identity);

impl Identity {
    /// Decode an identity from the unpadded URL-safe base64 of an ed25519 key.
    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(DecodeError::InvalidBase64)?;
        let bytes: [u8; 32] = decoded.as_slice().try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Self::Ed25519(bytes))
    }

    /// Encode the key bytes as unpadded URL-safe base64.
    pub fn to_base64(&self) -> String {
        match self {
            Self::Ed25519(key) => general_purpose::URL_SAFE_NO_PAD.encode(key),
        }
    }

    /// The first six characters of the base64 form, for compact diagnostics.
    pub fn to_base64_short(&self) -> String {
        let value = self.to_base64();
        value[..6].to_owned()
    }

    /// Whether `signature` is this identity's signature over `message`.
    ///
    /// Bytes that are not the canonical encoding of a key, a weak key, and a
    /// non-canonical signature all verify as false.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        match (self, signature) {
            (Self::Ed25519(key), Signature::Ed25519(signature)) => strict_ed25519_key(key)
                .is_some_and(|key| key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(signature)).is_ok()),
        }
    }
}

/// The ed25519 key `bytes` encode, if they are its canonical encoding and it is not weak.
fn strict_ed25519_key(bytes: &[u8; 32]) -> Option<ed25519_dalek::VerifyingKey> {
    let key = ed25519_dalek::VerifyingKey::from_bytes(bytes).ok()?;
    // `from_bytes` accepts a non-canonical encoding and keeps the input bytes, so re-encode the point.
    (key.to_edwards().compress().to_bytes() == *bytes && !key.is_weak()).then_some(key)
}

impl Signature {
    /// Encode the signature bytes as unpadded URL-safe base64.
    pub fn to_base64(&self) -> String {
        match self {
            Self::Ed25519(signature) => general_purpose::URL_SAFE_NO_PAD.encode(signature),
        }
    }

    /// Decode an ed25519 signature from unpadded URL-safe base64.
    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(DecodeError::InvalidBase64)?;
        let bytes: [u8; 64] = decoded.as_slice().try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Self::Ed25519(bytes))
    }
}

impl NodeId {
    /// The identity this node signs as.
    pub fn identity(&self) -> Identity { self.0 }
}

impl From<Identity> for NodeId {
    fn from(identity: Identity) -> Self { Self(identity) }
}

impl From<NodeId> for Identity {
    fn from(node: NodeId) -> Self { node.0 }
}

impl std::ops::Deref for NodeId {
    type Target = Identity;
    fn deref(&self) -> &Identity { &self.0 }
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.write_str(&self.to_base64_short())
        } else {
            f.write_str(&self.to_base64())
        }
    }
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.to_base64()) }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Display::fmt(&self.0, f) }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Debug::fmt(&self.0, f) }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.to_base64()) }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.to_base64()) }
}

/// The binary form of [`Identity`]: the derived enum encoding, variant index then key bytes.
#[derive(Serialize, Deserialize)]
#[serde(rename = "Identity")]
enum IdentityBinary {
    Ed25519([u8; 32]),
}

/// The binary form of [`Signature`]. serde implements arrays only up to 32
/// elements, so the 64 bytes travel as two halves, which bincode writes as
/// exactly the 64 raw bytes after the variant index.
#[derive(Serialize, Deserialize)]
#[serde(rename = "Signature")]
enum SignatureBinary {
    Ed25519([u8; 32], [u8; 32]),
}

impl Serialize for Identity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            match *self {
                Self::Ed25519(key) => IdentityBinary::Ed25519(key).serialize(serializer),
            }
        }
    }
}

impl<'de> Deserialize<'de> for Identity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            Self::from_base64(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
        } else {
            match IdentityBinary::deserialize(deserializer)? {
                IdentityBinary::Ed25519(key) => Ok(Self::Ed25519(key)),
            }
        }
    }
}

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            match self {
                Self::Ed25519(bytes) => {
                    let (mut first, mut second) = ([0u8; 32], [0u8; 32]);
                    first.copy_from_slice(&bytes[..32]);
                    second.copy_from_slice(&bytes[32..]);
                    SignatureBinary::Ed25519(first, second).serialize(serializer)
                }
            }
        }
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            Self::from_base64(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
        } else {
            match SignatureBinary::deserialize(deserializer)? {
                SignatureBinary::Ed25519(first, second) => {
                    let mut bytes = [0u8; 64];
                    bytes[..32].copy_from_slice(&first);
                    bytes[32..].copy_from_slice(&second);
                    Ok(Self::Ed25519(bytes))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;

    fn identity_of(key: &ed25519_dalek::SigningKey) -> Identity { Identity::Ed25519(key.verifying_key().to_bytes()) }

    /// Every genesis hashes its minter's binary encoding, so the variant index
    /// and the byte layout are pinned here: a change fails this test rather
    /// than silently re-deriving every entity id.
    #[test]
    fn identity_encodings_are_pinned() {
        let identity = Identity::Ed25519([7; 32]);
        assert_eq!(bincode::serialize(&identity).unwrap(), [0u32.to_le_bytes().as_slice(), &[7; 32]].concat());
        assert_eq!(bincode::deserialize::<Identity>(&bincode::serialize(&identity).unwrap()).unwrap(), identity);
        assert_eq!(serde_json::to_string(&identity).unwrap(), format!("\"{}\"", identity.to_base64()));
        assert_eq!(serde_json::from_str::<Identity>(&serde_json::to_string(&identity).unwrap()).unwrap(), identity);
        assert_eq!(Identity::from_base64(identity.to_base64()).unwrap(), identity);
        assert!(matches!(Identity::from_base64("AQIDBAUGBwgJCgsMDQ4PEA"), Err(DecodeError::InvalidLength)));

        let node = NodeId::from(identity);
        assert_eq!(bincode::serialize(&node).unwrap(), bincode::serialize(&identity).unwrap(), "a node id encodes as its identity");
        assert_eq!(serde_json::to_string(&node).unwrap(), serde_json::to_string(&identity).unwrap());
    }

    #[test]
    fn signature_encodings_are_pinned() {
        let mut bytes = [0u8; 64];
        bytes.iter_mut().enumerate().for_each(|(index, byte)| *byte = index as u8);
        let signature = Signature::Ed25519(bytes);
        let encoded = bincode::serialize(&signature).unwrap();
        assert_eq!(encoded, [0u32.to_le_bytes().as_slice(), &bytes].concat());
        assert_eq!(bincode::deserialize::<Signature>(&encoded).unwrap(), signature);
        assert_eq!(serde_json::to_string(&signature).unwrap(), format!("\"{}\"", signature.to_base64()));
        assert_eq!(serde_json::from_str::<Signature>(&serde_json::to_string(&signature).unwrap()).unwrap(), signature);
    }

    #[test]
    fn verify_accepts_only_the_keys_own_signature_over_the_message() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[42; 32]);
        let identity = identity_of(&key);
        let signature = Signature::Ed25519(key.sign(b"message").to_bytes());
        assert!(identity.verify(b"message", &signature));
        assert!(!identity.verify(b"other message", &signature));
        assert!(!identity_of(&ed25519_dalek::SigningKey::from_bytes(&[43; 32])).verify(b"message", &signature));

        let Signature::Ed25519(mut tampered) = signature;
        tampered[0] ^= 1;
        assert!(!identity.verify(b"message", &Signature::Ed25519(tampered)));
    }

    #[test]
    fn verify_refuses_a_weak_or_non_canonical_key() {
        // The identity point: a weak key any signature could be forged against.
        let mut identity_point = [0u8; 32];
        identity_point[0] = 1;
        assert!(!Identity::Ed25519(identity_point).verify(b"message", &Signature::Ed25519([0; 64])));

        // 3 + p, a second encoding of the point whose y is 3. The library accepts it as a sound key.
        let mut non_canonical = [0xff; 32];
        non_canonical[0] = 0xf0;
        non_canonical[31] = 0x7f;
        let accepted = ed25519_dalek::VerifyingKey::from_bytes(&non_canonical).unwrap();
        assert!(!accepted.is_weak());
        assert_ne!(accepted.to_edwards().compress().to_bytes(), non_canonical);
        assert!(strict_ed25519_key(&non_canonical).is_none());
        assert!(!Identity::Ed25519(non_canonical).verify(b"message", &Signature::Ed25519([0; 64])));
    }
}
