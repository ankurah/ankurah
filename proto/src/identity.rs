use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

use crate::DecodeError;

/// A public verifying key, of the signature scheme its variant names: who minted a genesis, or
/// signed a presence or an attestation. specs/protocol/bytes.md section 3.1 gives its encoding.
#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
pub enum Identity {
    /// An ed25519 verifying key in its 32-byte compressed encoding.
    Ed25519([u8; 32]),
}

/// A signature by an [`Identity`], in the identity's scheme. Its text form
/// names the scheme as an identity's does.
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

/// The scheme an ed25519 key or signature names in its text form.
const ED25519: &str = "ed25519";

impl Identity {
    /// The first six characters of the key's base64, without the scheme, for compact diagnostics.
    pub fn to_base64_short(&self) -> String {
        match self {
            Self::Ed25519(key) => general_purpose::URL_SAFE_NO_PAD.encode(key)[..6].to_owned(),
        }
    }

    /// Whether `signature` is this identity's signature over `message`, by the rule of
    /// specs/protocol/bytes.md section 4.7.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        match (self, signature) {
            (Self::Ed25519(key), Signature::Ed25519(signature)) => {
                // Any crate in a build can enable ed25519-dalek's `legacy_compatibility`, which weakens the library's own check of S.
                ed25519_scalar_below_group_order(signature)
                    && strict_ed25519_key(key)
                        .is_some_and(|key| key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(signature)).is_ok())
            }
        }
    }
}

/// The order of the ed25519 group, L = 2^252 + 27742317777372353535851937790883648493, little-endian.
const ED25519_GROUP_ORDER: [u8; 32] = [
    0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
];

/// Whether an ed25519 signature's `S`, its last 32 bytes read as a little-endian integer, is below the group order.
fn ed25519_scalar_below_group_order(signature: &[u8; 64]) -> bool {
    signature[32..].iter().rev().cmp(ED25519_GROUP_ORDER.iter().rev()).is_lt()
}

/// The ed25519 key `bytes` encode, if they are its canonical encoding and it is not weak.
fn strict_ed25519_key(bytes: &[u8; 32]) -> Option<ed25519_dalek::VerifyingKey> {
    let key = ed25519_dalek::VerifyingKey::from_bytes(bytes).ok()?;
    // `from_bytes` accepts a non-canonical encoding and keeps the input bytes, so re-encode the point.
    (key.to_edwards().compress().to_bytes() == *bytes && !key.is_weak()).then_some(key)
}

/// The scheme and the base64 a text form names, split at the first `:`.
fn split_scheme(text: &str) -> Result<(&str, &str), DecodeError> { text.split_once(':').ok_or(DecodeError::MissingScheme) }

/// The `N` bytes whose unpadded URL-safe base64 `text` is.
fn decode_base64<const N: usize>(text: &str) -> Result<[u8; N], DecodeError> {
    let decoded = general_purpose::URL_SAFE_NO_PAD.decode(text).map_err(DecodeError::InvalidBase64)?;
    decoded.as_slice().try_into().map_err(|_| DecodeError::InvalidLength)
}

impl FromStr for Identity {
    type Err = DecodeError;

    /// Parse the text form: a scheme this build knows, `:`, then the base64 of the key.
    fn from_str(text: &str) -> Result<Self, DecodeError> {
        match split_scheme(text)? {
            (ED25519, key) => Ok(Self::Ed25519(decode_base64(key)?)),
            (scheme, _) => Err(DecodeError::UnknownScheme(scheme.to_owned())),
        }
    }
}

impl FromStr for Signature {
    type Err = DecodeError;

    /// Parse the text form: a scheme this build knows, `:`, then the base64 of the signature.
    fn from_str(text: &str) -> Result<Self, DecodeError> {
        match split_scheme(text)? {
            (ED25519, signature) => Ok(Self::Ed25519(decode_base64(signature)?)),
            (scheme, _) => Err(DecodeError::UnknownScheme(scheme.to_owned())),
        }
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

/// The text form; the alternate form `{:#}` is the short diagnostic one, without the scheme.
impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            return f.write_str(&self.to_base64_short());
        }
        match self {
            Self::Ed25519(key) => write!(f, "{ED25519}:{}", general_purpose::URL_SAFE_NO_PAD.encode(key)),
        }
    }
}

/// The full text form, also under `{:#?}`, whose alternate flag would otherwise select the short form.
impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.to_string()) }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Display::fmt(&self.0, f) }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Debug::fmt(&self.0, f) }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ed25519(signature) => write!(f, "{ED25519}:{}", general_purpose::URL_SAFE_NO_PAD.encode(signature)),
        }
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Display::fmt(self, f) }
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
            serializer.collect_str(self)
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
            String::deserialize(deserializer)?.parse().map_err(serde::de::Error::custom)
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
            serializer.collect_str(self)
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
            String::deserialize(deserializer)?.parse().map_err(serde::de::Error::custom)
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
    use crate::encoding::{decode_exact, encode};
    use ed25519_dalek::Signer;

    fn identity_of(key: &ed25519_dalek::SigningKey) -> Identity { Identity::Ed25519(key.verifying_key().to_bytes()) }

    /// Every genesis hashes its minter's binary encoding, so the variant index
    /// and the byte layout are pinned here: a change fails this test rather
    /// than silently re-deriving every entity id.
    #[test]
    fn identity_encodings_are_pinned() {
        let identity = Identity::Ed25519([7; 32]);
        assert_eq!(encode(&identity), [0u32.to_le_bytes().as_slice(), &[7; 32]].concat());
        assert_eq!(decode_exact::<Identity>(&encode(&identity)).unwrap(), identity);

        let node = NodeId::from(identity);
        assert_eq!(encode(&node), [0u32.to_le_bytes().as_slice(), &[7; 32]].concat(), "a node id encodes as its identity");
    }

    /// The text form names the scheme, so a key of a later scheme can never be read as an ed25519 key.
    #[test]
    fn an_identity_prints_and_parses_with_its_scheme_named() {
        let identity = Identity::Ed25519([7; 32]);
        let text = "ed25519:BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";
        assert_eq!(identity.to_string(), text);
        assert_eq!(text.parse::<Identity>().unwrap(), identity);
        assert_eq!(serde_json::to_string(&identity).unwrap(), format!("\"{text}\""));
        assert_eq!(serde_json::from_str::<Identity>(&format!("\"{text}\"")).unwrap(), identity);
        assert_eq!(format!("{identity:#}"), "BwcHBw", "the short diagnostic form names no scheme");
        assert_eq!(format!("{identity:#?}"), text, "pretty debug output keeps the full form");

        let node = NodeId::from(identity);
        assert_eq!(node.to_string(), text, "a node id prints as its identity");
        assert_eq!(serde_json::to_string(&node).unwrap(), format!("\"{text}\""));
    }

    #[test]
    fn identity_text_without_a_known_scheme_is_refused() {
        assert!(matches!("BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".parse::<Identity>(), Err(DecodeError::MissingScheme)));
        assert!(
            matches!("rsa:BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".parse::<Identity>(), Err(DecodeError::UnknownScheme(scheme)) if scheme == "rsa")
        );
        assert!(matches!("ed25519:AQIDBAUGBwgJCgsMDQ4PEA".parse::<Identity>(), Err(DecodeError::InvalidLength)));
        assert!(serde_json::from_str::<Identity>("\"BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc\"").is_err());
    }

    #[test]
    fn signature_encodings_are_pinned() {
        let mut bytes = [0u8; 64];
        bytes.iter_mut().enumerate().for_each(|(index, byte)| *byte = index as u8);
        let signature = Signature::Ed25519(bytes);
        let encoded = encode(&signature);
        assert_eq!(encoded, [0u32.to_le_bytes().as_slice(), &bytes].concat());
        assert_eq!(decode_exact::<Signature>(&encoded).unwrap(), signature);

        let text = "ed25519:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0-Pw";
        assert_eq!(signature.to_string(), text);
        assert_eq!(text.parse::<Signature>().unwrap(), signature);
        assert_eq!(serde_json::to_string(&signature).unwrap(), format!("\"{text}\""));
        assert_eq!(serde_json::from_str::<Signature>(&format!("\"{text}\"")).unwrap(), signature);
        assert!(matches!(text["ed25519:".len()..].parse::<Signature>(), Err(DecodeError::MissingScheme)));
        assert!(matches!(text.replacen("ed25519", "rsa", 1).parse::<Signature>(), Err(DecodeError::UnknownScheme(_))));
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

    /// The ed25519 vectors of specs/protocol/bytes.md section 5, over the message `message`: one
    /// signature the rule of section 4.7 accepts, then one for each rule that refuses a signature
    /// on its own.
    #[test]
    fn verify_follows_the_rule_and_the_vectors_of_the_byte_specification() {
        let base_point = "5866666666666666666666666666666666666666666666666666666666666666";
        for (key, signature, verifies, case) in [
            (
                base_point,
                "5866666666666666666666666666666666666666666666666666666666666666f4a5bddca212b9849c7b53f96034edaf51addd8a2dc7cd06e20c117e5546b204",
                true,
                "R = B and S = 1 + k",
            ),
            (
                "0100000000000000000000000000000000000000000000000000000000000000",
                "58666666666666666666666666666666666666666666666666666666666666660100000000000000000000000000000000000000000000000000000000000000",
                false,
                "a key of small order",
            ),
            (
                base_point,
                "0100000000000000000000000000000000000000000000000000000000000000ce3838e9daae0d280fb00ea0aaeb78b58b6deadcfff78187974fedb20ac4d80f",
                false,
                "an R of small order",
            ),
            (
                base_point,
                "5866666666666666666666666666666666666666666666666666666666666666e179b339bd75cbdc72184b9c3f2eccc451addd8a2dc7cd06e20c117e5546b214",
                false,
                "an S not below the group order",
            ),
            (
                base_point,
                "98519eadf35b995233b51b5cd23e9cc5a28b639b5a4af0ec903cb960d81b78192827bea5c0e0ad7153f2599106042da3a250ea9c45bacdca959d700a7d53f30b",
                false,
                "a signature valid only under the cofactored equation",
            ),
        ] {
            let key = Identity::Ed25519(hex::decode(key).unwrap().try_into().unwrap());
            let signature = Signature::Ed25519(hex::decode(signature).unwrap().try_into().unwrap());
            assert_eq!(key.verify(b"message", &signature), verifies, "{case}");
        }
    }

    /// ed25519-dalek's `VerifyingKey::from_bytes` takes both keys below; this build refuses each before it verifies.
    #[test]
    fn a_key_of_small_order_or_not_in_its_points_own_encoding_is_refused() {
        // The identity point, under which the signature R = B, S = 1 holds the equation for any message.
        let mut identity_point = [0u8; 32];
        identity_point[0] = 1;
        assert!(ed25519_dalek::VerifyingKey::from_bytes(&identity_point).is_ok());
        assert!(strict_ed25519_key(&identity_point).is_none());

        // 3 + p, a second encoding of the point whose y is 3.
        let mut non_canonical = [0xff; 32];
        non_canonical[0] = 0xf0;
        non_canonical[31] = 0x7f;
        let accepted = ed25519_dalek::VerifyingKey::from_bytes(&non_canonical).unwrap();
        assert!(!accepted.is_weak());
        assert_ne!(accepted.to_edwards().compress().to_bytes(), non_canonical);
        assert!(strict_ed25519_key(&non_canonical).is_none());
    }

    /// ed25519-dalek's `legacy_compatibility` feature lets its own check pass an `S` of the group
    /// order or more, so this build refuses one before calling the library.
    #[test]
    fn a_signature_whose_scalar_is_not_below_the_group_order_is_refused_by_this_builds_own_check() {
        let point = "5866666666666666666666666666666666666666666666666666666666666666";
        let with_scalar = |scalar: &str| -> [u8; 64] { hex::decode(format!("{point}{scalar}")).unwrap().try_into().unwrap() };
        for (scalar, below, case) in [
            ("e179b339bd75cbdc72184b9c3f2eccc451addd8a2dc7cd06e20c117e5546b214", false, "the vectors' S plus L"),
            ("edd3f55c1a631258d69cf7a2def9de1400000000000000000000000000000010", false, "L"),
            ("ecd3f55c1a631258d69cf7a2def9de1400000000000000000000000000000010", true, "L minus 1"),
            ("f4a5bddca212b9849c7b53f96034edaf51addd8a2dc7cd06e20c117e5546b204", true, "the vectors' accepted S"),
        ] {
            assert_eq!(ed25519_scalar_below_group_order(&with_scalar(scalar)), below, "{case}");
        }
    }
}
