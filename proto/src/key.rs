use serde::{Deserialize, Serialize};

use crate::id::NodeId;

/// A node's public key and the signature scheme for verifying its presence.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum PublicKey {
    Ed25519([u8; 32]),
}

/// A signature made with the private key of a [`PublicKey`], tagged with the same scheme.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum Signature {
    /// The two halves of an ed25519 signature, in the order the scheme writes them.
    Ed25519 { r: [u8; 32], s: [u8; 32] },
}

impl PublicKey {
    /// The node id derived from this public key. An ed25519 public key's bytes are its node id.
    pub fn node_id(&self) -> NodeId {
        match self {
            Self::Ed25519(key) => NodeId::from_bytes(*key),
        }
    }

    /// Checks whether `signature` matches `message` and this public key.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        match (self, signature) {
            (Self::Ed25519(key), Signature::Ed25519 { r, s }) => ed25519_dalek::VerifyingKey::from_bytes(key)
                .is_ok_and(|key| key.verify_strict(message, &ed25519_dalek::Signature::from_components(*r, *s)).is_ok()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;

    fn public_key(key: &ed25519_dalek::SigningKey) -> PublicKey { PublicKey::Ed25519(key.verifying_key().to_bytes()) }

    fn sign(key: &ed25519_dalek::SigningKey, message: &[u8]) -> Signature {
        let signature = key.sign(message);
        Signature::Ed25519 { r: *signature.r_bytes(), s: *signature.s_bytes() }
    }

    #[test]
    fn verify_accepts_only_the_keys_own_signature_over_the_message() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[42; 32]);
        let signature = sign(&key, b"message");
        assert!(public_key(&key).verify(b"message", &signature));
        assert!(!public_key(&key).verify(b"other message", &signature));
        assert!(!public_key(&ed25519_dalek::SigningKey::from_bytes(&[43; 32])).verify(b"message", &signature));

        let Signature::Ed25519 { mut r, s } = signature;
        r[0] ^= 1;
        assert!(!public_key(&key).verify(b"message", &Signature::Ed25519 { r, s }));
    }

    #[test]
    fn bytes_that_encode_no_ed25519_key_verify_nothing() {
        // No point on the curve has this y coordinate.
        let key = PublicKey::Ed25519([2; 32]);
        assert!(ed25519_dalek::VerifyingKey::from_bytes(&[2; 32]).is_err());
        assert!(!key.verify(b"message", &Signature::Ed25519 { r: [0; 32], s: [0; 32] }));
    }
}
