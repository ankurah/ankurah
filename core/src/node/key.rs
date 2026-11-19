use ankurah_proto as proto;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

/// A node's keypair for signing its presence. The public key determines the node id.
#[derive(Clone)]
pub struct NodeKey(ed25519_dalek::SigningKey);

/// The signature scheme prefix that prevents the parser from accepting keypairs from other schemes.
const ED25519: &str = "ed25519";

#[derive(Debug, thiserror::Error)]
#[error("this text is not a node key")]
pub struct InvalidNodeKey;

impl NodeKey {
    pub fn generate() -> Self {
        let mut private_key = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut private_key);
        Self(ed25519_dalek::SigningKey::from_bytes(&private_key))
    }

    /// The signature scheme and private key encode the keypair for storage between runs.
    pub fn to_secret_text(&self) -> String { format!("{ED25519}:{}", URL_SAFE_NO_PAD.encode(self.0.as_bytes())) }

    pub fn public_key(&self) -> proto::PublicKey { proto::PublicKey::Ed25519(self.0.verifying_key().to_bytes()) }

    pub(crate) fn sign(&self, message: &[u8]) -> proto::Signature {
        let signature = ed25519_dalek::Signer::sign(&self.0, message);
        proto::Signature::Ed25519 { r: *signature.r_bytes(), s: *signature.s_bytes() }
    }
}

impl std::str::FromStr for NodeKey {
    type Err = InvalidNodeKey;

    fn from_str(text: &str) -> Result<Self, InvalidNodeKey> {
        let private_key = text.strip_prefix(ED25519).and_then(|rest| rest.strip_prefix(':')).ok_or(InvalidNodeKey)?;
        let private_key: [u8; 32] =
            URL_SAFE_NO_PAD.decode(private_key).ok().and_then(|bytes| bytes.try_into().ok()).ok_or(InvalidNodeKey)?;
        Ok(Self(ed25519_dalek::SigningKey::from_bytes(&private_key)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_secret_text_gives_back_the_same_keypair() {
        let key = NodeKey::generate();
        let restored: NodeKey = key.to_secret_text().parse().unwrap();
        assert_eq!(restored.public_key(), key.public_key());
        assert!(key.public_key().verify(b"message", &restored.sign(b"message")));
        assert_ne!(NodeKey::generate().public_key(), key.public_key());
    }

    #[test]
    fn text_of_another_scheme_or_of_the_wrong_length_is_refused() {
        let text = NodeKey::generate().to_secret_text();
        let private_key = text.strip_prefix("ed25519:").unwrap();
        for refused in
            [private_key.to_owned(), format!("rsa:{private_key}"), format!("ed25519:{private_key}AA"), text[..text.len() - 2].to_owned()]
        {
            assert!(refused.parse::<NodeKey>().is_err(), "{refused}");
        }
    }
}
