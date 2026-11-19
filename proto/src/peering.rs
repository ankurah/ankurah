use serde::{Deserialize, Serialize};

use crate::{
    id::NodeId,
    key::{PublicKey, Signature},
};

/// The wire protocol version this binary speaks.
///
/// Carried in the [`Presence`] handshake and compared with
/// [`protocol_compatible`]; nodes refuse peers whose version is not
/// compatible. Bump this whenever any wire or persisted format changes
/// incompatibly (event or state encodings, request shapes, message
/// framing).
///
/// Versions number RELEASES, not development steps: one bump per published
/// release whose wire or persisted formats changed incompatibly, regardless
/// of how many changes that release accumulated.
///
/// History:
/// - absent: 0.9.x and earlier carried no version in Presence and are unsupported.
/// - 1: the initial unreleased 0.10 development wire.
pub const PROTOCOL_VERSION: u32 = 1;

/// Whether a peer advertising `remote` can interoperate with this binary.
///
/// Exact match for now: refuse on mismatch was the #294 decision, because
/// serving an older version would require maintaining dual codecs for every
/// changed message. Isolated here so a future version can widen acceptance
/// to a range without touching the handshake again.
pub fn protocol_compatible(remote: u32) -> bool { remote == PROTOCOL_VERSION }

/// One side of a connection. Including the signer's side in a presence signature prevents a node from accepting its own
/// presence, sent back on that connection, as the peer's presence.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionSide {
    /// The side that opened the connection.
    Dialing,
    /// The side that accepted it.
    Accepting,
}

impl ConnectionSide {
    pub fn opposite(self) -> Self {
        match self {
            Self::Dialing => Self::Accepting,
            Self::Accepting => Self::Dialing,
        }
    }
}

/// Random bytes for preventing reuse of a signed presence with a different challenge.
/// Each node sends a challenge that the peer must include when signing its presence.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Challenge([u8; 32]);

impl Challenge {
    pub fn random() -> Self {
        let mut bytes = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
        Self(bytes)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    /// The public key for verifying the presence signature and deriving the node id.
    pub key: PublicKey,
    pub durable: bool,
    /// See [`PROTOCOL_VERSION`].
    pub protocol_version: u32,
}

/// A prefix that distinguishes presence signatures from signatures for other purposes.
const PRESENCE_SIGNATURE_DOMAIN: &[u8] = b"ankurah presence";

impl Presence {
    pub fn node_id(&self) -> NodeId { self.key.node_id() }

    /// Signs the presence for the peer's `challenge` and this node's `side` of the connection.
    /// `sign` must use the private key corresponding to `self.key`.
    pub fn sign(self, side: ConnectionSide, challenge: &Challenge, sign: impl FnOnce(&[u8]) -> Signature) -> SignedPresence {
        let signature = sign(&self.signed_bytes(side, challenge));
        SignedPresence { presence: self, signature }
    }

    fn signed_bytes(&self, side: ConnectionSide, challenge: &Challenge) -> Vec<u8> {
        // Every field serializes without custom errors, and every collection supplies its length.
        // bincode writes to a Vec without a size limit, so this serialization cannot return an error.
        bincode::serialize(&(PRESENCE_SIGNATURE_DOMAIN, side, challenge, self)).expect("a presence always encodes")
    }
}

/// A presence with a signature for a challenge and connection side. The receiver must verify the signature before acceptance.
/// A valid signature proves use of the private key, but the sender may have forwarded the challenge to another node.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SignedPresence {
    pub presence: Presence,
    /// The signature over the signer's [`ConnectionSide`], the receiver's [`Challenge`] and the presence.
    pub signature: Signature,
}

impl SignedPresence {
    /// Checks whether the signature matches this presence, `side` and `challenge` under the presence's public key.
    pub fn answers(&self, side: ConnectionSide, challenge: &Challenge) -> bool {
        self.presence.key.verify(&self.presence.signed_bytes(side, challenge), &self.signature)
    }
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Presence[{}: durable {} proto v{}]", self.node_id().to_base64_short(), self.durable, self.protocol_version)
    }
}

/// Sent best-effort before closing when a peer's Presence advertises an
/// incompatible protocol version.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PresenceRejection {
    /// The protocol version the refusing node requires.
    pub expected: u32,
    /// The version the refused peer offered.
    pub received: u32,
}

impl std::fmt::Display for PresenceRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "incompatible protocol version (required {}, offered {})", self.expected, self.received)
    }
}

impl std::error::Error for PresenceRejection {}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;

    fn key(seed: u8) -> ed25519_dalek::SigningKey { ed25519_dalek::SigningKey::from_bytes(&[seed; 32]) }

    fn presence(key: &ed25519_dalek::SigningKey) -> Presence {
        Presence { key: PublicKey::Ed25519(key.verifying_key().to_bytes()), durable: true, protocol_version: PROTOCOL_VERSION }
    }

    fn sign_with(key: &ed25519_dalek::SigningKey) -> impl FnOnce(&[u8]) -> Signature + '_ {
        |bytes| {
            let signature = key.sign(bytes);
            Signature::Ed25519 { r: *signature.r_bytes(), s: *signature.s_bytes() }
        }
    }

    #[test]
    fn presence_round_trip() {
        let p = presence(&key(1)).sign(ConnectionSide::Dialing, &Challenge::random(), sign_with(&key(1)));
        let bytes = bincode::serialize(&crate::Message::Presence(p.clone())).unwrap();
        match bincode::deserialize::<crate::Message>(&bytes).unwrap() {
            crate::Message::Presence(q) => assert_eq!(p, q),
            other => panic!("expected Presence, got {other}"),
        }
    }

    #[test]
    fn a_signed_presence_answers_only_the_challenge_and_as_the_side_it_was_signed_for() {
        let challenge = Challenge::random();
        let signed = presence(&key(1)).sign(ConnectionSide::Dialing, &challenge, sign_with(&key(1)));
        assert!(signed.answers(ConnectionSide::Dialing, &challenge));
        assert!(!signed.answers(ConnectionSide::Dialing, &Challenge::random()));
        assert!(!signed.answers(ConnectionSide::Accepting, &challenge));
    }

    #[test]
    fn a_presence_signed_by_another_key_or_changed_after_signing_answers_nothing() {
        let challenge = Challenge::random();
        let side = ConnectionSide::Dialing;
        assert!(!presence(&key(1)).sign(side, &challenge, sign_with(&key(2))).answers(side, &challenge));

        let signed = presence(&key(1)).sign(side, &challenge, sign_with(&key(1)));
        let changes: [fn(&mut Presence); 3] = [
            |presence| presence.key = PublicKey::Ed25519(key(2).verifying_key().to_bytes()),
            |presence| presence.durable = false,
            |presence| presence.protocol_version += 1,
        ];
        for (index, change) in changes.into_iter().enumerate() {
            let mut changed = signed.clone();
            change(&mut changed.presence);
            assert!(!changed.answers(side, &challenge), "change {index}");
        }
    }

    #[test]
    fn compatibility_is_exact_match() {
        assert!(protocol_compatible(PROTOCOL_VERSION));
        assert!(!protocol_compatible(0));
        assert!(!protocol_compatible(PROTOCOL_VERSION + 1));
    }
}
