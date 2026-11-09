use serde::{Deserialize, Serialize};

use crate::{
    domain_tag::DomainTag,
    identity::{NodeId, Signature},
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

/// A fresh nonce a node sends the peer on a new connection. The peer's presence
/// must answer it, so a presence signed for one connection does not
/// authenticate another.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct HandshakeChallenge {
    issuer: NodeId,
    nonce: [u8; 32],
}

impl HandshakeChallenge {
    /// A challenge from `issuer` with a freshly drawn nonce.
    pub fn new(issuer: NodeId) -> Self {
        let mut nonce = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut nonce);
        Self { issuer, nonce }
    }

    /// The node that issued this challenge and expects the answer.
    pub fn issuer(&self) -> NodeId { self.issuer }
}

/// A node's signed claims about itself, answering one connection's challenge.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: NodeId,
    pub durable: bool,
    /// The challenge this presence answers.
    pub challenge: HandshakeChallenge,
    /// Unix ms when the presence was signed. Advisory: nothing checks it.
    pub timestamp: u64,
    /// `node_id`'s signature over [`PresenceClaims::signable_bytes`].
    pub signature: Signature,
    /// See [`PROTOCOL_VERSION`].
    pub protocol_version: u32,
}

/// The part of a [`Presence`] its signature covers.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PresenceClaims {
    pub node_id: NodeId,
    pub durable: bool,
    pub challenge: HandshakeChallenge,
    pub timestamp: u64,
    pub protocol_version: u32,
}

impl PresenceClaims {
    /// The bytes a presence signature covers: [`DomainTag::Presence`], then the bincode claims.
    pub fn signable_bytes(&self) -> Vec<u8> {
        let mut bytes = DomainTag::Presence.encoded();
        // Cannot fail: every field is a fixed-shape value bincode can encode.
        bytes.extend(bincode::serialize(self).expect("presence claims serialize"));
        bytes
    }
}

impl Presence {
    /// The claims this presence's signature covers.
    pub fn claims(&self) -> PresenceClaims {
        PresenceClaims {
            node_id: self.node_id,
            durable: self.durable,
            challenge: self.challenge,
            timestamp: self.timestamp,
            protocol_version: self.protocol_version,
        }
    }

    /// Whether `signature` is `node_id`'s signature over this presence's claims.
    pub fn signature_is_valid(&self) -> bool { self.node_id.verify(&self.claims().signable_bytes(), &self.signature) }
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Presence[{}: durable {} proto v{}]", self.node_id.to_base64_short(), self.durable, self.protocol_version)
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
    use crate::Identity;
    use ed25519_dalek::Signer;

    fn key(byte: u8) -> ed25519_dalek::SigningKey { ed25519_dalek::SigningKey::from_bytes(&[byte; 32]) }

    fn node_id(key: &ed25519_dalek::SigningKey) -> NodeId { Identity::Ed25519(key.verifying_key().to_bytes()).into() }

    /// `presence`, signed by `signer` over its claims.
    fn signed(mut presence: Presence, signer: &ed25519_dalek::SigningKey) -> Presence {
        presence.signature = Signature::Ed25519(signer.sign(&presence.claims().signable_bytes()).to_bytes());
        presence
    }

    fn presence() -> Presence {
        let unsigned = Presence {
            node_id: node_id(&key(42)),
            durable: true,
            challenge: HandshakeChallenge::new(node_id(&key(41))),
            timestamp: 1_720_000_000_000,
            signature: Signature::Ed25519([0; 64]),
            protocol_version: PROTOCOL_VERSION,
        };
        signed(unsigned, &key(42))
    }

    #[test]
    fn presence_round_trip() {
        let p = presence();
        let bytes = bincode::serialize(&crate::Message::Presence(p.clone())).unwrap();
        match bincode::deserialize::<crate::Message>(&bytes).unwrap() {
            crate::Message::Presence(q) => assert_eq!(p, q),
            other => panic!("expected Presence, got {other}"),
        }
    }

    /// The presence vector of specs/protocol/bytes.md section 5: the bytes a presence signature covers.
    #[test]
    fn the_signed_bytes_match_the_byte_specification_vector() {
        let claims = PresenceClaims {
            node_id: Identity::Ed25519([0x99; 32]).into(),
            durable: true,
            challenge: HandshakeChallenge { issuer: Identity::Ed25519([0x77; 32]).into(), nonce: [0x88; 32] },
            timestamp: 1_720_000_000_000,
            protocol_version: 1,
        };
        assert_eq!(
            hex::encode(claims.signable_bytes()),
            "13616e6b757261682e70726573656e63652e76310000000099999999999999999999999999999999999999999999999999999999999999990100000000777777777777777777777777777777777777777777777777777777777777777788888888888888888888888888888888888888888888888888888888888888880030fd779001000001000000"
        );
    }

    #[test]
    fn changing_any_claim_invalidates_the_signature() {
        let p = presence();
        assert!(p.signature_is_valid());

        let mut forged = p.clone();
        forged.durable = false;
        assert!(!forged.signature_is_valid());

        let mut forged = p.clone();
        forged.timestamp += 1;
        assert!(!forged.signature_is_valid());

        let mut forged = p.clone();
        forged.protocol_version += 1;
        assert!(!forged.signature_is_valid());

        let mut forged = p.clone();
        forged.challenge = HandshakeChallenge::new(p.challenge.issuer());
        assert!(!forged.signature_is_valid());

        let mut forged = p.clone();
        forged.node_id = node_id(&key(43));
        assert!(!forged.signature_is_valid());
    }

    #[test]
    fn each_challenge_draws_a_fresh_nonce() {
        let issuer = node_id(&key(1));
        assert_ne!(HandshakeChallenge::new(issuer), HandshakeChallenge::new(issuer));
    }

    #[test]
    fn compatibility_is_exact_match() {
        assert!(protocol_compatible(PROTOCOL_VERSION));
        assert!(!protocol_compatible(0));
        assert!(!protocol_compatible(PROTOCOL_VERSION + 1));
    }
}
