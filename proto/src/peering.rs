use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    domain_tag::DomainTag,
    encoding,
    identity::{NodeId, Signature},
};

/// The newest wire protocol version this build speaks.
///
/// Versions number RELEASES, not development steps: one bump per published
/// release whose wire or persisted formats changed incompatibly, regardless
/// of how many changes that release accumulated.
///
/// History:
/// - absent: 0.9.x and earlier carried no version and are unsupported.
/// - 1: the initial unreleased 0.10 development wire.
pub const PROTOCOL_VERSION: u32 = 1;

/// The protocol versions one node speaks: every version from `lowest` to `highest`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersions {
    pub lowest: u32,
    pub highest: u32,
}

impl ProtocolVersions {
    /// The versions this build speaks.
    pub const SPOKEN: Self = Self { lowest: PROTOCOL_VERSION, highest: PROTOCOL_VERSION };

    /// The version two nodes offering `self` and `other` use: the highest both include, if any.
    pub fn highest_shared(self, other: Self) -> Option<u32> {
        let highest = self.highest.min(other.highest);
        (highest >= self.lowest.max(other.lowest)).then_some(highest)
    }
}

impl std::fmt::Display for ProtocolVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{} to {}", self.lowest, self.highest) }
}

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

/// The first frame each side of a connection sends: the versions its sender speaks, then the
/// challenge the receiver's presence must answer.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Opening {
    pub versions: ProtocolVersions,
    pub challenge: HandshakeChallenge,
}

/// The version negotiation of one connection as one side saw it: the versions it offered, the
/// versions it saw the peer offer, and the version it chose from the two.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct VersionNegotiation {
    pub offered: ProtocolVersions,
    pub peer_offered: ProtocolVersions,
    pub chosen: u32,
}

/// A node's signed claims about itself, answering one connection's challenge.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub claims: PresenceClaims,
    /// `claims.node_id`'s signature over [`DomainTag::Presence`] and the claims.
    pub signature: Signature,
}

/// One side of a connection, which a presence names as its signer's so that a presence signed for
/// one side never passes as the other's.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionRole {
    /// The side that opened the connection.
    Dialing,
    /// The side that accepted it.
    Accepting,
}

impl ConnectionRole {
    /// The other side of the same connection.
    pub fn opposite(self) -> Self {
        match self {
            Self::Dialing => Self::Accepting,
            Self::Accepting => Self::Dialing,
        }
    }
}

/// What a node claims in a presence. The presence's signature covers all of it.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PresenceClaims {
    pub node_id: NodeId,
    pub durable: bool,
    /// The signer's side of the connection.
    pub role: ConnectionRole,
    /// The receiver's challenge this presence answers.
    pub challenge: HandshakeChallenge,
    /// The challenge the signer's own opening carried on this connection.
    pub own_challenge: HandshakeChallenge,
    /// Unix ms when the presence was signed. Advisory: nothing checks it.
    pub timestamp: u64,
    /// The connection's version negotiation as the signer saw it.
    pub negotiation: VersionNegotiation,
    /// Numbers core assigns to abilities a later build adds, one for each the signer has. A node
    /// ignores a number it does not know; this build assigns none.
    pub features: Vec<u32>,
    /// Bytes the signer's plugins announce, each under its plugin's name. A node ignores a name it
    /// does not know.
    pub announcements: BTreeMap<String, Vec<u8>>,
}

impl PresenceClaims {
    /// These claims, signed by `sign`. The presence verifies only if `sign` signs as `node_id`.
    pub fn sign(self, sign: impl FnOnce(&[u8]) -> Signature) -> Presence {
        let signature = sign(&self.signed_bytes());
        Presence { claims: self, signature }
    }

    /// The bytes a presence signature covers: [`DomainTag::Presence`], then the encoded claims.
    fn signed_bytes(&self) -> Vec<u8> {
        let mut bytes = DomainTag::Presence.encoded().to_vec();
        encoding::encode_into(&mut bytes, self);
        bytes
    }
}

impl Presence {
    /// Whether `signature` is `claims.node_id`'s signature over the claims.
    pub fn signature_is_valid(&self) -> bool { self.claims.node_id.verify(&self.claims.signed_bytes(), &self.signature) }
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let claims = &self.claims;
        write!(f, "Presence[{}: durable {} proto v{}]", claims.node_id.to_base64_short(), claims.durable, claims.negotiation.chosen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{decode_exact, encode};

    use crate::{Identity, Message};
    use ed25519_dalek::Signer;

    fn key(byte: u8) -> ed25519_dalek::SigningKey { ed25519_dalek::SigningKey::from_bytes(&[byte; 32]) }

    fn node_id(key: &ed25519_dalek::SigningKey) -> NodeId { Identity::Ed25519(key.verifying_key().to_bytes()).into() }

    fn sign_as(key: &ed25519_dalek::SigningKey) -> impl FnOnce(&[u8]) -> Signature + '_ {
        |bytes| Signature::Ed25519(key.sign(bytes).to_bytes())
    }

    fn versions(lowest: u32, highest: u32) -> ProtocolVersions { ProtocolVersions { lowest, highest } }

    fn claims() -> PresenceClaims {
        PresenceClaims {
            node_id: node_id(&key(42)),
            durable: true,
            role: ConnectionRole::Dialing,
            challenge: HandshakeChallenge::new(node_id(&key(41))),
            own_challenge: HandshakeChallenge::new(node_id(&key(42))),
            timestamp: 1_720_000_000_000,
            negotiation: VersionNegotiation { offered: versions(1, 3), peer_offered: versions(1, 1), chosen: 1 },
            features: vec![7],
            announcements: BTreeMap::from([("plugin".to_owned(), vec![1, 2])]),
        }
    }

    #[test]
    fn presence_round_trip() {
        let p = claims().sign(sign_as(&key(42)));
        match Message::decode(&Message::Presence(p.clone()).encode()).unwrap() {
            Message::Presence(q) => assert_eq!(p, q),
            other => panic!("expected Presence, got {other}"),
        }
    }

    /// The presence vector of specs/protocol/bytes.md section 5: the bytes a presence signature covers.
    #[test]
    fn the_signed_bytes_match_the_byte_specification_vector() {
        let claims = PresenceClaims {
            node_id: Identity::Ed25519([0x99; 32]).into(),
            durable: true,
            role: ConnectionRole::Dialing,
            challenge: HandshakeChallenge { issuer: Identity::Ed25519([0x77; 32]).into(), nonce: [0x88; 32] },
            own_challenge: HandshakeChallenge { issuer: Identity::Ed25519([0x99; 32]).into(), nonce: [0xaa; 32] },
            timestamp: 1_720_000_000_000,
            negotiation: VersionNegotiation { offered: versions(1, 3), peer_offered: versions(1, 1), chosen: 1 },
            features: vec![7],
            announcements: BTreeMap::from([("p".to_owned(), vec![1, 2])]),
        };
        assert_eq!(
            hex::encode(claims.signed_bytes()),
            concat!(
                "13616e6b757261682e70726573656e63652e7631",
                "00000000",
                "9999999999999999999999999999999999999999999999999999999999999999",
                "01",
                "00000000",
                "00000000",
                "7777777777777777777777777777777777777777777777777777777777777777",
                "8888888888888888888888888888888888888888888888888888888888888888",
                "00000000",
                "9999999999999999999999999999999999999999999999999999999999999999",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0030fd7790010000",
                "01000000",
                "03000000",
                "01000000",
                "01000000",
                "01000000",
                "0100000000000000",
                "07000000",
                "0100000000000000",
                "0100000000000000",
                "70",
                "0200000000000000",
                "0102",
            )
        );
    }

    #[test]
    fn changing_any_claim_invalidates_the_signature() {
        let p = claims().sign(sign_as(&key(42)));
        assert!(p.signature_is_valid());

        let changes: [fn(&mut PresenceClaims); 12] = [
            |claims| claims.node_id = node_id(&key(43)),
            |claims| claims.durable = false,
            |claims| claims.role = claims.role.opposite(),
            |claims| claims.challenge = HandshakeChallenge::new(claims.challenge.issuer()),
            |claims| claims.own_challenge = HandshakeChallenge::new(claims.own_challenge.issuer()),
            |claims| claims.timestamp += 1,
            |claims| claims.negotiation.offered.highest = 1,
            |claims| claims.negotiation.peer_offered.highest = 3,
            |claims| claims.negotiation.chosen = 3,
            |claims| claims.features.push(8),
            |claims| claims.announcements.clear(),
            |claims| {
                claims.announcements.insert("plugin".to_owned(), vec![3]);
            },
        ];
        for (index, change) in changes.into_iter().enumerate() {
            let mut forged = p.clone();
            change(&mut forged.claims);
            assert!(!forged.signature_is_valid(), "change {index}");
        }
    }

    /// A receiver checks a presence over its own encoding of the claims it decoded, so claims
    /// signed with their announcements out of key order decode to the same claims and fail.
    #[test]
    fn claims_signed_with_their_announcements_out_of_order_fail_their_signature() {
        let mut claims = claims();
        claims.announcements = BTreeMap::from([("a".to_owned(), vec![1]), ("b".to_owned(), vec![2])]);
        let in_order = encode(&claims);
        // The announcements close the claims: each a one-byte name and a one-byte value, each with its length.
        let entry = (8 + 1) + (8 + 1);
        let at = in_order.len() - 2 * entry;
        let mut swapped = in_order.clone();
        swapped[at..].copy_from_slice(&[&in_order[at + entry..], &in_order[at..at + entry]].concat());

        let decoded: PresenceClaims = decode_exact(&swapped).unwrap();
        assert_eq!(decoded, claims);
        let signature = Signature::Ed25519(key(42).sign(&[DomainTag::Presence.encoded(), &swapped].concat()).to_bytes());
        assert!(!Presence { claims: decoded, signature }.signature_is_valid());
        assert!(claims.sign(sign_as(&key(42))).signature_is_valid(), "the same claims signed in order");
    }

    /// A presence's signed bytes hold its signer's role, so the index of each role is pinned.
    #[test]
    fn connection_role_variant_order_is_pinned() {
        assert_eq!(encode(&ConnectionRole::Dialing), [0, 0, 0, 0]);
        assert_eq!(encode(&ConnectionRole::Accepting), [1, 0, 0, 0]);
    }

    #[test]
    fn each_challenge_draws_a_fresh_nonce() {
        let issuer = node_id(&key(1));
        assert_ne!(HandshakeChallenge::new(issuer), HandshakeChallenge::new(issuer));
    }

    #[test]
    fn two_offers_settle_on_the_highest_version_both_include() {
        assert_eq!(versions(1, 1).highest_shared(versions(1, 3)), Some(1));
        assert_eq!(versions(1, 3).highest_shared(versions(1, 1)), Some(1));
        assert_eq!(versions(2, 5).highest_shared(versions(1, 4)), Some(4));
        assert_eq!(versions(1, 1).highest_shared(versions(2, 3)), None);
        assert_eq!(versions(2, 3).highest_shared(versions(1, 1)), None);
        assert_eq!(versions(1, 5).highest_shared(versions(4, 3)), None, "an offer whose lowest exceeds its highest includes nothing");
    }
}
