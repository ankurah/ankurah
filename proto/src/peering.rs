use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    domain_tag::DomainTag,
    encoding::decode_exact,
    identity::{NodeId, Signature},
};

/// The newest wire protocol version this build speaks.
///
/// Versions number RELEASES, not development steps: one bump per published
/// release whose wire format changed incompatibly, regardless of how many
/// changes that release accumulated. Each storage engine versions its stored
/// format on its own.
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
///
/// Sent as [`Message::Open`](crate::Message::Open), its first twelve bytes are the message's
/// variant index and then `versions`. Those bytes keep their place and meaning in every
/// protocol version, so any build can tell whether it shares a version with the sender before
/// it reads anything else.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Opening {
    pub versions: ProtocolVersions,
    pub challenge: HandshakeChallenge,
}

/// Why a connection's first frame opens no connection this build can take.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpeningError {
    /// The sender speaks only versions the reader does not; the rest of the frame was not read.
    #[error("the peer speaks protocol versions {0}, none of which this node speaks")]
    NoCommonVersion(ProtocolVersions),
    /// Neither an opening nor a refusal, or an opening whose challenge does not decode.
    #[error("not an opening frame this build can read: {0}")]
    Undecodable(String),
}

// The variant indices of `Message::Open` and `Message::NoCommonVersion`, which open their frames
// in every protocol version; message.rs pins them.
const OPEN_INDEX: u32 = 0;
const NO_COMMON_VERSION_INDEX: u32 = 1;

impl Opening {
    /// Read a connection's first frame the way every build reads it. Its first twelve bytes, the
    /// message's variant index and the versions its sender speaks, come first. The rest is read
    /// only when the sender shares a version with `spoken`, and must hold the challenge and
    /// nothing after it. A refusal in place of an opening names the versions its sender speaks.
    pub fn read(frame: &[u8], spoken: ProtocolVersions) -> Result<Self, OpeningError> {
        let word = |at: usize| frame.get(at..at + 4).and_then(|bytes| bytes.try_into().ok()).map(u32::from_le_bytes);
        let (Some(index), Some(lowest), Some(highest)) = (word(0), word(4), word(8)) else {
            return Err(OpeningError::Undecodable(format!("a frame of {} bytes", frame.len())));
        };
        let versions = ProtocolVersions { lowest, highest };
        match index {
            OPEN_INDEX if spoken.highest_shared(versions).is_some() => {
                let challenge = decode_exact(&frame[12..]).map_err(|error| OpeningError::Undecodable(error.to_string()))?;
                Ok(Self { versions, challenge })
            }
            OPEN_INDEX | NO_COMMON_VERSION_INDEX => Err(OpeningError::NoCommonVersion(versions)),
            other => Err(OpeningError::Undecodable(format!("a message of kind {other} where an opening belongs"))),
        }
    }
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

/// What a node claims in a presence. The presence's signature covers all of it.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PresenceClaims {
    pub node_id: NodeId,
    pub durable: bool,
    /// The receiver's challenge this presence answers.
    pub challenge: HandshakeChallenge,
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

    /// The bytes a presence signature covers: [`DomainTag::Presence`], then the bincode claims.
    fn signed_bytes(&self) -> Vec<u8> {
        let mut bytes = DomainTag::Presence.encoded();
        // Cannot fail: bincode encodes every field of the claims.
        bytes.extend(bincode::serialize(self).expect("presence claims serialize"));
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
            challenge: HandshakeChallenge::new(node_id(&key(41))),
            timestamp: 1_720_000_000_000,
            negotiation: VersionNegotiation { offered: versions(1, 3), peer_offered: versions(1, 1), chosen: 1 },
            features: vec![7],
            announcements: BTreeMap::from([("plugin".to_owned(), vec![1, 2])]),
        }
    }

    #[test]
    fn presence_round_trip() {
        let p = claims().sign(sign_as(&key(42)));
        let bytes = bincode::serialize(&Message::Presence(p.clone())).unwrap();
        match bincode::deserialize::<Message>(&bytes).unwrap() {
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
            challenge: HandshakeChallenge { issuer: Identity::Ed25519([0x77; 32]).into(), nonce: [0x88; 32] },
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
                "7777777777777777777777777777777777777777777777777777777777777777",
                "8888888888888888888888888888888888888888888888888888888888888888",
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

        let changes: [fn(&mut PresenceClaims); 10] = [
            |claims| claims.node_id = node_id(&key(43)),
            |claims| claims.durable = false,
            |claims| claims.challenge = HandshakeChallenge::new(claims.challenge.issuer()),
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

    fn opening(versions: ProtocolVersions) -> Opening {
        Opening { versions, challenge: HandshakeChallenge { issuer: Identity::Ed25519([0x77; 32]).into(), nonce: [0x88; 32] } }
    }

    /// The opening and refusal vectors of specs/protocol/bytes.md section 6: each frame's bytes.
    #[test]
    fn the_opening_and_refusal_frames_match_the_byte_specification_vectors() {
        let open = bincode::serialize(&Message::Open(opening(versions(1, 3)))).unwrap();
        assert_eq!(
            hex::encode(&open),
            concat!(
                "00000000",
                "01000000",
                "03000000",
                "00000000",
                "7777777777777777777777777777777777777777777777777777777777777777",
                "8888888888888888888888888888888888888888888888888888888888888888",
            )
        );
        assert_eq!(Opening::read(&open, ProtocolVersions::SPOKEN), Ok(opening(versions(1, 3))));

        let refusal = bincode::serialize(&Message::NoCommonVersion(versions(1, 1))).unwrap();
        assert_eq!(hex::encode(&refusal), "010000000100000001000000");
    }

    /// A version-1 reader rejects bytes after the challenge, so reaching the refusal below shows
    /// the rest of the frame was never read.
    #[test]
    fn an_opening_sharing_no_version_is_refused_before_the_rest_is_read() {
        let mut frame = bincode::serialize(&Message::Open(opening(versions(2, 3)))).unwrap();
        frame.extend_from_slice(b"bytes a version-1 reader rejects");
        assert_eq!(Opening::read(&frame, versions(1, 1)), Err(OpeningError::NoCommonVersion(versions(2, 3))));

        let mut shared = bincode::serialize(&Message::Open(opening(versions(1, 3)))).unwrap();
        shared.extend_from_slice(b"bytes a version-1 reader rejects");
        assert!(matches!(Opening::read(&shared, versions(1, 1)), Err(OpeningError::Undecodable(_))), "shared versions read the rest");
    }

    #[test]
    fn a_refusal_in_place_of_an_opening_names_the_versions_its_sender_speaks() {
        let refusal = bincode::serialize(&Message::NoCommonVersion(versions(4, 6))).unwrap();
        assert_eq!(Opening::read(&refusal, versions(1, 1)), Err(OpeningError::NoCommonVersion(versions(4, 6))));
    }

    #[test]
    fn a_frame_that_opens_with_neither_an_opening_nor_a_refusal_is_undecodable() {
        let open = bincode::serialize(&Message::Open(opening(versions(1, 1)))).unwrap();
        let mut other_kind = open.clone();
        other_kind[0] = 2;
        for frame in [&open[..11], &open[..40], &other_kind[..], &open[..0]] {
            assert!(matches!(Opening::read(frame, versions(1, 1)), Err(OpeningError::Undecodable(_))), "{frame:?}");
        }
    }
}
