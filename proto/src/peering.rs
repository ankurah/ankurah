use serde::{Deserialize, Serialize};

use crate::{id::EntityId, node_id::NodeId, node_id::Signature, Attested, EntityState, Event};

/// The wire protocol version this binary speaks.
///
/// Carried in the [`Presence`] handshake and compared with
/// [`protocol_compatible`]; nodes refuse peers whose version is not
/// compatible. Bump this whenever any wire or persisted format changes
/// incompatibly (event or state encodings, request shapes, message
/// framing).
///
/// History:
/// - absent: 0.9.x and earlier carried no version in Presence. Such peers
///   are classified as version 0 (see [`is_version0_presence`]) and refused.
/// - 1: the 0.9 wire shapes plus the versioned Presence handshake (#294).
/// - 2: the Phase A id-keyed epoch: LWW diff v2 / state 0xA2, resolved
///   Identifier selections, RegisterSchema.
/// - 3: the model-id wire envelope (#330): Event/EntityState/EntityDelta/
///   SubscriptionUpdateItem carry the model-definition entity id instead of a
///   collection name, and NodeUpdate/NodeResponse carry once-per-connection
///   catalog schema defs.
/// - 4: the identity/attestation substrate (specs/identity-attestation/
///   spec.md): 32-byte content-hash EntityIds, the EventBody genesis/update
///   split with domain-tagged ids, ed25519 NodeIds with signed Presence, and
///   the structured attestation envelope.
/// - 5: Presence carries the system-root genesis proof in addition to its
///   materialized state, so first join can verify the self-certifying root
///   before granting durable routing.
/// - 6: the handshake is challenge-bound (replayed Presence frames no longer
///   authenticate a fresh connection), and shipped schema states carry their
///   self-certifying genesis proof.
pub const PROTOCOL_VERSION: u32 = 6;

/// Domain tag for presence signatures (RFC specs/identity-attestation/
/// spec.md I.2): the signature covers `PRESENCE_TAG ||
/// bincode(PresenceClaims)`.
pub const PRESENCE_TAG: &[u8] = b"ankurah.presence.v1";

/// A fresh, receiver-generated nonce that binds one signed Presence to one
/// connection attempt. Both sides send a challenge before accepting the
/// other's Presence, so possession of an old signed frame is insufficient to
/// impersonate its node on a new transport connection.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HandshakeChallenge {
    /// The node that generated the challenge and must receive the answer.
    issuer: NodeId,
    nonce: [u8; 32],
}

impl HandshakeChallenge {
    pub fn new(issuer: NodeId, nonce: [u8; 32]) -> Self { Self { issuer, nonce } }

    pub fn issuer(self) -> NodeId { self.issuer }

    pub fn nonce(self) -> [u8; 32] { self.nonce }
}

/// Whether a peer advertising `remote` can interoperate with this binary.
///
/// Exact match for now: refuse on mismatch was the #294 decision, because
/// serving an older version would require maintaining dual codecs for every
/// changed message. Isolated here so a future version can widen acceptance
/// to a range without touching the handshake again.
pub fn protocol_compatible(remote: u32) -> bool { remote == PROTOCOL_VERSION }

/// A signed presence claim (RFC specs/identity-attestation/spec.md I.2).
/// `register_peer` verifies the signature before inserting the peer, making
/// node identity unforgeable at the handshake; the `durable` flag is
/// separately verified against the system-root founder record.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: NodeId,
    pub durable: bool,
    pub system_root: Option<SystemRootProof>,
    /// The receiver-generated challenge this Presence answers.
    pub challenge: HandshakeChallenge,
    /// Unix ms, freshness signal (advisory).
    pub timestamp: u64,
    /// By `node_id`'s key, over [`PRESENCE_TAG`] `||`
    /// `bincode(`[`PresenceClaims`]`)`.
    pub signature: Signature,
    /// See [`PROTOCOL_VERSION`]. Kept as the LAST field (the #294
    /// convention) so any future field additions keep the version readable
    /// by the same suffix position. The pre-#294 prefix property itself did
    /// not survive protocol 4's id-width change; version-0 peers are still
    /// classified by [`is_version0_presence`], which pins the OLD widths.
    pub protocol_version: u32,
}

/// The complete RFC-1 proof for a system root.
///
/// `genesis` is content-addressed and therefore proves which root is named;
/// `state` must be the exact materialization of that genesis. Core validates
/// both before reserving a first-join root. The Presence signature binds this
/// proof through [`Self::entity_id`], which is included in
/// [`PresenceClaims`]. RFC-1 keeps the root immutable, so no later root-state
/// lineage is needed here.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SystemRootProof {
    pub genesis: Event,
    pub state: Attested<EntityState>,
}

impl SystemRootProof {
    pub fn entity_id(&self) -> EntityId { self.genesis.entity_id }
}

/// The projection of [`Presence`] the signature covers: everything a peer
/// asserts about itself (identity, class, which system it roots, freshness).
/// The full root proof stays outside the signature; its content-addressed
/// genesis id binds the proof bytes after core verifies that the carried state
/// is the exact genesis materialization.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PresenceClaims {
    pub node_id: NodeId,
    pub durable: bool,
    pub system_root: Option<EntityId>,
    pub challenge: HandshakeChallenge,
    pub timestamp: u64,
    pub protocol_version: u32,
}

impl Presence {
    /// The claims projection this presence asserts.
    pub fn claims(&self) -> PresenceClaims {
        PresenceClaims {
            node_id: self.node_id,
            durable: self.durable,
            system_root: self.system_root.as_ref().map(SystemRootProof::entity_id),
            challenge: self.challenge,
            timestamp: self.timestamp,
            protocol_version: self.protocol_version,
        }
    }

    /// The exact bytes a presence signature covers.
    pub fn signable_bytes(claims: &PresenceClaims) -> Vec<u8> {
        let mut bytes = PRESENCE_TAG.to_vec();
        bytes.extend(bincode::serialize(claims).expect("presence claims serialize"));
        bytes
    }

    /// Whether the signature is valid under `node_id`'s key for this
    /// presence's claims.
    pub fn verify_signature(&self) -> bool { self.node_id.verify(&Self::signable_bytes(&self.claims()), &self.signature) }

    /// Verify both proof of key possession and binding to the challenge issued
    /// for this connection.
    pub fn verify_for(&self, expected: HandshakeChallenge) -> bool { self.challenge == expected && self.verify_signature() }
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.system_root {
            Some(r) => {
                write!(
                    f,
                    "Presence[{}: durable {} proto v{} system_root: {}]",
                    self.node_id.to_base64_short(),
                    self.durable,
                    self.protocol_version,
                    r.state.payload
                )
            }
            None => {
                write!(f, "Presence[{}: durable {} proto v{}]", self.node_id.to_base64_short(), self.durable, self.protocol_version)
            }
        }
    }
}

/// Sent best-effort before closing when a peer's Presence advertises an
/// incompatible protocol version. Pre-versioning (0.9.x) peers cannot
/// decode this message; they only observe the close.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PresenceRejection {
    /// The protocol version the refusing node requires.
    pub expected: u32,
    /// The version the refused peer offered (0 = pre-versioning peer).
    pub received: u32,
}

impl std::fmt::Display for PresenceRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "incompatible protocol version (required {}, offered {})", self.expected, self.received)
    }
}

impl std::error::Error for PresenceRejection {}

/// Why `register_peer` refused a Presence. Connectors relay the version
/// rejection best-effort (older peers understand the close, newer ones the
/// message) and simply close on an invalid signature: there is no
/// authenticated channel to explain anything over.
#[derive(Debug, thiserror::Error)]
pub enum PresenceRefusal {
    #[error("{0}")]
    IncompatibleVersion(#[from] PresenceRejection),
    #[error("presence signature invalid for node {0}")]
    InvalidSignature(NodeId),
    #[error("presence from node {0} answered a different connection challenge")]
    UnexpectedChallenge(NodeId),
    #[error("node {0} cannot register a connection to itself")]
    SelfConnection(NodeId),
    #[error("system-root proof invalid for node {0}")]
    InvalidSystemRoot(NodeId),
}

/// The Presence shape that pre-#294 binaries (0.9.x and earlier) send,
/// pinned at the OLD field widths (16-byte ULID node ids), since the live
/// types have moved on. Used only to classify an undecodable handshake,
/// never constructed. The root payload is probed by its Option tag alone:
/// mirroring the whole 0.9 EntityState shape buys nothing for a
/// classification whose output is a log line.
#[derive(Deserialize)]
#[allow(dead_code)]
struct LegacyPresenceProbe {
    node_id: [u8; 16],
    durable: bool,
    system_root: Option<()>,
}

/// True if `data` (an entire [`crate::Message`] frame that failed normal
/// decoding) parses as a pre-versioning (version 0) Presence, so the
/// refusal can name the real problem instead of a generic decode error.
///
/// Only meaningful after normal decode fails: a current-version Presence
/// decodes normally and never reaches this classifier.
pub fn is_version0_presence(data: &[u8]) -> bool {
    // Legacy Message is a bincode enum whose Presence was u32 little-endian
    // variant 0. V6 preserves that discriminant and appends its challenge at
    // variant 3, so an old Presence is never reinterpreted as a challenge.
    if data.len() < 4 || data[..4] != [0, 0, 0, 0] {
        return false;
    }
    crate::message::decode_exact::<LegacyPresenceProbe>(&data[4..]).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;

    fn presence() -> Presence {
        let key = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        let node_id = NodeId::from(key.verifying_key());
        let receiver = NodeId::from(ed25519_dalek::SigningKey::from_bytes(&[41u8; 32]).verifying_key());
        let challenge = HandshakeChallenge::new(receiver, [0xA5; 32]);
        let claims = PresenceClaims {
            node_id,
            durable: true,
            system_root: None,
            challenge,
            timestamp: 1_720_000_000_000,
            protocol_version: PROTOCOL_VERSION,
        };
        let signature: Signature = key.sign(&Presence::signable_bytes(&claims)).into();
        Presence {
            node_id,
            durable: true,
            system_root: None,
            challenge,
            timestamp: claims.timestamp,
            signature,
            protocol_version: PROTOCOL_VERSION,
        }
    }

    /// The 0.9.x wire shapes, mirrored at their ORIGINAL widths for the
    /// version-0 classification tests (node ids were 16-byte ULIDs).
    #[derive(Serialize)]
    struct LegacyPresenceOwned {
        node_id: [u8; 16],
        durable: bool,
        system_root: Option<()>,
    }
    #[derive(Serialize)]
    enum LegacyMessage {
        Presence(LegacyPresenceOwned),
        #[allow(dead_code)]
        PeerMessage(()),
    }

    #[test]
    fn presence_round_trip() {
        let p = presence();
        let bytes = crate::encode_message(&crate::Message::Presence(p.clone())).unwrap();
        match crate::decode_message(&bytes).unwrap() {
            crate::Message::Presence(q) => assert_eq!(p, q),
            other => panic!("expected Presence, got {other}"),
        }
    }

    #[test]
    fn signature_verifies_and_tamper_fails() {
        let p = presence();
        assert!(p.verify_signature());
        assert!(p.verify_for(p.challenge));

        // A genuine Presence from another connection is not valid for this
        // receiver-generated challenge, even though its signature is intact.
        let another_challenge = HandshakeChallenge::new(p.challenge.issuer(), [0xA6; 32]);
        assert!(p.verify_signature());
        assert!(!p.verify_for(another_challenge));

        // Flipping any signed claim invalidates the signature.
        let mut forged = p.clone();
        forged.durable = false;
        assert!(!forged.verify_signature());

        let mut forged = p.clone();
        forged.timestamp += 1;
        assert!(!forged.verify_signature());

        let mut forged = p.clone();
        forged.protocol_version += 1;
        assert!(!forged.verify_signature());

        let mut forged = p.clone();
        forged.challenge = another_challenge;
        assert!(!forged.verify_signature());

        // A different node id cannot claim this signature.
        let other = ed25519_dalek::SigningKey::from_bytes(&[43u8; 32]);
        let mut forged = p.clone();
        forged.node_id = NodeId::from(other.verifying_key());
        assert!(!forged.verify_signature());
    }

    #[test]
    fn classifies_version0_presence() {
        let old_bytes =
            bincode::serialize(&LegacyMessage::Presence(LegacyPresenceOwned { node_id: [7u8; 16], durable: false, system_root: None }))
                .unwrap();
        // An old presence fails current decoding and classifies as version 0.
        assert!(crate::decode_message(&old_bytes).is_err());
        assert!(is_version0_presence(&old_bytes));
        // Garbage and non-Presence variants do not.
        assert!(!is_version0_presence(&[]));
        assert!(!is_version0_presence(&[7, 7, 7, 7, 7]));
        assert!(!is_version0_presence(&[1, 0, 0, 0, 0, 0]));
    }

    #[test]
    fn compatibility_is_exact_match() {
        assert!(protocol_compatible(PROTOCOL_VERSION));
        assert!(!protocol_compatible(0));
        assert!(!protocol_compatible(PROTOCOL_VERSION + 1));
    }
}
