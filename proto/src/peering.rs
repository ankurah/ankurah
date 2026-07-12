use serde::{Deserialize, Serialize};

use crate::{id::EntityId, node_id::NodeId, node_id::Signature, Attested, EntityState};

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
pub const PROTOCOL_VERSION: u32 = 4;

/// Domain tag for presence signatures (RFC specs/identity-attestation/
/// spec.md I.2): the signature covers `PRESENCE_TAG ||
/// bincode(PresenceClaims)`.
pub const PRESENCE_TAG: &[u8] = b"ankurah.presence.v0";

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
    pub system_root: Option<Attested<EntityState>>,
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

/// The projection of [`Presence`] the signature covers: everything a peer
/// asserts about itself (identity, class, which system it roots, freshness).
/// The full root STATE stays outside the signature (it carries its own
/// attestations); the root's entity id binds which system is claimed.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PresenceClaims {
    pub node_id: NodeId,
    pub durable: bool,
    pub system_root: Option<EntityId>,
    pub timestamp: u64,
}

impl Presence {
    /// The claims projection this presence asserts.
    pub fn claims(&self) -> PresenceClaims {
        PresenceClaims {
            node_id: self.node_id,
            durable: self.durable,
            system_root: self.system_root.as_ref().map(|root| root.payload.entity_id),
            timestamp: self.timestamp,
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
    pub fn verify(&self) -> bool { self.node_id.verify(&Self::signable_bytes(&self.claims()), &self.signature) }
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
                    r.payload
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
    // Message is a bincode enum: u32 little-endian variant index, and
    // Message::Presence is variant 0.
    if data.len() < 4 || data[..4] != [0, 0, 0, 0] {
        return false;
    }
    bincode::deserialize::<LegacyPresenceProbe>(&data[4..]).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;

    fn presence() -> Presence {
        let key = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        let node_id = NodeId::from(key.verifying_key());
        let claims = PresenceClaims { node_id, durable: true, system_root: None, timestamp: 1_720_000_000_000 };
        let signature: Signature = key.sign(&Presence::signable_bytes(&claims)).into();
        Presence { node_id, durable: true, system_root: None, timestamp: claims.timestamp, signature, protocol_version: PROTOCOL_VERSION }
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
        let bytes = bincode::serialize(&crate::Message::Presence(p.clone())).unwrap();
        match bincode::deserialize::<crate::Message>(&bytes).unwrap() {
            crate::Message::Presence(q) => assert_eq!(p, q),
            other => panic!("expected Presence, got {other}"),
        }
    }

    #[test]
    fn signature_verifies_and_tamper_fails() {
        let p = presence();
        assert!(p.verify());

        // Flipping any signed claim invalidates the signature.
        let mut forged = p.clone();
        forged.durable = false;
        assert!(!forged.verify());

        let mut forged = p.clone();
        forged.timestamp += 1;
        assert!(!forged.verify());

        // A different node id cannot claim this signature.
        let other = ed25519_dalek::SigningKey::from_bytes(&[43u8; 32]);
        let mut forged = p.clone();
        forged.node_id = NodeId::from(other.verifying_key());
        assert!(!forged.verify());
    }

    #[test]
    fn classifies_version0_presence() {
        let old_bytes =
            bincode::serialize(&LegacyMessage::Presence(LegacyPresenceOwned { node_id: [7u8; 16], durable: false, system_root: None }))
                .unwrap();
        // An old presence fails current decoding and classifies as version 0.
        assert!(bincode::deserialize::<crate::Message>(&old_bytes).is_err());
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
