use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::{clock::Clock, id::EntityId, Attested, CollectionId, EntityState, StateBuffers};

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
/// - absent: 0.9.x and earlier carried no version in Presence. Such peers
///   are classified as version 0 (see [`is_version0_presence`]) and refused.
/// - 1: the 0.10.0 wire (0.10.0 is not yet released to crates.io). The
///   version field itself arrives with it (#294); 0.10.0's serialized
///   contract is incompatible with 0.9.x.
pub const PROTOCOL_VERSION: u32 = 1;

/// Whether a peer advertising `remote` can interoperate with this binary.
///
/// Exact match for now: refuse on mismatch was the #294 decision, because
/// serving an older version would require maintaining dual codecs for every
/// changed message. Isolated here so a future version can widen acceptance
/// to a range without touching the handshake again.
pub fn protocol_compatible(remote: u32) -> bool { remote == PROTOCOL_VERSION }

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: EntityId,
    pub durable: bool,
    pub system_root: Option<Attested<EntityState>>,
    /// See [`PROTOCOL_VERSION`]. Kept as the LAST field, which used to make a
    /// pre-#294 ephemeral Presence a strict prefix of this one. That prefix
    /// property ended when the entity id widened to 32 bytes: `node_id` is
    /// now a different width than the one a 0.9.x peer writes, so no 0.9.x
    /// Presence is a prefix of a current one. Recognizing those peers is
    /// [`is_version0_presence`]'s job alone, against the frozen mirror
    /// structs below.
    pub protocol_version: u32,
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

/// The Presence shape that pre-#294 binaries (0.9.x and earlier) send: no
/// protocol_version field. Used only to classify an undecodable handshake,
/// never constructed.
#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
struct LegacyPresence {
    node_id: LegacyId,
    durable: bool,
    system_root: Option<Attested<LegacyEntityState>>,
}

/// The 16-byte ULID-shaped id a 0.9.x peer writes for `node_id` and
/// `entity_id`. Frozen here so widening the live [`EntityId`] leaves
/// version-0 detection reading exactly the bytes 0.9.x sends. Bincode
/// encodes a newtype struct as its inner value, matching 0.9.x's
/// `[u8; 16]` fixed array with no length prefix.
#[derive(Serialize, Deserialize)]
struct LegacyId([u8; 16]);

/// The EntityState nested inside a 0.9.x durable Presence, pinned as its own
/// struct so later changes to the live EntityState shape cannot silently
/// change version-0 detection.
#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
struct LegacyEntityState {
    entity_id: LegacyId,
    collection: CollectionId,
    state: LegacyState,
}

/// The State nested inside a 0.9.x durable Presence: buffers and head only,
/// with no membership set. Frozen for the same reason as [`LegacyId`] —
/// the live [`State`] grew a `memberships` field between these two fields,
/// which a 0.9.x peer does not send.
#[derive(Serialize, Deserialize, Default)]
#[allow(dead_code)]
struct LegacyState {
    state_buffers: StateBuffers,
    head: Clock,
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
    bincode::DefaultOptions::new().with_fixint_encoding().reject_trailing_bytes().deserialize::<LegacyPresence>(&data[4..]).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn presence() -> Presence {
        Presence { node_id: EntityId::random(), durable: true, system_root: None, protocol_version: PROTOCOL_VERSION }
    }

    /// The 0.9.x wire shapes, mirrored for compatibility tests.
    #[derive(Serialize)]
    enum LegacyMessage {
        Presence(LegacyPresence),
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

    /// The advertised version is the last field, so a current Presence ends
    /// with exactly those four bytes. The 0.9.x encoding is not a prefix of
    /// the current one (the node ids differ in width), which is why
    /// [`is_version0_presence`] classifies against frozen mirror structs
    /// rather than by prefix match.
    #[test]
    fn the_advertised_version_is_the_last_field() {
        let p = presence();
        let bytes = bincode::serialize(&crate::Message::Presence(p.clone())).unwrap();
        assert_eq!(bytes[bytes.len() - 4..], PROTOCOL_VERSION.to_le_bytes());
    }

    #[test]
    fn classifies_version0_presence() {
        let old_bytes =
            bincode::serialize(&LegacyMessage::Presence(LegacyPresence { node_id: LegacyId([7; 16]), durable: false, system_root: None }))
                .unwrap();
        // An old presence fails current decoding and classifies as version 0.
        assert!(bincode::deserialize::<crate::Message>(&old_bytes).is_err());
        assert!(is_version0_presence(&old_bytes));

        // The same legacy payload with an advertised protocol version is a
        // versioned v1/v2 handshake, not pre-versioning v0. Reject trailing
        // bytes so the classifier reports only the shape it names.
        let mut versioned_old_bytes = old_bytes.clone();
        versioned_old_bytes.extend_from_slice(&1u32.to_le_bytes());
        assert!(!is_version0_presence(&versioned_old_bytes));

        // Garbage and non-Presence variants do not.
        assert!(!is_version0_presence(&[]));
        assert!(!is_version0_presence(&[7, 7, 7, 7, 7]));
        assert!(!is_version0_presence(&[1, 0, 0, 0, 0, 0]));
    }

    #[test]
    fn classifies_durable_version0_presence_with_legacy_system_root() {
        let old_bytes = bincode::serialize(&LegacyMessage::Presence(LegacyPresence {
            node_id: LegacyId([7; 16]),
            durable: true,
            system_root: Some(Attested::opt(
                LegacyEntityState {
                    entity_id: LegacyId([9; 16]),
                    collection: CollectionId::fixed_name("_ankurah_system"),
                    state: LegacyState::default(),
                },
                None,
            )),
        }))
        .unwrap();

        assert!(bincode::deserialize::<crate::Message>(&old_bytes).is_err());
        assert!(is_version0_presence(&old_bytes));
    }

    #[test]
    fn compatibility_is_exact_match() {
        assert!(protocol_compatible(PROTOCOL_VERSION));
        assert!(!protocol_compatible(0));
        assert!(!protocol_compatible(PROTOCOL_VERSION + 1));
    }
}
