use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::{id::EntityId, Attested, CollectionId, EntityState, State};

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
    /// See [`PROTOCOL_VERSION`]. Kept as the LAST field so an ephemeral
    /// pre-#294 Presence (`system_root: None`) is a strict prefix of this one.
    /// A durable legacy Presence also uses the old collection-bearing nested
    /// EntityState shape; [`is_version0_presence`] mirrors that shape when
    /// classifying an otherwise-undecodable handshake.
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
    node_id: EntityId,
    durable: bool,
    system_root: Option<Attested<LegacyEntityState>>,
}

/// The EntityState nested inside a 0.9.x durable Presence, pinned as its own
/// struct so later changes to the live EntityState shape cannot silently
/// change version-0 detection.
#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
struct LegacyEntityState {
    entity_id: EntityId,
    collection: CollectionId,
    state: State,
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
        Presence { node_id: EntityId::new(), durable: true, system_root: None, protocol_version: PROTOCOL_VERSION }
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

    /// With no nested system root, the pre-#294 encoding remains a strict
    /// prefix of the current one because protocol_version is last.
    #[test]
    fn legacy_encoding_is_a_prefix() {
        let p = presence();
        let new_bytes = bincode::serialize(&crate::Message::Presence(p.clone())).unwrap();
        let old_bytes =
            bincode::serialize(&LegacyMessage::Presence(LegacyPresence { node_id: p.node_id, durable: p.durable, system_root: None }))
                .unwrap();
        assert_eq!(new_bytes[..old_bytes.len()], old_bytes[..]);
        assert_eq!(new_bytes.len(), old_bytes.len() + 4);
        assert_eq!(new_bytes[old_bytes.len()..], PROTOCOL_VERSION.to_le_bytes());
    }

    #[test]
    fn classifies_version0_presence() {
        let old_bytes =
            bincode::serialize(&LegacyMessage::Presence(LegacyPresence { node_id: EntityId::new(), durable: false, system_root: None }))
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
            node_id: EntityId::new(),
            durable: true,
            system_root: Some(Attested::opt(
                LegacyEntityState {
                    entity_id: EntityId::new(),
                    collection: CollectionId::fixed_name("_ankurah_system"),
                    state: State::default(),
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
