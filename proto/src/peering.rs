use serde::{Deserialize, Serialize};

use crate::{id::EntityId, Attested, EntityState};

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
pub const PROTOCOL_VERSION: u32 = 2;

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
    /// See [`PROTOCOL_VERSION`]. Kept as the LAST field so the pre-#294
    /// Presence encoding is a strict prefix of this one: reading an old
    /// peer's Presence fails at exactly this field, which is what lets
    /// [`is_version0_presence`] classify it instead of guessing.
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
#[derive(Deserialize)]
#[allow(dead_code)]
struct LegacyPresence {
    node_id: EntityId,
    durable: bool,
    system_root: Option<Attested<EntityState>>,
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
    bincode::deserialize::<LegacyPresence>(&data[4..]).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn presence() -> Presence {
        Presence { node_id: EntityId::new(), durable: true, system_root: None, protocol_version: PROTOCOL_VERSION }
    }

    /// The 0.9.x wire shapes, mirrored for compatibility tests.
    #[derive(Serialize)]
    struct LegacyPresenceOwned {
        node_id: EntityId,
        durable: bool,
        system_root: Option<Attested<EntityState>>,
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

    /// The pre-#294 encoding must remain a strict prefix of the current
    /// one (protocol_version is the last field). If this breaks, version-0
    /// classification and the documented failure mode both break.
    #[test]
    fn legacy_encoding_is_a_prefix() {
        let p = presence();
        let new_bytes = bincode::serialize(&crate::Message::Presence(p.clone())).unwrap();
        let old_bytes = bincode::serialize(&LegacyMessage::Presence(LegacyPresenceOwned {
            node_id: p.node_id,
            durable: p.durable,
            system_root: p.system_root,
        }))
        .unwrap();
        assert_eq!(new_bytes[..old_bytes.len()], old_bytes[..]);
        assert_eq!(new_bytes.len(), old_bytes.len() + 4);
        assert_eq!(new_bytes[old_bytes.len()..], PROTOCOL_VERSION.to_le_bytes());
    }

    #[test]
    fn classifies_version0_presence() {
        let old_bytes = bincode::serialize(&LegacyMessage::Presence(LegacyPresenceOwned {
            node_id: EntityId::new(),
            durable: false,
            system_root: None,
        }))
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
