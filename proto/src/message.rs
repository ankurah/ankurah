use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
    node_id::{NodeId, Signature},
    peering::{HandshakeChallenge, Presence, PresenceRejection},
    request::{NodeRequest, NodeResponse},
    subscription::QueryId,
    update::{NodeUpdate, NodeUpdateAck},
};

pub const PEER_MESSAGE_TAG: &[u8] = b"ankurah.peer-message.v0";

/// Maximum encoded size of one outer protocol frame. This matches the
/// conservative default ceiling used by common WebSocket implementations and
/// bounds both serialization and deserialization work inside proto.
pub const MAX_WIRE_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    // Do not reorder the first three variants: protocol v5 serialized them as
    // discriminants 0, 1, and 2. New handshake frames append at 3 so an old
    // Presence can never be reinterpreted as a v6 challenge.
    Presence(Presence),
    PeerMessage(SignedPeerMessage),
    /// Best-effort notice that the sender is refusing the connection over
    /// a protocol version mismatch; the connection closes right after.
    PresenceRejected(PresenceRejection),
    /// Fresh receiver nonce. A peer must answer this value in its signed
    /// Presence before any NodeMessage is admitted on the connection.
    HandshakeChallenge(HandshakeChallenge),
    // TODO RPC messages
}

fn wire_options(limit: u64) -> impl Options {
    // bincode's top-level serialize/deserialize helpers use little-endian
    // fixed-width integers, unlike DefaultOptions' varint default. Spell out
    // fixint here to preserve every existing wire discriminant and length.
    bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian().with_limit(limit).reject_trailing_bytes()
}

pub(crate) fn decode_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> bincode::Result<T> {
    if bytes.len() > MAX_WIRE_MESSAGE_BYTES {
        return Err(Box::new(bincode::ErrorKind::SizeLimit));
    }
    // Limit allocations and bytes consumed to the frame actually supplied;
    // a forged collection length cannot claim a larger budget than its frame.
    wire_options(bytes.len() as u64).deserialize(bytes)
}

/// Decode exactly one bounded outer wire frame.
pub fn decode_message(bytes: &[u8]) -> bincode::Result<Message> { decode_exact(bytes) }

/// Encode one bounded outer wire frame using the legacy-compatible fixed-int
/// configuration.
pub fn encode_message(message: &Message) -> bincode::Result<Vec<u8>> { wire_options(MAX_WIRE_MESSAGE_BYTES as u64).serialize(message) }

/// One direction of an authenticated peer session. Every post-handshake frame
/// is signed over the receiver-issued challenge and a monotonically allocated
/// sequence number. Receivers keep a bounded replay window so unique reordered
/// frames remain usable while duplicates and stale replays are rejected. This
/// prevents an active relay from turning a forwarded Presence into authority
/// to inject unsigned frames as that node.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignedPeerMessage {
    pub session: HandshakeChallenge,
    pub sequence: u64,
    pub message: NodeMessage,
    pub signature: Signature,
}

impl SignedPeerMessage {
    pub fn signable_bytes(session: HandshakeChallenge, sequence: u64, message: &NodeMessage) -> Vec<u8> {
        let mut bytes = PEER_MESSAGE_TAG.to_vec();
        bytes.extend(bincode::serialize(&(session, sequence, message)).expect("peer message serializes"));
        bytes
    }

    pub fn verify(&self, signer: NodeId) -> bool {
        signer.verify(&Self::signable_bytes(self.session, self.sequence, &self.message), &self.signature)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeMessage {
    Request { auth: Vec<AuthData>, request: NodeRequest },
    Response(NodeResponse),
    Update(NodeUpdate),
    UpdateAck(NodeUpdateAck),
    UnsubscribeQuery { from: NodeId, query_id: QueryId },
}

impl NodeMessage {
    /// The sender identity declared by this message's wire envelope.
    ///
    /// Connectors must compare this value with the peer authenticated during
    /// their Presence handshake before admitting the message to a node.
    pub fn declared_sender(&self) -> NodeId {
        match self {
            NodeMessage::Request { request, .. } => request.from,
            NodeMessage::Response(response) => response.from,
            NodeMessage::Update(update) => update.from,
            NodeMessage::UpdateAck(update_ack) => update_ack.from,
            NodeMessage::UnsubscribeQuery { from, .. } => *from,
        }
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::HandshakeChallenge(_) => write!(f, "HandshakeChallenge"),
            Message::Presence(presence) => write!(f, "Presence: {}", presence),
            Message::PeerMessage(frame) => write!(f, "PeerMessage[{}]: {}", frame.sequence, frame.message),
            Message::PresenceRejected(rejection) => write!(f, "PresenceRejected: {}", rejection),
        }
    }
}

impl std::fmt::Display for NodeMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeMessage::Request { request, .. } => write!(f, "Request: {}", request),
            NodeMessage::Response(response) => write!(f, "Response: {}", response),
            NodeMessage::Update(update) => write!(f, "Update: {}", update),
            NodeMessage::UpdateAck(update_ack) => write!(f, "UpdateAck: {}", update_ack),
            NodeMessage::UnsubscribeQuery { from, query_id } => write!(f, "Unsubscribe: {} {}", from, query_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeId { NodeId::from_bytes([byte; 32]) }

    fn challenge() -> HandshakeChallenge { HandshakeChallenge::new(node(0x31), [0xC1; 32]) }

    fn discriminant(message: &Message) -> u32 {
        let encoded = encode_message(message).unwrap();
        u32::from_le_bytes(encoded[..4].try_into().unwrap())
    }

    #[test]
    fn v5_discriminants_are_frozen_and_challenge_is_appended() {
        let challenge = challenge();
        let presence = Presence {
            node_id: node(0x32),
            durable: false,
            system_root: None,
            challenge,
            timestamp: 0,
            signature: Signature::from_bytes([0; 64]),
            protocol_version: crate::PROTOCOL_VERSION,
        };
        let peer = SignedPeerMessage {
            session: challenge,
            sequence: 0,
            message: NodeMessage::UnsubscribeQuery { from: node(0x32), query_id: QueryId::test(1) },
            signature: Signature::from_bytes([0; 64]),
        };
        let rejection = PresenceRejection { expected: crate::PROTOCOL_VERSION, received: 5 };

        assert_eq!(discriminant(&Message::Presence(presence)), 0);
        assert_eq!(discriminant(&Message::PeerMessage(peer)), 1);
        assert_eq!(discriminant(&Message::PresenceRejected(rejection)), 2);
        assert_eq!(discriminant(&Message::HandshakeChallenge(challenge)), 3);
    }

    #[test]
    fn wire_codec_matches_legacy_fixint_bytes_and_rejects_trailing_data() {
        let message = Message::HandshakeChallenge(challenge());
        let encoded = encode_message(&message).unwrap();
        assert_eq!(encoded, bincode::serialize(&message).unwrap(), "codec must retain bincode 1.x helper encoding");
        assert!(matches!(decode_message(&encoded).unwrap(), Message::HandshakeChallenge(_)));

        let mut suffixed = encoded;
        suffixed.push(0);
        assert!(decode_message(&suffixed).is_err(), "a frame containing trailing bytes is not exact");
    }
}
