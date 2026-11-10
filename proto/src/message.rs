use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
    encoding::{self, UndecodableVariant},
    identity::NodeId,
    peering::{Opening, Presence, ProtocolVersions},
    request::{NodeRequest, NodeResponse},
    subscription::QueryId,
    update::{NodeUpdate, NodeUpdateAck},
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// The first frame each side of a connection sends, before any presence.
    Open(Opening),
    /// The answer to an opening that offers no version the receiver speaks: the versions the
    /// receiver speaks. The connection closes right after. Laid out as an opening's first twelve
    /// bytes, so any build reads it.
    NoCommonVersion(ProtocolVersions),
    Presence(Presence),
    PeerMessage(NodeMessage),
    /// The accepting side's answer to a dialer's presence in place of its own: it will not
    /// register the dialer, for the reason given, written for a person to read. The connection
    /// closes right after.
    Refused(String),
    // TODO RPC messages
}

/// Why a connection's first frame opens no connection this build can take.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpeningError {
    /// The sender speaks only versions the reader does not; the rest of the frame was not read.
    #[error("the peer speaks protocol versions {0}, none of which this node speaks")]
    NoCommonVersion(ProtocolVersions),
    /// Neither an opening nor a `NoCommonVersion` answer, or an opening whose challenge does not
    /// decode.
    #[error("not an opening frame this build can read: {0}")]
    Undecodable(String),
}

/// Why a frame holds no message this build reads.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnreadableFrame {
    /// A message of a kind a later protocol version defines: a variant of [`Message`], or of the
    /// [`NodeMessage`] a peer message holds, that this build does not have. `indices` are the
    /// variant indices that name the kind, the message's first: `[5]` names a message kind,
    /// `[3, 5]` a peer message kind.
    #[error("a message of kind {indices:?}, which this build does not know")]
    UnknownKind { indices: Vec<u32> },
    /// Bytes that do not decode as the message kind they name, or bytes left over after it.
    #[error("a frame whose bytes do not decode: {0}")]
    Malformed(String),
}

impl UnreadableFrame {
    /// Whether the connection this frame arrived on closes (specs/protocol/bytes.md section 6): always for a kind this build
    /// does not know, and for a malformed frame only during the handshake, since that handshake will never complete.
    pub fn closes_connection(&self, handshake_complete: bool) -> bool {
        match self {
            UnreadableFrame::UnknownKind { .. } => true,
            UnreadableFrame::Malformed(_) => !handshake_complete,
        }
    }
}

// The variant indices of `Message::Open` and `Message::NoCommonVersion`, which open their frames
// in every protocol version, and of `Message::PeerMessage`, whose `NodeMessage` a later version
// may extend; the tests below pin them.
const OPEN_INDEX: u32 = 0;
const NO_COMMON_VERSION_INDEX: u32 = 1;
const PEER_MESSAGE_INDEX: u32 = 3;

impl Message {
    /// This message as a frame.
    pub fn encode(&self) -> Vec<u8> { encoding::encode(self) }

    /// The message `frame` holds; bytes left over after it make the frame unreadable.
    pub fn decode(frame: &[u8]) -> Result<Self, UnreadableFrame> {
        encoding::decode_variant(frame).map_err(|undecodable| match undecodable {
            UndecodableVariant::Unknown { index } => UnreadableFrame::UnknownKind { indices: vec![index] },
            UndecodableVariant::Malformed(error) => match encoding::unknown_variant_within::<NodeMessage>(frame, PEER_MESSAGE_INDEX) {
                Some(peer_message) => UnreadableFrame::UnknownKind { indices: vec![PEER_MESSAGE_INDEX, peer_message] },
                None => UnreadableFrame::Malformed(error.to_string()),
            },
        })
    }

    /// The opening a connection's first frame holds, read as every build reads one: past its first
    /// twelve bytes only when the sender shares a version with `spoken` (specs/protocol/bytes.md section 6).
    pub fn read_opening(frame: &[u8], spoken: ProtocolVersions) -> Result<Opening, OpeningError> {
        let word = |at: usize| frame.get(at..at + 4).and_then(|bytes| bytes.try_into().ok()).map(u32::from_le_bytes);
        let (Some(index), Some(lowest), Some(highest)) = (word(0), word(4), word(8)) else {
            return Err(OpeningError::Undecodable(format!("a frame of {} bytes", frame.len())));
        };
        let versions = ProtocolVersions { lowest, highest };
        match index {
            OPEN_INDEX if spoken.highest_shared(versions).is_some() => {
                let challenge = encoding::decode_exact(&frame[12..]).map_err(|error| OpeningError::Undecodable(error.to_string()))?;
                Ok(Opening { versions, challenge })
            }
            OPEN_INDEX | NO_COMMON_VERSION_INDEX => Err(OpeningError::NoCommonVersion(versions)),
            other => Err(OpeningError::Undecodable(format!("a message of kind {other} where an opening belongs"))),
        }
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
    /// The node this message names as its sender.
    pub fn sender(&self) -> NodeId {
        match self {
            NodeMessage::Request { request, .. } => request.from,
            NodeMessage::Response(response) => response.from,
            NodeMessage::Update(update) => update.from,
            NodeMessage::UpdateAck(ack) => ack.from,
            NodeMessage::UnsubscribeQuery { from, .. } => *from,
        }
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Open(opening) => write!(f, "Open: versions {} from {:#}", opening.versions, opening.challenge.issuer()),
            Message::NoCommonVersion(versions) => write!(f, "NoCommonVersion: speaks {versions}"),
            Message::Presence(presence) => write!(f, "Presence: {}", presence),
            Message::PeerMessage(node_message) => write!(f, "PeerMessage: {}", node_message),
            Message::Refused(reason) => write!(f, "Refused: {reason}"),
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
    use crate::encoding::{decode_exact, encode};
    use crate::{
        carried_body::CarriedBody,
        identity::{Identity, Signature},
        peering::{ConnectionRole, HandshakeChallenge, PresenceClaims, VersionNegotiation},
        request::{NodeRequestBody, NodeResponseBody, RequestId},
        update::{NodeUpdateAckBody, NodeUpdateBody, UpdateId},
    };

    fn node(byte: u8) -> NodeId { Identity::Ed25519([byte; 32]).into() }

    /// The first four bytes of a message are its variant index, which every peer reads to frame it.
    /// The opening's and the `NoCommonVersion` answer's stay fixed in every protocol version.
    #[test]
    fn message_variant_order_is_pinned() {
        let challenge = HandshakeChallenge::new(node(1));
        let versions = ProtocolVersions::SPOKEN;
        let claims = PresenceClaims {
            node_id: node(2),
            durable: false,
            role: ConnectionRole::Dialing,
            challenge,
            own_challenge: HandshakeChallenge::new(node(2)),
            timestamp: 0,
            negotiation: VersionNegotiation { offered: versions, peer_offered: versions, chosen: 1 },
            features: vec![],
            announcements: Default::default(),
        };
        let presence = Presence { claims, signature: Signature::Ed25519([0; 64]) };
        let unsubscribe = NodeMessage::UnsubscribeQuery { from: node(2), query_id: QueryId::test(1) };
        for (message, index) in [
            (Message::Open(Opening { versions, challenge }), [0, 0, 0, 0]),
            (Message::NoCommonVersion(versions), [1, 0, 0, 0]),
            (Message::Presence(presence), [2, 0, 0, 0]),
            (Message::PeerMessage(unsubscribe), [3, 0, 0, 0]),
            (Message::Refused("not today".into()), [4, 0, 0, 0]),
        ] {
            assert_eq!(message.encode()[..4], index, "{message}");
        }
    }

    /// One message of each kind, in declaration order, from `from` to `to`.
    fn one_node_message_of_each_kind(from: NodeId, to: NodeId) -> [NodeMessage; 5] {
        let request = NodeRequest { id: RequestId::new(), to, from, body: CarriedBody::encode(&NodeRequestBody::Get { ids: vec![] }) };
        let response = NodeResponse { request_id: RequestId::new(), from, to, body: CarriedBody::encode(&NodeResponseBody::Success) };
        let body = CarriedBody::encode(&NodeUpdateBody::SubscriptionUpdate { items: vec![] });
        let update = NodeUpdate { id: UpdateId::new(), from, to, body };
        let ack = NodeUpdateAck { id: UpdateId::new(), from, to, body: NodeUpdateAckBody::Success };
        [
            NodeMessage::Request { auth: vec![], request },
            NodeMessage::Response(response),
            NodeMessage::Update(update),
            NodeMessage::UpdateAck(ack),
            NodeMessage::UnsubscribeQuery { from, query_id: QueryId::test(1) },
        ]
    }

    #[test]
    fn node_message_variant_order_is_pinned() {
        let indices = [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0], [3, 0, 0, 0], [4, 0, 0, 0]];
        for (message, index) in one_node_message_of_each_kind(node(1), node(2)).into_iter().zip(indices) {
            assert_eq!(encode(&message)[..4], index, "{message}");
        }
    }

    #[test]
    fn every_node_message_names_its_sender() {
        for message in one_node_message_of_each_kind(node(1), node(2)) {
            assert_eq!(message.sender(), node(1), "{message}");
        }
    }

    fn versions(lowest: u32, highest: u32) -> ProtocolVersions { ProtocolVersions { lowest, highest } }

    /// An opening offering `versions`, under the challenge issued by `Ed25519([0x77; 32])` with nonce `[0x88; 32]`.
    fn opening(versions: ProtocolVersions) -> Opening {
        let challenge = decode_exact(&[[0; 4].as_slice(), &[0x77; 32], &[0x88; 32]].concat()).unwrap();
        Opening { versions, challenge }
    }

    /// The vectors of specs/protocol/bytes.md section 6 for an opening and a `NoCommonVersion`
    /// answer: each frame's bytes.
    #[test]
    fn the_opening_and_no_common_version_frames_match_the_byte_specification_vectors() {
        let open = Message::Open(opening(versions(1, 3))).encode();
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
        assert_eq!(Message::read_opening(&open, ProtocolVersions::SPOKEN), Ok(opening(versions(1, 3))));

        let no_common_version = Message::NoCommonVersion(versions(1, 1)).encode();
        assert_eq!(hex::encode(&no_common_version), "010000000100000001000000");
    }

    /// The `Refused` vector of specs/protocol/bytes.md section 6: an accepting side's answer to a
    /// presence it will not register.
    #[test]
    fn the_refused_frame_matches_the_byte_specification_vector() {
        let refused = Message::Refused("not today".into()).encode();
        assert_eq!(hex::encode(&refused), concat!("04000000", "0900000000000000", "6e6f7420746f646179"));
        assert!(matches!(Message::decode(&refused), Ok(Message::Refused(reason)) if reason == "not today"));
    }

    /// A version-1 reader rejects bytes after the challenge, so reaching `NoCommonVersion` below
    /// shows the rest of the frame was never read.
    #[test]
    fn no_common_version_is_found_before_the_rest_of_an_opening_is_read() {
        let mut frame = Message::Open(opening(versions(2, 3))).encode();
        frame.extend_from_slice(b"bytes a version-1 reader rejects");
        assert_eq!(Message::read_opening(&frame, versions(1, 1)), Err(OpeningError::NoCommonVersion(versions(2, 3))));

        let mut shared = Message::Open(opening(versions(1, 3))).encode();
        shared.extend_from_slice(b"bytes a version-1 reader rejects");
        assert!(
            matches!(Message::read_opening(&shared, versions(1, 1)), Err(OpeningError::Undecodable(_))),
            "shared versions read the rest"
        );
    }

    #[test]
    fn a_no_common_version_answer_in_place_of_an_opening_names_the_versions_its_sender_speaks() {
        let no_common_version = Message::NoCommonVersion(versions(4, 6)).encode();
        assert_eq!(Message::read_opening(&no_common_version, versions(1, 1)), Err(OpeningError::NoCommonVersion(versions(4, 6))));
    }

    #[test]
    fn a_frame_that_opens_with_neither_an_opening_nor_a_no_common_version_answer_is_undecodable() {
        let open = Message::Open(opening(versions(1, 1))).encode();
        let mut other_kind = open.clone();
        other_kind[0] = 2;
        for frame in [&open[..11], &open[..40], &other_kind[..], &open[..0]] {
            assert!(matches!(Message::read_opening(frame, versions(1, 1)), Err(OpeningError::Undecodable(_))), "{frame:?}");
        }
    }

    /// A later version may add a kind of message, or of peer message; this build tells either from
    /// a frame whose bytes do not decode.
    #[test]
    fn a_message_of_a_kind_this_build_does_not_know_is_told_from_a_malformed_frame() {
        assert_eq!(Message::decode(&[99, 0, 0, 0]).unwrap_err(), UnreadableFrame::UnknownKind { indices: vec![99] });
        assert_eq!(Message::decode(&[3, 0, 0, 0, 99, 0, 0, 0]).unwrap_err(), UnreadableFrame::UnknownKind { indices: vec![3, 99] });
        let mut trailing = Message::Refused("not today".into()).encode();
        trailing.push(0);
        for frame in [&[3, 0, 0, 0, 4, 0, 0, 0][..], &[2, 0, 0, 0][..], &trailing[..], &[][..]] {
            assert!(matches!(Message::decode(frame), Err(UnreadableFrame::Malformed(_))), "{frame:?}");
        }
    }

    #[test]
    fn an_unknown_kind_closes_the_connection_and_a_malformed_frame_closes_it_only_during_the_handshake() {
        let unknown = UnreadableFrame::UnknownKind { indices: vec![99] };
        assert!(unknown.closes_connection(false) && unknown.closes_connection(true));
        let malformed = UnreadableFrame::Malformed("a frame of no kind".into());
        assert!(malformed.closes_connection(false), "a malformed frame closes a handshake");
        assert!(!malformed.closes_connection(true), "a malformed frame after the handshake is dropped");
    }
}
