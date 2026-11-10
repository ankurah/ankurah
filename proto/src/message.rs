use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
    encoding,
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
    // TODO RPC messages
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
// in every protocol version; the tests below pin them.
const OPEN_INDEX: u32 = 0;
const NO_COMMON_VERSION_INDEX: u32 = 1;

impl Message {
    /// This message as a frame.
    pub fn encode(&self) -> Vec<u8> { encoding::encode(self) }

    /// The message `frame` holds; bytes left over after it make the frame undecodable.
    pub fn decode(frame: &[u8]) -> bincode::Result<Self> { encoding::decode_exact(frame) }

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

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Open(opening) => write!(f, "Open: versions {} from {:#}", opening.versions, opening.challenge.issuer()),
            Message::NoCommonVersion(versions) => write!(f, "NoCommonVersion: speaks {versions}"),
            Message::Presence(presence) => write!(f, "Presence: {}", presence),
            Message::PeerMessage(node_message) => write!(f, "PeerMessage: {}", node_message),
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
        peering::{HandshakeChallenge, PresenceClaims, VersionNegotiation},
        request::{NodeRequestBody, NodeResponseBody, RequestId},
        update::{NodeUpdateAckBody, NodeUpdateBody, UpdateId},
    };

    fn node(byte: u8) -> NodeId { Identity::Ed25519([byte; 32]).into() }

    /// The first four bytes of a message are its variant index, which every peer reads to frame it.
    /// The opening's and the refusal's stay fixed in every protocol version.
    #[test]
    fn message_variant_order_is_pinned() {
        let challenge = HandshakeChallenge::new(node(1));
        let versions = ProtocolVersions::SPOKEN;
        let claims = PresenceClaims {
            node_id: node(2),
            durable: false,
            challenge,
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
        ] {
            assert_eq!(message.encode()[..4], index, "{message}");
        }
    }

    #[test]
    fn node_message_variant_order_is_pinned() {
        let (from, to) = (node(1), node(2));
        let request = NodeRequest { id: RequestId::new(), to, from, body: CarriedBody::encode(&NodeRequestBody::Get { ids: vec![] }) };
        let response = NodeResponse { request_id: RequestId::new(), from, to, body: CarriedBody::encode(&NodeResponseBody::Success) };
        let body = CarriedBody::encode(&NodeUpdateBody::SubscriptionUpdate { items: vec![] });
        let update = NodeUpdate { id: UpdateId::new(), from, to, body };
        let ack = NodeUpdateAck { id: UpdateId::new(), from, to, body: NodeUpdateAckBody::Success };
        for (message, index) in [
            (NodeMessage::Request { auth: vec![], request }, [0, 0, 0, 0]),
            (NodeMessage::Response(response), [1, 0, 0, 0]),
            (NodeMessage::Update(update), [2, 0, 0, 0]),
            (NodeMessage::UpdateAck(ack), [3, 0, 0, 0]),
            (NodeMessage::UnsubscribeQuery { from, query_id: QueryId::test(1) }, [4, 0, 0, 0]),
        ] {
            assert_eq!(encode(&message)[..4], index, "{message}");
        }
    }

    fn versions(lowest: u32, highest: u32) -> ProtocolVersions { ProtocolVersions { lowest, highest } }

    /// An opening offering `versions`, under the challenge issued by `Ed25519([0x77; 32])` with nonce `[0x88; 32]`.
    fn opening(versions: ProtocolVersions) -> Opening {
        let challenge = decode_exact(&[[0; 4].as_slice(), &[0x77; 32], &[0x88; 32]].concat()).unwrap();
        Opening { versions, challenge }
    }

    /// The opening and refusal vectors of specs/protocol/bytes.md section 6: each frame's bytes.
    #[test]
    fn the_opening_and_refusal_frames_match_the_byte_specification_vectors() {
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

        let refusal = Message::NoCommonVersion(versions(1, 1)).encode();
        assert_eq!(hex::encode(&refusal), "010000000100000001000000");
    }

    /// A version-1 reader rejects bytes after the challenge, so reaching the refusal below shows
    /// the rest of the frame was never read.
    #[test]
    fn an_opening_sharing_no_version_is_refused_before_the_rest_is_read() {
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
    fn a_refusal_in_place_of_an_opening_names_the_versions_its_sender_speaks() {
        let refusal = Message::NoCommonVersion(versions(4, 6)).encode();
        assert_eq!(Message::read_opening(&refusal, versions(1, 1)), Err(OpeningError::NoCommonVersion(versions(4, 6))));
    }

    #[test]
    fn a_frame_that_opens_with_neither_an_opening_nor_a_refusal_is_undecodable() {
        let open = Message::Open(opening(versions(1, 1))).encode();
        let mut other_kind = open.clone();
        other_kind[0] = 2;
        for frame in [&open[..11], &open[..40], &other_kind[..], &open[..0]] {
            assert!(matches!(Message::read_opening(frame, versions(1, 1)), Err(OpeningError::Undecodable(_))), "{frame:?}");
        }
    }
}
