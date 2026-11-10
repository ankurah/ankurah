use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
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
    use crate::encoding::encode;
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
            assert_eq!(encode(&message)[..4], index, "{message}");
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
}
