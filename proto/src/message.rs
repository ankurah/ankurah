use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
    identity::NodeId,
    peering::{HandshakeChallenge, Presence, PresenceRejection},
    request::{NodeRequest, NodeResponse},
    subscription::QueryId,
    update::{NodeUpdate, NodeUpdateAck},
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Presence(Presence),
    PeerMessage(NodeMessage),
    /// Best-effort notice that the sender is refusing the connection over
    /// a protocol version mismatch; the connection closes right after.
    PresenceRejected(PresenceRejection),
    /// A fresh challenge the receiver's presence must answer. Each side sends
    /// one when a connection opens, before any presence.
    Challenge(HandshakeChallenge),
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
            Message::Presence(presence) => write!(f, "Presence: {}", presence),
            Message::PeerMessage(node_message) => write!(f, "PeerMessage: {}", node_message),
            Message::PresenceRejected(rejection) => write!(f, "PresenceRejected: {}", rejection),
            Message::Challenge(challenge) => write!(f, "Challenge from {:#}", challenge.issuer()),
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
    use crate::{
        identity::{Identity, Signature},
        request::{NodeRequestBody, NodeResponseBody, RequestId},
        update::{NodeUpdateAckBody, NodeUpdateBody, UpdateId},
    };

    fn node(byte: u8) -> NodeId { Identity::Ed25519([byte; 32]).into() }

    /// The first four bytes of a message are its variant index, which every peer reads to frame it.
    #[test]
    fn message_variant_order_is_pinned() {
        let challenge = HandshakeChallenge::new(node(1));
        let presence = Presence {
            node_id: node(2),
            durable: false,
            challenge,
            timestamp: 0,
            signature: Signature::Ed25519([0; 64]),
            protocol_version: 1,
        };
        let unsubscribe = NodeMessage::UnsubscribeQuery { from: node(2), query_id: QueryId::test(1) };
        for (message, index) in [
            (Message::Presence(presence), [0, 0, 0, 0]),
            (Message::PeerMessage(unsubscribe), [1, 0, 0, 0]),
            (Message::PresenceRejected(PresenceRejection { expected: 1, received: 0 }), [2, 0, 0, 0]),
            (Message::Challenge(challenge), [3, 0, 0, 0]),
        ] {
            assert_eq!(bincode::serialize(&message).unwrap()[..4], index, "{message}");
        }
    }

    #[test]
    fn node_message_variant_order_is_pinned() {
        let (from, to) = (node(1), node(2));
        let request = NodeRequest { id: RequestId::new(), to, from, body: NodeRequestBody::Get { ids: vec![] } };
        let response = NodeResponse { request_id: RequestId::new(), from, to, body: NodeResponseBody::Success };
        let update = NodeUpdate { id: UpdateId::new(), from, to, body: NodeUpdateBody::SubscriptionUpdate { items: vec![] } };
        let ack = NodeUpdateAck { id: UpdateId::new(), from, to, body: NodeUpdateAckBody::Success };
        for (message, index) in [
            (NodeMessage::Request { auth: vec![], request }, [0, 0, 0, 0]),
            (NodeMessage::Response(response), [1, 0, 0, 0]),
            (NodeMessage::Update(update), [2, 0, 0, 0]),
            (NodeMessage::UpdateAck(ack), [3, 0, 0, 0]),
            (NodeMessage::UnsubscribeQuery { from, query_id: QueryId::test(1) }, [4, 0, 0, 0]),
        ] {
            assert_eq!(bincode::serialize(&message).unwrap()[..4], index, "{message}");
        }
    }
}
