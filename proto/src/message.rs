use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
    identity::NodeId,
    peering::{Presence, PresenceRejection},
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
        identity::Identity,
        request::{NodeRequestBody, NodeResponseBody, RequestId},
        update::{NodeUpdateAckBody, NodeUpdateBody, UpdateId},
    };

    fn node(byte: u8) -> NodeId { Identity::Ed25519([byte; 32]).into() }

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
}
