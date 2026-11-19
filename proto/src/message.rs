use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
    id::NodeId,
    peering::{Challenge, PresenceRejection, SignedPresence},
    request::{NodeRequest, NodeResponse},
    subscription::QueryId,
    update::{NodeUpdate, NodeUpdateAck},
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// The first message each side sends when a connection opens.
    Challenge(Challenge),
    /// Each side's answer to the other's challenge.
    Presence(SignedPresence),
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
    ReleaseComparison { from: NodeId, comparison: crate::ComparisonId },
}

impl NodeMessage {
    /// The sender node id the message declares.
    pub fn sender(&self) -> NodeId {
        match self {
            NodeMessage::Request { request, .. } => request.from,
            NodeMessage::Response(response) => response.from,
            NodeMessage::Update(update) => update.from,
            NodeMessage::UpdateAck(ack) => ack.from,
            NodeMessage::UnsubscribeQuery { from, .. } | NodeMessage::ReleaseComparison { from, .. } => *from,
        }
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Challenge(_) => write!(f, "Challenge"),
            Message::Presence(signed) => write!(f, "Presence: {}", signed.presence),
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
            NodeMessage::ReleaseComparison { from, comparison } => write!(f, "Release comparison: {from} {comparison:?}"),
        }
    }
}
