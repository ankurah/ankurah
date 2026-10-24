use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthData,
    id::EntityId,
    peering::{Presence, PresenceRejection},
    request::{NodeRequest, NodeResponse, RequestId},
    subscription::QueryId,
    StateFrame, StateStreamId,
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
    Request {
        auth: Vec<AuthData>,
        request: NodeRequest,
    },
    Response(NodeResponse),
    /// A state-stream frame after frame zero, pushed by the stream's responder `from`
    /// to its requester `to`. `request_id` names the Fetch or SubscribeQuery that opened
    /// the stream, so the requester can route a frame that arrives before that
    /// request's response. No response is sent; the requester returns frame credit
    /// in StateFeedback.
    StateFrame {
        from: EntityId,
        to: EntityId,
        request_id: RequestId,
        frame: StateFrame,
    },
    UnsubscribeQuery {
        from: EntityId,
        query_id: QueryId,
    },
    /// One-way cleanup of a stream owned by `from` on `to`. No response is sent.
    /// Ends its active round and live delivery; a replacement stream is unaffected.
    AbandonStateStream {
        from: EntityId,
        to: EntityId,
        stream: StateStreamId,
    },
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
            NodeMessage::StateFrame { from, to, request_id, frame } => {
                write!(f, "StateFrame {from}->{to} {request_id} {} sequence:{}", frame.stream, frame.sequence)
            }
            NodeMessage::UnsubscribeQuery { from, query_id } => write!(f, "Unsubscribe: {} {}", from, query_id),
            NodeMessage::AbandonStateStream { from, to, stream } => write!(f, "AbandonStateStream {from}->{to}: {stream}"),
        }
    }
}
