use crate::{id::EntityId, RequestId, StateFrame};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UpdateId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeUpdateBody {
    /// Continuation of Fetch or SubscribeQuery; route by the original request
    /// even if this frame outruns its metadata response. Apply only after frame 0
    /// validates. UpdateAck is routing receipt here; ContinueState returns credit.
    StateFrame { request_id: RequestId, frame: StateFrame },
}

/// An update from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeUpdate {
    pub id: UpdateId,
    pub from: EntityId,
    pub to: EntityId,
    pub body: NodeUpdateBody,
}

/// An acknowledgement of an update from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeUpdateAck {
    pub id: UpdateId,
    pub from: EntityId,
    pub to: EntityId,
    pub body: NodeUpdateAckBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeUpdateAckBody {
    Success,
    Error(String),
}

impl std::fmt::Display for UpdateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "N{}", &id_str[20..])
    }
}

impl std::fmt::Display for NodeUpdateAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "UpdateAck({})", self.id) }
}

impl Default for UpdateId {
    fn default() -> Self { Self::new() }
}

impl UpdateId {
    pub fn new() -> Self { Self(Ulid::new()) }
}
impl std::fmt::Display for NodeUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Update {} from {}->{}: {}", self.id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for NodeUpdateBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeUpdateBody::StateFrame { request_id, frame } => {
                write!(f, "StateFrame {request_id} {} sequence:{}", frame.stream, frame.sequence)
            }
        }
    }
}

impl std::fmt::Display for NodeUpdateAckBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeUpdateAckBody::Success => write!(f, "Success"),
            NodeUpdateAckBody::Error(e) => write!(f, "Error: {e}"),
        }
    }
}
