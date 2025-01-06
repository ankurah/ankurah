pub mod human_id;
pub mod message;
// pub mod record;
pub mod record_id;

pub use human_id::*;
pub use message::*;
// pub use record::*;
use ankql::ast;
pub use record_id::ID;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use ulid::Ulid;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct NodeId(Ulid);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "N{}", &id_str[20..])
    }
}

impl From<NodeId> for String {
    fn from(node_id: NodeId) -> Self {
        node_id.0.to_string()
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct RequestId(Ulid);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "R{}", &id_str[20..])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(Ulid);

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S-{}", self.0.to_string())
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }
}

impl SubscriptionId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// To be used only for testing
    pub fn test(id: u64) -> Self {
        Self(Ulid::from_parts(id, 0))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeRequest {
    pub id: RequestId,
    pub to: NodeId,
    pub from: NodeId,
    pub body: NodeRequestBody,
}

impl std::fmt::Display for NodeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Request({}) {}->{} {}",
            self.id, self.from, self.to, self.body
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResponse {
    pub request_id: RequestId,
    pub from: NodeId,
    pub to: NodeId,
    pub body: NodeResponseBody,
}

impl std::fmt::Display for NodeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Response({}) {}->{} {}",
            self.request_id, self.from, self.to, self.body
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecordEvent {
    pub id: ID,
    pub bucket_name: String,
    pub operations: BTreeMap<String, Vec<Operation>>,
}

impl std::fmt::Display for RecordEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordEvent({} {} {})",
            self.bucket_name,
            self.id,
            self.operations
                .iter()
                .map(|(backend, ops)| format!(
                    "{} => {} bytes",
                    backend,
                    ops.iter().map(|op| op.diff.len()).sum::<usize>()
                ))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl RecordEvent {
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn id(&self) -> ID {
        self.id
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    pub diff: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecordState {
    pub state_buffers: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeRequestBody {
    // Events to be committed on the remote node
    CommitEvents(Vec<RecordEvent>),
    // Request to fetch records matching a predicate
    FetchRecords {
        collection: String,
        predicate: ast::Predicate,
    },
    Subscribe {
        collection: String,
        predicate: ast::Predicate,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

impl std::fmt::Display for NodeRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRequestBody::CommitEvents(events) => {
                write!(
                    f,
                    "CommitEvents [{}]",
                    events
                        .iter()
                        .map(|e| format!("{}", e))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            NodeRequestBody::FetchRecords {
                collection,
                predicate,
            } => {
                write!(f, "FetchRecords {collection} {predicate}")
            }
            NodeRequestBody::Subscribe {
                collection,
                predicate,
            } => {
                write!(f, "Subscribe {collection} {predicate}")
            }
            NodeRequestBody::Unsubscribe { subscription_id } => {
                write!(f, "Unsubscribe {subscription_id}")
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete,
    // Response to FetchRecords
    Fetch(Vec<(ID, RecordState)>),
    Subscribe {
        initial: Vec<(ID, RecordState)>,
        subscription_id: SubscriptionId,
    },
    Success,
    Error(String),
}

impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeResponseBody::CommitComplete => write!(f, "CommitComplete"),
            NodeResponseBody::Fetch(records) => write!(
                f,
                "Fetch [{}]",
                records
                    .iter()
                    .map(|(id, _)| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            NodeResponseBody::Subscribe {
                initial,
                subscription_id,
            } => write!(
                f,
                "Subscribe {} [{}]",
                subscription_id,
                initial
                    .iter()
                    .map(|(id, _)| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            NodeResponseBody::Success => write!(f, "Success"),
            NodeResponseBody::Error(e) => write!(f, "Error: {e}"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Request(NodeRequest),
    Response(NodeResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    Presence(Presence),
    PeerMessage(PeerMessage),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    Presence(Presence),
    PeerMessage(PeerMessage),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: NodeId,
    pub durable: bool,
}
