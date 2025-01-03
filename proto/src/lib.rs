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
use std::{collections::BTreeMap, ops::Deref};

use ulid::Ulid;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct NodeId(Ulid);

impl From<NodeId> for String {
    fn from(node_id: NodeId) -> Self {
        node_id.0.to_string()
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct RequestId(Ulid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(usize);

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

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeRequest {
    pub id: RequestId,
    pub to: NodeId,
    pub from: NodeId,
    pub body: NodeRequestBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResponse {
    pub request_id: RequestId,
    pub from: NodeId,
    pub to: NodeId,
    pub body: NodeResponseBody,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecordEvent {
    pub id: ID,
    pub bucket_name: String,
    pub operations: BTreeMap<String, Vec<Operation>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete,
    // Response to FetchRecords
    Fetch(Vec<RecordState>),
    Subscribe {
        initial: Vec<RecordState>,
        subscription_id: SubscriptionId,
    },
    Success,
    Error(String),
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Presence {
    pub node_id: NodeId,
}

impl Deref for SubscriptionId {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<usize> for SubscriptionId {
    fn eq(&self, other: &usize) -> bool {
        self.0 == *other
    }
}

impl From<usize> for SubscriptionId {
    fn from(value: usize) -> Self {
        Self(value)
    }
}
