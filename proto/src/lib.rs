pub mod human_id;
pub mod message;
pub mod record;
pub mod record_id;

pub use human_id::*;
pub use message::*;
pub use record::*;
pub use record_id::ID;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use ulid::Ulid;
use wasm_bindgen::prelude::*;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct NodeId(Ulid);

impl From<NodeId> for String {
    fn from(node_id: NodeId) -> Self {
        node_id.0.to_string()
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct RequestId(Ulid);

impl NodeId {
    pub fn new() -> Self {
        Self(Ulid::new())
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
        bucket_name: String,
        predicate: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponseBody {
    // Response to CommitEvents
    CommitComplete,
    // Response to FetchRecords
    Records(Vec<RecordState>),
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
