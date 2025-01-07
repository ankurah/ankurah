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
use std::collections::{BTreeMap, BTreeSet};

use ulid::Ulid;
use uuid::Uuid;
use wasm_bindgen::prelude::*;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct NodeId(Ulid);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "N{}", &id_str[20..])
    }
}

impl From<NodeId> for String {
    fn from(node_id: NodeId) -> Self { node_id.0.to_string() }
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "S-{}", self.0.to_string()) }
}

impl Default for NodeId {
    fn default() -> Self { Self::new() }
}

impl NodeId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl Default for RequestId {
    fn default() -> Self { Self::new() }
}

impl RequestId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl Default for SubscriptionId {
    fn default() -> Self { Self::new() }
}

impl SubscriptionId {
    pub fn new() -> Self { Self(Ulid::new()) }

    /// To be used only for testing
    pub fn test(id: u64) -> Self { Self(Ulid::from_parts(id, 0)) }
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
        write!(f, "Request({}) {}->{} {}", self.id, self.from, self.to, self.body)
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
        write!(f, "Response({}) {}->{} {}", self.request_id, self.from, self.to, self.body)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecordEvent {
    pub id: ID,
    pub bucket_name: String,
    pub record_id: ID,
    pub operations: BTreeMap<String, Vec<Operation>>,
    /// The set of concurrent events (usually only one) which is the precursor of this event
    pub parent: Clock,
}

/// S set of event ids which create a dag of events
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct Clock(BTreeSet<ID>);

impl Clock {
    pub fn new(ids: impl Into<BTreeSet<ID>>) -> Self { Self(ids.into()) }

    pub fn as_slice(&self) -> &BTreeSet<ID> { &self.0 }

    pub fn to_strings(&self) -> Vec<String> { self.0.iter().map(|id| id.to_string()).collect() }

    pub fn from_strings(strings: Vec<String>) -> Result<Self, ulid::DecodeError> {
        let ids = strings
            .into_iter()
            .map(|s| {
                let ulid = Ulid::from_string(&s)?;
                Ok(ID::from_ulid(ulid))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        Ok(Self(ids))
    }

    pub fn insert(&mut self, id: ID) { self.0.insert(id); }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

impl From<Vec<Uuid>> for Clock {
    fn from(uuids: Vec<Uuid>) -> Self {
        let ids = uuids
            .into_iter()
            .map(|uuid| {
                let ulid = Ulid::from(uuid);
                ID::from_ulid(ulid)
            })
            .collect();
        Self(ids)
    }
}

impl From<&Clock> for Vec<Uuid> {
    fn from(clock: &Clock) -> Self {
        clock
            .0
            .iter()
            .map(|id| {
                let ulid: Ulid = (*id).into();
                ulid.into()
            })
            .collect()
    }
}

impl std::fmt::Display for RecordEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordEvent({} {}/{} {} {})",
            self.id,
            self.bucket_name,
            self.record_id,
            self.parent,
            self.operations
                .iter()
                .map(|(backend, ops)| format!("{} => {}b", backend, ops.iter().map(|op| op.diff.len()).sum::<usize>()))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl std::fmt::Display for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "[{}]", self.to_strings().join(", ")) }
}

impl RecordEvent {
    pub fn bucket_name(&self) -> &str { &self.bucket_name }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    pub diff: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct RecordState {
    /// The current accumulated state of the record inclusive of all events up to this point
    pub state_buffers: BTreeMap<String, Vec<u8>>,
    /// The set of concurrent events (usually only one) which have been applied to the record state above
    pub head: Clock,
}

impl std::fmt::Display for RecordState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordState({})",
            self.state_buffers.iter().map(|(backend, buf)| format!("{} => {}b", backend, buf.len())).collect::<Vec<_>>().join(" ")
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeRequestBody {
    // Events to be committed on the remote node
    CommitEvents(Vec<RecordEvent>),
    // Request to fetch records matching a predicate
    FetchRecords { collection: String, predicate: ast::Predicate },
    Subscribe { collection: String, predicate: ast::Predicate },
    Unsubscribe { subscription_id: SubscriptionId },
}

impl std::fmt::Display for NodeRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRequestBody::CommitEvents(events) => {
                write!(f, "CommitEvents [{}]", events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::FetchRecords { collection, predicate } => {
                write!(f, "FetchRecords {collection} {predicate}")
            }
            NodeRequestBody::Subscribe { collection, predicate } => {
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
    Subscribe { initial: Vec<(ID, RecordState)>, subscription_id: SubscriptionId },
    Success,
    Error(String),
}

impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeResponseBody::CommitComplete => write!(f, "CommitComplete"),
            NodeResponseBody::Fetch(records) => {
                write!(f, "Fetch [{}]", records.iter().map(|(id, _)| id.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::Subscribe { initial, subscription_id } => write!(
                f,
                "Subscribe {} initial [{}]",
                subscription_id,
                initial.iter().map(|(id, state)| format!("{} {}", id, state)).collect::<Vec<_>>().join(", ")
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

impl TryFrom<JsValue> for Clock {
    type Error = anyhow::Error;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        if value.is_undefined() || value.is_null() {
            return Ok(Clock::default());
        }
        let ids: Vec<String> = serde_wasm_bindgen::from_value(value).map_err(|e| anyhow::anyhow!("Failed to parse clock: {}", e))?;
        Self::from_strings(ids).map_err(|e| anyhow::anyhow!("Failed to parse clock: {}", e))
    }
}

impl From<&Clock> for JsValue {
    fn from(val: &Clock) -> Self {
        let strings = val.to_strings();
        // This should not be able to fail
        serde_wasm_bindgen::to_value(&strings).expect("Failed to serialize clock")
    }
}
