pub mod human_id;
pub mod id;
pub mod message;

use ankql::ast;
pub use human_id::*;
pub use id::ID;
pub use message::*;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use ulid::Ulid;
use uuid::Uuid;
use wasm_bindgen::prelude::*;

#[derive(Debug)]
pub enum DecodeError {
    NotStringValue,
    InvalidBase64(base64::DecodeError),
    InvalidLength,
    InvalidUlid,
    InvalidFallback,
    Other(anyhow::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::NotStringValue => write!(f, "Not a string value"),
            DecodeError::InvalidBase64(e) => write!(f, "Invalid Base64: {}", e),
            DecodeError::InvalidLength => write!(f, "Invalid Length"),
            DecodeError::InvalidUlid => write!(f, "Invalid ULID"),
            DecodeError::InvalidFallback => write!(f, "Invalid Fallback"),
            DecodeError::Other(e) => write!(f, "Other: {}", e),
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CollectionId(String);
impl From<&str> for CollectionId {
    fn from(val: &str) -> Self { CollectionId(val.to_string()) }
}

impl From<CollectionId> for String {
    fn from(collection_id: CollectionId) -> Self { collection_id.0 }
}
impl AsRef<str> for CollectionId {
    fn as_ref(&self) -> &str { &self.0 }
}

impl CollectionId {
    pub fn as_str(&self) -> &str { &self.0 }
}

impl std::fmt::Display for CollectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize, Hash)]
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
    pub to: ID,
    pub from: ID,
    pub body: NodeRequestBody,
}

impl std::fmt::Display for NodeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Request {} from {}->{}: {}", self.id, self.from, self.to, self.body)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResponse {
    pub request_id: RequestId,
    pub from: ID,
    pub to: ID,
    pub body: NodeResponseBody,
}

impl std::fmt::Display for NodeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Response({}) {}->{} {}", self.request_id, self.from, self.to, self.body)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: ID,
    pub collection: CollectionId,
    pub entity_id: ID,
    pub operations: BTreeMap<String, Vec<Operation>>,
    /// The set of concurrent events (usually only one) which is the precursor of this event
    pub parent: Clock,
}

/// S set of event ids which create a dag of events
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct Clock(BTreeSet<ID>);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClockOrdering {
    Parent,
    Child,
    Sibling,
    Unrelated,
}

impl Clock {
    pub fn new(ids: impl Into<BTreeSet<ID>>) -> Self { Self(ids.into()) }

    pub fn as_slice(&self) -> &BTreeSet<ID> { &self.0 }

    pub fn to_strings(&self) -> Vec<String> { self.0.iter().map(|id| id.to_string()).collect() }

    pub fn from_strings(strings: Vec<String>) -> Result<Self, DecodeError> {
        let ids = strings.into_iter().map(|s| s.try_into()).collect::<Result<BTreeSet<_>, _>>()?;
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

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event({} {}/{} {} {})",
            self.id,
            self.collection,
            self.entity_id,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    pub diff: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: BTreeMap<String, Vec<u8>>,
    /// The set of concurrent events (usually only one) which have most recently been applied to the entity state above
    pub head: Clock,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State(clock {} buffers {})",
            self.head,
            self.state_buffers.iter().map(|(backend, buf)| format!("{} => {}b", backend, buf.len())).collect::<Vec<_>>().join(" ")
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeRequestBody {
    // Events to be committed on the remote node
    CommitEvents(Vec<Event>),
    // Request to fetch entities matching a predicate
    Fetch { collection: CollectionId, predicate: ast::Predicate },
    Subscribe { subscription_id: SubscriptionId, collection: CollectionId, predicate: ast::Predicate },
    Unsubscribe { subscription_id: SubscriptionId },
}

impl std::fmt::Display for NodeRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRequestBody::CommitEvents(events) => {
                write!(f, "CommitEvents [{}]", events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::Fetch { collection, predicate } => {
                write!(f, "Fetch {collection} {predicate}")
            }
            NodeRequestBody::Subscribe { subscription_id, collection, predicate } => {
                write!(f, "Subscribe {subscription_id} {collection} {predicate}")
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
    Fetch(Vec<(ID, State)>),
    Subscribe { initial: Vec<(ID, State)>, subscription_id: SubscriptionId },
    Success,
    Error(String),
}

impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeResponseBody::CommitComplete => write!(f, "CommitComplete"),
            NodeResponseBody::Fetch(tuples) => {
                write!(f, "Fetch [{}]", tuples.iter().map(|(id, _)| id.to_string()).collect::<Vec<_>>().join(", "))
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
pub enum NodeMessage {
    Request(NodeRequest),
    Response(NodeResponse),
}

impl std::fmt::Display for NodeMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeMessage::Request(request) => write!(f, "Request: {}", request),
            NodeMessage::Response(response) => write!(f, "Response: {}", response),
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Presence(Presence),
    PeerMessage(NodeMessage),
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Presence(presence) => write!(f, "Presence: {}", presence),
            Message::PeerMessage(node_message) => write!(f, "PeerMessage: {}", node_message),
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: ID,
    pub durable: bool,
}

impl std::fmt::Display for Presence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Presence({} {})", self.node_id, self.durable) }
}

impl TryFrom<JsValue> for Clock {
    type Error = DecodeError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        if value.is_undefined() || value.is_null() {
            return Ok(Clock::default());
        }
        let ids: Vec<String> =
            serde_wasm_bindgen::from_value(value).map_err(|e| DecodeError::Other(anyhow::anyhow!("Failed to parse clock: {}", e)))?;
        Self::from_strings(ids)
    }
}

impl From<&Clock> for JsValue {
    fn from(val: &Clock) -> Self {
        let strings = val.to_strings();
        // This should not be able to fail
        serde_wasm_bindgen::to_value(&strings).expect("Failed to serialize clock")
    }
}
