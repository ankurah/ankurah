pub mod clock;
pub mod entity_id;
pub mod error;
pub mod human_id;
pub mod impls;
pub use clock::Clock;
pub use entity_id::ID;
pub use human_id::*;

use ankql::ast;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize, Hash)]
pub struct NodeId(Ulid);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CollectionId(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TransactionId(Ulid);

/// Raw context data that can be transmitted between nodes - this may be a bearer token
/// or some other arbitrary data at the discretion of the Policy Agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthData(pub Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: RequestId,
    pub to: NodeId,
    pub from: NodeId,
    pub body: RequestBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Update {
    pub id: UpdateId,
    pub from: NodeId,
    pub to: NodeId,
    pub body: UpdateBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub request_id: RequestId,
    pub from: NodeId,
    pub to: NodeId,
    pub body: ResponseBody,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize, Hash)]
pub struct RequestId(Ulid);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: ID,
    pub collection: CollectionId,
    pub entity_id: ID,
    pub operations: BTreeMap<String, Vec<Operation>>,
    /// The set of concurrent events (usually only one) which is the precursor of this event
    pub parent: Clock,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attested<T> {
    pub payload: T,
    pub attestations: Vec<Attestation>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    pub diff: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: BTreeMap<String, Vec<u8>>,
    /// The set of concurrent events (usually only one) which have been applied to the entity state above
    pub head: Clock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestBody {
    // Request that the Events to be committed on the remote node
    CommitTransaction { id: TransactionId, events: Vec<Attested<Event>> },
    // Request to fetch entities matching a predicate
    Fetch { collection: CollectionId, predicate: ast::Predicate },
    Subscribe { subscription_id: SubscriptionId, collection: CollectionId, predicate: ast::Predicate },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum UpdateBody {
    /// New events for a subscription
    SubscriptionUpdate { subscription_id: SubscriptionId, events: Vec<Attested<Event>> },
    /// Unsubscribe from a subscription
    Unsubscribe { subscription_id: SubscriptionId },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UpdateId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateAck {
    pub id: UpdateId,
    pub from: NodeId,
    pub to: NodeId,
    pub body: UpdateAckBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum UpdateAckBody {
    Success,
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseBody {
    // Response to CommitEvents
    CommitComplete,
    Fetch(Vec<(ID, State)>),
    Subscribe { initial: Vec<(ID, State)>, subscription_id: SubscriptionId },
    Success,
    Error(String),
}
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeMessage {
    Request { auth: AuthData, request: Request },
    Response(Response),
    Update(Update),
    UpdateAck(UpdateAck),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Presence(Presence),
    PeerMessage(NodeMessage),
    // TODO RPC messages
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Presence {
    pub node_id: NodeId,
    pub durable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attestation(pub Vec<u8>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    // TODO
}
