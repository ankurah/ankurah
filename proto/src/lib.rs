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

/// Raw context data that can be transmitted between nodes - this may be a bearer token
/// or some other arbitrary data at the discretion of the Policy Agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthData(pub Vec<u8>);

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

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct EventWrapper {
//     pub event: Event,
//     pub attestation: Attestation,
// }

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
pub enum NodeRequestBody {
    // Request that the Events to be committed on the remote node
    CommitEvents { events: Vec<Event> },
    // Request to fetch entities matching a predicate
    Fetch { collection: CollectionId, predicate: ast::Predicate },
    Subscribe { subscription_id: SubscriptionId, collection: CollectionId, predicate: ast::Predicate },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NotificationId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub enum NotificationBody {
    /// New events for a subscription
    SubscriptionUpdate { subscription_id: SubscriptionId, events: Vec<Event> },
    /// Unsubscribe from a subscription
    Unsubscribe { subscription_id: SubscriptionId },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Notification {
    pub id: NotificationId,
    pub from: NodeId,
    pub to: NodeId,
    pub body: NotificationBody,
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
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeMessage {
    Request { auth: AuthData, request: NodeRequest },
    Response(NodeResponse),
    Notification(Notification),
    AckNotification { id: NotificationId },
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
