use ankql::ast;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    auth::Attested,
    collection::CollectionId,
    data::{Event, State},
    id::ID,
    subscription::SubscriptionId,
    transaction::TransactionId,
};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize, Hash, Default)]
pub struct RequestId(Ulid);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "R{}", &id_str[20..])
    }
}

impl RequestId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

/// A request from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeRequest {
    pub id: RequestId,
    pub to: ID,
    pub from: ID,
    pub body: NodeRequestBody,
}

/// The body of a request from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeRequestBody {
    // Request that the Events to be committed on the remote node
    CommitTransaction { id: TransactionId, events: Vec<Attested<Event>> },
    // Request to fetch entities matching a predicate
    Fetch { collection: CollectionId, predicate: ast::Predicate },
    Subscribe { subscription_id: SubscriptionId, collection: CollectionId, predicate: ast::Predicate },
}

/// A response from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResponse {
    pub request_id: RequestId,
    pub from: ID,
    pub to: ID,
    pub body: NodeResponseBody,
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

impl std::fmt::Display for NodeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Request {} from {}->{}: {}", self.id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for NodeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Response({}) {}->{} {}", self.request_id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for NodeRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRequestBody::CommitTransaction { id, events } => {
                write!(f, "CommitTransaction {id} [{}]", events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::Fetch { collection, predicate } => {
                write!(f, "Fetch {collection} {predicate}")
            }
            NodeRequestBody::Subscribe { subscription_id, collection, predicate } => {
                write!(f, "Subscribe {subscription_id} {collection} {predicate}")
            }
        }
    }
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
