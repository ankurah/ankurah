use crate::{auth::Attested, data::Event, id::ID, subscription::SubscriptionId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UpdateId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub enum UpdateBody {
    /// New events for a subscription
    SubscriptionUpdate { subscription_id: SubscriptionId, events: Vec<Attested<Event>> },
    /// Unsubscribe from a subscription
    Unsubscribe { subscription_id: SubscriptionId },
}

/// An update from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeUpdate {
    pub id: UpdateId,
    pub from: ID,
    pub to: ID,
    pub body: UpdateBody,
}

/// An acknowledgement of an update from one node to another
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeUpdateAck {
    pub id: UpdateId,
    pub from: ID,
    pub to: ID,
    pub body: UpdateAckBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum UpdateAckBody {
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

impl UpdateId {
    pub fn new() -> Self { Self(Ulid::new()) }
}
impl std::fmt::Display for NodeUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Update {} from {}->{}: {}", self.id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for UpdateBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateBody::SubscriptionUpdate { subscription_id, events } => {
                write!(
                    f,
                    "SubscriptionUpdate {subscription_id} [{}]",
                    events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ")
                )
            }
            UpdateBody::Unsubscribe { subscription_id } => {
                write!(f, "Unsubscribe {subscription_id}")
            }
        }
    }
}

impl std::fmt::Display for UpdateAckBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateAckBody::Success => write!(f, "Success"),
            UpdateAckBody::Error(e) => write!(f, "Error: {e}"),
        }
    }
}
