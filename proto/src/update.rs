use crate::{
    auth::Attested,
    data::{EntityState, Event},
    id::EntityId,
    subscription::SubscriptionId,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UpdateId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeUpdateBody {
    /// New events for a subscription
    SubscriptionUpdate { subscription_id: SubscriptionId, items: Vec<SubscriptionUpdateItem> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SubscriptionUpdateItem {
    /// LEFT OFF HERE - there's a weird tension between Attested<EntityState> and grouping events / states together.
    /// arguably we don't really need the event and the entitystate to be grouped together
    Initial {
        state: Attested<EntityState>,
    },
    Add {
        state: Attested<EntityState>,
        events: Vec<Attested<Event>>,
    },
    Change {
        events: Vec<Attested<Event>>,
    },
    // Note: this is not a resultset change, it's a subscription change
    // that means we don't care about removes, because the reactor handles that
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
            NodeUpdateBody::SubscriptionUpdate { subscription_id, items } => {
                write!(
                    f,
                    "SubscriptionUpdate {subscription_id} [{}]",
                    items.iter().map(|i| format!("{}", i)).collect::<Vec<_>>().join(", ")
                )
            }
        }
    }
}

impl std::fmt::Display for SubscriptionUpdateItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionUpdateItem::Initial { state } => write!(f, "Initial: {}", state),
            SubscriptionUpdateItem::Add { state, events } => {
                write!(f, "Add: {} {}", state, events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
            }
            SubscriptionUpdateItem::Change { events } => {
                write!(f, "Change: {}", events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
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
