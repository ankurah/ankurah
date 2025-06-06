use crate::{
    auth::Attested,
    data::{EntityState, Event},
    id::EntityId,
    subscription::SubscriptionId,
    CollectionId, EventFragment, StateFragment,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UpdateId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeUpdateBody {
    /// New events for a subscription
    SubscriptionUpdate { subscription_id: SubscriptionId, items: Vec<SubscriptionUpdateItem>, initial: bool },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SubscriptionUpdateItem {
    Initial { entity_id: EntityId, collection: CollectionId, state: StateFragment },
    Add { entity_id: EntityId, collection: CollectionId, state: StateFragment, event: EventFragment },
    Change { entity_id: EntityId, collection: CollectionId, event: EventFragment },
    // Note: this is not a resultset change, it's a subscription change
    // that means we don't care about removes, because the reactor handles that
}

impl SubscriptionUpdateItem {
    pub fn initial(entity_id: EntityId, collection: CollectionId, state: Attested<EntityState>) -> Self {
        Self::Initial { entity_id, collection, state: state.into() }
    }
    pub fn add(entity_id: EntityId, collection: CollectionId, state: Attested<EntityState>, event: Attested<Event>) -> Self {
        // TODO sanity check to make sure the events are for the same entity
        Self::Add { entity_id, collection, state: state.into(), event: event.into() }
    }
    pub fn change(entity_id: EntityId, collection: CollectionId, event: Attested<Event>) -> Self {
        Self::Change { entity_id, collection, event: event.into() }
    }
    pub fn entity_id(&self) -> EntityId {
        match self {
            Self::Initial { entity_id, .. } => *entity_id,
            Self::Add { entity_id, .. } => *entity_id,
            Self::Change { entity_id, .. } => *entity_id,
        }
    }
    pub fn into_parts(self) -> (EntityId, CollectionId, Option<StateFragment>, Option<EventFragment>) {
        match self {
            Self::Initial { entity_id, collection, state } => (entity_id, collection, Some(state), None),
            Self::Add { entity_id, collection, state, event } => (entity_id, collection, Some(state), Some(event)),
            Self::Change { entity_id, collection, event } => (entity_id, collection, None, Some(event)),
        }
    }
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

impl Default for UpdateId {
    fn default() -> Self { Self::new() }
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
            NodeUpdateBody::SubscriptionUpdate { subscription_id, items, initial } => {
                write!(
                    f,
                    "SubscriptionUpdate {subscription_id} [{}] initial: {initial}",
                    items.iter().map(|i| format!("{}", i)).collect::<Vec<_>>().join(", ")
                )
            }
        }
    }
}

impl std::fmt::Display for SubscriptionUpdateItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionUpdateItem::Initial { entity_id, collection, state } => {
                write!(f, "Initial: {} {} {}", entity_id, collection, state)
            }
            SubscriptionUpdateItem::Add { entity_id, collection, state, event: _ } => {
                write!(f, "Add: {entity_id} {collection} {state}")
            }
            SubscriptionUpdateItem::Change { entity_id, collection, event } => {
                write!(f, "Change: {entity_id} {collection} {event}")
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
