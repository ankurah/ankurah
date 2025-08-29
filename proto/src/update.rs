use crate::{
    auth::Attested,
    data::{EntityState, Event},
    id::EntityId,
    subscription::PredicateId,
    CollectionId, EventFragment, StateFragment,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UpdateId(Ulid);

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeUpdateBody {
    /// New events for a subscription
    SubscriptionUpdate { items: Vec<SubscriptionUpdateItem>, initialized_predicate: Option<PredicateId> },
}

/// Content of an update - either state, events, or both
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum UpdateContent {
    /// Only state, no events (typically for initial population)
    StateOnly(StateFragment),
    /// Only events, no state (peer already has the state)
    EventOnly(Vec<EventFragment>),
    /// Both state and events (peer needs both)
    StateAndEvent(StateFragment, Vec<EventFragment>),
}

/// How an entity's membership changed for a specific predicate
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum MembershipChange {
    /// First time seeing this entity for this predicate
    Initial,
    /// Entity now matches predicate (wasn't matching before)
    Add,
    /// Entity no longer matches predicate (was matching before)  
    Remove,
}

/// A single entity update with all subscription relevance information
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionUpdateItem {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub content: UpdateContent,
    /// Which predicates this update is relevant to and how
    /// Uses PredicateId for remote subscriptions
    pub predicate_relevance: Vec<(PredicateId, MembershipChange)>,
    /// Whether this entity has an explicit entity-level subscription
    pub entity_subscribed: bool,
}

impl TryFrom<SubscriptionUpdateItem> for Attested<EntityState> {
    type Error = anyhow::Error;
    fn try_from(value: SubscriptionUpdateItem) -> Result<Self, Self::Error> {
        match value.content {
            UpdateContent::StateOnly(state) | UpdateContent::StateAndEvent(state, _) => {
                Ok((value.entity_id, value.collection, state).into())
            }
            UpdateContent::EventOnly(_) => Err(anyhow::anyhow!("Cannot convert event-only update to entity state")),
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
            NodeUpdateBody::SubscriptionUpdate { items } => {
                write!(f, "SubscriptionUpdate [{}]", items.iter().map(|i| format!("{}", i)).collect::<Vec<_>>().join(", "))
            }
        }
    }
}

impl std::fmt::Display for SubscriptionUpdateItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}: ", self.collection, self.entity_id)?;

        match &self.content {
            UpdateContent::StateOnly(state) => write!(f, "State({})", state)?,
            UpdateContent::EventOnly(events) => write!(f, "Events({})", events.len())?,
            UpdateContent::StateAndEvent(state, events) => write!(f, "State+Events({}, {})", state, events.len())?,
        }

        if !self.predicate_relevance.is_empty() {
            write!(f, " predicates:{}", self.predicate_relevance.len())?;
        }

        if self.entity_subscribed {
            write!(f, " [entity-sub]")?;
        }

        Ok(())
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
