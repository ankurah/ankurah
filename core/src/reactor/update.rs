use crate::{
    changes::{ChangeSet, ItemChange},
    entity::Entity,
};
use ankurah_proto::{self as proto, Attested, Event};

/// Describes how an entity's membership changed for a specific predicate
#[derive(Debug, Clone, PartialEq)]
pub enum MembershipChange {
    /// First time seeing this entity for this predicate
    Initial,
    /// Entity now matches predicate (wasn't matching before)
    Add,
    /// Entity no longer matches predicate (was matching before)
    Remove,
    // Note: No "Update" variant - if entity still matches and changed,
    // it's included in the ReactorUpdateItem but not as a membership change
}

/// Update from the reactor that supports both single and multi-predicate subscriptions
#[derive(Debug, Clone, PartialEq)]
pub struct ReactorUpdate<E = Entity, Ev = Attested<Event>> {
    /// All entities that changed, with their relevance information
    pub items: Vec<ReactorUpdateItem<E, Ev>>,
}

/// A single entity update with all relevance information
#[derive(Debug, Clone, PartialEq)]
pub struct ReactorUpdateItem<E = Entity, Ev = Attested<Event>> {
    /// The entity that changed
    pub entity: E,
    /// Events that caused this update
    pub events: Vec<Ev>,
    /// Whether this entity is explicitly subscribed (entity-level subscription)
    pub entity_subscribed: bool,
    /// Which predicates this update is relevant to and how
    /// Empty if only relevant due to entity_subscribed
    pub predicate_relevance: Vec<(proto::PredicateId, MembershipChange)>,
}

impl<E, Ev: Clone> ReactorUpdateItem<E, Ev> {
    /// Check if this item represents any membership change
    pub fn has_membership_change(&self) -> bool { !self.predicate_relevance.is_empty() }

    /// Check if this is purely an entity subscription update
    pub fn is_entity_only(&self) -> bool { self.entity_subscribed && self.predicate_relevance.is_empty() }
}

// Note: ReactorUpdate to ChangeSet<Entity> conversion removed since Entity doesn't implement View
// ReactorUpdate should be converted to ChangeSet<R> at the LiveQuery level instead
