use crate::reactor::comparison_index::ComparisonIndex;
use crate::reactor::{AbstractEntity, ReactorSubscriptionId};
use ankurah_proto as proto;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::reactor::candidate_changes::CandidateChanges;

pub struct WatcherSet {
    /// Each property path has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as change)
    index_watchers: HashMap<(proto::CollectionId, PropertyPath), ComparisonIndex<(ReactorSubscriptionId, proto::QueryId)>>,
    /// The set of watchers who want to be notified of any changes to a given collection
    wildcard_watchers: HashMap<proto::CollectionId, HashSet<(ReactorSubscriptionId, proto::QueryId)>>,
    /// Index of subscriptions that presently match each entity, either by predicate or by entity subscription.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: HashMap<ankurah_proto::EntityId, HashSet<EntityWatcherId>>,
}

impl WatcherSet {
    pub fn new() -> Self { Self { index_watchers: HashMap::new(), wildcard_watchers: HashMap::new(), entity_watchers: HashMap::new() } }
    /// Accumulate interested watchers for an entity change into CandidateChanges
    pub fn accumulate_interested_watchers<E: AbstractEntity, C>(
        &self,
        entity: &E,
        offset: usize,
        changes_arc: &Arc<Vec<C>>,
        candidates_by_sub: &mut HashMap<ReactorSubscriptionId, CandidateChanges<C>>,
    ) {
        let entity_id = AbstractEntity::id(entity);

        // Find subscriptions interested based on index watchers
        for ((collection_id, property_path), index_ref) in &self.index_watchers {
            if *collection_id == AbstractEntity::collection(entity) {
                // Extract value at the property path (handles both simple fields and JSON paths)
                if let Some(value) = property_path.extract_value(entity) {
                    for (subscription_id, query_id) in index_ref.find_matching(value) {
                        candidates_by_sub
                            .entry(subscription_id)
                            .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                            .add_query(query_id, offset);
                    }
                }
            }
        }

        // Check wildcard watchers for this collection
        if let Some(watchers) = self.wildcard_watchers.get(&AbstractEntity::collection(entity)) {
            for (subscription_id, query_id) in watchers.iter() {
                candidates_by_sub
                    .entry(*subscription_id)
                    .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                    .add_query(*query_id, offset);
            }
        }

        // Check entity watchers
        if let Some(subscription_ids) = self.entity_watchers.get(entity_id) {
            for sub_id in subscription_ids.iter() {
                match sub_id {
                    EntityWatcherId::Predicate(subscription_id, query_id) => {
                        candidates_by_sub
                            .entry(*subscription_id)
                            .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                            .add_query(*query_id, offset);
                    }
                    EntityWatcherId::Subscription(subscription_id) => {
                        candidates_by_sub
                            .entry(*subscription_id)
                            .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                            .add_entity(offset);
                    }
                }
            }
        }
    }

    /// Apply a watcher change
    pub fn apply_watcher_change(&mut self, change: WatcherChange) {
        match change {
            WatcherChange::Add { entity_id, subscription_id, query_id } => {
                self.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Predicate(subscription_id, query_id));
            }
            WatcherChange::Remove { entity_id, subscription_id, query_id } => {
                if let Some(watchers) = self.entity_watchers.get_mut(&entity_id) {
                    watchers.remove(&EntityWatcherId::Predicate(subscription_id, query_id));
                    if watchers.is_empty() {
                        self.entity_watchers.remove(&entity_id);
                    }
                }
            }
        }
    }

    /// Add entity subscription watcher for a subscription
    pub fn add_entity_subscription(&mut self, subscription_id: ReactorSubscriptionId, entity_id: proto::EntityId) {
        self.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Subscription(subscription_id));
    }

    /// Remove entity subscription watcher for a subscription
    pub fn remove_entity_subscription(&mut self, subscription_id: ReactorSubscriptionId, entity_id: proto::EntityId) {
        if let Some(watchers) = self.entity_watchers.get_mut(&entity_id) {
            watchers.remove(&EntityWatcherId::Subscription(subscription_id));
            if watchers.is_empty() {
                self.entity_watchers.remove(&entity_id);
            }
        }
    }

    /// Remove all entity subscription watchers for multiple entities
    pub fn remove_entity_subscriptions(
        &mut self,
        subscription_id: ReactorSubscriptionId,
        entity_ids: impl IntoIterator<Item = proto::EntityId>,
    ) {
        for entity_id in entity_ids {
            self.remove_entity_subscription(subscription_id, entity_id);
        }
    }

    /// Clear all entity watchers
    pub fn clear_entity_watchers(&mut self) { self.entity_watchers.clear(); }

    /// Get references to internal data for debugging
    pub fn debug_data(
        &self,
    ) -> (
        &HashMap<(proto::CollectionId, PropertyPath), ComparisonIndex<(ReactorSubscriptionId, proto::QueryId)>>,
        &HashMap<proto::CollectionId, HashSet<(ReactorSubscriptionId, proto::QueryId)>>,
        &HashMap<ankurah_proto::EntityId, HashSet<EntityWatcherId>>,
    ) {
        (&self.index_watchers, &self.wildcard_watchers, &self.entity_watchers)
    }

    /// Add predicate entity watcher for multiple entities
    pub fn add_predicate_entity_watchers(
        &mut self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        entity_ids: impl IntoIterator<Item = proto::EntityId>,
    ) {
        for entity_id in entity_ids {
            self.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Predicate(subscription_id, query_id));
        }
    }

    /// Remove predicate entity watchers for entities that no longer match
    /// Returns list of entity_ids that should be removed from the entity cache
    pub fn cleanup_removed_predicate_watchers(
        &mut self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        removed_entities: &[proto::EntityId],
    ) {
        for entity_id in removed_entities {
            if let Some(entity_watcher) = self.entity_watchers.get_mut(entity_id) {
                entity_watcher.remove(&EntityWatcherId::Predicate(subscription_id, query_id));
            }
        }
    }
    pub fn recurse_predicate_watchers(
        &mut self,
        collection_id: &proto::CollectionId,
        predicate: &ankql::ast::Predicate,
        watcher_id: (ReactorSubscriptionId, proto::QueryId), // Should this be a tuple of (subscription_id, query_id) or just subscription_id?
        op: WatcherOp,
    ) {
        use ankql::ast::{Expr, Predicate};
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                if let (Expr::Path(path), Expr::Literal(literal)) | (Expr::Literal(literal), Expr::Path(path)) = (&**left, &**right) {
                    // Use the full path for indexing.
                    // For simple paths like `name`, this is just "name".
                    // For JSON paths like `context.task_id`, this is "context.task_id".
                    // accumulate_interested_watchers will extract the value at this path.
                    let property_path = PropertyPath::from_path(path);
                    let index = self.index_watchers.entry((collection_id.clone(), property_path)).or_default();

                    match op {
                        WatcherOp::Add => {
                            index.add((*literal).clone(), operator.clone(), watcher_id);
                        }
                        WatcherOp::Remove => {
                            index.remove((*literal).clone(), operator.clone(), watcher_id);
                        }
                    }
                } else {
                    // warn!("Unsupported predicate: {:?}", predicate);
                }
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.recurse_predicate_watchers(collection_id, left, watcher_id, op);
                self.recurse_predicate_watchers(collection_id, right, watcher_id, op);
            }
            Predicate::Not(pred) => {
                self.recurse_predicate_watchers(collection_id, pred, watcher_id, op);
            }
            Predicate::IsNull(_) => {
                unimplemented!("Not sure how to implement this")
            }
            Predicate::True => {
                let set = self.wildcard_watchers.entry(collection_id.clone()).or_default();

                match op {
                    WatcherOp::Add => {
                        set.insert(watcher_id);
                    }
                    WatcherOp::Remove => {
                        set.remove(&watcher_id);
                    }
                }
            }
            Predicate::False => {
                unimplemented!("Not sure how to implement this")
            }
            // Placeholder should be transformed before reaching this point
            Predicate::Placeholder => {
                // This should not happen in normal operation as Placeholder should be transformed
                // before being used in subscriptions
                unimplemented!("Placeholder should be transformed before reactor processing")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum EntityWatcherId {
    Predicate(ReactorSubscriptionId, proto::QueryId),
    Subscription(ReactorSubscriptionId),
}

impl EntityWatcherId {
    pub fn subscription_id(&self) -> ReactorSubscriptionId {
        match self {
            EntityWatcherId::Predicate(sub_id, _) => *sub_id,
            EntityWatcherId::Subscription(sub_id) => *sub_id,
        }
    }
}

/// A path to a property value, supporting both simple fields and JSON sub-paths.
/// Used by the watcher system to index and extract values for comparison.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyPath {
    /// The root property name (e.g., "context" for "context.task_id")
    root: String,
    /// The sub-path within the property (e.g., ["task_id"] for "context.task_id"), empty for simple fields
    sub_path: Vec<String>,
}

impl PropertyPath {
    /// Create a PropertyPath from a PathExpr
    pub fn from_path(path: &ankql::ast::PathExpr) -> Self {
        let steps = &path.steps;
        Self { root: steps[0].clone(), sub_path: steps[1..].to_vec() }
    }

    /// Get the root property name
    pub fn root(&self) -> &str { &self.root }

    /// Check if this is a simple field (no sub-path)
    pub fn is_simple(&self) -> bool { self.sub_path.is_empty() }

    /// Extract the value at this path from an entity.
    /// For JSON paths, keeps the value wrapped as Value::Json to match index keys.
    pub fn extract_value<E: super::AbstractEntity>(&self, entity: &E) -> Option<crate::value::Value> {
        use crate::value::Value;

        let root_value = E::value(entity, &self.root)?;
        if self.sub_path.is_empty() {
            Some(root_value)
        } else {
            // Extract nested value from JSON, keeping it wrapped as Value::Json
            // This matches how literals are stored in the comparison index after TypeResolver
            match root_value {
                Value::Json(json) => {
                    let mut current = &json;
                    for key in &self.sub_path {
                        current = current.get(key)?;
                    }
                    // Keep as Value::Json to match index keys
                    Some(Value::Json(current.clone()))
                }
                Value::Binary(bytes) => {
                    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
                    let mut current = &json;
                    for key in &self.sub_path {
                        current = current.get(key)?;
                    }
                    // Keep as Value::Json to match index keys
                    Some(Value::Json(current.clone()))
                }
                _ => None, // Can't traverse into non-JSON types
            }
        }
    }
}

impl From<&str> for PropertyPath {
    fn from(val: &str) -> Self { PropertyPath { root: val.to_string(), sub_path: Vec::new() } }
}

#[derive(Debug, Copy, Clone)]
pub enum WatcherOp {
    Add,
    Remove,
}

/// Represents a change to entity watchers that needs to be applied to the WatcherSet
#[derive(Debug, Clone)]
pub enum WatcherChange {
    /// Add an entity watcher (notify_change decides EntityWatcherId variant)
    Add { entity_id: proto::EntityId, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId },
    /// Remove an entity watcher
    Remove { entity_id: proto::EntityId, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId },
}

impl WatcherChange {
    /// Create a watcher change for adding an entity watcher
    pub fn add(entity_id: proto::EntityId, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId) -> Self {
        Self::Add { entity_id, subscription_id, query_id }
    }

    /// Create a watcher change for removing an entity watcher
    pub fn remove(entity_id: proto::EntityId, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId) -> Self {
        Self::Remove { entity_id, subscription_id, query_id }
    }
}
