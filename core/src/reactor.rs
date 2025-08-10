use super::comparison_index::ComparisonIndex;

use crate::{changes::ChangeSet, entity::Entity, node::TNodeErased};
use ankurah_proto::{self as proto, Attested, EntityState, Event};
use ankurah_signals::{
    broadcast::{Broadcast, BroadcastId, Listener, ListenerGuard},
    porcelain::subscribe::{SignalGuard, SubscribeListener},
    Signal, Subscribe,
};
use std::collections::{HashMap, HashSet};

use std::sync::{Arc, Mutex};
use ulid::Ulid;

use crate::changes::{EntityChange, ItemChange};
use crate::collectionset::CollectionSet;
use crate::entity::WeakEntitySet;
use crate::policy::PolicyAgent;
use crate::resultset::ResultSet;
use crate::retrieval::LocalRetriever;
use crate::storage::StorageEngine;
use crate::util::safemap::SafeMap;
use crate::util::safeset::SafeSet;
use crate::value::Value;
use ankql::ast;
use ankql::selection::filter::Filterable;

#[cfg(feature = "instrument")]
use tracing::instrument;
use tracing::{debug, warn};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);

impl From<&str> for FieldId {
    fn from(val: &str) -> Self { FieldId(val.to_string()) }
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of entities
pub struct Reactor<SE, PA> {
    /// Current subscriptions
    subscriptions: SafeMap<ReactorSubId, Arc<Subscription>>,
    predicate_subscription_map: SafeMap<proto::PredicateId, ReactorSubId>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as change
    index_watchers: SafeMap<(proto::CollectionId, FieldId), Arc<std::sync::RwLock<ComparisonIndex>>>,
    /// The set of watchers who want to be notified of any changes to a given collection
    wildcard_watchers: SafeMap<proto::CollectionId, Arc<std::sync::RwLock<SafeSet<SubscriptionPredicateId>>>>,
    /// Index of subscriptions that presently match each entity.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: SafeMap<ankurah_proto::EntityId, HashSet<SubscriptionPredicateId>>,
    /// Reference to the storage engine
    collections: CollectionSet<SE>,

    entityset: WeakEntitySet,
    // Weak reference to the node
    // node: OnceCell<WeakNode<PA>>,
    _policy_agent: PA,
}

#[derive(Debug, Copy, Clone)]
enum WatcherOp {
    Add,
    Remove,
}

impl<SE, PA> Reactor<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(collections: CollectionSet<SE>, entityset: WeakEntitySet, policy_agent: PA) -> Arc<Self> {
        Arc::new(Self {
            subscriptions: SafeMap::new(),
            predicate_subscription_map: SafeMap::new(),
            index_watchers: SafeMap::new(),
            wildcard_watchers: SafeMap::new(),
            entity_watchers: SafeMap::new(),
            collections,
            entityset,
            _policy_agent: policy_agent,
            // node: OnceCell::new(),
        })
    }

    /// Create a new subscription container
    pub fn subscribe(&self) -> Arc<Subscription> {
        // Create subscription
        let subscription = Subscription::new();

        // Register the subscription
        self.subscriptions.insert(subscription.id, subscription.clone());

        subscription
    }

    fn manage_watchers_recurse(
        &self,
        collection_id: &proto::CollectionId,
        predicate: &ast::Predicate,
        sub_id: proto::PredicateId,
        predicate_id: proto::PredicateId,
        op: WatcherOp,
    ) {
        use ankql::ast::{Expr, Identifier, Predicate};
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                if let (Expr::Identifier(field), Expr::Literal(literal)) | (Expr::Literal(literal), Expr::Identifier(field)) =
                    (&**left, &**right)
                {
                    let field_name = match field {
                        Identifier::Property(name) => name.clone(),
                        Identifier::CollectionProperty(_, name) => name.clone(),
                    };

                    let field_id = FieldId(field_name);
                    let sub_pred_id = (sub_id, predicate_id);
                    match op {
                        WatcherOp::Add => {
                            let index = self.index_watchers.get_or_default((collection_id.clone(), field_id));
                            index.write().unwrap().add((*literal).clone(), operator.clone(), sub_pred_id);
                        }
                        WatcherOp::Remove => {
                            let index = self.index_watchers.get_or_default((collection_id.clone(), field_id));
                            index.write().unwrap().remove((*literal).clone(), operator.clone(), sub_pred_id);
                        }
                    }
                } else {
                    // warn!("Unsupported predicate: {:?}", predicate);
                }
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.manage_watchers_recurse(collection_id, left, sub_id, predicate_id, op);
                self.manage_watchers_recurse(collection_id, right, sub_id, predicate_id, op);
            }
            Predicate::Not(pred) => {
                self.manage_watchers_recurse(collection_id, pred, sub_id, predicate_id, op);
            }
            Predicate::IsNull(_) => {
                unimplemented!("Not sure how to implement this")
            }
            Predicate::True => {
                let set = self.wildcard_watchers.get_or_default(collection_id.clone());
                let sub_pred_id = (sub_id, predicate_id);
                match op {
                    WatcherOp::Add => {
                        set.write().unwrap().insert(sub_pred_id);
                    }
                    WatcherOp::Remove => {
                        set.write().unwrap().remove(&sub_pred_id);
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

    /// Remove a subscription and clean up its watchers
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(sub_id = %sub_id)))]
    pub(crate) fn unsubscribe(&self, sub_id: proto::PredicateId) {
        if let Some(sub) = self.subscriptions.remove(&sub_id) {
            // Remove all predicates from watchers
            let predicates = sub.predicates.lock().unwrap();
            for (predicate_id, predicate_state) in predicates.iter() {
                // Remove from index watchers
                self.manage_watchers_recurse(&sub.collection_id, &predicate_state.predicate, sub_id, *predicate_id, WatcherOp::Remove);

                // Remove from entity watchers using predicate's matching entities
                for entity in predicate_state.matching_entities.iter() {
                    self.entity_watchers.set_remove(&entity.id, &(sub_id, *predicate_id));
                }
            }
        }
    }

    /// Update entity watchers when an entity's matching status changes
    fn update_entity_watchers(&self, entity: &Entity, matching: bool, sub_pred_id: SubscriptionPredicateId) {
        // This is handled by update_predicate_matching_entities
        // We just need to update the global entity watchers
        if matching {
            self.entity_watchers.set_insert(entity.id, sub_pred_id);
        } else {
            self.entity_watchers.set_remove(&entity.id, &sub_pred_id);
        }
    }

    /// Update predicate matching entities when an entity's matching status changes
    fn update_predicate_matching_entities(
        &self,
        subscription: &Arc<Subscription>,
        predicate_id: proto::PredicateId,
        entity: &Entity,
        matching: bool,
    ) {
        let mut predicates = subscription.predicates.lock().unwrap();
        if let Some(predicate_state) = predicates.get_mut(&predicate_id) {
            let did_match = predicate_state.matching_entities.iter().any(|e| e.id() == entity.id());
            match (did_match, matching) {
                (false, true) => {
                    predicate_state.matching_entities.push(entity.clone());
                }
                (true, false) => {
                    predicate_state.matching_entities.retain(|r| r.id != entity.id);
                }
                _ => {} // No change needed
            }
        }
    }

    /// Notify subscriptions about an entity change
    pub fn notify_change(&self, changes: Vec<EntityChange>) {
        // pretty format self
        debug!("Reactor.notify_change({:?})", changes);
        // Group changes by subscription
        let mut sub_changes: std::collections::HashMap<proto::PredicateId, Vec<ItemChange<Entity>>> = std::collections::HashMap::new();

        for change in changes {
            let (entity, events) = change.into_parts();

            let mut possibly_interested_sub_preds = HashSet::new();

            debug!("Reactor - index watchers: {:?}", self.index_watchers);
            // Find subscriptions that might be interested based on index watchers
            for ((collection_id, field_id), index_ref) in self.index_watchers.to_vec() {
                // Get the field value from the entity
                if collection_id == entity.collection {
                    if let Some(field_value) = entity.value(&field_id.0) {
                        possibly_interested_sub_preds.extend(index_ref.read().unwrap().find_matching(Value::String(field_value)));
                    }
                }
            }

            // Also check wildcard watchers for this collection
            if let Some(watchers) = self.wildcard_watchers.get(&entity.collection) {
                for watcher in watchers.read().unwrap().to_vec() {
                    possibly_interested_sub_preds.insert(watcher);
                }
            }

            // Check entity watchers
            if let Some(watchers) = self.entity_watchers.get(&entity.id()) {
                for watcher in watchers.iter() {
                    possibly_interested_sub_preds.insert(*watcher);
                }
            }

            debug!(" possibly_interested_sub_preds: {possibly_interested_sub_preds:?}");
            // Check each possibly interested subscription-predicate pair with full predicate evaluation
            for (sub_id, predicate_id) in possibly_interested_sub_preds {
                if let Some(subscription) = self.subscriptions.get(&sub_id) {
                    // Get the predicate state
                    let (did_match, matches) = {
                        let mut predicates = subscription.predicates.lock().unwrap();
                        if let Some(predicate_state) = predicates.get_mut(&predicate_id) {
                            // Skip uninitialized predicates
                            if !predicate_state.initialized {
                                continue;
                            }

                            // Use evaluate_predicate directly on the entity
                            debug!("\tnotify_change predicate: {}:{} {:?}", sub_id, predicate_id, predicate_state.predicate);
                            let matches = ankql::selection::filter::evaluate_predicate::<Entity>(&entity, &predicate_state.predicate)
                                .unwrap_or(false);
                            let did_match = predicate_state.matching_entities.iter().any(|r| r.id() == entity.id());

                            (did_match, matches)
                        } else {
                            continue; // Predicate not found
                        }
                    };

                    use ankql::selection::filter::Filterable;
                    debug!("\tnotify_change matches: {matches} did_match: {did_match} {}: {:?}", entity.id, entity.value("status"));

                    // Update entity watchers and predicate matching entities
                    self.update_entity_watchers(&entity, matches, (sub_id, predicate_id));
                    self.update_predicate_matching_entities(&subscription, predicate_id, &entity, matches);

                    // Determine the change type
                    let new_change: Option<ItemChange<Entity>> = if matches != did_match {
                        // Matching status changed
                        Some(if matches {
                            ItemChange::Add { item: entity.clone(), events: events.clone() }
                        } else {
                            ItemChange::Remove { item: entity.clone(), events: events.clone() }
                        })
                    } else if matches {
                        // Entity still matches but was updated
                        Some(ItemChange::Update { item: entity.clone(), events: events.clone() })
                    } else {
                        // Entity didn't match before and still doesn't match
                        None
                    };

                    // Add the change to the subscription's changes if there is one
                    if let Some(new_change) = new_change {
                        sub_changes.entry(sub_id).or_default().push(new_change);
                    }
                }
            }
        }

        // Send batched notifications
        for (sub_id, changes) in sub_changes {
            if let Some(subscription) = self.subscriptions.get(&sub_id) {
                // Get all matching entities across all predicates for this subscription
                let all_matching_entities: Vec<Entity> = {
                    let predicates = subscription.predicates.lock().unwrap();
                    let mut all_entities = Vec::new();
                    for predicate_state in predicates.values() {
                        all_entities.extend(predicate_state.matching_entities.iter().cloned());
                    }
                    // Deduplicate by entity ID
                    all_entities.sort_by_key(|e| e.id);
                    all_entities.dedup_by_key(|e| e.id);
                    all_entities
                };

                // Call legacy callback if present
                if let Some(callback) = &subscription.legacy_callback {
                    callback(ChangeSet { resultset: ResultSet { loaded: true, items: all_matching_entities }, changes });
                }
            }
        }
    }

    /// Notify all subscriptions that their entities have been removed but do not remove the subscriptions
    pub fn system_reset(&self) {
        // Collect all current subscriptions and their matching entities
        let subs = self.subscriptions.to_vec();
        self.entity_watchers.clear();

        // For each subscription, generate removal notifications for all its entities
        for (_sub_id, subscription) in subs {
            let mut all_entities = Vec::new();

            // Clear all predicate matching entities and collect them
            {
                let mut predicates = subscription.predicates.lock().unwrap();
                for predicate_state in predicates.values_mut() {
                    all_entities.extend(predicate_state.matching_entities.drain(..));
                }
            }

            if !all_entities.is_empty() {
                // Deduplicate entities by ID
                all_entities.sort_by_key(|e| e.id);
                all_entities.dedup_by_key(|e| e.id);

                // Create removal changes for all entities
                let changes: Vec<ItemChange<Entity>> = all_entities
                    .iter()
                    .map(|entity| ItemChange::Remove {
                        item: entity.clone(),
                        events: vec![], // No events for system reset
                    })
                    .collect();

                // Call legacy callback if present
                if let Some(callback) = &subscription.legacy_callback {
                    callback(ChangeSet {
                        changes,
                        resultset: ResultSet {
                            loaded: true,
                            items: vec![], // Empty resultset since everything is removed
                        },
                    });
                }
            }
        }
    }
}

impl<SE, PA> std::fmt::Debug for Reactor<SE, PA> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Reactor {{ subscriptions: {:?}, index_watchers: {:?}, wildcard_watchers: {:?}, entity_watchers: {:?} }}",
            self.subscriptions, self.index_watchers, self.wildcard_watchers, self.entity_watchers
        )
    }
}

/// A subscription that can be shared between indexes
pub struct Subscription {
    /// Unique ID for this subscription (channel/connection level)
    pub(crate) id: ReactorSubId,
    /// All predicate states, keyed by PredicateId
    pub(crate) predicates: Mutex<HashMap<proto::PredicateId, PredicateState>>,
    /// Entity-level subscriptions (explicit, not predicate-based)
    pub(crate) entity_subscriptions: Mutex<HashSet<proto::EntityId>>,
    /// Broadcast for signal implementation
    broadcast: Broadcast,
    /// Stored listeners that receive ReactorUpdate
    listeners: Mutex<Vec<SubscribeListener<ReactorUpdate>>>,
}

impl Subscription {
    /// Create a new subscription
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            id: ReactorSubId::new(),
            predicates: Mutex::new(HashMap::new()),
            entity_subscriptions: Mutex::new(HashSet::new()),
            broadcast: Broadcast::new(),
            listeners: Mutex::new(Vec::new()),
        })
    }

    /// Add a predicate to this subscription
    pub fn add_predicate<SE, PA>(
        &self,
        reactor: &Reactor<SE, PA>,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        predicate: ankql::ast::Predicate,
    ) where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Start watching the relevant indexes
        Reactor::manage_watchers_recurse(reactor, collection_id, &predicate, self.id, predicate_id, WatcherOp::Add);

        // Add predicate to subscription
        self.predicates
            .lock()
            .unwrap()
            .insert(predicate_id, PredicateState { predicate, initialized: false, matching_entities: Vec::new() });

        // Update predicate mapping
        reactor.predicate_subscription_map.insert(predicate_id, self.id);
    }

    /// Remove a predicate from this subscription
    pub fn remove_predicate(&self, predicate_id: &proto::PredicateId) { self.predicates.lock().unwrap().remove(predicate_id); }

    /// Initialize a specific predicate by performing initial evaluation
    pub async fn initialize<SE, PA>(
        &self,
        reactor: &Reactor<SE, PA>,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        states: Vec<Attested<EntityState>>,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Find initial matching entities
        let storage_collection = reactor.collections.get(collection_id).await?;
        let mut matching_entities = Vec::new();

        // in theory, any state that is in the collection should already have its events in the storage collection as well
        let retriever = LocalRetriever::new(storage_collection.clone());

        // Get the predicate
        let predicate = {
            let predicates = self.predicates.lock().unwrap();
            if let Some(state) = predicates.get(&predicate_id) {
                state.predicate.clone()
            } else {
                return Err(anyhow::anyhow!("Predicate {:?} not found in subscription", predicate_id));
            }
        };

        // Convert states to Entity and filter by predicate
        for state in states {
            let (_, entity) =
                reactor.entityset.with_state(&retriever, state.payload.entity_id, collection_id.to_owned(), state.payload.state).await?;

            // Evaluate predicate for each entity
            if ankql::selection::filter::evaluate_predicate(&entity, &predicate).unwrap_or(false) {
                matching_entities.push(entity.clone());

                // Set up entity watchers
                reactor.entity_watchers.set_insert(entity.id, (self.id, predicate_id));
            }
        }

        // Initialize the predicate with matching entities
        self.initialize_predicate(predicate_id, matching_entities.clone());

        Ok(())
    }

    /// Add an entity subscription
    pub fn add_entity_subscription(&self, entity_id: proto::EntityId) { self.entity_subscriptions.lock().unwrap().insert(entity_id); }

    /// Remove an entity subscription
    pub fn remove_entity_subscription(&self, entity_id: &proto::EntityId) { self.entity_subscriptions.lock().unwrap().remove(entity_id); }

    /// Initialize a specific predicate with its initial state
    pub fn initialize_predicate(&self, predicate_id: PredicateId, matching_entities: Vec<Entity>) {
        if let Some(state) = self.predicates.lock().unwrap().get_mut(&predicate_id) {
            state.matching_entities = matching_entities;
            state.initialized = true;
        }
    }

    /// Initialize the entire subscription (for single-predicate case)
    pub fn initialize(&self, matching_entities: Vec<Entity>) {
        // Assumes single predicate - get the first one
        let mut predicates = self.predicates.lock().unwrap();
        if let Some((_, state)) = predicates.iter_mut().next() {
            state.matching_entities = matching_entities;
            state.initialized = true;
        }
    }

    /// Check if a predicate is initialized
    pub fn is_predicate_initialized(&self, predicate_id: &PredicateId) -> bool {
        self.predicates.lock().unwrap().get(predicate_id).map(|state| state.initialized).unwrap_or(false)
    }

    /// Check if all predicates are initialized
    pub fn is_fully_initialized(&self) -> bool { self.predicates.lock().unwrap().values().all(|state| state.initialized) }

    /// Notify listeners with a ReactorUpdate
    pub(crate) fn notify(&self, update: ReactorUpdate) {
        // Call all registered listeners with the update
        let listeners = self.listeners.lock().unwrap();
        for listener in listeners.iter() {
            listener(update.clone());
        }

        // Call legacy callback if present (convert ReactorUpdate to ChangeSet)
        if let Some(callback) = &self.legacy_callback {
            let changeset: ChangeSet<Entity> = update.into();
            callback(changeset);
        }

        // Also notify for signal compatibility
        self.broadcast.send();
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let predicates = self.predicates.lock().unwrap();
        write!(f, "Subscription {{ id: {:?}, collection_id: {:?}, predicates: {} }}", self.id, self.collection_id, predicates.len())
    }
}

// Implement Signal trait for Subscription
impl Signal for Subscription {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.broadcast.reference().listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.broadcast.id() }
}

// Implement Subscribe trait for Subscription
// Note: We're subscribing to ReactorUpdate, not ChangeSet
impl Subscribe<ReactorUpdate> for Subscription {
    fn subscribe<F>(&self, listener: F) -> SignalGuard
    where F: ankurah_signals::porcelain::subscribe::IntoSubscribeListener<ReactorUpdate> {
        let listener = listener.into_subscribe_listener();

        // Store the listener
        self.listeners.lock().unwrap().push(listener);

        // Also set up broadcast listener for compatibility
        let guard = self.listen(Arc::new(move || {
            // Signal notification happens in notify()
        }));
        SignalGuard::new(guard)
    }
}

// ============================================================================
// New structures for unified reactor design
// ============================================================================

/// Unique identifier for a reactor subscription (connection/channel level)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReactorSubId(Ulid);

impl ReactorSubId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl std::fmt::Display for ReactorSubId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "RS-{}", self.0) }
}

// TODO - see if we can optimize this by using just predicate id, or by using an Arc<PredicateState> or something like that
// so we don't have to do two lookups. (Post refactor optimization step)
/// A subscription-predicate pair used by watchers and indexes
pub type SubscriptionPredicateId = (ReactorSubId, proto::PredicateId);

/// State for a single predicate within a subscription
#[derive(Debug)]
struct PredicateState {
    predicate: ankql::ast::Predicate,
    initialized: bool,
    matching_entities: Vec<Entity>,
}

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
#[derive(Debug, Clone)]
pub struct ReactorUpdate {
    /// All entities that changed, with their relevance information
    pub items: Vec<ReactorUpdateItem>,
}

/// A single entity update with all relevance information
#[derive(Debug, Clone)]
pub struct ReactorUpdateItem {
    /// The entity that changed
    pub entity: Entity,
    /// Events that caused this update
    pub events: Vec<Attested<Event>>,
    /// Whether this entity is explicitly subscribed (entity-level subscription)
    pub entity_subscribed: bool,
    /// Which predicates this update is relevant to and how
    /// Empty if only relevant due to entity_subscribed
    pub predicate_relevance: Vec<(PredicateId, MembershipChange)>,
}

impl ReactorUpdateItem {
    /// Check if this item represents any membership change
    pub fn has_membership_change(&self) -> bool { !self.predicate_relevance.is_empty() }

    /// Check if this is purely an entity subscription update
    pub fn is_entity_only(&self) -> bool { self.entity_subscribed && self.predicate_relevance.is_empty() }
}

// Conversion from ReactorUpdate to ChangeSet for local subscriptions
impl From<ReactorUpdate> for ChangeSet<Entity> {
    fn from(update: ReactorUpdate) -> Self {
        // Local subscriptions have exactly one predicate
        let mut changes = Vec::new();
        let mut all_entities = Vec::new();

        for item in update.items {
            all_entities.push(item.entity.clone());

            // Get the membership change for the single predicate (if any)
            let membership = item.predicate_relevance.first().map(|(_, m)| m);

            let change = match membership {
                Some(MembershipChange::Initial) => ItemChange::Initial { item: item.entity },
                Some(MembershipChange::Add) => ItemChange::Add { item: item.entity, events: item.events },
                Some(MembershipChange::Remove) => ItemChange::Remove { item: item.entity, events: item.events },
                None if item.entity_subscribed => {
                    // Entity subscription update without predicate membership change
                    ItemChange::Update { item: item.entity, events: item.events }
                }
                None if !item.events.is_empty() => {
                    // Entity still matches predicate and has changes
                    ItemChange::Update { item: item.entity, events: item.events }
                }
                _ => continue, // Skip items with no relevance
            };

            changes.push(change);
        }

        ChangeSet { changes, resultset: ResultSet { loaded: true, items: all_entities } }
    }
}
