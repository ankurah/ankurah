mod comparison_index;
mod subscription;
mod update;

pub(crate) use self::{
    comparison_index::ComparisonIndex,
    subscription::{ReactorSubscription, ReactorSubscriptionId},
    update::{MembershipChange, ReactorUpdate, ReactorUpdateItem},
};

use crate::{
    changes::EntityChange, entity::Entity, error::SubscriptionError, reactor::subscription::ReactorSubInner, resultset::EntityResultSet,
    storage::StorageEngine, value::Value,
};
use ankql::selection::filter::Filterable;
use ankurah_proto::{self as proto};
use indexmap::IndexMap;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use tracing::debug;

/// Trait for entities that can be used in reactor notifications
pub trait AbstractEntity: ankql::selection::filter::Filterable + Clone + std::fmt::Debug {
    fn collection(&self) -> proto::CollectionId;
    fn id(&self) -> proto::EntityId;
    fn value(&self, field: &str) -> Option<String>;
}

/// Trait for types that can be used in notify_change
pub trait ChangeNotification: std::fmt::Debug + std::fmt::Display {
    type Entity: AbstractEntity;
    type Event: Clone + std::fmt::Debug;

    fn into_parts(self) -> (Self::Entity, Vec<Self::Event>);
}

// Implement ReactorEntity for Entity
impl AbstractEntity for Entity {
    fn collection(&self) -> proto::CollectionId { self.collection.clone() }

    fn id(&self) -> proto::EntityId { self.id }

    fn value(&self, field: &str) -> Option<String> { ankql::selection::filter::Filterable::value(self, field) }
}

// Implement the trait for EntityChange
impl ChangeNotification for EntityChange {
    type Entity = Entity;
    type Event = ankurah_proto::Attested<proto::Event>;

    fn into_parts(self) -> (Self::Entity, Vec<Self::Event>) { self.into_parts() }
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of entities
pub struct Reactor<E: AbstractEntity = Entity, Ev = ankurah_proto::Attested<ankurah_proto::Event>>(Arc<ReactorInner<E, Ev>>);

struct ReactorInner<E: AbstractEntity, Ev> {
    subscriptions: Mutex<HashMap<ReactorSubscriptionId, SubscriptionState<E, Ev>>>,
    watcher_set: Mutex<WatcherSet>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum EntityWatcherId {
    Predicate(ReactorSubscriptionId, proto::PredicateId),
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

struct WatcherSet {
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as change
    index_watchers: HashMap<(proto::CollectionId, FieldId), ComparisonIndex<(ReactorSubscriptionId, proto::PredicateId)>>,
    /// The set of watchers who want to be notified of any changes to a given collection
    wildcard_watchers: HashMap<proto::CollectionId, HashSet<(ReactorSubscriptionId, proto::PredicateId)>>,
    /// Index of subscriptions that presently match each entity, either by predicate or by entity subscription.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: HashMap<ankurah_proto::EntityId, HashSet<EntityWatcherId>>,
}

/// State for a single predicate within a subscription
#[derive(Debug, Clone)]
struct PredicateState<E: AbstractEntity> {
    // TODO make this a clonable PredicateSubscription and store it instead of the channel?
    pub(crate) subscription_id: ReactorSubscriptionId,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) predicate: ankql::ast::Predicate,
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) paused: bool, // When true, skip notifications (used during initialization and updates)
    pub(crate) resultset: EntityResultSet<E>,
    pub(crate) version: u32,
}

struct SubscriptionState<E: AbstractEntity, Ev> {
    pub(crate) id: ReactorSubscriptionId,
    pub(crate) predicates: HashMap<proto::PredicateId, PredicateState<E>>,
    /// The set of entities that are subscribed to by this subscription
    pub(crate) entity_subscriptions: HashSet<proto::EntityId>,
    // not sure if we actually need this
    pub(crate) entities: HashMap<proto::EntityId, E>,
    pub(crate) broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);

impl From<&str> for FieldId {
    fn from(val: &str) -> Self { FieldId(val.to_string()) }
}

#[derive(Debug, Copy, Clone)]
enum WatcherOp {
    Add,
    Remove,
}

// don't require Clone SE or PA, because we have an Arc
impl<E: AbstractEntity, Ev> Clone for Reactor<E, Ev> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<E: AbstractEntity, Ev: Clone> Default for Reactor<E, Ev> {
    fn default() -> Self { Self::new() }
}

impl<E: AbstractEntity, Ev: Clone> Reactor<E, Ev> {
    pub fn new() -> Self {
        Self(Arc::new(ReactorInner {
            subscriptions: Mutex::new(HashMap::new()),
            watcher_set: Mutex::new(WatcherSet {
                index_watchers: HashMap::new(),
                wildcard_watchers: HashMap::new(),
                entity_watchers: HashMap::new(),
            }),
        }))
    }

    /// Create a new subscription container
    pub fn subscribe(&self) -> ReactorSubscription<E, Ev> {
        let broadcast = ankurah_signals::broadcast::Broadcast::new();
        let subscription = SubscriptionState {
            id: ReactorSubscriptionId::new(),
            predicates: HashMap::new(),
            entity_subscriptions: HashSet::new(),
            entities: HashMap::new(),
            broadcast: broadcast.clone(),
        };
        let subscription_id = subscription.id;
        self.0.subscriptions.lock().unwrap().insert(subscription_id, subscription);
        ReactorSubscription(Arc::new(ReactorSubInner { subscription_id, reactor: self.clone(), broadcast }))
    }
}

impl<E: AbstractEntity, Ev> Reactor<E, Ev> {
    /// Remove a subscription and all its predicates
    pub(crate) fn unsubscribe(&self, sub_id: ReactorSubscriptionId) -> Result<(), SubscriptionError> {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();
        let mut watcher_set = self.0.watcher_set.lock().unwrap();

        if let Some(sub) = subscriptions.remove(&sub_id) {
            // Remove all predicates from watchers
            for (predicate_id, predicate_state) in sub.predicates {
                // Remove from index watcher
                watcher_set.recurse_predicate_watchers(
                    &predicate_state.collection_id,
                    &predicate_state.predicate,
                    (sub_id, predicate_id),
                    WatcherOp::Remove,
                );

                // Remove from entity watchers using predicate's matching entities
                for entity_id in predicate_state.resultset.keys() {
                    if let Some(watchers) = watcher_set.entity_watchers.get_mut(&entity_id) {
                        // only remove the subscription watcher, not the entity watchers based on currently matching predicates
                        watchers.remove(&EntityWatcherId::Subscription(sub_id));
                    }
                }
            }
        } else {
            return Err(SubscriptionError::SubscriptionNotFound);
        }

        Ok(())
    }

    /// Remove a predicate from a subscription
    pub fn remove_predicate(
        &self,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
    ) -> Result<(), SubscriptionError> {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();
        let mut watcher_state = self.0.watcher_set.lock().unwrap();

        // remove the predicate from the subscription
        let predicate_state = match subscriptions.get_mut(&subscription_id) {
            Some(sub) => match sub.predicates.remove(&predicate_id) {
                Some(p) => p,
                None => return Err(SubscriptionError::PredicateNotFound),
            },
            None => return Err(SubscriptionError::SubscriptionNotFound),
        };

        // Remove from watchers
        let watcher_id = (subscription_id, predicate_id);
        watcher_state.recurse_predicate_watchers(&predicate_state.collection_id, &predicate_state.predicate, watcher_id, WatcherOp::Remove);

        Ok(())
    }

    /// Add entity subscriptions to a subscription
    pub fn add_entity_subscriptions(&self, subscription_id: ReactorSubscriptionId, entity_ids: impl IntoIterator<Item = proto::EntityId>) {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();
        let mut watcher_state = self.0.watcher_set.lock().unwrap();

        if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
            for entity_id in entity_ids {
                subscription.entity_subscriptions.insert(entity_id);
                watcher_state.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Subscription(subscription_id));
            }
        }
    }

    /// Remove entity subscriptions from a subscription
    pub fn remove_entity_subscriptions(
        &self,
        subscription_id: ReactorSubscriptionId,
        entity_ids: impl IntoIterator<Item = proto::EntityId>,
    ) {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();
        let mut watcher_state = self.0.watcher_set.lock().unwrap();

        if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
            for entity_id in entity_ids {
                subscription.entity_subscriptions.remove(&entity_id);

                // TODO: Check if any predicates match this entity before removing from entity_watchers
                // For now, only remove if no predicates match
                let should_remove = !subscription.predicates.values().any(|p| p.resultset.contains_key(&entity_id));

                if should_remove {
                    if let Some(watchers) = watcher_state.entity_watchers.get_mut(&entity_id) {
                        watchers.remove(&EntityWatcherId::Subscription(subscription_id));
                        if watchers.is_empty() {
                            watcher_state.entity_watchers.remove(&entity_id);
                        }
                    }
                }
            }
        }
    }

    /// Update predicate matching entities when an entity's matching status changes
    /// We need to keep a list of matching entities for subscription / predicate in order to keep the entity resident in memory
    /// so we don't have to re-fetch it from storage every time (later, make this an LRU cached Entity, but just hold the Entity for now)
    fn update_predicate_matching_entities(
        subscription: &mut SubscriptionState<E, Ev>,
        predicate_id: proto::PredicateId,
        entity: &E,
        matching: bool,
    ) {
        if let Some(predicate_state) = subscription.predicates.get_mut(&predicate_id) {
            let entity_id = AbstractEntity::id(entity);
            let did_match = predicate_state.resultset.contains_key(&entity_id);

            match (did_match, matching) {
                (false, true) => {
                    // Entity now matches - add to matching set and cache the entity
                    tracing::info!(
                        "PUSH entity {} to predicate resultset {} len: {}",
                        entity_id,
                        predicate_id,
                        predicate_state.resultset.len()
                    );
                    predicate_state.resultset.push(entity.clone());
                    subscription.entities.insert(entity_id, entity.clone());
                }
                (true, false) => {
                    tracing::info!("REMOVE entity {} from predicate resultset {}", entity_id, predicate_id);
                    // Entity no longer matches - remove from matching set
                    // (but keep in entity cache for now - it might still be needed by other predicates)
                    predicate_state.resultset.remove(&entity_id);
                }
                _ => {} // No change needed
            }
        }
    }
}

impl<E: AbstractEntity + 'static, Ev: Clone> Reactor<E, Ev> {
    /// Set a predicate for a subscription (handles both initial and updates)
    /// This is the ONLY method for setting/updating predicates
    pub fn add_predicate(
        &self,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        collection_id: proto::CollectionId,
        predicate: ankql::ast::Predicate,
        resultset: EntityResultSet<E>, // TODO: For v0, pass existing resultset; for v>0, get from predicate state
        included_entities: Vec<E>,     // TODO: For updates, these are only NEW entities from remote
        version: u32,                  // TODO: Track version for predicate updates
    ) -> anyhow::Result<()> {
        // TODO: Check if predicate already exists for this predicate_id
        // If it exists, this is an update - store the old resultset for diff computation
        // If it doesn't exist, this is initial subscription - old resultset is empty

        // TODO: Break this up - for v0 use passed resultset, for v>0 get from predicate state
        // First, add or update the predicate in the subscription (from add_predicate logic)
        {
            let mut subscriptions = self.0.subscriptions.lock().unwrap();
            let mut watcher_state = self.0.watcher_set.lock().unwrap();

            let subscription =
                subscriptions.get_mut(&subscription_id).ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?;

            // Check if this is an update or initial
            let is_update = subscription.predicates.contains_key(&predicate_id);

            if !is_update {
                // Initial case: create new predicate state with the passed-in resultset
                use std::collections::hash_map::Entry;
                match subscription.predicates.entry(predicate_id) {
                    Entry::Vacant(v) => {
                        v.insert(PredicateState {
                            subscription_id,
                            collection_id: collection_id.clone(),
                            predicate: predicate.clone(),
                            paused: true, // Start paused until initialization completes
                            resultset: resultset.clone(),
                            version,
                        });
                    }
                    Entry::Occupied(_) => {
                        return Err(anyhow::anyhow!("Predicate already exists"));
                    }
                }

                // Set up watchers for new predicate
                let watcher_id = (subscription_id, predicate_id);
                watcher_state.recurse_predicate_watchers(&collection_id, &predicate, watcher_id, WatcherOp::Add);
            } else {
                // TODO: Update case - update the predicate AST
                // For v>0, should get the resultset from existing predicate state, not use passed one
                if let Some(pred_state) = subscription.predicates.get_mut(&predicate_id) {
                    pred_state.predicate = predicate.clone();
                }
            }
        }

        // TODO: When predicate already exists (update case):
        // 1. Save the old matching entity IDs before any changes
        // 2. The included_entities parameter contains only NEW entities from remote
        //    that match the new predicate but weren't known before
        // 3. We need to re-evaluate ALL existing entities against the new predicate
        let mut matching_entities = Vec::new();
        let mut reactor_update_items: Vec<ReactorUpdateItem<E, Ev>> = Vec::new();

        // TODO: This loop should only process truly new entities (not in old resultset)
        // For updates, these are entities the remote found that match new but not old predicate
        for entity in included_entities {
            if ankql::selection::filter::evaluate_predicate(&entity, &predicate).unwrap_or(false) {
                matching_entities.push(entity);
            }
        }

        // Update state with matching entities and mark as initialized
        {
            let mut watcher_state = self.0.watcher_set.lock().unwrap();
            let mut subscriptions = self.0.subscriptions.lock().unwrap();

            // Update subscription's predicate state
            if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
                // Set up entity watchers for matching entities
                for entity in &matching_entities {
                    let entity_id = AbstractEntity::id(entity);
                    subscription.entity_subscriptions.insert(entity_id);
                    watcher_state.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Subscription(subscription_id));

                    let entity_watcher = watcher_state.entity_watchers.entry(entity_id).or_default();
                    // same as in notify_change, we need to add a watcher for the subscription itself, which lingers
                    // until the user expressly tells us they want to stop watching
                    // and one for the predicate that gets removed when the predicate no longer matches
                    entity_watcher.insert(EntityWatcherId::Subscription(subscription_id));
                    entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, predicate_id));
                    reactor_update_items.push(ReactorUpdateItem {
                        entity: entity.clone(),
                        events: vec![],
                        entity_subscribed: true, // TODO: decide if this is relevant given that we implicitly subscribe all predicate matched entities
                        predicate_relevance: vec![(predicate_id, MembershipChange::Initial)],
                    });
                }
                if let Some(pred_state) = subscription.predicates.get_mut(&predicate_id) {
                    // TODO: DO NOT use replace_all!
                    // Instead:
                    // 1. Add new matching_entities to resultset (from initial_entities param)
                    // 2. Re-evaluate each entity currently in resultset against new predicate
                    // 3. Remove entities that no longer match new predicate
                    // 4. Track all changes for the ReactorUpdate (Add/Remove/Initial)
                    // 5. Update the predicate AST to the new one

                    // TEMPORARY: Current code assumes initial subscription only
                    tracing::info!(
                        "INITIALIZE predicate resultset.replace_all {} matching_entities.len(): {} len: {}",
                        predicate_id,
                        matching_entities.len(),
                        pred_state.resultset.len()
                    );
                    // TODO: Use ResultSetBatch to apply diffs atomically
                    // let mut batch = pred_state.resultset.batch();
                    // for entity in matching_entities {
                    //     if !pred_state.resultset.contains_key(&entity.id()) {
                    //         batch.add(entity);  // NEW match
                    //     }
                    // }
                    // for old_id in pred_state.resultset.keys() {
                    //     if !matching_entities.iter().any(|e| e.id() == old_id) {
                    //         batch.remove(old_id);  // No longer matches
                    //     }
                    // }
                    // drop(batch);  // Triggers single notification
                    // Do NOT use replace_all
                    //
                    // TODO: Consider optimization: Could use HashSet for O(1) lookups instead of O(n) scans
                    // Note: Can't use lexicographic zip since resultsets maintain insertion order,
                    // not EntityId order (and may later support custom sort orders)
                    pred_state.resultset.replace_all(matching_entities); // FIXME - switch to batch, audit TODOS, and search for sensible DRY code
                    pred_state.paused = false; // Unpause now that initialization is complete
                    pred_state.resultset.set_loaded(true);
                }
            }
        }

        // Use the version parameter to track which update completed
        let reactor_update = ReactorUpdate::<E, Ev> { items: reactor_update_items, initialized_predicate: Some((predicate_id, version)) };

        // Notify the subscription
        {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            if let Some(subscription) = subscriptions.get(&subscription_id) {
                subscription.notify(reactor_update);
            }
        }

        Ok(())
    }

    /// Add a new predicate to a subscription (v0 - initial subscription)
    /// Takes a resultset and does no diffing

    /// Pause a predicate from receiving notifications - gets unpaused by update_predicate
    pub fn pause_predicate(&self, predicate_id: proto::PredicateId) {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();

        // Find the subscription that has this predicate
        for subscription in subscriptions.values_mut() {
            if let Some(pred_state) = subscription.predicates.get_mut(&predicate_id) {
                pred_state.paused = true;
            }
        }
    }

    /// Update an existing predicate (v>0)
    /// Does diffing against the current resultset
    pub fn update_predicate(
        &self,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        collection_id: proto::CollectionId,
        predicate: ankql::ast::Predicate,
        included_entities: Vec<E>,
        version: u32,
        emit_removes: bool, // Whether to emit Remove events for entities that no longer match
    ) -> anyhow::Result<()> {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();
        let mut watcher_state = self.0.watcher_set.lock().unwrap();

        // Get the subscription
        let subscription =
            subscriptions.get_mut(&subscription_id).ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?;

        // Get mutable reference to predicate state (must exist for v>0)
        let pred_state = subscription.predicates.get_mut(&predicate_id).ok_or_else(|| anyhow::anyhow!("Predicate not found for update"))?;

        // Update the predicate AST
        let old_predicate = pred_state.predicate.clone();
        pred_state.predicate = predicate.clone();

        // Update index watchers if predicate changed
        let watcher_id = (subscription_id, predicate_id);
        watcher_state.recurse_predicate_watchers(&collection_id, &old_predicate, watcher_id, WatcherOp::Remove);
        watcher_state.recurse_predicate_watchers(&collection_id, &predicate, watcher_id, WatcherOp::Add);

        // Create batch for atomic updates
        let mut batch = pred_state.resultset.batch();
        let mut reactor_update_items = Vec::new();

        // Process included entities (only truly new ones from remote)
        for entity in included_entities {
            if ankql::selection::filter::evaluate_predicate(&entity, &predicate).unwrap_or(false) {
                let entity_id = AbstractEntity::id(&entity);

                // Check if this is truly new to the resultset
                if !batch.contains(&entity_id) {
                    // Add to batch
                    batch.add(entity.clone());

                    // Add to subscription entities map
                    subscription.entities.insert(entity_id, entity.clone());

                    // Set up entity watchers
                    subscription.entity_subscriptions.insert(entity_id);
                    let entity_watcher = watcher_state.entity_watchers.entry(entity_id).or_default();
                    entity_watcher.insert(EntityWatcherId::Subscription(subscription_id));
                    entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, predicate_id));

                    // Create reactor update item (Initial for truly new entities)
                    reactor_update_items.push(ReactorUpdateItem {
                        entity,
                        events: vec![],
                        entity_subscribed: true,
                        predicate_relevance: vec![(predicate_id, MembershipChange::Initial)],
                    });
                }
            }
        }

        // Remove entities that no longer match the new predicate
        let mut to_remove = Vec::new();
        for (entity_id, entity) in batch.iter_entities() {
            if !ankql::selection::filter::evaluate_predicate(&entity, &predicate).unwrap_or(false) {
                tracing::debug!("Entity {:?} no longer matches predicate", entity_id);

                // If emit_removes is true, create a Remove event for local subscriptions
                if emit_removes {
                    tracing::debug!("Creating Remove event for entity {:?}", entity_id);
                    reactor_update_items.push(ReactorUpdateItem {
                        entity: entity.clone(),
                        events: vec![],
                        entity_subscribed: false,
                        predicate_relevance: vec![(predicate_id, MembershipChange::Remove)],
                    });
                }

                to_remove.push(entity_id);
            }
        }
        tracing::info!("Removing {} entities that no longer match", to_remove.len());

        // Remove non-matching entities from the resultset and clean up watchers
        for entity_id in to_remove {
            batch.remove(entity_id);

            // Clean up entity predicate watcher (but keep subscription watcher)
            if let Some(entity_watcher) = watcher_state.entity_watchers.get_mut(&entity_id) {
                entity_watcher.remove(&EntityWatcherId::Predicate(subscription_id, predicate_id));

                // TODO: Investigate if subscription.entities is being correctly populated and used
                // If no more predicates are watching this entity, remove it from subscription.entities
                // (but only if it's not explicitly subscribed)
                if !subscription.entity_subscriptions.contains(&entity_id) {
                    let has_other_predicates =
                        entity_watcher.iter().any(|w| matches!(w, EntityWatcherId::Predicate(sub_id, _) if *sub_id == subscription_id));
                    if !has_other_predicates {
                        subscription.entities.remove(&entity_id);
                    }
                }
            }
        }

        // Unpause now that update is complete
        pred_state.paused = false;
        pred_state.version = version;
        pred_state.resultset.set_loaded(true);

        // Drop batch to apply changes
        drop(batch);

        // Send reactor update
        let reactor_update = ReactorUpdate::<E, Ev> { items: reactor_update_items, initialized_predicate: Some((predicate_id, version)) };
        subscription.notify(reactor_update);

        Ok(())
    }

    // Private helper methods are no longer needed since add_predicate and update_predicate
    // handle everything inline with a single lock acquisition

    /// Notify subscriptions about an entity change
    pub fn notify_change<C: ChangeNotification<Entity = E, Event = Ev>>(&self, changes: Vec<C>) {
        let mut watcher_set = self.0.watcher_set.lock().unwrap();

        let mut items: std::collections::HashMap<ReactorSubscriptionId, IndexMap<proto::EntityId, ReactorUpdateItem<E, Ev>>> =
            std::collections::HashMap::new();

        debug!("Reactor.notify_change({:?})", changes);
        for change in changes {
            let (entity, events) = change.into_parts();

            // Collect all potentially interested (subscription_id, predicate_id) pairs
            let mut possibly_interested_watchers: HashSet<(ReactorSubscriptionId, proto::PredicateId)> = HashSet::new();

            debug!("Reactor - index watchers: {:?}", watcher_set.index_watchers);
            // Find subscriptions that might be interested based on index watchers
            for ((collection_id, field_id), index_ref) in &watcher_set.index_watchers {
                // Get the field value from the entity
                if *collection_id == AbstractEntity::collection(&entity) {
                    if let Some(field_value) = AbstractEntity::value(&entity, &field_id.0) {
                        possibly_interested_watchers.extend(index_ref.find_matching(Value::String(field_value)));
                    }
                }
            }

            // Also check wildcard watchers for this collection
            if let Some(watchers) = watcher_set.wildcard_watchers.get(&AbstractEntity::collection(&entity)) {
                for watcher in watchers.iter() {
                    possibly_interested_watchers.insert(*watcher);
                }
            }

            // Check entity watchers - these are subscription-level, not predicate-level
            // We'll need to expand these to all predicates for the subscription

            let mut entity_subscribed = HashSet::new();
            if let Some(subscription_ids) = watcher_set.entity_watchers.get(&AbstractEntity::id(&entity)) {
                for sub_id in subscription_ids.iter() {
                    // Get all predicates for this subscription
                    if let Some(sub) = self.0.subscriptions.lock().unwrap().get(&sub_id.subscription_id()) {
                        for predicate_id in sub.predicates.keys() {
                            possibly_interested_watchers.insert((sub_id.subscription_id(), *predicate_id));
                            entity_subscribed.insert(*predicate_id);
                        }
                    }
                }
            }

            debug!(" possibly_interested_watchers: {possibly_interested_watchers:?}");
            let mut subscriptions = self.0.subscriptions.lock().unwrap();
            // Check each possibly interested subscription-predicate pair with full predicate evaluation
            for (subscription_id, predicate_id) in possibly_interested_watchers {
                if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
                    // Get the predicate state
                    let (did_match, matches) = {
                        if let Some(predicate_state) = subscription.predicates.get(&predicate_id) {
                            // Skip paused predicates
                            if predicate_state.paused {
                                continue;
                            }

                            // Use evaluate_predicate directly on the entity
                            debug!("\tnotify_change predicate: {} {:?}", predicate_id, predicate_state.predicate);
                            let matches =
                                ankql::selection::filter::evaluate_predicate(&entity, &predicate_state.predicate).unwrap_or(false);
                            let did_match = predicate_state.resultset.contains_key(&AbstractEntity::id(&entity));

                            (did_match, matches)
                        } else {
                            continue; // Predicate not found
                        }
                    };

                    tracing::info!(
                        "\tnotify_change matches: {matches} did_match: {did_match} {}: {:?}",
                        AbstractEntity::id(&entity),
                        AbstractEntity::value(&entity, "status")
                    );

                    let entity_watcher = watcher_set.entity_watchers.entry(entity.id()).or_default();
                    if matches {
                        // Entity subscriptions are implicit / sticky...
                        // Register one for the predicate that gets removed when the predicate no longer matches
                        entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, predicate_id));
                        // and one for the subscription itself, which lingers until the user expressly tells us they want to stop watching
                        // entity_watcher.insert(EntityWatcherId::Subscription(subscription_id));
                    } else {
                        // When the predicate no longer matches, only remove the predicate watcher, not the subscription watcher
                        entity_watcher.remove(&EntityWatcherId::Predicate(subscription_id, predicate_id));
                    }
                    Self::update_predicate_matching_entities(subscription, predicate_id, &entity, matches);

                    let sub_entities = items.entry(subscription_id).or_default();

                    let membership_change = match (did_match, matches) {
                        (true, false) => Some(MembershipChange::Remove),
                        (false, true) => Some(MembershipChange::Add),
                        _ => None,
                    };

                    let entity_subscribed = entity_subscribed.contains(&predicate_id);
                    if membership_change.is_some() || entity_subscribed {
                        tracing::info!("Reactor SENDING UPDATE to subscription {}", subscription_id);
                        match sub_entities.entry(AbstractEntity::id(&entity)) {
                            indexmap::map::Entry::Vacant(v) => {
                                v.insert(ReactorUpdateItem {
                                    entity: entity.clone(),
                                    events: events.clone(),
                                    entity_subscribed,
                                    predicate_relevance: membership_change.map(|mc| (predicate_id, mc)).into_iter().collect(),
                                });
                            }
                            indexmap::map::Entry::Occupied(mut o) => {
                                if let Some(mc) = membership_change {
                                    o.get_mut().predicate_relevance.push((predicate_id, mc));
                                }
                            }
                        }
                    }
                }
            }
        }

        for (sub_id, sub_items) in items {
            if let Some(subscription) = self.0.subscriptions.lock().unwrap().get(&sub_id) {
                subscription.notify(ReactorUpdate { items: sub_items.into_values().collect(), initialized_predicate: None });
            }
        }
    }

    /// Notify all subscriptions that their entities have been removed but do not remove the subscriptions
    pub fn system_reset(&self) {
        // Clear entity watchers first - no entities are being watched after reset, because any previously existing entities "stopped existing"
        // as part of the system reset.
        {
            let mut watcher_state = self.0.watcher_set.lock().unwrap();
            watcher_state.entity_watchers.clear();
        }

        let mut subscriptions = self.0.subscriptions.lock().unwrap();

        for (subscription_id, subscription_state) in subscriptions.iter_mut() {
            let mut update_items: Vec<ReactorUpdateItem<E, Ev>> = Vec::new();

            // For each predicate in this subscription
            for (predicate_id, predicate_state) in &mut subscription_state.predicates {
                // For each entity that was matching this predicate
                for entity_id in predicate_state.resultset.keys() {
                    // Try to get the entity from the subscription's cache
                    if let Some(entity) = subscription_state.entities.get(&entity_id) {
                        update_items.push(ReactorUpdateItem {
                            entity: entity.clone(),
                            events: vec![], // No events for system reset, because it's not a "change", its Thanos snapping his fingers
                            entity_subscribed: false, // All entities "stopped existing" and thus cannot be subscribed to
                            predicate_relevance: vec![(*predicate_id, MembershipChange::Remove)], // The predicates still exist, but all previous matching entities are unmatched
                        });
                    }
                }

                // Clear the matching entities for this predicate
                predicate_state.resultset.clear();
                predicate_state.resultset.set_loaded(false);
            }

            // Clear entity subscriptions and cached entities
            subscription_state.entity_subscriptions.clear();
            subscription_state.entities.clear();

            // Send the notification if there were any updates
            if !update_items.is_empty() {
                let reactor_update = ReactorUpdate { items: update_items, initialized_predicate: None };
                subscription_state.notify(reactor_update);
            }
        }
    }
}

impl<E: AbstractEntity> std::fmt::Debug for Reactor<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let watcher_set = self.0.watcher_set.lock().unwrap();
        let subscriptions = self.0.subscriptions.lock().unwrap();
        write!(
            f,
            "Reactor {{ subscriptions: {:?}, index_watchers: {:?}, wildcard_watchers: {:?}, entity_watchers: {:?} }}",
            subscriptions, watcher_set.index_watchers, watcher_set.wildcard_watchers, watcher_set.entity_watchers
        )
    }
}

impl WatcherSet {
    fn recurse_predicate_watchers(
        &mut self,
        collection_id: &proto::CollectionId,
        predicate: &ankql::ast::Predicate,
        watcher_id: (ReactorSubscriptionId, proto::PredicateId), // Should this be a tuple of (subscription_id, predicate_id) or just subscription_id?
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
                    let index = self.index_watchers.entry((collection_id.clone(), field_id)).or_default();

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

impl<E: AbstractEntity, Ev: Clone> SubscriptionState<E, Ev> {
    fn notify(&self, update: ReactorUpdate<E, Ev>) { self.broadcast.send(update); }
}

impl<E: AbstractEntity, Ev> std::fmt::Debug for SubscriptionState<E, Ev> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription {{ id: {:?}, predicates: {} }}", self.id, self.predicates.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_signals::Subscribe;
    use proto::{CollectionId, PredicateId};

    pub fn watcher<T: Clone + Send + 'static>() -> (Box<dyn Fn(T) + Send + Sync>, Box<dyn Fn() -> Vec<T> + Send + Sync>) {
        let values = Arc::new(Mutex::new(Vec::new()));
        let accumulate = {
            let values = values.clone();
            Box::new(move |value: T| {
                values.lock().unwrap().push(value);
            })
        };

        let check = Box::new(move || values.lock().unwrap().drain(..).collect());

        (accumulate, check)
    }

    #[derive(Debug, Clone)]
    struct TestEntity {
        id: proto::EntityId,
        collection: proto::CollectionId,
        state: Arc<Mutex<HashMap<String, String>>>,
    }
    impl Eq for TestEntity {}
    impl PartialEq for TestEntity {
        fn eq(&self, other: &Self) -> bool { self.id == other.id }
    }
    impl PartialOrd for TestEntity {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.id.cmp(&other.id)) }
    }
    #[derive(Debug, Clone, PartialEq)]
    struct TestEvent {
        id: proto::EventId,
        collection: proto::CollectionId,
        changes: HashMap<String, String>,
    }
    impl TestEntity {
        fn new(name: &str, status: &str) -> Self {
            Self {
                id: proto::EntityId::new(),
                collection: proto::CollectionId::fixed_name("album"),
                state: Arc::new(Mutex::new(HashMap::from([
                    ("name".to_string(), name.to_string()),
                    ("status".to_string(), status.to_string()),
                ]))),
            }
        }
    }
    impl Filterable for TestEntity {
        fn collection(&self) -> &str { self.collection.as_str() }
        fn value(&self, field: &str) -> Option<String> { self.state.lock().unwrap().get(field).cloned() }
    }
    impl AbstractEntity for TestEntity {
        fn collection(&self) -> proto::CollectionId { self.collection.clone() }
        fn id(&self) -> proto::EntityId { self.id }
        fn value(&self, field: &str) -> Option<String> { self.state.lock().unwrap().get(field).cloned() }
    }

    /// Test that once a predicate matches an entity, that entity continues to be watched
    /// by the ReactorSubscriptionId until the user explicitly unwatches it
    #[test]
    fn test_entity_remains_watched_after_predicate_stops_matching() {
        let reactor = Reactor::<TestEntity, TestEvent>::new();

        // Set up a subscription with a predicate that matches status="pending"
        let rsub = reactor.subscribe();
        let (w, check) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
        let _guard = rsub.subscribe(w); // ReactorSubscription needs to implement ankurah_signals::Subscribe

        let predicate_id = PredicateId::new();
        let collection_id = CollectionId::fixed_name("album");
        let predicate = "status = 'pending'".try_into().unwrap();
        let entity1 = TestEntity::new("Test Album", "pending");
        let resultset = EntityResultSet::empty();

        reactor.add_predicate(rsub.id(), predicate_id, collection_id, predicate, resultset, vec![entity1.clone()], 0).unwrap();

        // something like this
        assert_eq!(
            check(),
            vec![ReactorUpdate {
                items: vec![ReactorUpdateItem {
                    entity: entity1.clone(),
                    events: vec![],
                    entity_subscribed: true,
                    predicate_relevance: vec![(predicate_id, MembershipChange::Initial)],
                }],
                initialized_predicate: Some((predicate_id, 0)),
            }]
        );

        // TODO: For now, this test validates the setup. The actual notify_change test
        // will require fixing the remaining compilation issues with Entity creation
        // and the generic type constraints.

        // The key behavior we want to test:
        // 1. When notify_change is called with an entity that no longer matches the predicate
        // 2. The Predicate watcher should be removed (entity no longer matches)
        // 3. The Subscription watcher should remain (entity should stay watched)
    }

    // TODO: Add more test cases:
    // 2. A watched entity _shall not_ become unwatched simply because a predicate stops matching
    //    (partially covered above, but could be more explicit)
    // 3. When the user expressly requests (via a pub method on reactor) that an entity be unwatched,
    //    that request should be ignored if any predicates on that subscription still match the entity
    // 4. Test consolidation of multiple predicates from same subscription in notify_change
    // 5. Test that wildcard watchers work correctly
    // 6. Test index_watchers for field-specific comparisons
    // 7. Test proper cleanup when unsubscribing (all watchers removed)
    // 8. Test multiple subscriptions watching the same entity
}
