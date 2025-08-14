mod comparison_index;
mod subscription;
mod update;

use self::{
    comparison_index::ComparisonIndex,
    subscription::{ReactorSubscription, ReactorSubscriptionId},
    update::{MembershipChange, ReactorUpdate, ReactorUpdateItem},
};

use crate::{
    changes::{ChangeSet, EntityChange, ItemChange},
    entity::{Entity, WeakEntitySet},
    error::SubscriptionError,
    policy::PolicyAgent,
    resultset::ResultSet,
    retrieval::LocalRetriever,
    storage::{StorageCollection, StorageCollectionWrapper, StorageEngine},
    value::Value,
};
use ankql::selection::filter::Filterable;
use ankurah_proto::{self as proto, Attested, EntityState};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

#[cfg(feature = "instrument")]
use tracing::instrument;
use tracing::{debug, warn};

/// Trait for entities that can be used in reactor notifications
pub trait ReactorEntity: ankql::selection::filter::Filterable + Clone + std::fmt::Debug {
    fn collection(&self) -> proto::CollectionId;
    fn id(&self) -> proto::EntityId;
    fn value(&self, field: &str) -> Option<String>;
}

/// Trait for types that can be used in notify_change
pub trait ChangeNotification {
    type Entity: ReactorEntity;
    type Event: Clone + std::fmt::Debug;

    fn into_parts(self) -> (Self::Entity, Vec<Self::Event>);
}

// Implement ReactorEntity for Entity
impl ReactorEntity for Entity {
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
pub struct Reactor(Arc<ReactorInner>);

struct ReactorInner {
    subscriptions: Mutex<HashMap<ReactorSubscriptionId, SubscriptionState>>,
    watcher_set: Mutex<WatcherSet>,
    entityset: WeakEntitySet,
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
struct PredicateState {
    pub(crate) subscription_id: ReactorSubscriptionId,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) predicate: ankql::ast::Predicate,
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) initialized: bool,
    pub(crate) matching_entities: HashSet<proto::EntityId>,
}

struct SubscriptionState {
    pub(crate) id: ReactorSubscriptionId,
    pub(crate) predicates: HashMap<proto::PredicateId, PredicateState>,
    pub(crate) entity_subscriptions: HashSet<proto::EntityId>,
    pub(crate) entities: HashMap<proto::EntityId, Entity>,
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
impl Clone for Reactor {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl Reactor {
    pub fn new(entityset: WeakEntitySet) -> Self {
        Self(Arc::new(ReactorInner {
            subscriptions: Mutex::new(HashMap::new()),
            watcher_set: Mutex::new(WatcherSet {
                index_watchers: HashMap::new(),
                wildcard_watchers: HashMap::new(),
                entity_watchers: HashMap::new(),
            }),
            entityset,
        }))
    }

    /// Create a new subscription container
    pub fn subscribe(&self) -> ReactorSubscription {
        let subscription = SubscriptionState::new();
        let subscription_id = subscription.id;
        self.0.subscriptions.lock().unwrap().insert(subscription_id, subscription);
        ReactorSubscription::new(subscription_id, Box::new(self.clone()))
    }

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
                for entity_id in predicate_state.matching_entities {
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

    /// Add a predicate to a subscription
    pub fn add_predicate(
        &self,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> Result<(), SubscriptionError> {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();
        let mut watcher_state = self.0.watcher_set.lock().unwrap();

        // Add the predicate to the subscription
        if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
            use std::collections::hash_map::Entry;
            match subscription.predicates.entry(predicate_id) {
                Entry::Vacant(v) => {
                    v.insert(PredicateState {
                        subscription_id,
                        collection_id: collection_id.clone(),
                        predicate: predicate.clone(),
                        initialized: false,
                        matching_entities: HashSet::new(),
                    });
                }
                Entry::Occupied(o) => {
                    return Err(SubscriptionError::PredicateAlreadySubscribed);
                }
            };
        } else {
            return Err(SubscriptionError::SubscriptionNotFound);
        }

        // Set up watchers
        let watcher_id = (subscription_id, predicate_id);
        watcher_state.recurse_predicate_watchers(collection_id, &predicate, watcher_id, WatcherOp::Add);

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
                let should_remove = !subscription.predicates.values().any(|p| p.matching_entities.contains(&entity_id));

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

    /// Initialize a specific predicate by performing initial evaluation
    /// This is async and does the initial fetch and evaluation
    pub async fn initialize(
        &self,
        storage_collection: StorageCollectionWrapper,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        initial_states: Vec<Attested<EntityState>>,
    ) -> anyhow::Result<()> {
        let (collection_id, predicate) = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            let subscription =
                subscriptions.get(&subscription_id).ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?;
            let predicate_state =
                subscription.predicates.get(&predicate_id).ok_or_else(|| anyhow::anyhow!("Predicate {:?} not found", predicate_id))?;
            (predicate_state.collection_id.clone(), predicate_state.predicate.clone())
        };

        // Find initial matching entities
        let mut matching_entities = Vec::new();
        let mut matching_entity_ids = Vec::new();

        // in theory, any state that is in the collection should already have its events in the storage collection as well
        let retriever = LocalRetriever::new(storage_collection.clone());

        // Convert states to Entity and filter by predicate
        for state in initial_states {
            let (_, entity) =
                self.0.entityset.with_state(&retriever, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;

            // Evaluate predicate for each entity
            if ankql::selection::filter::evaluate_predicate(&entity, &predicate).unwrap_or(false) {
                matching_entity_ids.push(entity.id);
                matching_entities.push(entity.clone());
            }
        }

        // Update state with matching entities and mark as initialized
        {
            let mut subscriptions = self.0.subscriptions.lock().unwrap();
            let mut watcher_state = self.0.watcher_set.lock().unwrap();

            // Update subscription's predicate state
            if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
                if let Some(pred_state) = subscription.predicates.get_mut(&predicate_id) {
                    pred_state.matching_entities = matching_entity_ids.iter().cloned().collect();
                    pred_state.initialized = true;
                }
            }

            // Set up entity watchers for matching entities
            for entity_id in &matching_entity_ids {
                let entity_watcher = watcher_state.entity_watchers.entry(*entity_id).or_default();
                // same as in notify_change, we need to add a watcher for the subscription itself, which lingers
                // until the user expressly tells us they want to stop watching
                // and one for the predicate that gets removed when the predicate no longer matches
                entity_watcher.insert(EntityWatcherId::Subscription(subscription_id));
                entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, predicate_id));
            }
        }

        // Call add_entity_subscriptions for all returned entities
        self.add_entity_subscriptions(subscription_id, matching_entity_ids.clone());

        // Build ReactorUpdate with MembershipChange::Initial for each matching entity
        let reactor_update_items: Vec<ReactorUpdateItem> = matching_entities
            .into_iter()
            .map(|entity| ReactorUpdateItem {
                entity,
                events: vec![],           // No events for initial state
                entity_subscribed: false, // These are predicate matches, not direct entity subscriptions
                predicate_relevance: vec![(predicate_id, MembershipChange::Initial)],
            })
            .collect();

        let reactor_update = ReactorUpdate { items: reactor_update_items };

        // Notify the subscription
        {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            if let Some(subscription) = subscriptions.get(&subscription_id) {
                subscription.notify(reactor_update);
            }
        }

        Ok(())
    }

    /// Update predicate matching entities when an entity's matching status changes
    /// We need to keep a list of matching entities for subscription / predicate in order to keep the entity resident in memory
    /// so we don't have to re-fetch it from storage every time (later, make this an LRU cached Entity, but just hold the Entity for now)
    fn update_predicate_matching_entities(
        &self,
        subscription: &SubscriptionState,
        predicate_id: proto::PredicateId,
        entity: &Entity,
        matching: bool,
    ) {
        todo!("it's not obvious to me that this belongs on the SubscriptionState/PredicateState, or rather, it seems weird that we're mixing Subsription Config and Subscription State. Perhaps that's just in my head though");
        // let mut predicates = subscription.predicates.lock().unwrap();
        // if let Some(predicate_state) = predicates.get_mut(&predicate_id) {
        //     let did_match = predicate_state.matching_entities.iter().any(|e| e.id() == entity.id());
        //     match (did_match, matching) {
        //         (false, true) => {
        //             predicate_state.matching_entities.push(entity.clone());
        //         }
        //         (true, false) => {
        //             predicate_state.matching_entities.retain(|r| r.id != entity.id);
        //         }
        //         _ => {} // No change needed
        //     }
        // }
    }

    // TODO: Should add (but not remove) entity subscriptions for changed entities
    // TODO: Build ReactorUpdate with ReactorUpdateItem containing predicate_relevance
    // TODO: Temporarily restore callback with ReactorUpdate (not ChangeSet)
    /// Notify subscriptions about an entity change
    pub fn notify_change<C: ChangeNotification>(&self, changes: Vec<C>) {
        let watcher_set = self.0.watcher_set.lock().unwrap();

        let mut items: std::collections::HashMap<ReactorSubscriptionId, HashMap<proto::EntityId, ReactorUpdateItem<C::Entity, C::Event>>> =
            std::collections::HashMap::new();

        debug!("Reactor.notify_change({:?})", changes);
        for change in changes {
            let (entity, events) = change.into_parts();

            // Collect all potentially interested (subscription_id, predicate_id) pairs
            let mut possibly_interested_watchers: HashSet<(ReactorSubscriptionId, proto::PredicateId)> = HashSet::new();

            debug!("Reactor - index watchers: {:?}", watcher_set.index_watchers);
            // Find subscriptions that might be interested based on index watchers
            for ((collection_id, field_id), index_ref) in watcher_set.index_watchers {
                // Get the field value from the entity
                if collection_id == entity.collection {
                    if let Some(field_value) = entity.value(&field_id.0) {
                        possibly_interested_watchers.extend(index_ref.find_matching(Value::String(field_value)));
                    }
                }
            }

            // Also check wildcard watchers for this collection
            if let Some(watchers) = watcher_set.wildcard_watchers.get(&entity.collection) {
                for watcher in watchers.iter() {
                    possibly_interested_watchers.insert(*watcher);
                }
            }

            // Check entity watchers - these are subscription-level, not predicate-level
            // We'll need to expand these to all predicates for the subscription
            if let Some(subscription_ids) = watcher_set.entity_watchers.get(&entity.id()) {
                for sub_id in subscription_ids.iter() {
                    // Get all predicates for this subscription
                    if let Some(sub) = self.0.subscriptions.lock().unwrap().get(&sub_id.subscription_id()) {
                        for predicate_id in sub.predicates.keys() {
                            possibly_interested_watchers.insert((*sub_id, *predicate_id));
                        }
                    }
                }
            }

            debug!(" possibly_interested_watchers: {possibly_interested_watchers:?}");
            let subscriptions = self.0.subscriptions.lock().unwrap();
            // Check each possibly interested subscription-predicate pair with full predicate evaluation
            for (subscription_id, predicate_id) in possibly_interested_watchers {
                if let Some(subscription) = subscriptions.get(&subscription_id) {
                    // Get the predicate state
                    let (did_match, matches) = {
                        if let Some(predicate_state) = subscription.predicates.get(&predicate_id) {
                            // Skip uninitialized predicates
                            if !predicate_state.initialized {
                                continue;
                            }

                            // Use evaluate_predicate directly on the entity
                            debug!("\tnotify_change predicate: {} {:?}", predicate_id, predicate_state.predicate);
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

                    let entity_watcher = watcher_set.entity_watchers.entry(entity.id).or_default();
                    if matches {
                        // Entity subscriptions are implicit / sticky...
                        // Register one for the predicate that gets removed when the predicate no longer matches
                        entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, predicate_id));
                        // and one for the subscription itself, which lingers until the user expressly tells us they want to stop watching
                        entity_watcher.insert(EntityWatcherId::Subscription(subscription_id));
                    } else {
                        // When the predicate no longer matches, only remove the predicate watcher, not the subscription watcher
                        entity_watcher.remove(&EntityWatcherId::Predicate(subscription_id, predicate_id));
                    }
                    self.update_predicate_matching_entities(&subscription, predicate_id, &entity, matches);

                    let sub_entities = items.entry(subscription_id).or_default();

                    let membership_change = match (did_match, matches) {
                        (true, false) => Some(MembershipChange::Remove),
                        (false, true) => Some(MembershipChange::Add),
                        _ => None,
                    };
                    match sub_entities.entry(entity.id) {
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert(ReactorUpdateItem {
                                entity: entity.clone(),
                                events: events.clone(),
                                entity_subscribed: entity_watcher.contains(&EntityWatcherId::Subscription(subscription_id)),
                                predicate_relevance: membership_change.map(|mc| vec![(predicate_id, mc)]).unwrap_or_default(),
                            });
                        }
                        std::collections::hash_map::Entry::Occupied(o) => {
                            if let Some(mc) = membership_change {
                                o.get_mut().predicate_relevance.push((predicate_id, mc));
                            }
                        }
                    }
                }
            }
        }

        for (sub_id, sub_items) in items {
            if let Some(subscription) = self.0.subscriptions.lock().unwrap().get(&sub_id) {
                subscription.notify(ReactorUpdate { items: sub_items.values().cloned().collect() });
            }
        }
    }

    /// Notify all subscriptions that their entities have been removed but do not remove the subscriptions
    pub fn system_reset(&self) {
        // Clear entity watchers
        {
            let mut watcher_state = self.0.watcher_set.lock().unwrap();
            watcher_state.entity_watchers.clear();
        }

        // Collect all current subscriptions and their matching entities
        let subscriptions_snapshot = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.clone()
        };

        // For each subscription, generate removal notifications for all its entities
        for (_sub_id, mut subscription) in subscriptions_snapshot {
            let mut all_entities = Vec::new();

            // Clear all predicate matching entities and collect them
            for predicate_state in subscription.predicates.values_mut() {
                all_entities.extend(predicate_state.matching_entities.drain(..));
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

                subscription.notify(ChangeSet { resultset: ResultSet { loaded: true, items: all_entities }, changes });
            }
        }
    }
}

impl std::fmt::Debug for Reactor {
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
                    let index = self.index_watchers.entry((collection_id.clone(), field_id)).or_insert_with(|| ComparisonIndex::new());

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
                let set = self.wildcard_watchers.entry(collection_id.clone()).or_insert_with(|| HashSet::new());

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

impl SubscriptionState {
    /// Create a new subscription
    pub fn new() -> Self {
        Self {
            id: ReactorSubscriptionId::new(),
            predicates: HashMap::new(),
            entity_subscriptions: HashSet::new(),
            entities: HashMap::new(),
        }
    }

    fn notify<E, Ev>(&self, update: ReactorUpdate<E, Ev>) { todo!("implement signals (after the Reactor is fully cleaned up)") }
}

impl std::fmt::Debug for SubscriptionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription {{ id: {:?}, predicates: {} }}", self.id, self.predicates.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::WeakEntitySet;
    use proto::PredicateId;

    /// Test that once a predicate matches an entity, that entity continues to be watched
    /// by the ReactorSubscriptionId until the user explicitly unwatches it
    #[test]
    fn test_entity_remains_watched_after_predicate_stops_matching() {
        // Setup: Create a simple reactor
        let entityset = WeakEntitySet::default();
        let reactor = Reactor::new(entityset);

        let task_id = proto::EntityId::new();
        let collection_id = proto::CollectionId::fixed_name("tasks");

        // Set up a subscription with a predicate that matches status="pending"
        let subscription = reactor.subscribe();
        let subscription_id = subscription.id();

        let predicate_id = PredicateId::new();
        reactor.add_predicate(subscription_id, predicate_id, &collection_id, "status = 'pending'".try_into().unwrap()).unwrap();

        // Manually set up the initial state: entity matches predicate and is being watched
        {
            let mut subscriptions = reactor.0.subscriptions.lock().unwrap();
            if let Some(sub) = subscriptions.get_mut(&subscription_id) {
                if let Some(pred_state) = sub.predicates.get_mut(&predicate_id) {
                    pred_state.initialized = true;
                    pred_state.matching_entities.insert(task_id);
                }
            }

            // Add entity watchers - both predicate and subscription watchers
            let mut watcher_set = reactor.0.watcher_set.lock().unwrap();
            let entity_watchers = watcher_set.entity_watchers.entry(task_id).or_default();
            entity_watchers.insert(EntityWatcherId::Predicate(subscription_id, predicate_id));
            entity_watchers.insert(EntityWatcherId::Subscription(subscription_id));
        }

        // Verify initial state: entity is being watched by both predicate and subscription
        {
            let watcher_set = reactor.0.watcher_set.lock().unwrap();
            let watchers = watcher_set.entity_watchers.get(&task_id).unwrap();
            assert!(watchers.contains(&EntityWatcherId::Subscription(subscription_id)));
            assert!(watchers.contains(&EntityWatcherId::Predicate(subscription_id, predicate_id)));
        }

        // TODO: For now, this test validates the setup. The actual notify_change test
        // will require fixing the remaining compilation issues with Entity creation
        // and the generic type constraints.

        // The key behavior we want to test:
        // 1. When notify_change is called with an entity that no longer matches the predicate
        // 2. The Predicate watcher should be removed (entity no longer matches)
        // 3. The Subscription watcher should remain (entity should stay watched)

        println!("Test setup complete - entity is being watched by both predicate and subscription");
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
