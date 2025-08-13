mod comparison_index;
mod subscription;
mod update;

use self::{
    comparison_index::ComparisonIndex,
    subscription::{ReactorSubscription, ReactorSubscriptionId},
    update::ReactorUpdate,
};

use crate::{
    changes::{ChangeSet, EntityChange, ItemChange},
    collectionset::CollectionSet,
    entity::{Entity, WeakEntitySet},
    policy::PolicyAgent,
    resultset::ResultSet,
    retrieval::LocalRetriever,
    storage::StorageEngine,
    util::{safemap::SafeMap, safeset::SafeSet},
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

// Type alias for experimenting with different watcher ID approaches
// Option 1: Just PredicateId
// type WatcherId = proto::PredicateId;
// Option 2: Tuple of (SubscriptionId, PredicateId) - currently active
type WatcherId = (ReactorSubscriptionId, proto::PredicateId);

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of entities
pub struct Reactor<SE, PA>(Arc<ReactorInner<SE, PA>>);

struct ReactorInner<SE, PA> {
    state: Mutex<ReactorState>,

    /// Reference to the storage engine
    collections: CollectionSet<SE>,
    entityset: WeakEntitySet,
    _policy_agent: PA,
}

struct ReactorState {
    subscriptions: HashMap<ReactorSubscriptionId, SubscriptionState>,
    predicates: HashMap<proto::PredicateId, PredicateState>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as change
    index_watchers: HashMap<(proto::CollectionId, FieldId), ComparisonIndex<WatcherId>>,
    /// The set of watchers who want to be notified of any changes to a given collection
    wildcard_watchers: HashMap<proto::CollectionId, HashSet<WatcherId>>,
    /// Index of subscriptions that presently match each entity, either by predicate or by entity subscription.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: HashMap<ankurah_proto::EntityId, HashSet<ReactorSubscriptionId>>,
}

/// State for a single predicate within a subscription
#[derive(Debug)]
struct PredicateState {
    pub(crate) subscription_id: ReactorSubscriptionId,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) predicate: ankql::ast::Predicate,
    pub(crate) initialized: bool,
    // TODO: Change from Vec<Entity> to HashSet<EntityId> to avoid storing entities twice
    pub(crate) matching_entities: Vec<Entity>,
}

struct SubscriptionState {
    pub(crate) id: ReactorSubscriptionId,
    // TODO: Replace SafeMap with Mutex<HashMap>
    pub(crate) predicates: HashMap<proto::PredicateId, PredicateState>,
    // TODO: entity_subscriptions tracks explicitly subscribed entities (via add_entity_subscription)
    pub(crate) entity_subscriptions: HashSet<proto::EntityId>,
    // TODO: Add entities field to store all entities (from predicates or direct subscriptions)
    // pub(crate) entities: Mutex<HashMap<proto::EntityId, Entity>>,
    // TODO: Add callback field temporarily until signals are implemented (should take ReactorUpdate)
    // pub(crate) callback: Box<dyn Fn(ReactorUpdate) + Send + Sync>,
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
impl<SE, PA> Clone for Reactor<SE, PA> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<SE, PA> Reactor<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(collections: CollectionSet<SE>, entityset: WeakEntitySet, policy_agent: PA) -> Self {
        Self(Arc::new(ReactorInner {
            state: Mutex::new(ReactorState {
                subscriptions: HashMap::new(),
                predicates: HashMap::new(),
                index_watchers: HashMap::new(),
                wildcard_watchers: HashMap::new(),
                entity_watchers: HashMap::new(),
            }),
            collections,
            entityset,
            _policy_agent: policy_agent,
        }))
    }

    /// Create a new subscription container
    pub fn subscribe(&self) -> ReactorSubscription {
        let subscription = SubscriptionState::new();
        let subscription_id = subscription.id;
        self.0.state.lock().unwrap().subscriptions.insert(subscription_id, subscription);

        ReactorSubscription::new(subscription_id, Box::new(self.clone()))
    }

    /// Add a predicate to a subscription
    pub fn add_predicate(
        &self,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        predicate: ankql::ast::Predicate,
    ) {
        let mut state = self.0.state.lock().unwrap();

        // Create predicate state
        let predicate_state = PredicateState {
            subscription_id,
            collection_id: collection_id.clone(),
            predicate: predicate.clone(),
            initialized: false,
            matching_entities: Vec::new(),
        };

        // Add to global predicates map
        state.predicates.insert(predicate_id, predicate_state.clone());

        // Add to subscription's predicate list
        if let Some(subscription) = state.subscriptions.get_mut(&subscription_id) {
            subscription.predicates.insert(predicate_id, predicate_state);
        }

        // Set up watchers
        let watcher_id = (subscription_id, predicate_id);
        state.manage_watchers_recurse(collection_id, &predicate, watcher_id, WatcherOp::Add);
    }

    /// Remove a predicate from a subscription
    pub fn remove_predicate(&self, subscription_id: ReactorSubscriptionId, predicate_id: proto::PredicateId) {
        let mut state = self.0.state.lock().unwrap();

        // Get predicate state before removing
        if let Some(predicate_state) = state.predicates.get(&predicate_id) {
            // Remove from watchers
            let watcher_id = (subscription_id, predicate_id);
            state.manage_watchers_recurse(&predicate_state.collection_id, &predicate_state.predicate, watcher_id, WatcherOp::Remove);

            // Remove from entity watchers
            for entity in predicate_state.matching_entities.iter() {
                if let Some(watchers) = state.entity_watchers.get_mut(&entity.id) {
                    watchers.remove(&subscription_id);
                }
            }
        }

        // Remove from global predicates map
        state.predicates.remove(&predicate_id);

        // Remove from subscription's predicate list
        if let Some(subscription) = state.subscriptions.get_mut(&subscription_id) {
            subscription.predicates.remove(&predicate_id);
        }
    }

    /// Add entity subscriptions to a subscription
    pub fn add_entity_subscriptions(&self, subscription_id: ReactorSubscriptionId, entity_ids: impl IntoIterator<Item = proto::EntityId>) {
        let mut state = self.0.state.lock().unwrap();
        if let Some(subscription) = state.subscriptions.get_mut(&subscription_id) {
            for entity_id in entity_ids {
                subscription.entity_subscriptions.insert(entity_id);
                // Also add to entity watchers
                state.entity_watchers.entry(entity_id).or_default().insert(subscription_id);
            }
        }
    }

    /// Remove entity subscriptions from a subscription
    pub fn remove_entity_subscriptions(
        &self,
        subscription_id: ReactorSubscriptionId,
        entity_ids: impl IntoIterator<Item = proto::EntityId>,
    ) {
        let mut state = self.0.state.lock().unwrap();
        if let Some(subscription) = state.subscriptions.get_mut(&subscription_id) {
            for entity_id in entity_ids {
                subscription.entity_subscriptions.remove(&entity_id);

                // TODO: Check if any predicates match this entity before removing from entity_watchers
                // For now, only remove if no predicates match
                let should_remove = !state
                    .predicates
                    .values()
                    .filter(|p| p.subscription_id == subscription_id)
                    .any(|p| p.matching_entities.iter().any(|e| e.id == entity_id));

                if should_remove {
                    if let Some(watchers) = state.entity_watchers.get_mut(&entity_id) {
                        watchers.remove(&subscription_id);
                        if watchers.is_empty() {
                            state.entity_watchers.remove(&entity_id);
                        }
                    }
                }
            }
        }
    }

    // TODO: Build ReactorUpdate with MembershipChange::Initial for each matching entity
    // TODO: Call add_entity_subscriptions for all returned entities
    /// Initialize a specific predicate by performing initial evaluation
    /// Initialize a subscription by performing initial evaluation and calling the callback
    /// This is async and does the initial fetch and evaluation
    pub async fn initialize(
        &self,
        subscription: &ReactorSubscription,
        predicate_id: proto::PredicateId,
        initial_states: Vec<Attested<EntityState>>,
    ) -> anyhow::Result<()> {
        // Find initial matching entities
        let storage_collection = self.0.collections.get(&subscription.collection_id()).await?;
        let mut matching_entities = Vec::new();

        // in theory, any state that is in the collection should already have its events in the storage collection as well
        let retriever = LocalRetriever::new(storage_collection.clone());
        // Convert states to Entity and filter by predicate
        for state in states {
            let (_, entity) = self
                .entityset
                .with_state(&retriever, state.payload.entity_id, subscription.collection_id.to_owned(), state.payload.state)
                .await?;

            // Evaluate predicate for each entity
            if ankql::selection::filter::evaluate_predicate(&entity, &subscription.predicate).unwrap_or(false) {
                matching_entities.push(entity.clone());

                // Set up entity watchers
                self.entity_watchers.set_insert(entity.id, subscription.id);
            }
        }

        // Update subscription's matching entities
        *subscription.matching_entities.lock().unwrap() = matching_entities.clone();

        // Mark subscription as initialized
        subscription.initialized.store(true, Ordering::SeqCst);

        // Call callback with initial state
        (subscription.callback)(ChangeSet {
            changes: matching_entities.iter().map(|entity| ItemChange::Initial { item: entity.clone() }).collect(),
            resultset: ResultSet { loaded: true, items: matching_entities.clone() },
        });

        Ok(())
    }

    /// Remove a subscription and all its predicates
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(sub_id = %sub_id)))]
    pub(crate) fn unsubscribe(&self, sub_id: ReactorSubscriptionId) {
        let mut state = self.0.state.lock().unwrap();
        if let Some(sub) = state.subscriptions.remove(&sub_id) {
            // Remove all predicates from watchers
            for (predicate_id, predicate_state) in sub.predicates.iter() {
                // Remove from index watchers
                let watcher_id = (sub_id, *predicate_id);
                state.manage_watchers_recurse(&predicate_state.collection_id, &predicate_state.predicate, watcher_id, WatcherOp::Remove);

                // Remove from entity watchers using predicate's matching entities
                for entity in predicate_state.matching_entities.iter() {
                    if let Some(watchers) = state.entity_watchers.get_mut(&entity.id) {
                        watchers.remove(&sub_id);
                    }
                }

                // Also remove from global predicates map
                state.predicates.remove(predicate_id);
            }
        }
    }

    pub(crate) fn unsubscribe_predicate(&self, predicate_id: proto::PredicateId) {
        // Get predicate state to find its subscription
        if let Some(predicate_state) = self.0.predicates.remove(&predicate_id) {
            let watcher_id = (predicate_state.subscription_id, predicate_id);
            // Remove from index watchers
            self.manage_watchers_recurse(&predicate_state.collection_id, &predicate_state.predicate, watcher_id, WatcherOp::Remove);

            // Remove from subscription's predicate list
            if let Some(sub) = self.0.subscriptions.get(&predicate_state.subscription_id) {
                sub.remove_predicate(&predicate_id);
            }

            // Remove from entity watchers
            for entity in predicate_state.matching_entities.iter() {
                self.0.entity_watchers.set_remove(&entity.id, &predicate_state.subscription_id);
            }
        }
    }

    /// Update entity watchers when an entity's matching status changes
    fn update_entity_watchers(&self, entity: &Entity, matching: bool, subscription_id: ReactorSubscriptionId) {
        // This is handled by update_predicate_matching_entities
        // We just need to update the global entity watchers
        if matching {
            self.0.entity_watchers.set_insert(entity.id, subscription_id);
        } else {
            self.0.entity_watchers.set_remove(&entity.id, &subscription_id);
        }
    }

    /// Update predicate matching entities when an entity's matching status changes
    fn update_predicate_matching_entities(
        &self,
        subscription: &Arc<SubscriptionState>,
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

    // TODO: Should add (but not remove) entity subscriptions for changed entities
    // TODO: Build ReactorUpdate with ReactorUpdateItem containing predicate_relevance
    // TODO: Temporarily restore callback with ReactorUpdate (not ChangeSet)
    /// Notify subscriptions about an entity change
    pub fn notify_change(&self, changes: Vec<EntityChange>) {
        // pretty format self
        debug!("Reactor.notify_change({:?})", changes);
        // TODO: Change to build ReactorUpdate instead of using ItemChange
        // Group changes by subscription
        let mut sub_changes: std::collections::HashMap<ReactorSubscriptionId, Vec<ItemChange<Entity>>> = std::collections::HashMap::new();

        for change in changes {
            let (entity, events) = change.into_parts();

            // Collect all potentially interested (subscription_id, predicate_id) pairs
            let mut possibly_interested_watchers: HashSet<WatcherId> = HashSet::new();

            debug!("Reactor - index watchers: {:?}", self.0.index_watchers);
            // Find subscriptions that might be interested based on index watchers
            for ((collection_id, field_id), index_ref) in self.0.index_watchers.to_vec() {
                // Get the field value from the entity
                if collection_id == entity.collection {
                    if let Some(field_value) = entity.value(&field_id.0) {
                        possibly_interested_watchers.extend(index_ref.read().unwrap().find_matching(Value::String(field_value)));
                    }
                }
            }

            // Also check wildcard watchers for this collection
            if let Some(watchers) = self.0.wildcard_watchers.get(&entity.collection) {
                for watcher in watchers.read().unwrap().to_vec() {
                    possibly_interested_watchers.insert(watcher);
                }
            }

            // Check entity watchers - these are subscription-level, not predicate-level
            // We'll need to expand these to all predicates for the subscription
            if let Some(subscription_ids) = self.0.entity_watchers.get(&entity.id()) {
                for sub_id in subscription_ids.iter() {
                    // Get all predicates for this subscription
                    if let Some(sub) = self.0.subscriptions.get(sub_id) {
                        for predicate_id in sub.predicates.keys() {
                            possibly_interested_watchers.insert((*sub_id, *predicate_id));
                        }
                    }
                }
            }

            debug!(" possibly_interested_watchers: {possibly_interested_watchers:?}");
            // Check each possibly interested subscription-predicate pair with full predicate evaluation
            for (subscription_id, predicate_id) in possibly_interested_watchers {
                if let Some(subscription) = self.0.subscriptions.get(&subscription_id) {
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

                    // Update entity watchers and predicate matching entities
                    self.update_entity_watchers(&entity, matches, subscription_id);
                    self.update_predicate_matching_entities(&subscription, predicate_id, &entity, matches);

                    // TODO: Replace with MembershipChange calculation
                    // Determine the membership change for this predicate
                    let new_change: Option<ItemChange<Entity>> = if matches != did_match {
                        // Matching status changed
                        Some(if matches {
                            // TODO: Should be MembershipChange::Add (or Initial if first time)
                            ItemChange::Add { item: entity.clone(), events: events.clone() }
                        } else {
                            // TODO: Should be MembershipChange::Remove
                            ItemChange::Remove { item: entity.clone(), events: events.clone() }
                        })
                    } else if matches {
                        // Entity still matches but was updated
                        // TODO: No MembershipChange or predicate_relevance, but it should be include in ReactorUpdateItem
                        //       This should probably be done outside of the matches check, because it has to work for entity_subscriptions as well
                        Some(ItemChange::Update { item: entity.clone(), events: events.clone() })
                    } else {
                        // Entity didn't match before and still doesn't match
                        None
                    };

                    // TODO: Build ReactorUpdateItem with predicate_relevance instead
                    // Add the change to the subscription's changes if there is one
                    if let Some(new_change) = new_change {
                        sub_changes.entry(subscription_id).or_default().push(new_change);
                    }
                }
            }
        }

        // TODO: Build and send ReactorUpdate instead of ChangeSet
        // TODO: Temporarily restore callback here with:
        // (subscription.callback)(ReactorUpdate { items: reactor_update_items })
        // Send batched notifications
        for (sub_id, changes) in sub_changes {
            if let Some(subscription) = self.0.subscriptions.get(&sub_id) {
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

                subscription.notify(ChangeSet { resultset: ResultSet { loaded: true, items: all_matching_entities }, changes });
            }
        }
    }

    /// Notify all subscriptions that their entities have been removed but do not remove the subscriptions
    pub fn system_reset(&self) {
        // Collect all current subscriptions and their matching entities
        let subs = self.subscriptions.to_vec();
        self.0.entity_watchers.clear();

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

                subscription.notify(ChangeSet { resultset: ResultSet { loaded: true, items: all_matching_entities }, changes });
            }
        }
    }
}

impl<SE, PA> std::fmt::Debug for Reactor<SE, PA> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Reactor {{ subscriptions: {:?}, index_watchers: {:?}, wildcard_watchers: {:?}, entity_watchers: {:?} }}",
            self.0.subscriptions, self.0.index_watchers, self.0.wildcard_watchers, self.0.entity_watchers
        )
    }
}

impl ReactorState {
    fn manage_watchers_recurse(
        &mut self,
        collection_id: &proto::CollectionId,
        predicate: &ankql::ast::Predicate,
        watcher_id: WatcherId,
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
                self.manage_watchers_recurse(collection_id, left, watcher_id, op);
                self.manage_watchers_recurse(collection_id, right, watcher_id, op);
            }
            Predicate::Not(pred) => {
                self.manage_watchers_recurse(collection_id, pred, watcher_id, op);
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
            // broadcast: Broadcast::new(),
            // listeners: Mutex::new(Vec::new()),
        }
    }

    fn notify(&self, update: ReactorUpdate) { todo!("implement signals (after the Reactor is fully cleaned up)") }
}

impl std::fmt::Debug for SubscriptionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription {{ id: {:?}, predicates: {} }}", self.id, self.predicates.len())
    }
}
