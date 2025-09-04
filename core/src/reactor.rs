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
    value::Value,
};
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

struct WatcherSet {
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as change
    index_watchers: HashMap<(proto::CollectionId, FieldId), ComparisonIndex<(ReactorSubscriptionId, proto::QueryId)>>,
    /// The set of watchers who want to be notified of any changes to a given collection
    wildcard_watchers: HashMap<proto::CollectionId, HashSet<(ReactorSubscriptionId, proto::QueryId)>>,
    /// Index of subscriptions that presently match each entity, either by predicate or by entity subscription.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: HashMap<ankurah_proto::EntityId, HashSet<EntityWatcherId>>,
}

/// State for a single predicate within a subscription
#[derive(Debug, Clone)]
struct QueryState<E: AbstractEntity> {
    // TODO make this a clonable PredicateSubscription and store it instead of the channel?
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) selection: ankql::ast::Selection,
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) paused: bool, // When true, skip notifications (used during initialization and updates)
    pub(crate) resultset: EntityResultSet<E>,
    pub(crate) version: u32,
}

struct SubscriptionState<E: AbstractEntity, Ev> {
    pub(crate) id: ReactorSubscriptionId,
    pub(crate) queries: HashMap<proto::QueryId, QueryState<E>>,
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
            queries: HashMap::new(),
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
            for (query_id, query_state) in sub.queries {
                // Remove from index watcher
                watcher_set.recurse_predicate_watchers(
                    &query_state.collection_id,
                    &query_state.selection.predicate,
                    (sub_id, query_id),
                    WatcherOp::Remove,
                );

                // Remove from entity watchers using predicate's matching entities
                for entity_id in query_state.resultset.keys() {
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
    pub fn remove_predicate(&self, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId) -> Result<(), SubscriptionError> {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();
        let mut watcher_state = self.0.watcher_set.lock().unwrap();

        // remove the predicate from the subscription
        let query_state = match subscriptions.get_mut(&subscription_id) {
            Some(sub) => match sub.queries.remove(&query_id) {
                Some(p) => p,
                None => return Err(SubscriptionError::PredicateNotFound),
            },
            None => return Err(SubscriptionError::SubscriptionNotFound),
        };

        // Remove from watchers
        let watcher_id = (subscription_id, query_id);
        watcher_state.recurse_predicate_watchers(
            &query_state.collection_id,
            &query_state.selection.predicate,
            watcher_id,
            WatcherOp::Remove,
        );

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
                let should_remove = !subscription.queries.values().any(|p| p.resultset.contains_key(&entity_id));

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
    fn update_query_matching_entities(subscription: &mut SubscriptionState<E, Ev>, query_id: proto::QueryId, entity: &E, matching: bool) {
        if let Some(predicate_state) = subscription.queries.get_mut(&query_id) {
            let entity_id = AbstractEntity::id(entity);
            let did_match = predicate_state.resultset.contains_key(&entity_id);

            match (did_match, matching) {
                (false, true) => {
                    // Entity now matches - add to matching set and cache the entity
                    tracing::info!(
                        "PUSH entity {} to predicate resultset {} len: {}",
                        entity_id,
                        query_id,
                        predicate_state.resultset.len()
                    );
                    predicate_state.resultset.push(entity.clone());
                    subscription.entities.insert(entity_id, entity.clone());
                }
                (true, false) => {
                    tracing::info!("REMOVE entity {} from predicate resultset {}", entity_id, query_id);
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
    /// Add a new predicate to a subscription (initial subscription only)
    /// Fails if query_id already exists - use update_query for updates
    /// The resultset must be pre-populated with entities that match the predicate
    pub fn add_query(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        resultset: EntityResultSet<E>,
    ) -> anyhow::Result<()> {
        let mut reactor_update_items: Vec<ReactorUpdateItem<E, Ev>> = Vec::new();

        // Create new predicate state and process entities in single subscriptions lock
        let mut subscriptions = self.0.subscriptions.lock().expect("failed to lock subscriptions");
        let subscription =
            subscriptions.get_mut(&subscription_id).ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?;

        // Fail if predicate already exists, otherwise insert and get mutable reference
        use std::collections::hash_map::Entry;
        let pred_state = match subscription.queries.entry(query_id) {
            Entry::Vacant(v) => v.insert(QueryState {
                collection_id: collection_id.clone(),
                selection: selection.clone(),
                paused: false,
                resultset: resultset.clone(),
                version: 0,
            }),
            Entry::Occupied(_) => return Err(anyhow::anyhow!("Predicate {:?} already exists", query_id)),
        };

        // Set up predicate watchers
        let mut watcher_state = self.0.watcher_set.lock().unwrap();
        let watcher_id = (subscription_id, query_id);
        watcher_state.recurse_predicate_watchers(&collection_id, &selection.predicate, watcher_id, WatcherOp::Add);

        // Process entities from the pre-populated resultset
        let read_guard = pred_state.resultset.read();
        for (entity_id, entity) in read_guard.iter_entities() {
            subscription.entity_subscriptions.insert(entity_id);
            let entity_watcher = watcher_state.entity_watchers.entry(entity_id).or_default();
            // entity_watcher.insert(EntityWatcherId::Subscription(subscription_id)); // Uncomment this when we have unsubscribe on drop from the peer
            entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, query_id));
            reactor_update_items.push(ReactorUpdateItem {
                entity: entity.clone(),
                events: vec![],
                entity_subscribed: true,
                predicate_relevance: vec![(query_id, MembershipChange::Initial)],
            });
        }
        drop(watcher_state);
        drop(read_guard);

        // pred_state.paused = false; // Unpause now that initialization is complete

        let broadcast = subscription.broadcast.clone(); // clone the broadcast to eliminate the potential deadlock
        drop(subscriptions); // Drop subscriptions lock

        resultset.set_loaded(true);
        broadcast.send(ReactorUpdate { items: reactor_update_items, initialized_query: Some((query_id, 0)) });

        Ok(())
    }

    /// Pause a predicate from receiving notifications - gets unpaused by update_query
    pub fn pause_query(&self, query_id: proto::QueryId) {
        let mut subscriptions = self.0.subscriptions.lock().unwrap();

        // Find the subscription that has this predicate
        for subscription in subscriptions.values_mut() {
            if let Some(pred_state) = subscription.queries.get_mut(&query_id) {
                pred_state.paused = true;
            }
        }
    }

    /// Update an existing predicate (v>0)
    /// Does diffing against the current resultset
    pub fn update_query(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
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
        let query_state = subscription.queries.get_mut(&query_id).ok_or_else(|| anyhow::anyhow!("Predicate not found for update"))?;

        // Update the predicate AST
        let old_query = query_state.selection.clone();
        query_state.selection = selection.clone();

        // Update index watchers if predicate changed
        let watcher_id = (subscription_id, query_id);
        watcher_state.recurse_predicate_watchers(&collection_id, &old_query.predicate, watcher_id, WatcherOp::Remove);
        watcher_state.recurse_predicate_watchers(&collection_id, &selection.predicate, watcher_id, WatcherOp::Add);

        // Create write guard for atomic updates
        let mut rw_resultset = query_state.resultset.write();
        let mut reactor_update_items = Vec::new();

        // Process included entities (only truly new ones from remote)
        for entity in included_entities {
            // I think we can trust that the included_entities are already known to match the predicate, so we probably don't need to evaluate it here?
            // it would be nice to not assume that though in case of race conditions - but that gets tricky with order by + limit scenarios
            // We know that the server doesn't presently include the full set of entities that match when updating a predicate.
            // I don't know if we can get away with that or not. I suspect that we may need to fully re-query in LiveQuery.update_selection when an orderby/limit is involved.
            if ankql::selection::filter::evaluate_predicate(&entity, &selection.predicate).unwrap_or(false) {
                let entity_id = AbstractEntity::id(&entity);

                // Check if this is truly new to the resultset
                if !rw_resultset.contains(&entity_id) {
                    // Add to write guard
                    rw_resultset.add(entity.clone());

                    // Add to subscription entities map
                    subscription.entities.insert(entity_id, entity.clone());

                    // Set up entity watchers
                    subscription.entity_subscriptions.insert(entity_id);
                    let entity_watcher = watcher_state.entity_watchers.entry(entity_id).or_default();
                    entity_watcher.insert(EntityWatcherId::Subscription(subscription_id));
                    entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, query_id));

                    // Create reactor update item (Initial for truly new entities)
                    reactor_update_items.push(ReactorUpdateItem {
                        entity,
                        events: vec![],
                        entity_subscribed: true,
                        predicate_relevance: vec![(query_id, MembershipChange::Initial)],
                    });
                }
            }
        }

        // Remove entities that no longer match the new predicate
        let mut to_remove = Vec::new();
        for (entity_id, entity) in rw_resultset.iter_entities() {
            // same quest as above for wisdom of calling this here in the case of an orderby/limit scenario
            // versus updating LiveQuery to re-query the local storage after all relevant events/states have been applied
            if !ankql::selection::filter::evaluate_predicate(entity, &selection.predicate).unwrap_or(false) {
                tracing::debug!("Entity {:?} no longer matches predicate", entity_id);

                // If emit_removes is true, create a Remove event for local subscriptions
                if emit_removes {
                    tracing::debug!("Creating Remove event for entity {:?}", entity_id);
                    reactor_update_items.push(ReactorUpdateItem {
                        entity: entity.clone(),
                        events: vec![],
                        entity_subscribed: false,
                        predicate_relevance: vec![(query_id, MembershipChange::Remove)],
                    });
                }

                to_remove.push(entity_id);
            }
        }
        tracing::info!("Removing {} entities that no longer match", to_remove.len());

        // Remove non-matching entities from the resultset and clean up watchers
        for entity_id in to_remove {
            rw_resultset.remove(entity_id);

            // Clean up entity predicate watcher (but keep subscription watcher)
            if let Some(entity_watcher) = watcher_state.entity_watchers.get_mut(&entity_id) {
                entity_watcher.remove(&EntityWatcherId::Predicate(subscription_id, query_id));

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
        query_state.paused = false;
        query_state.version = version;
        query_state.resultset.set_loaded(true);

        // Drop write guard to apply changes
        drop(rw_resultset);

        // Send reactor update
        let reactor_update = ReactorUpdate::<E, Ev> { items: reactor_update_items, initialized_query: Some((query_id, version)) };
        subscription.notify(reactor_update);

        Ok(())
    }

    /// Notify subscriptions about an entity change
    pub fn notify_change<C: ChangeNotification<Entity = E, Event = Ev>>(&self, changes: Vec<C>) {
        let mut watcher_set = self.0.watcher_set.lock().unwrap();

        let mut items: std::collections::HashMap<ReactorSubscriptionId, IndexMap<proto::EntityId, ReactorUpdateItem<E, Ev>>> =
            std::collections::HashMap::new();

        debug!("Reactor.notify_change({:?})", changes);
        for change in changes {
            let (entity, events) = change.into_parts();

            // Collect all potentially interested (subscription_id, query_id) pairs
            let mut possibly_interested_watchers: HashSet<(ReactorSubscriptionId, proto::QueryId)> = HashSet::new();

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
                        for query_id in sub.queries.keys() {
                            possibly_interested_watchers.insert((sub_id.subscription_id(), *query_id));
                            entity_subscribed.insert(*query_id);
                        }
                    }
                }
            }

            debug!(" possibly_interested_watchers: {possibly_interested_watchers:?}");
            let mut subscriptions = self.0.subscriptions.lock().unwrap();
            // Check each possibly interested subscription-predicate pair with full predicate evaluation
            for (subscription_id, query_id) in possibly_interested_watchers {
                if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
                    // Get the predicate state
                    let (did_match, matches) = {
                        if let Some(query_state) = subscription.queries.get(&query_id) {
                            // Skip paused predicates
                            if query_state.paused {
                                continue;
                            }

                            debug!("\tnotify_change predicate: {} {:?}", query_id, query_state.selection);
                            // So the other calls to evaluate_predicate probably require re-querying the local storage after all relevant events/states have
                            // been applied by the applier - but this one I'm not so clear about. we do need to know if the entity matches the predicate,
                            // but when limit is involved, that means we may need to go re-query when one of the previously matching entities is no longer matching
                            // This is the hardest part to do right.
                            let matches =
                                ankql::selection::filter::evaluate_predicate(&entity, &query_state.selection.predicate).unwrap_or(false);
                            let did_match = query_state.resultset.contains_key(&AbstractEntity::id(&entity));

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
                        entity_watcher.insert(EntityWatcherId::Predicate(subscription_id, query_id));
                        // and one for the subscription itself, which lingers until the user expressly tells us they want to stop watching
                        // entity_watcher.insert(EntityWatcherId::Subscription(subscription_id));
                    } else {
                        // When the predicate no longer matches, only remove the predicate watcher, not the subscription watcher
                        entity_watcher.remove(&EntityWatcherId::Predicate(subscription_id, query_id));
                    }
                    Self::update_query_matching_entities(subscription, query_id, &entity, matches);

                    let sub_entities = items.entry(subscription_id).or_default();

                    let membership_change = match (did_match, matches) {
                        (true, false) => Some(MembershipChange::Remove),
                        (false, true) => Some(MembershipChange::Add),
                        _ => None,
                    };

                    let entity_subscribed = entity_subscribed.contains(&query_id);
                    if membership_change.is_some() || entity_subscribed {
                        tracing::info!("Reactor SENDING UPDATE to subscription {}", subscription_id);
                        match sub_entities.entry(AbstractEntity::id(&entity)) {
                            indexmap::map::Entry::Vacant(v) => {
                                v.insert(ReactorUpdateItem {
                                    entity: entity.clone(),
                                    events: events.clone(),
                                    entity_subscribed,
                                    predicate_relevance: membership_change.map(|mc| (query_id, mc)).into_iter().collect(),
                                });
                            }
                            indexmap::map::Entry::Occupied(mut o) => {
                                if let Some(mc) = membership_change {
                                    o.get_mut().predicate_relevance.push((query_id, mc));
                                }
                            }
                        }
                    }
                }
            }
        }

        for (sub_id, sub_items) in items {
            if let Some(subscription) = self.0.subscriptions.lock().unwrap().get(&sub_id) {
                subscription.notify(ReactorUpdate { items: sub_items.into_values().collect(), initialized_query: None });
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

        for (_, subscription_state) in subscriptions.iter_mut() {
            let mut update_items: Vec<ReactorUpdateItem<E, Ev>> = Vec::new();

            // For each predicate in this subscription
            for (query_id, predicate_state) in &mut subscription_state.queries {
                // For each entity that was matching this predicate
                for entity_id in predicate_state.resultset.keys() {
                    // Try to get the entity from the subscription's cache
                    if let Some(entity) = subscription_state.entities.get(&entity_id) {
                        update_items.push(ReactorUpdateItem {
                            entity: entity.clone(),
                            events: vec![], // No events for system reset, because it's not a "change", its Thanos snapping his fingers
                            entity_subscribed: false, // All entities "stopped existing" and thus cannot be subscribed to
                            predicate_relevance: vec![(*query_id, MembershipChange::Remove)], // The predicates still exist, but all previous matching entities are unmatched
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
                let reactor_update = ReactorUpdate { items: update_items, initialized_query: None };
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
        watcher_id: (ReactorSubscriptionId, proto::QueryId), // Should this be a tuple of (subscription_id, query_id) or just subscription_id?
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
        write!(f, "Subscription {{ id: {:?}, predicates: {} }}", self.id, self.queries.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::selection::filter::Filterable;
    use ankurah_signals::Subscribe;
    use proto::{CollectionId, QueryId};

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

        let query_id = QueryId::new();
        let collection_id = CollectionId::fixed_name("album");
        let selection = "status = 'pending'".try_into().unwrap();
        let entity1 = TestEntity::new("Test Album", "pending");
        let resultset = EntityResultSet::single(entity1.clone());
        reactor.add_query(rsub.id(), query_id, collection_id, selection, resultset).unwrap();

        // something like this
        assert_eq!(
            check(),
            vec![ReactorUpdate {
                items: vec![ReactorUpdateItem {
                    entity: entity1.clone(),
                    events: vec![],
                    entity_subscribed: true,
                    predicate_relevance: vec![(query_id, MembershipChange::Initial)],
                }],
                initialized_query: Some((query_id, 0)),
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
