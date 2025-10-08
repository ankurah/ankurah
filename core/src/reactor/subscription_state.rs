use crate::{
    reactor::{
        AbstractEntity, CandidateChanges, ChangeNotification, MembershipChange, ReactorSubscriptionId, ReactorUpdate, ReactorUpdateItem,
        WatcherChange,
    },
    resultset::EntityResultSet,
};
use ankql::selection::filter::evaluate_predicate;
use ankurah_proto::{self as proto};
use futures::future;
use indexmap::IndexMap;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::debug;

type GapFillData<E> = (
    proto::QueryId,
    std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
    proto::CollectionId,
    ankql::ast::Selection,
    EntityResultSet<E>,
    Option<E>,
    usize,
);

/// State for a single predicate within a subscription
pub struct QueryState<E: AbstractEntity + ankql::selection::filter::Filterable> {
    // TODO make this a clonable PredicateSubscription and store it instead of the channel?
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) selection: ankql::ast::Selection,
    pub(crate) gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>, // For filling gaps when LIMIT is applied
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) paused: bool, // When true, skip notifications (used during initialization and updates)
    pub(crate) resultset: EntityResultSet<E>,
    pub(crate) version: u32,
}

// We would call this ReactorSubscription, but that name is reserved for the public API
// so instead we will call it Subscription and just scope it to the reactor package
pub(super) struct Subscription<E: AbstractEntity + ankql::selection::filter::Filterable, Ev>(Arc<Inner<E, Ev>>);

impl<E: AbstractEntity + ankql::selection::filter::Filterable, Ev> Clone for Subscription<E, Ev> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<E: AbstractEntity + ankql::selection::filter::Filterable, Ev> std::ops::Deref for Subscription<E, Ev> {
    type Target = Inner<E, Ev>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<E: AbstractEntity + ankql::selection::filter::Filterable, Ev> Subscription<E, Ev> {
    /// Get the subscription ID
    pub fn id(&self) -> ReactorSubscriptionId { self.0.id }
}

pub(super) struct Inner<E: AbstractEntity + ankql::selection::filter::Filterable, Ev> {
    pub(super) id: ReactorSubscriptionId,
    state: std::sync::Mutex<State<E, Ev>>,
}
struct State<E: AbstractEntity + ankql::selection::filter::Filterable, Ev> {
    pub(crate) queries: HashMap<proto::QueryId, QueryState<E>>,
    /// The set of entities that are subscribed to by this subscription
    pub(crate) entity_subscriptions: HashSet<proto::EntityId>,
    // not sure if we actually need this
    pub(crate) entities: HashMap<proto::EntityId, E>,
    pub(crate) broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
}

impl<E: AbstractEntity + ankql::selection::filter::Filterable + Send + 'static, Ev: Clone + Send + 'static> Subscription<E, Ev> {
    pub fn new(broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>) -> Self {
        Self(Arc::new(Inner {
            id: ReactorSubscriptionId::new(),
            state: std::sync::Mutex::new(State {
                queries: HashMap::new(),
                entity_subscriptions: HashSet::new(),
                entities: HashMap::new(),
                broadcast,
            }),
        }))
    }

    /// Add entity subscription
    pub fn add_entity_subscription(&self, entity_id: proto::EntityId) {
        let mut state = self.state.lock().unwrap();
        state.entity_subscriptions.insert(entity_id);
    }

    /// Remove entity subscription
    pub fn remove_entity_subscription(&self, entity_id: proto::EntityId) {
        let mut state = self.state.lock().unwrap();
        state.entity_subscriptions.remove(&entity_id);
    }

    /// Check if any queries match this entity (for determining if entity watcher should be removed)
    pub fn any_query_matches(&self, entity_id: &proto::EntityId) -> bool {
        let state = self.state.lock().unwrap();
        state.queries.values().any(|q| q.resultset.contains_key(entity_id))
    }

    /// System reset - clear all matching entities and notify
    pub fn system_reset(&self) {
        let state = &mut *self.state.lock().unwrap();
        let mut update_items: Vec<ReactorUpdateItem<E, Ev>> = Vec::new();

        // For each query in this subscription
        for (query_id, query_state) in &mut state.queries {
            // For each entity that was matching this query
            for entity_id in query_state.resultset.keys() {
                // Try to get the entity from the subscription's cache
                if let Some(entity) = state.entities.get(&entity_id) {
                    update_items.push(ReactorUpdateItem {
                        entity: entity.clone(),
                        events: vec![], // No events for system reset
                        entity_subscribed: false,
                        predicate_relevance: vec![(*query_id, MembershipChange::Remove)],
                    });
                }
            }

            // Clear the matching entities for this query
            query_state.resultset.clear();
            query_state.resultset.set_loaded(false);
        }

        // Clear entity subscriptions and cached entities
        state.entity_subscriptions.clear();
        state.entities.clear();

        // Send the notification if there were any updates
        if !update_items.is_empty() {
            let reactor_update = ReactorUpdate { items: update_items, initialized_query: None };
            state.broadcast.send(reactor_update);
        }
    }

    /// Get the number of queries for debugging
    pub fn queries_len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.queries.len()
    }

    /// Add a new query to the subscription
    /// Returns the list of entities from the resultset for setting up watchers
    pub fn add_query(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
    ) -> Result<Vec<(proto::EntityId, E)>, anyhow::Error> {
        let state = &mut *self.state.lock().unwrap();

        // Fail if query already exists
        use std::collections::hash_map::Entry;
        let query_state = match state.queries.entry(query_id) {
            Entry::Vacant(v) => v.insert(QueryState {
                collection_id,
                selection: selection.clone(),
                gap_fetcher,
                paused: false,
                resultset: resultset.clone(),
                version: 0,
            }),
            Entry::Occupied(_) => return Err(anyhow::anyhow!("Query {:?} already exists", query_id)),
        };

        // Configure resultset ordering and limit
        query_state.resultset.order_by(
            selection
                .order_by
                .map(|ob| crate::reactor::build_key_spec_from_selection(ob.as_slice(), &query_state.resultset))
                .transpose()?,
        );
        query_state.resultset.limit(selection.limit.map(|l| l as usize));

        // Collect entities for watcher setup
        let read_guard = query_state.resultset.read();
        let entities: Vec<(proto::EntityId, E)> = read_guard.iter_entities().map(|(id, e)| (id, e.clone())).collect();

        // Add to entity subscriptions
        for (entity_id, _) in &entities {
            state.entity_subscriptions.insert(*entity_id);
        }

        drop(read_guard);
        resultset.set_loaded(true);

        Ok(entities)
    }

    /// Send initialization ReactorUpdate for add_query
    pub fn send_add_query_update(&self, query_id: proto::QueryId, entities: Vec<(proto::EntityId, E)>) {
        let state = self.state.lock().unwrap();
        let reactor_update_items: Vec<ReactorUpdateItem<E, Ev>> = entities
            .into_iter()
            .map(|(_, entity)| ReactorUpdateItem {
                entity,
                events: vec![],
                entity_subscribed: true,
                predicate_relevance: vec![(query_id, MembershipChange::Initial)],
            })
            .collect();

        state.broadcast.send(ReactorUpdate { items: reactor_update_items, initialized_query: Some((query_id, 0)) });
    }

    /// Update an existing query
    /// Returns (old_predicate, newly_added_entities, removed_entities)
    pub fn update_query(
        &self,
        query_id: proto::QueryId,
        _collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        included_entities: Vec<E>,
        version: u32,
        emit_removes: bool,
    ) -> anyhow::Result<(ankql::ast::Predicate, Vec<(proto::EntityId, E)>, Vec<proto::EntityId>, Vec<ReactorUpdateItem<E, Ev>>)> {
        let state = &mut *self.state.lock().unwrap();

        // Get mutable reference to query state (must exist for v>0)
        let query_state = state.queries.get_mut(&query_id).ok_or_else(|| anyhow::anyhow!("Query not found for update"))?;

        // Save old predicate for watcher updates
        let old_predicate = query_state.selection.predicate.clone();

        // Update the query AST
        query_state.selection = selection.clone();
        query_state.resultset.order_by(
            selection
                .order_by
                .map(|ob| crate::reactor::build_key_spec_from_selection(ob.as_slice(), &query_state.resultset))
                .transpose()?,
        );

        // Check if LIMIT changed
        if query_state.selection.limit != selection.limit {
            query_state.resultset.limit(selection.limit.map(|l| l as usize));
        }

        // Create write guard for atomic updates
        let mut rw_resultset = query_state.resultset.write();
        let mut reactor_update_items = Vec::new();
        let mut newly_added = Vec::new();

        // Mark all entities dirty for re-evaluation
        rw_resultset.mark_all_dirty();

        // Process included entities (only truly new ones from remote)
        for entity in included_entities {
            if ankql::selection::filter::evaluate_predicate(&entity, &selection.predicate).unwrap_or(false) {
                let entity_id = *AbstractEntity::id(&entity);

                // Check if this is truly new to the resultset
                if !rw_resultset.contains(&entity_id) {
                    // Add to write guard
                    rw_resultset.add(entity.clone());

                    // Add to subscription entities map
                    state.entities.insert(entity_id, entity.clone());

                    // Track for entity watcher setup
                    state.entity_subscriptions.insert(entity_id);
                    newly_added.push((entity_id, entity.clone()));

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
        let mut removed_entities = Vec::new();
        rw_resultset.retain_dirty(|entity| {
            if let Ok(true) = ankql::selection::filter::evaluate_predicate(entity, &selection.predicate) {
                return true;
            };
            let entity_id = *entity.id();
            tracing::debug!("Entity {:?} no longer matches predicate", entity_id);

            removed_entities.push(entity_id);

            // If emit_removes is true, create a Remove event
            if emit_removes {
                tracing::debug!("Creating Remove event for entity {:?}", entity_id);
                reactor_update_items.push(ReactorUpdateItem {
                    entity: entity.clone(),
                    events: vec![],
                    entity_subscribed: false,
                    predicate_relevance: vec![(query_id, MembershipChange::Remove)],
                });
            }

            false
        });

        // Unpause now that update is complete
        query_state.paused = false;
        query_state.version = version;
        query_state.resultset.set_loaded(true);

        // Drop write guard to apply changes
        drop(rw_resultset);

        Ok((old_predicate, newly_added, removed_entities, reactor_update_items))
    }

    /// Send update query ReactorUpdate
    pub fn send_update_query_update(&self, query_id: proto::QueryId, version: u32, items: Vec<ReactorUpdateItem<E, Ev>>) {
        let state = self.state.lock().unwrap();
        state.broadcast.send(ReactorUpdate { items, initialized_query: Some((query_id, version)) });
    }

    /// Remove a query and return its state for cleanup
    pub fn remove_query(&self, query_id: proto::QueryId) -> Option<QueryState<E>> {
        let mut state = self.state.lock().unwrap();
        state.queries.remove(&query_id)
    }

    /// Get all queries for cleanup (used by unsubscribe)
    pub fn take_all_queries(&self) -> HashMap<proto::QueryId, QueryState<E>> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.queries)
    }

    /// Evaluate candidate changes for this subscription and spawn gap filling/notification task
    /// Returns watcher changes that need to be applied to the global WatcherSet
    pub async fn evaluate_changes<C: ChangeNotification<Entity = E, Event = Ev> + Clone>(
        self,
        candidates: CandidateChanges<C>,
    ) -> Vec<WatcherChange> {
        let mut watcher_changes = Vec::new();
        let mut items: IndexMap<proto::EntityId, ReactorUpdateItem<E, Ev>> = IndexMap::new();

        // Take the state lock once for all evaluations
        let mut state_guard = self.state.lock().unwrap();
        let state = &mut *state_guard;

        // Process query-specific candidates using direct lookup
        for query_candidate in candidates.query_iter() {
            let query_id = *query_candidate.query_id;

            // Direct lookup - skip if query doesn't exist or is paused
            let query_state = match state.queries.get_mut(&query_id) {
                Some(qs) if !qs.paused => qs,
                _ => continue,
            };

            debug!("\tevaluate_changes query: {} {:?}", query_id, query_state.selection);

            // Process all candidate changes for this query
            for change in query_candidate.iter() {
                let entity = change.entity();
                let entity_id = *AbstractEntity::id(entity);

                debug!("Subscription {} evaluating entity {} for query {}", self.id(), entity_id, query_id);

                let matches = evaluate_predicate(entity, &query_state.selection.predicate).unwrap_or(false);
                let did_match = query_state.resultset.contains_key(&entity_id);
                let entity_subscribed = state.entity_subscriptions.contains(&entity_id);

                // Process membership change in one match
                let membership_change = match (did_match, matches) {
                    (false, true) => {
                        // Entity now matches - add to matching set
                        let entity_clone = entity.clone();
                        query_state.resultset.write().add(entity_clone.clone());
                        state.entities.insert(entity_id, entity_clone);
                        watcher_changes.push(WatcherChange::add(entity_id, self.id, query_id));
                        Some(MembershipChange::Add)
                    }
                    (true, false) => {
                        // Entity no longer matches - remove from matching set
                        query_state.resultset.write().remove(entity_id);
                        watcher_changes.push(WatcherChange::remove(entity_id, self.id, query_id));
                        Some(MembershipChange::Remove)
                    }
                    _ => {
                        // No membership change - just track watcher state
                        watcher_changes.push(if matches {
                            WatcherChange::add(entity_id, self.id, query_id)
                        } else {
                            WatcherChange::remove(entity_id, self.id, query_id)
                        });
                        None
                    }
                };

                // Emit if matches, matched before, or explicitly subscribed
                if matches || did_match || entity_subscribed {
                    let item = items.entry(entity_id).or_insert_with(|| ReactorUpdateItem {
                        entity: entity.clone(),
                        events: change.events().to_vec(),
                        entity_subscribed: false, // Hardcoded - will be removed
                        predicate_relevance: Vec::new(),
                    });

                    if let Some(mc) = membership_change {
                        item.predicate_relevance.push((query_id, mc));
                    }
                }
            }
        }

        // Process entity-level subscriptions not covered by query processing
        for change in candidates.entity_iter() {
            let entity = change.entity();
            let entity_id = *AbstractEntity::id(entity);

            if state.entity_subscriptions.contains(&entity_id) {
                // !items.contains_key(&entity_id) {
                items.entry(entity_id).or_insert(ReactorUpdateItem {
                    entity: entity.clone(),
                    events: change.events().to_vec(),
                    entity_subscribed: false, // Hardcoded - will be removed
                    predicate_relevance: Vec::new(),
                });
            }
        }

        // Collect gap fill data while we have the state lock
        let gaps_to_fill = self.collect_gaps_to_fill_internal(&state);
        let broadcast = state.broadcast.clone();

        // Drop the state lock before spawning
        drop(state_guard);

        // Spawn background task for gap filling and notification
        let update_items: Vec<ReactorUpdateItem<E, Ev>> = items.into_values().collect();
        if !gaps_to_fill.is_empty() {
            crate::task::spawn(self.clone().fill_gaps_and_notify(update_items, gaps_to_fill, broadcast, None));
        } else if !update_items.is_empty() {
            broadcast.send(ReactorUpdate { items: update_items, initialized_query: None });
        }

        watcher_changes
    }

    /// Collect gaps to fill (internal version that works with locked state)
    fn collect_gaps_to_fill_internal(&self, state: &State<E, Ev>) -> Vec<GapFillData<E>> {
        state.queries.iter().filter_map(|(query_id, query_state)| self.extract_gap_data(*query_id, query_state)).collect()
    }

    /// Fill gaps and send notification
    /// Combined method to handle gap filling and notification in a single task
    async fn fill_gaps_and_notify(
        self,
        mut items: Vec<ReactorUpdateItem<E, Ev>>,
        gaps_to_fill: Vec<GapFillData<E>>,
        broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
        initialized_query: Option<(proto::QueryId, u32)>,
    ) {
        // Clear gap_dirty flags immediately for all queries
        for (_, _, _, _, ref resultset, _, _) in &gaps_to_fill {
            resultset.clear_gap_dirty();
        }

        // Process all gap fills concurrently
        let gap_fill_futures =
            gaps_to_fill.into_iter().map(|(query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size)| {
                Self::process_gap_fill(query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size)
            });

        let gap_results = future::join_all(gap_fill_futures).await;

        // Collect all the new items from gap filling
        for gap_items in gap_results {
            items.extend(gap_items);
        }

        // Send the consolidated update
        if !items.is_empty() {
            broadcast.send(ReactorUpdate { items, initialized_query });
        }
    }

    fn extract_gap_data(&self, query_id: proto::QueryId, query_state: &QueryState<E>) -> Option<GapFillData<E>> {
        let resultset = &query_state.resultset;

        if !resultset.is_gap_dirty() {
            return None;
        }

        let limit = resultset.get_limit()?;
        let current_len = resultset.len();

        if current_len >= limit {
            return None;
        }

        let gap_size = limit - current_len;
        let last_entity = resultset.last_entity();

        Some((
            query_id,
            query_state.gap_fetcher.clone(),
            query_state.collection_id.clone(),
            query_state.selection.clone(),
            resultset.clone(),
            last_entity,
            gap_size,
        ))
    }

    async fn process_gap_fill(
        query_id: proto::QueryId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        resultset: EntityResultSet<E>,
        last_entity: Option<E>,
        gap_size: usize,
    ) -> Vec<ReactorUpdateItem<E, Ev>> {
        tracing::debug!("Gap filling for query {} - need {} entities", query_id, gap_size);

        match gap_fetcher.fetch_gap(&collection_id, &selection, last_entity.as_ref(), gap_size).await {
            Ok(gap_entities) => {
                if !gap_entities.is_empty() {
                    tracing::debug!("Gap filling fetched {} entities for query {}", gap_entities.len(), query_id);

                    let mut write = resultset.write();
                    let mut gap_items = Vec::new();

                    for entity in gap_entities {
                        if write.add(entity.clone()) {
                            gap_items.push(ReactorUpdateItem {
                                entity,
                                events: vec![],
                                entity_subscribed: false,
                                predicate_relevance: vec![(query_id, MembershipChange::Add)],
                            });
                        }
                    }

                    gap_items
                } else {
                    tracing::debug!("Gap filling found no entities for query {}", query_id);
                    Vec::new()
                }
            }
            Err(e) => {
                tracing::warn!("Gap filling failed for query {}: {}", query_id, e);
                Vec::new()
            }
        }
    }
}
