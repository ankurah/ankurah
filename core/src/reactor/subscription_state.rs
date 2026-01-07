use crate::{
    reactor::{
        AbstractEntity, CandidateChanges, ChangeNotification, MembershipChange, ReactorSubscriptionId, ReactorUpdate, ReactorUpdateItem,
        WatcherChange,
    },
    resultset::EntityResultSet,
    selection::filter::{evaluate_predicate, Filterable},
};
use ankurah_proto::{self as proto};
use futures::future;
use indexmap::IndexMap;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::debug;

/// Trait for accumulating ReactorUpdateItems during update_query
/// Allows for both collecting items (Vec) and discarding them (unit type)
/// Vec accumulator collects all items including removes; () accumulator discards everything
pub trait UpdateItemAccumulator<E, Ev> {
    fn push_initial(&mut self, entity: &E, query_id: proto::QueryId);
    fn push_remove(&mut self, entity: &E, query_id: proto::QueryId);
}

impl<E: Clone, Ev> UpdateItemAccumulator<E, Ev> for Vec<ReactorUpdateItem<E, Ev>> {
    fn push_initial(&mut self, entity: &E, query_id: proto::QueryId) {
        Vec::push(
            self,
            ReactorUpdateItem { entity: entity.clone(), events: vec![], predicate_relevance: vec![(query_id, MembershipChange::Initial)] },
        );
    }

    fn push_remove(&mut self, entity: &E, query_id: proto::QueryId) {
        Vec::push(
            self,
            ReactorUpdateItem { entity: entity.clone(), events: vec![], predicate_relevance: vec![(query_id, MembershipChange::Remove)] },
        );
    }
}

impl<E, Ev> UpdateItemAccumulator<E, Ev> for () {
    fn push_initial(&mut self, _entity: &E, _query_id: proto::QueryId) {}
    fn push_remove(&mut self, _entity: &E, _query_id: proto::QueryId) {}
}

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
pub struct QueryState<E: AbstractEntity + Filterable> {
    // TODO make this a clonable PredicateSubscription and store it instead of the channel?
    pub(crate) collection_id: proto::CollectionId,
    /// Selection is None until first update_query call (after register_query)
    pub(crate) selection: Option<ankql::ast::Selection>,
    pub(crate) gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>, // For filling gaps when LIMIT is applied
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) paused: bool, // When true, skip notifications (used during initialization and updates)
    pub(crate) resultset: EntityResultSet<E>,
    pub(crate) version: u32,
}

// We would call this ReactorSubscription, but that name is reserved for the public API
// so instead we will call it Subscription and just scope it to the reactor package
pub(super) struct Subscription<E: AbstractEntity + Filterable, Ev>(Arc<Inner<E, Ev>>);

impl<E: AbstractEntity + Filterable, Ev> Clone for Subscription<E, Ev> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<E: AbstractEntity + Filterable, Ev> std::ops::Deref for Subscription<E, Ev> {
    type Target = Inner<E, Ev>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<E: AbstractEntity + Filterable, Ev> Subscription<E, Ev> {
    /// Get the subscription ID
    pub fn id(&self) -> ReactorSubscriptionId { self.0.id }
}

pub(super) struct Inner<E: AbstractEntity + Filterable, Ev> {
    pub(super) id: ReactorSubscriptionId,
    state: std::sync::Mutex<State<E, Ev>>,
    watcher_set: Arc<std::sync::Mutex<crate::reactor::watcherset::WatcherSet>>,
}
struct State<E: AbstractEntity + Filterable, Ev> {
    pub(crate) queries: HashMap<proto::QueryId, QueryState<E>>,
    /// The set of entities that are subscribed to by this subscription
    pub(crate) entity_subscriptions: HashSet<proto::EntityId>,
    // not sure if we actually need this
    pub(crate) entities: HashMap<proto::EntityId, E>,
    pub(crate) broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Subscription<E, Ev> {
    pub fn new(
        broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
        watcher_set: Arc<std::sync::Mutex<crate::reactor::watcherset::WatcherSet>>,
    ) -> Self {
        Self(Arc::new(Inner {
            id: ReactorSubscriptionId::new(),
            state: std::sync::Mutex::new(State {
                queries: HashMap::new(),
                entity_subscriptions: HashSet::new(),
                entities: HashMap::new(),
                broadcast,
            }),
            watcher_set,
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
            let reactor_update = ReactorUpdate { items: update_items };
            state.broadcast.send(reactor_update);
        }
    }

    /// Get the number of queries for debugging
    pub fn queries_len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.queries.len()
    }

    /// Register a new query with the subscription (with empty resultset)
    /// The resultset will be populated later by update_query
    /// Selection is stored as None; update_query will set it on first call
    pub fn register_query(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
    ) -> Result<(), anyhow::Error> {
        let mut state = self.state.lock().unwrap();

        // Fail if query already exists
        use std::collections::hash_map::Entry;
        match state.queries.entry(query_id) {
            Entry::Vacant(v) => {
                v.insert(QueryState { collection_id, selection: None, gap_fetcher, paused: false, resultset, version: 0 });
                Ok(())
            }
            Entry::Occupied(_) => Err(anyhow::anyhow!("Query {:?} already exists", query_id)),
        }
    }

    /// Update predicate watchers for a query (index/wildcard watchers)
    /// If old_predicate is None, only adds watchers (for initial setup)
    /// If old_predicate is Some, removes old watchers and adds new ones
    pub fn update_predicate_watchers(
        &self,
        query_id: proto::QueryId,
        collection_id: &proto::CollectionId,
        old_predicate: Option<&ankql::ast::Predicate>,
        new_predicate: &ankql::ast::Predicate,
    ) {
        let mut watcher_set = self.watcher_set.lock().unwrap();
        let watcher_id = (self.id, query_id);

        if let Some(old_pred) = old_predicate {
            watcher_set.recurse_predicate_watchers(collection_id, old_pred, watcher_id, crate::reactor::watcherset::WatcherOp::Remove);
        }
        watcher_set.recurse_predicate_watchers(collection_id, new_predicate, watcher_id, crate::reactor::watcherset::WatcherOp::Add);
    }

    /// Add entity watchers for entities in a query's resultset
    pub fn add_entity_watchers(&self, query_id: proto::QueryId, entity_ids: impl Iterator<Item = proto::EntityId>) {
        let mut watcher_set = self.watcher_set.lock().unwrap();
        watcher_set.add_predicate_entity_watchers(self.id, query_id, entity_ids);
    }
    /// Update an existing query
    /// Generic over the accumulator type - pass Vec to collect items, () to discard
    /// Handles watcher management internally (both predicate and entity watchers)
    /// Returns newly_added_entities for server delta generation
    pub fn update_query<A: UpdateItemAccumulator<E, Ev>>(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        included_entities: Vec<E>,
        version: u32,
        reactor_updates: &mut A,
    ) -> anyhow::Result<Vec<E>> {
        let mut state_guard = self.state.lock().unwrap();
        let state = &mut *state_guard;

        // Get mutable reference to query state (must exist)
        let query_state = state.queries.get_mut(&query_id).ok_or_else(|| anyhow::anyhow!("Query not found for update"))?;

        // Check if this is the first update (selection is None)
        let is_first_update = query_state.selection.is_none();

        // Save old selection for comparison
        let old_selection = query_state.selection.replace(selection.clone());

        // Update resultset configuration
        query_state.resultset.order_by(
            selection
                .order_by
                .map(|ob| crate::reactor::build_key_spec_from_selection(ob.as_slice(), &query_state.resultset))
                .transpose()?,
        );

        // Set limit if this is first update OR if limit changed
        if is_first_update || old_selection.as_ref().map(|s| s.limit) != Some(selection.limit) {
            query_state.resultset.limit(selection.limit.map(|l| l as usize));
        }

        // Create write guard for atomic updates
        let mut rw_resultset = query_state.resultset.write();
        let mut newly_added: Vec<E> = Vec::new();

        // Mark all entities dirty for re-evaluation
        rw_resultset.mark_all_dirty();

        // Process included entities (only truly new ones from remote)
        for entity in included_entities {
            if evaluate_predicate(&entity, &selection.predicate).unwrap_or(false) {
                let entity_id = *AbstractEntity::id(&entity);

                // Check if this is truly new to the resultset
                if !rw_resultset.contains(&entity_id) {
                    rw_resultset.add(entity.clone());
                    state.entities.insert(entity_id, entity.clone());
                    state.entity_subscriptions.insert(entity_id);
                    reactor_updates.push_initial(&entity, query_id);
                    newly_added.push(entity);
                }
            }
        }

        // Remove entities that no longer match the new predicate
        let mut removed_entities = Vec::new();
        rw_resultset.retain_dirty(|entity| {
            if let Ok(true) = evaluate_predicate(entity, &selection.predicate) {
                return true;
            };
            let entity_id = *entity.id();
            tracing::debug!("Entity {:?} no longer matches predicate", entity_id);

            removed_entities.push(entity_id);
            reactor_updates.push_remove(entity, query_id);
            false
        });

        // Unpause now that update is complete
        query_state.paused = false;
        query_state.version = version;
        query_state.resultset.set_loaded(true);

        // Drop write guard to apply changes
        drop(rw_resultset);

        // Drop state lock before updating watchers
        drop(state_guard);

        // Update predicate watchers (setup on first update, or update if predicate changed)
        let should_update_watchers = if is_first_update {
            true
        } else if let Some(ref old_sel) = old_selection {
            old_sel.predicate != selection.predicate
        } else {
            false
        };

        if should_update_watchers {
            let old_pred = old_selection.as_ref().map(|s| &s.predicate);
            self.update_predicate_watchers(query_id, &collection_id, old_pred, &selection.predicate);
        }

        // Add entity watchers for newly added entities
        if !newly_added.is_empty() {
            self.add_entity_watchers(query_id, newly_added.iter().map(|e| *AbstractEntity::id(e)));
        }

        // Remove entity watchers for removed entities
        if !removed_entities.is_empty() {
            let mut watcher_set = self.watcher_set.lock().unwrap();
            watcher_set.cleanup_removed_predicate_watchers(self.id, query_id, &removed_entities);
        }

        Ok(newly_added)
    }

    /// Send ReactorUpdate with the given items
    pub fn send_update(&self, items: Vec<ReactorUpdateItem<E, Ev>>) {
        let state = self.state.lock().unwrap();
        state.broadcast.send(ReactorUpdate { items });
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

            let selection = query_state.selection.as_ref().expect("evaluate_changes called before update_query");
            debug!("\tevaluate_changes query: {} {:?}", query_id, selection);

            // Process all candidate changes for this query
            for change in query_candidate.iter() {
                let entity = change.entity();
                let entity_id = *AbstractEntity::id(entity);

                debug!("Subscription {} evaluating entity {} for query {}", self.id(), entity_id, query_id);

                let matches = evaluate_predicate(entity, &selection.predicate).unwrap_or(false);
                let did_match = query_state.resultset.contains_key(&entity_id);

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
                let entity_subscribed = state.entity_subscriptions.contains(&entity_id);
                if matches || did_match || entity_subscribed {
                    let item = items.entry(entity_id).or_insert_with(|| ReactorUpdateItem {
                        entity: entity.clone(),
                        events: change.events().to_vec(),
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
        // fill_gaps_and_notify will register entity watchers for gap-filled entities
        let update_items: Vec<ReactorUpdateItem<E, Ev>> = items.into_values().collect();
        if !gaps_to_fill.is_empty() {
            crate::task::spawn(self.clone().fill_gaps_and_notify(update_items, gaps_to_fill, broadcast));
        } else if !update_items.is_empty() {
            broadcast.send(ReactorUpdate { items: update_items });
        }

        watcher_changes
    }

    /// Collect gaps to fill (internal version that works with locked state)
    fn collect_gaps_to_fill_internal(&self, state: &State<E, Ev>) -> Vec<GapFillData<E>> {
        state.queries.iter().filter_map(|(query_id, query_state)| self.extract_gap_data(*query_id, query_state)).collect()
    }

    /// Fill gaps for a specific query and append entities to the provided vector
    /// Also registers entity watchers for gap-filled entities
    pub async fn fill_gaps_for_query_entities(&self, query_id: proto::QueryId, entities: &mut Vec<E>) {
        let gap_data = {
            let state = self.state.lock().unwrap();
            state.queries.get(&query_id).and_then(|query_state| self.extract_gap_data(query_id, query_state))
        };

        let Some((query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size)) = gap_data else {
            return;
        };

        // Clear gap_dirty flag immediately
        resultset.clear_gap_dirty();

        // Process gap fill
        let gap_filled_entities =
            Self::process_gap_fill_entities(query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size).await;

        // Register entity watchers and append entities
        if !gap_filled_entities.is_empty() {
            self.add_entity_watchers(query_id, gap_filled_entities.iter().map(|e| *AbstractEntity::id(e)));
            entities.extend(gap_filled_entities);
        }
    }

    /// Fill gaps for a specific query and push ReactorUpdateItems to the accumulator
    /// Also registers entity watchers for gap-filled entities
    /// Used by add_query_and_notify and update_query_and_notify
    pub async fn fill_gaps_for_query<A: UpdateItemAccumulator<E, Ev>>(&self, query_id: proto::QueryId, reactor_updates: &mut A) {
        let gap_data = {
            let state = self.state.lock().unwrap();
            state.queries.get(&query_id).and_then(|query_state| self.extract_gap_data(query_id, query_state))
        };

        let Some((query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size)) = gap_data else {
            return;
        };

        // Clear gap_dirty flag immediately
        resultset.clear_gap_dirty();

        // Process gap fill
        let gap_filled_entities =
            Self::process_gap_fill_entities(query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size).await;

        // Register entity watchers and push items for gap-filled entities
        if !gap_filled_entities.is_empty() {
            self.add_entity_watchers(query_id, gap_filled_entities.iter().map(|e| *AbstractEntity::id(e)));

            for entity in gap_filled_entities {
                reactor_updates.push_initial(&entity, query_id);
            }
        }
    }

    async fn process_gap_fill_entities(
        query_id: proto::QueryId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        resultset: EntityResultSet<E>,
        last_entity: Option<E>,
        gap_size: usize,
    ) -> Vec<E> {
        tracing::debug!("Gap filling for query {} - need {} entities", query_id, gap_size);

        match gap_fetcher.fetch_gap(&collection_id, &selection, last_entity.as_ref(), gap_size).await {
            Ok(gap_entities) => {
                if !gap_entities.is_empty() {
                    tracing::debug!("Gap filling fetched {} entities for query {}", gap_entities.len(), query_id);

                    let mut write = resultset.write();
                    let mut added_entities = Vec::new();

                    for entity in gap_entities {
                        if write.add(entity.clone()) {
                            added_entities.push(entity);
                        }
                    }

                    added_entities
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

    /// Fill gaps and send notification
    /// Combined method to handle gap filling and notification in a single task
    async fn fill_gaps_and_notify(
        self,
        mut items: Vec<ReactorUpdateItem<E, Ev>>,
        gaps_to_fill: Vec<GapFillData<E>>,
        broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
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

        let gap_results: Vec<(ankurah_proto::QueryId, Vec<ReactorUpdateItem<E, Ev>>)> = future::join_all(gap_fill_futures).await;

        // Collect all the new items from gap filling and register entity watchers
        for (query_id, gap_items) in gap_results {
            if !gap_items.is_empty() {
                // Register entity watchers for gap-filled entities
                let entity_ids: Vec<_> = gap_items.iter().map(|item| *AbstractEntity::id(&item.entity)).collect();
                self.add_entity_watchers(query_id, entity_ids.into_iter());

                items.extend(gap_items);
            }
        }

        // Send the consolidated update
        if !items.is_empty() {
            broadcast.send(ReactorUpdate { items });
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

        // Selection should always be Some by the time gap filling happens
        let selection = query_state.selection.clone().expect("extract_gap_data called before update_query");

        Some((
            query_id,
            query_state.gap_fetcher.clone(),
            query_state.collection_id.clone(),
            selection,
            resultset.clone(),
            last_entity,
            gap_size,
        ))
    }

    fn spawn_gap_filling_task(&self, update: ReactorUpdate<E, Ev>, gaps_to_fill: Vec<GapFillData<E>>) {
        let broadcast = self.state.lock().unwrap().broadcast.clone();
        let initial_items = update.items;

        crate::task::spawn(async move {
            let mut all_items = initial_items;

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
            for (_query_id, gap_items) in gap_results {
                all_items.extend(gap_items);
            }

            broadcast.send(ReactorUpdate { items: all_items });
        });
    }

    async fn process_gap_fill(
        query_id: proto::QueryId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        resultset: EntityResultSet<E>,
        last_entity: Option<E>,
        gap_size: usize,
    ) -> (proto::QueryId, Vec<ReactorUpdateItem<E, Ev>>) {
        tracing::debug!("Gap filling for query {} - need {} entities", query_id, gap_size);

        let gap_items = match gap_fetcher.fetch_gap(&collection_id, &selection, last_entity.as_ref(), gap_size).await {
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
        };

        (query_id, gap_items)
    }
}

// Entity-specific methods for remote subscriptions
impl Subscription<crate::entity::Entity, ankurah_proto::Attested<ankurah_proto::Event>> {
    /// Upsert a query - register if it doesn't exist, or return the existing resultset
    /// Idempotent - safe to call multiple times with the same query_id
    /// Constructs gap_fetcher lazily (only if query doesn't exist)
    pub fn upsert_query<SE, PA>(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        node: &crate::node::Node<SE, PA>,
        cdata: &PA::ContextData,
    ) -> EntityResultSet<crate::entity::Entity>
    where
        SE: crate::storage::StorageEngine + Send + Sync + 'static,
        PA: crate::policy::PolicyAgent + Send + Sync + 'static,
    {
        let mut state = self.state.lock().unwrap();

        use std::collections::hash_map::Entry;
        match state.queries.entry(query_id) {
            Entry::Vacant(v) => {
                let resultset = EntityResultSet::empty();
                // Only create gap fetcher if query doesn't exist
                let gap_fetcher = std::sync::Arc::new(crate::reactor::fetch_gap::QueryGapFetcher::new(node, cdata.clone()));
                v.insert(QueryState {
                    collection_id,
                    selection: None,
                    gap_fetcher,
                    paused: false,
                    resultset: resultset.clone(),
                    version: 0,
                });
                resultset
            }
            Entry::Occupied(o) => o.get().resultset.clone(),
        }
    }
}
