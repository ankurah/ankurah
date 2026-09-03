use crate::internal::prelude::*;
use crate::reactor::{
    AbstractEntity, CandidateChanges, ChangeNotification, MembershipChange, ReactorSubscriptionId, ReactorUpdate, ReactorUpdateItem,
    WatcherChange,
};
use crate::selection::filter::{evaluate_predicate, Filterable};
use ankql::ast::Resolved;
use futures::future;
use indexmap::IndexMap;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::debug;

/// Collects query updates into a `Vec`, or discards them through `()`.
pub(super) trait UpdateItemAccumulator<E> {
    fn push_initial(&mut self, entity: &E, query_id: proto::QueryId);
    fn push_remove(&mut self, entity: &E, query_id: proto::QueryId);
}

impl<E: Clone, Ev> UpdateItemAccumulator<E> for Vec<ReactorUpdateItem<E, Ev>> {
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

impl<E> UpdateItemAccumulator<E> for () {
    fn push_initial(&mut self, _entity: &E, _query_id: proto::QueryId) {}
    fn push_remove(&mut self, _entity: &E, _query_id: proto::QueryId) {}
}

pub(super) struct PendingGapFill<E: AbstractEntity> {
    query_id: proto::QueryId,
    generation: u64,
    request: Option<GapRequest<E>>,
}

struct GapRequest<E: AbstractEntity> {
    fetcher: Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
    collection_id: proto::CollectionId,
    selection: ankql::ast::Selection<Resolved>,
    last_entity: Option<E>,
    size: usize,
}

struct FetchedGap<E: AbstractEntity> {
    query_id: proto::QueryId,
    generation: u64,
    entities: Vec<E>,
}

/// State for a single predicate within a subscription
pub(super) struct QueryState<E: AbstractEntity + Filterable> {
    // TODO make this a clonable PredicateSubscription and store it instead of the channel?
    pub(crate) collection_id: proto::CollectionId,
    /// Selection is None until first update_query call (after register_query)
    pub(crate) selection: Option<ankql::ast::Selection<Resolved>>,
    pub(crate) gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>, // For filling gaps when LIMIT is applied
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) paused: bool, // When true, skip notifications (used during initialization and updates)
    pub(crate) resultset: EntityResultSet<E>,
    pub(crate) version: u32,
    generation: u64,
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
    notify_lock: Arc<tokio::sync::Mutex<()>>,
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
        notify_lock: Arc<tokio::sync::Mutex<()>>,
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
            notify_lock,
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

    /// Clear results after the reactor has cleared its shared watcher indexes.
    pub fn system_reset(&self) {
        let (update_items, broadcast) = {
            let state = &mut *self.state.lock().unwrap();
            let mut update_items = Vec::new();
            for (query_id, query_state) in &mut state.queries {
                for entity_id in query_state.resultset.keys() {
                    if let Some(entity) = state.entities.get(&entity_id) {
                        update_items.push(ReactorUpdateItem {
                            entity: entity.clone(),
                            events: vec![],
                            predicate_relevance: vec![(*query_id, MembershipChange::Remove)],
                        });
                    }
                }
                query_state.selection = None;
                query_state.version = query_state.version.saturating_add(1);
                query_state.generation = query_state.generation.checked_add(1).expect("query generation exhausted");
                query_state.paused = true;
                query_state.resultset.clear();
                query_state.resultset.set_loaded(false);
            }
            state.entity_subscriptions.clear();
            state.entities.clear();
            (update_items, state.broadcast.clone())
        };

        if !update_items.is_empty() {
            broadcast.send(ReactorUpdate { items: update_items });
        }
    }

    /// Get the number of queries for debugging
    pub fn queries_len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.queries.len()
    }

    pub fn contains_query(&self, query_id: proto::QueryId) -> bool { self.state.lock().unwrap().queries.contains_key(&query_id) }

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

        use std::collections::hash_map::Entry;
        match state.queries.entry(query_id) {
            Entry::Vacant(v) => {
                v.insert(QueryState { collection_id, selection: None, gap_fetcher, paused: false, resultset, version: 0, generation: 0 });
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
        old_predicate: Option<&ankql::ast::Predicate<Resolved>>,
        new_predicate: &ankql::ast::Predicate<Resolved>,
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
    /// Update a query, returning entities newly added to its result set.
    pub fn update_query<A: UpdateItemAccumulator<E>>(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        included_entities: Vec<E>,
        version: u32,
        reactor_updates: &mut A,
    ) -> anyhow::Result<Vec<E>> {
        let mut state_guard = self.state.lock().unwrap();
        let state = &mut *state_guard;

        let query_state = state.queries.get_mut(&query_id).ok_or_else(|| anyhow::anyhow!("Query not found for update"))?;
        if query_state.collection_id != collection_id {
            anyhow::bail!("query {query_id} is bound to collection '{}', not '{collection_id}'", query_state.collection_id);
        }
        if version < query_state.version {
            anyhow::bail!("stale query version {version} for {query_id}; current version is {}", query_state.version);
        }
        query_state.version = version;
        query_state.generation = query_state.generation.checked_add(1).expect("query generation exhausted");

        let is_first_update = query_state.selection.is_none();
        let old_selection = query_state.selection.replace(selection.clone());

        // Update resultset configuration
        query_state.resultset.order_by(
            selection
                .order_by
                .map(|ob| crate::reactor::build_key_spec_from_selection(ob.as_slice(), &query_state.resultset))
                .transpose()?,
        );

        if is_first_update || old_selection.as_ref().map(|s| s.limit) != Some(selection.limit) {
            query_state.resultset.limit(selection.limit.map(|l| l as usize));
        }

        let mut rw_resultset = query_state.resultset.write();
        let mut newly_added: Vec<E> = Vec::new();

        rw_resultset.mark_all_dirty();

        for entity in included_entities {
            if evaluate_predicate(&entity, &selection.predicate).unwrap_or(false) {
                let entity_id = *AbstractEntity::id(&entity);

                if !rw_resultset.contains(&entity_id) {
                    rw_resultset.add(entity.clone());
                    state.entities.insert(entity_id, entity.clone());
                    state.entity_subscriptions.insert(entity_id);
                    reactor_updates.push_initial(&entity, query_id);
                    newly_added.push(entity);
                }
            }
        }

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

        query_state.paused = false;

        rw_resultset.set_loaded(true);
        drop(rw_resultset);

        drop(state_guard);

        let should_update_watchers = is_first_update || old_selection.as_ref().is_some_and(|old| old.predicate != selection.predicate);

        if should_update_watchers {
            let old_pred = old_selection.as_ref().map(|s| &s.predicate);
            self.update_predicate_watchers(query_id, &collection_id, old_pred, &selection.predicate);
        }

        if !newly_added.is_empty() {
            self.add_entity_watchers(query_id, newly_added.iter().map(|e| *AbstractEntity::id(e)));
        }

        if !removed_entities.is_empty() {
            let mut watcher_set = self.watcher_set.lock().unwrap();
            watcher_set.cleanup_removed_predicate_watchers(self.id, query_id, &removed_entities);
        }

        Ok(newly_added)
    }

    /// Send ReactorUpdate with the given items
    pub fn send_update(&self, items: Vec<ReactorUpdateItem<E, Ev>>) {
        let broadcast = self.state.lock().unwrap().broadcast.clone();
        broadcast.send(ReactorUpdate { items });
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

    /// Evaluate changes and return global watcher updates.
    pub fn evaluate_changes<C: ChangeNotification<Entity = E, Event = Ev>>(self, candidates: CandidateChanges<C>) -> Vec<WatcherChange> {
        let (watcher_changes, update_items, gaps_to_fill, broadcast) = {
            let mut watcher_changes = Vec::new();
            let mut items: IndexMap<proto::EntityId, ReactorUpdateItem<E, Ev>> = IndexMap::new();
            let mut state_guard = self.state.lock().unwrap();
            let state = &mut *state_guard;

            for query_candidate in candidates.query_iter() {
                let query_id = *query_candidate.query_id;

                let query_state = match state.queries.get_mut(&query_id) {
                    Some(qs) if !qs.paused => qs,
                    _ => continue,
                };

                let selection = query_state.selection.as_ref().expect("evaluate_changes called before update_query");
                debug!("\tevaluate_changes query: {} {:?}", query_id, selection);

                for change in query_candidate.iter() {
                    let entity = change.entity();
                    let entity_id = *AbstractEntity::id(entity);

                    debug!("Subscription {} evaluating entity {} for query {}", self.id(), entity_id, query_id);

                    let matches = evaluate_predicate(entity, &selection.predicate).unwrap_or(false);
                    let did_match = query_state.resultset.contains_key(&entity_id);

                    let membership_change = match (did_match, matches) {
                        (false, true) => {
                            let entity_clone = entity.clone();
                            query_state.resultset.write().add(entity_clone.clone());
                            state.entities.insert(entity_id, entity_clone);
                            watcher_changes.push(WatcherChange::add(entity_id, self.id, query_id));
                            Some(MembershipChange::Add)
                        }
                        (true, false) => {
                            query_state.resultset.write().remove(entity_id);
                            watcher_changes.push(WatcherChange::remove(entity_id, self.id, query_id));
                            Some(MembershipChange::Remove)
                        }
                        _ => {
                            watcher_changes.push(if matches {
                                WatcherChange::add(entity_id, self.id, query_id)
                            } else {
                                WatcherChange::remove(entity_id, self.id, query_id)
                            });
                            None
                        }
                    };

                    if matches || did_match || state.entity_subscriptions.contains(&entity_id) {
                        let item = items.entry(entity_id).or_insert_with(|| ReactorUpdateItem {
                            entity: entity.clone(),
                            events: change.events().to_vec(),
                            predicate_relevance: Vec::new(),
                        });

                        if let Some(change) = membership_change {
                            item.predicate_relevance.push((query_id, change));
                        }
                    }
                }
            }

            // Process entity-level subscriptions not covered by query processing
            for change in candidates.entity_iter() {
                let entity = change.entity();
                let entity_id = *AbstractEntity::id(entity);

                if state.entity_subscriptions.contains(&entity_id) {
                    items.entry(entity_id).or_insert(ReactorUpdateItem {
                        entity: entity.clone(),
                        events: change.events().to_vec(),
                        predicate_relevance: Vec::new(),
                    });
                }
            }
            let gaps_to_fill = self.take_gap_fills_internal(state);
            let broadcast = state.broadcast.clone();
            (watcher_changes, items.into_values().collect::<Vec<_>>(), gaps_to_fill, broadcast)
        };

        if gaps_to_fill.is_empty() {
            if !update_items.is_empty() {
                broadcast.send(ReactorUpdate { items: update_items });
            }
        } else {
            crate::task::spawn(self.fill_gaps_and_notify(update_items, gaps_to_fill, broadcast));
        }

        watcher_changes
    }

    /// Collect gaps to fill (internal version that works with locked state)
    fn take_gap_fills_internal(&self, state: &mut State<E, Ev>) -> Vec<PendingGapFill<E>> {
        state.queries.iter_mut().filter_map(|(query_id, query_state)| self.take_gap_fill_for(*query_id, query_state)).collect()
    }

    pub(super) fn take_gap_fill(&self, query_id: proto::QueryId) -> anyhow::Result<PendingGapFill<E>> {
        let mut state = self.state.lock().unwrap();
        let query_state = state.queries.get_mut(&query_id).ok_or_else(|| anyhow::anyhow!("Query not found for gap fill"))?;
        Ok(self.take_gap_fill_for(query_id, query_state).unwrap_or(PendingGapFill {
            query_id,
            generation: query_state.generation,
            request: None,
        }))
    }

    pub(super) async fn finish_gap_fill(&self, pending: PendingGapFill<E>) -> (tokio::sync::OwnedMutexGuard<()>, Option<Vec<E>>) {
        let fetched = Self::fetch_gap(pending).await;
        let notify = self.notify_lock.clone().lock_owned().await;
        let entities = self.apply_gap(fetched);
        (notify, entities)
    }

    /// Fill dirty gaps and notify if their query generations still match.
    async fn fill_gaps_and_notify(
        self,
        mut items: Vec<ReactorUpdateItem<E, Ev>>,
        gaps_to_fill: Vec<PendingGapFill<E>>,
        broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
    ) {
        let fetched = future::join_all(gaps_to_fill.into_iter().map(Self::fetch_gap)).await;
        let _notify = self.notify_lock.clone().lock_owned().await;
        for gap in fetched {
            let query_id = gap.query_id;
            if let Some(entities) = self.apply_gap(gap) {
                items.extend(entities.into_iter().map(|entity| ReactorUpdateItem {
                    entity,
                    events: vec![],
                    predicate_relevance: vec![(query_id, MembershipChange::Add)],
                }));
            }
        }

        if !items.is_empty() {
            broadcast.send(ReactorUpdate { items });
        }
    }

    fn take_gap_fill_for(&self, query_id: proto::QueryId, query_state: &mut QueryState<E>) -> Option<PendingGapFill<E>> {
        let resultset = &query_state.resultset;

        if query_state.paused || query_state.selection.is_none() || !resultset.is_gap_dirty() {
            return None;
        }

        let limit = resultset.get_limit()?;
        let current_len = resultset.len();

        if current_len >= limit {
            return None;
        }

        let gap_size = limit - current_len;
        let last_entity = resultset.last_entity();

        let selection = query_state.selection.clone()?;

        resultset.clear_gap_dirty();
        Some(PendingGapFill {
            query_id,
            generation: query_state.generation,
            request: Some(GapRequest {
                fetcher: query_state.gap_fetcher.clone(),
                collection_id: query_state.collection_id.clone(),
                selection,
                last_entity,
                size: gap_size,
            }),
        })
    }

    async fn fetch_gap(pending: PendingGapFill<E>) -> FetchedGap<E> {
        let Some(request) = pending.request else {
            return FetchedGap { query_id: pending.query_id, generation: pending.generation, entities: Vec::new() };
        };

        tracing::debug!("Gap filling for query {} - need {} entities", pending.query_id, request.size);
        let entities =
            match request.fetcher.fetch_gap(&request.collection_id, &request.selection, request.last_entity.as_ref(), request.size).await {
                Ok(entities) => entities,
                Err(error) => {
                    tracing::warn!("Gap filling failed for query {}: {}", pending.query_id, error);
                    Vec::new()
                }
            };
        FetchedGap { query_id: pending.query_id, generation: pending.generation, entities }
    }

    fn apply_gap(&self, fetched: FetchedGap<E>) -> Option<Vec<E>> {
        let mut state = self.state.lock().unwrap();
        let Some(query_state) = state.queries.get_mut(&fetched.query_id) else {
            return None;
        };
        if query_state.generation != fetched.generation || query_state.paused {
            return None;
        }

        let mut added = Vec::new();
        let mut resultset = query_state.resultset.write();
        for entity in fetched.entities {
            if resultset.add(entity.clone()) {
                added.push(entity);
            }
        }
        drop(resultset);
        for entity in &added {
            state.entities.insert(*AbstractEntity::id(entity), entity.clone());
        }
        drop(state);

        if !added.is_empty() {
            self.add_entity_watchers(fetched.query_id, added.iter().map(|entity| *AbstractEntity::id(entity)));
        }
        Some(added)
    }
}

// Entity-specific methods for remote subscriptions
impl Subscription<crate::entity::Entity, ankurah_proto::Attested<ankurah_proto::Event>> {
    /// Register a new query or reset an existing one to a fresh baseline.
    pub fn register_or_get_query(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<crate::entity::Entity>>,
        version: u32,
    ) -> anyhow::Result<EntityResultSet<crate::entity::Entity>> {
        let (resultset, stale) = {
            let mut state = self.state.lock().unwrap();

            use std::collections::hash_map::Entry;
            match state.queries.entry(query_id) {
                Entry::Vacant(v) => {
                    let resultset = EntityResultSet::empty();
                    v.insert(QueryState {
                        collection_id: collection_id.clone(),
                        selection: None,
                        gap_fetcher,
                        paused: false,
                        resultset: resultset.clone(),
                        version,
                        generation: 0,
                    });
                    (resultset, None)
                }
                Entry::Occupied(mut o) => {
                    let query_state = o.get_mut();
                    if query_state.collection_id != collection_id {
                        anyhow::bail!("query {query_id} is already bound to collection '{}'", query_state.collection_id);
                    }
                    if version < query_state.version {
                        anyhow::bail!("stale query version {version} for {query_id}; current version is {}", query_state.version);
                    }
                    let old_selection = query_state.selection.take();
                    let old_ids: Vec<proto::EntityId> = query_state.resultset.keys().collect();
                    query_state.resultset.clear();
                    query_state.resultset.set_loaded(false);
                    query_state.gap_fetcher = gap_fetcher;
                    query_state.paused = true;
                    query_state.version = version;
                    query_state.generation = query_state.generation.checked_add(1).expect("query generation exhausted");
                    (query_state.resultset.clone(), old_selection.map(|selection| (selection, old_ids)))
                }
            }
        };
        if let Some((old_selection, old_ids)) = stale {
            let mut watcher_set = self.watcher_set.lock().unwrap();
            watcher_set.recurse_predicate_watchers(
                &collection_id,
                &old_selection.predicate,
                (self.id, query_id),
                crate::reactor::watcherset::WatcherOp::Remove,
            );
            watcher_set.cleanup_removed_predicate_watchers(self.id, query_id, &old_ids);
        }
        Ok(resultset)
    }
}
