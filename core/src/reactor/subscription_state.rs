use crate::internal::prelude::*;
use crate::reactor::{
    delivery::{self, DeliveredHeads, DeliveryPlan, PreparedEvents},
    AbstractEntity, CandidateChanges, ChangeNotification, LocalEntitySource, MembershipChange, ReactorSubscriptionId, ReactorUpdate,
    ReactorUpdateItem, WatcherChange,
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

mod remote_query;

/// A notification that `entity` entered or left `query_id`'s results, carrying no events.
fn membership_item<E: Clone, Ev>(entity: &E, query_id: proto::QueryId, change: MembershipChange) -> ReactorUpdateItem<E, Ev> {
    ReactorUpdateItem { entity: entity.clone(), events: vec![], predicate_relevance: vec![(query_id, change)] }
}

type GapFillData<E> = (
    proto::QueryId,
    std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
    ankql::ast::Selection<Resolved>,
    EntityResultSet<E>,
    Option<E>,
    usize,
);

/// State for a single predicate within a subscription
pub(super) struct QueryState<E: AbstractEntity + Filterable + Send + 'static> {
    // TODO make this a clonable PredicateSubscription and store it instead of the channel?
    /// None before initialization.
    pub(crate) selection: Option<ankql::ast::Selection<Resolved>>,
    pub(crate) gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>, // For filling gaps when LIMIT is applied
    pub(crate) source: Arc<dyn LocalEntitySource<E>>,
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) paused: bool, // When true, skip notifications (used during initialization and updates)
    pub(crate) resultset: EntityResultSet<E>,
    pub(crate) version: u32,
    /// Remains pending across partial publications of a replacement selection.
    initializing: bool,
    delivered: DeliveredHeads,
}

// We would call this ReactorSubscription, but that name is reserved for the public API
// so instead we will call it Subscription and just scope it to the reactor package
pub(super) struct Subscription<E: AbstractEntity + Filterable + Send + 'static, Ev>(Arc<Inner<E, Ev>>);

impl<E: AbstractEntity + Filterable + Send + 'static, Ev> Clone for Subscription<E, Ev> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev> std::ops::Deref for Subscription<E, Ev> {
    type Target = Inner<E, Ev>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev> Subscription<E, Ev> {
    /// Get the subscription ID
    pub fn id(&self) -> ReactorSubscriptionId { self.0.id }
}

pub(super) struct Inner<E: AbstractEntity + Filterable + Send + 'static, Ev> {
    pub(super) id: ReactorSubscriptionId,
    state: std::sync::Mutex<State<E, Ev>>,
    watcher_set: Arc<std::sync::Mutex<crate::reactor::watcherset::WatcherSet>>,
}
struct State<E: AbstractEntity + Filterable + Send + 'static, Ev> {
    pub(crate) queries: HashMap<proto::QueryId, QueryState<E>>,
    /// Explicit entity subscriptions, independent of the matches in `queries`.
    pub(crate) entity_subscriptions: HashSet<proto::EntityId>,
    entity_delivered: DeliveredHeads,
    // not sure if we actually need this
    pub(crate) entities: HashMap<proto::EntityId, E>,
    pub(crate) broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: crate::reactor::ReactorEvent> Subscription<E, Ev> {
    pub fn new(
        broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
        watcher_set: Arc<std::sync::Mutex<crate::reactor::watcherset::WatcherSet>>,
    ) -> Self {
        Self(Arc::new(Inner {
            id: ReactorSubscriptionId::new(),
            state: std::sync::Mutex::new(State {
                queries: HashMap::new(),
                entity_subscriptions: HashSet::new(),
                entity_delivered: DeliveredHeads::default(),
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
        state.entity_delivered.forget(&entity_id);
    }

    /// Check if any queries match this entity (for determining if entity watcher should be removed)
    pub fn any_query_matches(&self, entity_id: &proto::EntityId) -> bool {
        let state = self.state.lock().unwrap();
        state.queries.values().any(|q| q.resultset.contains_key(entity_id))
    }

    /// Clear current results when the reactor closes.
    pub fn close(&self) {
        let (update_items, broadcast) = {
            let state = &mut *self.state.lock().unwrap();
            let mut update_items = Vec::new();
            for (query_id, query_state) in std::mem::take(&mut state.queries) {
                for entity_id in query_state.resultset.keys() {
                    if let Some(entity) = state.entities.get(&entity_id).filter(|entity| query_state.source.can_read(entity)) {
                        update_items.push(ReactorUpdateItem {
                            entity: entity.clone(),
                            events: vec![],
                            predicate_relevance: vec![(query_id, MembershipChange::Remove)],
                        });
                    }
                }
                query_state.resultset.set_loaded(false);
                query_state.resultset.clear();
            }
            state.entity_subscriptions.clear();
            state.entity_delivered.clear();
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

    /// Register a local query if absent, preserving an existing resultset. Returns whether it was inserted.
    pub fn ensure_query_registered(
        &self,
        query_id: proto::QueryId,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
        source: Arc<dyn LocalEntitySource<E>>,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        if let Some(query) = state.queries.get_mut(&query_id) {
            query.source = source;
            query.gap_fetcher = gap_fetcher;
            return false;
        }
        state.queries.insert(
            query_id,
            QueryState {
                selection: None,
                gap_fetcher,
                source,
                paused: false,
                resultset,
                version: 0,
                initializing: true,
                delivered: DeliveredHeads::default(),
            },
        );
        true
    }

    /// Update predicate watchers for a query (index/wildcard watchers)
    /// If old_predicate is None, only adds watchers (for initial setup)
    /// If old_predicate is Some, removes old watchers and adds new ones
    pub fn update_predicate_watchers(
        &self,
        query_id: proto::QueryId,
        old_predicate: Option<&ankql::ast::Predicate<Resolved>>,
        new_predicate: &ankql::ast::Predicate<Resolved>,
    ) {
        let mut watcher_set = self.watcher_set.lock().unwrap();
        let watcher_id = (self.id, query_id);

        if let Some(old_pred) = old_predicate {
            watcher_set.recurse_predicate_watchers(old_pred, watcher_id, crate::reactor::watcherset::WatcherOp::Remove);
        }
        watcher_set.recurse_predicate_watchers(new_predicate, watcher_id, crate::reactor::watcherset::WatcherOp::Add);
    }

    /// Add entity watchers for entities in a query's resultset
    pub fn add_entity_watchers(&self, query_id: proto::QueryId, entity_ids: impl Iterator<Item = proto::EntityId>) {
        let mut watcher_set = self.watcher_set.lock().unwrap();
        watcher_set.add_predicate_entity_watchers(self.id, query_id, entity_ids);
    }
    /// Update a query's selection and results, pushing the membership changes to `reactor_updates`.
    pub fn update_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        included_entities: Vec<E>,
        version: u32,
        reactor_updates: &mut Vec<ReactorUpdateItem<E, Ev>>,
        remote: Option<&crate::reactor::RemoteMembership>,
        complete: bool,
    ) -> anyhow::Result<()> {
        let mut state_guard = self.state.lock().unwrap();
        let state = &mut *state_guard;

        let query_state = state.queries.get_mut(&query_id).ok_or_else(|| anyhow::anyhow!("Query not found for update"))?;
        if query_state.version != version {
            query_state.initializing = true;
        }
        let initial_baseline = query_state.initializing && complete;
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

        let mut removed_entities = Vec::new();
        if let Some(membership) = remote {
            (newly_added, removed_entities) = query_state.reconcile_remote_rows(
                query_id,
                &mut rw_resultset,
                included_entities,
                membership,
                &selection.predicate,
                initial_baseline,
                reactor_updates,
            );
            for entity in &newly_added {
                state.entities.insert(entity.id(), entity.clone());
            }
        } else {
            rw_resultset.mark_all_dirty();

            for entity in included_entities {
                if evaluate_predicate(&entity, &selection.predicate).unwrap_or(false)
                    && query_state.source.can_read(&entity)
                    && query_state.source.admits_query_entity(&entity)
                {
                    let entity_id = AbstractEntity::id(&entity);

                    if !rw_resultset.contains(&entity_id) {
                        rw_resultset.add(entity.clone());
                        state.entities.insert(entity_id, entity.clone());
                        reactor_updates.push(membership_item(&entity, query_id, MembershipChange::Initial));
                        newly_added.push(entity);
                    }
                }
            }

            rw_resultset.retain_dirty(|entity| {
                let can_read = query_state.source.can_read(entity);
                if can_read
                    && query_state.source.admits_query_entity(entity)
                    && evaluate_predicate(entity, &selection.predicate).unwrap_or(false)
                {
                    return true;
                };
                let entity_id = entity.id();
                tracing::debug!("Entity {:?} no longer matches predicate", entity_id);

                removed_entities.push(entity_id);
                if can_read {
                    reactor_updates.push(membership_item(entity, query_id, MembershipChange::Remove));
                }
                false
            });
        }

        // Existing event frontiers survive silent state repairs and refreshes.
        for (id, entity) in rw_resultset.iter_entities() {
            if let Some(head) = remote.and_then(|m| m.known_heads.get(&id)).cloned().or_else(|| entity.head()) {
                query_state.delivered.start(id, head);
            }
        }
        query_state.delivered.retain(|id| rw_resultset.contains(id));
        query_state.paused = false;
        query_state.version = version;
        if complete {
            query_state.initializing = false;
        }

        drop(rw_resultset);

        drop(state_guard);

        let should_update_watchers = is_first_update || old_selection.as_ref().is_some_and(|old| old.predicate != selection.predicate);

        if should_update_watchers {
            let old_pred = old_selection.as_ref().map(|s| &s.predicate);
            self.update_predicate_watchers(query_id, old_pred, &selection.predicate);
        }

        if !newly_added.is_empty() {
            self.add_entity_watchers(query_id, newly_added.iter().map(|e| AbstractEntity::id(e)));
        }

        if !removed_entities.is_empty() {
            let mut watcher_set = self.watcher_set.lock().unwrap();
            watcher_set.cleanup_removed_predicate_watchers(self.id, query_id, &removed_entities);
        }

        Ok(())
    }

    /// Send ReactorUpdate with the given items
    pub fn send_update(&self, items: Vec<ReactorUpdateItem<E, Ev>>) {
        let broadcast = self.state.lock().unwrap().broadcast.clone();
        broadcast.send(ReactorUpdate { items });
    }

    /// Get all queries for cleanup (used by unsubscribe)
    pub fn take_all_queries(&self) -> HashMap<proto::QueryId, QueryState<E>> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.queries)
    }

    /// Resolve causal delivery before mutating membership. No mutex guard crosses
    /// history I/O; the reactor's notification lock serializes the whole operation.
    pub(super) async fn prepare_events<C: ChangeNotification<Entity = E, Event = Ev>>(
        &self,
        candidates: &CandidateChanges<C>,
        held: Option<&HashSet<proto::EntityId>>,
        history: Option<&(dyn crate::retrieval::GetEvents + Send + Sync)>,
    ) -> (PreparedEvents<Ev>, Vec<crate::reactor::NotificationFailure>) {
        let plans = {
            let state = self.state.lock().unwrap();
            let mut plans = Vec::new();
            for candidate in candidates.query_iter() {
                let id = *candidate.query_id;
                let Some(query) = state.queries.get(&id).filter(|q| !q.paused) else { continue };
                let Some(selection) = query.selection.as_ref() else { continue };
                let mut rows = Vec::new();
                for (offset, change) in candidate.iter().enumerate() {
                    let entity_id = change.entity().id();
                    let Some(effect) = query.effect_of(id, &selection.predicate, change, held) else { continue };
                    if effect.publishes(state.entity_subscriptions.contains(&entity_id)) {
                        rows.push((offset, entity_id, change.events()));
                    }
                }
                let delivered = query.delivered.of(rows.iter().map(|(_, id, _)| *id));
                plans.push(DeliveryPlan { query: Some(id), version: query.version, delivered, rows });
            }
            let rows: Vec<_> = candidates
                .entity_iter()
                .enumerate()
                .filter(|(_, c)| state.entity_subscriptions.contains(&c.entity().id()))
                .map(|(offset, c)| (offset, c.entity().id(), c.events()))
                .collect();
            let delivered = state.entity_delivered.of(rows.iter().map(|(_, id, _)| *id));
            plans.push(DeliveryPlan { query: None, version: 0, delivered, rows });
            plans
        };
        let (prepared, failures) = delivery::prepare(history, plans).await;
        if !failures.is_empty() {
            let mut state = self.state.lock().unwrap();
            for failure in &failures {
                if let Some(query) = failure.query.and_then(|id| state.queries.get_mut(&id)) {
                    query.paused = true;
                    query.resultset.set_loaded(false);
                } else if failure.query.is_none() {
                    state.entity_subscriptions.clear();
                    state.entity_delivered.clear();
                }
            }
        }
        (prepared, failures)
    }

    pub(super) async fn filter_query_events(
        &self,
        query_id: proto::QueryId,
        changes: &mut [ReactorUpdateItem<E, Ev>],
        baseline: &HashMap<proto::EntityId, proto::Clock>,
        history: Option<&(dyn crate::retrieval::GetEvents + Send + Sync)>,
    ) -> Result<DeliveredHeads, RetrievalError> {
        let mut delivered = {
            let state = self.state.lock().unwrap();
            let query = state.queries.get(&query_id).ok_or_else(|| RetrievalError::Other("query disappeared".into()))?;
            let mut delivered = query.delivered.of(changes.iter().map(|change| change.entity.id()));
            for change in changes.iter() {
                if let Some(head) = baseline.get(&change.entity.id()) {
                    delivered.start(change.entity.id(), head.clone());
                }
            }
            delivered
        };
        delivery::filter_changes(history, &mut delivered, changes).await?;
        Ok(delivered)
    }

    pub(super) fn commit_query_events(&self, query_id: proto::QueryId, delivered: DeliveredHeads) {
        let mut state = self.state.lock().unwrap();
        if let Some(query) = state.queries.get_mut(&query_id) {
            query.delivered.extend_from(delivered, |id| query.resultset.contains_key(id));
        }
    }

    /// Evaluate candidate changes for this subscription and spawn gap filling/notification task
    /// Returns watcher changes that need to be applied to the global WatcherSet
    pub fn evaluate_changes<C: ChangeNotification<Entity = E, Event = Ev>>(
        self,
        candidates: CandidateChanges<C>,
        prepared: PreparedEvents<Ev>,
    ) -> Vec<WatcherChange> {
        self.evaluate_candidates(candidates, prepared, None, None)
    }

    fn evaluate_candidates<C: ChangeNotification<Entity = E, Event = Ev>>(
        self,
        candidates: CandidateChanges<C>,
        mut prepared: PreparedEvents<Ev>,
        held: Option<&HashSet<proto::EntityId>>,
        notified: Option<&mut HashSet<proto::EntityId>>,
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

            // Explicit publications own their refill and callback boundary.
            let mut current_publication = false;
            // Process all candidate changes for this query
            for (offset, change) in query_candidate.iter().enumerate() {
                current_publication |= held.is_some()
                    && change
                        .query_origin()
                        .is_some_and(|origin| origin.query == query_id && query_state.source.query_origin() == Some(origin));
                let entity = change.entity();
                let entity_id = AbstractEntity::id(entity);

                debug!("Subscription {} evaluating entity {} for query {}", self.id(), entity_id, query_id);

                let Some(effect) = query_state.effect_of(query_id, &selection.predicate, change, held) else { continue };
                let (did_match, matches) = (effect.did_match, effect.matches);

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

                // Removal currently carries state too; never send a newly denied state (#426 owns claw-back).
                if effect.publishes(state.entity_subscriptions.contains(&entity_id)) {
                    // Planning made the same decision, so this change has prepared events unless
                    // the query changed meanwhile (a gap fill can add a row while history is
                    // read); then it publishes without events.
                    let (head, events) = prepared.take(Some(query_id), offset).unwrap_or_default();
                    query_state.delivered.advance(entity_id, head);
                    let duplicate = !change.events().is_empty() && events.is_empty();
                    if duplicate && membership_change.is_none() {
                        continue;
                    }
                    let item = items.entry(entity_id).or_insert_with(|| ReactorUpdateItem {
                        entity: entity.clone(),
                        events: Vec::new(),
                        predicate_relevance: Vec::new(),
                    });
                    if item.events.is_empty() {
                        item.events = events;
                    } else {
                        for event in events {
                            if event.causal_event().is_some_and(|incoming| {
                                !item
                                    .events
                                    .iter()
                                    .any(|existing| existing.causal_event().is_some_and(|existing| existing.id() == incoming.id()))
                            }) {
                                item.events.push(event);
                            }
                        }
                    }

                    if let Some(mc) = membership_change {
                        item.predicate_relevance.push((query_id, mc));
                    }
                }
                if !matches {
                    query_state.delivered.forget(&entity_id);
                }
            }
            if current_publication {
                query_state.resultset.clear_gap_dirty();
            }
        }

        // Process entity-level subscriptions not covered by query processing
        for (offset, change) in candidates.entity_iter().enumerate() {
            let entity = change.entity();
            let entity_id = AbstractEntity::id(entity);

            if state.entity_subscriptions.contains(&entity_id) {
                let (head, events) = prepared.take(None, offset).unwrap_or_default();
                state.entity_delivered.advance(entity_id, head);
                if !items.contains_key(&entity_id) && (change.events().is_empty() || !events.is_empty()) {
                    items.insert(entity_id, ReactorUpdateItem { entity: entity.clone(), events, predicate_relevance: Vec::new() });
                }
            }
        }

        // Collect gap fill data while we have the state lock
        let gaps_to_fill = if held.is_some() { Vec::new() } else { self.collect_gaps_to_fill_internal(&state) };
        let broadcast = state.broadcast.clone();

        // Drop the state lock before spawning
        drop(state_guard);

        // Spawn background task for gap filling and notification
        // fill_gaps_and_notify will register entity watchers for gap-filled entities
        if let Some(notified) = notified {
            notified.extend(items.keys().copied());
        }
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

    /// Fill gaps for a specific query and push ReactorUpdateItems to `reactor_updates`
    /// Also registers entity watchers for gap-filled entities
    pub async fn fill_gaps_for_query(&self, query_id: proto::QueryId, reactor_updates: &mut Vec<ReactorUpdateItem<E, Ev>>) {
        let gap_data = {
            let state = self.state.lock().unwrap();
            state.queries.get(&query_id).and_then(|query_state| self.extract_gap_data(query_id, query_state))
        };

        let Some((query_id, gap_fetcher, selection, resultset, last_entity, gap_size)) = gap_data else {
            return;
        };

        // Clear gap_dirty flag immediately
        resultset.clear_gap_dirty();

        // Process gap fill
        let gap_filled_entities = self.process_gap_fill_entities(query_id, gap_fetcher, selection, resultset, last_entity, gap_size).await;

        // Register entity watchers and push items for gap-filled entities
        if !gap_filled_entities.is_empty() {
            self.add_entity_watchers(query_id, gap_filled_entities.iter().map(|e| AbstractEntity::id(e)));

            for entity in gap_filled_entities {
                reactor_updates.push(membership_item(&entity, query_id, MembershipChange::Initial));
            }
        }
    }

    // Keep the current source check and result mutation together: an old local gap fetch must
    // not append rows the remote stream did not admit after the stream installs its answer.
    fn insert_gap_entities(
        &self,
        query_id: proto::QueryId,
        selection: &ankql::ast::Selection<Resolved>,
        resultset: &EntityResultSet<E>,
        entities: Vec<E>,
    ) -> Vec<E> {
        let state = self.state.lock().unwrap();
        let Some(query) = state.queries.get(&query_id).filter(|query| query.selection.as_ref() == Some(selection)) else {
            return Vec::new();
        };
        let mut write = resultset.write();
        entities
            .into_iter()
            .filter(|entity| {
                query.source.can_read(entity)
                    && query.source.admits_query_entity(entity)
                    && evaluate_predicate(entity, &selection.predicate).unwrap_or(false)
                    && write.add(entity.clone())
            })
            .collect()
    }

    async fn process_gap_fill_entities(
        &self,
        query_id: proto::QueryId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
        selection: ankql::ast::Selection<Resolved>,
        resultset: EntityResultSet<E>,
        last_entity: Option<E>,
        gap_size: usize,
    ) -> Vec<E> {
        tracing::debug!("Gap filling for query {} - need {} entities", query_id, gap_size);

        match gap_fetcher.fetch_gap(&selection, last_entity.as_ref(), gap_size).await {
            Ok(gap_entities) => {
                if !gap_entities.is_empty() {
                    tracing::debug!("Gap filling fetched {} entities for query {}", gap_entities.len(), query_id);

                    self.insert_gap_entities(query_id, &selection, &resultset, gap_entities)
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
        for (_, _, _, ref resultset, _, _) in &gaps_to_fill {
            resultset.clear_gap_dirty();
        }

        // Process all gap fills concurrently
        let gap_fill_futures = gaps_to_fill.into_iter().map(|(query_id, gap_fetcher, selection, resultset, last_entity, gap_size)| {
            self.process_gap_fill(query_id, gap_fetcher, selection, resultset, last_entity, gap_size)
        });

        let gap_results: Vec<(ankurah_proto::QueryId, Vec<ReactorUpdateItem<E, Ev>>)> = future::join_all(gap_fill_futures).await;

        // Collect all the new items from gap filling and register entity watchers
        for (query_id, gap_items) in gap_results {
            if !gap_items.is_empty() {
                // Register entity watchers for gap-filled entities
                let entity_ids: Vec<_> = gap_items.iter().map(|item| AbstractEntity::id(&item.entity)).collect();
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

        Some((query_id, query_state.gap_fetcher.clone(), selection, resultset.clone(), last_entity, gap_size))
    }

    async fn process_gap_fill(
        &self,
        query_id: proto::QueryId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
        selection: ankql::ast::Selection<Resolved>,
        resultset: EntityResultSet<E>,
        last_entity: Option<E>,
        gap_size: usize,
    ) -> (proto::QueryId, Vec<ReactorUpdateItem<E, Ev>>) {
        tracing::debug!("Gap filling for query {} - need {} entities", query_id, gap_size);

        let gap_items = match gap_fetcher.fetch_gap(&selection, last_entity.as_ref(), gap_size).await {
            Ok(gap_entities) => {
                if !gap_entities.is_empty() {
                    tracing::debug!("Gap filling fetched {} entities for query {}", gap_entities.len(), query_id);

                    self.insert_gap_entities(query_id, &selection, &resultset, gap_entities)
                        .into_iter()
                        .map(|entity| ReactorUpdateItem {
                            entity,
                            events: vec![],
                            predicate_relevance: vec![(query_id, MembershipChange::Add)],
                        })
                        .collect()
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
