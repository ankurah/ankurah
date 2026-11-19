use crate::internal::prelude::*;
use crate::reactor::{
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

struct GapFill<E: AbstractEntity> {
    query: proto::QueryId,
    revision: Arc<()>,
    fetcher: Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
    selection: ankql::ast::Selection<Resolved>,
    last: Option<E>,
    size: usize,
}

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
    /// A particular installation, including reactivations with the same wire version.
    revision: Arc<()>,
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
    notify_lock: Arc<tokio::sync::Mutex<()>>,
}
struct State<E: AbstractEntity + Filterable + Send + 'static, Ev> {
    closed: bool,
    pub(crate) queries: HashMap<proto::QueryId, QueryState<E>>,
    /// Explicit entity subscriptions, independent of the matches in `queries`.
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
                closed: false,
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
        if !state.closed {
            state.entity_subscriptions.insert(entity_id);
        }
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

    /// Clear current results when the reactor closes.
    pub fn close(&self) {
        let (queries, entities, broadcast) = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.entity_subscriptions.clear();
            (std::mem::take(&mut state.queries), std::mem::take(&mut state.entities), state.broadcast.clone())
        };

        // Resultset listeners can drop subscription handles and reenter cleanup.
        // Detach the state before notifying them or dropping query-owned values.
        let mut update_items = Vec::new();
        for (query_id, query_state) in queries {
            for entity_id in query_state.resultset.keys() {
                if let Some(entity) = entities.get(&entity_id).filter(|entity| query_state.source.can_read(entity)) {
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

        if !update_items.is_empty() {
            broadcast.send(ReactorUpdate { items: update_items });
        }
    }

    /// Get the number of queries for debugging
    pub fn queries_len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.queries.len()
    }

    pub(super) fn query_revision(&self, query: proto::QueryId) -> Option<Arc<()>> {
        self.state.lock().unwrap().queries.get(&query).map(|query| query.revision.clone())
    }

    pub(super) fn is_current(&self, query: proto::QueryId, revision: &Arc<()>) -> bool {
        self.query_revision(query).is_some_and(|current| Arc::ptr_eq(&current, revision))
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
        if state.closed || state.queries.contains_key(&query_id) {
            return false;
        }
        state.queries.insert(
            query_id,
            QueryState { selection: None, gap_fetcher, source, paused: false, resultset, version: 0, revision: Arc::new(()) },
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
    /// Update a query, returning entities newly added to its result set.
    pub fn update_query<A: UpdateItemAccumulator<E>>(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        included_entities: Vec<E>,
        version: u32,
        reactor_updates: &mut A,
    ) -> anyhow::Result<Vec<E>> {
        let mut state_guard = self.state.lock().unwrap();
        let state = &mut *state_guard;

        let query_state = state.queries.get_mut(&query_id).ok_or_else(|| anyhow::anyhow!("Query not found for update"))?;
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
            if evaluate_predicate(&entity, &selection.predicate).unwrap_or(false) && query_state.source.can_read(&entity) {
                let entity_id = AbstractEntity::id(&entity);

                if !rw_resultset.contains(&entity_id) {
                    rw_resultset.add(entity.clone());
                    state.entities.insert(entity_id, entity.clone());
                    reactor_updates.push_initial(&entity, query_id);
                    newly_added.push(entity);
                }
            }
        }

        let mut removed_entities = Vec::new();
        rw_resultset.retain_dirty(|entity| {
            let can_read = query_state.source.can_read(entity);
            if can_read && evaluate_predicate(entity, &selection.predicate).unwrap_or(false) {
                return true;
            };
            let entity_id = entity.id();
            tracing::debug!("Entity {:?} no longer matches predicate", entity_id);

            removed_entities.push(entity_id);
            if can_read {
                reactor_updates.push_remove(entity, query_id);
            }
            false
        });

        query_state.paused = false;
        query_state.version = version;
        query_state.revision = Arc::new(());

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

    /// Evaluate candidate changes for this subscription and spawn gap filling/notification task
    /// Returns watcher changes that need to be applied to the global WatcherSet
    pub fn evaluate_changes<C: ChangeNotification<Entity = E, Event = Ev>>(self, candidates: CandidateChanges<C>) -> Vec<WatcherChange> {
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
                let entity_id = AbstractEntity::id(entity);

                debug!("Subscription {} evaluating entity {} for query {}", self.id(), entity_id, query_id);

                let can_read = query_state.source.can_read(entity);
                let matches = can_read && evaluate_predicate(entity, &selection.predicate).unwrap_or(false);
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
                // Removal currently carries state too; never send a newly denied state (#426 owns claw-back).
                if can_read && (matches || did_match || entity_subscribed) {
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
            let entity_id = AbstractEntity::id(entity);

            if state.entity_subscriptions.contains(&entity_id) && !items.contains_key(&entity_id) {
                items.entry(entity_id).or_insert(ReactorUpdateItem {
                    entity: entity.clone(),
                    events: change.events().to_vec(),
                    predicate_relevance: Vec::new(),
                });
            }
        }

        // Collect gap fill data while we have the state lock
        let gaps_to_fill = self.collect_gaps_to_fill_internal(&state);
        let revisions = if gaps_to_fill.is_empty() {
            HashMap::new()
        } else {
            state.queries.iter().map(|(id, query)| (*id, query.revision.clone())).collect()
        };
        let broadcast = state.broadcast.clone();

        // Drop the state lock before spawning
        drop(state_guard);

        // Spawn background task for gap filling and notification
        // fill_gaps_and_notify will register entity watchers for gap-filled entities
        let update_items: Vec<ReactorUpdateItem<E, Ev>> = items.into_values().collect();
        if !gaps_to_fill.is_empty() {
            crate::task::spawn(self.clone().fill_gaps_and_notify(update_items, gaps_to_fill, revisions));
        } else if !update_items.is_empty() {
            broadcast.send(ReactorUpdate { items: update_items });
        }

        watcher_changes
    }

    /// Capture only the query installations whose result sets have a gap.
    fn collect_gaps_to_fill_internal(&self, state: &State<E, Ev>) -> Vec<GapFill<E>> {
        state.queries.iter().filter_map(|(id, query)| self.extract_gap_data(*id, query)).collect()
    }

    pub async fn fill_gaps_for_query_entities(&self, query: proto::QueryId, entities: &mut Vec<E>) {
        entities.extend(self.fill_gap_for_query(query).await);
    }

    pub async fn fill_gaps_for_query<A: UpdateItemAccumulator<E>>(&self, query: proto::QueryId, updates: &mut A) {
        for entity in self.fill_gap_for_query(query).await {
            updates.push_initial(&entity, query);
        }
    }

    async fn fill_gap_for_query(&self, query: proto::QueryId) -> Vec<E> {
        let gap = {
            let state = self.state.lock().unwrap();
            state.queries.get(&query).and_then(|state| self.extract_gap_data(query, state))
        };
        match gap {
            Some(gap) => self.fill_gap(gap).await,
            None => Vec::new(),
        }
    }

    /// Fetch without the publication lock, then apply only to the installation
    /// which requested the gap. Close and query replacement invalidate it.
    async fn fill_gap(&self, gap: GapFill<E>) -> Vec<E> {
        let entities = match gap.fetcher.fetch_gap(&gap.selection, gap.last.as_ref(), gap.size).await {
            Ok(entities) => entities,
            Err(error) => {
                tracing::warn!("Gap filling failed for query {}: {}", gap.query, error);
                return Vec::new();
            }
        };
        let _notify = self.notify_lock.lock().await;
        // Match the existing entity-subscription lock order. Keep both through
        // mutation so synchronous query removal cannot leave orphan watchers.
        let mut watchers = self.watcher_set.lock().unwrap();
        let mut state = self.state.lock().unwrap();
        let Some(query) = state.queries.get(&gap.query).filter(|query| Arc::ptr_eq(&query.revision, &gap.revision)) else {
            return Vec::new();
        };
        let resultset = query.resultset.clone();
        let source = query.source.clone();
        let mut write = resultset.write();
        let mut added = Vec::new();
        for entity in entities {
            if source.can_read(&entity) && write.add(entity.clone()) {
                state.entities.insert(AbstractEntity::id(&entity), entity.clone());
                added.push(entity);
            }
        }
        watchers.add_predicate_entity_watchers(self.id, gap.query, added.iter().map(AbstractEntity::id));
        // Result-set listeners may remove queries or subscriptions synchronously.
        drop(state);
        drop(watchers);
        drop(write);
        added
    }

    async fn fill_gaps_and_notify(
        self,
        mut items: Vec<ReactorUpdateItem<E, Ev>>,
        gaps: Vec<GapFill<E>>,
        revisions: HashMap<proto::QueryId, Arc<()>>,
    ) {
        let results = future::join_all(gaps.into_iter().map(|gap| {
            let subscription = &self;
            async move {
                let query = gap.query;
                (query, subscription.fill_gap(gap).await)
            }
        }))
        .await;
        for (query, entities) in results {
            items.extend(entities.into_iter().map(|entity| ReactorUpdateItem {
                entity,
                events: Vec::new(),
                predicate_relevance: vec![(query, MembershipChange::Add)],
            }));
        }

        let _notify = self.notify_lock.lock().await;
        let state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        let current = |id: &proto::QueryId| {
            state.queries.get(id).is_some_and(|query| revisions.get(id).is_some_and(|revision| Arc::ptr_eq(&query.revision, revision)))
        };
        items.retain_mut(|item| {
            let had_relevance = !item.predicate_relevance.is_empty();
            item.predicate_relevance.retain(|(id, _)| current(id));
            !item.predicate_relevance.is_empty()
                || state.entity_subscriptions.contains(&AbstractEntity::id(&item.entity))
                || (!had_relevance
                    && state
                        .queries
                        .iter()
                        .any(|(id, query)| current(id) && query.resultset.contains_key(&AbstractEntity::id(&item.entity))))
        });
        let broadcast = state.broadcast.clone();
        drop(state);
        if !items.is_empty() {
            broadcast.send(ReactorUpdate { items });
        }
    }

    fn extract_gap_data(&self, query: proto::QueryId, state: &QueryState<E>) -> Option<GapFill<E>> {
        let resultset = &state.resultset;
        if !resultset.is_gap_dirty() {
            return None;
        }
        let size = resultset.get_limit()?.checked_sub(resultset.len()).filter(|size| *size > 0)?;
        resultset.clear_gap_dirty();
        Some(GapFill {
            query,
            revision: state.revision.clone(),
            fetcher: state.gap_fetcher.clone(),
            selection: state.selection.clone().expect("gap before query installation"),
            last: resultset.last_entity(),
            size,
        })
    }
}

// Entity-specific methods for remote subscriptions
impl Subscription<crate::entity::Entity, ankurah_proto::Attested<ankurah_proto::Event>> {
    /// Register a query if absent, installing the caller's gap fetcher,
    /// or return the already-registered query's resultset. Idempotent
    /// per query id: for an existing query every argument except the id
    /// is ignored and the provided fetcher is dropped.
    pub fn register_or_get_query(
        &self,
        query_id: proto::QueryId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<crate::entity::Entity>>,
        source: Arc<dyn LocalEntitySource>,
    ) -> EntityResultSet<crate::entity::Entity> {
        let mut state = self.state.lock().unwrap();

        use std::collections::hash_map::Entry;
        match state.queries.entry(query_id) {
            Entry::Vacant(v) => {
                let resultset = EntityResultSet::empty();
                v.insert(QueryState {
                    selection: None,
                    gap_fetcher,
                    source,
                    paused: false,
                    resultset: resultset.clone(),
                    version: 0,
                    revision: Arc::new(()),
                });
                resultset
            }
            Entry::Occupied(o) => o.get().resultset.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{entity::Entity, reactor::fetch_gap::GapFetcher};
    use ankurah_signals::{broadcast::Broadcast, Signal};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    struct EmptySource;

    #[async_trait::async_trait]
    impl LocalEntitySource<Entity> for EmptySource {
        async fn fetch_entities_from_local(&self, _selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl GapFetcher<Entity> for EmptySource {
        async fn fetch_gap(
            &self,
            _selection: &ankql::ast::Selection<Resolved>,
            _last_entity: Option<&Entity>,
            _gap_size: usize,
        ) -> Result<Vec<Entity>, RetrievalError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn close_listeners_can_reenter_subscription_cleanup() {
        let subscription = Subscription::<Entity, ()>::new(
            Broadcast::new(),
            Arc::new(Mutex::new(crate::reactor::WatcherSet::new())),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        let resultset = EntityResultSet::empty();
        resultset.set_loaded(true);
        subscription.ensure_query_registered(proto::QueryId::new(), resultset.clone(), Arc::new(EmptySource), Arc::new(EmptySource));
        let notifications = Arc::new(AtomicUsize::new(0));
        let _listener = resultset.listen(Arc::new({
            let subscription = subscription.clone();
            let notifications = notifications.clone();
            move |_| {
                // Use try_lock so a regression fails instead of hanging the test runner.
                let state = subscription.state.try_lock().expect("close must release state before notifying listeners");
                assert!(state.queries.is_empty());
                drop(state);
                assert!(subscription.take_all_queries().is_empty());
                notifications.fetch_add(1, Ordering::SeqCst);
            }
        }));

        subscription.close();
        assert!(!resultset.is_loaded());
        assert!(notifications.load(Ordering::SeqCst) > 0);
    }
}
