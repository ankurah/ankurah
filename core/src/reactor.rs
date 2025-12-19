mod candidate_changes;
mod comparison_index;
pub mod fetch_gap;
mod subscription;
mod subscription_state;
mod update;
mod watcherset;

pub(crate) use self::{
    candidate_changes::CandidateChanges,
    subscription::{ReactorSubscription, ReactorSubscriptionId},
    update::{MembershipChange, ReactorUpdate, ReactorUpdateItem},
    watcherset::{WatcherChange, WatcherSet},
};

// Re-export fetch_gap items
pub(crate) use self::fetch_gap::GapFetcher;

use crate::{
    entity::Entity,
    error::SubscriptionError,
    indexing::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder},
    reactor::{subscription::ReactorSubInner, subscription_state::Subscription, watcherset::WatcherOp},
    resultset::EntityResultSet,
    selection::filter::Filterable,
    value::{Value, ValueType},
};
use ankurah_proto::{self as proto};
use futures::future::join_all;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Trait for entities that can be used in reactor notifications
pub trait AbstractEntity: Clone + std::fmt::Debug {
    fn collection(&self) -> proto::CollectionId;
    fn id(&self) -> &proto::EntityId;
    fn value(&self, field: &str) -> Option<Value>;
}

/// Trait for types that can be used in notify_change
pub trait ChangeNotification: std::fmt::Debug + std::fmt::Display {
    type Entity: AbstractEntity;
    type Event: Clone + std::fmt::Debug;

    fn into_parts(self) -> (Self::Entity, Vec<Self::Event>);
    fn entity(&self) -> &Self::Entity;
    fn events(&self) -> &[Self::Event];
}

/// Hook trait for performing actions before notification is sent
pub trait PreNotifyHook {
    fn pre_notify(&self, version: u32);
}

/// No-op implementation for unit type
impl PreNotifyHook for () {
    fn pre_notify(&self, _version: u32) {}
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of entities
pub struct Reactor<
    E: AbstractEntity + Filterable + Send + 'static = Entity,
    Ev: Clone + Send + 'static = ankurah_proto::Attested<ankurah_proto::Event>,
>(Arc<ReactorInner<E, Ev>>);

struct ReactorInner<E: AbstractEntity + Filterable, Ev> {
    subscriptions: std::sync::Mutex<HashMap<ReactorSubscriptionId, Subscription<E, Ev>>>,
    // Shared with all subscriptions to allow them to manage their own watchers
    watcher_set: Arc<std::sync::Mutex<WatcherSet>>,
    /// Serializes notify_change invocations to ensure consistent watcher state
    notify_lock: tokio::sync::Mutex<()>,
}
// don't require Clone SE or PA, because we have an Arc
impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Clone for Reactor<E, Ev> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Default for Reactor<E, Ev> {
    fn default() -> Self { Self::new() }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Reactor<E, Ev> {
    pub fn new() -> Self {
        Self(Arc::new(ReactorInner {
            subscriptions: Mutex::new(HashMap::new()),
            watcher_set: Arc::new(Mutex::new(WatcherSet::new())),
            notify_lock: tokio::sync::Mutex::new(()),
        }))
    }

    /// Create a new subscription container
    pub fn subscribe(&self) -> ReactorSubscription<E, Ev> {
        let broadcast = ankurah_signals::broadcast::Broadcast::new();
        let subscription = Subscription::new(broadcast.clone(), self.0.watcher_set.clone());
        let subscription_id = subscription.id();
        self.0.subscriptions.lock().unwrap().insert(subscription_id, subscription);
        ReactorSubscription(Arc::new(ReactorSubInner { subscription_id, reactor: self.clone(), broadcast }))
    }

    /// Remove a subscription and all its predicates
    pub(crate) fn unsubscribe(&self, sub_id: ReactorSubscriptionId) -> Result<(), SubscriptionError> {
        let subscription = {
            let mut subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.remove(&sub_id).ok_or(SubscriptionError::SubscriptionNotFound)?
        };

        // Get all queries for cleanup
        let queries = subscription.take_all_queries();

        // Remove all predicates from watchers
        let mut watcher_set = self.0.watcher_set.lock().unwrap();
        for (query_id, query_state) in queries {
            // Remove from index watcher (only if selection was set)
            if let Some(selection) = &query_state.selection {
                watcher_set.recurse_predicate_watchers(
                    &query_state.collection_id,
                    &selection.predicate,
                    (sub_id, query_id),
                    WatcherOp::Remove,
                );
            }

            // Remove from entity watchers using predicate's matching entities
            let entity_ids: Vec<_> = query_state.resultset.keys().collect();
            watcher_set.remove_entity_subscriptions(sub_id, entity_ids);
        }

        Ok(())
    }

    /// Remove a predicate from a subscription
    pub fn remove_query(&self, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId) -> Result<(), SubscriptionError> {
        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned().ok_or(SubscriptionError::SubscriptionNotFound)?
        };

        // Remove the query from the subscription
        let query_state = subscription.remove_query(query_id).ok_or(SubscriptionError::PredicateNotFound)?;

        // Remove from watchers (only if selection was set)
        if let Some(selection) = &query_state.selection {
            let mut watcher_set = self.0.watcher_set.lock().unwrap();
            let watcher_id = (subscription_id, query_id);
            watcher_set.recurse_predicate_watchers(&query_state.collection_id, &selection.predicate, watcher_id, WatcherOp::Remove);
        }

        Ok(())
    }

    /// Add entity subscriptions to a subscription
    pub fn add_entity_subscriptions(&self, subscription_id: ReactorSubscriptionId, entity_ids: impl IntoIterator<Item = proto::EntityId>) {
        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned()
        };

        if let Some(subscription) = subscription {
            let mut watcher_set = self.0.watcher_set.lock().unwrap();
            for entity_id in entity_ids {
                subscription.add_entity_subscription(entity_id);
                watcher_set.add_entity_subscription(subscription_id, entity_id);
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
        let mut watcher_set = self.0.watcher_set.lock().unwrap();

        if let Some(subscription) = subscriptions.get_mut(&subscription_id) {
            for entity_id in entity_ids {
                subscription.remove_entity_subscription(entity_id);

                // TODO: Check if any predicates match this entity before removing from entity_watchers
                // For now, only remove if no predicates match
                let should_remove = !subscription.any_query_matches(&entity_id);

                if should_remove {
                    watcher_set.remove_entity_subscription(subscription_id, entity_id);
                }
            }
        }
    }
}

/// Build KeySpec from Selection's ORDER BY clause with type inference from sample entities
pub(crate) fn build_key_spec_from_selection<E: AbstractEntity>(
    order_by: &[ankql::ast::OrderByItem],
    resultset: &EntityResultSet<E>,
) -> anyhow::Result<KeySpec> {
    let mut keyparts = Vec::new();

    let read = resultset.read();
    for item in order_by {
        // Use the property name from the path (currently only simple paths are supported in ORDER BY)
        let column = item.path.property().to_string();

        // Infer type from first non-null value in resultset entities
        let value_type = read.iter_entities().find_map(|(_, e)| e.value(&column).map(|v| ValueType::of(&v))).unwrap_or(ValueType::String); // TODO: Get type from system catalog instead of defaulting to String

        let direction: IndexDirection = match item.direction {
            ankql::ast::OrderDirection::Asc => IndexDirection::Asc,
            ankql::ast::OrderDirection::Desc => IndexDirection::Desc,
        };

        keyparts.push(IndexKeyPart { column, sub_path: None, direction, value_type, nulls: Some(NullsOrder::Last), collation: None });
    }

    Ok(KeySpec { keyparts })
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Reactor<E, Ev> {
    /// Add a new predicate to a subscription (initial subscription only)
    /// Fails if query_id already exists - use update_query for updates
    ///
    /// Add a query and send initialization notification (for local subscriptions)
    /// Collects ReactorUpdateItems and sends them
    /// pre_notify_hook is called before sending notification (e.g., to mark LiveQuery initialized)
    pub async fn add_query_and_notify<H: PreNotifyHook>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        node: &dyn crate::node::TNodeErased<E>,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn GapFetcher<E>>,
        pre_notify_hook: H,
    ) -> anyhow::Result<()> {
        // Get subscription reference
        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned().ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?
        };

        // Fetch initial entities from local storage (do this first to avoid holding locks across await)
        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;

        // Register empty query state with subscription (will be populated by update_query)
        subscription.register_query(query_id, collection_id.clone(), resultset.clone(), gap_fetcher)?;

        // Populate the resultset and collect ReactorUpdateItems
        // update_query now handles all watcher management internally (predicate + entity)
        let mut reactor_update_items = Vec::new();
        let _newly_added = subscription.update_query(
            query_id,
            collection_id.clone(),
            selection.clone(),
            included_entities,
            1, // version 1 for initial add
            &mut reactor_update_items,
        )?;

        // Fill gaps if needed for this specific query
        // FIXME: Open question — is there a window where entity edits land between the local fetch
        // above and downstream notification handling (reactor.notify_change + evaluate_changes)
        // such that we need this gap fill to catch the missed edit-driven gap?
        subscription.fill_gaps_for_query(query_id, &mut reactor_update_items).await;

        // Mark as loaded
        resultset.set_loaded(true);

        // Call pre-notify hook (e.g., mark LiveQuery as initialized) with version 1
        pre_notify_hook.pre_notify(1);

        // Send the notification with collected items. We always notify because we're initializing the query.
        subscription.send_update(reactor_update_items);

        Ok(())
    }

    /// Update an existing predicate (v>0) and send notifications
    /// Does diffing against the current resultset
    /// Used by local LiveQuery updates
    /// pre_notify_hook is called before sending notification (e.g., to mark LiveQuery initialized)
    pub async fn update_query_and_notify<H: PreNotifyHook>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        node: &dyn crate::node::TNodeErased<E>,
        version: u32,
        pre_notify_hook: H,
    ) -> anyhow::Result<()> {
        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;

        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned().ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?
        };

        let mut reactor_update_items = Vec::new();
        // Update query - watcher management is handled internally
        let _newly_added = subscription.update_query(
            query_id,
            collection_id.clone(),
            selection.clone(),
            included_entities,
            version,
            &mut reactor_update_items,
        )?;

        // Fill gaps if needed for this specific query
        // FIXME: Same open question as add_query_and_notify — do edits that slip in between the
        // storage fetch and subsequent notify_change path require this gap fill to keep limits tight?
        subscription.fill_gaps_for_query(query_id, &mut reactor_update_items).await;

        // Call pre-notify hook (e.g., mark LiveQuery as initialized)
        pre_notify_hook.pre_notify(version);

        // Send reactor update
        if !reactor_update_items.is_empty() {
            subscription.send_update(reactor_update_items);
        }

        Ok(())
    }

    /// Notify subscriptions about an entity change
    pub async fn notify_change<C: ChangeNotification<Entity = E, Event = Ev> + Clone>(&self, changes: Vec<C>) {
        // Serialize notify_change invocations
        let _notify_guard = self.0.notify_lock.lock().await;

        // Wrap changes in Arc for sharing across subscriptions
        let changes: Arc<Vec<C>> = Arc::from(changes);

        tracing::debug!("Reactor.notify_change({} changes)", changes.len());

        // Build per-subscription candidate accumulators (first lock of watcher_set)
        let mut candidates_by_sub: HashMap<ReactorSubscriptionId, CandidateChanges<C>> = HashMap::new();
        {
            let watcher_set = self.0.watcher_set.lock().unwrap();
            for (offset, change) in changes.iter().enumerate() {
                watcher_set.accumulate_interested_watchers(change.entity(), offset, &changes, &mut candidates_by_sub);
            }
        }

        // Parallelize evaluate_changes calls across subscriptions
        // First, collect all the evaluation futures while holding the lock
        let evaluations = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            candidates_by_sub
                .into_iter()
                .filter_map(|(sub_id, candidates)| {
                    subscriptions.get(&sub_id).map(|subscription| subscription.clone().evaluate_changes(candidates))
                })
                .collect::<Vec<_>>()
        };

        // Now await all evaluations (lock is dropped)
        let all_watcher_changes: Vec<WatcherChange> = join_all(evaluations).await.into_iter().flatten().collect();

        // Apply all watcher changes to watcher_set (second lock of watcher_set)

        let mut watcher_set = self.0.watcher_set.lock().unwrap();
        for change in all_watcher_changes {
            watcher_set.apply_watcher_change(change);
        }
    }

    /// Notify all subscriptions that their entities have been removed but do not remove the subscriptions
    pub fn system_reset(&self) {
        // Clear entity watchers first - no entities are being watched after reset, because any previously existing entities "stopped existing"
        // as part of the system reset.
        {
            let mut watcher_set = self.0.watcher_set.lock().unwrap();
            watcher_set.clear_entity_watchers();
        }

        let subscriptions = self.0.subscriptions.lock().unwrap();
        for subscription in subscriptions.values() {
            subscription.system_reset();
        }
    }
}

// Entity-specific methods for remote subscriptions
impl Reactor<Entity, ankurah_proto::Attested<ankurah_proto::Event>> {
    /// Add or update a query for remote subscriptions (server-side)
    /// This method is idempotent - it works whether the query exists or not
    /// Constructs gap_fetcher internally using the provided Node and ContextData
    /// Returns all entities that currently match the selection (for delta generation)
    pub async fn upsert_query<SE, PA>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        node: &crate::node::Node<SE, PA>,
        cdata: &PA::ContextData,
        version: u32,
    ) -> anyhow::Result<Vec<Entity>>
    where
        SE: crate::storage::StorageEngine + Send + Sync + 'static,
        PA: crate::policy::PolicyAgent + Send + Sync + 'static,
    {
        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned().ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?
        };

        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;

        // Upsert query - register if new or get existing resultset
        // Gap fetcher is only created if query doesn't exist yet
        let resultset = subscription.upsert_query(query_id, collection_id.clone(), node, cdata);

        // Update query - watcher management is handled internally
        let mut all_entities =
            subscription.update_query(query_id, collection_id.clone(), selection.clone(), included_entities, version, &mut ())?;

        // Fill gaps if needed for this specific query (also registers entity watchers)
        // FIXME: Same follow-up — we should confirm whether edit-driven gaps can occur between the
        // storage fetch and notify_change handling, which would make this gap fill mandatory.
        subscription.fill_gaps_for_query_entities(query_id, &mut all_entities).await;

        resultset.set_loaded(true);

        // Return all entities (newly added + gap-filled)
        Ok(all_entities)
    }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> std::fmt::Debug for Reactor<E, Ev> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let watcher_set = self.0.watcher_set.lock().unwrap();
        let subscriptions = self.0.subscriptions.lock().unwrap();
        let (index_watchers, wildcard_watchers, entity_watchers) = watcher_set.debug_data();
        write!(
            f,
            "Reactor {{ subscriptions: {:?}, index_watchers: {:?}, wildcard_watchers: {:?}, entity_watchers: {:?} }}",
            subscriptions, index_watchers, wildcard_watchers, entity_watchers
        )
    }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> std::fmt::Debug for Subscription<E, Ev> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription {{ id: {:?}, queries: {} }}", self.id(), self.queries_len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::filter::Filterable;
    use ankurah_signals::Subscribe;
    use proto::{CollectionId, QueryId};
    use std::sync::Arc;

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
        fn value(&self, field: &str) -> Option<crate::value::Value> {
            self.state.lock().unwrap().get(field).cloned().map(crate::value::Value::String)
        }
    }
    impl AbstractEntity for TestEntity {
        fn collection(&self) -> proto::CollectionId { self.collection.clone() }
        fn id(&self) -> &proto::EntityId { &self.id }
        fn value(&self, field: &str) -> Option<crate::value::Value> {
            self.state.lock().unwrap().get(field).cloned().map(crate::value::Value::String)
        }
    }

    /// Mock gap fetcher for testing
    struct MockGapFetcher {
        entities: Vec<TestEntity>,
    }

    impl MockGapFetcher {
        fn new() -> Self { Self { entities: Vec::new() } }

        fn with_entities(entities: Vec<TestEntity>) -> Self { Self { entities } }
    }

    #[async_trait::async_trait]
    impl GapFetcher<TestEntity> for MockGapFetcher {
        async fn fetch_gap(
            &self,
            _collection_id: &proto::CollectionId,
            _selection: &ankql::ast::Selection,
            _last_entity: Option<&TestEntity>,
            _gap_size: usize,
        ) -> Result<Vec<TestEntity>, crate::error::RetrievalError> {
            // For testing, just return the pre-configured entities
            Ok(self.entities.clone())
        }
    }

    /// Mock node for testing
    struct MockNode {
        entities: Vec<TestEntity>,
    }

    #[async_trait::async_trait]
    impl crate::node::TNodeErased<TestEntity> for MockNode {
        fn unsubscribe_remote_predicate(&self, _query_id: proto::QueryId) {}
        fn update_remote_query(
            &self,
            _query_id: proto::QueryId,
            _selection: ankql::ast::Selection,
            _version: u32,
        ) -> Result<(), anyhow::Error> {
            Ok(())
        }
        async fn fetch_entities_from_local(
            &self,
            _collection_id: &proto::CollectionId,
            _selection: &ankql::ast::Selection,
        ) -> Result<Vec<TestEntity>, crate::error::RetrievalError> {
            Ok(self.entities.clone())
        }
        fn reactor(&self) -> &Reactor<TestEntity> { panic!("MockNode::reactor() should not be called in this test") }
        fn has_subscription_relay(&self) -> bool { false }
    }

    /// Test that once a predicate matches an entity, that entity continues to be watched
    /// by the ReactorSubscriptionId until the user explicitly unwatches it
    #[tokio::test]
    async fn test_entity_remains_watched_after_predicate_stops_matching() {
        let reactor = Reactor::<TestEntity, TestEvent>::new();

        // Set up a subscription with a predicate that matches status="pending"
        let rsub = reactor.subscribe();
        let (w, check) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
        let _guard = rsub.subscribe(w);

        let query_id = QueryId::new();
        let collection_id = CollectionId::fixed_name("album");
        let selection: ankql::ast::Selection = "status = 'pending'".try_into().unwrap();
        let entity1 = TestEntity::new("Test Album", "pending");
        let resultset: EntityResultSet<TestEntity> = EntityResultSet::empty();
        let mock_gap_fetcher = Arc::new(MockGapFetcher::new());
        let mock_node = MockNode { entities: vec![entity1.clone()] };

        // Add query using the reactor - this should send Initial notification
        reactor
            .add_query_and_notify(rsub.id(), query_id, collection_id, selection, &mock_node, resultset, mock_gap_fetcher, ())
            .await
            .unwrap();

        // something like this
        assert_eq!(
            check(),
            vec![ReactorUpdate {
                items: vec![ReactorUpdateItem {
                    entity: entity1.clone(),
                    events: vec![],
                    predicate_relevance: vec![(query_id, MembershipChange::Initial)],
                }],
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
