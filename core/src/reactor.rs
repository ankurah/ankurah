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
    reactor::{
        subscription::ReactorSubInner,
        subscription_state::{Subscription, UpdateItemAccumulator},
        watcherset::WatcherOp,
    },
    resultset::EntityResultSet,
    selection::filter::Filterable,
    value::{Value, ValueType},
};
use ankql::ast::Resolved;
use ankurah_proto::{self as proto};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

/// Trait for entities that can be used in reactor notifications
pub trait AbstractEntity: Clone + std::fmt::Debug {
    fn collection(&self) -> proto::CollectionId;
    fn id(&self) -> &proto::EntityId;
    fn value(&self, property: &ankql::ast::PropertyId) -> Option<Value>;
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
    fn is_current(&self, _version: u32) -> bool { true }
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
    /// Serializes reactor mutations and system reset.
    notify_lock: Arc<tokio::sync::Mutex<()>>,
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
            notify_lock: Arc::new(tokio::sync::Mutex::new(())),
        }))
    }

    /// Create a new subscription container
    pub fn subscribe(&self) -> ReactorSubscription<E, Ev> {
        let broadcast = ankurah_signals::broadcast::Broadcast::new();
        let subscription = Subscription::new(broadcast.clone(), self.0.watcher_set.clone(), self.0.notify_lock.clone());
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

    /// Clone the private subscription registered under `id`.
    fn subscription(&self, id: ReactorSubscriptionId) -> Option<Subscription<E, Ev>> {
        self.0.subscriptions.lock().unwrap().get(&id).cloned()
    }

    pub(crate) fn contains_query(&self, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId) -> bool {
        self.subscription(subscription_id).is_some_and(|subscription| subscription.contains_query(query_id))
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
    order_by: &[ankql::ast::OrderByItem<Resolved>],
    resultset: &EntityResultSet<E>,
) -> anyhow::Result<KeySpec<ankql::ast::PropertyId>> {
    let mut keyparts = Vec::new();

    let read = resultset.read();
    for item in order_by {
        // A resolved sort key names one property by its durable identity,
        // which is what the reactor's in-memory ordering keys on.
        let key = item.path.property_id();

        // Infer type from first non-null value in resultset entities
        let value_type = read.iter_entities().find_map(|(_, e)| e.value(&key).map(|v| ValueType::of(&v))).unwrap_or(ValueType::String); // TODO: Get type from system catalog instead of defaulting to String

        let direction: IndexDirection = match item.direction {
            ankql::ast::OrderDirection::Asc => IndexDirection::Asc,
            ankql::ast::OrderDirection::Desc => IndexDirection::Desc,
        };

        keyparts.push(IndexKeyPart { key, sub_path: None, direction, value_type, nulls: Some(NullsOrder::Last), collation: None });
    }

    Ok(KeySpec { keyparts })
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Reactor<E, Ev> {
    /// Add and initialize a local query, notifying its owner before listeners.
    pub async fn add_query_and_notify<H: PreNotifyHook>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        node: &dyn crate::node::TNodeErased<E>,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn GapFetcher<E>>,
        version: u32,
        pre_notify_hook: H,
    ) -> anyhow::Result<()> {
        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;
        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }

        let notify = self.0.notify_lock.clone().lock_owned().await;
        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned().ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?
        };

        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }

        subscription.register_query(query_id, collection_id.clone(), resultset.clone(), gap_fetcher)?;

        let mut reactor_update_items = Vec::new();
        subscription.update_query(
            query_id,
            collection_id.clone(),
            selection.clone(),
            included_entities,
            version,
            &mut reactor_update_items,
        )?;

        let gap_fill = subscription.take_gap_fill(query_id)?;
        drop(notify);
        let (_notify, gap_entities) = subscription.finish_gap_fill(gap_fill).await;
        let Some(gap_entities) = gap_entities else {
            return Ok(());
        };
        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }
        for entity in gap_entities {
            reactor_update_items.push_initial(&entity, query_id);
        }

        resultset.set_loaded(true);
        pre_notify_hook.pre_notify(version);
        subscription.send_update(reactor_update_items);

        Ok(())
    }

    /// Replace a local query's selection and notify its owner and listeners.
    pub async fn update_query_and_notify<H: PreNotifyHook>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        node: &dyn crate::node::TNodeErased<E>,
        version: u32,
        pre_notify_hook: H,
    ) -> anyhow::Result<()> {
        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;
        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }

        let notify = self.0.notify_lock.clone().lock_owned().await;
        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned().ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?
        };

        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }

        let mut reactor_update_items = Vec::new();
        subscription.update_query(
            query_id,
            collection_id.clone(),
            selection.clone(),
            included_entities,
            version,
            &mut reactor_update_items,
        )?;

        let gap_fill = subscription.take_gap_fill(query_id)?;
        drop(notify);
        let (_notify, gap_entities) = subscription.finish_gap_fill(gap_fill).await;
        let Some(gap_entities) = gap_entities else {
            return Ok(());
        };
        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }
        for entity in gap_entities {
            reactor_update_items.push_initial(&entity, query_id);
        }

        pre_notify_hook.pre_notify(version);
        if !reactor_update_items.is_empty() {
            subscription.send_update(reactor_update_items);
        }

        Ok(())
    }

    /// Notify subscriptions about an entity change
    pub async fn notify_change<C: ChangeNotification<Entity = E, Event = Ev>>(&self, changes: Vec<C>) {
        let _notify_guard = self.0.notify_lock.lock().await;
        let changes: Arc<Vec<C>> = Arc::from(changes);

        tracing::debug!("Reactor.notify_change({} changes)", changes.len());

        // Stable subscription order keeps seeded simulations reproducible.
        let mut candidates_by_sub: BTreeMap<ReactorSubscriptionId, CandidateChanges<C>> = BTreeMap::new();
        {
            let watcher_set = self.0.watcher_set.lock().unwrap();
            for (offset, change) in changes.iter().enumerate() {
                watcher_set.accumulate_interested_watchers(change.entity(), offset, &changes, &mut candidates_by_sub);
            }
        }

        let all_watcher_changes = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            candidates_by_sub
                .into_iter()
                .filter_map(|(sub_id, candidates)| {
                    subscriptions.get(&sub_id).map(|subscription| subscription.clone().evaluate_changes(candidates))
                })
                .flatten()
                .collect::<Vec<_>>()
        };

        let mut watcher_set = self.0.watcher_set.lock().unwrap();
        for change in all_watcher_changes {
            watcher_set.apply_watcher_change(change);
        }
    }

    /// Clear every subscription and hold updates until the caller finishes
    /// resetting the underlying storage.
    pub async fn begin_system_reset(&self) -> tokio::sync::OwnedMutexGuard<()> {
        let notify = self.0.notify_lock.clone().lock_owned().await;
        {
            let mut watcher_set = self.0.watcher_set.lock().unwrap();
            watcher_set.clear();
        }

        let subscriptions: Vec<_> = self.0.subscriptions.lock().unwrap().values().cloned().collect();
        for subscription in subscriptions {
            subscription.system_reset();
        }
        notify
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

    /// A deterministic durable identity for a fixture field name.
    fn prop(name: &str) -> ankql::ast::PropertyId {
        let mut bytes = [0u8; 32];
        let n = name.as_bytes();
        let len = n.len().min(32);
        bytes[..len].copy_from_slice(&n[..len]);
        ankql::ast::PropertyId::EntityId(proto::EntityId::from_bytes(bytes))
    }

    /// Bind a parsed selection's names to the fixture identities.
    fn resolve_fixture(selection: ankql::ast::Selection<ankql::ast::Parsed>) -> ankql::ast::Selection<Resolved> {
        use crate::schema::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty};
        struct FixtureResolver;
        impl ModelResolver for FixtureResolver {
            fn resolve_property(&self, _model: &proto::ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
                Ok(Some(ResolvedProperty { id: prop(name), value_type: crate::value::ValueType::String }))
            }
        }
        let model = proto::ModelId::EntityId(proto::EntityId::from_bytes([0x77; 32]));
        resolve_selection(&model, &FixtureResolver, selection).unwrap()
    }

    /// Parse a fixture query and bind it, in one step.
    fn sel(query: &str) -> ankql::ast::Selection<Resolved> { resolve_fixture(ankql::parser::parse_selection(query).unwrap()) }

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
        state: Arc<Mutex<HashMap<ankql::ast::PropertyId, String>>>,
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
                id: proto::EntityId::random(),
                collection: proto::CollectionId::fixed_name("album"),
                state: Arc::new(Mutex::new(HashMap::from([(prop("name"), name.to_string()), (prop("status"), status.to_string())]))),
            }
        }
    }
    impl Filterable for TestEntity {
        fn collection(&self) -> &str { self.collection.as_str() }
        fn value(&self, property: &ankql::ast::PropertyId) -> Option<crate::value::Value> {
            self.state.lock().unwrap().get(property).cloned().map(crate::value::Value::String)
        }
    }
    impl AbstractEntity for TestEntity {
        fn collection(&self) -> proto::CollectionId { self.collection.clone() }
        fn id(&self) -> &proto::EntityId { &self.id }
        fn value(&self, property: &ankql::ast::PropertyId) -> Option<crate::value::Value> {
            self.state.lock().unwrap().get(property).cloned().map(crate::value::Value::String)
        }
    }

    /// Mock gap fetcher for testing
    struct MockGapFetcher {
        entities: Vec<TestEntity>,
    }

    impl MockGapFetcher {
        fn new() -> Self { Self { entities: Vec::new() } }
    }

    #[async_trait::async_trait]
    impl GapFetcher<TestEntity> for MockGapFetcher {
        async fn fetch_gap(
            &self,
            _collection_id: &proto::CollectionId,
            _selection: &ankql::ast::Selection<Resolved>,
            _last_entity: Option<&TestEntity>,
            _gap_size: usize,
        ) -> Result<Vec<TestEntity>, crate::error::RetrievalError> {
            // For testing, just return the pre-configured entities
            Ok(self.entities.clone())
        }
    }

    struct ReentrantGapFetcher {
        reactor: Reactor<TestEntity, TestEvent>,
        entities: Vec<TestEntity>,
    }

    #[async_trait::async_trait]
    impl GapFetcher<TestEntity> for ReentrantGapFetcher {
        async fn fetch_gap(
            &self,
            _collection_id: &proto::CollectionId,
            _selection: &ankql::ast::Selection<Resolved>,
            _last_entity: Option<&TestEntity>,
            _gap_size: usize,
        ) -> Result<Vec<TestEntity>, crate::error::RetrievalError> {
            self.reactor.notify_change(Vec::<TestChange>::new()).await;
            Ok(self.entities.clone())
        }
    }

    /// Mock node for testing
    struct MockNode {
        entities: Vec<TestEntity>,
    }

    #[async_trait::async_trait]
    impl crate::node::TNodeErased<TestEntity> for MockNode {
        async fn fetch_entities_from_local(
            &self,
            _collection_id: &proto::CollectionId,
            _selection: &ankql::ast::Selection<Resolved>,
        ) -> Result<Vec<TestEntity>, crate::error::RetrievalError> {
            Ok(self.entities.clone())
        }
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
        let selection: ankql::ast::Selection<Resolved> = sel("status = 'pending'");
        let entity1 = TestEntity::new("Test Album", "pending");
        let resultset: EntityResultSet<TestEntity> = EntityResultSet::empty();
        let mock_gap_fetcher = Arc::new(MockGapFetcher::new());
        let mock_node = MockNode { entities: vec![entity1.clone()] };

        // Add query using the reactor - this should send Initial notification
        reactor
            .add_query_and_notify(rsub.id(), query_id, collection_id, selection, &mock_node, resultset, mock_gap_fetcher, 1, ())
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

    #[tokio::test]
    async fn gap_fetch_can_reenter_the_reactor() {
        let reactor = Reactor::<TestEntity, TestEvent>::new();
        let subscription = reactor.subscribe();
        let query_id = QueryId::new();
        let collection = CollectionId::fixed_name("album");
        let selected = TestEntity::new("Selected", "pending");
        let replacement = TestEntity::new("Replacement", "pending");
        let resultset = EntityResultSet::empty();
        let node = MockNode { entities: vec![selected.clone()] };
        let gap_fetcher = Arc::new(ReentrantGapFetcher { reactor: reactor.clone(), entities: vec![replacement.clone()] });

        reactor
            .add_query_and_notify(
                subscription.id(),
                query_id,
                collection,
                sel("status = 'pending' LIMIT 1"),
                &node,
                resultset.clone(),
                gap_fetcher,
                1,
                (),
            )
            .await
            .unwrap();

        selected.state.lock().unwrap().insert(prop("status"), "done".to_owned());
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reactor.notify_change(vec![TestChange { entity: selected, events: vec![] }]),
        )
        .await
        .expect("gap fetching must not hold the reactor gate");

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !resultset.contains_key(&replacement.id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the reentrant gap fill should complete");
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

    /// A ChangeNotification over the test entity/event types, so notify_change can be driven directly.
    #[derive(Debug, Clone)]
    struct TestChange {
        entity: TestEntity,
        events: Vec<TestEvent>,
    }
    impl std::fmt::Display for TestChange {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "TestChange({})", self.entity.id) }
    }
    impl ChangeNotification for TestChange {
        type Entity = TestEntity;
        type Event = TestEvent;
        fn into_parts(self) -> (Self::Entity, Vec<Self::Event>) { (self.entity, self.events) }
        fn entity(&self) -> &Self::Entity { &self.entity }
        fn events(&self) -> &[Self::Event] { &self.events }
    }

    /// notify_change must emit across subscriptions in a stable, id-sorted order.
    ///
    /// Any emission order is semantically legal, but the C1 simulation audit requires the same
    /// inputs to reproduce an identical trace. candidates_by_sub is a BTreeMap keyed on
    /// ReactorSubscriptionId, so the order is a strict refinement (sorted by subscription id) and
    /// is identical across runs. Every subscription entity-subscribes to the same entity id, so a
    /// single change fans out to all of them and their relative emission order is observable.
    #[tokio::test]
    async fn test_notify_change_emits_in_stable_subscription_order() {
        // Shared observer: every subscription pushes its id here as its update is emitted.
        // Because Broadcast::send runs listeners synchronously and evaluate_changes emits before
        // returning (no gap fill for entity subscriptions), the push order equals the
        // candidates_by_sub iteration order.
        async fn run_once(shared_entity: &TestEntity) -> Vec<ReactorSubscriptionId> {
            let reactor = Reactor::<TestEntity, TestEvent>::new();
            let emission_order = Arc::new(Mutex::new(Vec::<ReactorSubscriptionId>::new()));

            // Several subscriptions, all watching the same entity by id. Both the ReactorSubscription
            // handles and the listen guards must stay alive for the whole run: dropping a
            // ReactorSubscription unsubscribes it, and dropping a guard detaches its listener.
            let mut subs = Vec::new();
            let mut guards = Vec::new();
            for _ in 0..5 {
                let rsub = reactor.subscribe();
                let sub_id = rsub.id();
                let order = emission_order.clone();
                let guard = rsub.subscribe(Box::new(move |_update: ReactorUpdate<TestEntity, TestEvent>| {
                    order.lock().unwrap().push(sub_id);
                }) as Box<dyn Fn(ReactorUpdate<TestEntity, TestEvent>) + Send + Sync>);
                reactor.add_entity_subscriptions(sub_id, [shared_entity.id]);
                guards.push(guard);
                subs.push(rsub);
            }

            // A single change on the shared entity fans out to every subscription.
            let change = TestChange { entity: shared_entity.clone(), events: vec![] };
            reactor.notify_change(vec![change]).await;

            let observed = emission_order.lock().unwrap().clone();
            drop(guards);
            drop(subs);
            observed
        }

        let shared_entity = TestEntity::new("Album", "pending");
        let order1 = run_once(&shared_entity).await;
        let order2 = run_once(&shared_entity).await;

        assert_eq!(order1.len(), 5, "all subscriptions should be notified");

        // Within a run, the order is the ascending subscription-id sort (BTreeMap refinement),
        // not a HashMap-arbitrary order.
        let mut sorted1 = order1.clone();
        sorted1.sort();
        assert_eq!(order1, sorted1, "emission order must be sorted by subscription id");
        let mut sorted2 = order2.clone();
        sorted2.sort();
        assert_eq!(order2, sorted2, "emission order must be sorted by subscription id on the second run too");
    }
}
