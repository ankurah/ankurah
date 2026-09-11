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
    error::{RetrievalError, SubscriptionError},
    indexing::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder},
    reactor::{subscription::ReactorSubInner, subscription_state::Subscription, watcherset::WatcherOp},
    resultset::EntityResultSet,
    selection::filter::Filterable,
    value::{Value, ValueType},
};
use ankql::ast::Resolved;
use ankurah_proto::{self as proto};
use ankurah_signals::Wait;
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

/// Local entity retrieval for already policy-scoped selections; does not apply read policy.
#[async_trait::async_trait]
pub trait LocalEntitySource<E: AbstractEntity + Filterable + Send + 'static = Entity>: Send + Sync + 'static {
    async fn fetch_entities_from_local(
        &self,
        collection_id: &proto::CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<E>, RetrievalError>;
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
    /// Coordinates query installation, change dispatch, and clearing; never held across fetch I/O.
    notify_lock: tokio::sync::Mutex<()>,
    #[cfg(test)]
    publication_pause: Mutex<Option<(proto::CollectionId, tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
}
// don't require Clone SE or PA, because we have an Arc
impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Clone for Reactor<E, Ev> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Reactor<E, Ev> {
    #[cfg(test)]
    fn new_test() -> Self { Self::new(ankurah_signals::Mut::new(true)) }

    /// Clear subscriptions when the owner stops running, without retaining the reactor while waiting.
    pub(crate) fn new(run: impl Wait<bool> + Send + Sync + 'static) -> Self {
        let reactor = Self(Arc::new(ReactorInner {
            subscriptions: Mutex::new(HashMap::new()),
            watcher_set: Arc::new(Mutex::new(WatcherSet::new())),
            notify_lock: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            publication_pause: Mutex::new(None),
        }));
        let weak = Arc::downgrade(&reactor.0);
        crate::task::spawn(async move {
            run.wait_value(false).await;
            if let Some(inner) = weak.upgrade() {
                Self(inner).close().await;
            }
        });
        reactor
    }

    #[cfg(test)]
    pub(crate) fn pause_next_publication(
        &self,
        collection: proto::CollectionId,
    ) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        assert!(self.0.publication_pause.lock().unwrap().replace((collection, entered, resume)).is_none());
        (observed, release)
    }

    #[cfg(test)]
    async fn pause_publication_if_requested(&self, collection_id: &proto::CollectionId) {
        let pause = {
            let mut pause = self.0.publication_pause.lock().unwrap();
            if pause.as_ref().is_some_and(|(collection, _, _)| collection == collection_id) {
                pause.take()
            } else {
                None
            }
        };
        if let Some((_, entered, resume)) = pause {
            let _ = entered.send(());
            let _ = resume.await;
        }
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
        }
        watcher_set.remove_subscription_entity_watchers(sub_id);

        Ok(())
    }

    /// Clone the private subscription registered under `id`.
    fn subscription(&self, id: ReactorSubscriptionId) -> Option<Subscription<E, Ev>> {
        self.0.subscriptions.lock().unwrap().get(&id).cloned()
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
    /// Install or update a local query, notifying its owner before listeners.
    pub async fn upsert_query_and_notify<H: PreNotifyHook>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        node: &dyn LocalEntitySource<E>,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn GapFetcher<E>>,
        version: u32,
        pre_notify_hook: H,
    ) -> anyhow::Result<()> {
        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;
        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }

        let notify = self.0.notify_lock.lock().await;
        let subscription = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            subscriptions.get(&subscription_id).cloned().ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", subscription_id))?
        };

        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }

        let is_new = subscription.ensure_query_registered(query_id, collection_id.clone(), resultset.clone(), gap_fetcher);

        let mut reactor_update_items = Vec::new();
        subscription.update_query(
            query_id,
            collection_id.clone(),
            selection.clone(),
            included_entities,
            version,
            &mut reactor_update_items,
        )?;

        drop(notify);
        subscription.fill_gaps_for_query(query_id, &mut reactor_update_items).await;
        let _notify = self.0.notify_lock.lock().await;
        if !pre_notify_hook.is_current(version) {
            return Ok(());
        }

        resultset.set_loaded(true);
        pre_notify_hook.pre_notify(version);
        #[cfg(test)]
        self.pause_publication_if_requested(&collection_id).await;
        if is_new || !reactor_update_items.is_empty() {
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

        let evaluations = {
            let subscriptions = self.0.subscriptions.lock().unwrap();
            candidates_by_sub
                .into_iter()
                .filter_map(|(sub_id, candidates)| subscriptions.get(&sub_id).map(|subscription| (subscription.clone(), candidates)))
                .collect::<Vec<_>>()
        };

        let all_watcher_changes: Vec<_> =
            evaluations.into_iter().flat_map(|(subscription, candidates)| subscription.evaluate_changes(candidates)).collect();

        let mut watcher_set = self.0.watcher_set.lock().unwrap();
        for change in all_watcher_changes {
            watcher_set.apply_watcher_change(change);
        }
    }

    /// Clear current subscription results and watchers when the owner stops running.
    pub(crate) async fn close(&self) {
        let _notify = self.0.notify_lock.lock().await;
        {
            let mut watcher_set = self.0.watcher_set.lock().unwrap();
            watcher_set.clear();
        }

        let subscriptions: Vec<_> = self.0.subscriptions.lock().unwrap().values().cloned().collect();
        for subscription in subscriptions {
            subscription.close();
        }
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
    #[tokio::test]
    async fn node_halt_does_not_wait_for_result_clearing() {
        use crate::{
            node::Node,
            policy::{PermissiveAgent, DEFAULT_CONTEXT},
            test_utils::TestStorage,
        };
        let node = Node::new_durable(std::sync::Arc::new(TestStorage::default()), PermissiveAgent::new());
        node.system.create().await.unwrap();
        let query = node.context(DEFAULT_CONTEXT).unwrap().query_wait::<crate::schema::catalog::SysModelRowView>("true").await.unwrap();
        assert!(query.loaded());

        let notify = node.reactor.0.notify_lock.lock().await;
        node.system.halt(crate::error::NodeHaltReason::SystemLoad("fixture".into()));
        assert!(node.state().value().halt_reason().is_some());
        assert!(node.check_ready().is_err());
        assert!(query.loaded(), "clearing must wait for the publication lock");
        drop(notify);

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while query.loaded() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

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
    impl LocalEntitySource<TestEntity> for MockNode {
        async fn fetch_entities_from_local(
            &self,
            _collection_id: &proto::CollectionId,
            _selection: &ankql::ast::Selection<Resolved>,
        ) -> Result<Vec<TestEntity>, crate::error::RetrievalError> {
            Ok(self.entities.clone())
        }
    }

    #[tokio::test]
    async fn test_initial_query_notification() {
        let reactor = Reactor::<TestEntity, TestEvent>::new_test();

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
            .upsert_query_and_notify(rsub.id(), query_id, collection_id, selection, &mock_node, resultset, mock_gap_fetcher, 1, ())
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
    }

    #[tokio::test]
    async fn gap_fetch_can_reenter_the_reactor() {
        let reactor = Reactor::<TestEntity, TestEvent>::new_test();
        let subscription = reactor.subscribe();
        let query_id = QueryId::new();
        let collection = CollectionId::fixed_name("album");
        let selected = TestEntity::new("Selected", "pending");
        let replacement = TestEntity::new("Replacement", "pending");
        let resultset = EntityResultSet::empty();
        let node = MockNode { entities: vec![selected.clone()] };
        let gap_fetcher = Arc::new(ReentrantGapFetcher { reactor: reactor.clone(), entities: vec![replacement.clone()] });

        reactor
            .upsert_query_and_notify(
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

    #[test]
    fn change_listener_can_create_and_drop_a_subscription() {
        let (finished, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let reactor = Reactor::<TestEntity, TestEvent>::new_test();
                let subscription = reactor.subscribe();
                let entity = TestEntity::new("Album", "pending");
                reactor
                    .upsert_query_and_notify(
                        subscription.id(),
                        QueryId::new(),
                        entity.collection.clone(),
                        sel("status = 'pending'"),
                        &MockNode { entities: vec![entity.clone()] },
                        EntityResultSet::empty(),
                        Arc::new(MockGapFetcher::new()),
                        1,
                        (),
                    )
                    .await
                    .unwrap();
                let callback_reactor = reactor.clone();
                let _listener = subscription.subscribe(move |_update: ReactorUpdate<TestEntity, TestEvent>| {
                    drop(callback_reactor.subscribe());
                    finished.send(()).unwrap();
                });
                reactor.notify_change(vec![TestChange { entity, events: vec![] }]).await;
            });
        });
        received.recv_timeout(std::time::Duration::from_secs(2)).expect("query callbacks must not hold the subscriptions mutex");
    }

    #[tokio::test]
    async fn local_upsert_preserves_results_and_reports_membership_changes() {
        let reactor = Reactor::<TestEntity, TestEvent>::new_test();
        let subscription = reactor.subscribe();
        let query = QueryId::new();
        let pending = TestEntity::new("Pending", "pending");
        let done = TestEntity::new("Done", "done");
        let resultset = EntityResultSet::empty();
        let node = MockNode { entities: vec![pending.clone(), done.clone()] };
        let (listener, changes) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
        let _guard = subscription.subscribe(listener);
        let install = |selection, version| {
            reactor.upsert_query_and_notify(
                subscription.id(),
                query,
                pending.collection.clone(),
                selection,
                &node,
                resultset.clone(),
                Arc::new(MockGapFetcher::new()),
                version,
                (),
            )
        };

        install(sel("status = 'pending'"), 1).await.unwrap();
        assert!(resultset.contains_key(&pending.id));
        assert_eq!(changes().len(), 1);
        install(sel("status = 'pending'"), 1).await.unwrap();
        assert!(changes().is_empty(), "re-activation must not reset an unchanged baseline");

        install(sel("status = 'done'"), 2).await.unwrap();
        assert!(!resultset.contains_key(&pending.id));
        assert!(resultset.contains_key(&done.id));
        let updates = changes();
        assert_eq!(updates.len(), 1);
        assert!(updates[0]
            .items
            .iter()
            .any(|item| { item.entity.id == pending.id && item.predicate_relevance == vec![(query, MembershipChange::Remove)] }));
        assert!(updates[0]
            .items
            .iter()
            .any(|item| { item.entity.id == done.id && item.predicate_relevance == vec![(query, MembershipChange::Initial)] }));

        reactor.close().await;
        assert_eq!(resultset.len(), 0);
        assert!(!resultset.is_loaded());
        changes();
        install(sel("status = 'done'"), 3).await.unwrap();
        assert_eq!(reactor.subscription(subscription.id()).unwrap().queries_len(), 1);
        assert!(resultset.contains_key(&done.id));
        assert_eq!(changes().len(), 1);
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
            let reactor = Reactor::<TestEntity, TestEvent>::new_test();
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
