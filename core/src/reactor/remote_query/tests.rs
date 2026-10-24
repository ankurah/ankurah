use super::*;
use crate::{
    error::RetrievalError,
    reactor::{
        tests::{prop, sel, watcher, MockGapFetcher, MockNode, TestChange, TestEntity, TestEvent},
        MembershipChange, ReactorUpdate,
    },
};
use ankurah_signals::Subscribe;
use proto::QueryId;

/// The source of a query whose membership the stream `.1` decides; it admits the ids in `.0`.
struct RemoteSource(Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>, QueryOrigin);
fn origin(query: QueryId) -> QueryOrigin { QueryOrigin { query, version: 1, request: proto::RequestId::default() } }
#[async_trait::async_trait]
impl LocalEntitySource<TestEntity> for RemoteSource {
    fn query_origin(&self) -> Option<&QueryOrigin> { Some(&self.1) }
    fn admits_query_entity(&self, entity: &TestEntity) -> bool { self.0.read().unwrap().contains(&entity.id) }
    async fn fetch_entities_from_local(&self, _: &ankql::ast::Selection<Resolved>) -> Result<Vec<TestEntity>, RetrievalError> { Ok(vec![]) }
}
/// A change the stream `origin` applied.
struct OwnChange {
    change: TestChange,
    origin: QueryOrigin,
}
impl std::fmt::Display for OwnChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.change.fmt(f) }
}
impl std::fmt::Debug for OwnChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.change.fmt(f) }
}
impl ChangeNotification for OwnChange {
    type Entity = TestEntity;
    type Event = TestEvent;
    fn into_parts(self) -> (TestEntity, Vec<TestEvent>) { self.change.into_parts() }
    fn entity(&self) -> &TestEntity { self.change.entity() }
    fn events(&self) -> &[TestEvent] { self.change.events() }
    fn query_origin(&self) -> Option<&QueryOrigin> { Some(&self.origin) }
}

#[tokio::test]
async fn stream_admission_survives_late_data_and_readable_removals_still_notify() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let id = QueryId::new();
    let entity = TestEntity::new("formerly selected", "pending");
    let resultset = EntityResultSet::empty();
    // Start as a local query; remote installation must replace its source too.
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            id,
            sel("status = 'pending'"),
            Arc::new(MockNode { entities: vec![entity.clone()] }),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
        )
        .await
        .unwrap();
    let admitted = Arc::new(std::sync::RwLock::new([entity.id].into_iter().collect()));
    reactor
        .install_remote_query(
            subscription.id(),
            id,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted.clone(), origin(id))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![entity.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    let (watch, changes) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _listener = subscription.subscribe(watch);
    admitted.write().unwrap().clear();
    // The answer omits an offered row, which removes it. Later data from the stream neither
    // removes rows nor restores this one.
    reactor
        .install_remote_query(
            subscription.id(),
            id,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted.clone(), origin(id))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall {
                entities: vec![],
                membership: RemoteMembership {
                    offered: [entity.id].into_iter().collect(),
                    absent: HashSet::new(),
                    held: HashSet::new(),
                    known_heads: HashMap::new(),
                },
                changes: vec![],
                complete: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(resultset.len(), 0);
    let removal = changes();
    assert!(
        removal.iter().flat_map(|update| &update.items).any(|item| item.predicate_relevance.contains(&(id, MembershipChange::Remove))),
        "a removal the requester can read still notifies"
    );
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: entity.clone(), events: vec![] }, origin: origin(id) }]).await;
    assert_eq!(resultset.len(), 0, "a repair from the stream cannot restore a row the stream did not admit");
    assert!(changes().is_empty());
    // A local write or another retrieval still enters by the predicate alone.
    reactor.notify_change(vec![TestChange { entity: entity.clone(), events: vec![] }]).await;
    assert!(resultset.contains_key(&entity.id));
    let _ = changes();
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: entity.clone(), events: vec![] }, origin: origin(id) }]).await;
    assert!(resultset.contains_key(&entity.id), "a later repair from the stream cannot remove a row a local change added");
    assert!(changes().is_empty());
    let other = TestEntity::new("another query's row", "pending");
    reactor
        .notify_change(vec![OwnChange { change: TestChange { entity: other.clone(), events: vec![] }, origin: origin(QueryId::new()) }])
        .await;
    assert!(resultset.contains_key(&other.id), "another query's stream is not limited to the ids this stream admitted");
}

#[tokio::test]
async fn changes_from_a_replaced_stream_cannot_change_the_query_membership() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let resultset = EntityResultSet::empty();
    let old = origin(query);
    let current = QueryOrigin { request: proto::RequestId::new(), ..old.clone() };
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(Arc::new(std::sync::RwLock::new(HashSet::new())), current.clone())),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    let same_version = TestEntity::new("old same-version request", "pending");
    let old_version = TestEntity::new("old selection version", "pending");
    let own = TestEntity::new("not admitted by the current stream", "pending");
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: own.clone(), events: vec![] }, origin: current.clone() }]).await;
    assert!(!resultset.contains_key(&own.id));
    // An old stream's application can reach the notification lock after the stream was
    // replaced. Its data stays valid for other queries but cannot change this query's membership.
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: same_version.clone(), events: vec![] }, origin: old }]).await;
    assert!(!resultset.contains_key(&same_version.id));
    reactor
        .notify_change(vec![OwnChange {
            change: TestChange { entity: old_version.clone(), events: vec![] },
            origin: QueryOrigin { version: 0, ..current },
        }])
        .await;
    assert!(!resultset.contains_key(&old_version.id));
    // While the query rebinds after a disconnect, its source has no stream. A change tagged with
    // an old stream still is not a local write.
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(MockNode { entities: vec![] }),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
        )
        .await
        .unwrap();
    reactor
        .notify_change(vec![OwnChange { change: TestChange { entity: same_version.clone(), events: vec![] }, origin: origin(query) }])
        .await;
    assert!(!resultset.contains_key(&same_version.id));
    reactor.notify_change(vec![TestChange { entity: same_version.clone(), events: vec![] }]).await;
    assert!(resultset.contains_key(&same_version.id), "an untagged local or Fetch change still enters by the predicate");
}

#[tokio::test]
async fn a_complete_answer_removes_omitted_offered_rows_and_keeps_later_local_additions() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let omitted = TestEntity::new("offered, then omitted", "pending");
    let local = TestEntity::new("written after the offer", "pending");
    let arrival = TestEntity::new("admitted by the answer", "pending");
    let resultset = EntityResultSet::empty();
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(MockNode { entities: vec![omitted.clone()] }),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    // The offer lists the rows the query held when the requester took its snapshot. A local write
    // after that is not offered, so the answer cannot decide it.
    let offered = [omitted.id].into_iter().collect();
    reactor.notify_change(vec![TestChange { entity: local.clone(), events: vec![] }]).await;
    updates();
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(Arc::new(std::sync::RwLock::new([arrival.id].into_iter().collect())), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall {
                entities: vec![arrival.clone()],
                membership: RemoteMembership { offered, ..Default::default() },
                changes: vec![],
                complete: true,
            },
        )
        .await
        .unwrap();
    assert!(!resultset.contains_key(&omitted.id), "an offered row the answer omits leaves");
    assert!(
        updates()
            .iter()
            .flat_map(|update| &update.items)
            .any(|item| item.entity.id == arrival.id && item.predicate_relevance == vec![(query, MembershipChange::Add)]),
        "an arrival after local initialization is Add, not another Initial"
    );
    assert!(resultset.contains_key(&local.id), "a local addition the requester did not offer survives the answer");
    assert!(resultset.contains_key(&arrival.id));
}

#[tokio::test]
async fn a_partial_answer_keeps_rows_other_operations_added() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let arrival = TestEntity::new("admitted by the answer", "pending");
    let fetched = TestEntity::new("fetched by another operation", "pending");
    let resultset = EntityResultSet::empty();
    let admitted = Arc::new(std::sync::RwLock::new([arrival.id].into_iter().collect()));
    let install = || {
        reactor.install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![arrival.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
    };
    install().await.unwrap();
    reactor.notify_change(vec![TestChange { entity: fetched.clone(), events: vec![] }]).await;
    install().await.unwrap();
    assert!(resultset.contains_key(&fetched.id), "a partial answer cannot remove a row another operation added");
    assert!(resultset.contains_key(&arrival.id));
}

#[tokio::test]
async fn an_absence_removes_a_local_addition_and_later_stream_data_cannot_restore_it() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let arrival = TestEntity::new("admitted by the answer", "pending");
    let local = TestEntity::new("a local write the stream never admitted", "pending");
    let resultset = EntityResultSet::empty();
    let admitted = Arc::new(std::sync::RwLock::new([arrival.id].into_iter().collect()));
    let install = |absent: HashSet<proto::EntityId>| {
        reactor.install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall {
                entities: vec![arrival.clone()],
                membership: RemoteMembership { absent, ..Default::default() },
                changes: vec![],
                complete: true,
            },
        )
    };
    install(HashSet::new()).await.unwrap();
    reactor.notify_change(vec![TestChange { entity: local.clone(), events: vec![] }]).await;
    assert!(resultset.contains_key(&local.id));
    install([local.id].into_iter().collect()).await.unwrap();
    assert!(!resultset.contains_key(&local.id), "an absence removes even a row the stream never admitted");
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: local.clone(), events: vec![] }, origin: origin(query) }]).await;
    assert!(!resultset.contains_key(&local.id), "later data from the stream cannot restore a row it removed");
    assert_eq!(resultset.len(), 1);
    assert!(resultset.contains_key(&arrival.id));
}

#[tokio::test]
async fn finite_baseline_state_changes_preserve_sorted_initial_and_add_notifications() {
    for descending in [false, true] {
        for cached in [false, true] {
            let reactor = Reactor::<TestEntity, TestEvent>::new_test();
            let subscription = reactor.subscribe();
            let query = QueryId::new();
            let rows = [TestEntity::new("alpha", "pending"), TestEntity::new("beta", "pending"), TestEntity::new("gamma", "pending")];
            let resultset = EntityResultSet::empty();
            let selection = sel(if descending {
                "status = 'pending' ORDER BY name DESC LIMIT 2"
            } else {
                "status = 'pending' ORDER BY name ASC LIMIT 2"
            });
            if cached {
                reactor
                    .upsert_query_and_notify(
                        subscription.id(),
                        query,
                        selection.clone(),
                        Arc::new(MockNode { entities: vec![] }),
                        resultset.clone(),
                        Arc::new(MockGapFetcher::new()),
                        1,
                        (),
                    )
                    .await
                    .unwrap();
            }
            let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
            let _guard = subscription.subscribe(watch);
            let expected = if descending { vec![rows[2].id, rows[1].id] } else { vec![rows[0].id, rows[1].id] };
            // Deliberately oppose result order rather than depending on a
            // HashSet's random iteration to expose finite-round reordering.
            let mut changes: Vec<_> = expected
                .iter()
                .rev()
                .map(|id| ReactorUpdateItem {
                    entity: rows.iter().find(|entity| entity.id == *id).unwrap().clone(),
                    events: vec![],
                    predicate_relevance: vec![],
                })
                .collect();
            changes.push(ReactorUpdateItem {
                entity: if descending { rows[0].clone() } else { rows[2].clone() },
                events: vec![],
                predicate_relevance: vec![],
            });
            let admitted = Arc::new(std::sync::RwLock::new(rows.iter().map(|entity| entity.id).collect()));
            let notified = reactor
                .install_remote_query(
                    subscription.id(),
                    query,
                    selection,
                    Arc::new(RemoteSource(admitted, origin(query))),
                    resultset.clone(),
                    Arc::new(MockGapFetcher::new()),
                    1,
                    (),
                    RemoteInstall { entities: rows.to_vec(), membership: RemoteMembership::default(), changes: changes, complete: true },
                )
                .await
                .unwrap();
            assert_eq!(resultset.read().iter_entities().map(|(id, _)| id).collect::<Vec<_>>(), expected);
            let updates = updates();
            assert_eq!(updates.len(), 1);
            assert_eq!(
                updates[0].items.iter().map(|item| item.entity.id).collect::<Vec<_>>(),
                expected,
                "finite state-only changes cannot reorder the generated query membership notification"
            );
            let kind = if cached { MembershipChange::Add } else { MembershipChange::Initial };
            for item in &updates[0].items {
                assert_eq!(item.predicate_relevance, vec![(query, kind.clone())]);
                assert!(item.events.is_empty(), "baseline does not replay historical events");
            }
            assert_eq!(notified, expected.into_iter().collect());
        }
    }
}

#[tokio::test]
async fn explicit_live_publication_preserves_change_order_and_causing_events() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let first = TestEntity::new("alpha", "pending");
    let second = TestEntity::new("beta", "pending");
    let resultset = EntityResultSet::empty();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    let event = TestEvent { id: proto::EventId::from_bytes([45; 32]), changes: HashMap::new() };
    let admitted = Arc::new(std::sync::RwLock::new([first.id, second.id].into_iter().collect()));
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 2"),
            Arc::new(RemoteSource(admitted, origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall {
                entities: vec![first.clone(), second.clone()],
                membership: RemoteMembership::default(),
                changes: vec![
                    ReactorUpdateItem { entity: second.clone(), events: vec![event.clone()], predicate_relevance: vec![] },
                    ReactorUpdateItem { entity: first.clone(), events: vec![], predicate_relevance: vec![] },
                ],
                complete: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(resultset.read().iter_entities().map(|(id, _)| id).collect::<Vec<_>>(), [first.id, second.id]);
    let updates = updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(
        updates[0].items.iter().map(|item| item.entity.id).collect::<Vec<_>>(),
        [second.id, first.id],
        "explicit live transaction order remains independent of final result ordering"
    );
    assert_eq!(updates[0].items[0].events, [event]);
    assert!(updates[0].items[1].events.is_empty());
    assert!(updates[0].items.iter().all(|item| item.predicate_relevance == [(query, MembershipChange::Add)]));
}

#[tokio::test]
async fn finite_remote_install_notifies_an_existing_state_advance_without_historical_events() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let entity = TestEntity::new("before", "pending");
    let resultset = EntityResultSet::empty();
    let admitted = Arc::new(std::sync::RwLock::new([entity.id].into_iter().collect()));
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![entity.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    entity.state.lock().unwrap().insert(prop("name"), "after".into());
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted, origin(query))),
            resultset,
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall {
                entities: vec![entity.clone()],
                membership: RemoteMembership { offered: [entity.id].into_iter().collect(), ..Default::default() },
                changes: vec![ReactorUpdateItem { entity, events: vec![], predicate_relevance: vec![] }],
                complete: true,
            },
        )
        .await
        .unwrap();
    let updates = updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].items.len(), 1);
    assert!(updates[0].items[0].events.is_empty());
    assert!(updates[0].items[0].predicate_relevance.is_empty(), "same identity is Update, not repeated Initial/Add");
}

#[tokio::test]
async fn owner_publication_reports_callback_before_return_without_local_gap_work() {
    struct CountGaps(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl GapFetcher<TestEntity> for CountGaps {
        async fn fetch_gap(
            &self,
            _: &ankql::ast::Selection<Resolved>,
            _: Option<&TestEntity>,
            _: usize,
        ) -> Result<Vec<TestEntity>, RetrievalError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![])
        }
    }
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let first = TestEntity::new("first", "pending");
    let replacement = TestEntity::new("replacement", "pending");
    let resultset = EntityResultSet::empty();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gap = Arc::new(CountGaps(calls.clone()));
    let admitted = Arc::new(std::sync::RwLock::new([first.id, replacement.id].into_iter().collect()));
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' LIMIT 1"),
            Arc::new(RemoteSource(admitted.clone(), origin(query))),
            resultset.clone(),
            gap.clone(),
            1,
            (),
            RemoteInstall { entities: vec![first.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    first.state.lock().unwrap().insert(prop("status"), "done".into());
    let event = TestEvent { id: proto::EventId::from_bytes([43; 32]), changes: HashMap::new() };
    let notified = reactor
        .notify_remote_publication(
            subscription.id(),
            query,
            vec![OwnChange { change: TestChange { entity: first.clone(), events: vec![event.clone()] }, origin: origin(query) }],
            HashSet::new(),
        )
        .await
        .unwrap();
    assert_eq!(notified, [first.id].into_iter().collect());
    let first_callback = updates();
    assert_eq!(first_callback.len(), 1, "reported IDs must already have completed their synchronous publication callback");
    assert_eq!(first_callback[0].items[0].predicate_relevance, vec![(query, MembershipChange::Remove)]);
    assert_eq!(first_callback[0].items[0].events, vec![event]);
    assert!(!resultset.is_gap_dirty(), "the stream's publications refill this LIMIT window");
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' LIMIT 1"),
            Arc::new(RemoteSource(admitted, origin(query))),
            resultset.clone(),
            gap,
            1,
            (),
            RemoteInstall { entities: vec![replacement.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    let next_callback = updates();
    assert_eq!(next_callback.len(), 1);
    assert_eq!(next_callback[0].items[0].predicate_relevance, vec![(query, MembershipChange::Add)]);
    tokio::task::yield_now().await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0, "no delayed generic gap worker can reorder owner callbacks");
    // Ordinary local notifications retain their existing asynchronous gap path.
    replacement.state.lock().unwrap().insert(prop("status"), "done".into());
    reactor.notify_change(vec![TestChange { entity: replacement, events: vec![] }]).await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn replaced_selection_baseline_is_initial_but_live_and_reconnect_arrivals_are_add() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let cached = TestEntity::new("cached", "pending");
    let arrival = TestEntity::new("remote arrival", "pending");
    let live = TestEntity::new("live during replacement", "ready");
    let baseline = TestEntity::new("replacement baseline", "ready");
    let reconnected = TestEntity::new("reconnected arrival", "ready");
    let resultset = EntityResultSet::empty();
    let admitted = Arc::new(std::sync::RwLock::new([cached.id, arrival.id, live.id, baseline.id, reconnected.id].into_iter().collect()));
    let source = |version| Arc::new(RemoteSource(admitted.clone(), QueryOrigin { version, ..origin(query) }));
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(MockNode { entities: vec![cached] }),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            source(1),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![arrival.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    assert_eq!(
        updates()[0].items[0].predicate_relevance,
        vec![(query, MembershipChange::Add)],
        "cached first remote arrival is incremental"
    );
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            (),
            RemoteInstall { entities: vec![], membership: RemoteMembership::default(), changes: vec![], complete: false },
        )
        .await
        .unwrap();
    updates();
    let event = TestEvent { id: proto::EventId::from_bytes([44; 32]), changes: HashMap::new() };
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            (),
            RemoteInstall {
                entities: vec![live.clone()],
                membership: RemoteMembership::default(),
                changes: vec![ReactorUpdateItem { entity: live, events: vec![event.clone()], predicate_relevance: vec![] }],
                complete: false,
            },
        )
        .await
        .unwrap();
    let live_update = updates();
    assert_eq!(
        live_update[0].items[0].predicate_relevance,
        vec![(query, MembershipChange::Add)],
        "live publication is incremental even while replacement baseline is pending"
    );
    assert_eq!(live_update[0].items[0].events, vec![event]);
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            (),
            RemoteInstall { entities: vec![baseline], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    assert_eq!(
        updates()[0].items[0].predicate_relevance,
        vec![(query, MembershipChange::Initial)],
        "finite replacement baseline remains Initial after partial preparation"
    );
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            (),
            RemoteInstall { entities: vec![], membership: RemoteMembership::default(), changes: vec![], complete: false },
        )
        .await
        .unwrap();
    assert!(updates().is_empty());
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset,
            Arc::new(MockGapFetcher::new()),
            2,
            (),
            RemoteInstall { entities: vec![reconnected], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    assert_eq!(updates()[0].items[0].predicate_relevance, vec![(query, MembershipChange::Add)], "same-version reconnect is incremental");
}

#[tokio::test]
async fn pending_publication_preserves_visible_predicate_and_sort_until_joint_commit() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let exiting = TestEntity::new("AAA", "pending");
    let entrant = TestEntity::new("MMM", "pending");
    let resultset = EntityResultSet::empty();
    let admitted = Arc::new(std::sync::RwLock::new([exiting.id, entrant.id].into_iter().collect()));
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            Arc::new(RemoteSource(admitted.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![exiting.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    // The exit's data applied, but the same publication's entrant still
    // needs history. The public row/key remains at the committed cut.
    exiting.state.lock().unwrap().insert(prop("status"), "done".into());
    exiting.state.lock().unwrap().insert(prop("name"), "ZZZ".into());
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            Arc::new(RemoteSource(admitted.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall {
                entities: vec![exiting.clone(), entrant.clone()],
                membership: RemoteMembership {
                    offered: [exiting.id].into_iter().collect(),
                    absent: HashSet::new(),
                    held: [exiting.id].into_iter().collect(),
                    known_heads: HashMap::new(),
                },
                changes: vec![],
                complete: true,
            },
        )
        .await
        .unwrap();
    assert!(resultset.contains_key(&exiting.id), "finite baseline cannot leak a pending predicate/sort exit");
    assert!(!resultset.contains_key(&entrant.id));
    assert!(updates().is_empty(), "there is no empty-event premature removal");
    let earlier = TestEvent { id: proto::EventId::from_bytes([41; 32]), changes: HashMap::new() };
    reactor
        .notify_remote_publication(
            subscription.id(),
            query,
            vec![OwnChange { change: TestChange { entity: exiting.clone(), events: vec![earlier.clone()] }, origin: origin(query) }],
            [exiting.id].into_iter().collect(),
        )
        .await
        .unwrap();
    let earlier_updates = updates();
    assert_eq!(earlier_updates.len(), 1);
    assert_eq!(earlier_updates[0].items.len(), 1);
    assert!(earlier_updates[0].items[0].predicate_relevance.is_empty(), "an earlier ready edit cannot expose a later pending exit");
    assert_eq!(earlier_updates[0].items[0].events, vec![earlier]);
    assert!(resultset.contains_key(&exiting.id));

    admitted.write().unwrap().remove(&exiting.id);
    let event = TestEvent { id: proto::EventId::from_bytes([42; 32]), changes: HashMap::new() };
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            Arc::new(RemoteSource(admitted, origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall {
                entities: vec![entrant.clone()],
                membership: RemoteMembership {
                    offered: HashSet::new(),
                    absent: [exiting.id].into_iter().collect(),
                    held: HashSet::new(),
                    known_heads: HashMap::new(),
                },
                changes: vec![ReactorUpdateItem { entity: exiting.clone(), events: vec![event.clone()], predicate_relevance: vec![] }],
                complete: true,
            },
        )
        .await
        .unwrap();
    let updates = updates();
    assert_eq!(updates.len(), 1, "one coherent publication owns the notification boundary");
    assert_eq!(updates[0].items.len(), 2);
    assert_eq!(updates[0].items[0].predicate_relevance, vec![(query, MembershipChange::Remove)]);
    assert_eq!(updates[0].items[0].events, vec![event]);
    assert_eq!(updates[0].items[1].predicate_relevance, vec![(query, MembershipChange::Add)]);
    assert!(resultset.contains_key(&entrant.id));
    assert!(!resultset.contains_key(&exiting.id));
}

#[tokio::test]
async fn local_optimistic_member_can_enter_a_remote_sorted_limit() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let remote_row = TestEntity::new("ZZZ remote", "pending");
    let local = TestEntity::new("AAA local", "pending");
    let resultset = EntityResultSet::empty();
    let source = Arc::new(RemoteSource(Arc::new(std::sync::RwLock::new([remote_row.id].into_iter().collect())), origin(query)));
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            source,
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![remote_row.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: local.clone(), events: vec![] }, origin: origin(query) }]).await;
    assert!(resultset.contains_key(&remote_row.id));
    reactor.notify_change(vec![TestChange { entity: local.clone(), events: vec![] }]).await;
    assert_eq!(resultset.len(), 1);
    assert!(resultset.contains_key(&local.id), "normal local notifications still participate in predicate/sort/LIMIT");
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: local.clone(), events: vec![] }, origin: origin(query) }]).await;
    assert!(resultset.contains_key(&local.id), "an excluded own-stream job must not remove the legitimate optimistic row");
    let source = Arc::new(RemoteSource(Arc::new(std::sync::RwLock::new([remote_row.id].into_iter().collect())), origin(query)));
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            source,
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: vec![remote_row.clone()], membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    assert_eq!(resultset.len(), 1);
    assert!(resultset.contains_key(&local.id), "a partial answer sorts and limits its rows together with local additions");
}

/// Delivery planning and evaluation take a change's effect on the query from one decision, so
/// every change the query publishes carries the events planning prepared for it. If the two
/// disagreed, a published change would arrive without its events, and an update that moves no
/// membership would vanish.
#[tokio::test]
async fn published_changes_keep_the_events_planning_prepared() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let updated = TestEntity::new("updated", "pending");
    let exiting = TestEntity::new("exiting", "pending");
    let held = TestEntity::new("held by a pending publication", "pending");
    let unadmitted = TestEntity::new("not admitted", "pending");
    let local = TestEntity::new("written locally", "pending");
    let resultset = EntityResultSet::empty();
    let admitted = Arc::new(std::sync::RwLock::new([updated.id, exiting.id, held.id].into_iter().collect()));
    let members = vec![updated.clone(), exiting.clone(), held.clone()];
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted, origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: members, membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    let event = |byte| TestEvent { id: proto::EventId::from_bytes([byte; 32]), changes: HashMap::new() };
    let own = |entity: &TestEntity, byte| OwnChange {
        change: TestChange { entity: entity.clone(), events: vec![event(byte)] },
        origin: origin(query),
    };
    let published = || -> HashMap<_, _> {
        updates()
            .into_iter()
            .flat_map(|update| update.items)
            .map(|item| (item.entity.id, (item.events, item.predicate_relevance)))
            .collect()
    };
    exiting.state.lock().unwrap().insert(prop("status"), "done".into());
    held.state.lock().unwrap().insert(prop("status"), "done".into());
    reactor
        .notify_remote_publication(
            subscription.id(),
            query,
            vec![own(&updated, 1), own(&exiting, 2), own(&held, 3), own(&unadmitted, 4)],
            [held.id].into_iter().collect(),
        )
        .await
        .unwrap();
    let publication = published();
    assert_eq!(publication[&updated.id], (vec![event(1)], vec![]), "an update that moves no membership keeps its events");
    assert_eq!(publication[&exiting.id], (vec![event(2)], vec![(query, MembershipChange::Remove)]));
    assert_eq!(publication[&held.id], (vec![event(3)], vec![]), "a held row keeps its membership and still delivers its events");
    assert!(!publication.contains_key(&unadmitted.id), "the stream cannot add a row it did not admit");
    reactor.notify_change(vec![TestChange { entity: local.clone(), events: vec![event(5)] }]).await;
    assert_eq!(published()[&local.id], (vec![event(5)], vec![(query, MembershipChange::Add)]));
}

/// A publication that admits one row and removes others changes only those rows: the arrival
/// enters, each absent row leaves with its change's events, and every other row stays as it is.
#[tokio::test]
async fn a_publication_admits_and_removes_rows_without_reinstalling_the_query() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let kept = TestEntity::new("kept", "pending");
    let changed_exit = TestEntity::new("leaves with a change", "pending");
    let quiet_exit = TestEntity::new("leaves without a change", "pending");
    let arrival = TestEntity::new("arrives", "pending");
    let local = TestEntity::new("local addition", "pending");
    let resultset = EntityResultSet::empty();
    let admitted = Arc::new(std::sync::RwLock::new([kept.id, changed_exit.id, quiet_exit.id].into_iter().collect()));
    let members = vec![kept.clone(), changed_exit.clone(), quiet_exit.clone()];
    reactor
        .install_remote_query(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(RemoteSource(admitted.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            RemoteInstall { entities: members, membership: RemoteMembership::default(), changes: vec![], complete: true },
        )
        .await
        .unwrap();
    reactor.notify_change(vec![TestChange { entity: local.clone(), events: vec![] }]).await;
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);

    // The live query applies the publication's admissions to the stream's admitted ids first.
    let absent: HashSet<_> = [changed_exit.id, quiet_exit.id, local.id].into_iter().collect();
    admitted.write().unwrap().insert(arrival.id);
    admitted.write().unwrap().retain(|id| !absent.contains(id));
    let event = |byte| TestEvent { id: proto::EventId::from_bytes([byte; 32]), changes: HashMap::new() };
    let own = |entity: &TestEntity, events| OwnChange { change: TestChange { entity: entity.clone(), events }, origin: origin(query) };
    let notified = reactor
        .notify_remote_membership(
            subscription.id(),
            query,
            vec![own(&arrival, vec![event(1)]), own(&changed_exit, vec![event(2)]), own(&quiet_exit, vec![]), own(&local, vec![])],
            HashSet::new(),
            absent,
        )
        .await
        .unwrap();
    let published: HashMap<_, _> = updates()
        .into_iter()
        .flat_map(|update| update.items)
        .map(|item| (item.entity.id, (item.events, item.predicate_relevance)))
        .collect();
    assert_eq!(published[&arrival.id], (vec![event(1)], vec![(query, MembershipChange::Add)]));
    assert_eq!(published[&changed_exit.id], (vec![event(2)], vec![(query, MembershipChange::Remove)]));
    assert_eq!(published[&quiet_exit.id], (vec![], vec![(query, MembershipChange::Remove)]));
    assert_eq!(published[&local.id], (vec![], vec![(query, MembershipChange::Remove)]), "an absence removes a local addition too");
    assert!(!published.contains_key(&kept.id), "a row the publication does not name is not touched");
    assert_eq!(notified, [arrival.id, changed_exit.id, quiet_exit.id, local.id].into_iter().collect());
    assert_eq!(resultset.read().iter_entities().map(|(id, _)| id).collect::<HashSet<_>>(), [kept.id, arrival.id].into_iter().collect());
}
