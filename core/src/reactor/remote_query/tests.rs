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

struct CandidateWindow(Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>, QueryOrigin);
fn origin(query: QueryId) -> QueryOrigin { QueryOrigin { query, version: 1, request: proto::RequestId::default() } }
#[async_trait::async_trait]
impl LocalEntitySource<TestEntity> for CandidateWindow {
    fn query_origin(&self) -> Option<&QueryOrigin> { Some(&self.1) }
    fn admits_query_entity(&self, entity: &TestEntity) -> bool { self.0.read().unwrap().contains(&entity.id) }
    async fn fetch_entities_from_local(&self, _: &ankql::ast::Selection<Resolved>) -> Result<Vec<TestEntity>, RetrievalError> { Ok(vec![]) }
}
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
async fn source_admission_survives_late_data_and_preserves_readable_removals() {
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
    let gate = Arc::new(std::sync::RwLock::new([entity.id].into_iter().collect()));
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            id,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate.clone(), origin(id))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![entity.clone()],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    let (watch, changes) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _listener = subscription.subscribe(watch);
    gate.write().unwrap().clear();
    // Explicit source membership removes the row. The later data job does
    // not itself author an absence or membership resurrection.
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            id,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate.clone(), origin(id))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![],
            PeerQueryMembership {
                replace: [entity.id].into_iter().collect(),
                remove: HashSet::new(),
                preserve: HashSet::new(),
                baseline: HashMap::new(),
            },
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(resultset.len(), 0);
    let removal = changes();
    assert!(
        removal.iter().flat_map(|update| &update.items).any(|item| item.predicate_relevance.contains(&(id, MembershipChange::Remove))),
        "the readable explicit removal still notifies"
    );
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: entity.clone(), events: vec![] }, origin: origin(id) }]).await;
    assert_eq!(resultset.len(), 0, "an own-stream repair cannot re-admit the source outsider");
    assert!(changes().is_empty());
    // Other Fetch/local application retains the pre-stream predicate semantics.
    reactor.notify_change(vec![TestChange { entity: entity.clone(), events: vec![] }]).await;
    assert!(resultset.contains_key(&entity.id));
    let _ = changes();
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: entity.clone(), events: vec![] }, origin: origin(id) }]).await;
    assert!(resultset.contains_key(&entity.id), "a late own repair cannot undo that independent authorized admission");
    assert!(changes().is_empty());
    let other = TestEntity::new("another query's row", "pending");
    reactor
        .notify_change(vec![OwnChange { change: TestChange { entity: other.clone(), events: vec![] }, origin: origin(QueryId::new()) }])
        .await;
    assert!(resultset.contains_key(&other.id), "another query stream is independent of this source candidate window");
}

#[tokio::test]
async fn replaced_stream_notifications_do_not_borrow_the_replacement_authority() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let resultset = EntityResultSet::empty();
    let old = origin(query);
    let current = QueryOrigin { request: proto::RequestId::new(), ..old.clone() };
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(Arc::new(std::sync::RwLock::new(HashSet::new())), current.clone())),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    let same_version = TestEntity::new("old same-version request", "pending");
    let old_version = TestEntity::new("old selection version", "pending");
    let own = TestEntity::new("current source outsider", "pending");
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: own.clone(), events: vec![] }, origin: current.clone() }]).await;
    assert!(!resultset.contains_key(&own.id));
    // A completed old apply can reach the notification lock after replacement.
    // Its data remains valid for other queries, but cannot author this query's membership.
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: same_version.clone(), events: vec![] }, origin: old }]).await;
    assert!(!resultset.contains_key(&same_version.id));
    reactor
        .notify_change(vec![OwnChange {
            change: TestChange { entity: old_version.clone(), events: vec![] },
            origin: QueryOrigin { version: 0, ..current },
        }])
        .await;
    assert!(!resultset.contains_key(&old_version.id));
    // During disconnect/rebind the installed source can have no active
    // stream. That must not turn a tagged old change into a local write.
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
    assert!(resultset.contains_key(&same_version.id), "untagged local/Fetch changes remain independent");
}

#[tokio::test]
async fn peer_round_reconciles_frozen_known_rows_and_partial_updates_preserve_independent_additions() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let stale = TestEntity::new("captured former member", "pending");
    let local = TestEntity::new("inserted after requester capture", "pending");
    let fetched = TestEntity::new("fetched by another operation", "pending");
    let source_row = TestEntity::new("new source member", "pending");
    let resultset = EntityResultSet::empty();
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(MockNode { entities: vec![stale.clone()] }),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    // Freeze K before the local/Fetch changes. Full installation must not
    // take a second snapshot of public membership before awaiting its lock.
    let known = [stale.id].into_iter().collect();
    reactor.notify_change(vec![TestChange { entity: local.clone(), events: vec![] }]).await;
    let gate = Arc::new(std::sync::RwLock::new([source_row.id].into_iter().collect()));
    updates();
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![source_row.clone()],
            PeerQueryMembership { replace: known, remove: HashSet::new(), preserve: HashSet::new(), baseline: HashMap::new() },
            vec![],
        )
        .await
        .unwrap();
    assert!(!resultset.contains_key(&stale.id), "captured known absence is authoritative");
    assert!(
        updates()
            .iter()
            .flat_map(|update| &update.items)
            .any(|item| item.entity.id == source_row.id && item.predicate_relevance == vec![(query, MembershipChange::Add)]),
        "arrival after local initialization is Add, not another Initial"
    );
    assert!(resultset.contains_key(&local.id), "a new local ID outside K survives the full round");
    assert!(resultset.contains_key(&source_row.id));
    reactor.notify_change(vec![TestChange { entity: fetched.clone(), events: vec![] }]).await;
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![source_row.clone()],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    assert!(resultset.contains_key(&local.id));
    assert!(resultset.contains_key(&fetched.id), "unrelated partial Y cannot erase independently fetched X");
    // Explicit Absent wins even for an ID the source never admitted.
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![source_row.clone()],
            PeerQueryMembership {
                replace: HashSet::new(),
                remove: [local.id].into_iter().collect(),
                preserve: HashSet::new(),
                baseline: HashMap::new(),
            },
            vec![],
        )
        .await
        .unwrap();
    assert!(!resultset.contains_key(&local.id));
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: local.clone(), events: vec![] }, origin: origin(query) }]).await;
    assert!(!resultset.contains_key(&local.id), "late own data cannot reverse explicit source absence");
    assert!(resultset.contains_key(&fetched.id));
    // A newer absence also overrides a new outside-K addition at full commit.
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate, origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![source_row.clone()],
            PeerQueryMembership {
                replace: HashSet::new(),
                remove: [fetched.id].into_iter().collect(),
                preserve: HashSet::new(),
                baseline: HashMap::new(),
            },
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(resultset.len(), 1);
    assert!(resultset.contains_key(&source_row.id));
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
            let gate = Arc::new(std::sync::RwLock::new(rows.iter().map(|entity| entity.id).collect()));
            let notified = reactor
                .upsert_query_from_peer(
                    subscription.id(),
                    query,
                    selection,
                    Arc::new(CandidateWindow(gate, origin(query))),
                    resultset.clone(),
                    Arc::new(MockGapFetcher::new()),
                    1,
                    (),
                    rows.to_vec(),
                    PeerQueryMembership::default(),
                    changes,
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
    struct Partial;
    impl PreNotifyHook for Partial {
        fn marks_loaded(&self) -> bool { false }
        fn pre_notify(&self, _: u32) {}
    }
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let first = TestEntity::new("alpha", "pending");
    let second = TestEntity::new("beta", "pending");
    let resultset = EntityResultSet::empty();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    let event = TestEvent { id: proto::EventId::from_bytes([45; 32]), changes: HashMap::new() };
    let gate = Arc::new(std::sync::RwLock::new([first.id, second.id].into_iter().collect()));
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 2"),
            Arc::new(CandidateWindow(gate, origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            Partial,
            vec![first.clone(), second.clone()],
            PeerQueryMembership::default(),
            vec![
                ReactorUpdateItem { entity: second.clone(), events: vec![event.clone()], predicate_relevance: vec![] },
                ReactorUpdateItem { entity: first.clone(), events: vec![], predicate_relevance: vec![] },
            ],
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
async fn finite_peer_install_notifies_an_existing_state_advance_without_historical_events() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let entity = TestEntity::new("before", "pending");
    let resultset = EntityResultSet::empty();
    let gate = Arc::new(std::sync::RwLock::new([entity.id].into_iter().collect()));
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![entity.clone()],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    entity.state.lock().unwrap().insert(prop("name"), "after".into());
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(CandidateWindow(gate, origin(query))),
            resultset,
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![entity.clone()],
            PeerQueryMembership { replace: [entity.id].into_iter().collect(), ..Default::default() },
            vec![ReactorUpdateItem { entity, events: vec![], predicate_relevance: vec![] }],
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
    let gate = Arc::new(std::sync::RwLock::new([first.id, replacement.id].into_iter().collect()));
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' LIMIT 1"),
            Arc::new(CandidateWindow(gate.clone(), origin(query))),
            resultset.clone(),
            gap.clone(),
            1,
            (),
            vec![first.clone()],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    let (watch, updates) = watcher::<ReactorUpdate<TestEntity, TestEvent>>();
    let _guard = subscription.subscribe(watch);
    first.state.lock().unwrap().insert(prop("status"), "done".into());
    let event = TestEvent { id: proto::EventId::from_bytes([43; 32]), changes: HashMap::new() };
    let notified = reactor
        .notify_query_changes(
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
    assert!(!resultset.is_gap_dirty(), "the source owns this publication's replacement window");
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' LIMIT 1"),
            Arc::new(CandidateWindow(gate, origin(query))),
            resultset.clone(),
            gap,
            1,
            (),
            vec![replacement.clone()],
            PeerQueryMembership::default(),
            vec![],
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
    struct Partial;
    impl PreNotifyHook for Partial {
        fn marks_loaded(&self) -> bool { false }
        fn pre_notify(&self, _: u32) {}
    }
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let cached = TestEntity::new("cached", "pending");
    let arrival = TestEntity::new("remote arrival", "pending");
    let live = TestEntity::new("live during replacement", "ready");
    let baseline = TestEntity::new("replacement baseline", "ready");
    let reconnected = TestEntity::new("reconnected arrival", "ready");
    let resultset = EntityResultSet::empty();
    let gate = Arc::new(std::sync::RwLock::new([cached.id, arrival.id, live.id, baseline.id, reconnected.id].into_iter().collect()));
    let source = |version| Arc::new(CandidateWindow(gate.clone(), QueryOrigin { version, ..origin(query) }));
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
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            source(1),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![arrival.clone()],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(
        updates()[0].items[0].predicate_relevance,
        vec![(query, MembershipChange::Add)],
        "cached first remote arrival is incremental"
    );
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            Partial,
            vec![],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    updates();
    let event = TestEvent { id: proto::EventId::from_bytes([44; 32]), changes: HashMap::new() };
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            Partial,
            vec![live.clone()],
            PeerQueryMembership::default(),
            vec![ReactorUpdateItem { entity: live, events: vec![event.clone()], predicate_relevance: vec![] }],
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
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            (),
            vec![baseline],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(
        updates()[0].items[0].predicate_relevance,
        vec![(query, MembershipChange::Initial)],
        "finite replacement baseline remains Initial after partial preparation"
    );
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            Partial,
            vec![],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    assert!(updates().is_empty());
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'ready'"),
            source(2),
            resultset,
            Arc::new(MockGapFetcher::new()),
            2,
            (),
            vec![reconnected],
            PeerQueryMembership::default(),
            vec![],
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
    let gate = Arc::new(std::sync::RwLock::new([exiting.id, entrant.id].into_iter().collect()));
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            Arc::new(CandidateWindow(gate.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![exiting.clone()],
            PeerQueryMembership::default(),
            vec![],
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
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            Arc::new(CandidateWindow(gate.clone(), origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![exiting.clone(), entrant.clone()],
            PeerQueryMembership {
                replace: [exiting.id].into_iter().collect(),
                remove: HashSet::new(),
                preserve: [exiting.id].into_iter().collect(),
                baseline: HashMap::new(),
            },
            vec![],
        )
        .await
        .unwrap();
    assert!(resultset.contains_key(&exiting.id), "finite baseline cannot leak a pending predicate/sort exit");
    assert!(!resultset.contains_key(&entrant.id));
    assert!(updates().is_empty(), "there is no empty-event premature removal");
    let earlier = TestEvent { id: proto::EventId::from_bytes([41; 32]), changes: HashMap::new() };
    reactor
        .notify_query_changes(
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

    gate.write().unwrap().remove(&exiting.id);
    let event = TestEvent { id: proto::EventId::from_bytes([42; 32]), changes: HashMap::new() };
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            Arc::new(CandidateWindow(gate, origin(query))),
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![entrant.clone()],
            PeerQueryMembership {
                replace: HashSet::new(),
                remove: [exiting.id].into_iter().collect(),
                preserve: HashSet::new(),
                baseline: HashMap::new(),
            },
            vec![ReactorUpdateItem { entity: exiting.clone(), events: vec![event.clone()], predicate_relevance: vec![] }],
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
    let source_row = TestEntity::new("ZZZ source", "pending");
    let local = TestEntity::new("AAA local", "pending");
    let resultset = EntityResultSet::empty();
    let source = Arc::new(CandidateWindow(Arc::new(std::sync::RwLock::new([source_row.id].into_iter().collect())), origin(query)));
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            source,
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![source_row.clone()],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: local.clone(), events: vec![] }, origin: origin(query) }]).await;
    assert!(resultset.contains_key(&source_row.id));
    reactor.notify_change(vec![TestChange { entity: local.clone(), events: vec![] }]).await;
    assert_eq!(resultset.len(), 1);
    assert!(resultset.contains_key(&local.id), "normal local notifications still participate in predicate/sort/LIMIT");
    reactor.notify_change(vec![OwnChange { change: TestChange { entity: local.clone(), events: vec![] }, origin: origin(query) }]).await;
    assert!(resultset.contains_key(&local.id), "an excluded own-stream job must not remove the legitimate optimistic row");
    let source = Arc::new(CandidateWindow(Arc::new(std::sync::RwLock::new([source_row.id].into_iter().collect())), origin(query)));
    reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            sel("status = 'pending' ORDER BY name ASC LIMIT 1"),
            source,
            resultset.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
            vec![source_row.clone()],
            PeerQueryMembership::default(),
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(resultset.len(), 1);
    assert!(resultset.contains_key(&local.id), "partial source merge sorts and limits the combined set");
}
