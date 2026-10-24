//! What a committed publication from a query's stream publishes to that query.

use super::*;

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
