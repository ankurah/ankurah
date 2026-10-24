//! How an installed answer reconciles a query's rows with the rows the peer selected.

use super::*;

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
