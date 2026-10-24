//! Which membership notifications an installed answer publishes, and in what order.

use super::*;

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
