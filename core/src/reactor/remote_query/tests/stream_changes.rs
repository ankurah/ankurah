//! What changes from a query's own remote stream may do to its membership.

use super::*;

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
