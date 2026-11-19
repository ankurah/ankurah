use super::*;
use crate::connector::SendError;
use crate::node::handler::commit_transaction;
use crate::policy::{PermissiveAgent, DEFAULT_CONTEXT};
use crate::test_utils::TestStorage;
use ankurah_signals::Subscribe;
use proto::ConnectionSide::{Accepting, Dialing};

mod commit;
mod keypair;
mod trees;

#[derive(Clone)]
struct ClosedSender;

impl PeerSender for ClosedSender {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Err(SendError::ConnectionClosed) }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[derive(Clone)]
struct RecordingSender(std::sync::mpsc::Sender<proto::NodeMessage>);

impl PeerSender for RecordingSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.0.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

type TestNode = Node<TestStorage, PermissiveAgent>;

/// A peer connection for tests that use `node` as the accepting side.
async fn connect(node: &TestNode, peer: &TestNode, sender: impl PeerSender + 'static) -> ConnectionId {
    let challenge = proto::Challenge::random();
    node.register_peer(Accepting, &challenge, peer.presence(Dialing, &challenge).await.unwrap(), Box::new(sender)).await.unwrap()
}

#[tokio::test]
async fn presence_does_not_require_a_ready_catalog_but_rejects_a_halted_node() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let challenge = proto::Challenge::random();
    let presence =
        tokio::time::timeout(std::time::Duration::from_secs(2), node.presence(Dialing, &challenge)).await.unwrap().unwrap().presence;
    assert_eq!(Some(presence.node_id()), node.id());
    assert!(!presence.durable);
    assert_eq!(presence.protocol_version, proto::PROTOCOL_VERSION);
    assert_eq!(node.state().value(), NodeState::Startup);

    let halt_reason = node.lifecycle.halt(NodeHaltReason::CatalogLoad("test failure".into()));
    assert_eq!(node.state().value(), NodeState::Halted(halt_reason.clone()));
    assert_eq!(node.presence(Dialing, &challenge).await.unwrap_err(), halt_reason);
}

#[tokio::test]
async fn a_node_takes_its_id_from_the_keypair_its_application_supplies() {
    let storage = Arc::new(TestStorage::default());
    let key = NodeKey::generate();
    assert_eq!(Node::new_with_key(storage.clone(), PermissiveAgent::new(), key.clone()).id(), Some(key.public_key().node_id()));
    assert_eq!(Node::new_durable_with_key(storage, PermissiveAgent::new(), key.clone()).id(), Some(key.public_key().node_id()));
}

#[tokio::test]
async fn a_peer_that_does_not_prove_its_key_on_this_connection_is_refused() {
    let [node, peer, other] = [(); 3].map(|()| Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new()));
    let challenge = proto::Challenge::random();

    let signed_for_another_connection = peer.presence(Dialing, &proto::Challenge::random()).await.unwrap();
    let mut claiming_the_peers_key = other.presence(Dialing, &challenge).await.unwrap();
    claiming_the_peers_key.presence.key = signed_for_another_connection.presence.key;
    let the_nodes_own_presence_sent_back = node.presence(Accepting, &challenge).await.unwrap();
    for unproven in [signed_for_another_connection, claiming_the_peers_key, the_nodes_own_presence_sent_back] {
        assert_eq!(
            node.register_peer(Accepting, &challenge, unproven, Box::new(ClosedSender)).await,
            Err(PeerConnectionError::UnprovenKey)
        );
    }
    assert!(node.peer_connections.is_empty());
}

#[tokio::test]
async fn lifecycle_waits_for_the_catalog_and_never_revives_after_halting() {
    for halt_during_startup in [false, true] {
        let storage = Arc::new(TestStorage::default());
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        storage.hold_fetch.lock().unwrap().insert(ModelId::System(proto::SystemModel::Model), (entered, release_rx));
        let node = Node::new_durable(storage, PermissiveAgent::new());
        let state = node.state();
        let run = node.run.clone();
        assert!(run.get());
        assert_eq!(state.value(), NodeState::Startup);
        assert_eq!(node.check_ready(), Err(NodeReadinessError::NotReady));
        let mut ready = Box::pin(node.wait_ready());
        assert!(futures::poll!(&mut ready).is_pending());
        let (changes, observed) = std::sync::mpsc::channel();
        let _subscription = state.subscribe(changes);
        entered_rx.await.unwrap();
        assert_eq!(state.value(), NodeState::Startup);
        assert!(run.get());
        assert_eq!(node.check_ready(), Err(NodeReadinessError::NotReady));
        assert!(futures::poll!(&mut ready).is_pending());

        let reason = NodeHaltReason::CatalogLoad("first reason".into());
        if halt_during_startup {
            node.lifecycle.halt(reason.clone());
            assert_eq!(ready.await, Err(NodeReadinessError::Halted(reason.clone())));
            release.send(()).unwrap();
        } else {
            release.send(()).unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), ready).await.unwrap().unwrap();
            assert_eq!(node.check_ready(), Ok(()));
            assert!(run.get());
            assert_eq!(observed.try_recv().unwrap(), NodeState::Running);
            node.lifecycle.halt(reason.clone());
        }
        assert_eq!(observed.try_recv().unwrap(), NodeState::Halted(reason.clone()));
        assert_eq!(node.lifecycle.halt(NodeHaltReason::CatalogLoad("later reason".into())), reason);
        node.lifecycle.mark_running();
        assert_eq!(node.check_ready(), Err(NodeReadinessError::Halted(reason.clone())));
        assert_eq!(node.wait_ready().await, Err(NodeReadinessError::Halted(reason.clone())));
        assert_eq!(state.value(), NodeState::Halted(reason));
        assert!(!run.get());
    }
}

#[tokio::test]
async fn dropping_a_starting_node_cancels_its_readiness_wait() {
    let storage = Arc::new(TestStorage::default());
    let weak_storage = Arc::downgrade(&storage);
    let node = Node::new(storage, PermissiveAgent::new());
    node.wait_id().await.unwrap();
    let weak = node.weak();
    let run = node.run.clone();
    drop(node);
    assert!(weak.upgrade().is_none());
    assert!(!run.get());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while weak_storage.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn presence_uses_the_persisted_key_while_catalog_loading_is_pending() {
    let storage = Arc::new(TestStorage::default());
    let seed = Node::new_durable(storage.clone(), PermissiveAgent::new());
    seed.wait_ready().await.unwrap();
    let id = seed.id();
    drop(seed);

    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    storage.hold_fetch.lock().unwrap().insert(ModelId::System(proto::SystemModel::Model), (entered, release_rx));
    let node = Node::new_durable(storage, PermissiveAgent::new());
    entered_rx.await.unwrap();
    let challenge = proto::Challenge::random();
    let presence = tokio::time::timeout(std::time::Duration::from_secs(2), node.presence(Dialing, &challenge)).await.unwrap().unwrap();
    assert_eq!(Some(presence.presence.node_id()), id);
    assert!(presence.presence.durable);
    assert_eq!(node.state().value(), NodeState::Startup);
    release.send(()).unwrap();
    node.wait_ready().await.unwrap();
}

#[tokio::test]
async fn disconnect_releases_pending_requests() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let peer = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let (sent, _messages) = std::sync::mpsc::channel();
    let connection = connect(&node, &peer, RecordingSender(sent)).await;
    let request = node.request_on(connection, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());

    node.deregister_peer(connection);
    assert!(matches!(futures::poll!(&mut request), std::task::Poll::Ready(Err(RequestError::InternalChannelClosed))));
}

#[tokio::test]
async fn disconnect_releases_pending_requests_while_a_message_from_that_peer_is_being_handled() {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.wait_ready().await.unwrap();
    let peer = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let (sent, _messages) = std::sync::mpsc::channel();
    let connection = connect(&node, &peer, RecordingSender(sent)).await;

    let model = ModelId::System(proto::SystemModel::Model);
    let (entered, entered_rx) = oneshot::channel();
    let (_release, release_rx) = oneshot::channel();
    *storage.hold_state_snapshot.lock().unwrap() = Some((entered, release_rx));
    let selection = ankql::ast::Selection::<Resolved> { predicate: ankql::ast::Predicate::MemberOf(model), order_by: None, limit: None };
    let body = proto::NodeRequestBody::Fetch {
        selection,
        comparison: proto::ComparisonId::new(),
        key_spec: ankurah_core_types::key_spec::KeySpec::new(vec![]),
        known_state: proto::KnownState::empty(),
    };
    let fetch = proto::NodeRequest { id: proto::RequestId::new(), from: peer.id().unwrap(), to: node.id().unwrap(), body };
    let handling = node.handle_message(connection, proto::NodeMessage::Request { auth: vec![], request: fetch });
    tokio::pin!(handling);
    assert!(futures::poll!(&mut handling).is_pending());
    entered_rx.await.unwrap();

    let request = node.request_on(connection, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    node.deregister_peer(connection);
    assert!(matches!(futures::poll!(&mut request), std::task::Poll::Ready(Err(RequestError::InternalChannelClosed))));
}

#[tokio::test]
async fn two_connections_of_one_node_id_each_keep_their_own_requests() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let [first_peer, second_peer] =
        [(); 2].map(|()| Node::new_with_key(Arc::new(TestStorage::default()), PermissiveAgent::new(), key.clone()));
    let (sent_first, first_messages) = std::sync::mpsc::channel();
    let (sent_second, second_messages) = std::sync::mpsc::channel();
    let first = connect(&node, &first_peer, RecordingSender(sent_first)).await;
    let second = connect(&node, &second_peer, RecordingSender(sent_second)).await;

    let request = node.request_on(second, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    assert!(first_messages.try_recv().is_err());
    let Ok(proto::NodeMessage::Request { request: sent, .. }) = second_messages.try_recv() else {
        panic!("the request goes out on its own connection")
    };
    let response =
        |body| proto::NodeMessage::Response(proto::NodeResponse { request_id: sent.id.clone(), from: sent.to, to: sent.from, body });

    // A response on the first connection cannot complete the request on the second connection.
    node.handle_message(first, response(proto::NodeResponseBody::Get(vec![]))).await.unwrap();
    node.deregister_peer(first);
    assert!(futures::poll!(&mut request).is_pending());

    node.handle_message(second, response(proto::NodeResponseBody::Get(vec![]))).await.unwrap();
    assert!(matches!(request.await, Ok(proto::NodeResponseBody::Get(_))));
}

#[tokio::test]
async fn a_request_addressed_by_node_id_goes_out_on_the_newest_connection_of_that_id() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let peers = [(); 2].map(|()| Node::new_with_key(Arc::new(TestStorage::default()), PermissiveAgent::new(), key.clone()));
    let (sent_older, older_messages) = std::sync::mpsc::channel();
    let (sent_newer, newer_messages) = std::sync::mpsc::channel();
    let older = connect(&node, &peers[0], RecordingSender(sent_older)).await;
    let newer = connect(&node, &peers[1], RecordingSender(sent_newer)).await;
    assert_eq!(node.connections_of(key.public_key().node_id()), [older, newer]);

    let request = node.request(key.public_key().node_id(), &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    assert!(older_messages.try_recv().is_err());
    assert!(matches!(newer_messages.try_recv(), Ok(proto::NodeMessage::Request { .. })));
}

#[tokio::test]
async fn a_message_that_names_another_sender_than_its_connection_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let [peer, other] = [(); 2].map(|()| Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new()));
    let connection = connect(&node, &peer, ClosedSender).await;
    connect(&node, &other, ClosedSender).await;

    let unsubscribe = |from| proto::NodeMessage::UnsubscribeQuery { from, query_id: proto::QueryId::new() };
    assert!(node.handle_message(connection, unsubscribe(other.id().unwrap())).await.is_err());
    // The peer's own unsubscribe reaches its handler, which holds no such query.
    assert!(node
        .handle_message(connection, unsubscribe(peer.id().unwrap()))
        .await
        .unwrap_err()
        .downcast_ref::<crate::error::SubscriptionError>()
        .is_some());
}

#[tokio::test]
async fn halt_cancels_an_inflight_read_and_rejects_new_work() {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.wait_ready().await.unwrap();
    let transaction = node.privileged_context().begin();
    let id = transaction.create(&crate::schema::catalog::SysModelRow { label: "test".into(), name: "Test".into() }).await.unwrap().id();
    transaction.commit().await.unwrap();
    let peer = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let connection = connect(&node, &peer, ClosedSender).await;

    let model = ModelId::System(proto::SystemModel::Model);
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    storage.hold_fetch.lock().unwrap().insert(model, (entered, release_rx));
    let selection = ankql::ast::Selection::<Resolved> { predicate: ankql::ast::Predicate::MemberOf(model), order_by: None, limit: None };
    let read = node.fetch_entities_from_local(&selection);
    tokio::pin!(read);
    assert!(futures::poll!(&mut read).is_pending());
    entered_rx.await.unwrap();

    let reason = node.lifecycle.halt(NodeHaltReason::CatalogLoad("fixture".into()));
    assert!(matches!(read.await, Err(RetrievalError::NodeHalted(error)) if error == reason));
    assert!(release.send(()).is_err(), "halt cancelled the suspended read");
    assert!(matches!(node.fetch_entities_from_local(&selection).await, Err(RetrievalError::NodeHalted(error)) if error == reason));
    assert!(matches!(
        node.request_on(connection, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![id] }).await,
        Err(RequestError::NodeHalted(error)) if error == reason
    ));
    assert!(node
        .handle_message(connection, proto::NodeMessage::UnsubscribeQuery { from: peer.id().unwrap(), query_id: proto::QueryId::new() })
        .await
        .is_err());
}

#[tokio::test]
async fn remote_creations_are_not_registered_or_published_before_storage_succeeds() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.wait_ready().await?;
    let a = ModelId::EntityId(proto::EntityId::random());
    let b = ModelId::EntityId(proto::EntityId::random());
    let membership = |model| proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]);
    let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, membership(a));
    let update = proto::Event::update(genesis.entity_id, proto::Clock::singleton(&genesis), proto::AuthorId::Unknown, membership(b));
    let other_genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, membership(b));
    let other_update =
        proto::Event::update(other_genesis.entity_id, proto::Clock::singleton(&other_genesis), proto::AuthorId::Unknown, membership(a));

    let events = vec![genesis.clone().into(), other_genesis.clone().into(), update.clone().into(), other_update.clone().into()];
    let subscription = node.reactor.subscribe();
    subscription.add_entity_subscriptions([genesis.entity_id, other_genesis.entity_id]);
    let (updates, received) = std::sync::mpsc::channel();
    let _guard = subscription.subscribe(move |update| updates.send(update).unwrap());

    storage.fail_next_commit();
    let error = commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), events.clone()).await.unwrap_err();
    assert!(error.to_string().contains("test storage commit failed"), "{error}");
    assert!(node.entities.get(&genesis.entity_id).is_none());
    assert!(storage.dump_entity_events(genesis.entity_id).await?.is_empty());
    assert!(node.entities.get(&other_genesis.entity_id).is_none());
    assert!(storage.dump_entity_events(other_genesis.entity_id).await?.is_empty());
    assert!(received.try_recv().is_err());

    commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), events).await?;
    let changes = received.try_recv()?.items;
    let resident = changes.iter().find(|change| change.entity.id() == genesis.entity_id).unwrap().entity.clone();
    let other_resident = changes.iter().find(|change| change.entity.id() == other_genesis.entity_id).unwrap().entity.clone();
    assert_eq!(resident.head(), proto::Clock::singleton(&update));
    assert_eq!(resident.memberships(), [a, b].into_iter().collect());
    assert_eq!(storage.dump_entity_events(genesis.entity_id).await?.len(), 2);
    assert_eq!(other_resident.head(), proto::Clock::singleton(&other_update));
    assert_eq!(other_resident.memberships(), [a, b].into_iter().collect());
    assert_eq!(storage.dump_entity_events(other_genesis.entity_id).await?.len(), 2);
    assert_eq!(changes.len(), 2, "one change per entity, not per event");
    for (entity, expected) in [
        (genesis.entity_id, vec![genesis.into(), update.into()]),
        (other_genesis.entity_id, vec![other_genesis.into(), other_update.into()]),
    ] {
        assert_eq!(changes.iter().find(|change| change.entity.id() == entity).unwrap().events, expected);
    }
    assert!(received.try_recv().is_err());
    assert_eq!(node.entities.get(&resident.id()), Some(resident));
    Ok(())
}

#[tokio::test]
async fn remote_commit_waits_for_the_winners_resident_publication() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.wait_ready().await?;
    let [a, b, c] = [1, 2, 3].map(|byte| ModelId::EntityId(proto::EntityId::from_bytes([byte; 32])));
    let membership = |model| proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]);
    let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, membership(a));
    let id = genesis.entity_id;
    commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(genesis.clone(), None)]).await?;
    let getter = LocalEventGetter::new(storage.clone(), true);
    let entity = node.entities.get_or_retrieve(&LocalStateGetter::new(storage.clone()), &getter, &id).await?.unwrap();
    let ours = proto::Event::update(id, proto::Clock::singleton(&genesis), proto::AuthorId::Unknown, membership(c));
    let other = proto::Event::update(id, proto::Clock::singleton(&genesis), proto::AuthorId::Unknown, membership(b));
    let expected_events = [other.id(), ours.id()];
    let subscription = node.reactor.subscribe();
    subscription.add_entity_subscriptions([id]);
    let (updates, received) = std::sync::mpsc::channel();
    let _guard = subscription.subscribe(move |update| updates.send(update).unwrap());
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *storage.hold_after_commit.lock().unwrap() = Some((entered, release_rx));
    let winner = commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(other, None)]);
    tokio::pin!(winner);
    assert!(futures::poll!(&mut winner).is_pending());
    entered_rx.await?;

    assert_eq!(storage.get_state(id).await?.payload.state.memberships, [a, b].into_iter().collect());
    assert_eq!(entity.memberships(), [a].into_iter().collect());
    let pending = commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(ours, None)]);
    tokio::pin!(pending);
    assert!(futures::poll!(&mut pending).is_pending());
    assert_eq!(entity.memberships(), [a].into_iter().collect());
    assert!(received.try_recv().is_err());
    release.send(()).unwrap();
    winner.await?;
    assert_eq!(entity.memberships(), [a, b].into_iter().collect());
    pending.await?;

    assert_eq!(entity.memberships(), [a, b, c].into_iter().collect());
    assert_eq!(entity.to_state()?, storage.get_state(id).await?.payload.state);
    let notified: Vec<_> =
        received.try_iter().flat_map(|update| update.items).flat_map(|item| item.events).map(|event| event.payload.id()).collect();
    assert_eq!(notified, expected_events);
    Ok(())
}

#[tokio::test]
async fn reset_waits_for_an_update_already_entering_its_peer_sender() {
    #[derive(Clone)]
    struct HeldSender {
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        release: Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
    }
    impl PeerSender for HeldSender {
        fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> {
            self.entered.lock().unwrap().take().unwrap().send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(())
        }
        fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    }

    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.wait_ready().await.unwrap();
    let peer = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let connection =
        connect(&node, &peer, HeldSender { entered: Arc::new(Mutex::new(Some(entered))), release: Arc::new(Mutex::new(release_rx)) }).await;
    let sender = node.clone();
    let send = std::thread::spawn(move || sender.send_update(connection, proto::NodeUpdateBody::SubscriptionUpdate { items: vec![] }));
    entered_rx.await.unwrap();
    let reset = node.reset();
    tokio::pin!(reset);
    let pending = tokio::time::timeout(std::time::Duration::from_millis(100), reset.as_mut()).await;
    release.send(()).unwrap();
    send.join().unwrap();
    assert!(pending.is_err(), "reset must not finish while an old update can still be sent");
    reset.await.unwrap();
    assert_eq!(node.state().value(), NodeState::Halted(NodeHaltReason::Reset));
}
