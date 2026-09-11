use super::*;
use crate::connector::SendError;
use crate::policy::{PermissiveAgent, DEFAULT_CONTEXT};
use crate::test_utils::TestStorage;
use ankurah_signals::Subscribe;

#[derive(Clone)]
struct ClosedSender(proto::EntityId);

impl PeerSender for ClosedSender {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Err(SendError::ConnectionClosed) }
    fn recipient_node_id(&self) -> proto::EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[derive(Clone)]
struct RecordingSender(proto::EntityId, std::sync::mpsc::Sender<proto::NodeMessage>);

impl PeerSender for RecordingSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.1.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

fn presence(id: proto::EntityId) -> proto::Presence {
    proto::Presence { node_id: id, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION }
}

#[tokio::test]
async fn presence_does_not_require_an_adopted_system_but_rejects_a_halted_node() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let presence = tokio::time::timeout(std::time::Duration::from_secs(2), node.presence()).await.unwrap().unwrap();
    assert_eq!(presence.node_id, node.id);
    assert!(!presence.durable);
    assert!(presence.system_root.is_none());
    assert_eq!(presence.protocol_version, proto::PROTOCOL_VERSION);
    assert!(!node.system.is_system_ready());
    assert_eq!(node.state().value(), NodeState::Uninitialized);

    let halt_reason = node.system.halt(NodeHaltReason::SystemLoad("test failure".into()));
    assert_eq!(node.state().value(), NodeState::Halted(halt_reason.clone()));
    assert_eq!(node.presence().await.unwrap_err(), halt_reason);
}

#[tokio::test]
async fn lifecycle_waits_for_the_catalog_and_never_revives_after_halting() {
    for halt_during_startup in [false, true] {
        let storage = Arc::new(TestStorage::default());
        let table = storage.table(&CollectionId::fixed_name(crate::schema::MODEL_COLLECTION_ID));
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        *table.hold_fetch.lock().unwrap() = Some((entered, release_rx));
        let node = Node::new_durable(storage, PermissiveAgent::new());
        let state = node.state();
        let run = node.run.clone();
        assert!(run.get());
        assert_eq!(state.value(), NodeState::Uninitialized);
        assert_eq!(node.check_ready(), Err(NodeReadinessError::NotReady));
        let mut ready = Box::pin(node.wait_ready());
        assert!(futures::poll!(&mut ready).is_pending());
        let (changes, observed) = std::sync::mpsc::channel();
        let _subscription = state.subscribe(changes);
        node.system.create().await.unwrap();
        entered_rx.await.unwrap();
        assert_eq!(state.value(), NodeState::Startup);
        assert!(run.get());
        assert_eq!(node.check_ready(), Err(NodeReadinessError::NotReady));
        assert!(futures::poll!(&mut ready).is_pending());
        assert_eq!(observed.try_recv().unwrap(), NodeState::Startup);

        let reason = NodeHaltReason::SystemLoad("first reason".into());
        if halt_during_startup {
            node.system.halt(reason.clone());
            assert_eq!(ready.await, Err(NodeReadinessError::Halted(reason.clone())));
            release.send(()).unwrap();
        } else {
            release.send(()).unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), ready).await.unwrap().unwrap();
            assert_eq!(node.check_ready(), Ok(()));
            assert!(run.get());
            assert_eq!(observed.try_recv().unwrap(), NodeState::Running);
            node.system.halt(reason.clone());
        }
        assert_eq!(observed.try_recv().unwrap(), NodeState::Halted(reason.clone()));
        assert_eq!(node.system.halt(NodeHaltReason::CatalogLoad("later reason".into())), reason);
        node.system.mark_running();
        assert_eq!(node.check_ready(), Err(NodeReadinessError::Halted(reason.clone())));
        assert_eq!(node.wait_ready().await, Err(NodeReadinessError::Halted(reason.clone())));
        assert_eq!(state.value(), NodeState::Halted(reason));
        assert!(!run.get());
    }
}

#[tokio::test]
async fn dropping_an_uninitialized_node_cancels_its_readiness_wait() {
    let storage = Arc::new(TestStorage::default());
    let weak_storage = Arc::downgrade(&storage);
    let node = Node::new(storage, PermissiveAgent::new());
    node.system.wait_loaded().await.unwrap();
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
async fn presence_waits_for_the_persisted_system_root() {
    let storage = Arc::new(TestStorage::default());
    let seed = Node::new_durable(storage.clone(), PermissiveAgent::new());
    seed.system.create().await.unwrap();
    let root = seed.system.root().unwrap();
    drop(seed);

    let table = storage.table(&root.payload.collection);
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *table.hold_fetch.lock().unwrap() = Some((entered, release_rx));
    let node = Node::new_durable(storage, PermissiveAgent::new());
    entered_rx.await.unwrap();
    let presence = node.presence();
    tokio::pin!(presence);
    assert!(futures::poll!(&mut presence).is_pending());
    release.send(()).unwrap();
    let presence = tokio::time::timeout(std::time::Duration::from_secs(2), presence).await.unwrap().unwrap();
    assert_eq!(presence.node_id, node.id);
    assert!(presence.durable);
    assert_eq!(presence.system_root, Some(root));
}

#[tokio::test]
async fn disconnect_releases_pending_requests() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let peer = proto::EntityId::random();
    let (sent, _messages) = std::sync::mpsc::channel();
    node.register_peer(presence(peer), Box::new(RecordingSender(peer, sent))).await.unwrap();
    let request = node.request(peer, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { collection: "test".into(), ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());

    node.deregister_peer(peer);
    assert!(matches!(futures::poll!(&mut request), std::task::Poll::Ready(Err(RequestError::InternalChannelClosed))));
}

#[tokio::test]
async fn an_inflight_read_can_finish_after_halt_but_new_work_is_rejected() {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.system.create().await.unwrap();
    node.wait_ready().await.unwrap();
    let transaction = node.privileged_context().begin();
    let id = transaction.create(&crate::schema::catalog::SysModelRow { label: "test".into(), name: "Test".into() }).await.unwrap().id();
    transaction.commit().await.unwrap();
    let peer = proto::EntityId::random();
    node.register_peer(presence(peer), Box::new(ClosedSender(peer))).await.unwrap();

    let collection = CollectionId::fixed_name(crate::schema::MODEL_COLLECTION_ID);
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *storage.table(&collection).hold_fetch.lock().unwrap() = Some((entered, release_rx));
    let selection = ankql::ast::Selection::<Resolved> { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    let read = node.fetch_entities_from_local(&collection, &selection);
    tokio::pin!(read);
    assert!(futures::poll!(&mut read).is_pending());
    entered_rx.await.unwrap();

    let reason = node.system.halt(NodeHaltReason::SystemLoad("fixture".into()));
    release.send(()).unwrap();
    assert_eq!(read.await.unwrap().iter().map(Entity::id).collect::<Vec<_>>(), vec![id]);
    assert!(
        matches!(node.fetch_entities_from_local(&collection, &selection).await, Err(RetrievalError::NodeHalted(error)) if error == reason)
    );
    assert!(matches!(
        node.request(peer, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { collection: collection.clone(), ids: vec![id] }).await,
        Err(RequestError::NodeHalted(error)) if error == reason
    ));
    assert!(node.handle_message(proto::NodeMessage::UnsubscribeQuery { from: peer, query_id: proto::QueryId::new() }).await.is_err());
}
