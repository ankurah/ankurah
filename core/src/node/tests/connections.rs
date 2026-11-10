use super::{register_keyed_peer, RecordingSender};
use crate::connector::PeerProvenance;
use crate::entity::Entity;
use crate::error::ValidationError;
use crate::node::{Node, NodeKey};
use crate::policy::{AccessDenied, AttestationRequest, DefaultContext, PeerTrust, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT};
use crate::storage::StorageEngine;
use crate::test_utils::TestStorage;
use crate::util::Iterable;
use ankql::ast::{Predicate, Resolved};
use ankurah_proto as proto;
use std::sync::{mpsc, Arc, Mutex};
use tokio::sync::oneshot;

/// A request from `from` to `node` that any registered peer may make.
fn get_request(node: proto::NodeId, from: proto::NodeId) -> proto::NodeMessage {
    let body = proto::CarriedBody::encode(&proto::NodeRequestBody::Get { ids: vec![] });
    proto::NodeMessage::Request {
        auth: vec![proto::AuthData(vec![])],
        request: proto::NodeRequest { id: proto::RequestId::new(), to: node, from, body },
    }
}

/// A registered peer cannot pass a frame off as another peer's: a frame of any kind that names a
/// sender other than its connection's peer is refused, and that connection stays up.
#[tokio::test]
async fn a_frame_naming_a_sender_other_than_its_connections_peer_is_refused_and_the_connection_stays_up() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let (liar, other) = (NodeKey::generate(), NodeKey::generate());
    let (liar_sent, liar_frames) = mpsc::channel();
    let (other_sent, other_frames) = mpsc::channel();
    let liar_connection =
        register_keyed_peer(&node, &liar, false, PeerProvenance::Inbound, Box::new(RecordingSender(liar_sent))).await.unwrap();
    register_keyed_peer(&node, &other, false, PeerProvenance::Inbound, Box::new(RecordingSender(other_sent))).await.unwrap();
    let request = node.request(other.node_id(), &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    let proto::NodeMessage::Request { request: sent, .. } = other_frames.try_recv().unwrap() else { panic!("the request was not sent") };

    let in_others_name = [
        get_request(node.id, other.node_id()),
        proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: other.node_id(),
            to: node.id,
            body: proto::CarriedBody::encode(&proto::NodeUpdateBody::SubscriptionUpdate { items: vec![] }),
        }),
        proto::NodeMessage::Response(proto::NodeResponse {
            request_id: sent.id,
            from: other.node_id(),
            to: node.id,
            body: proto::CarriedBody::encode(&proto::NodeResponseBody::Get(vec![])),
        }),
        proto::NodeMessage::UnsubscribeQuery { from: other.node_id(), query_id: proto::QueryId::new() },
    ];
    for forged in in_others_name {
        node.handle_message(liar_connection, forged).await.unwrap();
    }
    assert!(liar_frames.try_recv().is_err() && other_frames.try_recv().is_err(), "neither peer was answered");
    assert!(futures::poll!(&mut request).is_pending(), "the forged response fulfilled nothing");

    node.handle_message(liar_connection, get_request(node.id, liar.node_id())).await.unwrap();
    assert!(matches!(liar_frames.try_recv(), Ok(proto::NodeMessage::Response(_))), "the liar's own frames are still served");
}

/// `peer`'s answer to `request`, which this node sent it.
fn answer(request: &proto::NodeRequest) -> proto::NodeMessage {
    let body = proto::CarriedBody::encode(&proto::NodeResponseBody::Get(vec![]));
    proto::NodeMessage::Response(proto::NodeResponse { request_id: request.id.clone(), from: request.to, to: request.from, body })
}

/// The request in `frames`, the next frame sent on a connection.
fn sent_request(frames: &mpsc::Receiver<proto::NodeMessage>) -> proto::NodeRequest {
    match frames.try_recv() {
        Ok(proto::NodeMessage::Request { request, .. }) => request,
        other => panic!("no request was sent on this connection: {other:?}"),
    }
}

/// Two connections of one node, as two browser tabs sharing a key make: each is served, each
/// receives the answers to its own requests, and removing one leaves the other as it was.
#[tokio::test]
async fn two_registrations_of_one_id_each_receive_their_own_answers_and_removing_one_leaves_the_other() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (first_sent, first_frames) = mpsc::channel();
    let (second_sent, second_frames) = mpsc::channel();
    let first = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(first_sent))).await.unwrap();
    let second = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(second_sent))).await.unwrap();

    for (registration, frames, other_frames) in [(first, &first_frames, &second_frames), (second, &second_frames, &first_frames)] {
        node.handle_message(registration, get_request(node.id, key.node_id())).await.unwrap();
        assert!(matches!(frames.try_recv(), Ok(proto::NodeMessage::Response(_))), "answered on the connection it arrived on");
        assert!(other_frames.try_recv().is_err(), "and on no other");
    }

    // A request of this node's goes out on the newest registration, and only its answer there fulfils it.
    let request = node.request(key.node_id(), &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    let sent = sent_request(&second_frames);
    assert!(first_frames.try_recv().is_err());
    node.handle_message(first, answer(&sent)).await.unwrap();
    assert!(futures::poll!(&mut request).is_pending(), "an answer on another connection fulfils nothing");
    node.handle_message(second, answer(&sent)).await.unwrap();
    assert!(matches!(request.await, Ok(proto::NodeResponseBody::Get(_))));

    node.deregister_peer(first);
    node.handle_message(first, get_request(node.id, key.node_id())).await.unwrap();
    assert!(first_frames.try_recv().is_err(), "a removed registration's frames are refused");
    node.handle_message(second, get_request(node.id, key.node_id())).await.unwrap();
    assert!(matches!(second_frames.try_recv(), Ok(proto::NodeMessage::Response(_))), "the other is still served");
    assert_eq!(node.granted_trust(&key.node_id()), Some(PeerTrust { durable: false }));
}

/// A configured durable server that also connects inbound, as one durable server dialing another
/// does, keeps its durable standing through the connection this node dialed, which durable
/// requests go out on.
#[tokio::test]
async fn an_inbound_registration_of_a_configured_peers_id_leaves_the_configured_one_durable_and_usable() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (configured_sent, configured_frames) = mpsc::channel();
    let (inbound_sent, inbound_frames) = mpsc::channel();
    let configured_sender = Box::new(RecordingSender(configured_sent));
    let configured = register_keyed_peer(&node, &key, true, PeerProvenance::Configured, configured_sender).await.unwrap();
    register_keyed_peer(&node, &key, true, PeerProvenance::Inbound, Box::new(RecordingSender(inbound_sent))).await.unwrap();

    assert_eq!(node.get_durable_peers(), vec![key.node_id()]);
    assert_eq!(node.granted_trust(&key.node_id()), Some(PeerTrust { durable: true }));
    assert!(node.subscription_relay.as_ref().unwrap().is_connected(&key.node_id()));
    let request = node.request_durable(key.node_id(), &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    let sent = sent_request(&configured_frames);
    assert!(inbound_frames.try_recv().is_err(), "nothing went out on the inbound connection");
    node.handle_message(configured, answer(&sent)).await.unwrap();
    assert!(matches!(request.await, Ok(proto::NodeResponseBody::Get(_))));
}

/// A peer is durable while one of its registrations is trusted as durable: the durable set and
/// the subscription relay change with its first durable registration and with its last, and with
/// nothing between.
#[tokio::test]
async fn the_durable_set_and_the_relay_follow_only_a_peers_first_and_last_durable_registration() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let relay = node.subscription_relay.as_ref().unwrap();
    let key = NodeKey::generate();
    let peer = key.node_id();
    let register = |provenance| {
        let (sent, _) = mpsc::channel();
        register_keyed_peer(&node, &key, true, provenance, Box::new(RecordingSender(sent)))
    };
    let durable = || (node.get_durable_peers() == vec![peer], relay.is_connected(&peer));

    assert_eq!(durable(), (false, false));
    let first = register(PeerProvenance::Configured).await.unwrap();
    assert_eq!(durable(), (true, true));
    let inbound = register(PeerProvenance::Inbound).await.unwrap();
    assert_eq!(durable(), (true, true));
    let second = register(PeerProvenance::Configured).await.unwrap();
    assert_eq!(durable(), (true, true));
    node.deregister_peer(first);
    assert_eq!(durable(), (true, true));
    node.deregister_peer(inbound);
    assert_eq!(durable(), (true, true));
    assert_eq!(node.granted_trust(&peer), Some(PeerTrust { durable: true }));
    node.deregister_peer(second);
    assert_eq!(durable(), (false, false));
    assert_eq!(node.granted_trust(&peer), None);
}

/// A permissive agent whose request check, once armed, signals that it has begun and waits until released.
#[derive(Clone)]
struct RequestHoldingAgent(Arc<Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>>);

#[async_trait::async_trait]
impl PolicyAgent for RequestHoldingAgent {
    type ContextData = &'static DefaultContext;

    async fn check_request<SE: StorageEngine, A: Iterable<proto::AuthData> + Send + Sync>(
        &self,
        _: &Node<SE, Self>,
        _: &A,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError> {
        let hold = self.0.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        Ok(vec![DEFAULT_CONTEXT])
    }

    fn sign_request<SE: StorageEngine, C: Iterable<Self::ContextData>>(
        &self,
        _: &crate::node::NodeInner<SE, Self>,
        _: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied> {
        Ok(vec![])
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &Entity,
        _: &Entity,
        _: &proto::Event,
    ) -> Result<Option<AttestationRequest>, AccessDenied> {
        Ok(None)
    }

    fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> { Ok(Predicate::True) }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn request_state_attestation<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<AttestationRequest> {
        None
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// A subscription request runs on the connection it arrived on, even when its peer registers
/// again while the request is being checked: its query stands on that connection, not the newer one.
#[tokio::test]
async fn a_subscription_whose_peer_registers_again_during_its_request_check_stands_on_the_connection_it_arrived_on() {
    let hold = Arc::new(Mutex::new(None));
    let node = Node::new_durable(Arc::new(TestStorage::default()), RequestHoldingAgent(hold.clone()));
    node.wait_ready().await.unwrap();
    let key = NodeKey::generate();
    let (old_sent, old_frames) = mpsc::channel();
    let old = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(old_sent))).await.unwrap();

    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *hold.lock().unwrap() = Some((entered, release_rx));
    let query_id = proto::QueryId::new();
    let selection = ankql::ast::Selection::<Resolved> {
        predicate: Predicate::MemberOf(proto::ModelId::EntityId(proto::EntityId::from_bytes([7; 32]))),
        order_by: None,
        limit: None,
    };
    let body =
        proto::CarriedBody::encode(&proto::NodeRequestBody::SubscribeQuery { query_id, selection, version: 1, known_matches: vec![] });
    let request = proto::NodeRequest { id: proto::RequestId::new(), to: node.id, from: key.node_id(), body };
    let handling = tokio::spawn({
        let node = node.clone();
        async move { node.handle_message(old, proto::NodeMessage::Request { auth: vec![proto::AuthData(vec![])], request }).await }
    });
    entered_rx.await.unwrap();
    let (new_sent, _new_frames) = mpsc::channel();
    let new = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(new_sent))).await.unwrap();
    release.send(()).unwrap();
    handling.await.unwrap().unwrap();

    assert!(
        old_frames.try_iter().any(|frame| matches!(frame, proto::NodeMessage::Response(_))),
        "answered on the connection it arrived on"
    );
    let new_connection = node.peers.registered(&new).unwrap();
    assert!(new_connection.subscription_handler.remove_predicate(query_id).await.is_err(), "the newer connection holds no such query");
    assert!(
        node.peers.registered(&old).unwrap().subscription_handler.remove_predicate(query_id).await.is_ok(),
        "the query stands on the connection it arrived on"
    );
}

/// A request from `from` to `node` subscribing `query_id` to every model definition.
fn subscribe_to_models(node: proto::NodeId, from: proto::NodeId, query_id: proto::QueryId) -> proto::NodeMessage {
    let selection = ankql::ast::Selection::<Resolved> {
        predicate: Predicate::MemberOf(proto::ModelId::System(proto::SystemModel::Model)),
        order_by: None,
        limit: None,
    };
    let body =
        proto::CarriedBody::encode(&proto::NodeRequestBody::SubscribeQuery { query_id, selection, version: 1, known_matches: vec![] });
    proto::NodeMessage::Request {
        auth: vec![proto::AuthData(vec![])],
        request: proto::NodeRequest { id: proto::RequestId::new(), to: node, from, body },
    }
}

/// The queries the subscription updates in `frames` name, in the order sent.
fn updated_queries(frames: &mpsc::Receiver<proto::NodeMessage>) -> Vec<proto::QueryId> {
    let mut queries = Vec::new();
    for frame in frames.try_iter() {
        if let proto::NodeMessage::Update(update) = frame {
            let Ok(proto::NodeUpdateBody::SubscriptionUpdate { items }) = update.body.decode() else {
                panic!("an update this build cannot read")
            };
            queries.extend(items.into_iter().flat_map(|item| item.predicate_relevance.into_iter().map(|(query_id, _)| query_id)));
        }
    }
    queries
}

/// Commit a new model definition on `node`, which every subscription to model definitions matches.
async fn define_a_model<SE, PA>(node: &Node<SE, PA>, name: &str)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let transaction = node.privileged_context().begin();
    transaction.create(&crate::schema::catalog::SysModelRow { label: name.into(), name: name.into() }).await.unwrap();
    transaction.commit().await.unwrap();
}

/// Each connection of one node receives the updates of the subscription it installed and no
/// other's; a subscription whose connection is removed sends nothing, on any connection.
#[tokio::test]
async fn a_subscriptions_updates_go_out_on_the_connection_that_installed_it_and_no_other() {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.wait_ready().await.unwrap();
    let key = NodeKey::generate();
    let (first_sent, first_frames) = mpsc::channel();
    let (second_sent, second_frames) = mpsc::channel();
    let first = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(first_sent))).await.unwrap();
    let second = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(second_sent))).await.unwrap();
    let (first_query, second_query) = (proto::QueryId::new(), proto::QueryId::new());
    for (registration, query_id, frames) in [(first, first_query, &first_frames), (second, second_query, &second_frames)] {
        node.handle_message(registration, subscribe_to_models(node.id, key.node_id(), query_id)).await.unwrap();
        assert!(matches!(frames.try_recv(), Ok(proto::NodeMessage::Response(_))), "subscribed on its own connection");
    }

    define_a_model(&node, "first").await;
    assert_eq!(updated_queries(&first_frames), vec![first_query]);
    assert_eq!(updated_queries(&second_frames), vec![second_query]);

    node.deregister_peer(first);
    define_a_model(&node, "second").await;
    assert_eq!(updated_queries(&first_frames), vec![]);
    assert_eq!(updated_queries(&second_frames), vec![second_query], "the removed connection's subscription sent nothing here");
}
