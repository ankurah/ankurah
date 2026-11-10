use super::{register_keyed_peer, RecordingSender};
use crate::connector::PeerProvenance;
use crate::entity::Entity;
use crate::error::ValidationError;
use crate::node::{Node, NodeKey};
use crate::policy::{AccessDenied, AttestationRequest, DefaultContext, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT};
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

/// A frame still arriving on a connection whose peer has since registered again elsewhere is
/// refused, not served as if it came on the newer connection.
#[tokio::test]
async fn a_frame_on_a_connection_its_peer_has_registered_again_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (old_sent, old_frames) = mpsc::channel();
    let (new_sent, new_frames) = mpsc::channel();
    let old = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(old_sent))).await.unwrap();
    let new = register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(new_sent))).await.unwrap();

    node.handle_message(old, get_request(node.id, key.node_id())).await.unwrap();
    assert!(old_frames.try_recv().is_err() && new_frames.try_recv().is_err());
    node.handle_message(new, get_request(node.id, key.node_id())).await.unwrap();
    assert!(matches!(new_frames.try_recv(), Ok(proto::NodeMessage::Response(_))));
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
    let old_connection = node.peers.registered(&old).unwrap();

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
        old_connection.subscription_handler.remove_predicate(query_id).await.is_ok(),
        "the query stands on the connection it arrived on"
    );
}
