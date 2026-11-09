//! A later build may send a request, a response or an update whose body is of a kind this build
//! does not know; the frame around it still decodes, and the node answers what it cannot read.

use super::{register_keyed_peer, RecordingSender};
use crate::connector::PeerProvenance;
use crate::error::RequestError;
use crate::node::{Node, NodeKey};
use crate::policy::{PermissiveAgent, DEFAULT_CONTEXT};
use crate::test_utils::TestStorage;
use ankurah_proto as proto;
use std::sync::{mpsc, Arc};

/// A carried body that is variant 99 of its kind, which no build defines yet.
fn unknown_kind<T>() -> proto::CarriedBody<T> { bincode::deserialize(&bincode::serialize(&vec![99u8, 0, 0, 0]).unwrap()).unwrap() }

/// A node with one registered peer holding only a key, whose frames it records.
async fn node_with_peer() -> (Node<TestStorage, PermissiveAgent>, proto::NodeId, mpsc::Receiver<proto::NodeMessage>) {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (sent, messages) = mpsc::channel();
    register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(key.node_id(), sent))).await.unwrap();
    (node, key.node_id(), messages)
}

#[tokio::test]
async fn a_request_whose_body_this_build_cannot_read_is_answered_unsupported() {
    let (node, peer, messages) = node_with_peer().await;
    let request = proto::NodeRequest { id: proto::RequestId::new(), to: node.id, from: peer, body: unknown_kind() };
    let request_id = request.id.clone();
    node.handle_message(proto::NodeMessage::Request { auth: vec![proto::AuthData(vec![])], request }).await.unwrap();

    let proto::NodeMessage::Response(response) = messages.try_recv().unwrap() else { panic!("the request was not answered") };
    assert_eq!(response.request_id, request_id);
    assert!(matches!(response.body.decode(), Ok(proto::NodeResponseBody::Unsupported)), "{:?}", response.body);
}

#[tokio::test]
async fn an_update_whose_body_this_build_cannot_read_is_acknowledged_unsupported() {
    let (node, peer, messages) = node_with_peer().await;
    let update = proto::NodeUpdate { id: proto::UpdateId::new(), from: peer, to: node.id, body: unknown_kind() };
    let update_id = update.id.clone();
    node.handle_message(proto::NodeMessage::Update(update)).await.unwrap();

    let proto::NodeMessage::UpdateAck(ack) = messages.try_recv().unwrap() else { panic!("the update was not acknowledged") };
    assert_eq!(ack.id, update_id);
    assert!(matches!(ack.body, proto::NodeUpdateAckBody::Unsupported), "{}", ack.body);
}

#[tokio::test]
async fn a_response_whose_body_this_build_cannot_read_fails_the_waiting_request() {
    let (node, peer, messages) = node_with_peer().await;
    let request = node.request(peer, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] });
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    let proto::NodeMessage::Request { request: sent, .. } = messages.try_recv().unwrap() else { panic!("the request was not sent") };

    let response = proto::NodeResponse { request_id: sent.id, from: peer, to: node.id, body: unknown_kind() };
    node.handle_message(proto::NodeMessage::Response(response)).await.unwrap();
    assert!(matches!(request.await, Err(RequestError::UnreadableResponse(_))));
}
