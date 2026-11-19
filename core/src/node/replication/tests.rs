use super::*;
use crate::{
    connector::{PeerSender, SendError},
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    session::Session,
    test_utils::TestStorage,
};
use ankurah_proto as proto;
use tokio::sync::mpsc;

#[derive(Clone)]
struct Sender(EntityId, mpsc::UnboundedSender<proto::NodeMessage>);
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.1.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn recipient_node_id(&self) -> EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn fixture() -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, EntityId, mpsc::UnboundedReceiver<proto::NodeMessage>)> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let peer = EntityId::random();
    let (sent, messages) = mpsc::unbounded_channel();
    let mut presence = node.presence().await?;
    presence.node_id = peer;
    node.register_peer(presence, Box::new(Sender(peer, sent))).await?;
    Ok((node, peer, messages))
}

async fn receive(messages: &mut mpsc::UnboundedReceiver<proto::NodeMessage>) -> anyhow::Result<proto::NodeMessage> {
    tokio::time::timeout(Duration::from_secs(3), messages.recv()).await?.ok_or_else(|| anyhow::anyhow!("peer channel closed"))
}

fn request(message: proto::NodeMessage) -> proto::NodeRequest {
    let proto::NodeMessage::Request { request, .. } = message else { panic!("expected request") };
    assert!(matches!(request.body, proto::NodeRequestBody::Fetch { .. }));
    request
}

async fn answer(
    node: &Node<TestStorage, PermissiveAgent>,
    peer: EntityId,
    request: proto::NodeRequest,
    body: proto::NodeResponseBody,
) -> anyhow::Result<()> {
    node.handle_message(proto::NodeMessage::Response(proto::NodeResponse { from: peer, to: node.id, request_id: request.id, body })).await
}

#[tokio::test]
async fn only_the_last_handle_cancels_an_in_flight_comparison() -> anyhow::Result<()> {
    let (node, peer, mut messages) = fixture().await?;
    let handle = node.replicate_from(peer, ankql::ast::Predicate::False.into(), DEFAULT_CONTEXT)?;
    let clone = handle.clone();
    let first = request(receive(&mut messages).await?);
    let proto::NodeRequestBody::Fetch { comparison, .. } = first.body else { unreachable!() };
    let connection = node.peer_connections.get(&peer).unwrap();
    assert_eq!(connection.pending_requests.len(), 1);
    drop(handle);
    tokio::task::yield_now().await;
    assert_eq!(connection.pending_requests.len(), 1, "the remaining clone keeps replication active");
    drop(clone);
    assert!(
        matches!(receive(&mut messages).await?, proto::NodeMessage::ReleaseComparison { comparison: released, .. } if released == comparison)
    );
    assert!(connection.pending_requests.is_empty());
    assert!(messages.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn a_limit_refusal_waits_for_explicit_retry_instead_of_renewing_its_budget_forever() -> anyhow::Result<()> {
    let (node, peer, mut messages) = fixture().await?;
    let handle = node.replicate_from(peer, ankql::ast::Predicate::False.into(), DEFAULT_CONTEXT)?;
    let first = request(receive(&mut messages).await?);
    answer(&node, peer, first, proto::NodeResponseBody::Compared(Err(ComparisonFailure::LimitExceeded))).await?;
    assert!(matches!(handle.wait_caught_up().await, Err(RetrievalError::StorageError(error))
        if error.downcast_ref::<ComparisonFailure>() == Some(&ComparisonFailure::LimitExceeded)));
    assert!(matches!(receive(&mut messages).await?, proto::NodeMessage::ReleaseComparison { .. }));
    assert!(tokio::time::timeout(Duration::from_millis(1100), messages.recv()).await.is_err());

    handle.retry();
    let next = request(receive(&mut messages).await?);
    let proto::NodeRequestBody::Fetch { comparison, ref key_spec, ref known_state, .. } = next.body else { unreachable!() };
    let body = proto::NodeResponseBody::Fetch(proto::ComparisonTarget {
        id: comparison,
        key_spec: key_spec.clone(),
        known_state: known_state.clone(),
        page: None,
    });
    answer(&node, peer, next, body).await?;
    assert_eq!(handle.wait_caught_up().await?, KnownState::empty());
    Ok(())
}

#[tokio::test]
async fn changing_sessions_cancels_the_old_request_before_sending_another() -> anyhow::Result<()> {
    let (node, peer, mut messages) = fixture().await?;
    let sessions = SessionSet::new();
    let _handle = node.replicate_from(peer, ankql::ast::Predicate::False.into(), sessions.clone())?;
    let first = request(receive(&mut messages).await?);
    let proto::NodeRequestBody::Fetch { comparison: old, .. } = first.body else { unreachable!() };
    sessions.own(&Session::new(DEFAULT_CONTEXT));
    assert!(matches!(receive(&mut messages).await?, proto::NodeMessage::ReleaseComparison { comparison, .. } if comparison == old));
    let second = request(receive(&mut messages).await?);
    assert!(matches!(second.body, proto::NodeRequestBody::Fetch { comparison, .. } if comparison != old));
    let connection = node.peer_connections.get(&peer).unwrap();
    assert_eq!(connection.pending_requests.len(), 1);
    assert!(!connection.pending_requests.contains_key(&first.id));
    Ok(())
}

#[tokio::test]
async fn a_retained_interest_does_not_keep_an_unprovisioned_node_alive() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let weak = node.weak();
    let handle = node.replicate_from(EntityId::random(), ankql::ast::Predicate::True.into(), DEFAULT_CONTEXT)?;
    tokio::task::yield_now().await;
    drop(node);
    assert!(matches!(tokio::time::timeout(Duration::from_secs(1), handle.wait_caught_up()).await?, Err(RetrievalError::NodeDropped(_))));
    assert!(weak.upgrade().is_none());
    handle.retry();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(1), handle.wait_caught_up()).await?, Err(RetrievalError::NodeDropped(_))));
    Ok(())
}

#[tokio::test]
async fn unavailable_required_history_stops_after_bounded_capture_retries() -> anyhow::Result<()> {
    let (node, peer, mut messages) = fixture().await?;
    let handle = node.replicate_from(peer, ankql::ast::Predicate::True.into(), DEFAULT_CONTEXT)?;
    for _ in 0..3 {
        let next = request(receive(&mut messages).await?);
        answer(&node, peer, next, proto::NodeResponseBody::Compared(Err(ComparisonFailure::MemberUnavailable))).await?;
        assert!(matches!(receive(&mut messages).await?, proto::NodeMessage::ReleaseComparison { .. }));
    }
    assert!(matches!(handle.wait_caught_up().await, Err(RetrievalError::StorageError(error))
        if error.downcast_ref::<ComparisonFailure>() == Some(&ComparisonFailure::MemberUnavailable)));
    assert!(tokio::time::timeout(Duration::from_millis(1100), messages.recv()).await.is_err());
    assert!(node.peer_connections.get(&peer).unwrap().pending_requests.is_empty());
    Ok(())
}
