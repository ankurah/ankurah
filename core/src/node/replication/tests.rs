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
struct Sender(mpsc::UnboundedSender<proto::NodeMessage>);
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.0.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn fixture() -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, NodeId, mpsc::UnboundedReceiver<proto::NodeMessage>)> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let key = crate::node::NodeKey::generate();
    let peer = key.public_key().node_id();
    let (sent, messages) = mpsc::unbounded_channel();
    crate::test_utils::connect_peer(&node, &key, true, Sender(sent)).await?;
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
    peer: NodeId,
    request: proto::NodeRequest,
    body: proto::NodeResponseBody,
) -> anyhow::Result<()> {
    node.handle_message(
        node.connections_of(peer)[0],
        proto::NodeMessage::Response(proto::NodeResponse { from: peer, to: node.id().unwrap(), request_id: request.id, body }),
    )
    .await
}

#[tokio::test]
async fn only_the_last_handle_cancels_an_in_flight_comparison() -> anyhow::Result<()> {
    let (node, peer, mut messages) = fixture().await?;
    let handle = node.replicate_from(peer, ankql::ast::Predicate::False.into(), DEFAULT_CONTEXT)?;
    let clone = handle.clone();
    let first = request(receive(&mut messages).await?);
    let proto::NodeRequestBody::Fetch { comparison, .. } = first.body else { unreachable!() };
    let connection = node.peer_connections.get(&node.connections_of(peer)[0]).unwrap();
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
    let connection = node.peer_connections.get(&node.connections_of(peer)[0]).unwrap();
    assert_eq!(connection.pending_requests.len(), 1);
    assert!(!connection.pending_requests.contains_key(&first.id));
    Ok(())
}

#[tokio::test]
async fn a_retained_interest_does_not_keep_an_unprovisioned_node_alive() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let weak = node.weak();
    let handle =
        node.replicate_from(crate::node::NodeKey::generate().public_key().node_id(), ankql::ast::Predicate::True.into(), DEFAULT_CONTEXT)?;
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
    assert!(node.peer_connections.get(&node.connections_of(peer)[0]).unwrap().pending_requests.is_empty());
    Ok(())
}

#[tokio::test]
async fn retry_rejects_a_finished_sweeps_late_success_or_refusal() -> anyhow::Result<()> {
    let progress =
        Arc::new(Progress { state: Mut::new(ReplicationState::Comparing), revision: AtomicU64::new(0), wake: tokio::sync::Notify::new() });
    let handle = Replication(Arc::new(Inner { progress: progress.clone(), cancel: CancelFlag::default() }));
    let old = progress.revision.load(Ordering::Relaxed);
    // The worker has selected a result, but retry arrives before publication.
    handle.retry();
    let mut waiting = Box::pin(handle.wait_caught_up());
    assert!(futures::poll!(&mut waiting).is_pending());
    assert!(!progress.publish(old, ReplicationState::CaughtUp(KnownState::empty())));
    assert!(!progress.publish(old, ReplicationState::Failed(RetrievalError::storage(ComparisonFailure::LimitExceeded))));
    assert!(matches!(handle.state().value(), ReplicationState::Connecting));
    assert!(futures::poll!(&mut waiting).is_pending(), "an obsolete completion must not satisfy the fresh wait");

    let current = progress.revision.load(Ordering::Relaxed);
    assert!(progress.publish(current, ReplicationState::CaughtUp(KnownState::empty())));
    assert_eq!(waiting.await?, KnownState::empty());

    // Signal listeners may synchronously request another sweep.
    let listener_handle = handle.clone();
    let _listener = handle.state().subscribe(move |state| {
        if matches!(state, ReplicationState::Comparing) {
            listener_handle.retry();
        }
    });
    assert!(progress.publish(current, ReplicationState::Comparing));
    assert!(matches!(handle.state().value(), ReplicationState::Connecting));
    assert!(!progress.publish(current, ReplicationState::CaughtUp(KnownState::empty())));
    Ok(())
}

#[tokio::test]
async fn a_newer_ephemeral_connection_with_the_same_key_does_not_displace_a_durable_source() -> anyhow::Result<()> {
    let (node, _, _) = fixture().await?;
    let key = crate::node::NodeKey::generate();
    let peer = key.public_key().node_id();
    let (sent, mut durable_messages) = mpsc::unbounded_channel();
    let durable = crate::test_utils::connect_peer(&node, &key, true, Sender(sent)).await?;
    let (sent, mut ephemeral_messages) = mpsc::unbounded_channel();
    crate::test_utils::connect_peer(&node, &key, false, Sender(sent)).await?;
    let handle = node.replicate_from(peer, ankql::ast::Predicate::False.into(), DEFAULT_CONTEXT)?;
    let request = request(receive(&mut durable_messages).await?);
    let proto::NodeRequestBody::Fetch { comparison, key_spec, known_state, .. } = request.body else { unreachable!() };
    node.handle_message(
        durable,
        proto::NodeMessage::Response(proto::NodeResponse {
            from: peer,
            to: node.id().unwrap(),
            request_id: request.id,
            body: proto::NodeResponseBody::Fetch(proto::ComparisonTarget { id: comparison, key_spec, known_state, page: None }),
        }),
    )
    .await?;
    assert_eq!(handle.wait_caught_up().await?, KnownState::empty());
    assert!(ephemeral_messages.try_recv().is_err());
    Ok(())
}
