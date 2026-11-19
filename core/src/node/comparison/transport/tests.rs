use super::*;
use crate::{connector::{PeerSender, SendError}, policy::{PermissiveAgent, DEFAULT_CONTEXT}, test_utils::TestStorage};
use std::{sync::mpsc, time::Duration};

#[derive(Clone)]
struct Sender(EntityId, mpsc::Sender<proto::NodeMessage>);
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.1.send(message).map_err(|_| SendError::ConnectionClosed) }
    fn recipient_node_id(&self) -> EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn fixture() -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, EntityId, mpsc::Receiver<proto::NodeMessage>)> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let peer = EntityId::random();
    let (sent, messages) = mpsc::channel();
    node.register_peer(proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION }, Box::new(Sender(peer, sent))).await?;
    Ok((node, peer, messages))
}

#[test]
fn simultaneous_local_admission_obeys_one_capacity_bound() {
    let registry = Arc::new(IncomingComparisons::default());
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let workers: Vec<_> = (0..16).map(|_| {
        let registry = registry.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            registry.admit(ComparisonId::new(), vec![DEFAULT_CONTEXT], ComparisonLimits { max_captures: 3, ..Default::default() }).is_ok()
        })
    }).collect();
    assert_eq!(workers.into_iter().map(|worker| worker.join().unwrap() as usize).sum::<usize>(), 3);
    assert_eq!(registry.0.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn delivery_before_reply_is_admitted_once_and_cancelled_requests_leave_no_registration() -> anyhow::Result<()> {
    for cancel in [false, true] {
        let (node, peer, messages) = fixture().await?;
        let id = ComparisonId::new();
        let connection = node.peer_connections.get(&peer).unwrap();
        connection.comparisons.admit(id, vec![DEFAULT_CONTEXT], ComparisonLimits::default())?;
        let selection = ankql::ast::Predicate::False.into();
        let members = vec![proto::RepairMember { known: proto::KnownEntity { entity_id: EntityId::random(), head: Default::default() }, key: None }];
        let credentials = vec![DEFAULT_CONTEXT];
        let mut request = Box::pin(node.request_comparison_repair(peer, id, &selection, members, false, &credentials));
        assert!(futures::poll!(&mut request).is_pending());
        let proto::NodeMessage::Request { request: sent, .. } = messages.try_recv()? else { panic!("expected request") };
        let proto::NodeRequestBody::Compare { request: proto::ComparisonRequest::Repair { delivery, .. }, .. } = sent.body else { panic!("expected repair") };
        let incoming = connection.comparisons.get(&id).unwrap();
        assert_eq!(incoming.deliveries.len(), 1);
        if cancel {
            drop(request);
            assert!(incoming.deliveries.is_empty());
            assert!(connection.pending_requests.is_empty());
            assert!(node.receive_comparison_repair(peer, id, delivery, vec![]).await.is_err());
        } else {
            node.receive_comparison_repair(peer, id, delivery.clone(), vec![]).await?;
            assert!(node.receive_comparison_repair(peer, id, delivery.clone(), vec![]).await.is_err());
            node.handle_message(proto::NodeMessage::Response(proto::NodeResponse {
                from: peer, to: node.id, request_id: sent.id,
                body: proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Repaired { delivery, unavailable: vec![] })),
            })).await?;
            let receipt = request.await?;
            assert_eq!(receipt.events, 0);
            assert_eq!(receipt.bytes, bincode::serialized_size(&Vec::<proto::EntityDelta>::new())? as usize);
            assert!(connection.pending_requests.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn unrequested_entities_and_deliveries_never_reach_application() -> anyhow::Result<()> {
    let (node, peer, _) = fixture().await?;
    let id = ComparisonId::new();
    let delivery = proto::UpdateId::new();
    assert!(node.receive_comparison_repair(peer, id, delivery.clone(), vec![]).await.is_err());
    let connection = node.peer_connections.get(&peer).unwrap();
    connection.comparisons.admit(id, vec![DEFAULT_CONTEXT], ComparisonLimits::default())?;
    let incoming = connection.comparisons.get(&id).unwrap();
    let (done, received) = oneshot::channel();
    incoming.deliveries.insert(delivery.clone(), Delivery { entities: [EntityId::random()].into_iter().collect(), done });
    let foreign = EntityId::random();
    let delta = proto::EntityDelta { entity_id: foreign, content: proto::DeltaContent::StateSnapshot { state: proto::StateFragment { state: Default::default(), attestations: Default::default() } } };
    assert!(node.receive_comparison_repair(peer, id, delivery, vec![delta]).await.is_err());
    assert!(received.await?.is_err());
    assert!(matches!(node.storage.get_state(foreign).await, Err(RetrievalError::EntityNotFound(_))));
    Ok(())
}

#[tokio::test]
async fn cancelling_initial_capture_releases_local_admission_before_any_request() -> anyhow::Result<()> {
    let (node, peer, messages) = fixture().await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let (entered, entering) = oneshot::channel();
    let (_resume, blocked) = oneshot::channel();
    *node.storage.hold_state_snapshot.lock().unwrap() = Some((entered, blocked));
    let owner = node.clone();
    let task = tokio::spawn(async move { owner.reconcile_query(peer, ankql::ast::Predicate::False.into(), &vec![DEFAULT_CONTEXT], None).await });
    tokio::time::timeout(Duration::from_secs(2), entering).await??;
    assert_eq!(connection.comparisons.0.lock().unwrap().len(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(connection.comparisons.0.lock().unwrap().is_empty());
    assert!(matches!(messages.try_recv()?, proto::NodeMessage::ReleaseComparison { .. }));
    assert!(messages.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn replacement_connection_rejects_old_deliveries_and_frees_pending_requests() -> anyhow::Result<()> {
    let (node, peer, _) = fixture().await?;
    let old = node.peer_connections.get(&peer).unwrap();
    let id = ComparisonId::new();
    old.comparisons.admit(id, vec![DEFAULT_CONTEXT], ComparisonLimits::default())?;
    let (done, waiting) = oneshot::channel();
    old.pending_requests.insert(proto::RequestId::new(), done);
    let (sent, _messages) = mpsc::channel();
    node.register_peer(proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION }, Box::new(Sender(peer, sent))).await?;
    assert!(waiting.await.is_err());
    assert!(old.comparisons.0.lock().unwrap().is_empty());
    assert!(node.receive_comparison_repair(peer, id, proto::UpdateId::new(), vec![]).await.is_err());
    Ok(())
}
