use super::*;
use crate::{
    connector::{PeerSender, SendError},
    policy::PermissiveAgent,
    test_utils::TestStorage,
};

#[derive(Clone)]
struct DiscardSender(proto::EntityId);
impl PeerSender for DiscardSender {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn recipient_node_id(&self) -> proto::EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[tokio::test]
async fn expiry_while_close_waits_cannot_acknowledge_a_removed_comparison() -> anyhow::Result<()> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let peer = proto::EntityId::random();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(DiscardSender(peer)),
    )
    .await?;
    let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True);
    let mut snapshot = super::super::QueryTree::default().refresh(spec, vec![]).await?;
    let known = Some(snapshot.offer().await?);
    let state = node.start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot, known).await?;
    let comparison = state.page.comparison;
    let connection = node.peer_connections.get(&peer).unwrap();
    let session = connection.comparisons.get(&comparison).unwrap();
    let lock = session.state.lock().await;
    let credentials: Vec<<PermissiveAgent as PolicyAgent>::ContextData> = vec![];
    let close = node.handle_request(
        &credentials,
        proto::NodeRequest {
            id: proto::RequestId::new(),
            from: peer,
            to: node.id,
            body: proto::NodeRequestBody::CloseComparison { comparison, outcome: proto::ComparisonOutcome::Applied },
        },
    );
    tokio::pin!(close);
    assert!(futures::poll!(&mut close).is_pending());
    node.expire_comparison(peer, comparison).await?;
    drop(lock);
    assert!(matches!(close.await?, proto::NodeResponseBody::ComparisonUnavailable { comparison: id } if id == comparison));
    assert!(!connection.comparisons.was_closed(&comparison));
    Ok(())
}
