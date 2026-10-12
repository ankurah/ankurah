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

#[tokio::test]
async fn cached_transfers_fill_replies_without_increasing_history_work_per_reply() -> anyhow::Result<()> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True);
    let states = (0..200)
        .map(|_| proto::Attested {
            payload: proto::EntityState { entity_id: proto::EntityId::random(), state: Default::default() },
            attestations: Default::default(),
        })
        .collect::<Vec<_>>();
    for state in &states {
        node.storage.set_state(state.clone());
    }
    let earlier = proto::Clock::genesis(proto::EventId::from_bytes([1; 32]));
    for (outside_query, known, cap) in [(false, None, 128), (false, Some(earlier), 32), (true, None, 32)] {
        let snapshot =
            super::super::QueryTree::default().refresh(spec.clone(), if outside_query { vec![] } else { states.clone() }).await?;
        let work = states.iter().map(|state| Work::Repair(state.payload.entity_id, known.clone())).collect();
        let mut comparison = Comparison {
            snapshot,
            work,
            summaries: HashMap::new(),
            leaves: Vec::new(),
            heads: HashSet::new(),
            repaired: HashSet::new(),
            hints: HashMap::new(),
            complete: false,
            replay: None,
            seen: HashSet::new(),
        };
        let mut received = HashSet::new();
        while !comparison.complete {
            let reply = comparison.reply(&node, ComparisonId::new(), &vec![], false).await?;
            assert_eq!(reply.items.len(), cap.min(200 - received.len()));
            for item in reply.items {
                let StateItem::Repair { delta, .. } = item else { panic!("expected a repair") };
                assert!(received.insert(delta.entity_id), "repair repeated across pages");
            }
        }
        assert_eq!(received.len(), 200);
    }
    Ok(())
}
