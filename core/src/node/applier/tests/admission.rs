//! Which peers' pushes the applier admits: a peer we hold a standing query
//! with, or one we have a Fetch in flight with.

use super::*;
use crate::connector::{PeerSender, SendError};

#[derive(Clone)]
struct Silent(EntityId);

impl PeerSender for Silent {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn recipient_node_id(&self) -> EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

fn genesis(value: &str) -> anyhow::Result<Event> {
    Ok(Event::genesis(
        None,
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(model_a())), value_operation(value)?]),
    ))
}

fn push(event: &Event) -> Vec<proto::SubscriptionUpdateItem> {
    vec![proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        content: proto::UpdateContent::EventOnly(vec![proto::Attested::from(event.clone()).into()]),
        predicate_relevance: vec![],
    }]
}

#[tokio::test]
async fn a_fetch_in_flight_admits_its_peers_pushes_until_it_finishes() -> anyhow::Result<()> {
    // An ephemeral node holding no standing query with the peer: only a Fetch admits its pushes.
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    let peer = EntityId::random();
    let presence = proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION };
    node.register_peer(presence, Box::new(Silent(peer))).await?;
    let (before, during, after) = (genesis("before")?, genesis("during")?, genesis("after")?);

    assert!(NodeApplier::apply_updates(&node, &peer, push(&before)).await.is_err(), "a push nobody asked for is refused");
    let fetch = node.peer_connections.get(&peer).unwrap().fetches.register(vec![]);
    NodeApplier::apply_updates(&node, &peer, push(&during)).await?;
    drop(fetch);
    assert!(NodeApplier::apply_updates(&node, &peer, push(&after)).await.is_err(), "a push after the Fetch finished is refused");

    let stored = |event: &Event| node.storage.get_state(event.entity_id);
    assert!(stored(&before).await.is_err() && stored(&after).await.is_err());
    assert!(stored(&during).await.is_ok(), "the push that arrived while the Fetch was in flight applied");
    Ok(())
}
