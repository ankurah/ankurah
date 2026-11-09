use super::{register_keyed_peer, RecordingSender};
use crate::connector::{PeerConnectionError, PeerProvenance};
use crate::node::{Node, NodeKey};
use crate::policy::{PeerTrust, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT};
use crate::test_utils::TestStorage;
use ankurah_proto as proto;
use std::sync::{mpsc, Arc};

fn commit_relays(messages: &mpsc::Receiver<proto::NodeMessage>) -> usize {
    messages
        .try_iter()
        .filter(|message| {
            matches!(
                message,
                proto::NodeMessage::Request {
                    request: proto::NodeRequest { body: proto::NodeRequestBody::CommitTransaction { .. }, .. },
                    ..
                }
            )
        })
        .count()
}

#[tokio::test]
async fn a_presence_with_a_forged_signature_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();

    let challenge = node.issue_challenge();
    let mut presence = key.sign_presence(false, challenge.challenge());
    presence.durable = true;
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(presence, challenge, PeerProvenance::Configured, Box::new(RecordingSender(key.node_id(), sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::InvalidSignature(key.node_id())), "a claim changed after signing");

    let impersonated = NodeKey::generate().node_id();
    let challenge = node.issue_challenge();
    let mut presence = key.sign_presence(false, challenge.challenge());
    presence.node_id = impersonated;
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(presence, challenge, PeerProvenance::Configured, Box::new(RecordingSender(impersonated, sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::InvalidSignature(impersonated)), "a signature by another node's key");
    assert_eq!((node.peer_trust(&key.node_id()), node.peer_trust(&impersonated)), (None, None));
}

#[tokio::test]
async fn a_presence_replayed_on_another_connection_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let other = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let peer = key.node_id();

    let first = node.issue_challenge();
    let presence = key.sign_presence(false, first.challenge());
    let (sent, _) = mpsc::channel();
    node.register_peer(presence.clone(), first, PeerProvenance::Inbound, Box::new(RecordingSender(peer, sent.clone()))).await.unwrap();

    let second = node.issue_challenge();
    let replayed =
        node.register_peer(presence.clone(), second, PeerProvenance::Inbound, Box::new(RecordingSender(peer, sent.clone()))).await;
    assert_eq!(replayed, Err(PeerConnectionError::ChallengeMismatch(peer)), "the same node, a new connection");

    let elsewhere = other.issue_challenge();
    let replayed = other.register_peer(presence, elsewhere, PeerProvenance::Inbound, Box::new(RecordingSender(peer, sent))).await;
    assert_eq!(replayed, Err(PeerConnectionError::ChallengeMismatch(peer)), "a presence signed for another node");
    assert_eq!(other.peer_trust(&peer), None);
}

#[tokio::test]
async fn a_reflected_self_connection_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    // A reflecting peer hands the node its own challenge, then hands back the node's answer.
    let challenge = node.issue_challenge();
    let reflected = node.presence(challenge.challenge()).await.unwrap();
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(reflected, challenge, PeerProvenance::Configured, Box::new(RecordingSender(node.id, sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::SelfConnection(node.id)));
    assert_eq!(node.peer_trust(&node.id), None);
}

#[test]
fn the_default_policy_trusts_a_durable_claim_only_from_a_configured_peer() {
    let agent = PermissiveAgent::new();
    let peer = NodeKey::generate().node_id();
    assert_eq!(agent.peer_trust(&peer, PeerProvenance::Configured, true), PeerTrust { durable: true });
    assert_eq!(agent.peer_trust(&peer, PeerProvenance::Configured, false), PeerTrust { durable: false });
    assert_eq!(agent.peer_trust(&peer, PeerProvenance::Inbound, true), PeerTrust { durable: false });
    assert_eq!(agent.peer_trust(&peer, PeerProvenance::Inbound, false), PeerTrust { durable: false });
}

#[tokio::test]
async fn only_a_configured_durable_peer_receives_local_commits() {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await.unwrap();
    let (inbound, configured) = (NodeKey::generate(), NodeKey::generate());
    let (inbound_sent, inbound_messages) = mpsc::channel();
    let (configured_sent, configured_messages) = mpsc::channel();
    let inbound_sender = Box::new(RecordingSender(inbound.node_id(), inbound_sent));
    register_keyed_peer(&node, &inbound, true, PeerProvenance::Inbound, inbound_sender).await.unwrap();
    let configured_sender = Box::new(RecordingSender(configured.node_id(), configured_sent));
    register_keyed_peer(&node, &configured, true, PeerProvenance::Configured, configured_sender).await.unwrap();

    assert_eq!(node.peer_trust(&inbound.node_id()), Some(PeerTrust { durable: false }));
    assert_eq!(node.peer_trust(&configured.node_id()), Some(PeerTrust { durable: true }));
    assert_eq!(node.get_durable_peers(), vec![configured.node_id()]);

    let relay = node.relay_to_required_peers(&DEFAULT_CONTEXT, proto::TransactionId::new(), &[]);
    tokio::pin!(relay);
    assert!(futures::poll!(&mut relay).is_pending(), "the relay waits for the configured durable peer");
    assert_eq!(commit_relays(&configured_messages), 1);
    assert_eq!(commit_relays(&inbound_messages), 0);
}

#[tokio::test]
async fn a_durable_peer_registering_again_inbound_leaves_durable_routing() {
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    server.system.create().await.unwrap();
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let relay = client.subscription_relay.as_ref().unwrap();

    let challenge = client.issue_challenge();
    let presence = server.presence(challenge.challenge()).await.unwrap();
    let (sent, _) = mpsc::channel();
    client.register_peer(presence, challenge, PeerProvenance::Configured, Box::new(RecordingSender(server.id, sent))).await.unwrap();
    assert_eq!(client.get_durable_peers(), vec![server.id]);
    assert!(relay.is_connected(&server.id));

    // Anyone can get the server's presence for the client's fresh challenge by relaying the challenge to it.
    let challenge = client.issue_challenge();
    let relayed = server.presence(challenge.challenge()).await.unwrap();
    let (relayer_sent, relayer_messages) = mpsc::channel();
    let relayer = Box::new(RecordingSender(server.id, relayer_sent));
    client.register_peer(relayed, challenge, PeerProvenance::Inbound, relayer).await.unwrap();

    assert_eq!(client.peer_trust(&server.id), Some(PeerTrust { durable: false }));
    assert!(client.get_durable_peers().is_empty());
    assert!(!relay.is_connected(&server.id));
    let commit = client.relay_to_required_peers(&DEFAULT_CONTEXT, proto::TransactionId::new(), &[]);
    tokio::pin!(commit);
    assert!(matches!(futures::poll!(&mut commit), std::task::Poll::Ready(Ok(()))), "no durable peer is left to relay to");
    assert_eq!(commit_relays(&relayer_messages), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registrations_of_one_id_leave_durable_routing_matching_the_connection_that_won() {
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    server.system.create().await.unwrap();
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let register = |provenance| {
        let (client, server) = (client.clone(), server.clone());
        tokio::spawn(async move {
            let challenge = client.issue_challenge();
            let presence = server.presence(challenge.challenge()).await.unwrap();
            let (sent, _) = mpsc::channel();
            client.register_peer(presence, challenge, provenance, Box::new(RecordingSender(server.id, sent))).await.unwrap();
        })
    };
    register(PeerProvenance::Configured).await.unwrap();

    for (first, second) in [(PeerProvenance::Configured, PeerProvenance::Inbound), (PeerProvenance::Inbound, PeerProvenance::Configured)] {
        for _ in 0..100 {
            let (first, second) = (register(first), register(second));
            first.await.unwrap();
            second.await.unwrap();
            let durable = client.peer_trust(&server.id).unwrap().durable;
            assert_eq!(client.get_durable_peers().contains(&server.id), durable);
            assert_eq!(client.subscription_relay.as_ref().unwrap().is_connected(&server.id), durable);
        }
    }
}

#[tokio::test]
async fn an_ephemeral_node_adopts_a_root_only_from_a_peer_it_trusts_as_durable() {
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    server.system.create().await.unwrap();
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());

    // An inbound peer offering a genuine root is not a source the client adopts from.
    let squatter = NodeKey::generate();
    let challenge = client.issue_challenge();
    let presence = proto::Presence { system_root: server.system.root(), ..squatter.sign_presence(true, challenge.challenge()) };
    let (sent, _) = mpsc::channel();
    client.register_peer(presence, challenge, PeerProvenance::Inbound, Box::new(RecordingSender(squatter.node_id(), sent))).await.unwrap();
    assert_eq!(client.system.root_id(), None);
    assert!(client.get_durable_peers().is_empty());

    let challenge = client.issue_challenge();
    let presence = server.presence(challenge.challenge()).await.unwrap();
    let (sent, _) = mpsc::channel();
    client.register_peer(presence, challenge, PeerProvenance::Configured, Box::new(RecordingSender(server.id, sent))).await.unwrap();
    assert_eq!(client.system.root_id(), server.system.root_id());
    assert_eq!(client.get_durable_peers(), vec![server.id]);
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_durable_node_keeps_its_identity_across_a_restart_with_its_persisted_key() {
    let dir = std::env::temp_dir().join(format!("ankurah-node-identity-{}", NodeKey::generate().node_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("node.key");
    let storage = Arc::new(TestStorage::default());

    let first = Node::new_durable_with_key(storage.clone(), PermissiveAgent::new(), NodeKey::load_or_create(&path).unwrap()).id;
    let restarted = Node::new_durable_with_key(storage.clone(), PermissiveAgent::new(), NodeKey::load_or_create(&path).unwrap()).id;
    assert_eq!(restarted, first);
    std::fs::remove_dir_all(&dir).unwrap();

    let unkeyed = Node::new_durable(storage, PermissiveAgent::new()).id;
    assert_ne!(unkeyed, first, "a durable node given no key draws a fresh identity");
}

#[tokio::test]
async fn an_ephemeral_node_draws_a_fresh_identity_each_time() {
    let storage = Arc::new(TestStorage::default());
    let first = Node::new(storage.clone(), PermissiveAgent::new()).id;
    assert_ne!(Node::new(storage, PermissiveAgent::new()).id, first);
}
