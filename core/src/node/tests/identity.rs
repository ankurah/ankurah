use super::{register_keyed_peer, register_keyed_peer_claiming, RecordingSender};
use crate::connector::{Handshake, PeerConnectionError, PeerProvenance};
use crate::node::{Node, NodeKey};
use crate::policy::{PeerTrust, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT};
use crate::test_helpers::opened_handshakes;
use crate::test_utils::TestStorage;
use ankurah_proto as proto;
use std::sync::{mpsc, Arc};

type TestNode = Node<TestStorage, PermissiveAgent>;

/// `client`'s side of a fresh handshake with `server`, reached as `provenance` says, and
/// `server`'s presence answering it. A server the client dialed registers the client first.
async fn answered_handshake(client: &TestNode, provenance: PeerProvenance, server: &TestNode) -> (Handshake, proto::Presence) {
    let (ours, mut theirs) = opened_handshakes(client.id, provenance, server.id);
    if provenance == PeerProvenance::Configured {
        let (sent, _) = mpsc::channel();
        server.register_peer(client.presence(&ours).await.unwrap(), &mut theirs, Box::new(RecordingSender(sent))).await.unwrap();
    }
    (ours, server.presence(&theirs).await.unwrap())
}

fn commit_relays(messages: &mpsc::Receiver<proto::NodeMessage>) -> usize {
    messages
        .try_iter()
        .filter(|message| match message {
            proto::NodeMessage::Request { request, .. } => {
                matches!(request.body.decode(), Ok(proto::NodeRequestBody::CommitTransaction { .. }))
            }
            _ => false,
        })
        .count()
}

#[tokio::test]
async fn a_presence_with_a_forged_signature_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();

    let (mut ours, theirs) = opened_handshakes(node.id, PeerProvenance::Configured, key.node_id());
    let mut presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    presence.claims.durable = true;
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(presence, &mut ours, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::InvalidSignature(key.node_id())), "a claim changed after signing");

    let impersonated = NodeKey::generate().node_id();
    let (mut ours, theirs) = opened_handshakes(node.id, PeerProvenance::Configured, key.node_id());
    let mut presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    presence.claims.node_id = impersonated;
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(presence, &mut ours, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::InvalidSignature(impersonated)), "a signature by another node's key");
    assert_eq!((node.granted_trust(&key.node_id()), node.granted_trust(&impersonated)), (None, None));
}

#[tokio::test]
async fn a_presence_replayed_on_another_connection_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let other = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let peer = key.node_id();

    let (mut first, theirs) = opened_handshakes(node.id, PeerProvenance::Inbound, peer);
    let presence = key.sign_presence(theirs.presence_claims(peer, false).unwrap());
    let (sent, _) = mpsc::channel();
    node.register_peer(presence.clone(), &mut first, Box::new(RecordingSender(sent.clone()))).await.unwrap();

    let (mut second, _) = opened_handshakes(node.id, PeerProvenance::Inbound, peer);
    let replayed = node.register_peer(presence.clone(), &mut second, Box::new(RecordingSender(sent.clone()))).await;
    assert_eq!(replayed, Err(PeerConnectionError::ChallengeMismatch(peer)), "the same node, a new connection");

    let (mut elsewhere, _) = opened_handshakes(other.id, PeerProvenance::Inbound, peer);
    let replayed = other.register_peer(presence, &mut elsewhere, Box::new(RecordingSender(sent))).await;
    assert_eq!(replayed, Err(PeerConnectionError::ChallengeMismatch(peer)), "a presence signed for another node");
    assert_eq!(other.granted_trust(&peer), None);
}

/// A middle party that opens to its dialer with a third node's challenge receives the dialer's
/// presence answering that challenge, since a dialer presents before the node it dialed does. The
/// dialer refuses the middle party's presence that follows, and the third node refuses the
/// dialer's presence arriving on a connection whose opening carried the middle party's challenge.
/// Had that opening carried the dialer's challenge instead, the third node would have registered
/// the middle party's connection as the dialer: the handshake does not close that.
#[tokio::test]
async fn a_presence_on_a_connection_that_relayed_a_third_nodes_challenge_is_refused_on_both_sides() {
    let node = || Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let (victim, third) = (node(), node());
    let middle = NodeKey::generate();
    let (sent, _) = mpsc::channel();
    let sender = || Box::new(RecordingSender(sent.clone()));

    // The middle party dials the third, which opens with its own challenge.
    let (mut middle_to_third, mut third_side) =
        (Handshake::new(middle.node_id(), PeerProvenance::Configured), third.begin_handshake(PeerProvenance::Inbound));
    middle_to_third.receive_opening(third_side.opening()).unwrap();
    third_side.receive_opening(middle_to_third.opening()).unwrap();

    // The victim dials the middle party, which opens with the third node's challenge; the victim
    // presents, answering it.
    let (mut victim_side, mut middle_side) =
        (victim.begin_handshake(PeerProvenance::Configured), Handshake::new(middle.node_id(), PeerProvenance::Inbound));
    victim_side.receive_opening(third_side.opening()).unwrap();
    middle_side.receive_opening(victim_side.opening()).unwrap();
    let victim_answer = victim.presence(&victim_side).await.unwrap();
    assert_eq!(victim_answer.claims.challenge, third_side.opening().challenge, "the victim's presence answers the third node");

    let middle_presence = middle.sign_presence(middle_side.presence_claims(middle.node_id(), false).unwrap());
    let refused = victim.register_peer(middle_presence, &mut victim_side, sender()).await;
    assert_eq!(refused, Err(PeerConnectionError::RelayedChallenge { peer: middle.node_id(), issuer: third.id }));
    assert_eq!(victim.granted_trust(&middle.node_id()), None);

    let relayed = third.register_peer(victim_answer, &mut third_side, sender()).await;
    assert_eq!(relayed, Err(PeerConnectionError::RelayedChallenge { peer: victim.id, issuer: middle.node_id() }));
    assert_eq!(third.granted_trust(&victim.id), None);
}

#[tokio::test]
async fn a_reflected_self_connection_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    // A reflecting peer hands the node its own opening, then hands back the node's answer.
    let mut handshake = node.begin_handshake(PeerProvenance::Configured);
    handshake.receive_opening(handshake.opening()).unwrap();
    let reflected = node.presence(&handshake).await.unwrap();
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(reflected, &mut handshake, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::SelfConnection(node.id)));
    assert_eq!(node.granted_trust(&node.id), None);
}

/// A party between the two nodes can change the openings, which nothing signs, so a peer that
/// saw other offers signs other ones; the signed offers and choice must match this side's.
#[tokio::test]
async fn a_presence_that_does_not_repeat_the_offers_and_the_chosen_version_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    const WIDER: proto::ProtocolVersions = proto::ProtocolVersions { lowest: 1, highest: 3 };
    let alterations: [fn(&mut proto::VersionNegotiation); 3] =
        [|negotiation| negotiation.peer_offered = WIDER, |negotiation| negotiation.offered = WIDER, |negotiation| negotiation.chosen = 2];
    for alter in alterations {
        let (sent, _) = mpsc::channel();
        let sender = Box::new(RecordingSender(sent));
        let refused =
            register_keyed_peer_claiming(&node, &key, false, |claims| alter(&mut claims.negotiation), PeerProvenance::Inbound, sender);
        assert_eq!(refused.await, Err(PeerConnectionError::OfferMismatch(key.node_id())));
        assert_eq!(node.granted_trust(&key.node_id()), None);
    }
}

#[tokio::test]
async fn unknown_feature_numbers_and_announcement_names_are_ignored() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (sent, _) = mpsc::channel();
    let unknown = |claims: &mut proto::PresenceClaims| {
        claims.features = vec![7, u32::MAX];
        claims.announcements.insert("a plugin this node does not have".into(), vec![1, 2, 3]);
    };
    register_keyed_peer_claiming(&node, &key, false, unknown, PeerProvenance::Inbound, Box::new(RecordingSender(sent))).await.unwrap();
    assert_eq!(node.granted_trust(&key.node_id()), Some(PeerTrust { durable: false }));
}

#[tokio::test]
async fn a_presence_before_the_peers_opening_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (_, theirs) = opened_handshakes(node.id, PeerProvenance::Inbound, key.node_id());
    let presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    let (sent, _) = mpsc::channel();
    let mut unopened = node.begin_handshake(PeerProvenance::Inbound);
    let refused = node.register_peer(presence, &mut unopened, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::NoOpening));
    assert_eq!(node.granted_trust(&key.node_id()), None);
}

/// One handshake registers its peer once: the same presence registered again on it is refused,
/// and the first registration stands.
#[tokio::test]
async fn a_second_registration_on_one_handshake_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (mut handshake, theirs) = opened_handshakes(node.id, PeerProvenance::Inbound, key.node_id());
    let presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    let (sent, _) = mpsc::channel();
    let first = node.register_peer(presence.clone(), &mut handshake, Box::new(RecordingSender(sent.clone()))).await.unwrap();

    let again = node.register_peer(presence, &mut handshake, Box::new(RecordingSender(sent))).await;
    assert_eq!(again, Err(PeerConnectionError::RepeatedRegistration));
    assert!(node.peers.registered(&first).is_some(), "the first registration stands");
}

/// A node that accepted a connection signs nothing for the dialer until it has registered the
/// dialer on that connection; a node that dialed presents first.
#[tokio::test]
async fn an_accepting_node_presents_only_once_it_has_registered_the_dialer() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (mut accepted, theirs) = opened_handshakes(node.id, PeerProvenance::Inbound, key.node_id());
    assert_eq!(node.presence(&accepted).await, Err(PeerConnectionError::PresenceBeforeRegistration));

    let presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    let (sent, _) = mpsc::channel();
    node.register_peer(presence, &mut accepted, Box::new(RecordingSender(sent))).await.unwrap();
    assert!(node.presence(&accepted).await.is_ok(), "the dialer is registered");

    let (dialed, _) = opened_handshakes(node.id, PeerProvenance::Configured, key.node_id());
    assert!(node.presence(&dialed).await.is_ok(), "a dialing node presents before it registers anyone");
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
    let (inbound, configured) = (NodeKey::generate(), NodeKey::generate());
    let (inbound_sent, inbound_messages) = mpsc::channel();
    let (configured_sent, configured_messages) = mpsc::channel();
    let inbound_sender = Box::new(RecordingSender(inbound_sent));
    register_keyed_peer(&node, &inbound, true, PeerProvenance::Inbound, inbound_sender).await.unwrap();
    let configured_sender = Box::new(RecordingSender(configured_sent));
    register_keyed_peer(&node, &configured, true, PeerProvenance::Configured, configured_sender).await.unwrap();

    assert_eq!(node.granted_trust(&inbound.node_id()), Some(PeerTrust { durable: false }));
    assert_eq!(node.granted_trust(&configured.node_id()), Some(PeerTrust { durable: true }));
    assert_eq!(node.get_durable_peers(), vec![configured.node_id()]);

    let relay = crate::node::durable_peer::relay_to_required_peers(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), &[]);
    tokio::pin!(relay);
    assert!(futures::poll!(&mut relay).is_pending(), "the relay waits for the configured durable peer");
    assert_eq!(commit_relays(&configured_messages), 1);
    assert_eq!(commit_relays(&inbound_messages), 0);
}

#[tokio::test]
async fn a_durable_peer_registering_again_inbound_leaves_durable_routing() {
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let relay = client.subscription_relay.as_ref().unwrap();

    let (mut handshake, presence) = answered_handshake(&client, PeerProvenance::Configured, &server).await;
    let (sent, _) = mpsc::channel();
    client.register_peer(presence, &mut handshake, Box::new(RecordingSender(sent))).await.unwrap();
    assert_eq!(client.get_durable_peers(), vec![server.id]);
    assert!(relay.is_connected(&server.id));

    // A server that dials an address presents to whoever answers there, answering the challenge
    // that answer carried: here the client's, relayed.
    let (mut handshake, relayed) = answered_handshake(&client, PeerProvenance::Inbound, &server).await;
    let (relayer_sent, relayer_messages) = mpsc::channel();
    let relayer = Box::new(RecordingSender(relayer_sent));
    client.register_peer(relayed, &mut handshake, relayer).await.unwrap();

    assert_eq!(client.granted_trust(&server.id), Some(PeerTrust { durable: false }));
    assert!(client.get_durable_peers().is_empty());
    assert!(!relay.is_connected(&server.id));
    let commit = crate::node::durable_peer::relay_to_required_peers(&client, &DEFAULT_CONTEXT, proto::TransactionId::new(), &[]);
    tokio::pin!(commit);
    assert!(matches!(futures::poll!(&mut commit), std::task::Poll::Ready(Ok(()))), "no durable peer is left to relay to");
    assert_eq!(commit_relays(&relayer_messages), 0);
}

/// A connection that closes after its peer registered again on a new one removes only its own
/// registration: the newer connection keeps its routing, durable standing and subscriptions.
#[tokio::test]
async fn a_closing_connection_leaves_a_newer_registration_of_its_peer_in_place() {
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let relay = client.subscription_relay.as_ref().unwrap();
    let register = || async {
        let (mut handshake, presence) = answered_handshake(&client, PeerProvenance::Configured, &server).await;
        let (sent, _) = mpsc::channel();
        client.register_peer(presence, &mut handshake, Box::new(RecordingSender(sent))).await.unwrap()
    };
    let (first, second) = (register().await, register().await);
    assert_ne!(first, second);

    client.deregister_peer(first);
    assert_eq!(client.granted_trust(&server.id), Some(PeerTrust { durable: true }));
    assert_eq!(client.get_durable_peers(), vec![server.id]);
    assert!(relay.is_connected(&server.id));

    client.deregister_peer(second);
    assert_eq!(client.granted_trust(&server.id), None);
    assert!(client.get_durable_peers().is_empty());
    assert!(!relay.is_connected(&server.id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registrations_of_one_id_leave_durable_routing_matching_the_connection_that_won() {
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let register = |provenance| {
        let (client, server) = (client.clone(), server.clone());
        tokio::spawn(async move {
            let (mut handshake, presence) = answered_handshake(&client, provenance, &server).await;
            let (sent, _) = mpsc::channel();
            client.register_peer(presence, &mut handshake, Box::new(RecordingSender(sent))).await.unwrap();
        })
    };
    register(PeerProvenance::Configured).await.unwrap();

    for (first, second) in [(PeerProvenance::Configured, PeerProvenance::Inbound), (PeerProvenance::Inbound, PeerProvenance::Configured)] {
        for _ in 0..100 {
            let (first, second) = (register(first), register(second));
            first.await.unwrap();
            second.await.unwrap();
            let durable = client.granted_trust(&server.id).unwrap().durable;
            assert_eq!(client.get_durable_peers().contains(&server.id), durable);
            assert_eq!(client.subscription_relay.as_ref().unwrap().is_connected(&server.id), durable);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_durable_node_keeps_its_identity_across_a_restart_with_its_persisted_key() {
    let dir = std::env::temp_dir().join(format!("ankurah-node-identity-{}", NodeKey::generate().node_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("node.key");
    let storage = Arc::new(TestStorage::default());

    let keyed = || Node::builder(storage.clone(), PermissiveAgent::new()).durable().key(NodeKey::load_or_create(&path).unwrap()).build();
    let first = keyed().unwrap().id;
    let restarted = keyed().unwrap().id;
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
