use super::{register_keyed_peer, register_keyed_peer_claiming, RecordingSender};
use crate::connector::{Handshake, PeerConnectionError, PeerProvenance};
use crate::node::{Node, NodeKey};
use crate::policy::{PeerTrust, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT};
use crate::test_utils::{opened_handshakes, TestStorage};
use ankurah_proto as proto;
use std::sync::{mpsc, Arc};

type TestNode = Node<TestStorage, PermissiveAgent>;

/// `client`'s side of a fresh handshake with `server`, and `server`'s presence answering it.
async fn answered_handshake(client: &TestNode, server: &TestNode) -> (Handshake, proto::Presence) {
    let (ours, theirs) = opened_handshakes(client.id, server.id);
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

    let (ours, theirs) = opened_handshakes(node.id, key.node_id());
    let mut presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    presence.claims.durable = true;
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(presence, ours, PeerProvenance::Configured, Box::new(RecordingSender(key.node_id(), sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::InvalidSignature(key.node_id())), "a claim changed after signing");

    let impersonated = NodeKey::generate().node_id();
    let (ours, theirs) = opened_handshakes(node.id, key.node_id());
    let mut presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    presence.claims.node_id = impersonated;
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(presence, ours, PeerProvenance::Configured, Box::new(RecordingSender(impersonated, sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::InvalidSignature(impersonated)), "a signature by another node's key");
    assert_eq!((node.granted_trust(&key.node_id()), node.granted_trust(&impersonated)), (None, None));
}

#[tokio::test]
async fn a_presence_replayed_on_another_connection_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let other = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let peer = key.node_id();

    let (first, theirs) = opened_handshakes(node.id, peer);
    let presence = key.sign_presence(theirs.presence_claims(peer, false).unwrap());
    let (sent, _) = mpsc::channel();
    node.register_peer(presence.clone(), first, PeerProvenance::Inbound, Box::new(RecordingSender(peer, sent.clone()))).await.unwrap();

    let (second, _) = opened_handshakes(node.id, peer);
    let replayed =
        node.register_peer(presence.clone(), second, PeerProvenance::Inbound, Box::new(RecordingSender(peer, sent.clone()))).await;
    assert_eq!(replayed, Err(PeerConnectionError::ChallengeMismatch(peer)), "the same node, a new connection");

    let (elsewhere, _) = opened_handshakes(other.id, peer);
    let replayed = other.register_peer(presence, elsewhere, PeerProvenance::Inbound, Box::new(RecordingSender(peer, sent))).await;
    assert_eq!(replayed, Err(PeerConnectionError::ChallengeMismatch(peer)), "a presence signed for another node");
    assert_eq!(other.granted_trust(&peer), None);
}

/// A middle node that answers its dialer's challenge itself while handing it a third node's
/// challenge is refused by the dialer, before the dialer sends a presence answering that
/// challenge; and the third node refuses the dialer's presence arriving on the middle node's
/// connection, in case the dialer had sent one.
#[tokio::test]
async fn a_presence_on_a_connection_that_relayed_a_third_nodes_challenge_is_refused_on_both_sides() {
    let node = || Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let (victim, middle, third) = (node(), node(), node());
    let (sent, _) = mpsc::channel();
    let sender = |peer| Box::new(RecordingSender(peer, sent.clone()));

    // The middle node dials the third, which opens with its own challenge.
    let (mut middle_to_third, mut third_side) = (middle.begin_handshake(), third.begin_handshake());
    middle_to_third.receive_opening(third_side.opening()).unwrap();
    third_side.receive_opening(middle_to_third.opening()).unwrap();

    // The victim dials the middle node, which opens with the third node's challenge and answers
    // the victim's challenge with its own presence.
    let (mut victim_side, mut middle_side) = (victim.begin_handshake(), middle.begin_handshake());
    victim_side.receive_opening(third_side.opening()).unwrap();
    middle_side.receive_opening(victim_side.opening()).unwrap();
    let middle_presence = middle.presence(&middle_side).await.unwrap();
    let victim_answer = victim.presence(&victim_side).await.unwrap();
    assert_eq!(victim_answer.claims.challenge, third_side.opening().challenge, "what the victim would sign answers the third node");

    let refused = victim.register_peer(middle_presence, victim_side, PeerProvenance::Configured, sender(middle.id)).await;
    assert_eq!(refused, Err(PeerConnectionError::RelayedChallenge { peer: middle.id, issuer: third.id }));
    assert_eq!(victim.granted_trust(&middle.id), None);

    let relayed = third.register_peer(victim_answer, third_side, PeerProvenance::Inbound, sender(victim.id)).await;
    assert_eq!(relayed, Err(PeerConnectionError::RelayedChallenge { peer: victim.id, issuer: middle.id }));
    assert_eq!(third.granted_trust(&victim.id), None);
}

#[tokio::test]
async fn a_reflected_self_connection_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    // A reflecting peer hands the node its own opening, then hands back the node's answer.
    let mut handshake = node.begin_handshake();
    handshake.receive_opening(handshake.opening()).unwrap();
    let reflected = node.presence(&handshake).await.unwrap();
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(reflected, handshake, PeerProvenance::Configured, Box::new(RecordingSender(node.id, sent))).await;
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
        let sender = Box::new(RecordingSender(key.node_id(), sent));
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
    register_keyed_peer_claiming(&node, &key, false, unknown, PeerProvenance::Inbound, Box::new(RecordingSender(key.node_id(), sent)))
        .await
        .unwrap();
    assert_eq!(node.granted_trust(&key.node_id()), Some(PeerTrust { durable: false }));
}

#[tokio::test]
async fn a_presence_before_the_peers_opening_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let (_, theirs) = opened_handshakes(node.id, key.node_id());
    let presence = key.sign_presence(theirs.presence_claims(key.node_id(), false).unwrap());
    let (sent, _) = mpsc::channel();
    let unopened = node.begin_handshake();
    let refused = node.register_peer(presence, unopened, PeerProvenance::Inbound, Box::new(RecordingSender(key.node_id(), sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::NoOpening));
    assert_eq!(node.granted_trust(&key.node_id()), None);
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
    let inbound_sender = Box::new(RecordingSender(inbound.node_id(), inbound_sent));
    register_keyed_peer(&node, &inbound, true, PeerProvenance::Inbound, inbound_sender).await.unwrap();
    let configured_sender = Box::new(RecordingSender(configured.node_id(), configured_sent));
    register_keyed_peer(&node, &configured, true, PeerProvenance::Configured, configured_sender).await.unwrap();

    assert_eq!(node.granted_trust(&inbound.node_id()), Some(PeerTrust { durable: false }));
    assert_eq!(node.granted_trust(&configured.node_id()), Some(PeerTrust { durable: true }));
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
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let relay = client.subscription_relay.as_ref().unwrap();

    let (handshake, presence) = answered_handshake(&client, &server).await;
    let (sent, _) = mpsc::channel();
    client.register_peer(presence, handshake, PeerProvenance::Configured, Box::new(RecordingSender(server.id, sent))).await.unwrap();
    assert_eq!(client.get_durable_peers(), vec![server.id]);
    assert!(relay.is_connected(&server.id));

    // Anyone can get the server's presence for the client's fresh opening by relaying the opening to it.
    let (handshake, relayed) = answered_handshake(&client, &server).await;
    let (relayer_sent, relayer_messages) = mpsc::channel();
    let relayer = Box::new(RecordingSender(server.id, relayer_sent));
    client.register_peer(relayed, handshake, PeerProvenance::Inbound, relayer).await.unwrap();

    assert_eq!(client.granted_trust(&server.id), Some(PeerTrust { durable: false }));
    assert!(client.get_durable_peers().is_empty());
    assert!(!relay.is_connected(&server.id));
    let commit = client.relay_to_required_peers(&DEFAULT_CONTEXT, proto::TransactionId::new(), &[]);
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
        let (handshake, presence) = answered_handshake(&client, &server).await;
        let (sent, _) = mpsc::channel();
        client.register_peer(presence, handshake, PeerProvenance::Configured, Box::new(RecordingSender(server.id, sent))).await.unwrap()
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
        register_keyed_peer(&node, &liar, false, PeerProvenance::Inbound, Box::new(RecordingSender(liar.node_id(), liar_sent)))
            .await
            .unwrap();
    register_keyed_peer(&node, &other, false, PeerProvenance::Inbound, Box::new(RecordingSender(other.node_id(), other_sent)))
        .await
        .unwrap();
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
    let old =
        register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(key.node_id(), old_sent))).await.unwrap();
    let new =
        register_keyed_peer(&node, &key, false, PeerProvenance::Inbound, Box::new(RecordingSender(key.node_id(), new_sent))).await.unwrap();

    node.handle_message(old, get_request(node.id, key.node_id())).await.unwrap();
    assert!(old_frames.try_recv().is_err() && new_frames.try_recv().is_err());
    node.handle_message(new, get_request(node.id, key.node_id())).await.unwrap();
    assert!(matches!(new_frames.try_recv(), Ok(proto::NodeMessage::Response(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registrations_of_one_id_leave_durable_routing_matching_the_connection_that_won() {
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let register = |provenance| {
        let (client, server) = (client.clone(), server.clone());
        tokio::spawn(async move {
            let (handshake, presence) = answered_handshake(&client, &server).await;
            let (sent, _) = mpsc::channel();
            client.register_peer(presence, handshake, provenance, Box::new(RecordingSender(server.id, sent))).await.unwrap();
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
