use super::{register_keyed_peer, register_keyed_peer_claiming, RecordingSender};
use crate::connector::{Handshake, PeerConnectionError, PeerProvenance};
use crate::node::{Node, NodeKey};
use crate::policy::{PeerTrust, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT};
use crate::test_helpers::opened_handshakes;
use crate::test_utils::TestStorage;
use ankurah_proto as proto;
use std::sync::{mpsc, Arc};

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

/// A dialing side presents before it knows who answers. Its presence names the dialing side, so
/// whoever answers cannot pass it to a node that dialed as the presence of the side that accepted.
#[tokio::test]
async fn a_dialing_sides_presence_offered_as_an_accepting_sides_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    // Each side holds the other's opening, and both dialed.
    let (mut ours, mut dialing) =
        (node.begin_handshake(PeerProvenance::Configured), Handshake::new(key.node_id(), PeerProvenance::Configured));
    ours.receive_opening(dialing.opening()).unwrap();
    dialing.receive_opening(ours.opening()).unwrap();
    let presence = key.sign_presence(dialing.presence_claims(key.node_id(), true).unwrap());
    let (sent, _) = mpsc::channel();
    let refused = node.register_peer(presence, &mut ours, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::RoleMismatch(key.node_id())));
    assert_eq!(node.granted_trust(&key.node_id()), None);
}

/// A presence names its signer's own opening, so it registers only on the connection whose
/// opening carried that challenge.
#[tokio::test]
async fn a_presence_naming_a_challenge_other_than_its_own_openings_is_refused() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = NodeKey::generate();
    let another_opening = proto::HandshakeChallenge::new(key.node_id());
    let (sent, _) = mpsc::channel();
    let sender = Box::new(RecordingSender(sent));
    let refused =
        register_keyed_peer_claiming(&node, &key, false, |claims| claims.own_challenge = another_opening, PeerProvenance::Inbound, sender);
    assert_eq!(refused.await, Err(PeerConnectionError::OwnChallengeMismatch(key.node_id())));
    assert_eq!(node.granted_trust(&key.node_id()), None);
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

/// Registrations and removals of one id racing each other leave the durable set and the relay
/// agreeing with the registrations that remain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_registrations_and_removals_of_one_id_leave_the_durable_set_and_the_relay_matching_what_remains() {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let key = Arc::new(NodeKey::generate());
    let peer = key.node_id();
    let durable = || (node.get_durable_peers().contains(&peer), node.subscription_relay.as_ref().unwrap().is_connected(&peer));
    for _ in 0..100 {
        let provenances = [PeerProvenance::Configured, PeerProvenance::Inbound, PeerProvenance::Configured, PeerProvenance::Inbound];
        let registering = provenances.map(|provenance| {
            let (node, key) = (node.clone(), key.clone());
            tokio::spawn(async move {
                let (sent, _) = mpsc::channel();
                register_keyed_peer(&node, &key, true, provenance, Box::new(RecordingSender(sent))).await.unwrap()
            })
        });
        let mut registrations = Vec::new();
        for registration in registering {
            registrations.push(registration.await.unwrap());
        }
        assert_eq!(durable(), (true, true));

        let removing: Vec<_> = registrations
            .into_iter()
            .map(|registration| {
                let node = node.clone();
                tokio::spawn(async move { node.deregister_peer(registration) })
            })
            .collect();
        for removal in removing {
            removal.await.unwrap();
        }
        assert_eq!(durable(), (false, false));
        assert_eq!(node.granted_trust(&peer), None);
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
    let first = keyed().id;
    let restarted = keyed().id;
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
