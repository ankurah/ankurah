//! Adversarial coverage for the identity and attestation substrate.
//!
//! These tests stay on public seams: connector-facing Presence registration,
//! wire update ingestion, and remote transaction ingestion. They deliberately
//! assert what an untrusted peer cannot make core accept or expose to policy.

use ankql::ast::Predicate;
use ankurah::core::{
    connector::{PeerFrameError, PeerHandshake, PeerIdentityMismatch, PeerSender, SendError},
    entity::Entity,
    error::{RequestError, ValidationError},
    node::{Node as NodeAlias, NodeInner},
    policy::{AccessDenied, Admission, DefaultContext, PolicyAgent, DEFAULT_CONTEXT},
    property::{
        backend::{lww::LWWBackend, PropertyBackend},
        PropertyKey,
    },
    storage::StorageEngine,
    util::Iterable,
    value::Value,
};
use ankurah::proto::{self, Attested};
use ankurah::{Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::Barrier;

#[derive(Debug, Model, Serialize, Deserialize)]
pub struct IdentityRecord {
    #[active_type(LWW)]
    title: String,
}

#[derive(Clone)]
struct RecordingAgent {
    attest_local_events: bool,
    deny_events: Arc<AtomicBool>,
    received_event_attestation_counts: Arc<Mutex<Vec<usize>>>,
}

impl RecordingAgent {
    fn new(attest_local_events: bool) -> Self {
        Self {
            attest_local_events,
            deny_events: Arc::new(AtomicBool::new(false)),
            received_event_attestation_counts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn take_received_counts(&self) -> Vec<usize> { std::mem::take(&mut *self.received_event_attestation_counts.lock().unwrap()) }

    fn deny_events(&self) { self.deny_events.store(true, Ordering::Release); }
}

#[async_trait]
impl PolicyAgent for RecordingAgent {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &NodeAlias<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        _entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Admission, AccessDenied> {
        if self.deny_events.load(Ordering::Acquire) {
            return Err(AccessDenied::ByPolicy("identity test policy denied event"));
        }
        Ok(if self.attest_local_events { Admission::Attest { claims: b"test-admission".to_vec() } } else { Admission::Allow })
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        let count = event.attestations.len();
        self.received_event_attestation_counts.lock().unwrap().push(count);
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeAlias<SE, Self>, _state: &proto::EntityState) -> Admission {
        if self.attest_local_events {
            Admission::Attest { claims: b"test-state".to_vec() }
        } else {
            Admission::Allow
        }
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, _collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn filter_predicate<C>(&self, _data: &C, _collection: &proto::CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(predicate)
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &proto::EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
        _resolver: Option<std::sync::Weak<dyn ankurah::core::property::PropertyResolver>>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _peer_id: &proto::NodeId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

#[derive(Clone)]
struct NullSender {
    recipient: proto::NodeId,
}

#[async_trait]
impl PeerSender for NullSender {
    fn send_message(&self, _message: proto::SignedPeerMessage) -> Result<(), SendError> { Ok(()) }

    fn close(&self) {}

    fn recipient_node_id(&self) -> proto::NodeId { self.recipient }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

fn registration<SE1, PA1, SE2, PA2>(
    receiver: &Node<SE1, PA1>,
    peer: &Node<SE2, PA2>,
) -> (proto::Presence, PeerHandshake, proto::HandshakeChallenge)
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    let incoming = receiver.begin_peer_handshake();
    let peer_challenge = peer.begin_peer_handshake().challenge();
    (peer.presence(incoming.challenge()), incoming, peer_challenge)
}

fn signed_get_frame(
    key: &SigningKey,
    session: proto::HandshakeChallenge,
    sequence: u64,
    from: proto::NodeId,
    to: proto::NodeId,
) -> proto::SignedPeerMessage {
    let message = proto::NodeMessage::Request {
        auth: vec![],
        request: proto::NodeRequest {
            id: proto::RequestId::new(),
            to,
            from,
            body: proto::NodeRequestBody::Get { collection: proto::CollectionId::from("signed-frame"), ids: vec![] },
        },
    };
    let signature = key.sign(&proto::SignedPeerMessage::signable_bytes(session, sequence, &message)).into();
    proto::SignedPeerMessage { session, sequence, message, signature }
}

#[tokio::test]
async fn forged_presence_signature_is_rejected() {
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let claimant = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let (mut forged, handshake, outgoing_session) = registration(&receiver, &claimant);
    let challenge = handshake.challenge();

    // `durable` is signed. Reusing the claimant's genuine signature after
    // changing it must not authenticate a different peer class.
    forged.durable = !forged.durable;
    assert!(!forged.verify_for(challenge));

    let result = receiver.register_peer(forged, handshake, outgoing_session, Box::new(NullSender { recipient: claimant.id }));
    assert!(matches!(result, Err(proto::PresenceRefusal::InvalidSignature(id)) if id == claimant.id));
    assert!(receiver.get_durable_peers().is_empty(), "a refused presence must not affect durable routing");
}

#[tokio::test]
async fn captured_presence_cannot_authenticate_a_fresh_connection() -> Result<()> {
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let peer = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let (captured, first_handshake, first_outgoing) = registration(&receiver, &peer);
    let captured_bytes = bincode::serialize(&captured)?;
    receiver.register_peer(captured, first_handshake, first_outgoing, Box::new(NullSender { recipient: peer.id }))?;

    let replayed: proto::Presence = bincode::deserialize(&captured_bytes)?;
    let second_handshake = receiver.begin_peer_handshake();
    let second_outgoing = peer.begin_peer_handshake().challenge();
    let result = receiver.register_peer(replayed, second_handshake, second_outgoing, Box::new(NullSender { recipient: peer.id }));
    assert!(matches!(result, Err(proto::PresenceRefusal::UnexpectedChallenge(id)) if id == peer.id));
    Ok(())
}

#[tokio::test]
async fn reflected_self_presence_is_rejected() -> Result<()> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let handshake = node.begin_peer_handshake();
    let outgoing = node.begin_peer_handshake().challenge();
    let presence = node.presence(handshake.challenge());
    let result = node.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: node.id }));
    assert!(matches!(result, Err(proto::PresenceRefusal::SelfConnection(id)) if id == node.id));
    Ok(())
}

#[tokio::test]
async fn peer_frames_are_session_signed_and_replay_protected() -> Result<()> {
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let peer_key = SigningKey::from_bytes(&[0xD1; 32]);
    let peer = Node::new_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), peer_key.clone());
    let (presence, handshake, outgoing) = registration(&receiver, &peer);
    let session = handshake.challenge();
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: peer.id }))?;

    let message = proto::NodeMessage::Request {
        auth: vec![],
        request: proto::NodeRequest {
            id: proto::RequestId::new(),
            to: receiver.id,
            from: peer.id,
            body: proto::NodeRequestBody::Get { collection: proto::CollectionId::from("signed-frame"), ids: vec![] },
        },
    };
    let signature = peer_key.sign(&proto::SignedPeerMessage::signable_bytes(session, 0, &message)).into();
    let frame = proto::SignedPeerMessage { session, sequence: 0, message, signature };
    let replay = bincode::serialize(&frame)?;
    receiver.verify_peer_message(peer.id, frame)?;

    let replay: proto::SignedPeerMessage = bincode::deserialize(&replay)?;
    assert!(matches!(
        receiver.verify_peer_message(peer.id, replay),
        Err(PeerFrameError::ReplayedSequence { peer: id, sequence: 0 }) if id == peer.id
    ));

    let new_handshake = receiver.begin_peer_handshake();
    let new_outgoing = peer.begin_peer_handshake().challenge();
    let new_presence = peer.presence(new_handshake.challenge());
    receiver.register_peer(new_presence, new_handshake, new_outgoing, Box::new(NullSender { recipient: peer.id }))?;

    let message = proto::NodeMessage::Request {
        auth: vec![],
        request: proto::NodeRequest {
            id: proto::RequestId::new(),
            to: receiver.id,
            from: peer.id,
            body: proto::NodeRequestBody::Get { collection: proto::CollectionId::from("old-session"), ids: vec![] },
        },
    };
    let signature = peer_key.sign(&proto::SignedPeerMessage::signable_bytes(session, 1, &message)).into();
    let old_session_frame = proto::SignedPeerMessage { session, sequence: 1, message, signature };
    assert!(
        matches!(receiver.verify_peer_message(peer.id, old_session_frame), Err(PeerFrameError::WrongSession { peer: id }) if id == peer.id)
    );
    Ok(())
}

#[tokio::test]
async fn stale_disconnect_cannot_remove_a_replacement_session() -> Result<()> {
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let peer_key = SigningKey::from_bytes(&[0xD4; 32]);
    let peer = Node::new_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), peer_key.clone());

    let (presence, first_handshake, outgoing) = registration(&receiver, &peer);
    let first_session = first_handshake.challenge();
    receiver.register_peer(presence, first_handshake, outgoing, Box::new(NullSender { recipient: peer.id }))?;

    let (presence, second_handshake, outgoing) = registration(&receiver, &peer);
    let second_session = second_handshake.challenge();
    receiver.register_peer(presence, second_handshake, outgoing, Box::new(NullSender { recipient: peer.id }))?;

    assert!(!receiver.deregister_peer_session(peer.id, first_session));
    receiver.verify_peer_message(peer.id, signed_get_frame(&peer_key, second_session, 0, peer.id, receiver.id))?;
    assert!(receiver.deregister_peer_session(peer.id, second_session));
    assert!(matches!(
        receiver.verify_peer_message(peer.id, signed_get_frame(&peer_key, second_session, 1, peer.id, receiver.id)),
        Err(PeerFrameError::PeerNotRegistered { peer: id }) if id == peer.id
    ));
    Ok(())
}

#[tokio::test]
async fn replacing_a_session_fails_its_inflight_requests_instead_of_hanging() -> Result<()> {
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let peer = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let (presence, handshake, outgoing) = registration(&receiver, &peer);
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: peer.id }))?;

    let requester = receiver.clone();
    let peer_id = peer.id;
    let pending = tokio::spawn(async move {
        requester
            .request(
                peer_id,
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::Get { collection: proto::CollectionId::from("replacement"), ids: vec![] },
            )
            .await
    });
    tokio::task::yield_now().await;

    let (presence, handshake, outgoing) = registration(&receiver, &peer);
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: peer.id }))?;
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), pending).await??;
    assert!(matches!(result, Err(RequestError::ConnectionLost)));
    Ok(())
}

#[tokio::test]
async fn peer_frame_verification_rejects_forgery_and_bounds_replay_memory() -> Result<()> {
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let peer_key = SigningKey::from_bytes(&[0xD2; 32]);
    let peer = Node::new_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), peer_key.clone());
    let (presence, handshake, outgoing) = registration(&receiver, &peer);
    let session = handshake.challenge();
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: peer.id }))?;

    let attacker_key = SigningKey::from_bytes(&[0xD3; 32]);
    let forged = signed_get_frame(&attacker_key, session, 0, peer.id, receiver.id);
    assert!(matches!(
        receiver.verify_peer_message(peer.id, forged),
        Err(PeerFrameError::InvalidSignature { peer: id }) if id == peer.id
    ));

    // Transport scheduling may deliver unique frames out of order, so the
    // bounded window accepts both while remembering each sequence exactly.
    receiver.verify_peer_message(peer.id, signed_get_frame(&peer_key, session, 2, peer.id, receiver.id))?;
    receiver.verify_peer_message(peer.id, signed_get_frame(&peer_key, session, 1, peer.id, receiver.id))?;

    // Advancing the high-water mark evicts sequence zero from the 4096-frame
    // window. It must be stale rather than reusable, even though it was never
    // accepted earlier.
    receiver.verify_peer_message(peer.id, signed_get_frame(&peer_key, session, 4096, peer.id, receiver.id))?;
    assert!(matches!(
        receiver.verify_peer_message(peer.id, signed_get_frame(&peer_key, session, 0, peer.id, receiver.id)),
        Err(PeerFrameError::StaleSequence { peer: id, sequence: 0 }) if id == peer.id
    ));
    Ok(())
}

#[tokio::test]
async fn authenticated_peer_cannot_spoof_another_messages_sender() -> Result<()> {
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let node_a = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let node_b_key = SigningKey::from_bytes(&[0xB2; 32]);
    let node_b = Node::new_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), node_b_key.clone());

    // B has completed the signed-Presence handshake for this connection.
    let (presence, handshake, outgoing_session) = registration(&receiver, &node_b);
    let incoming_session = handshake.challenge();
    receiver.register_peer(presence, handshake, outgoing_session, Box::new(NullSender { recipient: node_b.id }))?;

    // The same connection now submits a request whose envelope claims A as
    // its sender. The channel binding must reject it before request dispatch.
    let message = proto::NodeMessage::Request {
        auth: vec![],
        request: proto::NodeRequest {
            id: proto::RequestId::new(),
            to: receiver.id,
            from: node_a.id,
            body: proto::NodeRequestBody::Get { collection: proto::CollectionId::from("spoof-target"), ids: vec![] },
        },
    };
    let signature = node_b_key.sign(&proto::SignedPeerMessage::signable_bytes(incoming_session, 0, &message)).into();
    let frame = proto::SignedPeerMessage { session: incoming_session, sequence: 0, message, signature };
    let error = receiver.verify_peer_message(node_b.id, frame).expect_err("B must not be allowed to speak as A");
    assert!(matches!(
        error,
        PeerFrameError::IdentityMismatch(PeerIdentityMismatch { authenticated_peer, declared_sender })
            if authenticated_peer == node_b.id && declared_sender == node_a.id
    ));
    Ok(())
}

#[tokio::test]
async fn unrecognized_durable_claimant_is_not_added_to_durable_peers() -> Result<()> {
    let receiver = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    receiver.system.create().await?;

    // A genuine signature proves control of this node key, but the separate
    // system makes it an unrecognized durable for `receiver`.
    let foreign = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    foreign.system.create().await?;
    let (presence, handshake, outgoing_session) = registration(&receiver, &foreign);
    assert!(presence.verify_for(handshake.challenge()));

    receiver.register_peer(presence, handshake, outgoing_session, Box::new(NullSender { recipient: foreign.id }))?;
    assert!(!receiver.get_durable_peers().contains(&foreign.id));
    assert!(receiver.get_durable_peer_random().is_none());
    Ok(())
}

#[tokio::test]
async fn replacing_a_founder_connection_cannot_inherit_durable_routing() -> Result<()> {
    let founder_key = SigningKey::from_bytes(&[0xF0; 32]);
    let founder = Node::new_durable_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), founder_key.clone());
    founder.system.create().await?;

    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    receiver.system.wait_loaded().await;
    let (presence, handshake, outgoing) = registration(&receiver, &founder);
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: founder.id }))?;
    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.system.wait_system_ready()).await?;
    assert_eq!(receiver.get_durable_peers(), vec![founder.id]);

    // The same key may found another system, but that valid signature must
    // not let the conflicting connection replace this system's founder route.
    let foreign = Node::new_durable_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), founder_key.clone());
    foreign.system.create().await?;
    assert_ne!(foreign.system.root_id(), founder.system.root_id());
    let (presence, handshake, outgoing) = registration(&receiver, &foreign);
    assert!(matches!(
        receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: foreign.id })),
        Err(proto::PresenceRefusal::InvalidSystemRoot(id)) if id == founder.id
    ));
    assert_eq!(receiver.get_durable_peers(), vec![founder.id]);

    // A signed non-durable replacement is a valid peer connection, but it
    // must actively remove the old durable classification for that NodeId.
    let ephemeral = Node::new_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), founder_key);
    let (presence, handshake, outgoing) = registration(&receiver, &ephemeral);
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: ephemeral.id }))?;
    assert!(receiver.get_durable_peers().is_empty());
    assert!(receiver.get_durable_peer_random().is_none());
    Ok(())
}

#[tokio::test]
async fn signed_presence_with_unproven_root_state_is_rejected() -> Result<()> {
    let founder = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    founder.system.create().await?;
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    receiver.system.wait_loaded().await;
    let (mut presence, handshake, outgoing_session) = registration(&receiver, &founder);
    let challenge = handshake.challenge();
    let proof = presence.system_root.as_mut().expect("initialized durable advertises a root proof");

    // Presence signs the content-addressed root id. Keeping that id/genesis but
    // substituting an arbitrary current state therefore leaves the signature
    // valid and specifically exercises core's proof validation.
    proof.state.payload.state.head = proto::Clock::default();
    assert!(presence.verify_for(challenge), "root-state substitution does not alter the signed root id projection");

    let result = receiver.register_peer(presence, handshake, outgoing_session, Box::new(NullSender { recipient: founder.id }));
    assert!(matches!(result, Err(proto::PresenceRefusal::InvalidSystemRoot(id)) if id == founder.id));
    assert!(receiver.get_durable_peers().is_empty());
    assert!(receiver.system.root().is_none(), "an invalid proof must not reserve a root");
    Ok(())
}

#[tokio::test]
async fn first_join_peer_cannot_send_before_root_persistence_completes() -> Result<()> {
    let founder_key = SigningKey::from_bytes(&[0xE1; 32]);
    let founder = Node::new_durable_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), founder_key.clone());
    founder.system.create().await?;
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    receiver.system.wait_loaded().await;

    let (presence, handshake, outgoing) = registration(&receiver, &founder);
    let session = handshake.challenge();
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: founder.id }))?;
    let frame = signed_get_frame(&founder_key, session, 0, founder.id, receiver.id);
    assert!(matches!(
        receiver.verify_peer_message(founder.id, frame),
        Err(PeerFrameError::PeerNotReady { peer: id }) if id == founder.id
    ));
    assert!(receiver.get_durable_peers().is_empty());

    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.system.wait_system_ready()).await?;
    let frame = signed_get_frame(&founder_key, session, 0, founder.id, receiver.id);
    receiver.verify_peer_message(founder.id, frame)?;
    assert_eq!(receiver.get_durable_peers(), vec![founder.id]);
    Ok(())
}

#[tokio::test]
async fn nonfounder_peer_cannot_poison_the_catalog_with_shipped_schema() -> Result<()> {
    let founder = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    founder.system.create().await?;
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    receiver.system.wait_loaded().await;

    let founder_handshake = receiver.begin_peer_handshake();
    let founder_session = founder_handshake.challenge();
    let founder_outgoing = founder.begin_peer_handshake().challenge();
    let founder_presence = founder.presence(founder_session);
    let root = founder_presence.system_root.clone().expect("founder advertises its root proof");
    receiver.register_peer(founder_presence, founder_handshake, founder_outgoing, Box::new(NullSender { recipient: founder.id }))?;
    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.system.wait_system_ready()).await?;

    let attacker_key = SigningKey::from_bytes(&[0xE2; 32]);
    let attacker = Node::new_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), attacker_key.clone());
    let (presence, handshake, outgoing) = registration(&receiver, &attacker);
    let attacker_session = handshake.challenge();
    receiver.register_peer(presence, handshake, outgoing, Box::new(NullSender { recipient: attacker.id }))?;

    let before = receiver.catalog.counts();
    let message = proto::NodeMessage::Update(proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: attacker.id,
        to: receiver.id,
        body: proto::NodeUpdateBody::SubscriptionUpdate { items: vec![] },
        schema: vec![proto::StateWithGenesis { genesis: root.genesis.into(), state: root.state }],
    });
    let signature = attacker_key.sign(&proto::SignedPeerMessage::signable_bytes(attacker_session, 0, &message)).into();
    receiver
        .handle_peer_message(attacker.id, proto::SignedPeerMessage { session: attacker_session, sequence: 0, message, signature })
        .await?;

    assert_eq!(receiver.catalog.counts(), before, "non-founder schema envelope must leave the catalog untouched");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_first_roots_reserve_exactly_one_durable_founder() -> Result<()> {
    let founder_a = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    founder_a.system.create().await?;
    let founder_b = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    founder_b.system.create().await?;
    let receiver = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    receiver.system.wait_loaded().await;
    let (presence_a, handshake_a, outgoing_a) = registration(&receiver, &founder_a);
    let (presence_b, handshake_b, outgoing_b) = registration(&receiver, &founder_b);
    let founder_a_id = founder_a.id;
    let founder_b_id = founder_b.id;
    let root_a = presence_a.system_root.as_ref().unwrap().entity_id();
    let root_b = presence_b.system_root.as_ref().unwrap().entity_id();
    assert_ne!(root_a, root_b);

    let barrier = Arc::new(Barrier::new(3));

    let task_a = {
        let receiver = receiver.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            receiver.register_peer(presence_a, handshake_a, outgoing_a, Box::new(NullSender { recipient: founder_a_id }))
        })
    };
    let task_b = {
        let receiver = receiver.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            receiver.register_peer(presence_b, handshake_b, outgoing_b, Box::new(NullSender { recipient: founder_b_id }))
        })
    };
    barrier.wait().await;
    task_a.await??;
    task_b.await??;

    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.system.wait_system_ready()).await?;
    let durables = receiver.get_durable_peers();
    assert_eq!(durables.len(), 1, "only the atomically reserved root may enter durable routing");
    let winner = *durables.iter().next().unwrap();
    let expected_root = if winner == founder_a_id {
        root_a
    } else if winner == founder_b_id {
        root_b
    } else {
        panic!("unexpected winner")
    };
    assert_eq!(receiver.system.root_id(), Some(expected_root));
    Ok(())
}

#[tokio::test]
async fn transplanted_attestation_is_stripped_before_policy_sees_it() -> Result<()> {
    let server_agent = RecordingAgent::new(true);
    let client_agent = RecordingAgent::new(false);
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent.clone());

    let _connection = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;
    let ctx_s = server.context(DEFAULT_CONTEXT)?;
    let ctx_c = client.context(DEFAULT_CONTEXT)?;

    // Establish a relay context without matching the entity we create below.
    let _relay = ctx_c.query_wait::<IdentityRecordView>("title = 'never-match'").await?;
    let record_id = {
        let trx = ctx_s.begin();
        let record = trx.create(&IdentityRecord { title: "before".to_owned() }).await?;
        let id = record.id();
        trx.commit().await?;
        id
    };
    let view = ctx_c.get::<IdentityRecordView>(record_id).await?;
    assert_eq!(view.title()?, "before");

    let model = server.catalog.model_id_for(IdentityRecord::collection().as_str()).expect("model registered");
    let title = server.catalog.resolve(IdentityRecord::collection().as_str(), "title").expect("title registered");
    let server_collection = ctx_s.collection(&IdentityRecord::collection()).await?;
    let genesis = server_collection
        .dump_entity_events(record_id)
        .await?
        .into_iter()
        .find(|event| event.payload.is_entity_create())
        .expect("stored genesis");
    let transplanted = genesis.attestations.first().expect("attesting agent signed genesis").clone();

    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(title), Some(Value::String("after".to_owned())));
    let operations = backend.to_operations()?.expect("LWW write produces operations");
    let update = proto::Event {
        entity_id: record_id,
        model,
        parent: view.entity().head().clone(),
        body: proto::EventBody::Update { operations: proto::OperationSet(BTreeMap::from([("lww".to_owned(), operations)])) },
    };
    assert!(!transplanted.matches_event(&update), "the genuine envelope names the genesis, not this update");
    let update_id = update.id();

    // Ignore any earlier setup traffic. The assertion below concerns exactly
    // the forged wire delivery.
    client_agent.take_received_counts();
    let invalid_before = client.invalid_attestation_count();
    let fragment: proto::EventFragment = Attested { payload: update, attestations: proto::AttestationSet(vec![transplanted]) }.into();
    client
        .handle_message(proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: server.id,
            to: client.id,
            body: proto::NodeUpdateBody::SubscriptionUpdate {
                items: vec![proto::SubscriptionUpdateItem {
                    entity_id: record_id,
                    model,
                    content: proto::UpdateContent::EventOnly(vec![fragment]),
                    predicate_relevance: vec![],
                }],
            },
            schema: vec![],
        }))
        .await?;

    let seen = client_agent.take_received_counts();
    assert!(!seen.is_empty(), "policy must be invoked for the delivered event");
    assert!(seen.iter().all(|count| *count == 0), "policy must see payloads only after core strips the transplant; observed {seen:?}");
    assert_eq!(client.invalid_attestation_count(), invalid_before + 1);
    assert_eq!(view.title()?, "after", "the valid payload remains admissible after its invalid envelope is stripped");

    let client_collection = ctx_c.collection(&IdentityRecord::collection()).await?;
    let stored = client_collection
        .dump_entity_events(record_id)
        .await?
        .into_iter()
        .find(|event| event.payload.id() == update_id)
        .expect("accepted update stored");
    assert!(stored.attestations.is_empty(), "the transplanted envelope must not reach storage");
    Ok(())
}

#[tokio::test]
async fn genesis_id_mismatch_is_rejected_at_remote_commit_ingress() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context(DEFAULT_CONTEXT)?;
    let trx = context.begin();
    trx.create(&IdentityRecord { title: "register-model".to_owned() }).await?;
    trx.commit().await?;

    let model = node.catalog.model_id_for(IdentityRecord::collection().as_str()).expect("model registered");
    let system = node.system.root().expect("system root").payload.entity_id;
    let mut malformed = proto::Event::genesis(model, Some(system), proto::OperationSet(BTreeMap::new()));
    malformed.entity_id = proto::EntityId::from_bytes([0xA5; 32]);
    assert!(matches!(malformed.validate_structure(), Err(proto::EventStructureError::GenesisIdMismatch { .. })));

    let result = node.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(malformed, None)]).await;
    assert!(result.is_err(), "remote commit must reject a genesis whose claimed entity id is not its derived id");
    Ok(())
}

#[tokio::test]
async fn cross_system_genesis_replay_is_rejected() -> Result<()> {
    let foreign = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    foreign.system.create().await?;
    let foreign_system = foreign.system.root().expect("foreign root").payload.entity_id;

    let receiver = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    receiver.system.create().await?;
    let receiver_system = receiver.system.root().expect("receiver root").payload.entity_id;
    assert_ne!(foreign_system, receiver_system);
    let context = receiver.context(DEFAULT_CONTEXT)?;
    let trx = context.begin();
    trx.create(&IdentityRecord { title: "register-model".to_owned() }).await?;
    trx.commit().await?;
    let receiver_model = receiver.catalog.model_id_for(IdentityRecord::collection().as_str()).expect("model registered");

    // This genesis is internally self-consistent and therefore survives the
    // ordinary ID recomputation check. It must still be refused because its
    // signed preimage names a different system root.
    let replay = proto::Event::genesis(receiver_model, Some(foreign_system), proto::OperationSet(BTreeMap::new()));
    replay.validate_structure()?;
    let replay_id = replay.entity_id;
    let result = receiver.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(replay, None)]).await;
    assert!(result.is_err(), "a valid genesis bound to another system must not be admitted");

    let collection = context.collection(&IdentityRecord::collection()).await?;
    assert!(collection.dump_entity_events(replay_id).await?.is_empty(), "rejected replay must not touch storage");
    Ok(())
}

#[tokio::test]
async fn founder_state_attestation_survives_ephemeral_storage() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), RecordingAgent::new(true));
    server.system.create().await?;
    let server_context = server.context(DEFAULT_CONTEXT)?;
    let record_id = {
        let trx = server_context.begin();
        let record = trx.create(&IdentityRecord { title: "portable".to_owned() }).await?;
        let id = record.id();
        trx.commit().await?;
        id
    };

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;
    let client_context = client.context(DEFAULT_CONTEXT)?;
    let fetched = client_context.fetch::<IdentityRecordView>("title = 'portable'").await?;
    assert_eq!(fetched.len(), 1, "the founder-attested snapshot must materialize on the client");

    let collection = client_context.collection(&IdentityRecord::collection()).await?;
    let stored = collection.get_state(record_id).await?;
    assert!(stored
        .attestations
        .iter()
        .any(|envelope| { envelope.attester == server.id && envelope.matches_state(&stored.payload) && envelope.verify_signature() }));
    Ok(())
}

#[tokio::test]
async fn denied_remote_genesis_leaves_no_storage_or_resident_phantom() -> Result<()> {
    let agent = RecordingAgent::new(false);
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    let context = node.context(DEFAULT_CONTEXT)?;

    // Register the model while the agent still allows writes.
    let trx = context.begin();
    trx.create(&IdentityRecord { title: "seed".to_owned() }).await?;
    trx.commit().await?;

    let model = node.catalog.model_id_for(IdentityRecord::collection().as_str()).expect("model registered");
    let system = node.system.root_id().expect("system root");
    let denied = proto::Event::genesis(model, Some(system), proto::OperationSet(BTreeMap::new()));
    let denied_id = denied.entity_id;
    agent.deny_events();

    let result = node.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(denied, None)]).await;
    assert!(result.is_err(), "policy must reject the remote genesis");
    assert!(node.get_resident_entity(denied_id).is_none(), "denial must evict the empty resident placeholder");

    let collection = context.collection(&IdentityRecord::collection()).await?;
    assert!(collection.dump_entity_events(denied_id).await?.is_empty(), "denial must not persist the event");
    assert!(collection.get_state(denied_id).await.is_err(), "denial must not persist state");
    Ok(())
}

#[tokio::test]
async fn state_snapshot_with_mismatched_genesis_cannot_create_an_entity() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;
    let client_context = client.context(DEFAULT_CONTEXT)?;
    let _relay = client_context.query_wait::<IdentityRecordView>("title = 'never-match'").await?;

    let model = server.catalog.model_id_for(IdentityRecord::collection().as_str()).expect("model registered by query");
    let forged_id = proto::EntityId::from_bytes([0xC7; 32]);
    let wrong_genesis =
        proto::Event::genesis(model, Some(server.system.root_id().expect("server system root")), proto::OperationSet(BTreeMap::new()));
    assert_ne!(wrong_genesis.entity_id, forged_id);
    let wrong_genesis_id = wrong_genesis.id();
    let proof = proto::StateWithGenesis {
        genesis: Attested::opt(wrong_genesis, None),
        state: Attested::opt(proto::EntityState { entity_id: forged_id, model, state: proto::State::default() }, None),
    };
    client
        .handle_message(proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: server.id,
            to: client.id,
            body: proto::NodeUpdateBody::SubscriptionUpdate {
                items: vec![proto::SubscriptionUpdateItem {
                    entity_id: forged_id,
                    model,
                    content: proto::UpdateContent::StateAndEvent(proof, vec![]),
                    predicate_relevance: vec![],
                }],
            },
            schema: vec![],
        }))
        .await?;

    assert!(client.get_resident_entity(forged_id).is_none());
    let collection = client_context.collection(&IdentityRecord::collection()).await?;
    assert!(collection.get_state(forged_id).await.is_err());
    assert!(
        collection.get_events(vec![wrong_genesis_id]).await?.is_empty(),
        "a mismatched inline identity proof must be rejected before it enters the cache"
    );
    Ok(())
}
