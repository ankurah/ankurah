mod common;

use ankurah::{Model, Node};
use ankurah_core::connector::{PeerProvenance, PeerSender, SendError};
use ankurah_core::node::NodeKey;
use ankurah_core::test_helpers::keyed_peer_claims;
use ankurah_jwt_auth::{JwtAgent, JwtContext};
use ankurah_proto as proto;
use ankurah_storage_sled::SledStorageEngine;
use std::sync::{mpsc, Arc};
use std::time::Duration;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Post {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub body: String,
}

#[derive(Clone)]
struct RecordingSender {
    peer: proto::NodeId,
    sent: mpsc::Sender<proto::NodeMessage>,
}

impl PeerSender for RecordingSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.sent.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn recipient_node_id(&self) -> proto::NodeId { self.peer }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// The credentials a frame carries; only requests carry any.
fn auth_data(message: &proto::NodeMessage) -> &[proto::AuthData] {
    match message {
        proto::NodeMessage::Request { auth, .. } => auth,
        proto::NodeMessage::Response(_)
        | proto::NodeMessage::Update(_)
        | proto::NodeMessage::UpdateAck(_)
        | proto::NodeMessage::UnsubscribeQuery { .. } => &[],
    }
}

fn is_commit_relay(message: &proto::NodeMessage) -> bool {
    match message {
        proto::NodeMessage::Request { request, .. } => {
            matches!(request.body.decode(), Ok(proto::NodeRequestBody::CommitTransaction { .. }))
        }
        _ => false,
    }
}

/// A peer that connects to a durable node and claims to be durable is served
/// as a client: it never becomes a relay target, sees no local commit or
/// credential, and cannot make a root context commit fail.
#[tokio::test]
async fn inbound_durable_claim_is_not_a_relay_target() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), common::blog_config_path())?;
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    agent.set_policy(&node, &agent.config()).await?;
    node.wait_ready().await?;

    let key = NodeKey::generate();
    let peer = key.node_id();
    let (sent, relayed) = mpsc::channel();
    let (handshake, claims) = keyed_peer_claims(node.id, &key, true);
    node.register_peer(key.sign_presence(claims), &handshake, PeerProvenance::Inbound, Box::new(RecordingSender { peer, sent })).await?;
    let durable_peers = node.get_durable_peers();

    let root = node.context_async(JwtContext::system()).await?;
    let trx = root.begin();
    trx.create(&Post { title: "Root post".into(), body: "committed under the root context".into() }).await?;
    let root_commit = tokio::time::timeout(Duration::from_secs(5), trx.commit()).await;

    let claims = common::make_claims("editor-1", &["Editor"], "editor@blog.com");
    let token = common::sign_token(&keys, &claims);
    let editor = node.context_async(JwtContext::from_claims(claims, token)).await?;
    let trx = editor.begin();
    trx.create(&Post { title: "Editor post".into(), body: "committed under a user context".into() }).await?;
    let editor_commit = tokio::time::timeout(Duration::from_secs(5), trx.commit()).await;

    let frames: Vec<_> = relayed.try_iter().collect();
    let relays = frames.iter().filter(|frame| is_commit_relay(frame)).count();
    let credentials: Vec<_> = frames.iter().flat_map(auth_data).filter(|auth| !auth.0.is_empty()).collect();
    assert!(durable_peers.is_empty(), "the inbound peer became a relay target: {durable_peers:?}");
    assert!(matches!(root_commit, Ok(Ok(_))), "the root context commit failed while the peer was connected: {root_commit:?}");
    assert!(matches!(editor_commit, Ok(Ok(_))), "the user commit did not complete: {editor_commit:?}");
    assert_eq!(relays, 0, "the inbound peer received local commits");
    assert!(credentials.is_empty(), "frames sent to the inbound peer carried bearer credentials: {credentials:?}");
    Ok(())
}
