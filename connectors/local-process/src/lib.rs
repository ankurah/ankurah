use ankurah_core::policy::PolicyAgent;
use ankurah_core::storage::StorageEngine;
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::mpsc;

use ankurah_core::connector::{PeerSender, SendError};
use ankurah_core::node::{Node, WeakNode};

#[derive(Clone)]
/// Sender for local process connection
pub struct LocalProcessSender {
    sender: mpsc::Sender<proto::SignedPeerMessage>,
    node_id: proto::NodeId,
}

#[async_trait]
impl PeerSender for LocalProcessSender {
    fn send_message(&self, message: proto::SignedPeerMessage) -> Result<(), SendError> {
        self.sender.try_send(message).map_err(|_| SendError::ConnectionClosed)?;
        Ok(())
    }

    fn recipient_node_id(&self) -> proto::NodeId { self.node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// connector which establishes one sender between each of the two given nodes
pub struct LocalProcessConnection<SE1, PA1, SE2, PA2>
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    receiver1_task: tokio::task::JoinHandle<()>,
    receiver2_task: tokio::task::JoinHandle<()>,
    node1: WeakNode<SE1, PA1>,
    node2: WeakNode<SE2, PA2>,
    node1_id: proto::NodeId,
    node2_id: proto::NodeId,
    node1_incoming_session: proto::HandshakeChallenge,
    node2_incoming_session: proto::HandshakeChallenge,
}

impl<SE1, PA1, SE2, PA2> LocalProcessConnection<SE1, PA1, SE2, PA2>
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    /// Create a new LocalConnector and establish connection between the nodes
    pub async fn new(node1: &Node<SE1, PA1>, node2: &Node<SE2, PA2>) -> anyhow::Result<Self> {
        let (node1_tx, node1_rx) = mpsc::channel(1024);
        let (node2_tx, node2_rx) = mpsc::channel(1024);

        // Exercise the same challenge-bound identity proof as network
        // transports even though both peers share this trusted process.
        let node1_handshake = node1.begin_peer_handshake();
        let node2_handshake = node2.begin_peer_handshake();
        let node1_challenge = node1_handshake.challenge();
        let node2_challenge = node2_handshake.challenge();
        let node2_presence = node2.presence(node1_challenge);
        let node1_presence = node1.presence(node2_challenge);
        node1.register_peer(
            node2_presence,
            node1_handshake,
            node2_challenge,
            Box::new(LocalProcessSender { sender: node2_tx, node_id: node2.id }),
        )?;

        if let Err(rejection) = node2.register_peer(
            node1_presence,
            node2_handshake,
            node1_challenge,
            Box::new(LocalProcessSender { sender: node1_tx, node_id: node1.id }),
        ) {
            node1.deregister_peer_session(node2.id, node1_challenge);
            return Err(rejection.into());
        }

        let receiver1_task = Self::setup_receiver(node1.clone(), node2.id, node1_rx);
        let receiver2_task = Self::setup_receiver(node2.clone(), node1.id, node2_rx);

        Ok(Self {
            node1: node1.weak(),
            node2: node2.weak(),
            node1_id: node1.id,
            node2_id: node2.id,
            node1_incoming_session: node1_challenge,
            node2_incoming_session: node2_challenge,
            receiver1_task,
            receiver2_task,
        })
    }

    fn setup_receiver<SE, PA>(
        node: Node<SE, PA>,
        authenticated_peer: proto::NodeId,
        mut rx: mpsc::Receiver<proto::SignedPeerMessage>,
    ) -> tokio::task::JoinHandle<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let message = match node.verify_peer_message(authenticated_peer, message) {
                    Ok(message) => message,
                    Err(_) => break,
                };
                let _ = node.handle_verified_peer_message(message).await;
            }
        })
    }
}

impl<SE1, PA1, SE2, PA2> Drop for LocalProcessConnection<SE1, PA1, SE2, PA2>
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.receiver1_task.abort();
        self.receiver2_task.abort();
        if let Some(node1) = self.node1.upgrade() {
            node1.deregister_peer_session(self.node2_id, self.node1_incoming_session);
        }
        if let Some(node2) = self.node2.upgrade() {
            node2.deregister_peer_session(self.node1_id, self.node2_incoming_session);
        }
    }
}
