use ankurah_proto as proto;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::{PeerSender, SendError};
use crate::node::Node;

#[derive(Clone)]
/// Sender for local process connection
pub struct LocalProcessSender {
    node_id: proto::NodeId,
    sender: mpsc::Sender<proto::PeerMessage>,
}

#[async_trait]
impl PeerSender for LocalProcessSender {
    fn node_id(&self) -> proto::NodeId {
        self.node_id.clone()
    }

    async fn send_message(&self, message: proto::PeerMessage) -> Result<(), SendError> {
        self.sender
            .send(message)
            .await
            .map_err(|_| SendError::ConnectionClosed)?;
        Ok(())
    }

    fn cloned(&self) -> Box<dyn PeerSender> {
        Box::new(self.clone())
    }
}

/// connector which establishes one sender between each of the two given nodes
pub struct LocalProcessConnection {
    receiver1_task: tokio::task::JoinHandle<()>,
    receiver2_task: tokio::task::JoinHandle<()>,
}

impl LocalProcessConnection {
    /// Create a new LocalConnector and establish connection between the nodes
    pub async fn new(node1: &Arc<Node>, node2: &Arc<Node>) -> anyhow::Result<Self> {
        let (node1_tx, node1_rx) = mpsc::channel(100);
        let (node2_tx, node2_rx) = mpsc::channel(100);

        // we have to register the senders with the nodes
        node1
            .register_peer(Box::new(LocalProcessSender {
                node_id: node2.id.clone(),
                sender: node2_tx,
            }))
            .await;
        node2
            .register_peer(Box::new(LocalProcessSender {
                node_id: node1.id.clone(),
                sender: node1_tx,
            }))
            .await;

        Ok(Self {
            receiver1_task: Self::setup_receiver(node1, node1_rx),
            receiver2_task: Self::setup_receiver(node2, node2_rx),
        })
    }

    fn setup_receiver(
        node: &Arc<Node>,
        mut rx: mpsc::Receiver<proto::PeerMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let node = node.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let _ = node.handle_message(message).await;
            }
        })
    }
}

impl Drop for LocalProcessConnection {
    fn drop(&mut self) {
        self.receiver1_task.abort();
        self.receiver2_task.abort();
        // TODO - deregister the senders from the nodes
    }
}
