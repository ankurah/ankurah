use ankurah_core::policy::PolicyAgent;
use ankurah_core::storage::StorageEngine;
use ankurah_proto as proto;
use async_trait::async_trait;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

use ankurah_core::connector::{PeerSender, SendError};
use ankurah_core::node::{Node, WeakNode};

#[derive(Clone)]
/// Sender for local process connection
pub struct LocalProcessSender {
    sender: mpsc::Sender<proto::NodeMessage>,
    node_id: proto::ID,
}

#[async_trait]
impl PeerSender for LocalProcessSender {
    async fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.sender.send(message).await.map_err(|_| SendError::ConnectionClosed)?;
        Ok(())
    }

    fn recipient_node_id(&self) -> proto::ID { self.node_id.clone() }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// connector which establishes one sender between each of the two given nodes
pub struct LocalProcessConnection<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    receiver1_task: tokio::task::JoinHandle<()>,
    receiver2_task: tokio::task::JoinHandle<()>,
    node1: WeakNode<SE, PA>,
    node2: WeakNode<SE, PA>,
    node1_id: proto::ID,
    node2_id: proto::ID,
}

impl<SE, PA> LocalProcessConnection<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Create a new LocalConnector and establish connection between the nodes
    pub async fn new(node1: &Node<SE, PA>, node2: &Node<SE, PA>) -> anyhow::Result<Self> {
        let (node1_tx, node1_rx) = mpsc::channel(100);
        let (node2_tx, node2_rx) = mpsc::channel(100);

        // we have to register the senders with the nodes
        node1.register_peer(
            proto::Presence { node_id: node2.id.clone(), durable: node2.durable },
            Box::new(LocalProcessSender { sender: node2_tx, node_id: node2.id.clone() }),
        );
        node2.register_peer(
            proto::Presence { node_id: node1.id.clone(), durable: node1.durable },
            Box::new(LocalProcessSender { sender: node1_tx, node_id: node1.id.clone() }),
        );

        let receiver1_task = Self::setup_receiver(node1.clone(), node1_rx);
        let receiver2_task = Self::setup_receiver(node2.clone(), node2_rx);

        Ok(Self {
            node1: node1.weak(),
            node2: node2.weak(),
            node1_id: node1.id.clone(),
            node2_id: node2.id.clone(),
            receiver1_task,
            receiver2_task,
        })
    }

    fn setup_receiver(node: Node<SE, PA>, mut rx: mpsc::Receiver<proto::NodeMessage>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let _ = node.handle_message(message).await;
            }
        })
    }
}

impl<SE, PA> Drop for LocalProcessConnection<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.receiver1_task.abort();
        self.receiver2_task.abort();
        if let Some(node1) = self.node1.upgrade() {
            node1.deregister_peer(self.node2_id.clone());
        }
        if let Some(node2) = self.node2.upgrade() {
            node2.deregister_peer(self.node1_id.clone());
        }
    }
}
