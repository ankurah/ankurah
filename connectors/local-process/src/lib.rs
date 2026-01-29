use ankurah_core::policy::PolicyAgent;
use ankurah_core::storage::StorageEngine;
use ankurah_proto as proto;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

use ankurah_core::connector::{PeerSender, SendError};
use ankurah_core::node::{Node, WeakNode};

#[derive(Clone)]
/// Sender for local process connection
pub struct LocalProcessSender {
    sender: mpsc::Sender<proto::NodeMessage>,
    node_id: proto::EntityId,
}

#[async_trait]
impl PeerSender for LocalProcessSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.sender.try_send(message).map_err(|_| SendError::ConnectionClosed)?;
        Ok(())
    }

    fn recipient_node_id(&self) -> proto::EntityId { self.node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

pub type MessageTransform = Arc<dyn Fn(proto::NodeMessage) -> proto::NodeMessage + Send + Sync>;

fn identity_transform() -> MessageTransform { Arc::new(|message| message) }

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
    node1_id: proto::EntityId,
    node2_id: proto::EntityId,
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
        Self::new_with_transform(node1, node2, identity_transform(), identity_transform()).await
    }

    /// Create a new LocalConnector with optional message transforms per direction.
    /// `to_node1` runs on messages delivered to node1, `to_node2` on messages delivered to node2.
    pub async fn new_with_transform(
        node1: &Node<SE1, PA1>,
        node2: &Node<SE2, PA2>,
        to_node1: MessageTransform,
        to_node2: MessageTransform,
    ) -> anyhow::Result<Self> {
        let (node1_tx, node1_rx) = mpsc::channel(1024);
        let (node2_tx, node2_rx) = mpsc::channel(1024);

        // we have to register the senders with the nodes
        node1.register_peer(
            proto::Presence { node_id: node2.id, durable: node2.durable, system_root: node2.system.root() },
            Box::new(LocalProcessSender { sender: node2_tx, node_id: node2.id }),
        );
        node2.register_peer(
            proto::Presence { node_id: node1.id, durable: node1.durable, system_root: node1.system.root() },
            Box::new(LocalProcessSender { sender: node1_tx, node_id: node1.id }),
        );

        let receiver1_task = Self::setup_receiver(node1.clone(), node1_rx, to_node1);
        let receiver2_task = Self::setup_receiver(node2.clone(), node2_rx, to_node2);

        Ok(Self { node1: node1.weak(), node2: node2.weak(), node1_id: node1.id, node2_id: node2.id, receiver1_task, receiver2_task })
    }

    fn setup_receiver<SE, PA>(
        node: Node<SE, PA>,
        mut rx: mpsc::Receiver<proto::NodeMessage>,
        transform: MessageTransform,
    ) -> tokio::task::JoinHandle<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let node = node.clone();
                let message = (transform)(message);
                tokio::spawn(async move {
                    let _ = node.handle_message(message).await;
                });
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
            node1.deregister_peer(self.node2_id);
        }
        if let Some(node2) = self.node2.upgrade() {
            node2.deregister_peer(self.node1_id);
        }
    }
}
