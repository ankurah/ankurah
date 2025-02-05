use ankurah_core::{
    connector::{PeerSender, SendError},
    node::Node,
    proto,
    traits::{Context, PolicyAgent},
};

use async_trait::async_trait;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

#[derive(Clone)]
/// Sender for local process connection
pub struct LocalProcessSender {
    sender: mpsc::Sender<proto::NodeMessage>,
    node_id: proto::NodeId,
}

#[async_trait]
impl PeerSender for LocalProcessSender {
    async fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.sender.send(message).await.map_err(|_| SendError::ConnectionClosed)?;
        Ok(())
    }

    fn recipient_node_id(&self) -> proto::NodeId { self.node_id.clone() }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// connector which establishes one sender between each of the two given nodes
pub struct LocalProcessConnection<Ctx: Context + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> {
    receiver1_task: tokio::task::JoinHandle<()>,
    receiver2_task: tokio::task::JoinHandle<()>,
    node1: Weak<Node<Ctx, PA>>,
    node2: Weak<Node<Ctx, PA>>,
    node1_id: proto::NodeId,
    node2_id: proto::NodeId,
}

impl<Ctx: Context + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> LocalProcessConnection<Ctx, PA> {
    /// Create a new LocalConnector and establish connection between the nodes
    pub async fn new(node1: &Arc<Node<Ctx, PA>>, node2: &Arc<Node<Ctx, PA>>) -> anyhow::Result<Self> {
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

        let receiver1_task = Self::setup_receiver(node1, node1_rx);
        let receiver2_task = Self::setup_receiver(node2, node2_rx);

        Ok(Self {
            node1: Arc::downgrade(node1),
            node2: Arc::downgrade(node2),
            node1_id: node1.id.clone(),
            node2_id: node2.id.clone(),
            receiver1_task,
            receiver2_task,
        })
    }

    fn setup_receiver(node: &Arc<Node<Ctx, PA>>, mut rx: mpsc::Receiver<proto::NodeMessage>) -> tokio::task::JoinHandle<()> {
        let node = node.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let _ = node.handle_message(message).await;
            }
        })
    }
}

impl<Ctx: Context + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> Drop for LocalProcessConnection<Ctx, PA> {
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
