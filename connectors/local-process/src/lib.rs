use ankurah_core::policy::PolicyAgent;
use ankurah_core::signals::{Read, Wait};
use ankurah_core::storage::StorageEngine;
use ankurah_core::NodeState;
use ankurah_proto::{
    self as proto,
    ConnectionSide::{Accepting, Dialing},
};
use async_trait::async_trait;
use tokio::sync::mpsc;

use ankurah_core::connector::{ConnectionId, PeerSender, SendError};
use ankurah_core::node::{Node, WeakNode};

#[derive(Clone)]
/// Sender for local process connection
pub struct LocalProcessSender {
    sender: mpsc::Sender<proto::NodeMessage>,
}

#[async_trait]
impl PeerSender for LocalProcessSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.sender.try_send(message).map_err(|_| SendError::ConnectionClosed)?;
        Ok(())
    }

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
    /// node1's connection to node2.
    connection1: ConnectionId,
    /// node2's connection to node1.
    connection2: ConnectionId,
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

        // The nodes use separate challenges and opposite connection sides to verify each other's presence.
        let register_node1 = async {
            let challenge = proto::Challenge::random();
            let presence = node2.presence(Accepting, &challenge).await?;
            node1.register_peer(Dialing, &challenge, presence, Box::new(LocalProcessSender { sender: node2_tx })).await
        };
        let register_node2 = async {
            let challenge = proto::Challenge::random();
            let presence = node1.presence(Dialing, &challenge).await?;
            node2.register_peer(Accepting, &challenge, presence, Box::new(LocalProcessSender { sender: node1_tx })).await
        };
        // Let an ephemeral node adopt before advertising its own system to the durable node.
        let (connection1, connection2) = if node1.durable && !node2.durable {
            let connection2 = register_node2.await?;
            match register_node1.await {
                Ok(connection1) => (connection1, connection2),
                Err(error) => {
                    node2.deregister_peer(connection2);
                    return Err(error.into());
                }
            }
        } else {
            let connection1 = register_node1.await?;
            match register_node2.await {
                Ok(connection2) => (connection1, connection2),
                Err(error) => {
                    node1.deregister_peer(connection1);
                    return Err(error.into());
                }
            }
        };

        let receiver1_task = Self::setup_receiver(node1.clone(), connection1, node2.state(), node1_rx);
        let receiver2_task = Self::setup_receiver(node2.clone(), connection2, node1.state(), node2_rx);

        Ok(Self { node1: node1.weak(), node2: node2.weak(), connection1, connection2, receiver1_task, receiver2_task })
    }

    fn setup_receiver<SE, PA>(
        node: Node<SE, PA>,
        connection: ConnectionId,
        peer_state: Read<NodeState>,
        mut rx: mpsc::Receiver<proto::NodeMessage>,
    ) -> tokio::task::JoinHandle<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            let node_state = node.state();
            loop {
                let message = tokio::select! {
                    biased;
                    _ = node_state.wait_for(|state| state.halt_reason().is_some()) => break,
                    _ = peer_state.wait_for(|state| state.halt_reason().is_some()) => break,
                    message = rx.recv() => match message { Some(message) => message, None => break },
                };
                let node = node.clone();
                tokio::spawn(async move {
                    let _ = node.handle_message(connection, message).await;
                });
            }
            node.deregister_peer(connection);
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
            node1.deregister_peer(self.connection1);
        }
        if let Some(node2) = self.node2.upgrade() {
            node2.deregister_peer(self.connection2);
        }
    }
}
