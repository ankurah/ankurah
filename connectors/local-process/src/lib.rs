use ankurah_core::policy::PolicyAgent;
use ankurah_core::signals::{Read, Wait};
use ankurah_core::storage::StorageEngine;
use ankurah_core::NodeState;
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::mpsc;

use ankurah_core::connector::{PeerProvenance, PeerSender, SendError};
use ankurah_core::node::{ConnectionRegistration, Node, WeakNode};

#[derive(Clone)]
/// Sender for local process connection
pub struct LocalProcessSender {
    sender: mpsc::Sender<proto::NodeMessage>,
    node_id: proto::NodeId,
}

#[async_trait]
impl PeerSender for LocalProcessSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
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
    /// `node1`'s registration of `node2`.
    registration1: ConnectionRegistration,
    /// `node2`'s registration of `node1`.
    registration2: ConnectionRegistration,
}

impl<SE1, PA1, SE2, PA2> LocalProcessConnection<SE1, PA1, SE2, PA2>
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    /// Connect the two nodes as if one had dialed the other: an ephemeral node dials a durable
    /// one, and between nodes of the same class `node1` dials `node2`. The dialing side registers
    /// its peer as configured, the accepting side as inbound.
    pub async fn new(node1: &Node<SE1, PA1>, node2: &Node<SE2, PA2>) -> anyhow::Result<Self> {
        let (node1_tx, node1_rx) = mpsc::channel(1024);
        let (node2_tx, node2_rx) = mpsc::channel(1024);

        let node2_dials = node1.durable && !node2.durable;
        let (provenance1, provenance2) = if node2_dials {
            (PeerProvenance::Inbound, PeerProvenance::Configured)
        } else {
            (PeerProvenance::Configured, PeerProvenance::Inbound)
        };
        // Each opening goes to the other side as it is, and both presences are signed before the
        // registrations spend the handshakes they were made from.
        let (mut handshake1, mut handshake2) = (node1.begin_handshake(), node2.begin_handshake());
        handshake1.receive_opening(handshake2.opening())?;
        handshake2.receive_opening(handshake1.opening())?;
        let (presence1, presence2) = (node1.presence(&handshake1).await?, node2.presence(&handshake2).await?);
        let register_node1 =
            node1.register_peer(presence2, handshake1, provenance1, Box::new(LocalProcessSender { sender: node2_tx, node_id: node2.id }));
        let register_node2 =
            node2.register_peer(presence1, handshake2, provenance2, Box::new(LocalProcessSender { sender: node1_tx, node_id: node1.id }));
        // The dialing side registers its peer first, as a websocket client registers the server before sending its presence.
        let (registration1, registration2) = if node2_dials {
            let registration2 = register_node2.await?;
            match register_node1.await {
                Ok(registration1) => (registration1, registration2),
                Err(error) => {
                    node2.deregister_peer(registration2);
                    return Err(error.into());
                }
            }
        } else {
            let registration1 = register_node1.await?;
            match register_node2.await {
                Ok(registration2) => (registration1, registration2),
                Err(error) => {
                    node1.deregister_peer(registration1);
                    return Err(error.into());
                }
            }
        };

        let receiver1_task = Self::setup_receiver(node1.clone(), registration1, node2.state(), node1_rx);
        let receiver2_task = Self::setup_receiver(node2.clone(), registration2, node1.state(), node2_rx);

        Ok(Self { node1: node1.weak(), node2: node2.weak(), registration1, registration2, receiver1_task, receiver2_task })
    }

    fn setup_receiver<SE, PA>(
        node: Node<SE, PA>,
        registration: ConnectionRegistration,
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
                    let _ = node.handle_message(message).await;
                });
            }
            node.deregister_peer(registration);
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
            node1.deregister_peer(self.registration1);
        }
        if let Some(node2) = self.node2.upgrade() {
            node2.deregister_peer(self.registration2);
        }
    }
}
