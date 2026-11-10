use ankurah_core::policy::PolicyAgent;
use ankurah_core::signals::{Read, Wait};
use ankurah_core::storage::StorageEngine;
use ankurah_core::NodeState;
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::mpsc;

use ankurah_core::connector::{PeerConnectionError, PeerProvenance, PeerSender, SendError};
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
    /// Connect the two nodes as [`register_each_other`] registers them.
    pub async fn new(node1: &Node<SE1, PA1>, node2: &Node<SE2, PA2>) -> anyhow::Result<Self> {
        let (node1_tx, node1_rx) = mpsc::channel(1024);
        let (node2_tx, node2_rx) = mpsc::channel(1024);
        let to_node2 = Box::new(LocalProcessSender { sender: node2_tx, node_id: node2.id });
        let to_node1 = Box::new(LocalProcessSender { sender: node1_tx, node_id: node1.id });
        let (registration1, registration2) = register_each_other(node1, node2, to_node2, to_node1).await?;

        let receiver1_task = Self::setup_receiver(node1.clone(), registration1, node2.state(), node1_rx);
        let receiver2_task = Self::setup_receiver(node2.clone(), registration2, node1.state(), node2_rx);

        Ok(Self { node1: node1.weak(), node2: node2.weak(), registration1, registration2, receiver1_task, receiver2_task })
    }

    /// The registration `node`, one of the two connected nodes, holds of the other: the one a frame
    /// from the other node arrives on.
    pub fn registration_held_by(&self, node: proto::NodeId) -> Option<ConnectionRegistration> {
        match node {
            node if node == self.registration2.peer() => Some(self.registration1),
            node if node == self.registration1.peer() => Some(self.registration2),
            _ => None,
        }
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
                    let _ = node.handle_message(registration, message).await;
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

/// Register `node1` and `node2` with each other in one process, as if one had dialed the other:
/// an ephemeral node dials a durable one, and between nodes of the same class `node1` dials
/// `node2`. The dialing side registers its peer as `Configured`, the accepting side as
/// `Inbound`. Returns `node1`'s registration of `node2`, which frames from `node2` arrive on, and
/// `node2`'s of `node1`; a refusal by either side leaves neither registered.
pub async fn register_each_other<SE1, PA1, SE2, PA2>(
    node1: &Node<SE1, PA1>,
    node2: &Node<SE2, PA2>,
    to_node2: Box<dyn PeerSender>,
    to_node1: Box<dyn PeerSender>,
) -> Result<(ConnectionRegistration, ConnectionRegistration), PeerConnectionError>
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    if node1.durable && !node2.durable {
        let (registration2, registration1) = dial(node2, node1, to_node1, to_node2).await?;
        Ok((registration1, registration2))
    } else {
        dial(node1, node2, to_node2, to_node1).await
    }
}

/// Walk the handshake from `dialer` to `acceptor` in the order every connector follows: each
/// sends its opening, the dialer presents, the acceptor registers the dialer and only then
/// presents, and the dialer registers the acceptor. Returns the dialer's registration, then the
/// acceptor's.
async fn dial<SE1, PA1, SE2, PA2>(
    dialer: &Node<SE1, PA1>,
    acceptor: &Node<SE2, PA2>,
    to_acceptor: Box<dyn PeerSender>,
    to_dialer: Box<dyn PeerSender>,
) -> Result<(ConnectionRegistration, ConnectionRegistration), PeerConnectionError>
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    let (mut dialer_side, mut acceptor_side) = (dialer.begin_handshake(), acceptor.begin_handshake());
    dialer_side.receive_opening(acceptor_side.opening())?;
    acceptor_side.receive_opening(dialer_side.opening())?;
    let dialer_presence = dialer.presence(&dialer_side).await?;
    let acceptor_registration = acceptor.register_peer(dialer_presence, &acceptor_side, PeerProvenance::Inbound, to_dialer).await?;
    let registered = match acceptor.presence(&acceptor_side).await {
        Ok(acceptor_presence) => dialer.register_peer(acceptor_presence, &dialer_side, PeerProvenance::Configured, to_acceptor).await,
        Err(refusal) => Err(refusal),
    };
    match registered {
        Ok(dialer_registration) => Ok((dialer_registration, acceptor_registration)),
        Err(refusal) => {
            acceptor.deregister_peer(acceptor_registration);
            Err(refusal)
        }
    }
}
