use std::sync::{
    Arc, RwLock,
    atomic::{AtomicUsize, Ordering},
};

use rand::{Rng, rngs::StdRng};
use uuid::Uuid;

use crate::network::NetworkRef;

#[derive(Clone)]
pub struct Position {
    pub x: i64,
    pub y: i64,
    pub z: i64,
}

impl Position {
    pub fn random(rng: &mut StdRng) -> Self {
        Self { x: rng.gen_range(-500..500), y: rng.gen_range(-500..500), z: rng.gen_range(-500..500) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self { Self(Uuid::new_v4()) }
}

// A node is a service or agent that exists on a machine. we are using position to simulate physical distance.
#[derive(Clone)]
pub struct Node(Arc<NodeInner>);

struct NodeInner {
    id: NodeId,
    position: Position,
    neighbors: RwLock<Vec<NodeId>>,
    network: NetworkRef,
    received_messages: AtomicUsize,
    work_count: AtomicUsize,
    start_time: u64,
}

#[derive(Clone)]
pub struct Message {
    pub from: NodeId,
    pub payload: MessagePayload,
}

#[derive(Clone)]
pub enum MessagePayload {
    Heartbeat { timestamp: u64 },
    // Add more message types as needed
}

impl Node {
    pub fn new(position: Position, network: NetworkRef, neighbors: Vec<NodeId>, start_time: u64) -> Self {
        let id = NodeId::new();
        Self(Arc::new(NodeInner {
            id,
            position,
            neighbors: RwLock::new(neighbors),
            network,
            received_messages: AtomicUsize::new(0),
            work_count: AtomicUsize::new(0),
            start_time,
        }))
    }

    pub fn id(&self) -> &NodeId { &self.0.id }

    pub fn position(&self) -> &Position { &self.0.position }

    pub fn start_time(&self) -> u64 { self.0.start_time }

    pub fn received_message_count(&self) -> usize { self.0.received_messages.load(Ordering::SeqCst) }

    pub fn work_count(&self) -> usize { self.0.work_count.load(Ordering::SeqCst) }

    pub fn do_work(&self) {
        self.0.work_count.fetch_add(1, Ordering::SeqCst);
        // TODO: should be called by the simulator on some interval starting at start_time
        // we will fill this out after we have the simulator working smoothly
        // one of the things we will do is to send messages to the neighbors
    }

    pub fn receive_message(&self, message: Message) {
        self.0.received_messages.fetch_add(1, Ordering::SeqCst);
        match message.payload {
            MessagePayload::Heartbeat { timestamp } => {
                // Process heartbeat
                println!("Node {:?} received heartbeat from {:?} at time {}", self.0.id, message.from, timestamp);
            }
        }
    }
}
