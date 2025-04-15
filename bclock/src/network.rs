use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
};

use rand::{rngs::StdRng, seq::IteratorRandom};

use crate::{
    node::{Message, Node, NodeId, Position},
    timer::SimulatedTimer,
};

#[derive(Clone)]
pub struct Network(Arc<NetworkInner>);

pub struct NetworkInner {
    nodes: RwLock<HashMap<NodeId, Node>>,
}

impl Network {
    pub fn new() -> Self { Self(Arc::new(NetworkInner { nodes: RwLock::new(HashMap::new()) })) }

    pub fn register_node(&self,node) {
        let mut nodes = self.0.nodes.write().unwrap();
        nodes.insert(id, node);
    }

    pub fn select_neighbors(&self, rng: &mut StdRng, position: &Position) -> Vec<NodeId> {
        let nodes = self.0.nodes.read().unwrap();
        let mut neighbors = Vec::new();
        // Only proceed if we have at least 2 nodes
        if let (Some(first), Some(second)) = (nodes.keys().next(), nodes.keys().nth(1)) {
            neighbors.push(first.clone());
            neighbors.push(second.clone());
            neighbors.extend(nodes.keys().skip(2).choose_multiple(rng, 5).into_iter().cloned());
        }
        neighbors
    }

    pub fn send_message(&self, from: &NodeId, to: &NodeId, message: Message, timer: &SimulatedTimer) -> bool {
        let nodes = self.0.nodes.read().unwrap();

        // Get sender and receiver positions
        let (sender_pos, receiver_pos) = match (nodes.get(from), nodes.get(to)) {
            (Some(sender_pos), Some(receiver_pos)) => (sender_pos, receiver_pos),
            _ => return false,
        };

        // Calculate delay based on distance
        let delay = calculate_network_delay(sender_pos, receiver_pos);

        // Schedule message delivery
        let to = to.clone();
        let message = message.clone();
        timer.schedule_at(
            timer.now() + delay,
            Box::new(move || {
                // TODO: Need to handle message delivery through simulator
                // receiver.receive_message(message);
            }),
        );

        true
    }

    pub fn reference(&self) -> NetworkRef { NetworkRef(Arc::downgrade(&self.0)) }
}

// Calculate network delay based on physical distance between nodes
fn calculate_network_delay(from: &Position, to: &Position) -> u64 {
    // Euclidean distance between nodes (rounded up to nearest time unit)
    let distance = ((from.x - to.x).pow(2) + (from.y - to.y).pow(2) + (from.z - to.z).pow(2)) as f64;
    distance.sqrt().ceil() as u64
}

#[derive(Clone)]
pub struct NetworkRef(Weak<NetworkInner>);

impl NetworkRef {
    pub fn get(&self) -> Option<Arc<NetworkInner>> { self.0.upgrade() }
}
