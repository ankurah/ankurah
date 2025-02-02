//! The goal of this crate is to test the idea of a distributed causal clock amid thousands
//! or even millions of nodes that emit events on various topics.
//!
//! In order to determine ordering between any arbitrary clock readings, we need to maintain a
//! much smaller set of "beacon" nodes that we can use to efficiently determine ordering.
//!
//! Crucially, these beacon nodes are not pre-selected, and they're not even static,
//! but rather selected emergently by the whole cluster of nodes.
//!
//! Also: Not all nodes will have the same beacon set. One of the key measures of success
//! in this system is that it should have a high degree of overlap between those nodes
//! which are selected as beacons, while also having a degree of local connectivity.
//!
//! Similar to kademlia, where each node knows a lot about their local neighborhood
//! while also knowing some distant nodes. UNLIKE kademlia, we want the network to
//! broadly agree on which distant nodes are selected. And similarly, local neighbors
//! should agree on which local nodes are selected

use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
};

mod simulation;
mod timer;

pub use simulation::Simulation;
pub use timer::Timer;

static MAX_RETAINED_PEERS: usize = 20;
static PING_INTERVAL: u64 = 5;

lazy_static::lazy_static! {
    static ref NODE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    // TODO - we need some way to simulate a gossip network, so each node has its own topic that other nodes can subscribe to
}

#[derive(Clone)]
pub struct Position {
    x: i64,
    y: i64,
    z: i64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NodeId(u64);

/// a Peer is a node we choose to stay connected to. For now we'll use direct connections,
/// but later we'll use a gossip protocol to keep the set of neighbors up to date
#[derive(Clone)]
pub struct Peer {
    node_id: NodeId,
    // latency in milliseconds - for this purpose of this example, this will be fixed
    latency: u64,
    // reliability is a measure of how often the node is emitting events. Some tests may pause nodes for a while
    // which would reduce reliability
    reliability: u64,
    // notariety is a measure of how many other nodes are using this node in their clock readings
    // This will be highly dynamic as the system converges on a consensus set of beacon nodes
    notariety: u64,
    /// NOTE: we may have to maintain some kind of history in order to properly score reliability and notariety

    /// The latest dot we've received from this peer
    latest_dot: Dot,
}

/// A sorted list of peers we have heard about recently
/// We will add to this list as we hear about new peers based on their Dots in received events
/// We will score them, and remove the lowest scoring peers to maintain a maximum size.
/// This scoring should balance local and global relevancy of the clock readings, so we can have a common point of comparison
/// for events emitted locally and globally
/// New clock readings will draw from this set in an attempt to balance local and global relevancy of the clock readings
///
/// The set may expand above MAX_RETAINED_PEERS momentarily, but we will re-score, and cull it back down to MAX_RETAINED_PEERS
/// after each event is recieved
#[derive(Clone)]
pub struct PeerSet {
    peers: Vec<Peer>,
}

/// A point in time from the perspective of a node
#[derive(Clone)]
pub struct Dot {
    /// Unique identifier for the node
    node_id: NodeId,
    /// Counter for the node
    counter: u64,
}

impl PeerSet {
    pub fn new() -> Self { Self { peers: Vec::new() } }
}

pub struct Clock(BTreeSet<Dot>);

pub enum Payload {
    Ping,
    Operation(String),
}

pub struct Event {
    clock: Clock,
    payload: Payload,
}

#[derive(Clone)]
pub struct Node<T: Timer> {
    /// Unique identifier for the node
    id: NodeId,
    /// The time the node was "started"
    start_time: u64,
    /// Counter for the node
    last_counter: u64,
    /// Location in the 3D space
    position: Position,
    /// Our curated set of peers
    peerset: PeerSet,
    /// The timer this node uses
    timer: T,
}

impl<T: Timer> Node<T> {
    pub fn new(position: Position, start_time: u64, timer: T) -> Self {
        let id = NodeId(NODE_ID_COUNTER.fetch_add(1, Ordering::Relaxed));
        let node = Self { id, start_time, last_counter: 0, position, peerset: PeerSet::new(), timer };

        // Set up ping interval
        let mut node_ref = node.clone();
        let _guard = timer.set_interval(
            Box::new(move || {
                node_ref.ping();
            }),
            PING_INTERVAL,
        );

        node
    }

    /// Emits a ping event and increments the node's counter
    pub fn ping(&mut self) -> Event {
        self.last_counter += 1;
        Event {
            clock: Clock(BTreeSet::new()), // TODO: implement proper clock
            payload: Payload::Ping,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn create_1k_nodes_and_emit_pings() {
        let mut simulation = Simulation::new(1234);
        simulation.create_test_nodes(1000);

        // Run for 10 "minutes" (600 seconds)
        let duration = 600;
        let start = Instant::now();
        let event_count = simulation.run(duration);
        let elapsed = start.elapsed();

        // Verify each node's counter
        for node in &simulation.nodes {
            let expected_pings = (duration - node.start_time) / PING_INTERVAL;
            assert_eq!(
                node.last_counter, expected_pings,
                "Node {:?} should have emitted {} pings (started at {})",
                node.id.0, expected_pings, node.start_time
            );
        }

        println!(
            "Simulated {} seconds in {:?}, {} events ({} events/sec simulated time)",
            duration,
            elapsed,
            event_count,
            event_count as f64 / duration as f64
        );
    }

    // #[test]
    // fn beacon_sets_should_have_high_overlap() {}

    // #[test]
    // fn beacon_sets_should_have_local_connectivity() {}

    // #[test]
    // fn beacon_sets_should_have_global_connectivity() {}
}
