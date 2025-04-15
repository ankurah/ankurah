use std::time::Duration;

use crate::network::Network;
use crate::node::{Message, MessagePayload, Node, NodeId, Position};
use hierarchical_hash_wheel_timer::simulation::SimulationTimer;
use hierarchical_hash_wheel_timer::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use uuid::Uuid;

pub struct Simulation {
    pub nodes: Vec<Node>,
    pub timer: SimulationTimer<Uuid, OneShotClosureState<Uuid>, PeriodicClosureState<Uuid>>,
    pub rng: StdRng,
    pub start_variance: u64,
    pub network: Network,
}

impl Simulation {
    pub fn new(seed: u64) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        let network = Network::new();
        let timer = SimulationTimer::for_uuid_closures();
        Self { nodes: Vec::new(), timer, rng, start_variance: 100, network }
    }

    pub fn register_node(&mut self, node: Node) {
        self.network.register_node(node.clone());
        let start_time = self.rng.random_range(0..self.start_variance);
        self.timer.schedule_periodic(
            Duration::from_millis(start_time),
            Duration::from_millis(100),
            PeriodicClosureState::new(
                node.id(),
                Box::new(move |timer_id: Uuid| {
                    node.do_work();
                    TimerReturn::Reschedule(())
                }),
            ),
        );
        self.nodes.push(node);
    }

    /// Creates a random set of nodes for testing.
    /// Uses the simulation's RNG seed, ensuring reproducible test scenarios.
    pub fn create_test_peers(&mut self, count: usize) {
        for _ in 0..count {
            let position = Position::random(&mut self.rng);
            let start_time = self.rng.gen_range(0..self.start_variance);

            // Get network reference before mutable borrow
            let network_ref = self.network.reference();
            let neighbors = self.network.select_neighbors(&mut self.rng, &position);

            // Create node with all the data we gathered
            let node = Node::new(position, network_ref, neighbors, start_time);

            // Register the node
            self.register_node(node);
        }
    }

    /// Runs the simulation until the specified time
    pub fn run(&mut self, until: u64) -> usize {
        let mut events_processed = 0;
        while self.timer.advance(Some(until)) {
            events_processed += 1;
        }
        events_processed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_delivery_delay() {
        let mut sim = Simulation::new(42);

        // Create nodes with known positions
        let node0 = Node::new(Position { x: 0, y: 0, z: 0 }, sim.network.reference(), vec![], 0);
        let node2 = Node::new(Position { x: 200, y: 0, z: 0 }, sim.network.reference(), vec![], 0);

        // Get the IDs for later use
        let node0_id = node0.id().clone();
        let node2_id = node2.id().clone();

        // Register nodes
        sim.register_node(node0.clone());
        sim.register_node(node2.clone());

        // Verify initial state
        assert_eq!(sim.nodes[1].received_message_count(), 0);
        assert_eq!(sim.timer.next_tick(), None, "No ticks should be scheduled initially");

        // Send message from node 0 to node 2
        let msg = Message { from: node0_id.clone(), payload: MessagePayload::Heartbeat { timestamp: sim.timer.now() } };
        sim.network.send_message(&node0_id, &node2_id, msg, &sim.timer);

        // Check that a tick is scheduled for the message delivery
        // Distance from (0,0,0) to (200,0,0) = 200 units
        // So delay = 200 time units (1:1 with distance)
        assert_eq!(sim.timer.next_tick(), Some(200), "Message delivery should be scheduled for time 200 (equal to distance)");

        // Message shouldn't be delivered immediately
        assert_eq!(sim.nodes[1].received_message_count(), 0);

        // Run until time 199 - still shouldn't be delivered
        sim.run(199);
        assert_eq!(sim.nodes[1].received_message_count(), 0, "Message should not be delivered at time 199");
        assert_eq!(sim.timer.next_tick(), Some(200), "Message delivery should still be scheduled for time 200");

        // Run until time 201 - should be delivered by now
        sim.run(201);
        assert_eq!(sim.nodes[1].received_message_count(), 1, "Message should be delivered by time 201");
        assert_eq!(sim.timer.next_tick(), None, "No more ticks should be scheduled after message delivery");
    }

    #[test]
    fn test_node_work_intervals() {
        let mut sim = Simulation::new(42);

        // Create two nodes with different start times
        let node0 = Node::new(Position { x: 0, y: 0, z: 0 }, sim.network.reference(), vec![], 5); // Starts at t=5
        let node1 = Node::new(Position { x: 100, y: 0, z: 0 }, sim.network.reference(), vec![], 10); // Starts at t=10

        // Register nodes with their start times
        sim.register_node(node0.clone());
        sim.register_node(node1.clone());

        // At t=0, no work should have been done
        assert_eq!(node0.work_count(), 0, "Node 0 should not have worked at t=0");
        assert_eq!(node1.work_count(), 0, "Node 1 should not have worked at t=0");

        // Run to t=4, still no work
        sim.run(4);
        assert_eq!(node0.work_count(), 0, "Node 0 should not have worked before its start time");
        assert_eq!(node1.work_count(), 0, "Node 1 should not have worked before its start time");

        // Run to t=5, node0 should work once
        sim.run(5);
        assert_eq!(node0.work_count(), 1, "Node 0 should work at t=5");
        assert_eq!(node1.work_count(), 0, "Node 1 should still not have worked");

        // Run to t=10, node0 should work again and node1 should start
        sim.run(10);
        assert_eq!(node0.work_count(), 2, "Node 0 should work again at t=10");
        assert_eq!(node1.work_count(), 1, "Node 1 should work at t=10");

        // Run to t=20, both nodes should work again
        sim.run(20);
        assert_eq!(node0.work_count(), 3, "Node 0 should work at t=20");
        assert_eq!(node1.work_count(), 2, "Node 1 should work at t=20");
    }
}
