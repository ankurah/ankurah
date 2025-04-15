pub mod core;
pub mod network;
pub mod node;
pub mod simulator;
// pub mod timer;

fn main() {
    let mut sim = simulator::Simulation::new(1);
    sim.create_test_peers(10);
    sim.run(1000);
}
