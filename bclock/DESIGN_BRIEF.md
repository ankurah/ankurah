Summary of the Problem
We have a distributed system in which multiple peers (hosts/agents) each maintain knowledge about entities (conceptual records or objects in the system) through a stream of events (immutable records of changes or state updates). Each entity references its causal history (previous events) directly, but we want to allow for efficient global or partial synchronization without requiring all peers to hold a complete global log. This is an “eventually consistent” approach where new events spread virally throughout the network, occasionally anchored by “beacon” events that unify or summarize portions of the history.

We want to simulate this system in Rust to see how it performs under different workloads, focusing on write amplification (i.e. how many additional events or aggregator “beacon” references get created as a result of local updates) and how quickly and efficiently peers converge in different network conditions. We’ll measure metrics over time to assess how changes in event creation, reading, or aggregator pings affect the system’s overhead and convergence.

General Approach
Core Data Structures

Entity: A conceptual record or object in the system. It has an ID of type Uuid.

Event: Immutable data describing a change to one or more entities. It references (or “depends on”) a set of previous events. The event ID is a hash of its content (and references). We may or may not store the event ID explicitly—only in certain structures (like a peer’s Clock).

Peer: Represents a host or agent in the network that can create new events (writes), process queries (reads), and exchange data with other peers.

Causality & Beacon Pings

Each peer periodically generates beacon events with some probability, referencing the current known frontier of events. These serve as a partial aggregator or checkpoint that can unify separate subhistories. The frequency of beacon creation is controlled so that on average a fixed number of beacons are created per “virtual second,” regardless of the total number of peers.

Peers will occasionally gossip (share events) with randomly chosen neighbors. During gossip, each peer merges the other’s frontier of events if it contains something new.

Simulation Lifecycle

The simulation is parameterized by:

Number of peers (unknown to each peer; the simulation orchestrator knows).

Network topology (e.g., fully connected, random adjacency).

Workload Patterns:

Heterogeneous read/write: Different peers randomly generate reads or writes at different rates.

Lumpy: Bursts of writes from subsets of peers followed by bursts of read queries elsewhere, to model more realistic usage spikes.

In each “tick” (time step or sub-second unit):

A subset of peers emit events (writes) on certain entities.

Some peers perform reads, which might prompt partial event lookups or traversal.

A few peers randomly decide to create a beacon (aggregator event) referencing their known frontier.

Peers sync with neighbors (each peer picks one neighbor, or uses a gossip schedule).

After each cycle, we log metrics: number of events, number of references (write amplification), how many traversals are performed during queries, and how long the infection spread takes for new events.

Measurements & Metrics

Time-Series for Traversals: Measure how often a read or sync operation must walk the event lineage. Record how many events it traverses on average or in worst-case.

Write Amplification: For each user-level “write,” how many additional events (like beacons) get created or updated as a result? Compare that ratio across different network sizes and workloads.

Convergence Latency: How many ticks does it take for a new event to propagate to all peers (or to 90% of peers)?

Read Efficiency: In read-heavy scenarios, measure whether queries can quickly find needed events or if they must do deep traversals.

Implementation Steps

Model Data

struct Entity { id: Uuid } – just a placeholder.

struct Event { /_ contains references (hashes) to precursors, some small payload, etc. _/ }

type EventId = [u8; 32] /_ or other hash representation _/

struct Peer { id: Uuid, clock: Vec<EventId>, /_ known frontier or aggregator references _/ known_events: HashMap<EventId, Event>, … }

Sim Engine

A loop with for t in 0..simulation_ticks to represent each time step.

Randomly generate writes or reads per peer (according to configured pattern).

Check if a peer will create a beacon event this tick based on global average target.

Perform neighbor gossip with some probability or a fixed schedule.

Event Creation

When a peer writes to an entity, it creates a new Event referencing the peer’s current frontier (including references to the relevant entity’s last known event).

Compute EventId by hashing the event’s content.

Insert into known_events and update clock if it’s a direct next head.

Beacon Creation

If the peer decides to emit a beacon, it forms a special aggregator event referencing all or most of the peer’s current heads.

Insert the new aggregator event into known_events and push its ID into clock (possibly trimming older aggregator references).

Gossip & Merging

If peer A gossips with peer B, they exchange their frontiers (clock or aggregator references).

Each peer requests any unknown event IDs from the other.

They update their local known_events and clock with the newly obtained event references.

Reads

If the simulation triggers a read on peer P for entity E, define some way to project the current state of E from the known events. Possibly we do a traversal from E’s last known event backward.

Record how many events are traversed (for metrics) or how large a portion of clock was scanned.

Metrics & Logging

During each tick, record:

Number of new events vs. user-level writes.

Beacon events created.

Traversal counts during reads.

Gossip traffic (number of event IDs exchanged).

Convergence checks: how many peers have seen the newly created events after X ticks?

Why We’re Doing This
We want to examine:

Write Amplification: How many extra events (especially beacon aggregator events) does the system produce beyond the raw user-level writes?

Traversal Efficiency: How easily do reads or merges discover and fetch relevant events?

Scalability: Does the system handle more peers gracefully, or does overhead explode?

Convergence Latency: How quickly do new events “infect” the entire network?

This simulation gives us realistic performance data for an eventually consistent system with local per-entity lineages plus probabilistic beacon events, helping us refine parameters (like beacon frequency or gossip rates) before building a production system.
