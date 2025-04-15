- Create a struct `Entity` with a `head` representing the latest known clock frontier (list of `EventId`s).

- When a new `Event` is created:
  - It references the entity's current `head` in `precursors`.
  - Its ID (`EventId`) can be derived from hashing (`entity_id`, `payload`, sorted `precursors`, etc.).
  - The new event is appended locally, and the entity's `head` is updated to reference this event (and possibly older ones if needed).

# Node State

- Each node holds a local map of `EntityId` -> `Entity`.
- It also tracks a local "inbox" or buffer of events recently received from other nodes.
- Each node can occasionally produce a beacon event referencing a broad set of known heads to unify them.
- The simulation orchestrator controls which nodes connect, how often they sync, etc.

# Network / Gossip Layer

- In each cycle, the simulation picks random pairs of nodes to "exchange knowledge."
- The nodes share their known heads or a partial list of recent `EventId`s.
- Upon seeing an unknown `EventId`, a node requests the corresponding `Event` (and possibly transitively requests any unknown precursors).
- We record metrics on how many messages are sent and how many events are transferred.

# Workload Phases

1.  **Uniform:** The simulation uses a Poisson or uniform distribution to decide which node does a write or read in each cycle. The magnitude of the write (how many increments to `payload`) can also vary.
2.  **Bursty:** The simulation lumps writes into short intervals of high-intensity updates on a few entities, followed by lulls, interspersed with heavy read intervals.

# Measurement & Logging

- **Write Amplification:** For each user-initiated write, how many "system events" (beacons, aggregator merges) were created? We can track `# system events / # user writes`.
- **Network Traffic:** Count total gossip messages, total bytes, requests for events, etc.
- **Event Traversal:** Within a node, track how many times (and how deeply) the node resolves or merges precursors in a cycle.
- **Time-Series:** For each cycle, log the convergence measure (e.g., how many nodes know about each newly created event) to see how quickly the infection spreads.
- Possibly log a "network snapshot" each cycle to visualize entity states and known heads across nodes.

# Implementation Outline

1.  **Simulation Engine:**
    - A driver that increments "time" cycle by cycle.
    - Schedules write/read operations and controls how nodes connect.
2.  **Beacon:**
    - A specialized `Event` referencing multiple heads. Emitted with a certain probability each cycle so that the total beacon rate is stable across the entire network.
3.  **Logging:**
    - A structured data log or standard output with metrics each cycle.
4.  **CLI or Config:**
    - Command-line arguments to specify number of nodes, total cycles, ratio of write/read ops, random seed, etc.

# Goals & Expected Outcome

By running this simulation, we want to:

- Validate that the system converges (all nodes eventually learn about new events).
- Quantify the overhead in terms of extra events (beacons/aggregators) vs. user-generated events (write amplification).
- Observe how quickly the network converges under uniform vs. bursty loads, and how that depends on the number of nodes.
- Identify potential bottlenecks in gossip traffic or event traversal that might arise from large precursors sets or frequent aggregator references.

With this plan, the developer can experiment with various designs (e.g., different aggregator strategies, different ways to store or hash `Clock`) and measure which approach yields better performance under changing workloads. This sets the stage for further optimization and real-world deployment insights.
