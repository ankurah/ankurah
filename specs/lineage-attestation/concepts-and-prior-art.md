## Prior Art: Lineage Evidence and Catch-up Strategies

### Why We Care

- Ephemeral peers lag on entities they no longer track; accepting a new state requires proving it descends from (or diverges from) their stale head.
- Our Phase 1 plan leans on server-built event bridges or attested snapshots; both rely on bounded traversal cost on the client.
- Deep histories (or missing known matches) still force the client into an expensive lineage walk, motivating a survey of how other systems bound the work.

### Landscape of Existing Techniques

#### Version Vectors & Per-Entity Clocks

- **How it works**: Track a version vector (or dotted vector) for every entity; comparing two heads is a constant-time vector comparison.
- **Strengths**: Deterministic proof of ancestry; trivial to prune histories; widely deployed (Bayou, Dynamo, Cosmos DB).
- **Weaknesses**: Vectors grow with replica count; hard to compress in multi-tenant deployments; pushes heavy metadata to every write.
- **Fit for Ankurah**: In tension with our ethos of lightweight per-entity attestations and peer-specific policy agents. Highly variable replica counts make worst-case vector size unpredictable; we would need aggressive summarisation or thresholds to avoid unbounded growth.

#### Periodic Snapshots & Log Compaction

- **How it works**: Materialise checkpoints (per entity or per shard) and prune old events; change application becomes snapshot + short tail.
- **Strengths**: Predictable catch-up cost; amortises lineage validation by moving it off the critical path.
- **Weaknesses**: Requires durable peers to coordinate checkpoint cadence; snapshots must be trusted or accompanied by signatures; introduces pause-or-background work.
- **Fit for Ankurah**: Similar to today’s `StateSnapshot`, but proactive compaction could cap lineage depth. Needs policy on when to emit (time-based, event-count, size) and clarity on how checkpoints interplay with attested relations.

#### Merkle Trees, Commit Graphs, and Skip Structures

- **How it works**: Maintain additional ancestry indices—Merkle trees (Dynamo anti-entropy), Git’s commit graph generation numbers, skip lists that add parents every 2^k steps.
- **Strengths**: Logarithmic traversal; can prove inclusion/exclusion efficiently; battle-tested in replicated logs and blockchain clients.
- **Weaknesses**: Adds write amplification (extra hashes/edges); needs precise attestation so malicious peers cannot forge shortcuts; more complex storage layout.
- **Fit for Ankurah**: Promising for server-side bridge construction and possibly client-side verification if we can attest the extra edges. Needs careful treatment of reactive updates so skip links remain consistent across backends.

#### Cached Bridges / ForwardView-Style Materialisations

- **How it works**: Precompute “forward deltas” between frontier pairs (ReadySet ForwardView, Materialize arrangements). When a client is missing a range, ship the cached bridge.
- **Strengths**: Zero traversal at request time; naturally aligns with EventBridge semantics; cache can be shared across clients with similar gaps.
- **Weaknesses**: Cache invalidation and eviction policy are non-trivial; needs storage space proportional to hot gaps; stale caches risk re-sending long bridges.
- **Fit for Ankurah**: Matches our existing event-bridge design, especially if we can cache popular spans. We must monitor server CPU when refreshing caches after each write.

#### Probabilistic Skip Links (Bloom / Counting Filters)

- **How it works**: Emit auxiliary skip edges chosen probabilistically (e.g., include each ancestor in a Bloom filter, or record skip to every k-th ancestor with k sampled per write). Clients assimilate whichever skips they receive.
- **Strengths**: Keeps metadata lightweight; collisions merely push the traversal back to the baseline; can tune emission probability to balance amplification versus speed.
- **Weaknesses**: No deterministic guarantee—worst-case traversal still exists; Bloom filters must be attested or scoped per peer to avoid equivocation; write amplification remains a concern if probabilities are too high.
- **Fit for Ankurah**: Aligned with our ethos of probabilistic assistance. Needs future analysis around how PolicyAgents validate probabilistic evidence and how we expose failure cases to the caller.

### Comparative Snapshot

| Approach                 | Strengths                                     | Weaknesses / Risks                                                  | Alignment Notes                                                                   |
| ------------------------ | --------------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Version vectors          | Constant-time ancestry checks; mature         | Metadata grows with replica count; heavy signing surface            | Poor fit without aggressive pruning; conflicts with “minimal per-event metadata”. |
| Periodic snapshots       | Bounded catch-up cost; simple client          | Requires coordination; snapshot trust burdens PolicyAgent           | Natural extension of current snapshots; focus on cadence and attestation.         |
| Merkle / skip structures | Logarithmic traversal; strong proofs          | Extra storage & hashing; must attest skip edges                     | Good candidate for server bridge builder if we can amortise hashing.              |
| Cached bridges           | Zero traversal at apply time; reuse hot spans | Cache invalidation/eviction; higher server memory                   | Already similar to EventBridge strategy; explore caching policy.                  |
| Probabilistic skip links | Lightweight metadata; tunable trade-offs      | No hard guarantees; collision handling; write amplification choices | Ethos-aligned; needs modelling of failure probability and attestation story.      |

### Concepts Under Exploration

#### Emergent Beacon Reference Frames _(speculative)_

- **Working idea**: Let each node publish a mostly linear “beacon” chain that peers can adopt as a soft reference clock. Peers favour beacons that emit regularly and earn organic popularity, creating a shared context without exchanging full vectors.
- **Potential upside**: Keeps the shared hint space small, helping narrow lineage comparisons before we request explicit bridges. Fits the system’s probabilistic ethos and could emerge naturally as peers converge on reliable beacons.
- **Open concerns**: Needs a gossip/selection protocol to avoid beacon churn, plus safeguards against adversarial beacons skewing the frame. Still speculative—no known prior art yet—so we must analyse PolicyAgent implications and the write amplification of maintaining beacon chains.

#### Ephemeral Cache Pruning _(operational)_

- **Observation**: Ephemeral peers already evict stale entity state; they can reuse the same LRU/LFU policy for regular events, cached bridges, skip metadata, and future beacon hints.
- **Upside**: Keeps storage bounded without global coordination. Useful lineage evidence stays hot; evicted items can be reacquired from durable peers.
- **Caveats**: Excessive evictions increase miss rates and bandwidth. Instrumentation should surface eviction churn so we can tune cache size or adjust bridge delivery frequency.

### Hooks for Next Design Iterations

- Explore hybrid models: e.g., maintain deterministic skip links every N events plus probabilistic Bloom summaries for large gaps.
- Quantify write amplification for Bloom-backed skips (per-entity byte budget, signing overhead). Compare against deterministic skip lists for common event rates.
- Investigate server-side caching of frequent bridges: what is the hit rate across peers? Can we bias caches by query predicates?
- Revisit PolicyAgent requirements: which evidence types can it verify without replaying events (hash attestations, signed skip pointers, etc.)?
- Align write-path changes with the existing storage backends (Sled, Postgres, IndexedDB) to ensure skip metadata can be persisted without breaking parity.

### Open Questions

- How do we expose lineage evidence types on the wire so clients can select their preferred proof (deterministic vs probabilistic)?
- What metrics do we need on the server to detect when lineage budgets are routinely exhausted (per collection, per peer)?
- Can we piggyback skip-link emission on existing attestations to avoid extra signatures?
- How do we degrade gracefully when probabilistic shortcuts fail—retry with higher budget, request explicit bridges, or escalate to attested snapshots?

This document will evolve as we prototype skip-based aids and gather data on traversal costs under real workloads.
