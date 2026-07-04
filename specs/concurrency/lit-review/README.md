# Workstream B: literature review

Tracking: #269. Scope: specs/concurrency/phase-2.md, workstream B. Method set at kickoff, 2026-07-04.

## Purpose

Check phase 2's intended designs against the systems that already fought these battles. The output is not a survey for its own sake: every topic ends in findings that confirm, challenge, or extend a specific phase 2 design intent (RFCs #265, #266, #267, #268, #271, #272, #273, #274 and workstreams C and E).

## Method

One reader per topic, working independently from this document. Each reader produces one annotated bibliography file in this directory (format below). After all topic files land, an adversarial synthesis pass reads everything and produces design-deltas.md: every place the literature disagrees with phase 2's intended designs, each with an adopt or reject recommendation and rationale. The synthesis must challenge our design choices rather than affirm them; a synthesis that finds nothing to challenge is presumed to have failed and is redone.

## Current design, in brief (what the literature is checked against)

- Per-entity event DAGs. EventId is a SHA-256 content hash of entity id, operations, and parent clock; a Clock is a sorted deduplicated set of event ids; an entity's head is the current tip antichain. Cycles are structurally impossible; entity identity is anchored at the creation event.
- Causal comparison: symmetric backward BFS from two clocks with per-side dedup and a retrieval budget (one unit per event fetch, one internal 4x retry). Verdicts: Equal, StrictDescends, StrictAscends, DivergedSince (with the meet, the maximal common antichain), Disjoint (no shared history, rejected), BudgetExceeded. StrictDescends (wholesale adoption) requires cover containment plus a grounded exploration boundary: every discovered genesis root and every unexplored frontier id must lie inside the comparison side's ancestry.
- Merge: from the meet, a generalized topological sort emits layers of mutually concurrent events; property backends resolve within each layer (Yrs merges CRDT updates; LWW runs a per-property tournament using stored provenance, causal descent, then content-hash tiebreak). Resolution may depend only on graph facts and event contents, never wall clocks or arrival order.
- Ingest: incoming events are staged (discoverable for comparison) before any head references them, and committed to durable storage before any state snapshot references them; batches are topologically sorted receiver-side (Kahn); failures are contained per item.
- Today there is no pruning, no compaction, and no generation index; the budget is the only depth backstop. Planned: unified ingest planner (#268), persistent generation numbers plus applied-set index (#266), sealed-prefix checkpoints and a rejection horizon (#271), transactional visibility decision (#272), snapshot authority rule (#273), validated ingress (#274).

## Topics and seed lists

Seeds are starting points, not fences; follow citations where they lead. The list may be extended by workstream A findings.

1. Compaction and history lifecycle (feeds #271): Automerge columnar compression and document forking, Yjs garbage collection, Chronofold, OpSets, git shallow clone and grafts semantics.
2. Indexed causality (feeds #266): git commit-graph generation numbers (v1 and corrected v2), Mercurial revlog linkrev, reachability labeling literature (GRAIL, interval labels) for the applied-set index design.
3. Merkle-causal systems (feeds C4 and #274): Merkle-CRDTs (IPFS/OrbitDB), Byzantine causal broadcast, hash-graph gossip; what they pay for adversarial tolerance and which of those costs we already pay via content addressing.
4. Merge semantics (feeds the #267 contract law): Kleppmann's local-first corpus, ORDTs, movable-tree and rich-text CRDT merge anomalies.
5. Testing (feeds C1 and C8): FoundationDB simulation testing, turmoil and madsim style deterministic network simulation in Rust, Jepsen's checker vocabulary for convergence claims.
6. Staleness horizons and divergence bounding (feeds the #271 rejection-horizon decision; REQUIRED before D3 implementation): Dynamo vector clock truncation and its documented anomalies, CouchDB _revs_limit and conflict resurrection, Riak dotted version vectors and sibling management, Yjs and Automerge tombstone and GC retention windows, Matrix's retreat from depth-based event ordering (depth is adversarially manipulable), and product-level treatment of stale offline clients (forced resync beyond a window). This topic must evaluate the four candidate policies on their merits: retrieval budget as the only backstop (current), generation-distance horizon, attested wall-clock horizon, sealed-prefix structural horizon. Two hard requirements frame the evaluation: the accept or reject decision must be deterministic and convergent across nodes (a policy where one node rejects what another accepts is a permanent partition machine), and each candidate needs a DoS analysis (forged parentage that forces deep merges or deep walks).
7. Database internals adjacent to the RFC ladder: ARIES-style checkpointing and log truncation (#271), MVCC garbage collection and vacuum horizons (#266, #271), snapshot isolation and session guarantees including Bayou's tentative and committed write distinction (#272 and the ValueEntry::Pending question), anti-entropy Merkle-tree sync as in Dynamo and Cassandra AAE (bridge catch-up efficiency), causal-plus consistency systems (COPS, Eiger) for cross-entity causality, and deterministic transaction scheduling (Calvin) as the #268 scheduler analogy.

## Per-topic file format

File name: NN-slug.md (NN is the topic number). Structure:

- Title and a one-paragraph scope statement.
- Sources: one subsection per source with a link or full citation, 2 to 4 sentences on what it is and what it establishes, and an explicit relevance verdict line reading AGREES, CHALLENGES, or EXTENDS, naming the phase 2 design intent concerned (RFC number or workstream item).
- Findings: numbered, each tagged with the design area it touches.
- Candidate design deltas: bullets naming where this literature suggests changing phase 2 intent, one line of why each. The synthesis pass consumes these.

Standards: real sources with working links (verified), primary sources preferred, honest about uncertainty and paywalls, no em dashes anywhere, 150 to 400 lines per file.

## Deliverables checklist

- [ ] 01-compaction-lifecycle.md
- [ ] 02-indexed-causality.md
- [ ] 03-merkle-causal.md
- [ ] 04-merge-semantics.md
- [ ] 05-testing.md
- [ ] 06-staleness-horizons.md
- [ ] 07-database-internals.md
- [ ] design-deltas.md (adversarial synthesis)
