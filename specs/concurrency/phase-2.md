# Concurrency Phase 2: Maturation

Scope agreed 2026-07-04. Phase 1 was the event_dag overhaul plus the July
verification and remediation (PR #201; full history on branch
`archive/201-concurrent-updates-specs`). Phase 2 matures the architecture,
proves it under adversarial schedules, and then, as a separate PR, measures
and optimizes it. Strategic direction lives in RFC issues #265, #266, #267,
#268 and the four to be filed at kickoff (workstream D).

Working conventions carry over from phase 1: red-to-green commits where a
test can pin the change, tracker checkboxes updated in the fix commit, no em
dashes in anything GitHub-visible, `cargo fmt --all` before commits, `taplo
fmt` after any Cargo.toml change, and the validation gate (core lib,
ankurah-tests, jwt-auth, isolated `cargo check -p ankurah-core --features
wasm`, fmt, taplo).

## Workstream A: situational awareness

Survey all open GitHub issues and classify each against this plan: absorbed
by a phase 2 workstream, blocked on one, independent, or stale. Known
intersections to check first: #242-#247 (hardening list), #236 (Yrs empty
string, blocks an ignored test), #256 (read freshness), #264 (commit-time
policy hook, adjacent to validated ingress), #200 (chain optimization,
subsumed by #268 planning). Output: a triage table appended to this document
and comments on any issue whose fate this plan changes.

## Workstream B: literature review

Run with Opus subagents (expressly not Fable) as parallel readers, one topic
each, with an adversarial synthesis pass at the end that must challenge our
current design choices rather than affirm them. Seed list, to be extended
during A:

- Compaction and history lifecycle: Automerge columnar compression and
  document forking, Yjs garbage collection, Chronofold/OpSets, git shallow
  clone and grafts semantics.
- Indexed causality: git commit-graph generation numbers (v1 and corrected
  v2), Mercurial revlog linkrev, reachability labeling literature (GRAIL,
  interval labels) for the applied-set index design.
- Merkle-causal systems: Merkle-CRDTs (IPFS/OrbitDB), Byzantine causal
  broadcast, hash-graph gossip; what they pay for adversarial tolerance and
  which of those costs we already pay via content addressing.
- Merge semantics: Kleppmann's local-first corpus, ORDTs, movable-tree and
  rich-text CRDT merge anomalies (for backend contract law design).
- Testing: FoundationDB simulation testing, turmoil/madsim style deterministic
  network simulation in Rust, Jepsen's checker vocabulary for convergence
  claims.

Output: an annotated bibliography plus a short "design deltas" memo listing
every place the literature disagrees with phase 2's intended designs, each
with adopt/reject rationale.

## Workstream C: test bulletproofing

Goal: the concurrency approach should survive schedules nobody hand-wrote.

1. Deterministic multi-node simulation harness: seeded virtual transport
   between real Nodes with reorder, delay, duplication, drop, and partition;
   invariant checks after quiescence (all-node convergence, no lost write, no
   phantom entity, head antichain validity). V4 and V6 class bugs should be
   findable by search rather than by construction. Land early so workstream D
   refactors happen under this net.
2. Oracle scale-out: nightly high-seed run of the comparison property test
   (100k+ seeds) and a seeded-failure artifact format so any hit is
   reproducible from the log line.
3. Backend conformance kit (#267): executable laws (round-trip, layer
   permutation invariance, cross-order determinism, provenance) run against
   LWW and Yrs; revive or delete pn_counter as the third implementation.
4. Wire-level adversarial suite: malformed clocks, forged parents, cycle
   attempts, oversized batches (ties into #246), replay floods; assert
   containment semantics from the C3 work hold for every arm.

## Workstream D: architecture

Implementation order chosen so each step lands on the previous one's
foundation; each item is or becomes an RFC issue.

1. **#268 ingest pipeline.** Per-entity ready-queue scheduler: application is
   a convergent function of the staged set, not of arrival order. Absorbs the
   B3 gap-replay defense structurally, unifies partial-progress containment
   across arms, introduces the typed error taxonomy and the needs-state
   recovery outcome.
2. **#266 indexed causality plus applied-set.** Persistent per-event
   generation numbers, then a per-entity applied-set membership index (local,
   derived, never trusted from the wire). Redelivery and StrictAscends become
   O(1); gap detection becomes set difference; budget demotes to an anomaly
   guard.
3. **History lifecycle (RFC to file).** Sealed-prefix checkpoints: fold
   history below clock C into an attested snapshot, prune beneath, clamp
   traversals at C, carry a genesis attestation so Disjoint detection and
   entity identity survive sealing. This is also the GDPR/compaction story
   flagged as "Discuss" in DESIGN_GOALS.
   Includes the **concurrency rejection horizon** (question raised
   2026-07-04): today nothing rejects arbitrarily old concurrency; budget is
   the only backstop and it is error-shaped, not policy-shaped, so a
   months-stale branch can force a deep re-layering merge (thrash). With
   generations, staleness distance is deterministic (gen(head) minus
   gen(meet)); with sealing, the horizon is architectural: an event whose
   parents fall below the seal cannot integrate normally and hits an explicit
   policy decision (reject as too old, or route to a designated rebase/merge
   path). Expose the policy knob through PolicyAgent so applications choose
   their horizon; never silently drop.
4. **Transactional visibility (RFC to file).** Multi-entity commits are
   atomic locally and carry a TransactionId, but receivers integrate per
   entity, so peers can observe cross-entity states that never existed at the
   source. Decide and implement: receive-side transaction buffering
   (visibility unit = transaction) or an explicit documented contract of
   per-entity eventual consistency. Interacts with the scheduler (1) which
   gives a natural buffering point.
5. **Snapshot authority principle (RFC to file).** Events are the sole
   truth; a snapshot is a cached proof of a prefix that MUST degrade to a
   needs-events request when it cannot fast-forward. Closes the diverged
   snapshot staleness hole and deletes ad hoc reconciliation branches.
6. **Validated ingress.** A single ingress producing a ValidatedEvent type
   that is the only thing the applier accepts, making "no unvalidated event
   touches state" structural (closes the #244 class). Adjacent to #264's
   commit-time hook; coordinate the PolicyAgent surface once.
7. **Observability.** Verdict counters, budget and escalation histograms,
   anomaly alerts (empty meet, budget exceeded, disjoint rejection, phantom
   eviction). Small, but it is how V1/V2-class defects become visible in
   production instead of silent data loss.
8. **#265 remainder and #267 remainder.** Adoption-predicate unification,
   grounding-ancestry caching, step() drain restructure; the designed
   layer-view API once the conformance kit exists to constrain it.

## Workstream E (separate PR): benchmarking, then optimization

The benchmark HARNESS lands at phase 2 start so the baseline predates
workstream D; the optimization pass runs after D stabilizes.

1. Criterion micro-benchmarks: compare() across DAG shapes (linear deep,
   uneven diamond chains, wide antichains, disjoint), layer iteration, clock
   operations, topo sort; tracked in CI with regression thresholds.
2. Macro benchmarks: N-entity multi-node churn via the simulation harness
   (throughput, convergence latency, memory high-water).
3. Optimization targets, informed by measurements rather than assumption:
   - **Streaming application** (question raised 2026-07-04): the
     architecture is already streaming-shaped in its skeleton/payload split;
     the accumulator keeps the parent-edge map (small, unbounded) separate
     from event payloads (heavy, LRU-bounded), which is exactly the "cache to
     avoid refetches in the average case". The reverse walk already fetches
     one event at a time; with generations its explored window is bounded by
     the divergence window rather than by history. Remaining gaps to close:
     entity.rs buffers ALL layers into a Vec before applying (stream the
     consumer layer-by-layer under the head-recheck discipline, keeping the
     dag skeleton for LWW comparisons while payloads flow through the LRU);
     EventBridge materializes full batches on both ends (chunked bridge
     framing, coordinate with #246 size limits). True O(1)-memory reverse
     search is not achievable without precomputed indexes; bounded-by-
     divergence-window is the honest target and is sufficient.
   - Accumulator memory (dag map plus LRU clone duplication; Arc<Event>).
   - Grounding-ancestry incremental maintenance if benchmarks show it.
4. Success criteria: no correctness gate regressions; published
   before/after numbers per optimization; memory bounded by divergence
   window, not history, for the streaming consumer.

## Non-goals

Wire protocol redesign (chunked bridge framing rides existing message
shapes); the federated hypergraph aspirations in LONG_TERM_ASPIRATION; new
storage engines.

## Sequencing summary

A and B run first and in parallel (B's subagents are Opus by request). C1
lands before D begins. D proceeds 1 through 8 with the full gate per item. E's
harness lands with C, E's optimization runs as its own PR after D. Every
workstream ends by updating this document's checklist.

## Checklist

- [ ] A: issue triage table appended, issue comments posted
- [ ] B: bibliography plus design-deltas memo committed
- [ ] C1: simulation harness with reorder/drop/dup/partition and convergence invariants
- [ ] C2: nightly oracle scale-out
- [ ] C3: backend conformance kit; pn_counter decision
- [ ] C4: wire-level adversarial suite
- [ ] D1: #268 scheduler pipeline
- [ ] D2: #266 generations plus applied-set index
- [ ] D3: lifecycle RFC filed and implemented (checkpoints, rejection horizon)
- [ ] D4: transactional visibility RFC filed and decided
- [ ] D5: snapshot authority RFC filed and implemented
- [ ] D6: validated ingress
- [ ] D7: observability counters and alerts
- [ ] D8: #265 and #267 remainders
- [ ] E1: benchmark harness and baseline (lands at phase 2 start)
- [ ] E2: optimization PR with before/after numbers
