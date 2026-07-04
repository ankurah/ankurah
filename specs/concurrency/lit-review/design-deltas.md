# Design deltas: adversarial synthesis of workstream B

Tracking: #269. Adversarial synthesis over topic files 01 to 07. Every delta changes a
named artifact (an RFC issue or a phase-2.md workstream item) or is explicitly refused.
The refusals section and the three code-verification results are the evidence the
challenge was real, not a rubber stamp.

## Preamble: synthesis scope and the three verification results

Synthesized: the lit-review README, phase-2.md (workstreams C, D, E, checklist), RFC
issues #265, #266, #267, #268, #271, #272, #273, #274 with the landed-work comments on
#265/#266/#267, and the seven topic files. Three reader claims were checked against the
worktree code before any delta built on them was adopted.

Verification 1, Reader 7 on ValueEntry::Pending (lww.rs): CONFIRMED. The wire-ingest
path `apply_operations` calls `apply_operations_internal` with `event_id = None`
(lww.rs 159 to 162), which writes `ValueEntry::Pending` (lww.rs 297 to 301).
`ValueEntry::value()` returns the Pending value to `get()` (lww.rs 28 to 34), so it is
read as authoritative. But `apply_layer` seeds candidates only from stored entries that
carry an `event_id` and returns `MutationError::UpdateFailed` for any entry lacking one
(lww.rs 184 to 188), and `event_id()` returns `None` for Pending (lww.rs 36 to 41);
`to_state_buffer` also rejects Pending (lww.rs 117 to 119). So a Pending value is
read-visible yet neither merges nor persists. A real hole. Deltas 267-E and 272-B stand.

Verification 2, Reader 4 on #267 laws versus intent and yrs order-sensitivity: CONFIRMED
for law scope, NUANCED for the yrs mechanism. #267's listed laws (round-trip, permutation
invariance, cross-order determinism, provenance) certify replicas agree, not that they
agree on something useful (interleaving paper; OpSets). yrs `apply_layer` iterates
`layer.to_apply` in vector order into the shared doc (yrs.rs 181 to 199); CRDT merge is
commutative so final text converges under op-set permutation, but the interleaving
*outcome* convergence does not pin is exactly what Yjs is documented to get wrong
(backward interleaving, Weidner and Kleppmann). So the delta is "test yrs interleaving
directly against a golden intent oracle" (267-B), not "the DAG discipline guarantees
intent" and not "yrs diverges across orders."

Verification 3, Readers 3 and 6 on the budget producing non-convergent durable outcomes:
REFUTED as stated, REDUCED to an availability asymmetry. In `apply_event`
(entity.rs 385 to 392) and `apply_state` (entity.rs 459 to 467), `BudgetExceeded` returns
`Err(LineageError::BudgetExceeded)`. The `compare` retry loop escalates budget 4x up to
`max_budget`, then returns the error (comparison.rs 128 to 150); it commits nothing. Every
applier arm treats it as failure and calls `commit_event` only after a successful apply, so
a budget-failed event stays staged, uncommitted, retriable (node_applier.rs 131 to 163,
194 to 199, 345 to 348). No path turns `BudgetExceeded` into a durable accept-or-reject.
So it is a liveness failure, not a convergence failure. The genuine residual: a warm-cache
node integrates within budget while a cold-cache node stalls on the same comparison, so
they are transiently divergent until the cold node retries with more data. That is an
availability/observability problem, not the "permanent partition machine" the readers
describe. Deltas resting on "budget causes divergence" are downgraded throughout.

## Design deltas by area

### #266 indexed causality

266-A. Intent: generation is `1 + max(parent generation)`, cited as git "corrected commit
date v2" (#266 Representation, References). Lit: topic 02 (git design notes, Stolee)
shows this is git v1 topological level; v2 exists because v1 is loose on old-parent
reattachment and v2's fix anchors on wall clocks a clock-free design cannot copy; topic 06
corroborates (v1 walked ~3.8x more commits). Verdict: ADOPT-MODIFIED. The RFC cites a fix
it cannot use and names the version with the known pathology, misleading the D3 horizon
into trusting a tight bound. Change: edit #266 to state generation is git v1 topological
level, drop or caveat the v2 reference, note the old-parent looseness is inherited and
unfixable without a trusted clock.

266-B. Intent: applied-set gap detection is set difference (phase-2.md D2; #266 item 3).
Lit: topic 02 (Mercurial crossed linkrevs: a derived per-item index value cannot be
assumed to enumerate a monotone prefix). Verdict: ADOPT. A `generation < g` interval test
as a membership proxy reproduces the crossed-linkrev bug class. Change: state in phase-2.md
D2 (and #266 item 3) that the applied-set is a set of event ids with gap detection by true
set difference, never a generation-range test.

266-C. Intent: "Migration risk is low because the data is derived and rebuildable" (#266
Risks). Lit: topic 02 (three git commit-graph corruption post-mortems: overflow underflow
that blocked its own regeneration; offset-vs-absolute confusion; mixed-version corruption
GitLab disabled). Verdict: ADOPT-MODIFIED. A bit-packed persisted index acquires
representation edge cases that fail silently until a boundary input. Change: add to #266 a
self-consistency check (stored generation equals `1 + max(parent generation)`), a
delete-and-rebuild recovery path, a runtime-disable flag with pure-walk fallback, and a
note that generation is a small saturating `u32` with a deep-chain property-test arm; these
are correctness requirements, not polish.

266-D. Intent: alternatives rejected as "labels are invalidated by concurrent inserts and
are overkill" (#266 Alternatives). Lit: topic 02 (reachability survey, ACM CSUR 2024): the
invalidation cost is dominated by SCC merge/split and dynamism, neither of which ankurah
has; 2-hop labels insert incrementally without relabeling. Verdict: ADOPT-MODIFIED. The
rejection is right but the reason is wrong; the honest reason is the query shape (pairwise
compare plus meet) needs parent edges regardless, so a walk is unavoidable and a tighter
filter only trims constants. Change: replace the rationale in #266 with the query-shape
reason; note 2-hop labeling only if a future high-volume `dag_contains`-without-meet path
emerges.

### #267 backend contract and conformance kit

267-A. Intent: kit encodes round-trip, permutation invariance, cross-order determinism,
provenance (#267 item 2). Lit: topic 04 (interleaving paper, Peritext Example 3, list-move
duplication) and topic 05 (Jepsen, Kleppmann SEC): all these anomalies converge
deterministically and pass every listed law. Verdict: ADOPT. The kit certifies convergence
and cannot detect a semantically wrong merge. Change: add to #267 item 2 either one
intent-preservation law per structured backend (non-interleaving for sequences, span-intent
for rich text) or an explicit clause declaring intent quality out of scope and the
application's responsibility. Silence is the one unacceptable option.

267-B. Intent: cross-order determinism law resting on "resolution may depend only on graph
facts, never arrival order" (#267 Motivation). Lit: topic 04 (Fugue Table 1: Yjs
interleaving is order-sensitive in parts of its family) plus verification 2. Verdict:
ADOPT-MODIFIED. yrs final text converges under permutation, but the interleaving outcome
the laws do not pin is what Yjs mis-handles. Change: make the kit test yrs directly with
adversarial concurrent-insert configurations against a golden intent oracle, and record
that the guarantee proven is convergence of final state, not interleaving intent.

267-C. Intent: LWW runs provenance, then causal descent, then content-hash tiebreak (#267;
lww.rs 203 to 242). Lit: topic 04 (OpSets orders by Lamport to keep cause before effect; a
content-hash tiebreak is not causally consistent on its own) and topic 03 (Matrix ends on
event-id hash only after DAG ordering). Verdict: ADOPT. The code is correct today
(lww.rs 218 to 228 falls to the id comparison only on `Concurrent`), but the stage ordering
is load-bearing and only an implementation detail; a reorder could pick a causally-earlier
write. Change: promote the stage ordering to an asserted #267 law with a test that a
causally-later write always beats a causally-earlier one regardless of hash order.

267-D. Intent: LWW is the register backend; "no LWW/Yrs semantics changes" (#267
Non-goals). Lit: topic 04 (Preguica LWW-vs-MV; list-move multi-value register) shows LWW
silently discards the losing concurrent write, wrong for high-stakes fields (position,
assignee); a formatted body forced through one LWW register loses intent (Peritext).
Verdict: ADOPT-MODIFIED. Silent loss is a known trade the contract must surface. Change:
add to #267 item 3 a multi-value register option and a per-field rule for whether
concurrent loss is silent (LWW) or preserved (MV); state that structured text must live in
a CRDT backend, never an LWW register.

267-E. Intent: "decide ValueEntry::Pending (wire it to the transaction lifecycle, or
collapse the enum)" (#267 item 4). Lit: topic 07 (Bayou tentative writes are first-class
re-orderable participants with a provisional order key) plus verification 1. Verdict:
ADOPT. Pending is read-as-authoritative but cannot merge or persist; a hole, not a Bayou
tentative state. Change: keep as a #267 item but bind its resolution to the #272 semantics
decision: either give a wire-applied uncommitted value a provisional order key so it
participates in `apply_layer`, or forbid it from being read as authoritative and treat it
as an in-flight staging marker only.

### #268 ingest pipeline

268-A. Intent: uniform executor with per-item containment (#268 item 3). Lit: topic 05
(turmoil retry-race: dropped ack, retry, server double-applies; RisingWave declined
fault injection on non-idempotent writes). Verdict: ADOPT. A seeded harness attacks retry
idempotency at the apply-then-commit boundary first. Change: add to #268 item 3 an explicit
idempotency requirement (re-applying an already-committed event is a no-op) and add a C6
arm that duplicates the ack and asserts no double apply.

268-B. Intent: Disjoint is a rejection verdict (entity.rs 382 to 383). Lit: topic 03
(Byzantine causal broadcast, Auvolat et al.: buffer and re-request on a missing predecessor
rather than reject). Verdict: ADOPT-MODIFIED. Rejecting genuinely disjoint histories is
defensible, but the reject must be a function of `before(u)`; verification 3 confirms
Disjoint is decided by the completed comparison, not by `BudgetExceeded`, which is already
correct. Change: note in #268 (and #265's adoption-predicate item) that Disjoint is
reachable only through the grounded exploration boundary, never via `BudgetExceeded`, and
that a dangling-parent event stays staged (buffered) rather than becoming a hard drop.

### #271 history lifecycle (seal mechanism; horizon in its own section)

271-A. Intent: "is GDPR erasure a seal plus payload scrub" (#271 Open questions; the RFC
bundles GDPR and compaction). Lit: topic 01 (crypto-shredding: destroy a per-subject key,
leave hashes intact) shows erasure and compaction have opposite relationships to the hash
chain; sealing to erase breaks content addressing, crypto-shredding to save space does not.
Verdict: ADOPT. Change: split #271 so GDPR erasure is per-field crypto-shredding (hash
chain untouched) and sealing is size-only; answer the erasure open question "crypto-shred,
do not seal."

271-B. Intent: sealed snapshot with genesis attestation (#271 items 1, 2). Lit: topic 01
(OpSets: state is a function of the whole set, so sealing changes the object per node) and
topics 01/06 (Yjs id-range residue is the irreducible metadata floor). Verdict: ADOPT.
Change: require in #271 that the sealed snapshot is a content-addressed synthetic event (a
deterministic, node-agnostic function of graph facts) that retains a (source, clock-range)
id skeleton below the seal so behind-the-seal comparison probes can be answered.

271-C. Intent: prune events strictly below C (#271 item 1). Lit: topic 01 (Automerge Repo
concurrent-compaction data-loss bug) and topics 01/07 (ARIES fuzzy checkpoint, Calvin
Zig-Zag: checkpoint without quiescence via computed watermark and two-copy). Verdict:
ADOPT. The seal write is the same ordering hazard #268/#273 guard for ingest; a
stop-the-world seal is an avoidable spike given events-as-truth. Change: add to #271 the
seal write discipline (unique seal keys, delete pruned events only after the seal is
durably committed and referenced) and the without-quiescence read discipline (seal reads a
consistent snapshot of committed events while new events stage).

271-D. Intent: sealing folds per-backend state buffers (#271 item 1). Lit: topic 04
(Attiya: sequence-CRDT metadata is at least linear in total historical deletions,
independent of visible size). Verdict: ADOPT. The E divergence-window target bounds the
reverse walk but not the CRDT tombstone set. Change: state in #271 that sealing folds CRDT
state (yrs doc tombstones), and adjust the E4 memory criterion to note the CRDT tombstone
lower bound is deletion history, not the divergence window.

### #272 transactional visibility

272-A. Intent: decide receive-side buffering versus per-entity eventual consistency (#272
Decision). Lit: topic 07 (snapshot isolation and write skew: a local read-A-write-B
transaction has a cross-entity read-write anti-dependency ankurah records nowhere, so
multi-entity commits are at best SI even locally, and the per-entity receiver cannot detect
it). Verdict: ADOPT. Source-local semantics are SI, which the RFC never states. Change:
require #272 to state the isolation level for multi-entity commits; if per-entity eventual
consistency, say plainly that cross-entity write-skew-prone invariants are the
application's responsibility.

272-B. Intent: "ValueEntry::Pending semantics question belongs here" (#272 Motivation).
Lit: topic 07 (Bayou tentative/committed, provisional order key) plus verification 1.
Verdict: ADOPT. Change: #272 decides whether a wire-applied uncommitted value is a
Bayou-style tentative participant (needs a provisional order key to merge) or is not
authoritative state; #267 implements the mechanical disposition per that decision.

272-C. Intent: general cross-entity causal consistency is a non-goal (#272 Non-goals). Lit:
topic 07 (COPS/Eiger one-hop dependencies bound cross-entity metadata; Spanner shows the
clock alternative ankurah forbids). Verdict: REJECT as a scope change; record as context.
Cross-entity strict serializability is out of scope by construction under the no-wall-clock
invariant. Change: add one line to #272 Non-goals that cross-entity external consistency is
unavailable by construction so users do not assume a global order; do not add the
machinery. (See Refusals 3 and 4.)

### #273 snapshot authority

273-A. Intent: a snapshot that cannot fast-forward degrades to needs-events (#273
Principle; apply_state returns DivergedRequiresEvents, entity.rs 443 to 453). Lit: topic 01
(Oracle ORA-01555, Postgres xmin: a read past the oldest reader is refused, not answered
wrong) and topic 07 (session guarantees: server-side degradation does not deliver
cross-node read-your-writes). Verdict: ADOPT. #271 and #273 share one failure mode (cannot
serve below the seal/reader horizon), and the session dimension is a real gap. Change: have
#273 name the single "cannot serve, request events" path shared with #271, and add a
documented non-guarantee that cross-node read-your-writes and monotonic-reads are not
provided by snapshot degradation alone (or, if promised, a per-session write-set gated on
node freshness).

### #274 validated ingress

274-A. Intent: ValidatedEvent is the single admission type (#274 Proposal). Lit: topic 03
(Kleppmann-Howard I-confluence Theorem 3.1: a BEC-maintainable invariant must be
I-confluent; uniqueness and non-negative-balance are not) and topic 04 (move-op: locally
cycle-free re-parents compose into a global cycle). Verdict: ADOPT. `ValidatedEvent` risks
implying "globally valid." Change: state the I-confluence boundary in #274 (per-property,
causal-history-local checks are load-bearing; cross-entity uniqueness and referential
integrity are out of scope and belong to the durable/consensus tier), and note that
application-level structural invariants like an acyclic tree are not guaranteed by
per-event validity.

274-B. Intent: PolicyAgent admission at ingress (#274 item 4). Lit: topic 03 (Merkle-CRDTs,
Blocklace, OrbitDB: content addressing buys tamper-evidence, de-dup, cycle-freedom,
unforgeable happened-before, but NOT authenticity or authorization). Verdict: ADOPT.
Change: add to #274 that authorship/authorization is a signature layer distinct from
content-hash structural validity, and state the Byzantine guarantee at true precision
(weak-safety causal order and SEC among correct nodes for arbitrarily many Byzantine peers,
without authorship, strong-safety causal order, or a DoS bound, all of which need
signatures ankurah does not carry).

274-C. Intent: per-#246 size limits at ingress; DoS handling implicit (#274 item 1). Lit:
topic 03 (Blocklace: unsigned hash-DAG CRDTs allow unbounded distinct valid equivocations,
no finite-harm bound) and topic 06 (Matrix fake prev_events; Aleph fork-bomb: local
validation admits exponentially many forged units). Verdict: ADOPT. Content-hash de-dup
stops replay of identical events, not flooding of distinct concurrent ones; Aleph's fix is
non-local (one unit per creator per round). Change: add per-source ingress rate limiting to
#274 (bound forged breadth), cross-referenced from the #271 horizon and C4, since the seal
bounds forged depth but not breadth.

### C1/C2 test harness

C1-A. Intent: seeded transport, invariant checks after quiescence (phase-2.md C1). Lit:
topic 05 (S2, WarpStream, FDB run a seed twice and diff the full event trace; a
non-reproducible run voids all results). Verdict: ADOPT. Change: add to C1 a determinism
self-check (same seed twice, hash the full event trace, fail on any diff) as a first-class
invariant separate from convergence.

C1-B. Intent: "all-node convergence after quiescence" (phase-2.md C1). Lit: topic 05
(aphyr, Kleppmann SEC: convergence is liveness, unprovable by a finite run; Jepsen/Elle
give the assertion form). Verdict: ADOPT. Change: define C1 convergence only at an enforced
quiescence point (faults healed, in-flight released, sim drained), phrased as "every node's
final read exists, contains every acknowledged write, has no unexpected value, and is
byte-equal across nodes," asserted across permuted apply orders; prefer an Elle-style
dependency-graph cycle checker over a Knossos linearizability search for the C2 tier.

C1-C. Intent: fault vocabulary applied per run (phase-2.md C1, C2). Lit: topic 05 (swarm
testing, ISSTA 2012: all-features-every-run suppresses bugs; random subsets found 42% more
crashes). Verdict: ADOPT. Change: make C1/C2 fault injection swarm-style, each seed
enabling a random subset of the fault kinds, not their union.

C1-D. Intent: "simulate real Nodes," implying executor-swap (phase-2.md C1). Lit: topic 05
(Polar Signals state-machine-bus makes nondeterminism a compile error; Antithesis argues
language-level determinism leaks). Verdict: ADOPT-MODIFIED. The architecture fork is real
and phase 2 silently assumes one side. Change: record an explicit C1 architecture decision
(executor-swap versus state-machine-bus); if executor-swap, budget a recurring
determinism-audit against HashMap ordering, `Instant::now`, detached threads, and the
`spawn_blocking`-defeats-time-auto-advance gotcha (load-bearing because quiescence detection
needs all tasks blocked).

### C4 threat model

C4-A. Intent: arms for malformed clocks, forged parents, cycles, oversized batches, replay
floods (phase-2.md C4). Lit: topics 03 and 06 (equivocation flooding has no finite-harm
bound without signatures; forged/deep parentage is correctness-safe but resource-unsafe;
dangling-parent staging is permanent). Verdict: ADOPT. The listed arms omit the
equivocation-flooding and dangling-parent classes. Change: add to C4 explicit arms for
equivocation/concurrency flooding, forged/deep parentage (assert containment: per-item
failure, no head advance, budget-capped), and dangling-parent staging (permanent stage,
buffered not dropped), each mapping to a named threat-model claim, deciding per arm between
a signature/attribution layer and a rate/quantity cap.

### C5/C6/C7 coherence, recovery, storage conformance

C5C7-A. Intent: reactor/LiveQuery coherence (C5); storage engine conformance (C7). Lit:
topic 05 (Jepsen session guarantees compose into causal) and topic 07 (phase 1 wasm/postgres
divergent clock decoding is the drift existence proof). Verdict: ADOPT. Change: phrase C5
coherence as monotonic-reads / read-your-writes in Jepsen terms, reuse Elle where feasible;
no C7 scope change beyond citing the drift proof.

C6-A. Intent: crash/recovery injection at commit_event/set_state, mid-batch, mid-merge
(phase-2.md C6). Lit: topic 05 (RisingWave declined injection on non-idempotent writes) and
topic 07 (ARIES restartable recovery; ankurah needs no undo since merge is a pure function
and sealing folds forward). Verdict: ADOPT. Change: add to C6 an assertion that recovery is
replay-from-committed-state only (no undo pass) and that re-delivery across a crash at the
apply-then-commit boundary produces no duplicate application (ties to 268-A).

### E benchmarking and optimization

E-A. Intent: "memory bounded by divergence window, not history" (phase-2.md E3, E4). Lit:
topic 03 (merge cost scales with divergence age, corroborating) and topic 04 (Attiya lower
bound, qualifying). Verdict: ADOPT. The target is sound for the reverse walk but does not
bound a sequence-CRDT tombstone set. Change: keep the E3/E4 target; add the caveat (also
271-D) that the CRDT tombstone lower bound is deletion history and is folded by sealing, not
by the streaming consumer.

E-B. Intent: bridge catch-up rides head-driven backward BFS (phase-2.md E; topic 07
confirms no anti-entropy code exists). Lit: topic 07 (Dynamo/Cassandra AAE Merkle digests
answer "what differs" sublinearly but overstream on sparse diffs with fixed-depth trees and
rebuild under churn). Verdict: ADOPT-MODIFIED. A segment/Merkle digest over entity heads or
the #266 applied-set would skip converged ranges using content-addressed EventIds as leaf
hashes, but avoid the fixed-depth anti-pattern. Change: add to the E optimization targets a
divergence-summary digest over entity heads or the applied-set, avoiding fixed-depth trees
and partition-keyed segmentation that rebuilds under head movement; scope as a
measurement-driven target, not a wire redesign.

## Cross-reader contradictions

1. Generation-distance as a horizon signal. Topic 06 rates it "conditionally passes" and
layers it under the seal; topics 02 and 06 show `gen(head) - gen(meet)` is loosest exactly
in the adversarial-staleness case (v1 old-parent pathology) and measures depth not breadth.
Resolution: consistent once stated precisely. Generation distance is a valid deterministic
*soft* horizon for early warning, provided the function is immutable and locally
recomputed, but not a complete backstop. The structural seal is authoritative; generation
distance is advisory. The RFC must not let it masquerade as a tight bound (266-A).

2. Is sealing worth it? Topic 01 challenges pruning outright (Automerge keeps full history;
Automerge 3.0 solved size by lossless compression; the space win is "not all that great").
Topic 06 concludes #271's load-bearing justification is traversal-bounding and turning stale
arrivals into decisions, not bytes; topic 03 confirms merge cost scales with divergence age.
Resolution: agree once the goal is disambiguated. Sealing for *storage* is weakly motivated
and should be benchmarked against compression first (Refusal 2); sealing as the *structural
rejection horizon* is well motivated. Separate and justify the two goals independently.

3. Buffer versus reject on a missing predecessor. Topic 03 says never reject on a missing
predecessor, buffer and re-request; topic 06 and #271 want an explicit rejection horizon for
below-seal parentage. Resolution: no real conflict. Buffer a dangling parent that is merely
not-yet-arrived (may resolve); reject a parent below the seal (provably pruned, cannot
resolve). The distinction is above versus below the seal floor, a structural fact both
readers honor. Encoded in 268-B and the horizon recommendation.

## The #271 rejection-horizon resolution (load-bearing deliverable)

Recommendation: adopt the sealed-prefix structural horizon (topic 06 Candidate D) as the
authoritative, convergent accept-or-reject mechanism for #271. Layer generation-distance
(Candidate B) on top as a deterministic *soft* horizon for early warning and routing once
#266 lands, under strict discipline (immutable, locally recomputed, never from the wire).
Keep the retrieval budget (Candidate A) as an error-shaped safety valve beneath both, never
the policy horizon. Reject the attested wall-clock horizon (Candidate C) as a gate.

Why structural, not wall-clock (unanimous across topics): topic 06 (Cassandra
gc_grace_seconds zombies when repair misses the deadline; ScyllaDB replaced it with "GC only
after repair"; Dynamo timestamp-keyed truncation drops the descent pair; MongoDB
rejects-then-forces-resync on the objective fact that oplog entries are gone). Topic 01
(Kafka zombie/resurrection is per-node time-windowed compaction producing permanent
disagreement, the exact partition machine the requirement forbids; Postgres xmin and Oracle
ORA-01555 tie the reclaim point to the oldest reader, not a clock). Topic 07 (ARIES and
Calvin truncate at a computed watermark). Topic 03 (Matrix rejected depth as an untrusted
peer signal; a wall clock is the same class, additionally writer-settable). A structural
horizon (is this event's parent below seal clock C) is an objective content-addressed graph
fact every node with the same seal decides identically, satisfying the
determinism-and-convergence requirement by construction.

Convergence caveat on the seal itself: the one residual divergence is disagreement about
where C is. Topic 03 (Merkle-CRDTs, BEC) proves pruning needs a known replica set or an
external truth, and ankurah's open membership makes global stability unobservable; so the
seal must be justified by *authority scope* (a durable-tier attestation that a prefix is
canonical) carrying the genesis attestation, not by "everyone has it." Topic 01 (OpSets)
adds the sealed snapshot must be a content-addressed synthetic event so all nodes interpret
the same set. Topic 07 (Postgres xmin, CouchDB _revs_limit premature pruning under
conflicts) warns a horizon computed as a min over peer positions lets the slowest or dead
peer pin retention for everyone; the phase-2 bounded rejection horizon is the escape hatch
(cap how far back a participant may force retention; evict a stuck peer as an anomaly), and
the prune-safety invariant must be that a seal removes only events below the meet of all
live branches (proven-common history), tested as a hard invariant.

Layering and the PolicyAgent knob: the structural seal is the hard floor. An event whose
parents fall at or below C is refused the normal integration path and surfaces (via the #268
planner as a typed outcome, not a stringly error) to a PolicyAgent decision: reject as too
old, or route to a designated rebase/merge path; never silently drop. Above the seal,
generation distance lets a policy flag or route a branch whose `gen(head) - gen(meet)`
exceeds a threshold before the floor is reached, for early warning and UX, subject to
266-A's caveat that the distance is loose under old-parent reattachment. The PolicyAgent
knob exposes: (1) the structural reject-or-rebase decision for below-seal parentage (the
hard, convergent gate), and (2) an optional generation-distance threshold for soft routing
above the seal (advisory, deterministic, never a silent drop). Resurrection semantics for
the reject-or-rebase path must be defined up front (topic 06, CouchDB separate-sweep
resurrection): specify whether a rejected below-seal branch is dropped, quarantined, or
rebased, and whether it can resurface, before D3 implementation; this interacts with the
#272 Pending question.

DoS story: the structural seal converts forged deep parentage into a bounded rejection at
the seal boundary (defusing the Matrix fake-prev_events pattern, where harm came from
endlessly chasing parents below the retained frontier, and the Aleph fork-bomb's depth
axis). But content addressing bounds identity forgery, not ancestry volume, so the seal
alone does not bound forged *breadth* above it (many concurrent forged events at similar
generation). The seal must be paired with per-source ingress rate limiting at #274 (274-C),
Aleph's non-local mitigation in ankurah form. The budget remains underneath as a per-walk
safety valve; per verification 3 it is a liveness backstop, not a convergence mechanism, and
per topic 06 it fails the DoS requirement in the many-request limit (each walk stays under
budget while aggregate spend is unbounded across walks and entities).

Net: authoritative horizon = sealed-prefix structural (convergent by construction, DoS depth
bounded at the boundary). Soft horizon = generation distance (deterministic, advisory, loose
under old parents). Safety valve = retrieval budget (liveness only). Breadth bound =
per-source ingress rate limit at #274. Rejected gate = attested wall clock (resurrects data,
boundary-disagreement partition risk, writer-settable).

## Deltas the synthesis REFUSED to adopt

Refusal 1: "the budget produces non-convergent durable decisions" (Readers 3 and 6).
Refused as stated. Verification 3 shows `BudgetExceeded` is always an `Err` that fails the
operation and leaves the event staged-but-uncommitted; no path makes it a durable
accept-or-reject. Adopting a delta premised on budget causing divergence would mis-scope the
fix toward a convergence mechanism when the real exposure is an availability asymmetry
(warm-cache node integrates, cold-cache node stalls until retry). The correct smaller delta
is observability (the budget-exceeded counter and escalation histogram already in D7) plus
the honest note that budget is a liveness backstop.

Refusal 2: "adopt structural pruning for the storage-size goal" (implicit in #271's storage
motivation). Refused pending measurement. Topic 01 (Automerge 3.0: 10x to 100x reduction by
lossless columnar/RLE compression, zero correctness cost) and topic 06 (Automerge
maintainers: dropping history saves "not all that great") say compression may beat pruning
for size with none of pruning's correctness cost. Sealing *for the rejection horizon* is
adopted; sealing *for size* is deferred until the E workstream benchmarks columnar
compression of the event log against sealing. This declines the RFC's own stated first
motivation.

Refusal 3: "add cross-entity causal dependency tracking (COPS/Eiger one-hop edges) to #272."
Refused as a phase 2 change. The case is strong (one-hop nearest dependencies keyed by
generations would bound cross-entity metadata), but #272 explicitly excludes general
cross-entity causal consistency and the plan sequences it nowhere; adopting it would expand
D4 into a research item. Kept as documented context (272-C), parked for phase 3.

Refusal 4: "make generation numbers double as a cross-entity causal timestamp" (Reader 7,
from Eiger). Refused for phase 2. A sound inference, but it presupposes Refusal 3's edge
tracking and would couple #266's intra-entity depth to an unscoped cross-entity role.
Recorded as a phase-3 direction.

Refusal 5: "the C1 harness should use a hypervisor (Antithesis) rather than pure-Rust"
(topic 05). Refused as the primary plan. The critique is fair (language-level determinism
leaks), but hypervisor-based DST is disproportionate to phase 2, whose load-bearing
verification is the simulation harness. The adopted form is the weaker C1-D (record the
architecture decision, budget a determinism-audit, *consider* a hypervisor tool for the
nightly C2 tier only), not a wholesale replacement.
