# Concurrency Threat Model

Phase 2 workstream C item 4, part 1 of 2. Tracking: #269. Scope:
specs/concurrency/phase-2.md. This is the document the wire-level adversarial
suite (part 2) asserts against: every future adversarial test maps to a named
claim (C4-NN) in the registry below. The validated-ingress RFC (#274) calls this
document "the checklist this ingress implements."

Sections: (1) trust context, node classes, and the content-addressing ledger;
(2) adversary tiers and per-wire-arm attack surface; (3) the numbered claims
registry, the core artifact; (4) the attestation load-bearing map; (5) the
known-gaps table. Every claim about current behavior cites its enforcing code
seam. Where the literature challenges a claim it is
incorporated or rebutted inline. Where enforcement does not exist, the claim
records the status honestly (open gap, owning RFC) rather than aspirationally.

## Trust tiers (used throughout)

- **Byzantine-safe**: holds against arbitrary peers crafting wire payloads freely.
  The guarantee derives from structure (content addressing, deterministic graph
  facts) and needs no honest-peer assumption.
- **Trusted-peer**: holds only if peers are honest but possibly buggy; a malicious
  peer can violate it. These are the invariants a real PolicyAgent or a hardening
  item (#274, #246, #271) must convert to Byzantine-safe.
- **Attestation-dependent**: holds iff the attestations involved verify; exactly as
  strong as the PolicyAgent's signature scheme, and nil under the default
  PermissiveAgent.

The Byzantine guarantee at its true precision (lit review topic 3, sources 4/5/6,
design-delta 274-B): the content-addressed DAG delivers **weak-safety causal order
and strong eventual consistency among correct nodes for arbitrarily many Byzantine
peers, without authorship attribution, authorization, strong-safety causal order,
or a DoS bound**. The last four require a signature layer the codebase does not
carry. That is the difference between "the algorithm converges" (true, proven by
the construction) and "the system resists a malicious peer" (false by default,
only as true as the deployed PolicyAgent).

---

## 1. System and trust context

### 1.1 Node classes

Two classes, distinguished by the `durable` flag threaded through retrieval and the
applier. **Durable nodes** hold authoritative, definitive storage:
`LocalEventGetter::storage_is_definitive() == self.durable` (`core/src/retrieval.rs`),
so "event absent" is a real negative the creation-uniqueness guard relies on (C4-06);
they serve events/state and are the fallback ephemeral nodes fetch from. **Ephemeral
nodes** have non-definitive storage (`storage_is_definitive()` defaults to `false`)
and fall back to a durable peer: `CachedEventGetter::get_event` checks staging, then
local storage, then `node.get_durable_peer_random()`.

Peer class is **self-asserted, not authenticated**: `node.rs` `register_peer` inserts
into `durable_peers` when `presence.durable` is true, with no signature. Any peer can
advertise durable and be selected by `get_durable_peer_random`, which an ephemeral
node then trusts for fetches (C4-16, gap G-8).

### 1.2 What peers are trusted with today

`PolicyAgent` (`core/src/policy.rs`) is the single authority surface, carrying the
hooks that make peer input trustworthy: `validate_received_event`,
`validate_received_state`, `check_event`, `check_read_event`,
`validate_causal_assertion`, `attest_state`, and the read/write/collection gates.
The **default agent is `PermissiveAgent`**, which trusts everything: the two
`validate_received_*` and `validate_causal_assertion` return `Ok(())`,
`check_event` and `attest_state` return `None`, and every read/write check returns
`Ok(())`. Under PermissiveAgent, **every attestation-dependent and trusted-peer
claim below provides no guarantee**; only the Byzantine-safe structural ones
survive. Real PolicyAgents exist (jwt-auth ships `adversarial_tests.rs` and
`adversarial_rbac_tests.rs`) and are the intended production posture; this document
is written for that posture, stating which invariants a correct agent can enforce
versus which no agent can (missing seam, or not I-confluent).

### 1.3 What content addressing buys, and what it does not

The per-entity DAG is structurally a Merkle-CRDT (lit review topic 3): an `EventId`
is a SHA-256 hash over `(entity_id, operations, parent, generation)`; events reference
predecessors by hash; the head is the tip antichain; identity is anchored at a
creation event (empty `parent`). It buys **for free** (Byzantine-safe):

- **Self-verifying ids / tamper evidence**: the id is recomputed from contents on
  every use (`Event::id()`), never read from the wire (C4-01).
- **Cycle-freedom**: a parent hash is unknowable before its content exists, so an
  honest DAG has no cycle and a fabricated one is rejected (`topo_sort_events`, C4-04).
- **De-dup of identical replays**: byte-identical events share one id and collapse,
  keyed by `event.id()` (C4-05).
- **Unforgeable happened-before**: a peer cannot claim B precedes A unless A
  genuinely names B; the *genuine* ancestry relation is unforgeable (Finding 6).

It does **not** buy (each on a named gap or agent responsibility):

- **Attribution / authorization**: the hash says nothing about who authored or
  whether they were allowed; both need signatures layered above the DAG (Finding 6;
  274-B; OrbitDB `canAppend`). `Attestation` is opaque bytes (`proto/src/auth.rs`,
  `Attestation(pub Vec<u8>)`), meaningful only via the agent.
- **Equivocation resistance / DoS bound**: every forged concurrent event is a
  distinct genuine hash, so de-dup stops identical replays but not flooding of
  distinct ones; unsigned hash-DAG CRDTs permit unbounded distinct valid
  equivocations with no finite-harm bound (Finding 7, Blocklace; 274-C). Gap G-4.
- **Ordering authority**: weak-safety causal order only, not strong-safety
  (Finding 9: strong safety needs cryptography). Events carry a hashed
  generation, but admission re-derives it where possible and comparison uses it
  only for scheduling and suppress-only acceleration, never for a verdict
  (Finding 11, the Matrix depth CVE; C4-22).

---

## 2. Adversary catalog

### 2.1 Capability tiers

- **T0. Buggy peer (not malicious).** Honest software with defects: reordered
  batches, duplicate deliveries, a clock that lost sort order across a hand-rolled
  decoder (phase 1 wasm/postgres drift, V5), events before their parents. Cannot
  deliberately forge hashes but presents structurally surprising input. Most phase
  1 data-loss bugs (V1, V2, V4, V6) were reachable by T0, not only by malice.
- **T1. Malicious authenticated peer.** Passed `check_request`, now crafts wire
  payloads freely within its arms: forged parent clocks, deep/dangling parents,
  oversized batches, replay/equivocation floods, malformed clocks, fabricated
  cycles, disjoint genesis roots. Cannot break a content hash but can send any
  content under that content's correct id.
- **T2. Malicious peer with policy authority.** T1 plus attestations the local
  agent accepts (stolen key, compromised attester, or just PermissiveAgent).
  Defeats every attestation-dependent claim; Section 4 enumerates the damage.
- **T3. Compromised durable node.** T2 plus authoritative storage and the durable
  role: the fetch source for ephemeral peers (C4-16), the `GetEvents`/`EventBridge`
  server, a node the relay quorum waits on, and (once #271 lands) a seal signer. It
  can equivocate on served history and inject unvalidated events into an ephemeral
  peer's storage. The strongest in-scope adversary; defenses are mostly future work
  (attestation verification, phase 3 multi-durable quorum).

### 2.2 Wire arms and the validation each receives today

Receive-side arms and the policy hook each gets (verified in
`core/src/node_applier.rs` and `core/src/node.rs`):

| Wire arm | Handler | validate_event | validate_state | topo-sort | Note |
|---|---|---|---|---|---|
| `EventOnly` | `apply_update` | yes | n/a | yes | staged, applied parents-first |
| `StateAndEvent` | `apply_update` | yes | yes | yes | state fast-path, event fallback |
| `StateSnapshot` | `apply_delta_inner` | n/a | yes | n/a | state-only |
| `EventBridge` | `apply_delta_inner` | yes | n/a | yes | catch-up batch |
| `StateAndRelation` | `apply_delta_inner` | unimpl | unimpl | n/a | returns `InvalidUpdate` |
| `GetEvents` resp, mid-BFS | `CachedEventGetter::get_event` | **NO** (#244) | n/a | n/a | identity and lineage scope checked before caching; no policy hook |
| `GetEvents` req, served | `node.rs` `GetEvents` arm | n/a | n/a | n/a | serve side DOES `can_access_collection` + `check_read_event` |
| `commit_remote_transaction` | `node.rs` same | via `check_event` | via `attest_state` | n/a | every event is policy-checked on a fork before canonical mutation |

("yes" = via `validate_and_stage`.) The key asymmetry: **every `apply_*` arm
validates before staging, but the BFS-time remote fetch does not** (#244). Section 3
gives each row its own claim.

---

## 3. Claims registry

The core artifact. Each claim gives its **Invariant**, **Trust tier**
(Byzantine-safe | trusted-peer | attestation-dependent), **Enforcing seam**
(`path::function` verified, or "none today"), **Falsifying attack**, **Planned test
arm** (the C4 part-2 test that pins it), and **Status** (enforced | open gap (owner)
| partial).

### 3.1 Structural integrity claims (content addressing)

**C4-01. Event identity is a verifiable content hash.**
Invariant: an event's id equals `SHA-256(bincode(entity_id) || bincode(operations)
|| bincode(parent) || bincode(generation))`, recomputed from contents on every
use, never read from the wire; a peer cannot present an event under an id its
contents do not hash to.
Trust tier: Byzantine-safe.
Enforcing seam: `proto/src/data.rs` `EventId::from_parts` (the hash) and `Event::id()`
(recomputes on every call; no id field is stored or deserialized). `accumulator.rs`
`accumulate` keys the DAG by `event.id()` and `retrieval.rs` `stage_event` keys
staging by `event.id()`, so a lying declared id cannot enter the graph or be
discovered.
Falsifying attack (T1): send an event whose declared id mismatches its contents,
hoping a consumer trusts the declared id.
Planned test arm: forged-id arm asserting the recomputed id is what the DAG and
staging key on, and a mismatched declared id is inert.
Status: enforced. Content addressing gives inherent identity and tamper-evidence,
not authorship (Finding 6; see C4-20).

**C4-02. Model attribution is excluded from event identity.**
Invariant: two events differing only in `model` hash to the same `EventId`;
identity is `(entity_id, operations, parent, generation)` only.
Trust tier: Byzantine-safe as a hashing fact; model *attribution* is
trusted-peer.
Enforcing seam: `proto/src/data.rs` `EventId::from_parts` hashes entity_id,
operations, parent, and generation, deliberately omitting model;
`From<(EntityId, EntityId, EventFragment)>` supplies the model from the receiving
envelope. Falsifying attack (T1): deliver an event for entity E under model X
while identical content belongs to Y; both produce one id, so model resolution
decides which collection's storage receives it.
Planned test arm: cross-model id-collision arm asserting no cross-contamination
and that policy is checked against the model-resolved collection.
Status: enforced as a hashing fact; model attribution is gap G-2.

**C4-03. Clock deserialization normalizes to sorted, deduplicated order.**
Invariant: any `Clock` reconstructed from the wire is sorted and deduplicated before
use; wire order is never trusted for the binary-search membership tests
(`contains`/`insert`/`remove`) the head-antichain maintenance depends on.
Trust tier: Byzantine-safe.
Enforcing seam: `proto/src/clock.rs` `Clock` derives `#[serde(from = "Vec<EventId>")]`,
routing deserialization through `From<Vec<EventId>>` -> `normalized()`
(`ids.sort(); ids.dedup();`); `Clock::new` uses the same path.
Falsifying attack (T0/T1): an unsorted or duplicate-bearing parent clock so a
binary-search membership test silently returns the wrong answer, leaving a redundant
non-antichain tip after a merge.
Planned test arm: malformed-clock arm (unsorted, duplicated, reversed) asserting
normalization and correct head maintenance.
Status: enforced. **This contradicts the July verification review**, which listed
V5 ("clock sortedness is an unenforced load-bearing invariant") as an open MEDIUM;
the `#[serde(from ...)]` normalization now closes V5 at the type boundary for
deserialization. Residual: an in-process caller wrapping an unsorted `Vec` via a
non-normalizing path bypasses this, so #274 is asked to make clock validation an
explicit ingress check, not solely a serde attribute (V5 belongs at that seam).

**C4-04. Event batches are acyclic or rejected.**
Invariant: a multi-event batch is applied parents-first (topological order); a batch
containing a parent cycle is rejected, not applied in a corrupting order.
Trust tier: Byzantine-safe.
Enforcing seam: `event_dag/ordering.rs` `topo_sort_events` (Kahn's algorithm over
in-batch parent edges; dedups events by id and parents by set; returns
`MutationError::InvalidUpdate("event batch contains a parent cycle")` when the queue
drains with events remaining). Called by all three multi-event arms in
`node_applier.rs`.
Falsifying attack (T1): a batch whose events reference each other's ids to force a
cycle, or wire order applying a child before its staged parent (V4 gap-jump),
dropping the parent's operations.
Planned test arm: cycle-attempt arm asserting rejection; reordered-batch arm asserting
parents-first application regardless of wire order.
Status: enforced. Closes the V4 gap-jump class: the "already in causal order"
assumption was false; `topo_sort_events` is the fix, and receivers explicitly do
not trust sender ordering (node_applier.rs arm comments).

**C4-05. Identical replays de-duplicate.**
Invariant: re-delivering a byte-identical event is a no-op; no double apply, no DAG
inflation, no double head advance.
Trust tier: Byzantine-safe.
Enforcing seam: structural, staging and the DAG keyed by `event.id()`
(`core/src/retrieval.rs` `stage_event`; `accumulator.rs` `accumulate`); semantic,
`core/src/entity.rs` `apply_event` returns `Ok(false)` for `Equal`/`StrictAscends`
so an already-integrated event is discovered by BFS and skipped (the idempotency
comment documents why an `event_stored` check would false-positive: callers stage
before applying).
Falsifying attack (T1): replay-flood one event to force repeated work or a double
application.
Planned test arm: replay-flood arm asserting one application, no head drift, bounded
work.
Status: enforced for correctness. Caveat: de-dup stops *identical* replays only, not
*distinct* equivocations (gap G-4).

### 3.2 Causal-comparison claims (the BFS state machine)

**C4-06. A conflicting second genesis is rejected on durable nodes.**
Invariant: an entity is anchored at a single creation event; a creation event for an
entity with a non-empty head is either a re-delivery (no-op) or a distinct genesis
(rejected `Disjoint`), distinguished cheaply on durable nodes.
Trust tier: Byzantine-safe on durable nodes; trusted-peer-plus-BFS on ephemeral.
Enforcing seam: `entity.rs` `apply_event` creation guard: if `event.is_entity_create()
&& !self.head().is_empty()`, `event_stored()` true means re-delivery (`Ok(false)`),
and on definitive storage a not-stored creation event returns `LineageError::Disjoint`.
Ephemeral nodes fall through to BFS, which returns `Disjoint` on different roots
(`comparison.rs` `check_result`, `subject_root != other_root`).
Falsifying attack (T1): a fabricated alternate creation event for an existing entity
id, to fork identity or reset state.
Planned test arm: disjoint-genesis arm on both node classes; assert reject, and that
an ephemeral node reaches it via BFS grounding, not budget exhaustion.
Status: enforced. Depends on C4-08 (Disjoint must be grounded, not reached via
`BudgetExceeded`).

**C4-07. StrictDescends adoption requires a grounded exploration boundary.**
Invariant: wholesale adoption (StrictDescends: replace head, replay chain) fires
only when the subject introduces no lineage foreign to the comparison. A subject
smuggling a foreign line, as an extra genesis head or a multi-parent graft, must
fall through to the layered merge.
Trust tier: Byzantine-safe.
Enforcing seam: `comparison.rs` `check_result`, the `subject.covers(&comparison)`
block, additionally requires `roots_grounded` (every discovered genesis root in the
comparison ancestry) and `frontier_grounded` (every remaining subject-frontier id in
that ancestry) before returning `StrictDescends`. The pre-BFS quick check enforces
the same per event: `parents.is_empty() || !parents.iter().all(|p|
comparison_set.contains(p))` disqualifies the shortcut.
Falsifying attack (T1): send `{B, X}` over head `{A}` with `B.parent=[A]` and `X` an
independent genesis (V3), or a graft event joining a legitimate ancestor with an
independent root, hoping for adoption without merge.
Planned test arm: forged-parents arm (extra-genesis-head and multi-parent-graft
shapes) asserting `DivergedSince`, never `StrictDescends`.
Status: enforced. Closes V3 (the quick-check disjoint-root shortcut) and its BFS
analogue.

**C4-08. Disjoint and StrictDescends are never concluded from a budget-limited walk.**
Invariant: the accept/reject-shaping verdicts (Disjoint, wholesale StrictDescends)
are a function of grounded ancestry, not of a walk that ran out of budget; two nodes
of different cache depth must not disagree on them.
Trust tier: Byzantine-safe (convergence property).
Enforcing seam: `comparison.rs` `check_result`. StrictDescends is gated on
`roots_grounded && frontier_grounded` over the accumulated DAG
(`compute_ancestry_from_dag`), which holds only when the structure was actually
discovered. Disjoint returns only in the frontiers-exhausted branch (both frontiers
empty) with both roots found; the `BudgetExceeded` branch (`remaining_budget == 0`)
is a separate, later arm returning frontiers for resumption, not a verdict.
Falsifying attack (T1): deep/bogus parentage so a budget-limited walk fails to *find*
the shared root; if a naive impl then concluded Disjoint, a deep-cache node and a
cold-cache node would diverge permanently.
Planned test arm: budget-boundary arm placing the meet or a genesis root just past
the escalated budget; assert `BudgetExceeded` (resumable), never spurious
`Disjoint`/`StrictDescends`.
Status: enforced structurally, matching lit review Findings 2 and 10 (a verdict
reachable via `BudgetExceeded` is not convergent). Residual: budget is finite
(C4-09), so a legitimately deep divergence still ends in `BudgetExceeded` rather than
a decision, the honest limit #266/#271 remove.

**C4-09. Traversal is bounded; runaway walks terminate as an anomaly.**
Invariant: comparison cannot walk forever on malicious parentage; it is capped,
escalates once, then returns a resumable anomaly instead of hanging.
Trust tier: Byzantine-safe for termination; the *policy* consequence is open.
Enforcing seam: `event_dag/mod.rs` `DEFAULT_BUDGET = 1000`; `comparison.rs` `compare`
escalates to `max_budget = initial_budget * 4` (4000) then returns `BudgetExceeded`.
Budget is decremented per event, not per path (`step` calls `saturating_sub(1)` once
per fetched event; the per-side `processed` set makes expansion idempotent so a
diamond does not re-spend). `entity.rs` maps `BudgetExceeded` to
`LineageError::BudgetExceeded` carrying both frontiers.
Falsifying attack (T1): forged deep parentage (a long fabricated chain) or wide
parent fan-out to force a deep/wide walk (Finding 8; Kleppmann "updates that seem to
have occurred far in the past").
Planned test arm: forged-deep-parent and wide-parent arms asserting termination in
`BudgetExceeded` (no hang, no false verdict) and that diamond revisits do not inflate
per-event budget (the V1 side effect).
Status: enforced for termination and correctness; the V1 fix (per-side `Side` with
`processed`/`visited`/`opposite_heads_seen`) removed the double-decrement that gave
false `StrictDescends` and per-path budget burn. Open policy question:
`BudgetExceeded` is error-shaped, so an honest months-stale branch also hits it; #271
(horizon) and #266 (generation distance) own what a deep divergence *should* do.
Gap G-5.

**C4-10. A divergence yields a grounded meet or a conservative empty meet, never a
corrupting partial meet.**
Invariant: on divergence, the merge machinery gets either the true GCA antichain or,
when a comparison head shares nothing accountable with the subject, a conservative
empty meet forcing full re-layering; never a partial meet pretending to cover the
whole clock.
Trust tier: Byzantine-safe.
Enforcing seam: `comparison.rs` `check_result`, frontiers-exhausted branch:
`all_heads_accounted` requires every comparison head to have a meet candidate in its
ancestry (`compute_ancestry_from_dag`), else `DivergedSince { meet: vec![], .. }`;
the meet is the antichain of common nodes with `common_child_count == 0`.
Falsifying attack (T0/T1): late origin propagation via a longer path (V2), or a
comparison head deliberately sharing nothing, to force a partial meet whose head-tip
removal is a no-op and leaves a non-antichain head.
Planned test arm: late-origin and unaccounted-head arms asserting the correct meet or
a conservative empty meet, and a valid antichain head.
Status: enforced. Closes V2 by using the completed accumulated DAG at exhaustion
rather than a first-discovery snapshot.

### 3.3 Application and containment claims (the applier)

**C4-11. One bad item does not poison a batch.**
Invariant: in a multi-item update, a failing item is contained (recorded as a
per-item error), the remaining items still apply, and the reactor is notified for
the applied subset. Partial progress is real progress.
Trust tier: trusted-peer for the *set* (a malicious peer chooses the batch),
Byzantine-safe for the containment mechanism.
Enforcing seam: `node_applier.rs` `apply_updates` (per-item `async` block, errors
into `Vec<ApplyErrorItem>`, `notify_change` before returning the aggregate) and
`apply_deltas` (`ReadyChunks` draining, batches notified as they complete).
Falsifying attack (T1): one malformed item (unknown entity, bad lineage) among valid
ones, hoping to abort the good items with it (the V6 poison-batch regression).
Planned test arm: poison-item arm asserting the valid items apply and notify, error
reported per item.
Status: enforced. Closes V6: the per-item error collection replaces the old `?` that
aborted remaining items.

**C4-12. A failed apply does not leave a phantom empty entity resident.**
Invariant: if `get_retrieve_or_create` speculatively materializes an empty-head
entity for an update that then fails, the phantom is evicted so the entity does not
appear to exist with no state.
Trust tier: Byzantine-safe (local invariant).
Enforcing seam: `node_applier.rs` `apply_update` (EventOnly arm) calls
`remove_if_phantom(&entity_id)` on failure; `entity.rs`
`WeakEntitySet::remove_if_phantom` removes only an empty-head resident (a real entity
is never evicted). The guard `!event.is_entity_create() && self.head().is_empty()`
in `apply_event` (returns `MutationError::InvalidEvent`) makes such an apply fail.
Falsifying attack (T1): a non-creation `EventOnly` for an entity never materialized,
hoping to leak a phantom empty entity into the `WeakEntitySet` and queries.
Planned test arm: phantom-eviction arm asserting no resident survives and no phantom
appears in queries.
Status: enforced. Peer-state recovery is a documented follow-up (applier comment),
not yet implemented; reject-and-evict is safe.

**C4-13. Head mutation is atomic under a TOCTOU retry.**
Invariant: comparison and the head/backends mutation stay consistent under
concurrent writers; if the head moves between them, the operation retries against
fresh lineage rather than applying against a stale head.
Trust tier: Byzantine-safe (local concurrency), independent of peer honesty.
Enforcing seam: `entity.rs` `try_mutate` (write lock, compares
`state.head == expected_head`, updates and returns `Ok(false)` to retry on mismatch)
in the StrictDescends/StrictAscends arms; the DivergedSince arm re-checks
`state.head != head` under the lock before applying layers. `MAX_RETRIES = 5`, then
`MutationError::TOCTOUAttemptsExhausted`.
Falsifying attack: many concurrent writers interleaving so each comparison finishes
just before another's mutation commits (A4 item 3), to exhaust the retry budget.
Planned test arm: concurrent-writer arm asserting no torn head and either success or
a clean `TOCTOUAttemptsExhausted`; primarily a C1 simulation scenario.
Status: enforced. The 5-retry cap can surface `TOCTOUAttemptsExhausted` under extreme
contention, a clean error, not corruption.

### 3.4 Ingress and DoS claims (the honest gaps)

**C4-14. Every application arm validates events before staging.**
Invariant: on the `EventOnly`, `StateAndEvent`, and `EventBridge` arms, each event
fragment passes `validate_received_event` before it is staged or applied; transport
does not decide trust.
Trust tier: attestation-dependent (nil under PermissiveAgent).
Enforcing seam: `node_applier.rs` `validate_and_stage` calls
`node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)`
before `stage_event`, in all three arms; `from_peer_id` is threaded from `node.rs`
`handle_update` through `apply_updates`.
Falsifying attack (T2): craft an event a real agent rejects and send it on one of
these arms; defeated only if the agent's validation is correct.
Planned test arm: policy-rejection arm on each arm with a rejecting agent (mirrors
jwt-auth `adversarial_tests.rs`); assert no apply, no commit.
Status: enforced as a call site; effectiveness is the agent's responsibility. The
*asymmetry* with C4-15 (BFS fetch) is the gap.

**C4-15. BFS-time remote event fetch is unvalidated (OPEN GAP).**
Invariant (desired): an event fetched from a peer during BFS passes the same
`validate_received_event` gate as the application arms before it is written to
storage.
Trust tier: currently trusted-peer (should be attestation-dependent).
Enforcing seam: **partial structural validation only.** `retrieval.rs`
`CachedEventGetter::get_event` verifies that every returned payload recomputes to
the requested id and belongs to the expected entity and model before caching any
response member. It still performs no `validate_received_event` or attestation
check (contrast `validate_and_stage`).
Falsifying attack (T1/T3): during an ephemeral node's BFS fetch of a missing parent,
the durable peer returns the content-address-valid requested event even though a
real PolicyAgent would reject it; the body reaches storage without that policy gate.
Planned test arm: BFS-injection arm (peer returns a requested event rejected by the
receiving policy) asserting rejection once #274 lands; today it documents the gap.
Status: **open gap, issue #244, closed by #274.** #244 is the existence proof #274
cites for the "no unvalidated event touches state" class. Its original code names
(`EphemeralNodeRetriever::retrieve_event`, `NodeApplier::save_events`) predate the
refactor; the gap now lives at `CachedEventGetter::get_event`. Nil impact under
PermissiveAgent, critical under a real agent. Gap G-1.

**C4-16. Durable-peer selection is unauthenticated (OPEN GAP).**
Invariant (desired): the peer an ephemeral node fetches authoritative events from is
authenticated as genuinely durable, not merely self-asserting.
Trust tier: currently trusted-peer.
Enforcing seam: **none today.** `node.rs` `register_peer` inserts into
`durable_peers` from `presence.durable` (self-asserted, no signature);
`get_durable_peer_random` picks from it; `CachedEventGetter::get_event` fetches
there.
Falsifying attack (T1): advertise `durable: true` in a `Presence` to become the fetch
source, then chain with C4-15 to inject events.
Planned test arm: rogue-durable arm (a peer self-advertises durable and answers
fetches) asserting authentication rejects it once presence/identity gains
attestation.
Status: open gap, a PolicyAgent/presence-authentication concern adjacent to #274 and
phase 3 (durable-tier story). Gap G-8. Note: the *serving* side is better off, since
the `GetEvents` handler does run `can_access_collection` + `check_read_event` (C4-18).

**C4-17. No size or count limit on incoming event messages (OPEN GAP).**
Invariant (desired): a single peer message cannot carry unbounded events or payload;
oversized messages are rejected.
Trust tier: currently trusted-peer (no bound at all).
Enforcing seam: **none today.** `node_applier.rs` `apply_update`/`apply_delta_inner`
accept arbitrarily large `Vec<EventFragment>`; `proto/src/request.rs`
`DeltaContent`/`UpdateContent` carry unbounded event vectors. `collect_event_bridge`
(`node.rs`) walks the DAG backward with no depth or count bound (the comparison
sub-step is capped at 100000, the walk is not), so a peer with a very old or empty
head forces the durable node to serialize an entire entity history.
Falsifying attack (T1): a huge `EventBridge`/`EventOnly` list to exhaust receiver
memory (#246), or a bridge request from an ancient head to force an unbounded
backward walk (#247).
Planned test arm: oversized-batch arm (#246) and deep-bridge-request arm (#247)
asserting rejection or a `StateSnapshot` fallback once limits land.
Status: **open gap, issues #246 (receive limits) and #247 (serve-side bridge bound),
closed by #274 (ingress size limits).** Rated low in the issues; a genuine DoS
surface. Gap G-3.

**C4-18. Served events are read-filtered by policy.**
Invariant: when a node serves events (the `GetEvents` handler and the `EventBridge`
builder), each event passes the read policy before leaving the node; a peer cannot
read events it is not permitted to.
Trust tier: attestation-dependent (nil under PermissiveAgent).
Enforcing seam: `node.rs` `GetEvents` arm calls
`can_access_collection(cdata, &collection)` then `check_read_event(cdata, &event)`
per event, pushing only allowed events; `collect_event_bridge` runs
`check_read_event` over the collected events (post-walk).
Falsifying attack (T2): request events for a collection/entity the requester cannot
read, defeated only if the agent's read checks are correct.
Planned test arm: read-authorization arm with a restricting agent asserting denied
events are filtered from both `GetEvents` and `EventBridge` responses.
Status: enforced as call sites. Note the DoS interaction: `collect_event_bridge`
filters *after* the unbounded walk (C4-17), so a read-denied bridge still costs the
full traversal.

### 3.5 Commit policy claims

**C4-19. Local and remote commits preview policy on a fork.**
Invariant: before an event is committed, the PolicyAgent sees a clean before/after
pair produced on a throwaway fork, so a rejected event never touches the canonical
entity or storage.
Trust tier: attestation-dependent.
Enforcing seam: `core/src/context.rs` `commit_local_trx` forks (`entity.snapshot`),
applies the event to the fork, and calls `check_event(&entity_before, &forked, ..)`
before real application. `core/src/node.rs` `commit_remote_transaction` likewise
applies each event, including creation, to a preview and completes all policy checks
before the event set becomes durable or mutates the canonical resident.
Falsifying attack (T2): arrange a denial after an earlier event passed policy; a
broken implementation would expose a committed prefix or a poisoned resident.
Test arms in `tests/tests/remote_commit_atomicity.rs`:
`test_remote_commit_denial_leaves_nothing_durable` and
`test_rejected_creation_previews_and_leaves_no_resident`.
Status: **enforced**. The guarantee remains attestation-dependent: under
PermissiveAgent the policy permits every event. Issues #243 and #242 record the
original creation-preview design, and #274 defines the broader ingress policy seam.

### 3.6 Convergence-boundary claims (what no ingress can enforce)

**C4-20. Authorship and authorization are not structural.**
Invariant: the DAG proves *what* an event contains and *that* it happened-before its
named ancestors; it does not prove *who* wrote it or *whether they were allowed*.
Any authorship or authorization guarantee is exactly the PolicyAgent's signature
scheme, layered above content addressing.
Trust tier: attestation-dependent by construction.
Enforcing seam: `proto/src/auth.rs` `Attestation(pub Vec<u8>)` is opaque bytes with
no verification in the proto crate; all meaning is delegated to
`PolicyAgent::validate_received_event`/`validate_received_state`/`check_event`. The
event id (C4-01) binds contents, not identity.
Falsifying attack (T2): forge authorship, defeated only by a signature scheme the
agent enforces; PermissiveAgent enforces none.
Planned test arm: authorship-spoof arm with a signing agent asserting a wrong-author
event is rejected, and that PermissiveAgent accepts it.
Status: enforced-by-design boundary (Finding 6, 274-B); a documented property, not a
defect, telling the reader where the signature layer must live.

**C4-21. Validated ingress can enforce only I-confluent, before(u)-local invariants.**
Invariant: a validated-ingress check (#274) is load-bearing for convergence only if it
is a deterministic function of the event and its causal-predecessor closure
(before(u)), and only for I-confluent invariants. Cross-entity uniqueness, referential
integrity, and non-negative-quantity invariants are NOT I-confluent and cannot be
convergently enforced by any ingress rule without consensus.
Trust tier: Byzantine-safe *only within* the I-confluent boundary; outside it no tier
applies because no convergent enforcement exists.
Enforcing seam: architectural. The resolution invariant is respected by the comparison
engine, which decides on graph facts and content only (`comparison.rs`/`accumulator.rs`
read only ids, parent edges, operations; no wall clock, arrival order, or trusted wire
field). #274 must state the boundary so `ValidatedEvent` does not imply "globally
valid."
Falsifying attack (T2): rely on a cross-entity uniqueness check at ingress, then
present two concurrent events each locally satisfying it but jointly violating it
(Finding 3; Kleppmann-Howard Theorem 3.1: uniqueness is not I-confluent).
Planned test arm: a design assertion for #274, not a wire arm; the C3 conformance kit
audits that resolution reads only before(u)-derived facts.
Status: boundary documented (Finding 3, 274-A) so #274 does not overpromise. Gap G-7
(documentation boundary, owned by #274).

**C4-22. No wire-supplied ordering signal is trusted.**
Invariant: causal order, merge tiebreaks, and every comparison VERDICT derive from
DAG structure and content hashes only. Since D2 (#266), every `Event` carries a
mandatory `generation` field, but it is sealed inside the content hash (authentic,
not thereby correct), verified against the equation `gen == 1 + max(parent gens)`
at admission wherever parents are resolvable, and consumed exclusively as a
schedule key and a suppress-only fast-path gate; no generation value can select,
route, or conclude a verdict.
Trust tier: Byzantine-safe.
Enforcing seam: `proto/src/data.rs` `EventId::from_parts` (generation inside the
id hash, so storage cannot alter it without changing the id);
`core/src/ingest/verify.rs` `check_generation` (typed `GenerationMismatch`
rejection at admission; genesis must claim exactly 1);
`core/src/event_dag/prechecks.rs` + `comparison.rs` (P1/P2 wired suppress-only;
scheduling is a pure order choice) and `accumulator.rs` (walk-time edge checks
warn and demote, never reject); the stats surface is write-only during a walk so
counters cannot become a value-to-control-flow channel. A node-level kill-switch
(`Node::set_generation_accelerations_disabled`) can bypass every consumer at
runtime while stamping and admission verification stay on.
Falsifying attack (T1): the Matrix depth CVE analogue (Finding 11): inject events
claiming extreme generations to skew ordering. As built, the value can change
only WHEN work happens (schedule) or WHETHER a shortcut is attempted
(suppression); verdicts, meets, and layer partitions are byte-identical under
arbitrary corruption of either channel (payload or carried annotation).
Test arm: the gen-corruption immunity matrix
(`core/src/event_dag/tests.rs::gen_corruption_immunity`: randomized, large, and
wide tiers plus named seeds, two corruption channels and cross-mismatch, nightly
scaled), the R-D2-2b admission rejection pins
(`tests/tests/generation_admission.rs`), and the kill-switch equivalence pins.
Status: enforced. The 266-A obligation ("local, derived, never trusted from the
wire") is discharged in its load-bearing sense: the value rides the wire inside
the hash, admission re-derives and checks it wherever parents are held, and no
trust is ever placed in it for an outcome.

### 3.7 Claims registry summary

By primary trust tier (some claims are split; the canonical wording is in the claim
body):

- **Byzantine-safe (16):** C4-01, C4-03, C4-04, C4-05, C4-06 (durable path), C4-07,
  C4-08, C4-09, C4-10, C4-12, C4-13, C4-21 (within the I-confluent boundary), C4-22,
  C4-23, C4-24, C4-26 (the last three from the D2 addendum below).
- **Attestation-dependent (4):** C4-14, C4-18, C4-19, C4-20.
- **Trusted-peer / open gap (6):** C4-02 (attribution), C4-11 (batch set), C4-15,
  C4-16, C4-17, C4-25 (the ephemeral annotation envelope; blast radius bounded).

Open-gap claims C4-15, C4-16, and C4-17 each name their owning issue/RFC and
their status honestly (open gap, not aspirational enforcement).

### 3.8 D2 addendum: generation claims (added at the #266 close-out)

Added with the D2 close-out (PR #328) and verified against that branch's code at
milestone 6; C4-22 above was rewritten in the same pass from "enforced by absence"
to the as-built posture. Background for all four claims: a generation is sealed
inside the event id, so it is AUTHENTIC (storage or wire cannot alter it without
changing the id) without being thereby CORRECT (the hash seals a wrong value just
as faithfully); everything below follows from the usage discipline that a
generation may order work and gate optional shortcuts but never feed a verdict.

**C4-23. Generation inflation is a bounded slowdown, not a lever.**
Invariant: a wrong generation value (inflated, deflated, or saturated), delivered
through either channel (event payload or a state's carried annotation), can cost
at most the pre-D2 walk: a suppressed shortcut attempt, an unkeyed schedule, or a
disabled precheck. It cannot conclude, route, or reject anything, and it cannot
inflate traversal work past the existing budget bound (C4-09; decrements stay at
most two per distinct event under any schedule, pinned).
Trust tier: Byzantine-safe.
Enforcing seam: suppress-only wiring in `core/src/event_dag/comparison.rs` (a
precheck rejection only skips the positive quick-check attempt); eligibility at
consumption (`u32::MAX` saturation sentinel, the admitted-unverified id set,
walk-time demotion) in `prechecks.rs` and `comparison.rs`; the budget-invariant
pin on `CompareStats.budget_decrements`.
Falsifying attack (T1): craft self-consistent forged lineage with extreme stamps
to poison fast paths, starve the schedule, or force precheck disables (saturated
tips). Outcome: honest-walk cost, nothing else; the DoS surface is the walk
itself, already bounded and owned by C4-09/G-5.
Test arm: the immunity matrix's saturation and inflation shapes (gc-inflate-tip,
gc-sat-interior, gc-sat-tips, synthetic saturation) plus the wide tier.
Status: enforced.

**C4-24. Walk-detected stamp violations demote; they never reject committed
history, and they cannot fire under honest operation.**
Invariant: where the walk holds a child and all its parents it re-checks the
admission equation for free; a violation emits a `tracing` warning, increments
`CompareStats.edge_check_violations`, and demotes the child's value to
per-comparison ineligibility. It never changes a verdict and never retroactively
rejects committed history: that would be lifecycle surgery, expressly D3's
jurisdiction, and the escalation policy (keep detect-and-degrade, a strict mode,
or writer-facing network consequences) is recorded as #335 for the peer-to-peer
trust discussion.
Operational takeaway: this warning CANNOT fire under honest and correctly
implemented operation. An honest committer's stamp satisfies the equation by
construction (the stamp IS `1 + max(parent generations)`, saturating, and the
checker uses the same arithmetic), admission rejects a mismatch wherever parents
are locally resolvable, and the test suite asserts zero violations on every
honestly built corpus. Any production occurrence of the warning or a nonzero
`edge_check_violations` counter is therefore a HIGH-SIGNAL INCIDENT: a defective
client build, a hostile writer, or storage serving doctored payloads. Surface it
accordingly; systematic alert-grade surfacing belongs to the D7 observability
workstream when it starts.
Trust tier: Byzantine-safe (detection and degradation only; no authority).
Enforcing seam: `core/src/event_dag/accumulator.rs` `edge_check` (one evaluation
per child per comparison, registrations bounded by the DAG's edge count).
Falsifying attack (T1): deliver mis-stamped descendants of an adopted horizon
(the lane admission cannot check by design); the walk detects them on traversal
instead of trusting them silently.
Test arm: `walk_time_edge_checks` (doctored edge warns, demotes, verdict
unchanged; honest and saturated corpora produce zero violations); the immunity
baselines assert zero violations on every honest corpus.
Status: enforced posture (deliberate; the alternative is #335's question).

**C4-25. A poisoned ephemeral annotation wedges loudly; it never silently
corrupts.**
Invariant/posture, stated precisely: an ephemeral node adopts a state's carried
head-generation annotation inside the SAME trust envelope as the state buffers
themselves (it holds no event bodies to check against, by construction). If a
poisoned annotation is inherited, the node's own subsequent commits stamp from
the lie, and the next durable admission that holds real payloads REJECTS them
with the typed `GenerationMismatch`: the entity WEDGES bidirectionally with loud
typed errors (an equal-head snapshot does not heal the annotation) until a
strictly descending re-adoption or a resync replaces it. Nothing silently
corrupts at any point: verdicts never consult generations (C4-22), so the lie
costs availability of that entity's writes from that node, loudly, never data.
A durable node lying to an ephemeral it serves is OUT of threat model: that
durable is already the ephemeral's state authority (T3 can fabricate the state
buffers themselves); the annotation adds no new power.
Trust tier: trusted-peer (the adoption envelope), with the blast radius bounded
Byzantine-safe (loud wedge, no verdict influence).
Enforcing seam: `core/src/entity.rs` `apply_state` (annotation travels with the
head under one lock); the durable-side rejection seams of C4-26; the trust note
on `GClock` (`proto/src/clock.rs`).
Falsifying attack (T2/T3): serve a poisoned snapshot to an ephemeral, hoping for
silent propagation; the lie self-identifies at the first durable admission.
Test arm: the R-D2-2b rejection family plus the M4 GClock pins (a durable node
rejects a wire annotation contradicting a held payload).
Status: documented posture; enforced at every checkable seam.

**C4-26. A durable node never adopts a wire-carried annotation uninspected.**
Invariant: on a node with definitive storage, every wire state's annotation is
structurally validated against the head it claims to annotate, checked against
locally held tip payloads, and rejected TYPED when a tip's event is not locally
resolvable (`LineageRejection::UnresolvableHeadGenerationTip`); no code path
copies an uninspected wire value into a durable node's materialization.
Trust tier: Byzantine-safe on durable nodes (and the annotation rides INSIDE the
attested state envelope on every lane, so under a real PolicyAgent a forged
annotation additionally requires a forged attestation, C4-20).
Enforcing seam: `core/src/ingest/verify.rs` `verify_state_head_generations`,
gating all wire-state ingress lanes (plus `join_system`'s inline equivalent).
Falsifying attack (T1): send a durable node a state whose annotation inflates or
misattributes tip generations; rejected before adoption.
Test arm: the M4 GClock rejection pins (structural mismatch, held-payload
contradiction, unresolvable tip).
Status: enforced (the unresolvable-tip arm landed in the M4 remediation; no
production lane currently delivers wire states to durable nodes, so it is
belt-and-suspenders for future ingress).

---

## 4. Attestation load-bearing map

Every place an attestation is produced, consumed, or assumed, and what breaks if it
lies. Under PermissiveAgent every row degrades to "no guarantee"; the map is written
for a real PolicyAgent.

| Site | Attestation role | Verified by | If it lies (T2) |
|---|---|---|---|
| `node_applier::validate_and_stage` (`validate_received_event`) | admits an event fragment on EventOnly/StateAndEvent/EventBridge | `PolicyAgent::validate_received_event` | a forbidden event is staged, applied, and committed; the head advances on unauthorized data (C4-14) |
| `node_applier::apply_delta_inner` + `apply_update` (`validate_received_state`) | admits a state snapshot | `PolicyAgent::validate_received_state` | a forged state is adopted as the entity's state on the StrictDescends fast path, bypassing event history (C4-14) |
| `node.rs commit_remote_transaction` (`check_event`) | evaluates each remote event on a preview and yields an attestation stored on the event | `PolicyAgent::check_event` | an unauthorized remote transaction is attested and persisted because the policy check itself approved it incorrectly (C4-19) |
| `context.rs commit_local_trx` (`check_event`) | attests a locally generated event on a fork | `PolicyAgent::check_event` | a local event that policy should reject is attested and committed |
| `node_applier::save_state` and `apply_delta` (`attest_state`) | attaches an attestation to a saved state served to peers | `PolicyAgent::attest_state` (produces) / `validate_received_state` (consumes downstream) | a peer accepts a state the origin never legitimately attested; state authority is only as strong as this signature |
| `node.rs GetEvents` + `collect_event_bridge` (`check_read_event`) | read-filters served events | `PolicyAgent::check_read_event` | events leak to an unauthorized reader (C4-18) |
| `PolicyAgent::validate_causal_assertion` (`CausalAssertion`) | would validate a peer's *claim* about lineage between two heads | **no consumer today** | defined but unused: `validate_causal_assertion` has zero call sites in core; `CausalAssertion`/`CausalAssertionFragment` (`proto/src/request.rs`) carry a `CausalRelation` + `AttestationSet` that nothing in the applier or comparison path consumes. If a future path trusts an asserted relation without re-deriving it, a lying peer substitutes a false `StrictDescends`/`Disjoint` for a real divergence. Gap G-9. |
| `#271` sealed-prefix checkpoint (future) | a durable-tier attestation that a history prefix is canonical, carrying a genesis attestation so identity survives pruning | future `PolicyAgent` seal-validation | a lying seal lets a T3 durable node rewrite or amputate history below the seal, or forge the genesis binding so Disjoint detection accepts a foreign lineage. This is why #271 must justify sealing by *authority scope*, not by "everyone has it" (lit review Finding 4). |

Load-bearing summary: live attestation flow runs through the commit policy hooks,
event/state admission hooks, and read-filter hooks. The `CausalAssertion` and #271
seal attestations are *designed* surfaces with no enforcement yet. The sharpest risk
is C4-15 (unvalidated BFS fetch) with a T3 durable node: no attestation is consulted
at all, so even a correct PolicyAgent is bypassed.

---

## 5. Known gaps

Severity is rated for the intended production posture (a real PolicyAgent); under
PermissiveAgent every security gap is moot, which is itself meta-gap G-0.

| Gap | Description | Severity rationale | Owner |
|---|---|---|---|
| G-0 | PermissiveAgent is the default and enforces nothing | Meta: every attestation-dependent claim is nil until a real agent is deployed. Not a bug (it is the intended default for open development) but the reader must know the security claims are conditional. | Deployment choice; #274 designs the agent surface |
| G-1 | BFS-time remote event fetch skips `validate_received_event` and attestation checks after structural identity and lineage validation (C4-15) | Critical with a real agent: a peer injects a content-address-valid event straight into permanent storage mid-BFS even when policy would reject it. No impact under PermissiveAgent. | #244, closed by #274 |
| G-2 | Model is not bound into the event id; attribution rides the message envelope and catalog resolution (C4-02) | Medium: a peer can steer identical content through a chosen model into a collection's storage; cross-model contamination is possible if ingress or policy trusts the wrong attribution. | #274 (bind or validate model attribution at ingress) |
| G-3 | No size/count limit on incoming event messages, and no bound on the `collect_event_bridge` backward walk (C4-17) | Medium DoS: memory exhaustion on receive (#246) or forced full-history serialization on serve (#247). Rated low in the issues; genuine under an adversarial peer. | #246, #247, closed by #274 (ingress size limits) |
| G-4 | Equivocation / concurrency flooding: content-hash de-dup stops identical replays but not unbounded distinct valid concurrent events (C4-13) | High DoS: staging now has bounded atomic admission with retry/backpressure, so a rejected batch cannot evict retained ancestors or grow the staging area past its cap. The open risk is that admitted distinct events can still inflate the resident antichain and force deep or wide merge work with no finite-harm bound. Unsigned hash-DAG CRDTs have no structural cap (lit review Finding 7, Blocklace). | #246/#274 (rate/quantity cap) or a signature-attribution layer; cross-referenced from #271 horizon |
| G-5 | Deep-divergence rejection is error-shaped, not policy-shaped: a stale-but-honest branch and a forged deep branch both terminate in `BudgetExceeded` (C4-09) | Medium: no deterministic horizon; budget is the only backstop and its outcome is an anomaly, not a decision. The convergence risk is bounded because verdicts are grounded (C4-08), but the DoS/thrash surface is open. | #271 (rejection horizon) + #266 (generation distance) |
| G-6 | Closed: `commit_remote_transaction` previews creation and update events before canonical mutation (C4-19) | A rejection leaves no resident mutation, state buffer, or committed prefix; this is pinned by `remote_commit_atomicity`. The guarantee depends on the PolicyAgent making the correct decision. | #243, with the design recorded by #274/#242 |
| G-7 | `ValidatedEvent` (future) risks implying "globally valid"; only I-confluent, before(u)-local invariants are convergently enforceable (C4-21) | Documentation/design: if #274 lets validated ingress promise cross-entity uniqueness or referential integrity, it promises something no convergent rule can deliver (lit review Finding 3). | #274 (state the I-confluence boundary) |
| G-8 | Durable-peer role is self-asserted, not authenticated (C4-16) | High when chained with G-1: a rogue "durable" peer becomes the fetch source and injects events. | #274 + phase 3 durable-tier story |
| G-9 | The `CausalAssertion` attestation surface is defined but has no consumer; `validate_causal_assertion` is never called (Section 4) | Low today (unused), Medium latent: if a future optimization trusts a peer's asserted lineage relation without re-deriving it, a lying peer substitutes a false relation. | whichever workstream introduces relation-assertion shortcuts; must re-derive, not trust |
| G-10 | hard_reset residue (D2 addendum): a funnel persist STARTING after a reset completes, on a dead-system resident kept alive only by held strong Entity references, writes dead-system bytes into the successor system's storage with a truthfully current marker (the marker never lies; the illegitimacy is the cross-system delivery itself). The M4 reset fence closed every in-flight interleaving; this is the post-reset window only. | Low: hard_reset is a dev-only surface (maintainer ruling), the write is loud in storage terms (a fresh rehydration yields an unmarked resident with an empty applied-set), and no verdict or marker soundness is affected. Recorded so the one-entity-one-system invariant's enforcement debt is visible. | D3/D6 (cross-system delivery enforcement, plan REV 5 section D); residue documented on the PersistMarker doc (`core/src/entity.rs`) |
| G-11 | IndexedDB `fetch_states` silently drops undecodable rows: `execute_plan_query` (`storage/indexeddb-wasm/src/collection.rs`) skips a row whose record construction or state extraction fails (`Err(_) => continue` and `.ok()` filter arms), so a corrupted entity record vanishes from query results while `get_state` on the same row fails typed. Pre-existing pattern, recorded at the D2 close-out rather than redesigned (the M4 review's routing); the event-column and GClock generation decodes themselves now fail loudly, so the hole is the row-level drop, not the field decodes. | Medium for the fail-loudly discipline (a query can silently under-report), Low for integrity (nothing wrong is served; the row is omitted). | indexeddb read-path follow-up; C7 (storage engine conformance suite) is the natural test home |

---

## 6. Claims to tests

The wire-level adversarial suite (part 2) lives in the ankurah-tests crate
(core tests cannot construct a `Node`). New arms are in
`tests/tests/adversarial_wire.rs`; three pre-existing arms in
`tests/tests/{update_batch_containment,bridge_policy,commit_atomicity}.rs`
already pin claims and are cross-referenced rather than duplicated. Every arm
carries a doc comment citing its claim id(s).

Status legend: **enforced-pass** (a green test pins current behavior);
**gap-red-ignored** (an `#[ignore]`d red test pins what SHOULD happen and
un-ignores when the owning issue lands); **existing-suite** (pinned by a green
test outside this file); **design-boundary** (a documented property or a design
assertion, not a wire arm, per the claim body).

| Claim | Test `path::name` | Status |
|---|---|---|
| C4-01 | `adversarial_wire::malformed_clock_identity_is_order_independent_end_to_end` (id recomputed from contents, order-independent) | enforced-pass |
| C4-02 | none (model-attribution is gap G-2, deferred to #274; the hashing fact is exercised transitively by every event build) | design-boundary |
| C4-03 | `adversarial_wire::{malformed_clock_deserialization_normalizes, no_public_non_normalizing_clock_constructor, malformed_clock_identity_is_order_independent_end_to_end}` | enforced-pass |
| C4-04 | `adversarial_wire::{declared_cycle_is_unconstructible_content_addressing, fabricated_cycle_batch_is_contained}` | enforced-pass |
| C4-05 | `adversarial_wire::replay_flood_is_idempotent` | enforced-pass |
| C4-06 | `adversarial_wire::{forged_second_genesis_rejected_on_durable_node, forged_second_genesis_rejected_on_ephemeral_node}` | enforced-pass |
| C4-07 | `adversarial_wire::forged_extra_genesis_head_does_not_trigger_wholesale_adoption` | enforced-pass |
| C4-08 | exercised via the ephemeral second-genesis reject (grounded `Disjoint`, not a budget artifact) in `forged_second_genesis_rejected_on_ephemeral_node`; a dedicated budget-boundary arm is a C1 simulation scenario | design-boundary |
| C4-09 | fetch-failure termination exercised by `adversarial_wire::forged_dangling_parent_is_contained`; deep/wide-parent budget-exhaustion timing is a C1 simulation scenario, not a fast wire arm | enforced-pass (partial) |
| C4-10 | conservative-meet / merge correctness is a DAG-merge concern covered by the C1 simulation and `dag_auditing.rs`; no dedicated adversarial arm here | design-boundary |
| C4-11 | `adversarial_wire::{forged_dangling_parent_is_contained, phantom_entity_is_evicted_on_failed_apply}`; `update_batch_containment::test_event_only_unknown_entity_does_not_poison_batch` | enforced-pass |
| C4-12 | `adversarial_wire::phantom_entity_is_evicted_on_failed_apply` | enforced-pass |
| C4-13 | TOCTOU atomicity is a concurrent-writer scenario owned by the C1 simulation; not a single-node wire arm | design-boundary |
| C4-14 | `bridge_policy::test_event_bridge_events_are_policy_validated_on_receive`; `commit_atomicity` (check_event denial) | existing-suite |
| C4-15 | `adversarial_wire::bfs_fetched_events_are_policy_validated` | gap-red-ignored (G-1, #244/#274) |
| C4-16 | none (durable-peer authentication is gap G-8; no seam to assert against, adjacent to #274/phase 3) | design-boundary |
| C4-17 | `adversarial_wire::oversized_event_batch_is_rejected` | gap-red-ignored (G-3, #246/#247) |
| C4-18 | `bridge_policy::test_event_bridge_respects_read_policy_on_send` | existing-suite |
| C4-19 | `remote_commit_atomicity::{test_remote_commit_denial_leaves_nothing_durable, test_rejected_creation_previews_and_leaves_no_resident}` | existing-suite |
| C4-20 | authorship-is-not-structural is demonstrated by every arm accepting unsigned forged events under PermissiveAgent; the signing-agent rejection arm belongs with the jwt-auth suite | design-boundary |
| C4-21 | I-confluence boundary is a design assertion for #274 and a C3 conformance audit, not a wire arm (per the claim body) | design-boundary |
| C4-22 | `core/src/event_dag/tests.rs::gen_corruption_immunity` (the two-channel corruption matrix: randomized, large, and wide tiers plus named seeds, nightly scaled) and `tests/tests/generation_admission.rs` (typed rejection at admission, all lanes) | enforced-pass |
| C4-23 | immunity matrix saturation/inflation shapes (`gc-inflate-tip`, `gc-sat-interior`, `gc-sat-tips`, synthetic saturation) plus `budget_invariant` (decrements at most two per event) | enforced-pass |
| C4-24 | `event_dag::tests::walk_time_edge_checks` (doctored edge warns/demotes/verdict unchanged; honest and saturated corpora zero violations); every immunity baseline asserts zero violations on honest corpora | enforced-pass |
| C4-25 | `tests/tests/generation_admission.rs` (a poisoned-stamp commit rejects typed at durable admission) with the M4 GClock adoption-validation pins; the wedge shape itself is a documented posture, not a wire arm | enforced-pass (posture documented) |
| C4-26 | the M4 GClock rejection pins (structural mismatch, held-payload contradiction, unresolvable tip: `core/src/ingest/verify.rs` seams) | enforced-pass |
| G-4 | `adversarial_wire::equivocation_flood_antichain_is_bounded` | gap-red-ignored (G-4, #246 / signature layer) |

Coverage summary: of the twenty-six claims, the Byzantine-safe structural and
containment claims that a single-node wire delivery can falsify are pinned by
green arms (C4-01, C4-03, C4-04, C4-05, C4-06, C4-07, C4-09 partial, C4-11,
C4-12, C4-22, C4-23, C4-24, C4-25, C4-26); the three open ingress gaps are
pinned by red-ignored arms (C4-15/G-1, C4-17/G-3, plus G-4); the
attestation-dependent admission, read, and commit-preview claims are already
pinned by the existing policy suite (C4-14, C4-18, C4-19); and the remaining
claims are documented boundaries a wire arm cannot cheaply or meaningfully add
here (C4-02, C4-08, C4-10, C4-13, C4-16, C4-20, C4-21), each with
its owning issue or workstream named above.

No arm contradicted a claim. One behavior warranted a precise assertion choice:
on an ephemeral node a rejected second genesis (C4-06) still causes BFS to pull
the real genesis into local storage as a grounding side effect (the documented
C4-15 fetch behavior), so `forged_second_genesis_rejected_on_ephemeral_node`
asserts the semantic outcome (head unchanged, forged root not adopted, forged
event not committed) rather than a raw stored-event count.

---

## Appendix: source grounding

Claims are grounded in:
`core/src/event_dag/{comparison,ordering,accumulator,relation,mod}.rs`,
`core/src/{entity,node,node_applier,retrieval,policy,context}.rs`,
`proto/src/{data,clock,auth,id,request}.rs`; issues #242, #243, #244, #246, #247;
RFCs #271 and #274; the phase 2 lit review (topic 3 and design-deltas.md); and the
July 2026 verification review (V1 through V7) on archive/201-concurrent-updates-specs.
The D2 addendum (section 3.8, the C4-22 rewrite, G-10, G-11) additionally cites
`core/src/ingest/{verify,unverified}.rs`,
`core/src/event_dag/{prechecks,comparison,accumulator,stats}.rs`,
`core/src/entity.rs` (PersistMarker and GClock maintenance),
`proto/src/{data,clock,wasm}.rs`, `storage/indexeddb-wasm/src/collection.rs`,
and issue #335 (the walk-detected-violation escalation question).

Two places the code contradicts a prior description, noted at their claims: (1) the
verification review lists **V5 (clock sortedness) as unenforced MEDIUM**, but the tree
now normalizes at the deserialization boundary via `#[serde(from = "Vec<EventId>")]`
-> `normalized()` (C4-03); #274 is still asked to make it an explicit ingress check.
(2) Issue **#244 names symbols that predate the refactor**; requested-id and
lineage-scope validation now protect the cache boundary, but the policy gap remains
at `CachedEventGetter::get_event` (C4-15).

The wire-level adversarial suite (part 2 of C4) maps every claim above to a test or
a named boundary in section 6 (Claims to tests); the C4 checklist line in
specs/concurrency/phase-2.md ticks only when both parts land.
