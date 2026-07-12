# Concurrency Threat Model

Phase 2 workstream C item 4, part 1 of 2. Tracking: #269. Scope:
specs/concurrency/phase-2.md. This is the document the wire-level adversarial
suite (part 2) asserts against: every future adversarial test maps to a named
claim (C4-NN) in the registry below. The validated-ingress RFC (#274) calls this
document "the checklist this ingress implements."

Sections: (1) trust context, node classes, and the content-addressing ledger;
(2) adversary tiers and per-wire-arm attack surface; (3) the numbered claims
registry, the core artifact; (4) the attestation load-bearing map; (5) the
known-gaps table. Every claim about current behavior cites the code seam verified
against commit 90f9a67d (PR #201), with identity/attestation claims re-verified by
the RFC-1 implementation in this branch. Where the literature challenges a claim it is
incorporated or rebutted inline. Where enforcement does not exist, the claim
records the status honestly (open gap, owning RFC) rather than aspirationally.

## Trust tiers (used throughout)

- **Byzantine-safe**: holds against arbitrary peers crafting wire payloads freely.
  The guarantee derives from structure (content addressing, deterministic graph
  facts) and needs no honest-peer assumption.
- **Trusted-peer**: holds only if peers are honest but possibly buggy; a malicious
  peer can violate it. These are the invariants a real PolicyAgent or a hardening
  item (#274, #246, #271) must convert to Byzantine-safe.
- **Attestation-dependent**: holds iff a recognized durable signed the exact
  event/state envelope and the receiving PolicyAgent requires sufficient verified
  attestations. Core verification is uniform and agent-independent; sufficiency is
  still nil under the default PermissiveAgent.

The Byzantine guarantee at its true precision (lit review topic 3, sources 4/5/6,
design-delta 274-B): the content-addressed DAG delivers **weak-safety causal order
and strong eventual consistency among correct nodes for arbitrarily many Byzantine
peers, without event authorship attribution, a strict admission policy,
strong-safety causal order, or a DoS bound**. Core now carries node-signed admission
envelopes, but they are not author signatures and the default PermissiveAgent requires
no admission evidence. That is the difference between "the algorithm converges"
(true, proven by the construction) and "the system resists a malicious peer" (false
by default, only as true as the deployed PolicyAgent).

---

## 1. System and trust context

### 1.1 Node classes

Two classes, distinguished by the `durable` flag threaded through retrieval and the
applier. Every node has an ed25519 identity (`NodeId` is the verifying key), and its
challenge-bound signed `Presence` binds the claimed class, system root id,
protocol version, and receiver-issued session nonce. **Durable nodes** hold
authoritative, definitive storage:
`LocalEventGetter::storage_is_definitive() == self.durable` (`core/src/retrieval.rs`),
they serve events/state and are the fallback ephemeral nodes fetch from. **Ephemeral
nodes** have non-definitive storage (`storage_is_definitive()` defaults to `false`)
and fall back to a durable peer: `CachedEventGetter::get_event` checks staging, then
local storage, then `node.get_durable_peer_random()`.

Peer identity is authenticated by the presence signature and bound to every later
frame at connector dispatch. Durable class is accepted only when the NodeId equals
the founder in a verified, content-addressed `SystemRootProof`; the first valid proof
is atomically reserved before durable routing. An unrecognized claimant may remain
connected but is treated as non-durable and is never selected by
`get_durable_peer_random`. First join remains explicitly TOFU: before a root is
pinned, an ephemeral node trusts the first internally valid signed founder/root pair
it accepts (C4-16, residual G-8).

### 1.2 What peers are trusted with today

`PolicyAgent` (`core/src/policy.rs`) is the single authority surface, carrying the
hooks that make peer input trustworthy: `validate_received_event`,
`validate_received_state`, `check_event`, `check_read_event`,
`validate_causal_assertion`, `attest_state`, and the read/write/collection gates.
The **default agent is `PermissiveAgent`**, which requires no admission evidence:
the two `validate_received_*` and `validate_causal_assertion` return `Ok(())`,
`check_event` and `attest_state` return `Admission::Allow`, and every read/write
check returns `Ok(())`. Core nevertheless strips wrong-system, wrong-kind,
subject-mismatched, bad-signature, and unrecognized-attester envelopes before those
hooks run. Under
PermissiveAgent, **attestation sufficiency and trusted-peer claims provide no
guarantee**, while structural checks and envelope authenticity still survive. Real
PolicyAgents exist (jwt-auth ships `adversarial_tests.rs` and
`adversarial_rbac_tests.rs`) and are the intended production posture; this document
is written for that posture, stating which invariants a correct agent can enforce
versus which no agent can (missing seam, or not I-confluent).

### 1.3 What content addressing buys, and what it does not

The per-entity DAG is structurally a Merkle-CRDT (lit review topic 3). Update ids are
domain-tagged SHA-256 hashes over `(entity_id, operations, parent)`; genesis ids are
domain-tagged hashes over `(system, nonce, timestamp, initial_operations)`, and the
full genesis id is the `EntityId`. Events reference predecessors by hash and the
head is the tip antichain. It buys **for free** (Byzantine-safe):

- **Self-verifying ids / tamper evidence**: the id is recomputed from contents on
  every use (`Event::id()`), never read from the wire (C4-01).
- **Cycle-freedom**: a parent hash is unknowable before its content exists, so an
  honest DAG has no cycle and a fabricated one is rejected (`topo_sort_events`, C4-04).
- **De-dup of identical replays**: byte-identical events share one id and collapse,
  keyed by `event.id()` (C4-05).
- **Unforgeable happened-before**: a peer cannot claim B precedes A unless A
  genuinely names B; the *genuine* ancestry relation is unforgeable (Finding 6).

It does **not** buy (each on a named gap or agent responsibility):

- **Authorship**: the hash says nothing about who authored an event. Admission and
  state authorization now have structured, core-verified node signatures, but
  author signatures remain deliberately out of scope (C4-20).
- **Equivocation resistance / DoS bound**: every forged concurrent event is a
  distinct genuine hash, so de-dup stops identical replays but not flooding of
  distinct ones; unsigned hash-DAG CRDTs permit unbounded distinct valid
  equivocations with no finite-harm bound (Finding 7, Blocklace; 274-C). Gap G-4.
- **Ordering authority**: weak-safety causal order only, not strong-safety
  (Finding 9: strong safety needs cryptography); we trust no wire-supplied
  depth/generation signal (Finding 11, the Matrix depth CVE), and the codebase
  carries no such field, which is correct.

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
  can equivocate on served history and sign false admissions with the recognized
  authority key. The strongest in-scope adversary; a single-founder signature cannot
  defend against founder compromise (RFC-2 membership/quorum/revocation territory).

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
| `GetEvents` resp, mid-BFS | `CachedEventGetter::get_event` | yes | n/a | n/a | requested-id filter + core envelope verification before storage |
| `GetEvents` req, served | `node.rs` `GetEvents` arm | n/a | n/a | n/a | serve side DOES `can_access_collection` + `check_read_event` |
| `commit_remote_transaction` | `node.rs` same | via `check_event` | via `attest_state` | n/a | all event shapes previewed on a fork |

("yes" = mechanical envelope/subject/signature/attester verification in core,
followed by the PolicyAgent sufficiency hook.) The former BFS-time asymmetry is
closed at its existing seam; #274 still owns the structural single-ingress refactor,
size limits, and rate limits. Section 3 gives each row its own claim.

---

## 3. Claims registry

The core artifact. Each claim gives its **Invariant**, **Trust tier**
(Byzantine-safe | trusted-peer | attestation-dependent), **Enforcing seam**
(`path::function` verified, or "none today"), **Falsifying attack**, **Planned test
arm** (the C4 part-2 test that pins it), and **Status** (enforced | open gap (owner)
| partial).

### 3.1 Structural integrity claims (content addressing)

**C4-01. Event identity is a verifiable content hash.**
Invariant: an update id equals `SHA-256("ankurah.event.v0" ||
bincode(entity_id, operations, parent))`; a genesis id equals
`SHA-256("ankurah.genesis.v0" || bincode(system, nonce, timestamp,
initial_operations))`. Both are recomputed from contents, never read from the wire;
a peer cannot present an event under an id its contents do not hash to.
Trust tier: Byzantine-safe.
Enforcing seam: `proto/src/data.rs` `EventId::{from_genesis_parts,
from_update_parts}` and `Event::id()` (no id field is stored or deserialized).
`accumulator.rs`
`accumulate` keys the DAG by `event.id()` and `retrieval.rs` `stage_event` keys
staging by `event.id()`, so a lying declared id cannot enter the graph or be
discovered.
Falsifying attack (T1): send an event whose declared id mismatches its contents,
hoping a consumer trusts the declared id.
Planned test arm: forged-id arm asserting the recomputed id is what the DAG and
staging key on, and a mismatched declared id is inert.
Status: enforced. Content addressing gives inherent identity and tamper-evidence,
not authorship (Finding 6; see C4-20).

**C4-02. Model and collection are excluded from event identity by design.**
Invariant: two events differing only in the model envelope have the same `EventId`;
collection is no longer a wire event field at all. Entity identity and event history
are independent of storage organization and of the model under which a node reads
the entity.
Trust tier: Byzantine-safe as a hashing fact. Admission attribution is
attestation-dependent.
Enforcing seam: `proto/src/data.rs` hashes the two typed `EventBody` preimages and
deliberately omits `model`; `EventAdmitted` separately signs `(event_id, model)` so
an admission envelope cannot be transplanted onto the same event relabeled under a
different model. Receivers resolve the model id to a local collection before use.
Falsifying attack (T1/T2): relabel an admitted event under another model while
reusing its attestation; core strips the subject-mismatched envelope before policy.
Planned test arm: attestation-transplant arm asserting model relabeling preserves
the event id but invalidates the admission envelope.
Status: hashing exclusion is BY DESIGN and envelope binding is enforced. The former
G-2 is retired; #274 still owns consolidating model-resolution and structural checks
behind one ingress type.

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

**C4-06. Creation uniqueness is self-certifying on every node class.**
Invariant: an entity is anchored at exactly the genesis whose full 32-byte event id
is its `EntityId`. A different genesis is a different entity; claiming an existing
id with different genesis contents is structurally invalid.
Trust tier: Byzantine-safe on durable and ephemeral nodes.
Enforcing seam: `Event::validate_structure` requires a genesis body, an empty parent
clock, and `EntityId::from(event.id()) == event.entity_id`; every application seam
checks it before mutation or storage. `Node::validate_event_scope` additionally
requires ordinary genesis bodies to name the pinned local system; the sole
`system: None` exception is that pinned root's own genesis. No definitive-storage
negative is involved. A state-only snapshot cannot establish a fresh identity by
assertion: `Node::validate_state_identity` retrieves the event whose EventId bytes equal
the claimed EntityId, validates its structure/system/model, and only then allows state
materialization or direct-get storage.
Falsifying attack (T1): fabricate alternate genesis contents while retaining an
existing entity id, put a genesis body under a non-empty parent, or replay a valid
foreign-system genesis into local storage.
Planned test arm: genesis-id-mismatch, genesis-with-parent, cross-system replay, and
state-without-named-genesis arms, asserting rejection before storage.
Status: enforced structurally. The former durable/ephemeral asymmetry and
multi-durable same-id creation race are dissolved; a SHA-256 collision is the only
remaining route to two genesis preimages for one id.

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
fragment passes structural validation and core envelope verification, then
`validate_received_event`, before it is staged or applied; transport does not decide
trust.
Trust tier: structural/envelope authenticity is Byzantine-safe; admission
sufficiency is attestation-dependent (nil under PermissiveAgent).
Enforcing seam: `node_applier.rs` `validate_and_stage` calls
`Node::validate_incoming_event`, which rejects malformed events, strips wrong-kind,
subject-mismatched, invalid-signature, and unrecognized-attester envelopes, and only
then invokes the PolicyAgent. It runs before `stage_event` in all three arms.
Falsifying attack (T2): craft an event a real agent rejects and send it on one of
these arms; defeated only if the agent's validation is correct.
Planned test arm: policy-rejection arm on each arm with a rejecting agent (mirrors
jwt-auth `adversarial_tests.rs`); assert no apply, no commit.
Status: enforced as a call site; envelope authenticity is core's responsibility and
the minimum verified-attestation requirement remains the agent's responsibility.

**C4-15. BFS-time remote event fetch is validated (PARTIAL #274 closure).**
Invariant: an event fetched from a peer during BFS passes the same
`validate_received_event` gate and response-id filtering as the application arms
before it is written to storage.
Trust tier: structural/envelope authenticity is Byzantine-safe; admission
sufficiency is attestation-dependent.
Enforcing seam: `retrieval.rs` `CachedEventGetter::get_event` recomputes each returned
payload id, selects only the requested id, calls `Node::validate_incoming_event`, and
only then calls `add_event`. Unrequested, malformed, forged, or policy-insufficient
events do not enter permanent storage.
Falsifying attack (T1/T3): return an unrequested event or a requested payload carrying
a forged/transplanted envelope during an ephemeral node's missing-parent walk.
Planned test arm: `bfs_fetched_events_are_policy_validated`, now green, plus the
requested-id filter unit test.
Status: **partial closure complete; G-1 retired.** #274 still owns routing every arm
through one `ValidatedEvent` type plus size/rate/clock limits; this patch deliberately
closes the existing sharp seam without preempting that refactor.

**C4-16. Node identity is authenticated; durable recognition is founder-scoped.**
Invariant: a peer cannot claim another NodeId or enter `durable_peers` merely by
setting `durable: true`. In RFC-1, only the system-root founder is recognized as
durable.
Trust tier: Byzantine-safe for identity and root proof; first choice among valid roots
is TOFU.
Enforcing seam: both peers issue a fresh `HandshakeChallenge`; `Presence` is
signed under its NodeId over all claims including the receiver's challenge,
and `node.rs::register_peer` consumes the single-use verifier before insertion.
`SystemRootProof` carries the
content-addressed root genesis and exact genesis-materialized state; core checks its
system model/body/id, founder operation, and state before atomically reserving the
first root. A conflicting first root never enters durable routing. Production
connectors carry later traffic in `SignedPeerMessage`, binding the complete
message to that receiver-issued session and a sequence number. Core verifies
session, signature, declared sender, and a bounded replay window before dispatch;
duplicates and stale frames are rejected while unique in-window reordering is allowed.
`get_durable_peer_random` draws only from the verified set.
Falsifying attack (T1): replay a captured Presence on a fresh connection, reflect a
node's own Presence, alter a signed presence, substitute state under a known root id,
race two first roots, forge/replay/stale a post-handshake frame, spoof another
connected node's `from`, or advertise durable using a valid but non-founder key.
Planned test arms: captured/forged/reflected Presence, unproven-root-state,
concurrent-first-root, frame signature/session/replay-window, post-handshake
sender-spoof, and unrecognized-durable tests.
Status: identity enforcement and single-founder recognition are complete. Residual G-8
is the named TOFU choice among valid first roots and RFC-2 multi-durable membership,
not a self-asserted role. Authenticated TLS/QUIC remains responsible for secrecy;
an active relay can forward genuine frames or deny service but cannot inject accepted
traffic as the relayed node.

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

### 3.5 Local-commit policy claims

**C4-19. Local and remote commits preview policy on a fork.**
Invariant: before an event is committed, the PolicyAgent sees a clean
before/after pair produced on a throwaway fork, so a rejected event never touches
the canonical entity or storage.
Trust tier: attestation-dependent.
Enforcing seam: `core/src/context.rs` `commit_local_trx` forks (`entity.snapshot`),
applies the event to the fork, and calls `check_event(&entity_before, &forked, ..)`
before real application, for all events. `core/src/node.rs`
`commit_remote_transaction` now uses the same isolated-fork preview for both genesis
and update bodies, then mints any core-owned attestation and persists/applies.
Falsifying attack (T2): send a creation event via the remote-transaction path that a
real agent rejects, leaving the entity in a poisoned in-memory state (rejected but
applied).
Planned test arm: creation-rejection arm on the remote-transaction path asserting the
entity is not mutated or persisted on rejection.
Status: enforced; G-6/#243 is closed at the existing path. Batch-level atomicity is a
separate transaction concern and is not claimed here.

### 3.6 Convergence-boundary claims (what no ingress can enforce)

**C4-20. Authorship and authorization are not structural.**
Invariant: the DAG proves *what* an event contains and *that* it happened-before its
named ancestors; it does not prove *who* wrote it or *whether they were allowed*.
Authorship remains out of scope. Admission authorization is represented by a
structured node-signed envelope layered above content addressing.
Trust tier: attestation-dependent by construction.
Enforcing seam: `proto/src/auth.rs` defines domain-tagged `EventAdmitted` and
`StateAttested` envelopes. Core binds them to the pinned system plus exact event+model
or complete state digest, verifies ed25519 signatures, and recognizes attesters
against the system founder before agent sufficiency checks. Verified founder state
envelopes survive exact-state storage/replay; merges strip them as subject mismatches.
The envelope proves what a recognized node admitted; it does not identify the original
human/device author.
Falsifying attack (T2): forge or transplant an admission (rejected mechanically), or
compromise the recognized durable/policy authority (outside the envelope's promise).
Planned test arms: forged-signature, cross-kind/subject-transplant, and state-tamper
tests; authorship spoof remains a design-boundary test.
Status: structured admission authenticity is enforced; authorship remains the
explicit R1 non-goal (Finding 6, 274-B).

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
Invariant: causal order and merge tiebreaks derive from DAG structure and content
hashes only; no peer-claimed monotone distance (depth or generation) is read from the
wire and trusted.
Trust tier: Byzantine-safe.
Enforcing seam: structural absence. The comparison engine and LWW resolution read only
parent edges and event ids; `Event` (`proto/src/data.rs`) carries no depth/generation
field to trust. When #266 adds generations they must be "local, derived, never trusted
from the wire"; this claim is the standing audit obligation for that work.
Falsifying attack (T1): the Matrix depth CVE analogue (Finding 11): inject an event
claiming extreme depth/generation to skew ordering, impossible today (no such field)
and required to stay impossible after #266.
Planned test arm: forged-generation arm activated when #266 lands, asserting the local
generation index ignores any wire value.
Status: enforced by absence today. Standing obligation for #266/#271 (266-A,
Finding 11); the LWW content-hash tiebreak mirrors Matrix's event-id tiebreak, the
convergent design.

### 3.7 Claims registry summary

By primary trust tier (some claims are split; the canonical wording is in the claim
body):

- **Byzantine-safe structural core:** C4-01, C4-03, C4-04, C4-05, C4-06, C4-07,
  C4-08, C4-09, C4-10, C4-12, C4-13, C4-16 after root pinning, C4-21 within the
  I-confluent boundary, and C4-22. C4-02's hash exclusion and C4-14/C4-15's
  mechanical envelope checks are also Byzantine-safe subclaims.
- **Attestation-dependent sufficiency:** C4-02's admission attribution, C4-14,
  C4-15, C4-18, C4-19, and C4-20.
- **Trusted-peer / open gap:** C4-11 (batch set) and C4-17 (size bounds), plus the
  first-contact TOFU and future membership portion of C4-16.

The former C4-15 and C4-19 gaps are closed at their existing seams. C4-17 remains an
open ingress/DoS gap; C4-16 names its TOFU and multi-durable residual explicitly.

---

## 4. Attestation load-bearing map

Every place an attestation is produced, consumed, or assumed, and what breaks if it
lies. Core always rejects forged or transplanted envelopes; under PermissiveAgent the
remaining *sufficiency* requirement degrades to no guarantee.

| Site | Attestation role | Verified by | If it lies (T2) |
|---|---|---|---|
| `Node::validate_incoming_event` from application arms and BFS retrieval | strips wrong-system, malformed, wrong-kind, wrong-subject, invalid-signature, and unrecognized-attester envelopes, then asks whether verified evidence is sufficient | core ed25519/system/subject checks, then `PolicyAgent::validate_received_event` | a compromised recognized durable can sign an unauthorized event; a strict agent must still judge claims/sufficiency (C4-14/C4-15) |
| `Node::validate_incoming_state` from snapshot, subscription, schema, and direct-get paths | binds the pinned system and complete `EntityState` digest before policy; exact verified envelopes are retained in storage | core ed25519/system/subject checks, then `PolicyAgent::validate_received_state` | a compromised recognized durable can sign a false snapshot; forged, cross-system, or state-substituted snapshots lose their envelope before policy |
| `node.rs commit_remote_transaction` (`check_event`) | policy returns `Admission`; core signs `EventAdmitted(event, model, claims)` after fork preview | `PolicyAgent::check_event` (decision), core node key (signature) | a faulty policy admits an unauthorized remote transaction; envelope cryptography remains authentic but the verdict is wrong |
| `context.rs commit_local_trx` (`check_event`) | same produce split for locally generated events | `PolicyAgent::check_event` (decision), core node key (signature) | a faulty policy admits a local event it should reject |
| state-save/serve paths (`attest_state`) | policy returns `Admission`; core signs entity/model/head plus complete state digest | `PolicyAgent::attest_state` (decision), core node key (signature), downstream core+agent validation | a faulty authority signs a false state; tampering after signature is rejected mechanically |
| `node.rs GetEvents` + `collect_event_bridge` (`check_read_event`) | read-filters served events | `PolicyAgent::check_read_event` | events leak to an unauthorized reader (C4-18) |
| `PolicyAgent::validate_causal_assertion` (`CausalAssertion`) | would validate a peer's *claim* about lineage between two heads | **no consumer today** | defined but unused: `validate_causal_assertion` has zero call sites in core; `CausalAssertion`/`CausalAssertionFragment` (`proto/src/request.rs`) carry a `CausalRelation` + `AttestationSet` that nothing in the applier or comparison path consumes. If a future path trusts an asserted relation without re-deriving it, a lying peer substitutes a false `StrictDescends`/`Disjoint` for a real divergence. Gap G-9. |
| `#271` sealed-prefix checkpoint (future) | a durable-tier attestation that a history prefix is canonical, carrying a genesis attestation so identity survives pruning | future `PolicyAgent` seal-validation | a lying seal lets a T3 durable node rewrite or amputate history below the seal, or forge the genesis binding so Disjoint detection accepts a foreign lineage. This is why #271 must justify sealing by *authority scope*, not by "everyone has it" (lit review Finding 4). |

Load-bearing summary: event and state envelopes now have a uniform live producer and
consumer split: agents decide, core signs/verifies/binds, agents decide sufficiency.
The `CausalAssertion` and #271 seal attestations remain designed surfaces with no
enforcement yet. A compromised recognized durable still defeats admission evidence;
RFC-2 owns quorum/membership/revocation rather than pretending one signature solves
authority compromise.

---

## 5. Known gaps

Severity is rated for the intended production posture (a strict PolicyAgent).
PermissiveAgent still receives mechanically verified envelopes but requires none,
which is itself meta-gap G-0.

| Gap | Description | Severity rationale | Owner |
|---|---|---|---|
| G-0 | PermissiveAgent requires no admission evidence | Meta: core still strips forged/transplanted envelopes, but a payload with zero verified attestations is accepted. This is the intended open-development default; deployments that require durable admission must use a strict agent. | Deployment choice; RFC identity-attestation supplies the seam |
| G-1 | **Closed here:** BFS-time fetch now filters requested ids and validates structure/envelopes/policy before storage (C4-15) | The former critical bypass is gone at `CachedEventGetter`; #274 still owns the one-ingress refactor and resource limits. | identity-attestation RFC; #274 residual structure |
| G-2 | **Retired by design:** model/collection are outside event identity (C4-02) | Entity history is model-independent; `EventAdmitted` separately binds event+model and receivers resolve that model before storage. | identity-attestation RFC; #274 consolidates ingress |
| G-3 | No size/count limit on incoming event messages, and no bound on the `collect_event_bridge` backward walk (C4-17) | Medium DoS: memory exhaustion on receive (#246) or forced full-history serialization on serve (#247). Rated low in the issues; genuine under an adversarial peer. | #246, #247, closed by #274 (ingress size limits) |
| G-4 | Equivocation / concurrency flooding: content-hash de-dup stops identical replays but not unbounded distinct valid concurrent events (C4-13) | High DoS: an adversary inflates the antichain and forces unbounded staging and deep/wide merges with no finite-harm bound. Unsigned hash-DAG CRDTs have no structural cap (lit review Finding 7, Blocklace). | #246/#274 (rate/quantity cap) or a signature-attribution layer; cross-referenced from #271 horizon |
| G-5 | Deep-divergence rejection is error-shaped, not policy-shaped: a stale-but-honest branch and a forged deep branch both terminate in `BudgetExceeded` (C4-09) | Medium: no deterministic horizon; budget is the only backstop and its outcome is an anomaly, not a decision. The convergence risk is bounded because verdicts are grounded (C4-08), but the DoS/thrash surface is open. | #271 (rejection horizon) + #266 (generation distance) |
| G-6 | **Closed here:** remote genesis and updates both preview on an isolated fork before policy/storage (C4-19) | A rejected creation no longer poisons the resident entity. | identity-attestation RFC / #243 |
| G-7 | `ValidatedEvent` (future) risks implying "globally valid"; only I-confluent, before(u)-local invariants are convergently enforceable (C4-21) | Documentation/design: if #274 lets validated ingress promise cross-entity uniqueness or referential integrity, it promises something no convergent rule can deliver (lit review Finding 3). | #274 (state the I-confluence boundary) |
| G-8 | Signed identity, connection-bound dispatch, self-certifying root proof, and founder-scoped durable recognition are enforced; first-root choice is TOFU and additional durable membership is absent (C4-16) | A rogue key cannot fabricate state for a chosen root id or become a fetch source after pinning. At first contact it can still present its own internally valid founder/root pair; core cannot infer which valid system the user intended. | RFC-2 membership/configured expected-root API |
| G-9 | The `CausalAssertion` attestation surface is defined but has no consumer; `validate_causal_assertion` is never called (Section 4) | Low today (unused), Medium latent: if a future optimization trusts a peer's asserted lineage relation without re-deriving it, a lying peer substitutes a false relation. | whichever workstream introduces relation-assertion shortcuts; must re-derive, not trust |

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
| C4-02 | proto attestation subject-matching test (model relabel preserves event id but invalidates envelope) | enforced-pass |
| C4-03 | `adversarial_wire::{malformed_clock_deserialization_normalizes, no_public_non_normalizing_clock_constructor, malformed_clock_identity_is_order_independent_end_to_end}` | enforced-pass |
| C4-04 | `adversarial_wire::{declared_cycle_is_unconstructible_content_addressing, fabricated_cycle_batch_is_contained}` | enforced-pass |
| C4-05 | `adversarial_wire::replay_flood_is_idempotent` | enforced-pass |
| C4-06 | proto `structural_validation_rejects_malformed_events`; identity-attestation genesis-id-mismatch, cross-system, and state-without-genesis arms | enforced-pass |
| C4-07 | `adversarial_wire::forged_extra_genesis_head_does_not_trigger_wholesale_adoption` | enforced-pass |
| C4-08 | grounded comparison/budget-boundary coverage in the event-DAG suite; creation identity no longer needs BFS grounding | existing-suite/design-boundary |
| C4-09 | fetch-failure termination exercised by `adversarial_wire::forged_dangling_parent_is_contained`; deep/wide-parent budget-exhaustion timing is a C1 simulation scenario, not a fast wire arm | enforced-pass (partial) |
| C4-10 | conservative-meet / merge correctness is a DAG-merge concern covered by the C1 simulation and `dag_auditing.rs`; no dedicated adversarial arm here | design-boundary |
| C4-11 | `adversarial_wire::{forged_dangling_parent_is_contained, phantom_entity_is_evicted_on_failed_apply}`; `update_batch_containment::test_event_only_unknown_entity_does_not_poison_batch` | enforced-pass |
| C4-12 | `adversarial_wire::phantom_entity_is_evicted_on_failed_apply` | enforced-pass |
| C4-13 | TOCTOU atomicity is a concurrent-writer scenario owned by the C1 simulation; not a single-node wire arm | design-boundary |
| C4-14 | `bridge_policy::test_event_bridge_events_are_policy_validated_on_receive`; `commit_atomicity` (check_event denial) | existing-suite |
| C4-15 | `adversarial_wire::bfs_fetched_events_are_policy_validated`; retrieval requested-id filter unit test | enforced-pass; #274 refactor residual |
| C4-16 | identity-attestation forged-presence, unproven-root-state, concurrent-first-root, sender-spoof, and unrecognized-durable arms | enforced-pass; valid-first-root choice remains TOFU |
| C4-17 | `adversarial_wire::oversized_event_batch_is_rejected` | gap-red-ignored (G-3, #246/#247) |
| C4-18 | `bridge_policy::test_event_bridge_respects_read_policy_on_send` | existing-suite |
| C4-19 | `identity_attestation::denied_remote_genesis_leaves_no_storage_or_resident_phantom`; local multi-entity denial in `commit_atomicity` | enforced-pass |
| C4-20 | proto forged/transplanted/cross-system/state-tamper tests; identity-attestation state-portability arm; authorship itself remains a design boundary | enforced-pass plus design-boundary |
| C4-21 | I-confluence boundary is a design assertion for #274 and a C3 conformance audit, not a wire arm (per the claim body) | design-boundary |
| C4-22 | enforced by absence: `Event` carries no depth/generation field to forge; the forged-generation arm activates when #266 lands | design-boundary |
| G-4 | `adversarial_wire::equivocation_flood_antichain_is_bounded` | gap-red-ignored (G-4, #246 / signature layer) |

Coverage summary: the content, creation, presence, and envelope-authenticity claims
are pinned by green proto/core/wire arms. C4-15/G-1 and C4-19/G-6 moved from gaps to
green enforcement in this RFC. Resource limits remain red-ignored (C4-17/G-3 and
G-4); TOFU, authorship, I-confluence, and future causal/seal evidence remain honest
design boundaries with their owners named above.

No arm contradicted a claim. Creation identity no longer depends on an
ephemeral-versus-durable storage distinction: malformed same-id genesis attempts are
rejected structurally before ancestry retrieval.

---

## Appendix: source grounding

The original concurrency claims were verified at commit 90f9a67d. Identity,
creation, presence, attestation, and BFS-ingress dispositions were re-verified against
the RFC-1 implementation in this branch:
`core/src/event_dag/{comparison,ordering,accumulator,relation,mod}.rs`,
`core/src/{entity,node,node_applier,retrieval,policy,context}.rs`,
`proto/src/{data,clock,auth,id,request}.rs`; issues #242, #243, #244, #246, #247;
RFCs #271 and #274; the phase 2 lit review (topic 3 and design-deltas.md); and the
July 2026 verification review (V1 through V7) on archive/201-concurrent-updates-specs.

Two places the code contradicts a prior description, noted at their claims: (1) the
verification review lists **V5 (clock sortedness) as unenforced MEDIUM**, but the tree
now normalizes at the deserialization boundary via `#[serde(from = "Vec<EventId>")]`
-> `normalized()` (C4-03); #274 is still asked to make it an explicit ingress check.
(2) Issue **#244 names symbols that predate the refactor**; its live seam was
`CachedEventGetter::get_event`, where this RFC now filters and validates before
storage (C4-15).

The wire-level adversarial suite (part 2 of C4) maps every claim above to a test or
a named boundary in section 6 (Claims to tests); the C4 checklist line in
specs/concurrency/phase-2.md ticks only when both parts land.
