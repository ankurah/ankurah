# D1 Implementation Plan: RFC #268, the unified ingest pipeline

Status: REV 3, for maintainer review. Written 2026-07-06 by the main (Fable)
session; REV 2 the same day after an adversarial red-team pass whose findings
were independently verified against the code before adoption; REV 3 the same
day on a maintainer design ruling.

REV 3 changelog: the maintainer questioned the EventOnly asymmetry (Daniel,
2026-07-06; resident means the WeakEntitySet registered entity, never forks,
which are unregistered Transacted copies, entity.rs:477..492). StatePolicy is
deleted in favor of uniform state persistence after an entity's events on
every arm (recommended, pending confirm; section 4 item 6).

REV 4 changelog: the maintainer's lost-head-updates question exposed a third
face of the gap-jump class, verified in code: apply_event's StrictDescends
arm applies only the incoming event's operations and jumps the head
(entity.rs:299..313), so a resident rehydrated from a stale state buffer that
then receives a linear descendant silently orphans the committed-but-
unincorporated events' operations, and the corrupted state persists under a
head that transitively claims them. The executor gains verdict-driven gap
replay (section 2.3), R10 pins it, and the crash-window residue narrows to
stale READS before the next write (still D5). Verified free in production: no
UpdateContent::EventOnly construction exists outside tests anywhere in the
workspace. New red test R9 pins buffer parity; M3 updates the now-stale
get_resident_entity doc comment; the crash-window residue is explicitly
delegated to D5 (N31 is exactly its repair contract).

REV 2 changelog: adopted red-team findings F1..F7. The load-bearing change: B3
gap replay and NeedsEvents buffering require a node-scoped staging area that
does not exist today (staging currently dies when each applier call returns,
verified at retrieval.rs:95 and :162, fresh map per getter, constructed per
item at node_applier.rs:52). Section 2.8 designs that substrate; the planner
gains a descendant re-drive step; join_system joins the lane inventory; the
migration steps were resequenced; Get-path reentrancy gets an explicit
analysis and test; two new sign-off items (section 4).

Status note: approved by the maintainer 2026-07-06 (all section 4 items);
implementation in flight on PR #318. Two maintainer amendments were ruled
during implementation and are recorded in code comments at their sites, per
convention: (1) the commit lanes' NeedsState recovery is a typed atomic
failure, not an inline Get fetch (2.4; node.rs, plan_and_check_entity_group);
(2) verdict-driven gap replay lives in apply_event's StrictDescends arm,
reusing the DivergedSince layer machinery, not in the executor (2.3;
entity.rs). Where the sections below say otherwise, the code comments govern. Inputs: RFC #268 body and comments, the
book factorization chapter (ankurah.org), a verbatim requirements dossier
compiled from design-deltas.md and threat-model.md (citations spot-checked
against sources), a code inventory of the ingest surface whose sharpest
claims were re-verified against core/src, and an adversarial red-team review
whose load-bearing findings (F1..F7) were independently re-verified before
adoption. Nx and Tx references are the dossier's requirement and tension
numbers; they map to the RFC amendments and threat-model claims recorded on
the issues. file:line references are to main as of a2bed145.

## 0. Scope in one paragraph

Build one ingest pipeline (plan, then execute) that every event-application
path feeds, absorbing the B3 gap-replay defense structurally, unifying
containment, phantom eviction, and notification semantics, and replacing
stringly errors at the application boundary with a typed taxonomy (N1..N6).
Because the RFC's gap-replay residual case and the design-deltas buffering
mandate (N3, N10) have no substrate on current code, D1 also introduces the
node-scoped staging area (section 2.8). No wire changes, no comparison
semantics changes, no durability-invariant reordering (N7). Preserve every
existing behavioral pin (N8).

## 1. Findings that reshape the RFC's stated scope

The RFC enumerates four application loops. The code inventory plus the
red-team pass found SEVEN lanes that adopt entity state from input, three of
them outside the RFC's list:

1. commit_remote_transaction (core/src/node.rs:548) is a second, parallel
   ingest engine. It validates with check_event (write policy) rather than
   validate_received_event (attestation policy), and it is NOT failure-atomic:
   the per-event loop propagates errors with ? mid-loop, so a failure at event
   N leaves events 0..N durably committed with heads advanced, drops the
   suffix, and never calls notify_change (node.rs:607 sits after the loop).
   commit_local_trx by contrast is deliberately two-phase (context.rs:107..141,
   the V7 fix).
2. The Get response path (node.rs:782..792) bypasses everything: raw
   collection.set_state per state, no entity mediation, no event persistence,
   no notification, next to a live TODO acknowledging the gap.
3. join_system (core/src/system.rs:192) adopts a PEER-PROVIDED system root via
   raw storage.set_state: no with_state, no validate_received_state, no
   notification. Structurally the same bypass as the Get lane, on the trust
   bootstrap path. (create_system_root is the local-bootstrap sibling.)

Decision: all three are in scope. The first two change observable behavior and
appear in section 4; join_system gets entity-mediation only (section 2.7 note).

Facts that constrain the design throughout:

4. STAGING DIES PER CALL (red-team F1/F2, verified). Both getters allocate a
   fresh staging map at construction (retrieval.rs:95, :162); apply_updates
   constructs a getter per item (node_applier.rs:52); nothing at Node scope
   holds staging. Therefore "ancestors staged in a previous delivery but never
   applied" (RFC Proposal 2) is today an EMPTY SET, and "a dangling-parent
   event stays staged (buffered)" (268-B, N10) has nothing to stay in. On a
   durable node a child with a missing parent fails compare with EventNotFound
   and is dropped when the getter drops. B3 and buffering need the section 2.8
   substrate; without it the planner's closure walk adds nothing over today's
   in-batch topo sort.
5. EventOnly ingest never rewrites the state buffer (node_applier.rs:117..164;
   the doc comment at node.rs:743..755 describes the resident entity as
   legitimately ahead). The maintainer questioned this asymmetry (2026-07-06);
   the plan's recommendation is at-rest parity: a registered resident entity's
   materialized state should not outrun the persisted state buffer once a
   batch completes; forks (unregistered Transacted copies) are exempt by
   definition, and the resident is never ahead of the durable EVENT LOG at
   rest even today (applied events commit within the same loop iteration).
   D1 therefore persists state uniformly on every arm (section 2.3), which is
   free in production because EventOnly has no non-test producer (verified
   workspace-wide). Residue: a crash between commit_event and save_state can
   still leave the buffer legitimately behind the log (the commit-before-state
   ordering permits exactly this window); repairing that on rehydration is
   D5's contract (N31: a snapshot MUST degrade to a needs-events request when
   it cannot fast-forward). Until D5, the window self-heals on the next write
   through the comparison machinery, which pulls committed events from the
   log; it is a read-visibility lag, not data loss.
6. Errors cross the wire only as Error(String) (proto/src/request.rs:148,
   proto/src/update.rs:86). The typed taxonomy is purely local; a structured
   ack would be a wire change, excluded by N7.
7. The gap-jump class has THREE faces, not two (REV 4, verified at
   entity.rs:299..313). V4: child before staged parent within one batch,
   fixed by topo sort. B3: ancestors staged in a previous delivery, the RFC's
   residual case. NEW: ancestors COMMITTED to the log but not incorporated in
   the resident's materialized state (a stale-buffer rehydration after the
   commit-before-state crash window, or any evicted entity reloaded from a
   lagging snapshot); a linear descendant then verdicts StrictDescends and
   the apply orphans the gap events' operations while the new head
   transitively claims them, making the corruption undetectable afterward.
   The DivergedSince path is immune (layer replay incorporates the gap);
   only the linear case corrupts. Section 2.3's verdict-driven gap replay
   closes all three faces structurally.

## 2. Design

### 2.1 Module and layer position

New module core/src/ingest/ with staging.rs, plan.rs, executor.rs, error.rs,
mod.rs. In the factorization stack it sits between entity and node_applier:

    proto < event_dag < retrieval < entity < INGEST < node_applier, node, context

node_applier and the node.rs commit lanes become thin wire-shape adapters. The
reactor and peer communication stay OUTSIDE the pipeline: the executor returns
outcomes and EntityChange values; feeders decide notification and recovery
requests (N6: the applier owns peer communication).

### 2.2 The planner (resolves T1)

    plan_entity(head: &Clock, batch: &[EventId], staging: &StagingArea,
                getter: &impl GetEvents) -> IngestPlan

The planner is LEAN. It computes:

(a) The staging closure: batch events plus every staged-but-unapplied ancestor
    reachable from them by parent edges within the staging area (the B3
    residual case, N3, now real because staging persists across deliveries).
(b) Descendant re-drive: staged events whose parents are ALL satisfied by this
    plan (committed, applied, or scheduled earlier in it), found via the
    staging area's reverse index (section 2.8) and appended in topological
    position. This is the mechanism by which a buffered child integrates when
    its missing parent finally arrives; an ancestors-only walk can never find
    it (red-team F1 corollary).
(c) Topological order over the union (reusing event_dag/ordering.rs Kahn sort;
    cycle means rejection, N25).
(d) A static classification per event from cheap guards only: already
    committed (Skip, N9/N26), non-creation event for an unknown entity with
    empty head (NeedsState, N6), creation over a non-empty head (existing
    guard semantics preserved).

The planner does NOT precompute causal verdicts against projected heads.
Verdict-per-event stays where it lives today: inside entity.apply_event's
compare-and-retry loop under the TOCTOU discipline. Rationale: precomputing
verdicts either duplicates every comparison or trusts stale verdicts across
the TOCTOU window, and N7 forbids changing comparison semantics. This is a
deliberate, reviewable interpretation of RFC item 1's "intended action given
the projected head": the intended ACTION is the classification in (d); the
causal VERDICT remains execution-time. N28's convergence requirement is
satisfied because order derives from parent edges, dedup from committed-ness,
and verdicts are grounded (C4-08, N27). The red-team attacked this decision
directly and did not produce a divergence scenario; its blocker was the
missing substrate, not the lean planner.

Purity, stated honestly: the planner is WRITE-pure and deterministic modulo
its getter. It is bounded on GetEvents plus a read-only staging view, so it
provably cannot commit or set_state. On ephemeral nodes the getter may fetch
remotely mid-plan (CachedEventGetter), exactly as comparison does today.

### 2.3 The executor (resolves T8)

One executor owns the canonical sequence, pinned here as THE ordering (T8):

    per event:  stage (at intake) -> apply_event (resident head advances,
                TOCTOU inside entity, unchanged) -> commit_event
    per entity: after its events: save_state, uniformly on every arm (REV 3)
    per batch:  build EntityChange values (advance-only) and return them; the
                feeder notifies the reactor once per batch

Crash consequences preserved exactly (factorization invariants 1 and 2): a
crash may orphan committed events (harmless, content-addressed, idempotent)
but never produces persisted state referencing uncommitted events.

Containment modes:

- PerItem: subscription update batches and delta batches. One bad item is
  recorded (typed), the rest apply, the applied subset notifies (N19).
- Atomic: transactions. The policy phase runs for every event over a fork
  BEFORE any commit; any denial means nothing durable (mirrors
  commit_local_trx's two-phase V7 shape). Post-policy execution failures are
  storage-class errors, reported per event, not rolled back (same as local
  commit today).

Phantom eviction (N20) moves into the executor uniformly. Notification: the
executor builds an EntityChange only on real advance (the no-op suppression
from PR #299 becomes a pipeline invariant). The executor carries state
persistence and the suppression logic FROM ITS FIRST VERSION, because the
first arm cut over (EventBridge, M2) already needs both (red-team F4); the
existing save_state helper (node_applier.rs:211..226) is reused.

Verdict-driven gap replay (REV 4): when apply_event returns StrictDescends,
the verdict's chain is the deduplicated set of events the comparison visited
between the incoming event and the grounded head, discovered for free by the
BFS. Before accepting a StrictDescends apply that would jump past events not
already applied in this plan, the executor topologically sorts the chain's
unapplied members (order from parent edges, never traversal order, per the
factorization chapter's warning) and applies them first, sourcing bodies from
staging or the local log through the getter. This closes all three gap-jump
faces (section 1 fact 7) at the one place that sees every apply, without
plan-time verdict precomputation (the lean-planner decision stands: detection
rides the apply's own comparison, costing nothing extra). Chain COMPLETENESS
for grounded StrictDescends verdicts is asserted in the property oracle
during M1; if it proves unreliable, the fallback is an executor parent-edge
walk from the event down to the head via the getter, same result, one extra
walk only on gap-detected paths. Entity-level apply_event semantics are
unchanged; the executor orchestrates, which honors the RFC's ruling that gap
replay is a planning-layer concern, not an apply_event concern.
[AMENDED at M8 (maintainer, 2026-07-06, option "Fix the arm"): the replay
lives in apply_event's StrictDescends arm, reusing the DivergedSince layer
machinery (the linear gap is its degenerate case: meet = current head,
empty divergent branch). This heals every caller including the commit
lanes' fork previews, applies the chain atomically under one lock, and
makes apply_event no longer a single-event primitive. The completeness
assertion landed with M8. The paragraph above is kept for the record;
entity.rs governs.]

State-buffer persistence is UNIFORM (REV 3, maintainer ruling): after an
entity's events in a plan, the executor persists state, on every arm. The
former StatePolicy parameter is deleted. The EventOnly arm changes behavior
(it previously skipped save_state), which is free in production because no
non-test producer of EventOnly exists in the workspace; the sim harness's
resident-first convergence read remains correct (resident equals buffer at
quiescence once this lands). R9 pins the parity.

### 2.4 Outcomes and errors (resolves T5, T7; N5, N6, N10)

Per-event outcome (not an error):

    enum IngestOutcome { Applied, Skipped(SkipReason), NeedsState { entity },
                         NeedsEvents { missing: Vec<EventId> } }

Staging retention rule (new in REV 2, prevents rejected garbage accumulating):
an event is retained in the staging area ONLY on NeedsState or NeedsEvents.
Applied means committed (removed from staging by commit_event, as today).
Skipped, policy-denied, validation-failed, and lineage-rejected events are
REMOVED: rejection is not buffering. Transient failures (Budget, Storage,
Contention) are also removed; the sender's retry re-delivers, exactly as
today, and idempotency makes re-delivery safe (N9/N26).

- NeedsState: non-creation event for an entity with no local state. Scope: the
  lanes that hold policy context data, which are streaming updates (per-item
  cdata, node_applier.rs:52) and remote commit (node.rs:545). The feeder
  issues the EXISTING Get request (no wire change, T2) and routes the response
  through the shared state-apply function. Post-fetch semantics (red-team F7):
  adopt the fetched state via with_state, then RE-PLAN the entity against the
  staging area with an empty batch; the buffered event then applies through
  the normal verdict machinery, and if the fetched head already contains it,
  idempotency skips it. The delta lanes (Fetch/QuerySubscribed responses)
  carry no cdata (node_applier.rs:231..237) and bridges include genesis, so
  NeedsState should not arise there; if it does, the outcome is reported and
  the event buffered, with no recovery attempt from that lane.
- NeedsEvents: a parent is not applied, not staged, not fetchable. Per 268-B
  (N10, N13): the event stays in the staging area, the outcome is typed, no
  hard drop. Integration happens via the planner's descendant re-drive when
  the parent later arrives (2.2b), or via NeedsState-style recovery if state
  supersedes. In the D1 world there is no seal, so buffer is the only correct
  behavior (T5); the outcome enum is the surface D3's rejection horizon later
  extends with the PolicyAgent decision (N11), designed but inert in D1.

Typed error taxonomy, local-only (finding 6):

    enum IngestError {
        PolicyDenied(AccessDenied),
        Lineage(LineageRejection),        // Disjoint, non-creation over empty
                                          //   head, creation over non-empty
                                          //   head, batch cycle
        Budget(BudgetExceeded { .. }),    // resumable liveness anomaly, NOT a
                                          //   lineage rejection (N27, N40)
        Contention(ToctouExhausted),
        Storage(StorageFailure),
        Validation(ValidationFailure),
        Unsupported(&'static str),        // e.g. StateAndRelation arm
    }

carried as MutationError::Ingest(IngestError) so the existing
ApplyError::Items aggregation survives unchanged (N5). The red-team confirmed
this SEPARATES Disjoint from BudgetExceeded where today both live under
MutationError::LineageError, an improvement over the status quo relative to
N27, and that the Lineage grouping does not conflate what N27 keeps apart.

### 2.5 The intake seam (resolves T3; N35, N37, N38)

All arms enter through ONE intake function that validates (per that arm's
existing gate, unchanged in D1) and stages into the staging area:

    intake(events, gate: &dyn ArmGate, staging: &StagingArea)

D1 does NOT introduce the ValidatedEvent newtype; #274 owns it (D6).
Introducing it now with a permissive constructor would counterfeit the
guarantee #274 exists to make. D1 reduces D6's migration to one signature:
intake's input type. D1 also does not unify WHICH validation each arm runs
(check_event versus validate_received_event): admission policy is #274's
jurisdiction, and changing it silently would be a security-semantics change.

### 2.6 Idempotency and the D2 seam (resolves T6; N9, N39)

Pipeline invariant: planning an already-committed event yields Skip, and Skip
is success for ack purposes, so a lost ack plus sender retry cannot
double-apply (268-A). In D1 the committed-check rides existing machinery
(event_stored plus apply_event's Ok(false) for Equal/StrictAscends, C4-05);
D2's applied-set later makes it O(1). Commit remains the single seam where D2
computes and stores generations (N39): nothing commits an event whose parents
are not already committed or co-scheduled earlier in the same plan. Gap
detection stays parent-edge reachability, never a range test (266-B).

### 2.7 Feeder-by-feeder mapping

A shared state-apply function (with_state, then save_state, then advance-only
change) serves every state-bearing feed. It exists from M2 (red-team F4).

State persistence is uniform across every row (REV 3), so the table carries
only the feed shape and containment mode.

| Lane (today) | Pipeline feed (after) | Mode |
|---|---|---|
| EventOnly arm (node_applier.rs:117) | intake -> plan -> execute (now also persists state, REV 3) | PerItem |
| StateAndEvent arm (node_applier.rs:167) | state fast-path via shared state-apply; fallback -> pipeline | PerItem |
| EventBridge arm (node_applier.rs:345) | intake -> plan -> execute | PerItem |
| StateSnapshot arm (node_applier.rs:315) | shared state-apply | PerItem |
| Get response (node.rs:782) | shared state-apply (entity-mediated, notifying) | PerItem |
| join_system (system.rs:192) | entity-mediated via with_state, but NOT the shared state-apply function: the peer-attested root persists verbatim (re-attesting the trust anchor would swap its provenance), so it skips PersistState and the re-drive. Validation semantics UNCHANGED; trust bootstrap documented as a #274 item | PerItem |
| commit_remote_transaction (node.rs:548) | intake -> plan -> policy phase (fork, check_event) -> execute | Atomic |
| commit_local_trx (context.rs:70) | plan (ordering) + policy phase shared with the remote lane; phase two stays lane-owned (commit, fork heads, relay, then materialize+persist), so execute_plan serves PerItem plus remote commit only | Atomic |

### 2.8 The staging area (new in REV 2; the B3 substrate)

    core/src/ingest/staging.rs: StagingArea, held by Node, one per collection
    (or one per node keyed by collection; decided at implementation by lock
    granularity measurement, default per-collection)

Contents and contract:

- A map EventId -> Attested<Event> plus a REVERSE INDEX parent EventId ->
  staged child EventIds, both updated on stage, commit, and remove.
- Getters take the shared StagingArea at construction instead of allocating a
  private map (retrieval.rs constructors change; trait surface GetEvents /
  SuspenseEvents unchanged). get_event remains staging-then-storage;
  event_stored remains storage-only. Factorization invariants 1..3 are
  untouched; what changes is staging LIFETIME (node-scoped instead of
  per-call), which the book describes as mechanism, not contract.
- Retention: only NeedsState/NeedsEvents outcomes retain (2.4). Everything
  else removes on completion of its plan.
- Bounds: a per-node cap (default on the order of 10^4 events, configurable).
  At the cap, oldest-first eviction with a tracing warning and a counter;
  eviction is observable, never silent (spirit of N30), and is the interim
  liveness valve until #274 per-source rate limiting (N16) bounds forged
  breadth properly. An attacker flooding orphaned children is bounded by the
  cap; legitimate orphans evicted early are re-delivered by sender retry.
- Memory-only in D1: process restart loses buffered orphans; sender retry
  re-delivers; idempotency makes that safe. Crash-durable staging would
  entangle D3 lifecycle and C6 and is explicitly out of scope.

### 2.9 Get-path reentrancy (new in REV 2; red-team F6)

reactor.notify_change serializes on a non-reentrant async lock held across
evaluation (reactor.rs:332). The amended Get lane notifies AFTER apply
returns, from the feeder, exactly like every other lane; it never notifies
from inside reactor evaluation. The residual hazard is a cycle notify_change
-> evaluation -> get_entity -> get_from_peer -> notify_change. Analysis: the
reactor's evaluation gap-fill uses fetch_entities (the Fetch lane,
fetch_gap.rs:99), not get_entity, and fill_gaps_for_query runs at query setup,
not under notify_change. So the cycle is not reachable today. Guards anyway:
(a) R4 gains a reentrancy exercise (get_entity racing a notify storm on the
same entity under the sim; assert no deadlock via timeout); (b) a comment
contract on get_from_peer that it must never be called from reactor
evaluation paths.

## 3. Tension resolutions (quick reference)

| T | Resolution |
|---|---|
| T1 | Planner write-pure, deterministic modulo GetEvents; verdicts execution-time (2.2); red-team found no divergence scenario |
| T2 | NeedsState rides the existing Get request on cdata-bearing lanes; no wire change (2.4) |
| T3 | Single intake seam now, ValidatedEvent newtype at D6 (2.5) |
| T4 | Threat model treated as existing input |
| T5 | Buffer in the staging area with typed NeedsEvents; no seal yet, so no reject arm (2.4, 2.8) |
| T6 | Idempotency restated as pipeline invariant; D2 accelerates (2.6) |
| T7 | Budget, Contention, Unsupported get first-class arms beyond the RFC's four buckets (2.4) |
| T8 | Canonical ordering pinned in 2.3 |

## 4. Behavior changes requiring explicit maintainer sign-off

1. commit_remote_transaction becomes policy-atomic (Atomic mode): a mid-batch
   policy denial leaves NOTHING durable, where today it leaves a committed
   prefix with advanced heads and no notification. Also structurally closes
   the C4-19 creation-event asymmetry (G-6, #243): creation events get the
   same fork-preview as updates (the red-team verified entity.snapshot forks
   an empty-head entity cleanly, entity.rs:477..491).
2. The Get response path becomes entity-mediated and notifying; a stale fetch
   can no longer clobber a newer resident state (with_state comparison
   replaces raw set_state). Reentrancy analysis and guards in 2.9.
3. get_durable_peer_random's thread_rng becomes injectable and seedable
   (node.rs:806, sole thread_rng in core, three callers). Production default
   unchanged; the sim can drive gap-fill deterministically (needed by R5..R7).
4. NEW: the staging area itself (2.8): node-scoped in-memory buffering with a
   bounded cap and observable eviction. This is new state with a DoS surface,
   bounded by the cap and superseded properly by #274 rate limiting. Without
   it, B3 gap replay and 268-B buffering are unimplementable (verified).
5. NEW: join_system routes through the shared state-apply (entity-mediated)
   with validation semantics unchanged. Touches the trust-bootstrap path;
   listed for visibility, not because behavior meaningfully changes.
6. APPROVED (maintainer, 2026-07-06, as part of "factor in the lost head
   updates handling"): the EventOnly arm persists the state buffer like every
   other arm, restoring at-rest parity between the resident entity and the
   persisted state buffer. The maintainer questioned
   the asymmetry (2026-07-06) without ruling; the recommendation stands on
   its own merits: free in production (no non-test EventOnly producer
   exists), closes cold-rehydration staleness, and simplifies convergence
   reads. The node.rs:743..755 doc comment is corrected in the same commit
   (M3). Note the resident is never ahead of the durable EVENT LOG at rest
   even today; the lag is only in the state-buffer snapshot.

## 5. Test plan (red first, all under the C1 harness where applicable)

- R1 duplicate-ack retry: redeliver a committed batch; single application, no
  head drift (may land as a pin if already green).
- R2 remote-commit atomicity: policy denies event 2 of 3 via
  CommitTransaction; nothing durable, no head advance, typed error (red).
- R3 creation-rejection (C4-19 arm): rejected creation leaves no mutated
  resident entity (red).
- R4 Get-path visibility and safety: fetched entity notifies an established
  matching query (red); stale fetch cannot regress a newer resident; plus the
  2.9 reentrancy exercise (no deadlock under fetch-during-notify).
- R5 gap-replay residual (B3): deliver child, parent missing (NeedsEvents,
  buffered); deliver parent in a LATER delivery; planner's descendant re-drive
  schedules the buffered child; converged, no orphaned operations (red: today
  the child is simply gone when the getter drops).
- R6 dangling-parent buffering: durable node receives a child whose parent
  never arrives; typed NeedsEvents, event retained in the staging area,
  observable, no drop; cap-eviction path emits the warning and counter (red).
- R7 determinism: sim scenario inducing cross-peer gap-fill passes the
  determinism audit under seeded peer selection (blocked on M0).
- R8 staging bounds: flood distinct orphaned children past the cap; eviction
  is observable, node stays live, no unbounded growth (red: no cap exists).
- R9 state-buffer parity (REV 3): after an EventOnly apply reaches quiescence,
  the persisted state buffer head equals the resident entity head; a cold
  rehydration of the evicted entity yields the same materialized state (red
  today: the buffer lags until a state-bearing update arrives).
- R10 crash-window gap replay (REV 4): commit events, suppress the state
  write (or evict the entity so it rehydrates from the stale buffer), then
  deliver a linear descendant-only update; assert the reconstructed state
  incorporates the committed-but-unincorporated events' operations and the
  head is honest (red today: the StrictDescends jump orphans them, verified
  entity.rs:299..313).

Pins that stay green throughout: V4 wire shapes and V6 containment
(sim_wire_shapes.rs, update_batch_containment.rs), C2 atomicity and the
durable/ephemeral suites, all nine inter_node tests including the no-op
suppression test, sim smoke plus determinism audit, and a 500-seed local swarm
run on touched scenarios before the PR flips ready.

Benchmarks: the E1 harness lives in PR #276 (UNMERGED, red-team caught the
dangling reference). Either #276 merges before D1's perf-validation step or
the comparison runs from the e1 worktree against the D1 branch. The planner
walk is O(batch + closure) HashMap lookups, the same shape as
topo_sort_events, so the hot path is not expected to move; the bench compare
is the proof, not the hope.

## 6. Migration order (gate green at every step; REV 2 resequenced)

- M0 seedable peer selection: ALREADY IMPLEMENTED by PR #285 (alternate
  constructors new_with_seed / new_durable_with_seed, node-held
  Mutex<SmallRng>, get_durable_peer_random draws from it; plus a reactor
  notification-ordering fix). D1 rebases onto main when #285 merges; R7
  unblocks then. No D1 work required.
- M1 substrate: StagingArea (node-scoped, reverse index, retention, bounds)
  wired into getter construction with TODAY'S semantics preserved (retention
  initially mimics per-call drop until the executor lands, keeping behavior
  identical); planner (closure + descendant re-drive + classification);
  executor skeleton; the COMPLETE IngestOutcome and IngestError enums, arms
  inert (red-team F5: M2 must compile against full types, so they are born
  here, migrated onto later).
- M2 EventBridge arm cutover; the executor carries StatePolicy::Rewrite and
  advance-only suppression from birth (F4); V4 bridge tests and inter_node
  bridge tests pin.
- M3 EventOnly arm cutover (containment and phantom semantics into the
  executor; V6 and containment tests pin; the arm starts persisting state, R9
  goes green, and the stale "can be ahead" doc comment at node.rs:743..755 is
  corrected in the same commit).
- M4 StateAndEvent fallback plus StateSnapshot onto the shared state-apply;
  Get lane and join_system join it (R4 green here).
- M5 typed-error call-site migration onto MutationError::Ingest (the enums
  already exist from M1); Display strings improve ack payloads.
- M6 commit_remote_transaction cutover: Atomic mode, fork-policy phase (R2,
  R3 green); NeedsState recovery wired to the Get request here.
- M7 commit_local_trx reuse of planner and policy phase (C2 atomicity pins).
- M8 staging retention flips live (NeedsState/NeedsEvents retain, R5/R6/R8
  green); factorization chapter update in the book (the concurrency chapters
  must stay true); tick the D1 checklist line in specs/concurrency/phase-2.md
  in the landing commit.

One PR, branch phase-2-d1-ingest, commits stacked so every intermediate state
passes the full validation gate.

## 7. Risks and explicitly out-of-scope magnets

Risks: the unconditional-persist guard (persist gated only on a nonempty
head) is open-coded at three sites (executor, shared state-apply, and the
NodePersist seam); D5 changes when and whether state persists and should
collapse them to one policy seam, separating phantom suppression from
buffer-currency. TOCTOU interplay (apply_event's internal retry untouched); two-phase
Atomic mode fork-staleness under concurrent local writers (commit_local_trx
already has this shape and tolerates it via upstream re-apply, context.rs:161;
red-team could not construct a divergence; watched, not redesigned);
notification regressions on the StateAndEvent fast path (pinned);
staging-area memory posture (bounded, observable, section 2.8); planner
overhead (bench compare against the E1 harness, section 5); partial migration
states (each M leaves a consistent tree).

Out of scope, deliberately: unifying WHICH validation each arm runs and the
ValidatedEvent newtype (#274, D6); #242 fork-preview design beyond the
structural C4-19 fix; receive-side transaction visibility buffering (D4; the
pipeline's Atomic mode gives the seam); state-buffer policy unification (D5);
generations and applied-set (D2); wiring StateAndRelation and
validate_causal_assertion (typed Unsupported, stays stubbed); BFS-time fetch
validation (C4-15/#244, closed by #274); crash-durable staging (D3/C6);
crash-window STALE READS on rehydration (a stale-buffer entity serves old
reads until its next write triggers the executor's gap replay; making
rehydration itself repair eagerly is D5's N31 contract; the WRITE-side
corruption this window used to enable is closed in D1 by verdict-driven gap
replay, R10).

## 8. Requirement traceability (load-bearing subset)

N1..N6 sections 2.1-2.8; N3 and N10/N13 specifically section 2.8 plus 2.2(b)
and 2.4 (the substrate and re-drive that make them true); N7 sections 2.2/2.3
and finding 6; N8 section 5 pins; N9 section 2.6 and R1; N11 section 2.4
(designed surface, inert until D3); N16 noted as the proper successor of the
2.8 cap; N19/N20 section 2.3; N25 section 2.2(c); N26 section 2.6; N27
section 2.4 (separation improved over status quo, red-team confirmed); N28
section 2.2 rationale; N31 delegated to D5 for the crash-window rehydration
contract, with steady-state parity restored in D1 (REV 3); N35/N37/N38
section 2.5; N39/N40 sections 2.6 and 2.4; N41 untouched.
