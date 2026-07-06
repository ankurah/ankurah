# Per-lane end-to-end performance baseline (phase 2, E-shapes perf tier)

Wall-clock per-lane baseline for the concurrency phase 2 E-shapes workstream. It
is the real-runtime counterpart to the micro baseline in
`core/benches/BASELINE.md`, captured on the same machine at the same phase-2
point so later optimization work (workstream E2) has a fixed per-lane reference
that predates the workstream D refactors.

## Why this lives here and not next to BASELINE.md

`core/benches/BASELINE.md` documents the criterion micro-benches in
`core/benches/`, which reach the crate-internal event-DAG engine through
`ankurah_core::bench_support`. The per-lane end-to-end benches measured here live
in the `ankurah-tests` crate (`tests/benches/lane_perf.rs`) because they exercise
the full stack: real multi-threaded tokio and the production in-process connector
(`LocalProcessConnection`) across durable and ephemeral Nodes. That harness is
not a `core` bench and does not belong beside the core micro doc. It sits with
the phase-2 spec that governs it (`specs/concurrency/phase-2.md`, workstream E).
The disclosure style below deliberately mirrors `BASELINE.md`.

## These are advisory wall-clock figures, not guarantees

Numbers are criterion medians (the middle value of criterion's
`[lower median upper]` estimate). They are wall-clock figures on one machine and
are meant for relative comparison and regression tracking, not as absolute
throughput or latency guarantees. They are ADVISORY: the perf tier is not run in
the normal CI test job (criterion benches are not tests), so these do not gate
merges. They exist to catch large per-lane regressions across the workstream D
refactors and to give E2 a before-picture.

Only this tier produces performance numbers. The event-DAG-shape scale tier
(`tests/tests/sim_event_dag_shapes.rs`) runs on a single-threaded virtual
transport; its timings are meaningless as wall-clock and are never reported here.

## Hardware and toolchain

- Machine: Apple M4 Max, 16 cores, 128 GB RAM
- OS: macOS 26.5.1 (build 25F80)
- Toolchain: rustc 1.97.0-nightly (ca9a134e0 2026-04-26), cargo 1.97.0-nightly
- criterion 0.5 with the `async_tokio` feature
- Real multi-threaded tokio runtime (`new_multi_thread`), production
  `LocalProcessConnection` connector, in-memory sled engines
- Quiet machine, on AC power. Absolute values will differ on other hardware and
  on shared CI runners; capture your own baseline on the target machine before
  reading regressions into small deltas.

## Command

```sh
cargo bench -p ankurah-tests --bench lane_perf
```

Captured on main at `ce2f7cab` plus the E-shapes branch (this instrument), all
groups in one run, with the criterion sampling shown per group below. Raw
criterion log kept outside the repo (`lane_bench_corrected.log`).

## Measurements

Each case uses criterion `iter_custom`, so per-iteration setup (building nodes,
seeding history, connecting peers, establishing the subscription) is EXCLUDED
from the measured region. Only the operation under study is timed.

| Group / case                                    | Param | Median    | Derived          |
| ----------------------------------------------- | ----- | --------- | ---------------- |
| single_writer_commit / durable_lww_overwrite    | n/a   | 37.06 us  | ~27.0k commits/s |
| commit_to_subscriber_latency / server_to_client | n/a   | 84.37 us  |                  |
| fresh_fetch_snapshot                            | 100   | 110.06 us |                  |
| fresh_fetch_snapshot                            | 1000  | 132.61 us |                  |
| fresh_fetch_snapshot                            | 5000  | 136.98 us |                  |
| bridge_catchup                                  | 100   | 3.325 ms  | ~33 us/event     |
| bridge_catchup                                  | 1000  | 38.41 ms  | ~38 us/event     |
| bridge_catchup                                  | 5000  | 196.2 ms  | ~39 us/event     |
| subscription_establishment                      | 10    | 379.87 us |                  |
| subscription_establishment                      | 100   | 3.412 ms  |                  |
| subscription_establishment                      | 1000  | 39.24 ms  |                  |

Sampling: `single_writer_commit` and `commit_to_subscriber_latency` use enough
iterations to fill criterion's default 100-sample / 30-sample windows;
`fresh_fetch_snapshot` and `bridge_catchup` use 10 samples and
`subscription_establishment` uses 20, because each of their iterations does
heavy per-iteration setup (a fresh N-deep history or N resident entities) and a
full round trip.

### What each case measures

- **single_writer_commit**: one durable node, no peers. Per unit: begin a
  transaction, overwrite one LWW field on a seeded entity, commit. This is the
  local commit path (staging, event creation, `commit_event`, `set_state`) with
  no network. Divide 1 s by the median for commits/sec.
- **commit_to_subscriber_latency**: durable server and ephemeral client over one
  `LocalProcessConnection`. The client holds an initialized `LiveQuery` matching
  the entity. Per unit: arm a waiter, commit one edit on the server, and measure
  until the client's `LiveQuery` change notification fires. This is the
  end-to-end propagation latency of a single steady-state update (server commit
  to client reactor notification).
- **fresh_fetch_snapshot**: durable server carrying an entity with a
  `param`-deep chain of edits; a FRESH ephemeral client connects and fetches
  for the first time. LANE: a fresh fetch sends empty known_matches, so the
  server answers with a full StateSnapshot (`generate_entity_delta` Case 3),
  never an EventBridge. Depth therefore affects only the excluded setup; the
  measured region is one final-state snapshot adoption, and the numbers are
  expected to be roughly flat in depth. A lane guard asserts the client
  committed NO events locally (the snapshot arm commits none), so the lane
  cannot silently change without failing the bench. This is a useful number
  (fresh-client first-fetch latency at depth-N server history) but it is NOT
  catch-up replay.
- **bridge_catchup**: the TRUE EventBridge lane, via the stale-client shape,
  which is the only shape that produces `DeltaContent::EventBridge`. Setup
  (excluded): the server creates the entity; the client connects, fetches once
  so it persists the entity at that head, then disconnects (dropping the
  `LocalProcessConnection`); the server advances by `param` edits. Measured:
  the client reconnects and re-fetches; known_matches now carries the stale
  head, the server builds the bridge from stale head to current head
  (`generate_entity_delta` Case 2, including the server-side causal comparison
  and backward event walk), and the client applies the `param` gap events
  through the bridge arm (validate, stage, topo-sort, apply, commit each).
  Reconnect (two peer registrations, two task spawns) is inside the measured
  region and negligible against the bridge at any gap. Guards: the client must
  reach the final state, AND a lane guard asserts the client committed at
  least `param` events locally, which only the bridge arm does; if the server
  fell back to a StateSnapshot the count stays 0 and the bench fails rather
  than recording the wrong lane. A one-off instrumented run confirmed the lane
  switch on this exact shape (stored events: 0 after the fresh fetch, exactly
  `gap` after the stale re-fetch).
- **subscription_establishment**: durable server holding `param` resident
  matching entities; a fresh ephemeral client establishes and initializes a
  broad live query (via `nocache`, so it waits on the server round trip rather
  than resolving against the empty local cache). The measured region is the
  establishment plus initial-snapshot delivery for all N entities.

### Reading notes and caveats

- The commit throughput here (~37 us, ~27k commits/s) is the full local commit
  path on sled, NOT the bare `compare()`/engine microcost in
  `core/benches/BASELINE.md`. They are not comparable; this one includes
  storage, event creation, and state materialization.
- `fresh_fetch_snapshot` is roughly flat in history depth (110 -> 133 -> 137 us
  for 100 -> 1000 -> 5000), as the lane predicts: the response is one snapshot
  of final state regardless of depth. The small rise plausibly reflects
  server-side storage reads over a larger store. Do not read these as catch-up
  numbers: the first capture of this baseline made exactly that mistake,
  labeling this lane "bridge_catchup", which would imply ~30 ns/event replay;
  the E1 micro baseline alone shows that is impossible (one comparison over a
  1000-deep history is ~2.6 ms).
- `bridge_catchup` scales LINEARLY with gap depth: ~33-39 us per bridge-applied
  event, i.e. ~25-30k events/s through the bridge arm. The per-event cost is
  the same order as a local commit (~37 us), which is coherent: each bridge
  event is validated, staged, applied, and committed on the client, and the
  server additionally pays the causal comparison plus the backward walk to
  assemble the bridge. This linear-in-gap wall time is the honest catch-up
  baseline and the number E2's streaming/application work would move; the
  divergence-window target in workstream E governs its memory, not its time.
- `subscription_establishment` grows roughly linearly in resident-entity count
  (10 -> ~0.38 ms, 100 -> ~3.4 ms, 1000 -> ~39 ms, i.e. about 10x per 10x). The
  server matches the predicate over all N entities and ships a snapshot the
  client initializes on; that per-entity cost is the dominant term. This is the
  before-number for any future establishment-path optimization.
- Latency (~84 us) and throughput (~37 us) are single-connection, single-writer,
  in-process figures. Multi-writer contention, multi-peer relay fan-out, and
  real network transports are out of scope for this baseline and are separate
  measurements if and when they matter.
- Run-to-run variance on the multi-ms cases (bridge_catchup,
  subscription_establishment) is a few percent to ~15% at these sample counts;
  as stated above, capture a local baseline before reading a regression into a
  small delta.

## Re-running

Re-run the exact command above on the target machine. As with the micro
baseline, absolute values are machine-specific; capture a local baseline before
reading a regression into a small delta. When E2 lands, re-run and record the
after-numbers beside these with the ratio, holding the correctness gates.
