# Macro performance baseline (phase 2, E-vol perf tier)

Wall-clock macro baseline for the concurrency phase 2 macro/volume workstream
(E-vol). It is the real-runtime counterpart to the micro baseline in
`core/benches/BASELINE.md`, captured on the same machine at the same phase-2
point so later optimization work (workstream E2) has a fixed macro reference
that predates the workstream D refactors.

## Why this lives here and not next to BASELINE.md

`core/benches/BASELINE.md` documents the criterion micro-benches in
`core/benches/`, which reach the crate-internal event-DAG engine through
`ankurah_core::bench_support`. The macro benches measured here live in the
`ankurah-tests` crate (`tests/benches/macro_perf.rs`) because they exercise the
full stack: real multi-threaded tokio and the production in-process connector
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
merges. They exist to catch large macro regressions across the workstream D
refactors and to give E2 a before-picture.

Only this tier produces performance numbers. The volume tier
(`tests/tests/sim_volume.rs`) runs on a single-threaded virtual transport; its
timings are meaningless as wall-clock and are never reported here.

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
cargo bench -p ankurah-tests --bench macro_perf
```

Captured on main at `ce2f7cab` plus the E-vol branch (this instrument), with the
default criterion sampling shown per group below. Raw criterion log kept outside
the repo (`macro_bench_full.log`).

## Measurements

Each case uses criterion `iter_custom`, so per-iteration setup (building nodes,
seeding history, connecting peers, establishing the subscription) is EXCLUDED
from the measured region. Only the operation under study is timed.

| Group / case                                    | Param | Median    | Derived           |
| ----------------------------------------------- | ----- | --------- | ----------------- |
| single_writer_commit / durable_lww_overwrite    | n/a   | 37.16 us  | ~26.9k commits/s  |
| commit_to_subscriber_latency / server_to_client | n/a   | 82.62 us  |                   |
| bridge_catchup                                  | 100   | 99.21 us  |                   |
| bridge_catchup                                  | 1000  | 137.13 us |                   |
| bridge_catchup                                  | 5000  | 151.75 us |                   |
| subscription_establishment                      | 10    | 353.63 us |                   |
| subscription_establishment                      | 100   | 2.884 ms  |                   |
| subscription_establishment                      | 1000  | 36.278 ms |                   |

Sampling: `single_writer_commit` and `commit_to_subscriber_latency` use enough
iterations to fill criterion's default 100-sample / 30-sample windows;
`bridge_catchup` uses 10 samples and `subscription_establishment` uses 20,
because each of their iterations does heavy per-iteration setup (a fresh
N-deep history or N resident entities) and a full round trip.

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
- **bridge_catchup**: durable server carrying an entity with a `param`-deep
  chain of edits; a fresh ephemeral client connects and fetches. The measured
  region is only the catch-up fetch, which the EventBridge serves. A correctness
  guard asserts the client reaches the final state, so a broken bridge cannot
  read as a fast one.
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
- `bridge_catchup` grows strongly sublinearly in history depth (100 -> 1000 adds
  ~38 us, but 1000 -> 5000 adds only ~15 us on the median). The EventBridge
  ships the missing events in bulk rather than per-event round trips, so beyond a
  few hundred events the fixed fetch and round-trip cost dominates and the
  marginal per-event application cost is small at these depths. The 5000 case has
  a wide band on 10 samples (`[132.93 151.75 184.10] us`); treat its median as
  indicative, not precise. This sublinearity is exactly the property the E
  streaming-application target (bounded by divergence window, not history) means
  to preserve or improve; it is the number E2 must not regress.
- `subscription_establishment` grows roughly linearly in resident-entity count
  (10 -> ~0.35 ms, 100 -> ~2.9 ms, 1000 -> ~36 ms, i.e. about 10x per 10x). The
  server matches the predicate over all N entities and ships a snapshot the
  client initializes on; that per-entity cost is the dominant term. This is the
  before-number for any future establishment-path optimization.
- Latency (~83 us) and throughput (~37 us) are single-connection, single-writer,
  in-process figures. Multi-writer contention, multi-peer relay fan-out, and
  real network transports are out of scope for this baseline and are separate
  measurements if and when they matter.

## Re-running

Re-run the exact command above on the target machine. As with the micro
baseline, absolute values are machine-specific; capture a local baseline before
reading a regression into a small delta. When E2 lands, re-run and record the
after-numbers beside these with the ratio, holding the correctness gates.
