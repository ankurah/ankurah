//! Event-DAG-shape scale tier for the concurrency phase 2 E-shapes workstream.
//!
//! This is the CORRECTNESS-AND-MEMORY-AT-SCALE instrument, and it is
//! deliberately kept separate from the perf tier (`tests/benches/lane_perf.rs`,
//! real wall-clock). It drives the C1 deterministic simulation harness at
//! configurable event-DAG-shape scale and asserts:
//!
//! - the C1 convergence family (all-node convergence, no lost write, no phantom,
//!   head antichain validity) still holds, via `SimOutcome::assert_converged`;
//! - peak heap use per scenario stays within a LOOSE, documented,
//!   order-of-magnitude sanity bound. These bounds pin catastrophic regressions
//!   (a runaway accumulator, an unbounded buffer, a per-event leak), NOT noise;
//!   they are intentionally far from the observed peaks so ordinary run-to-run
//!   and machine-to-machine variance never trips them.
//!
//! What this tier scales is EVENT DAG SHAPE under the deterministic sim: deep
//! single-entity histories (long chains), wide concurrent antichains,
//! multi-entity churn, and subscription fan-out. This is NOT volume testing in
//! the real-deployment sense (millions of records, thousands of concurrent
//! ephemeral nodes against a durable node); that program is tracked separately
//! in issues #324 and #325.
//!
//! IMPORTANT: the simulation harness runs on a single-threaded virtual
//! transport. Its timings are meaningless as performance numbers and are NEVER
//! reported as such. Wall-clock lives exclusively in the perf tier. What the
//! sim harness gives us here is determinism (a run is a pure function of seed
//! and scenario) and the ability to check invariants and peak memory at a scale
//! the perf tier cannot reach cheaply.
//!
//! Scale is env-driven so the defaults stay inside a normal `cargo test`
//! (target: this file adds well under the ~30 s smoke budget) while a nightly
//! job can scale each knob up:
//!
//! - `SHAPE_ENTITIES`    (default 200): entities in the N-entity churn shape.
//! - `SHAPE_DEPTH`       (default 200): chain depth (number of sequential edits)
//!   for the deep single-entity-history shape.
//! - `SHAPE_ANTICHAIN`   (default  64): concurrent writers in the wide-antichain
//!   shape.
//! - `SHAPE_QUERIES`     (default  16): live queries in the subscription
//!   fan-out shape.
//! - `SHAPE_SEEDS`       (default   3): seeds per scenario (each its own swarm
//!   fault subset, except where a scenario needs a fixed schedule).
//!
//! Nightly scale example (see .github/workflows/nightly-oracle.yml):
//!
//!     SHAPE_ENTITIES=5000 SHAPE_DEPTH=5000 SHAPE_ANTICHAIN=512 SHAPE_QUERIES=128 \
//!     SHAPE_SEEDS=20 cargo test -p ankurah-tests --test sim_event_dag_shapes -- --ignored --nocapture
//!
//! Memory-attribution caveat (design-deltas E-A / 271-D): the "memory bounded
//! by divergence window, not history" target governs the reverse walk. A
//! sequence-CRDT tombstone set is instead lower-bounded by deletion history and
//! is folded by sealing, not by the streaming consumer. These scenarios use LWW
//! records and perform NO deletions, so their peaks are not confounded by that
//! floor; the bounds below would misattribute a CRDT tombstone accumulation if a
//! future scenario introduced deletions, and such a scenario must set its bound
//! from the deletion count, not from the divergence window.

use ankurah_tests::sim::alloc::{CountingAlloc, MemScope};
use ankurah_tests::sim::{body, run_once, FaultConfig, Field, SimOutcome, Workload};

// Install the counting allocator for THIS integration-test binary only. Each
// integration test is its own binary, so the per-allocation atomic bookkeeping
// perturbs nothing else in `cargo test -p ankurah-tests`.
#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

// The counting allocator is process-global, so two scenarios running on separate
// test threads at once would cross-contaminate each other's peak. `cargo test`
// runs test functions in parallel by default, so we serialize the MEASURED
// REGIONS behind this lock: only one scenario's `MemScope` is live at a time,
// making the per-scenario peak attributable regardless of `--test-threads`. The
// lock is held only around the measured `run_once`, so it does not serialize the
// whole suite, just the memory-sensitive windows. (A poisoned lock from a
// panicking scenario is fine to recover from: the next scenario re-measures from
// its own baseline.)
static MEASURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

// --- Scale knobs -----------------------------------------------------------

fn env_usize(key: &str, default: usize) -> usize { std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default) }

fn shape_entities() -> usize { env_usize("SHAPE_ENTITIES", 200) }
fn shape_depth() -> usize { env_usize("SHAPE_DEPTH", 200) }
fn shape_antichain() -> usize { env_usize("SHAPE_ANTICHAIN", 64) }
fn shape_queries() -> usize { env_usize("SHAPE_QUERIES", 16) }
fn shape_seeds() -> u64 { env_usize("SHAPE_SEEDS", 3) as u64 }

// --- Reporting -------------------------------------------------------------

/// Print a one-line, greppable memory record for a scenario run, then assert the
/// loose order-of-magnitude bound. `budget_bytes` is the catastrophe ceiling,
/// not a tight threshold: it is set well above observed peaks so it fires only
/// on a runaway (an unbounded buffer, an accumulator that never releases, a
/// per-event leak), never on ordinary variance.
fn report_and_assert_mem(scenario: &str, seed: u64, scope: &MemScope, budget_bytes: usize, outcome: &SimOutcome) {
    let peak = scope.peak_above_baseline();
    eprintln!(
        "SHAPEMEM scenario={scenario} seed={seed} peak_above_baseline_bytes={peak} baseline_bytes={} budget_bytes={budget_bytes} max_head_len={}",
        scope.baseline(),
        outcome.max_head_len
    );
    outcome.assert_converged();
    assert!(
        peak <= budget_bytes,
        "SHAPEMEM ORDER-OF-MAGNITUDE BOUND EXCEEDED scenario={scenario} seed={seed}: \
         peak_above_baseline={peak} bytes > budget={budget_bytes} bytes. This bound pins a \
         catastrophic memory regression, not noise; investigate an unbounded buffer or leak."
    );
}

/// Run one seed of a scenario under a memory scope and check invariants + bound.
/// Returns the peak-above-baseline so callers can also report an aggregate.
/// Holds `MEASURE_LOCK` across the measured region so the process-global peak is
/// attributable even when `cargo test` runs the scenarios in parallel.
fn run_measured<F, B>(scenario: &'static str, seed: u64, faults: FaultConfig, node_count: usize, budget_bytes: usize, body_fn: F) -> usize
where
    F: Fn() -> B,
    B: for<'w, 'b> FnOnce(&'b mut Workload<'w>) -> ankurah_tests::sim::ScenarioFut<'b>,
{
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scope = MemScope::begin();
    let outcome = run_once(scenario, seed, faults, node_count, body_fn());
    let peak = scope.peak_above_baseline();
    report_and_assert_mem(scenario, seed, &scope, budget_bytes, &outcome);
    peak
}

// --- Order-of-magnitude ceilings -------------------------------------------
//
// These are per-run peak-above-baseline ceilings, deliberately LOOSE. At the
// default scale on the capture machine (Apple M4 Max, see LANE-BASELINE.md) the
// observed peaks-above-baseline were roughly: churn ~29 MiB, deep history
// ~21-43 MiB (the 43 MiB is a first-run baseline-capture artifact on seed 0),
// wide antichain ~21 MiB, fan-out ~21 MiB. Each ceiling is a fixed floor (which
// absorbs harness/runtime slack and the first-run artifact) plus a generous
// per-unit cost times the knob, so a nightly run at higher scale never trips
// them from ordinary growth. The floor alone (128 MiB) sits at ~3-6x the worst
// observed default-scale peak; the per-unit terms add headroom on top as scale
// grows. A real regression (an unbounded buffer, an accumulator that never
// releases, a per-event leak) blows past even these numbers; ordinary run-to-run
// and machine-to-machine variance stays far under them. These pin catastrophe,
// not noise.

const FIXED_FLOOR_BYTES: usize = 128 * 1024 * 1024; // 128 MiB floor: slack + first-run artifact.

fn churn_budget(entities: usize) -> usize { FIXED_FLOOR_BYTES + entities * 512 * 1024 }
fn deep_budget(depth: usize) -> usize { FIXED_FLOOR_BYTES + depth * 512 * 1024 }
fn antichain_budget(writers: usize) -> usize { FIXED_FLOOR_BYTES + writers * 1024 * 1024 }
fn fanout_budget(queries: usize, entities: usize) -> usize { FIXED_FLOOR_BYTES + (queries + entities) * 512 * 1024 }

// ===========================================================================
// Scenario 1: N-entity multi-node churn (extends the C1 churn shape upward).
// ===========================================================================

/// Create `SHAPE_ENTITIES` entities spread across all origins, settle so every
/// node holds every entity, then edit each from a different origin. Convergence,
/// no-lost-write, no-phantom, and antichain validity must all hold at scale;
/// peak memory must stay within the loose churn bound.
#[test]
fn multi_entity_churn() {
    let entities = shape_entities();
    let budget = churn_budget(entities);
    let mut worst = 0usize;
    for seed in 0..shape_seeds() {
        let faults = FaultConfig::swarm_from_seed(seed);
        let peak = run_measured("multi_entity_churn", seed, faults, 4, budget, || {
            body(move |w: &mut Workload| {
                Box::pin(async move {
                    let mut ids = Vec::with_capacity(entities);
                    for i in 0..entities {
                        let origin = i % w.node_count();
                        ids.push(w.create_at(origin, Field::Title, &format!("e{i}")).await);
                    }
                    // Everyone holds every entity before the edit wave, so an
                    // edit can originate from a node other than the creator.
                    w.settle().await;
                    for (i, id) in ids.iter().enumerate() {
                        let origin = (i + 1) % w.node_count();
                        w.edit_at(origin, *id, Field::Body, &format!("b{i}")).await;
                    }
                })
            })
        });
        worst = worst.max(peak);
    }
    eprintln!("SHAPEMEM-SUMMARY multi_entity_churn entities={entities} worst_peak_above_baseline_bytes={worst} budget_bytes={budget}");
}

// ===========================================================================
// Scenario 2: deep single-entity history, then catch-up under the schedule.
// ===========================================================================

/// One entity with a chain of `SHAPE_DEPTH` sequential edits. Every ephemeral
/// node catches up through the scheduler one event at a time (acceptance-retry
/// on the CommitTransaction path), so this exercises the applier and staging at
/// depth and asserts all nodes converge on the deep chain.
///
/// NOTE ON SCOPE: this is the deterministic correctness-and-memory view of a
/// deep chain. Catch-up WALL TIME is a performance question and lives in the
/// perf tier: `lane_perf.rs::bench_bridge_catchup` (stale-client shape, the
/// only shape served by the EventBridge lane) and
/// `lane_perf.rs::bench_fresh_fetch_snapshot` (fresh client, snapshot lane).
/// Reporting a sim-harness catch-up time as a performance number is explicitly
/// forbidden.
#[test]
fn deep_single_entity_history() {
    let depth = shape_depth();
    let budget = deep_budget(depth);
    let mut worst = 0usize;
    // A fixed (fault-free) schedule keeps the deep-chain shape stable across
    // seeds; the depth, not the fault subset, is the variable under study here.
    for seed in 0..shape_seeds() {
        let peak = run_measured("deep_single_entity_history", seed, FaultConfig::none(), 3, budget, || {
            body(move |w: &mut Workload| {
                Box::pin(async move {
                    let e = w.create_at(0, Field::Title, "genesis").await;
                    for i in 0..depth {
                        // Alternate fields so successive edits are meaningful
                        // writes; each parents on the tracked head, extending
                        // one linear chain.
                        let field = if i % 2 == 0 { Field::Body } else { Field::Title };
                        w.edit_at(0, e, field, &format!("v{i}")).await;
                    }
                })
            })
        });
        worst = worst.max(peak);
    }
    eprintln!("SHAPEMEM-SUMMARY deep_single_entity_history depth={depth} worst_peak_above_baseline_bytes={worst} budget_bytes={budget}");
}

// ===========================================================================
// Scenario 3: wide concurrent antichain on one entity, then merge.
// ===========================================================================

/// Build a genuinely wide head: create one entity, settle, then issue
/// `SHAPE_ANTICHAIN` concurrent edits all parented on the SAME fork clock (so
/// they are mutual siblings, not a chain), alternating fields so they commute.
/// After they all propagate the head is a wide antichain, which the
/// head-antichain invariant validates non-trivially (`max_head_len` reports the
/// width). A final edit parented on the full wide head merges it back to a
/// single tip.
#[test]
fn wide_concurrent_antichain() {
    let writers = shape_antichain();
    let budget = antichain_budget(writers);
    let mut worst = 0usize;
    let mut worst_head = 0usize;
    for seed in 0..shape_seeds() {
        let faults = FaultConfig::swarm_from_seed(seed);
        // Hold the measurement lock across the measured region (see MEASURE_LOCK)
        // so the process-global peak is attributable under parallel test threads.
        let guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let scope = MemScope::begin();
        let outcome = run_once("wide_concurrent_antichain", seed, faults, 3, {
            body(move |w: &mut Workload| {
                Box::pin(async move {
                    let e = w.create_at(0, Field::Title, "base").await;
                    // Everyone must hold the base before the concurrent wave.
                    w.settle().await;
                    let fork = w.head_of(e).unwrap();
                    // Many concurrent siblings off the shared fork. Origin
                    // rotates so writers are spread across nodes; the fork is
                    // held stale on purpose so none observes another.
                    for k in 0..writers {
                        let origin = k % w.node_count();
                        let field = if k % 2 == 0 { Field::Title } else { Field::Body };
                        w.edit_from(origin, e, fork.clone(), field, &format!("w{k}")).await;
                    }
                    // Let the wide head form everywhere, then merge it to one tip.
                    w.settle().await;
                    let wide_head = w.head_of(e).unwrap();
                    w.edit_from(0, e, wide_head, Field::Title, "merged").await;
                })
            })
        });
        let peak = scope.peak_above_baseline();
        report_and_assert_mem("wide_concurrent_antichain", seed, &scope, budget, &outcome);
        drop(guard);
        worst = worst.max(peak);
        worst_head = worst_head.max(outcome.max_head_len);
    }
    eprintln!(
        "SHAPEMEM-SUMMARY wide_concurrent_antichain writers={writers} worst_head_len={worst_head} worst_peak_above_baseline_bytes={worst} budget_bytes={budget}"
    );
}

// ===========================================================================
// Scenario 4: subscription fan-out while churn runs.
// ===========================================================================

/// Open `SHAPE_QUERIES` live queries over overlapping predicates on the ephemeral
/// nodes, then run entity churn while they are live. The subscriptions stay open
/// through the quiescence barrier and the invariant checks (the driver lifts
/// them out for exactly this). Convergence must hold with the reactor and relay
/// under load; peak memory must stay within the fan-out bound.
///
/// The predicates overlap on purpose (`title LIKE 'e%'` plus per-bucket
/// refinements) so a single entity change is a candidate for several
/// subscriptions at once, exercising the reactor's per-subscription fan-out.
#[test]
fn subscription_fanout() {
    let queries = shape_queries();
    let entities = (shape_entities() / 4).max(8); // Keep the churn modest; the fan-out is the variable.
    let budget = fanout_budget(queries, entities);
    let mut worst = 0usize;
    for seed in 0..shape_seeds() {
        let faults = FaultConfig::swarm_from_seed(seed);
        let peak = run_measured("subscription_fanout", seed, faults, 3, budget, || {
            body(move |w: &mut Workload| {
                Box::pin(async move {
                    // Establish overlapping live queries across the ephemeral
                    // nodes. Every query matches the churn entities (all titled
                    // "e..."), so each entity change fans out to all of them.
                    let node_count = w.node_count();
                    for q in 0..queries {
                        let node = 1 + (q % node_count.saturating_sub(1).max(1));
                        let node = node.min(node_count - 1);
                        // A broad predicate plus a harmless per-query disjunct so
                        // the predicates are distinct query objects but overlap
                        // in truth set.
                        let pred = format!("title = 'e{q}' OR title != 'zzz{q}'");
                        w.subscribe(node, &pred).await;
                    }
                    // Churn: create then edit a batch of entities while the
                    // subscriptions are live.
                    let mut ids = Vec::with_capacity(entities);
                    for i in 0..entities {
                        let origin = i % node_count;
                        ids.push(w.create_at(origin, Field::Title, &format!("e{i}")).await);
                    }
                    w.settle().await;
                    for (i, id) in ids.iter().enumerate() {
                        let origin = (i + 1) % node_count;
                        w.edit_at(origin, *id, Field::Body, &format!("b{i}")).await;
                    }
                })
            })
        });
        worst = worst.max(peak);
    }
    eprintln!(
        "SHAPEMEM-SUMMARY subscription_fanout queries={queries} entities={entities} worst_peak_above_baseline_bytes={worst} budget_bytes={budget}"
    );
}

// ===========================================================================
// Nightly-scale variants: the same four shapes at high scale, `#[ignore]` so
// the normal test job runs only the default-scale tests above. The nightly job
// runs these with `--ignored` and the SHAPE_* knobs turned up.
// ===========================================================================

/// Nightly-scale churn. Same body as `multi_entity_churn`; separated so the
/// default job stays fast and the nightly job can crank `SHAPE_ENTITIES`.
#[test]
#[ignore = "nightly event-DAG-shape scale tier; run with --ignored and SHAPE_* env knobs"]
fn nightly_multi_entity_churn() { multi_entity_churn(); }

/// Nightly-scale deep history.
#[test]
#[ignore = "nightly event-DAG-shape scale tier; run with --ignored and SHAPE_* env knobs"]
fn nightly_deep_single_entity_history() { deep_single_entity_history(); }

/// Nightly-scale wide antichain.
#[test]
#[ignore = "nightly event-DAG-shape scale tier; run with --ignored and SHAPE_* env knobs"]
fn nightly_wide_concurrent_antichain() { wide_concurrent_antichain(); }

/// Nightly-scale subscription fan-out.
#[test]
#[ignore = "nightly event-DAG-shape scale tier; run with --ignored and SHAPE_* env knobs"]
fn nightly_subscription_fanout() { subscription_fanout(); }
