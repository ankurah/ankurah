//! Macro performance benchmarks for concurrency phase 2 (E-vol perf tier).
//!
//! This is the WALL-CLOCK, ADVISORY instrument, and it is deliberately kept
//! separate from the volume tier (`tests/tests/sim_volume.rs`, deterministic
//! correctness and memory). Everything here runs on a REAL multi-threaded tokio
//! runtime over the production in-process connector (`LocalProcessConnection`),
//! NOT the single-threaded sim transport. Only numbers from this tier are ever
//! reported as performance figures; sim-harness timings are meaningless as
//! wall-clock (single-threaded virtual transport) and are never quoted.
//!
//! Build and run:
//!
//! ```text
//! cargo bench -p ankurah-tests --bench macro_perf
//! ```
//!
//! This target is `harness = false` (criterion supplies its own harness) and is
//! NOT part of the normal `cargo test` job: criterion benches are not tests, and
//! the wall-clock work here is far heavier than the volume smoke budget.
//!
//! Measurements (all medians recorded in specs/concurrency/MACRO-BASELINE.md):
//!
//! - `single_writer_commit`: commit throughput on one durable node (one writer,
//!   no peers), reported by criterion as time per commit; MACRO-BASELINE.md
//!   converts to commits/sec.
//! - `commit_to_subscriber_latency`: wall time from a server commit to the
//!   client LiveQuery change notification firing, over a LocalProcessConnection.
//! - `bridge_catchup`: wall time for a fresh client to catch up a server entity
//!   with a history of 100 / 1000 / 5000 events, via the EventBridge fetch path.
//! - `subscription_establishment`: wall time to establish and initialize a live
//!   query at N resident entities on the server.
//!
//! Timing discipline: each case uses `iter_custom` so that per-iteration setup
//! (building nodes, seeding history, connecting peers) is EXCLUDED from the
//! measured region. The measured region is only the operation under study.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ankurah::core::node::nocache;
use ankurah::signals::Subscribe;
use ankurah::{policy::DEFAULT_CONTEXT as CTX, Mutable, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

// A small LWW-backed model. LWW so a field set is a single clean event,
// keeping the per-commit work representative of an ordinary scalar update.
#[derive(Debug, Clone, Serialize, Deserialize, ankurah::Model)]
pub struct Doc {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub body: String,
}

/// A real multi-threaded tokio runtime, so the connector's spawned tasks and the
/// node's background work run on a genuine threadpool (the production shape),
/// unlike the sim harness's single-threaded runtime.
fn multi_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("multi-thread runtime builds")
}

fn durable() -> Node<SledStorageEngine, PermissiveAgent> {
    Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new())
}

fn ephemeral() -> Node<SledStorageEngine, PermissiveAgent> {
    Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new())
}

// ===========================================================================
// 1. Single-writer commit throughput (one durable node, no peers).
// ===========================================================================

/// Time one edit-commit on a single durable node. Setup (node, system, seed
/// entity) is outside the measured region; only the begin/edit/commit of a
/// single field set is timed. criterion reports time-per-commit; divide
/// into a second for commits/sec.
fn bench_single_writer_commit(c: &mut Criterion) {
    let rt = multi_thread_runtime();
    let mut group = c.benchmark_group("single_writer_commit");
    group.throughput(Throughput::Elements(1));

    group.bench_function("durable_lww_overwrite", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async move {
                let node = durable();
                node.system.create().await.unwrap();
                let ctx = node.context(CTX).unwrap();

                // Seed one entity to edit repeatedly.
                let doc = {
                    let trx = ctx.begin();
                    let doc = trx.create(&Doc { title: "seed".into(), body: "0".into() }).await.unwrap();
                    let read = doc.read();
                    trx.commit().await.unwrap();
                    read
                };

                // Measure exactly `iters` sequential commits.
                let start = Instant::now();
                for i in 0..iters {
                    let trx = ctx.begin();
                    doc.edit(&trx).unwrap().body().set(&i.to_string()).unwrap();
                    trx.commit().await.unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

// ===========================================================================
// 2. Commit-to-subscriber propagation latency (server -> client LiveQuery).
// ===========================================================================

/// Time from a server commit to the client's LiveQuery change notification.
///
/// The server (durable) and client (ephemeral) are connected once per iteration
/// batch, the client establishes and initializes a live query, and then each
/// measured unit is a single server edit whose propagation fires the client's
/// notify. Only the commit-to-notification window is timed; connection and
/// subscription setup are excluded.
fn bench_commit_to_subscriber_latency(c: &mut Criterion) {
    let rt = multi_thread_runtime();
    let mut group = c.benchmark_group("commit_to_subscriber_latency");
    // Latency, not throughput: one propagation per iteration. Small sample size
    // keeps the wall time bounded since each unit is a full network round trip.
    group.sample_size(30);

    group.bench_function("server_edit_to_client_change", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async move {
                let server = durable();
                server.system.create().await.unwrap();
                let client = ephemeral();
                let _conn = LocalProcessConnection::new(&client, &server).await.unwrap();
                client.system.wait_system_ready().await;

                let server_ctx = server.context(CTX).unwrap();
                let client_ctx = client.context(CTX).unwrap();

                // Seed an entity the query will match.
                let doc = {
                    let trx = server_ctx.begin();
                    let doc = trx.create(&Doc { title: "watched".into(), body: "0".into() }).await.unwrap();
                    let read = doc.read();
                    trx.commit().await.unwrap();
                    read
                };

                // Establish and initialize the client's live query, then attach a
                // notify-firing listener. query_wait ensures the query is loaded
                // (its initial snapshot has arrived) before we start timing, so
                // the first measured edit is a steady-state update, not init.
                let query = client_ctx.query_wait::<DocView>("title = 'watched'").await.unwrap();
                let notify = Arc::new(Notify::new());
                let guard = {
                    let notify = notify.clone();
                    query.subscribe(move |_cs: ankurah::changes::ChangeSet<DocView>| {
                        notify.notify_one();
                    })
                };

                let mut total = Duration::ZERO;
                for i in 0..iters {
                    // Arm the waiter BEFORE committing so a fast propagation cannot
                    // fire before we start awaiting (Notify::notified is a future
                    // that must be created before the notify to be guaranteed).
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    // Poll once to register the waiter with the Notify.
                    let _ = futures_noop_poll(&mut notified);

                    let start = Instant::now();
                    {
                        let trx = server_ctx.begin();
                        doc.edit(&trx).unwrap().body().set(&format!("v{i}")).unwrap();
                        trx.commit().await.unwrap();
                    }
                    notified.await;
                    total += start.elapsed();
                }
                drop(guard);
                total
            })
        })
    });

    group.finish();
}

/// Poll a pinned future once with a no-op waker to register it (used to arm a
/// `Notify::notified()` waiter before the event that notifies it). Returns
/// whether it was already ready.
fn futures_noop_poll<F: std::future::Future>(fut: &mut std::pin::Pin<&mut F>) -> bool {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop_raw() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker { noop_raw() }
        RawWaker::new(std::ptr::null(), &RawWakerVTable::new(clone, no_op, no_op, no_op))
    }
    let waker = unsafe { Waker::from_raw(noop_raw()) };
    let mut cx = Context::from_waker(&waker);
    matches!(fut.as_mut().poll(&mut cx), Poll::Ready(_))
}

// ===========================================================================
// 3. Bridge catch-up wall time vs history depth.
// ===========================================================================

/// Time a fresh client catching up an entity whose server history is `depth`
/// events deep. The server and all history are built OUTSIDE the timed region;
/// the measured region is the client's connect-and-fetch that the EventBridge
/// serves. This is the honest wall-clock analogue of the deep-history sim
/// scenario (whose single-threaded timings are NOT reported).
fn bench_bridge_catchup(c: &mut Criterion) {
    let rt = multi_thread_runtime();
    let mut group = c.benchmark_group("bridge_catchup");
    group.sample_size(10); // Deep-history setup per iteration is expensive; keep samples modest.

    for depth in [100usize, 1000, 5000] {
        group.throughput(Throughput::Elements(depth as u64));
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_custom(|iters| {
                rt.block_on(async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // --- Setup (excluded): server with a depth-deep chain. ---
                        let server = durable();
                        server.system.create().await.unwrap();
                        let server_ctx = server.context(CTX).unwrap();
                        let doc = {
                            let trx = server_ctx.begin();
                            let doc = trx.create(&Doc { title: "chain".into(), body: "0".into() }).await.unwrap();
                            let read = doc.read();
                            trx.commit().await.unwrap();
                            read
                        };
                        for i in 0..depth {
                            let trx = server_ctx.begin();
                            doc.edit(&trx).unwrap().body().set(&i.to_string()).unwrap();
                            trx.commit().await.unwrap();
                        }

                        // Fresh client, connected but not yet caught up.
                        let client = ephemeral();
                        let _conn = LocalProcessConnection::new(&client, &server).await.unwrap();
                        client.system.wait_system_ready().await;
                        let client_ctx = client.context(CTX).unwrap();

                        // --- Measured: the catch-up fetch (EventBridge served). ---
                        let start = Instant::now();
                        let results = client_ctx.fetch::<DocView>("title = 'chain'").await.unwrap();
                        total += start.elapsed();

                        // Correctness guard so a broken bridge cannot masquerade as
                        // a fast one: the client must reach the final state.
                        assert_eq!(results.len(), 1, "client must materialize the entity");
                        assert_eq!(results[0].body().unwrap(), (depth - 1).to_string(), "client must reach the final state after catch-up");
                    }
                    total
                })
            })
        });
    }

    group.finish();
}

// ===========================================================================
// 4. Subscription establishment time at N resident entities.
// ===========================================================================

/// Time to establish and initialize a client live query when the server already
/// holds `n` resident matching entities. The server and its entities are built
/// outside the timed region; the measured region is the client's `query_wait`
/// (subscribe request, server-side matching over N entities, initial snapshot
/// delivery, client initialization).
fn bench_subscription_establishment(c: &mut Criterion) {
    let rt = multi_thread_runtime();
    let mut group = c.benchmark_group("subscription_establishment");
    group.sample_size(20);

    for n in [10usize, 100, 1000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                rt.block_on(async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // --- Setup (excluded): server with N resident entities. ---
                        let server = durable();
                        server.system.create().await.unwrap();
                        let server_ctx = server.context(CTX).unwrap();
                        {
                            let trx = server_ctx.begin();
                            for i in 0..n {
                                trx.create(&Doc { title: format!("resident{i}"), body: i.to_string() }).await.unwrap();
                            }
                            trx.commit().await.unwrap();
                        }

                        let client = ephemeral();
                        let _conn = LocalProcessConnection::new(&client, &server).await.unwrap();
                        client.system.wait_system_ready().await;
                        let client_ctx = client.context(CTX).unwrap();

                        // --- Measured: establish + initialize a broad live query. ---
                        // The predicate matches every resident entity, so the
                        // server does the full N-entity match and ships the
                        // snapshot the client must initialize on. nocache forces
                        // the query to wait on the server round trip rather than
                        // resolving against the (empty) local cache, so the
                        // measured region is the true establishment cost and the
                        // count assertion is deterministic.
                        let start = Instant::now();
                        let query = client_ctx.query_wait::<DocView>(nocache("title != 'zzz'").unwrap()).await.unwrap();
                        total += start.elapsed();

                        assert_eq!(query.ids().len(), n, "established query must see all N resident entities");
                    }
                    total
                })
            })
        });
    }

    group.finish();
}

criterion_group!(
    macro_perf,
    bench_single_writer_commit,
    bench_commit_to_subscriber_latency,
    bench_bridge_catchup,
    bench_subscription_establishment
);
criterion_main!(macro_perf);
