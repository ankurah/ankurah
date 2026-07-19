//! Criterion micro-benchmarks for the event DAG engine (workstream E1).
//!
//! Reaches the crate-internal comparison, layering, and ordering primitives
//! through the feature-gated `ankurah_core::bench_support` surface. Build and
//! run with:
//!
//! ```text
//! cargo bench -p ankurah-core --features bench-internals --bench event_dag
//! ```
//!
//! DAG generation is deterministic (fixed content-addressed seeds, no RNG
//! state carried between cases) and events are served from an in-memory
//! `GetEvents` map, so there are no storage engines in the hot path and
//! results are reproducible across runs and machines. Ids are content
//! addressed, so a given shape at a given size always produces the same DAG.
//!
//! Groups:
//! - `compare`: the causal comparison across DAG shapes (linear deep, uneven
//!   diamond chains, wide antichains, disjoint graphs).
//! - `layers`: draining a `DivergedSince` comparison into topological layers.
//! - `clock`: membership, normalizing construction, and insert on `Clock`.
//! - `toposort`: parents-first ordering of a shuffled event batch.

use std::collections::{BTreeMap, HashMap};

use ankurah_core::bench_support::{compare, topo_sort_events, AbstractCausalRelation, DEFAULT_BUDGET};
use ankurah_core::error::RetrievalError;
use ankurah_core::retrieval::GetEvents;
use ankurah_proto::{Attested, Clock, EntityId, Event, EventId, OperationSet};
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// ============================================================================
// In-memory event source and deterministic DAG generators
// ============================================================================

/// In-memory event source: the whole DAG lives in a `HashMap`, so `compare`
/// exercises only the traversal/accumulation logic, never storage IO.
#[derive(Clone)]
struct MemRetriever {
    events: HashMap<EventId, Event>,
}

impl MemRetriever {
    fn new() -> Self { Self { events: HashMap::new() } }
    fn add(&mut self, event: Event) -> EventId {
        let id = event.id();
        self.events.insert(id.clone(), event);
        id
    }
}

#[async_trait]
impl GetEvents for MemRetriever {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.events.get(event_id).cloned().ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }
    async fn event_stored(&self, _event_id: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
}

/// Deterministic event with a content-addressed id derived from `seed` and the
/// parent clock. The seed only differentiates otherwise-identical events; ids
/// remain stable across runs. A `u32` seed space keeps wide/deep shapes free
/// of collisions.
fn event(seed: u32, parents: &[EventId]) -> Event {
    let mut entity_id_bytes = [0u8; 16];
    entity_id_bytes[0..4].copy_from_slice(&seed.to_be_bytes());
    Event {
        entity_id: EntityId::from_bytes(entity_id_bytes),
        model: ankurah_proto::ModelId::EntityId(EntityId::from_bytes([0; 16])),
        parent: Clock::from(parents.to_vec()),
        operations: OperationSet(BTreeMap::new()),
    }
}

/// A generated scenario: the populated retriever plus the two clocks to
/// compare and the total event count (for throughput reporting).
struct Scenario {
    retriever: MemRetriever,
    subject: Clock,
    comparison: Clock,
    events: u64,
}

/// Linear chain root -> ... -> head of `depth` events. Compares head against
/// root: a `StrictDescends` that walks the whole chain (the general BFS path,
/// not the one-step fast path, because comparison is the root not head-1).
fn linear_deep(depth: usize) -> Scenario {
    let mut r = MemRetriever::new();
    let mut prev: Vec<EventId> = vec![];
    let mut root = None;
    let mut head = None;
    for i in 0..depth {
        let id = r.add(event(i as u32, &prev));
        if i == 0 {
            root = Some(id.clone());
        }
        prev = vec![id.clone()];
        head = Some(id);
    }
    Scenario {
        retriever: r,
        subject: Clock::from(vec![head.expect("depth >= 1")]),
        comparison: Clock::from(vec![root.expect("depth >= 1")]),
        events: depth as u64,
    }
}

/// A chain of `n` diamonds: each diamond forks a base into two concurrent
/// events A and B, then joins them. The two clocks are the pre-join tips of
/// the final diamond, forcing a real two-frontier BFS all the way down to a
/// `DivergedSince` at the last shared join. Uneven because the A and B legs
/// carry different seeds (distinct ids), so neither leg dominates.
fn diamond_chain(n: usize) -> Scenario {
    let mut r = MemRetriever::new();
    let mut seed = 0u32;
    let mut next = || {
        let s = seed;
        seed += 1;
        s
    };

    let mut base = r.add(event(next(), &[]));
    let mut last_a = base.clone();
    let mut last_b = base.clone();

    for _ in 0..n {
        let a = r.add(event(next(), &[base.clone()]));
        let b = r.add(event(next(), &[base.clone()]));
        last_a = a.clone();
        last_b = b.clone();
        let join = r.add(event(next(), &[a, b]));
        base = join;
    }

    // Compare the two pre-join tips of the final diamond: concurrent, meeting
    // at the previous join (or the root for n == 1).
    Scenario { retriever: r, subject: Clock::from(vec![last_a]), comparison: Clock::from(vec![last_b]), events: (1 + n * 3) as u64 }
}

/// A single root with `width` concurrent children (a wide antichain), joined
/// by one event that names all of them as parents. Compares the join against
/// the root: `StrictDescends`, but the join's parent clock is a width-wide
/// antichain, stressing the multi-parent frontier expansion.
fn wide_antichain(width: usize) -> Scenario {
    let mut r = MemRetriever::new();
    let root = r.add(event(0, &[]));
    let mut children = Vec::with_capacity(width);
    for i in 0..width {
        children.push(r.add(event(i as u32 + 1, &[root.clone()])));
    }
    let join = r.add(event(width as u32 + 1, &children));
    Scenario { retriever: r, subject: Clock::from(vec![join]), comparison: Clock::from(vec![root]), events: (width + 2) as u64 }
}

/// Two independent linear chains of `depth` each, sharing no root. Comparing
/// their heads yields `Disjoint`: both frontiers walk to their own genesis
/// before the exhaustion verdict fires.
fn disjoint(depth: usize) -> Scenario {
    let mut r = MemRetriever::new();
    let mut mk_chain = |base_seed: u32| -> EventId {
        let mut prev: Vec<EventId> = vec![];
        let mut head = None;
        for i in 0..depth {
            let id = r.add(event(base_seed + i as u32, &prev));
            prev = vec![id.clone()];
            head = Some(id);
        }
        head.expect("depth >= 1")
    };
    let head_a = mk_chain(0);
    let head_b = mk_chain(1_000_000);
    Scenario { retriever: r, subject: Clock::from(vec![head_a]), comparison: Clock::from(vec![head_b]), events: (2 * depth) as u64 }
}

// ============================================================================
// compare() across DAG shapes
// ============================================================================

fn bench_compare(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("compare");

    // Linear deep: head vs root, StrictDescends over the whole chain.
    for depth in [10usize, 100, 1000] {
        let s = linear_deep(depth);
        group.throughput(Throughput::Elements(s.events));
        group.bench_with_input(BenchmarkId::new("linear_deep", depth), &s, |b, s| {
            b.to_async(&rt).iter(|| async {
                let r = compare(s.retriever.clone(), &s.subject, &s.comparison, DEFAULT_BUDGET).await.unwrap();
                debug_assert!(matches!(r.relation(), AbstractCausalRelation::StrictDescends { .. }));
                std::hint::black_box(r.relation());
            });
        });
    }

    // Uneven diamond chains: two concurrent tips, DivergedSince at the last join.
    for n in [4usize, 16, 64] {
        let s = diamond_chain(n);
        group.throughput(Throughput::Elements(s.events));
        group.bench_with_input(BenchmarkId::new("diamond_chain", n), &s, |b, s| {
            b.to_async(&rt).iter(|| async {
                let r = compare(s.retriever.clone(), &s.subject, &s.comparison, DEFAULT_BUDGET).await.unwrap();
                debug_assert!(matches!(r.relation(), AbstractCausalRelation::DivergedSince { .. }));
                std::hint::black_box(r.relation());
            });
        });
    }

    // Wide antichains: join vs root, StrictDescends over a width-wide parent set.
    for width in [4usize, 16, 64] {
        let s = wide_antichain(width);
        group.throughput(Throughput::Elements(s.events));
        group.bench_with_input(BenchmarkId::new("wide_antichain", width), &s, |b, s| {
            b.to_async(&rt).iter(|| async {
                let r = compare(s.retriever.clone(), &s.subject, &s.comparison, DEFAULT_BUDGET).await.unwrap();
                debug_assert!(matches!(r.relation(), AbstractCausalRelation::StrictDescends { .. }));
                std::hint::black_box(r.relation());
            });
        });
    }

    // Disjoint graphs: two roots, exhaustion to a Disjoint verdict.
    for depth in [10usize, 100] {
        let s = disjoint(depth);
        group.throughput(Throughput::Elements(s.events));
        group.bench_with_input(BenchmarkId::new("disjoint", depth), &s, |b, s| {
            b.to_async(&rt).iter(|| async {
                let r = compare(s.retriever.clone(), &s.subject, &s.comparison, DEFAULT_BUDGET).await.unwrap();
                debug_assert!(matches!(r.relation(), AbstractCausalRelation::Disjoint { .. }));
                std::hint::black_box(r.relation());
            });
        });
    }

    group.finish();
}

// ============================================================================
// Layer iteration: DivergedSince -> EventLayers -> drain
// ============================================================================

fn bench_layers(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("layers");

    // A diamond chain diverges into two long concurrent tips; draining walks
    // every layer from the meet to both tips. current_head is the comparison
    // tip so the partition into to_apply / already_applied is non-trivial.
    for n in [4usize, 16, 64] {
        let s = diamond_chain(n);
        let current_head: Vec<EventId> = s.comparison.to_vec();
        group.throughput(Throughput::Elements(s.events));
        group.bench_with_input(BenchmarkId::new("diamond_chain_drain", n), &s, |b, s| {
            b.to_async(&rt).iter(|| async {
                let r = compare(s.retriever.clone(), &s.subject, &s.comparison, DEFAULT_BUDGET).await.unwrap();
                let mut layers = r.into_layers(current_head.clone()).expect("DivergedSince yields layers");
                let mut count = 0usize;
                while let Some(layer) = layers.next().await.unwrap() {
                    count += layer.to_apply + layer.already_applied;
                }
                std::hint::black_box(count);
            });
        });
    }

    group.finish();
}

// ============================================================================
// Clock operations: membership, normalization, insert
// ============================================================================

/// A vector of `n` distinct event ids in reverse (worst case for a sort that
/// normalizes), plus a mid-vector member to probe for.
fn clock_ids(n: usize) -> Vec<EventId> { (0..n as u32).rev().map(|i| event(i, &[]).id()).collect() }

fn bench_clock(c: &mut Criterion) {
    let mut group = c.benchmark_group("clock");

    for n in [8usize, 64, 512] {
        let ids = clock_ids(n);
        let sorted = Clock::from(ids.clone());
        // A member guaranteed present: the id at the middle of the set.
        let needle = ids[n / 2].clone();

        // Membership: binary search over the normalized inner vec.
        group.bench_with_input(BenchmarkId::new("contains", n), &(&sorted, &needle), |b, (clock, needle)| {
            b.iter(|| std::hint::black_box(clock.contains(needle)));
        });

        // Normalizing construction: sort + dedup from unsorted input.
        group.bench_with_input(BenchmarkId::new("normalize_from", n), &ids, |b, ids| {
            b.iter_batched(|| ids.clone(), |ids| std::hint::black_box(Clock::from(ids)), criterion::BatchSize::SmallInput);
        });

        // Insert: clone-and-insert a new id into a normalized clock.
        let extra = event(n as u32 + 1, &[]).id();
        group.bench_with_input(BenchmarkId::new("with_event", n), &(&sorted, &extra), |b, (clock, extra)| {
            b.iter(|| std::hint::black_box(clock.with_event((*extra).clone())));
        });
    }

    group.finish();
}

// ============================================================================
// Topological sort of event batches (ordering.rs)
// ============================================================================

/// A linear chain of `n` events as an `Attested<Event>` batch, then rotated so
/// no event precedes its parent in input order: worst case for Kahn's
/// algorithm, which must reorder the whole batch parents-first.
fn shuffled_chain_batch(n: usize) -> Vec<Attested<Event>> {
    let mut prev: Vec<EventId> = vec![];
    let mut events = Vec::with_capacity(n);
    for i in 0..n {
        let ev = event(i as u32, &prev);
        prev = vec![ev.id()];
        events.push(ev);
    }
    // Rotate by half so children appear before parents in the input.
    events.rotate_left(n / 2);
    events.into_iter().map(|e| Attested::opt(e, None)).collect()
}

fn bench_toposort(c: &mut Criterion) {
    let mut group = c.benchmark_group("toposort");

    for n in [16usize, 128, 1024] {
        let batch = shuffled_chain_batch(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("shuffled_chain", n), &batch, |b, batch| {
            b.iter_batched(
                || batch.clone(),
                |batch| {
                    let sorted = topo_sort_events(batch).unwrap();
                    std::hint::black_box(sorted.len());
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_compare, bench_layers, bench_clock, bench_toposort);
criterion_main!(benches);
