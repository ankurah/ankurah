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
//! results are reproducible across runs and machines.

use std::collections::HashMap;

use ankurah_core::bench_support::compare;
use ankurah_core::error::RetrievalError;
use ankurah_core::retrieval::GetEvents;
use ankurah_proto::{Clock, EntityId, Event, EventId, OperationSet};
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;

/// In-memory event source: the whole DAG lives in a `HashMap`, so `compare`
/// exercises only the traversal/accumulation logic, never storage IO.
#[derive(Clone)]
struct MemRetriever {
    events: HashMap<EventId, Event>,
}

impl MemRetriever {
    fn new() -> Self { Self { events: HashMap::new() } }
    fn add(&mut self, event: Event) { self.events.insert(event.id(), event); }
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
/// remain stable across runs.
fn event(seed: u16, parents: &[EventId]) -> Event {
    let mut entity_id_bytes = [0u8; 16];
    entity_id_bytes[0..2].copy_from_slice(&seed.to_be_bytes());
    Event {
        entity_id: EntityId::from_bytes(entity_id_bytes),
        collection: "bench".into(),
        parent: Clock::from(parents.to_vec()),
        operations: OperationSet(BTreeMap::new()),
    }
}

/// Build a linear chain of `depth` events; return the retriever and the head id.
fn linear_chain(depth: usize) -> (MemRetriever, EventId) {
    let mut r = MemRetriever::new();
    let mut prev: Vec<EventId> = vec![];
    let mut head = None;
    for i in 0..depth {
        let ev = event(i as u16, &prev);
        let id = ev.id();
        r.add(ev);
        prev = vec![id.clone()];
        head = Some(id);
    }
    (r, head.expect("depth >= 1"))
}

fn bench_compare_linear(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let (retriever, head) = linear_chain(100);
    let root = retriever.events.values().find(|e| e.parent.is_empty()).map(|e| e.id()).expect("chain has a root");
    let subject = Clock::from(vec![head]);
    let comparison = Clock::from(vec![root]);

    c.bench_function("compare/linear_deep/100", |b| {
        b.to_async(&rt).iter(|| async {
            let result = compare(retriever.clone(), &subject, &comparison, 1000).await.unwrap();
            std::hint::black_box(result.relation());
        });
    });
}

criterion_group!(benches, bench_compare_linear);
criterion_main!(benches);
