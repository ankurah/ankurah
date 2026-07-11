//! Stable entry points into the crate-internal event DAG engine for the
//! benchmark harness (workstream E). Gated behind the `bench-internals`
//! feature so it never widens the default public surface: the DAG comparison,
//! layering, and ordering primitives stay `pub(crate)` for everyone else.
//!
//! The benches live in a separate compilation unit (`core/benches/`) and thus
//! cannot reach `pub(crate)` items directly. Rather than restructure the
//! engine or scatter `pub` across it, this module offers narrow wrappers over
//! exactly the primitives the benches exercise: `compare`, the resulting
//! relation and accumulator, layer draining, and batch topological sort.

use crate::error::{MutationError, RetrievalError};
use crate::event_dag::accumulator::ComparisonResult;
use crate::event_dag::comparison::compare as compare_internal;
use crate::event_dag::layers::EventLayers;
use crate::event_dag::ordering::topo_sort_events as topo_sort_internal;
use crate::retrieval::GetEvents;
use ankurah_proto::{Attested, Clock, Event, EventId};

pub use crate::event_dag::relation::AbstractCausalRelation;
pub use crate::event_dag::DEFAULT_BUDGET;

/// Sizes of a drained layer. The internal `EventLayer` keeps its event vecs
/// `pub(crate)`; the benches only need the counts (to drive draining and to
/// keep the work from being optimized away), so we surface those rather than
/// widen `EventLayer`'s public API.
#[derive(Debug, Clone, Copy)]
pub struct BenchLayer {
    /// Events this layer would apply (not yet in the current head ancestry).
    pub to_apply: usize,
    /// Events already present in the current head ancestry.
    pub already_applied: usize,
}

/// Compare two clocks over the given event source, returning the causal
/// relation and the accumulated DAG. Mirror of the internal `compare`.
pub async fn compare<E: GetEvents>(
    event_getter: E,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<BenchComparisonResult<E>, RetrievalError> {
    let result = compare_internal(event_getter, subject, comparison, budget).await?;
    Ok(BenchComparisonResult(result))
}

/// Opaque wrapper over the crate-internal `ComparisonResult`. Exposes only the
/// relation and the layer-draining path the benches need.
pub struct BenchComparisonResult<E: GetEvents>(ComparisonResult<E>);

impl<E: GetEvents> BenchComparisonResult<E> {
    /// The causal relation verdict.
    pub fn relation(&self) -> &AbstractCausalRelation<EventId> { &self.0.relation }

    /// For a `DivergedSince` verdict, consume self to get a layer iterator
    /// seeded at the meet and partitioned against `current_head`. Returns
    /// `None` for non-divergent verdicts, matching the internal contract.
    pub fn into_layers(self, current_head: Vec<EventId>) -> Option<BenchEventLayers<E>> {
        self.0.into_layers(current_head).map(BenchEventLayers)
    }
}

/// Opaque wrapper over the crate-internal `EventLayers` async iterator.
pub struct BenchEventLayers<E: GetEvents>(EventLayers<E>);

impl<E: GetEvents> BenchEventLayers<E> {
    /// Yield the next topological layer's sizes, or `None` when drained.
    pub async fn next(&mut self) -> Result<Option<BenchLayer>, RetrievalError> {
        Ok(self.0.next().await?.map(|layer| BenchLayer { to_apply: layer.to_apply.len(), already_applied: layer.already_applied.len() }))
    }
}

/// Topologically sort a batch of events parents-first. Mirror of the internal
/// `topo_sort_events` used by the ingress applier.
pub fn topo_sort_events(events: Vec<Attested<Event>>) -> Result<Vec<Attested<Event>>, MutationError> { topo_sort_internal(events) }
