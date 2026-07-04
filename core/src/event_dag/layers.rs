//! Causal relation types for event layer comparison.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CausalRelation {
    Descends,
    Ascends,
    Concurrent,
}
