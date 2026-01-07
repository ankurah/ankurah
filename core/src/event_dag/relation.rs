use std::collections::BTreeSet;

/// Causal relationship between two clocks in the event DAG.
/// Aligned with proto::CausalRelation for wire compatibility.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AbstractCausalRelation<Id> {
    /// Identical lattice points.
    Equal,

    /// Subject strictly after other: Past(subject) ⊃ Past(other).
    /// Action: apply subject's operations (replay the chain).
    StrictDescends {
        /// Forward chain from other head to subject head (for replay).
        /// Events are in causal order: oldest first, newest last.
        chain: Vec<Id>,
    },

    /// Subject strictly before other: Past(subject) ⊂ Past(other).
    /// Action: no-op (incoming event is older than current state).
    StrictAscends,

    /// Both sides have advanced since the meet (GCA).
    /// This is true concurrency requiring merge.
    DivergedSince {
        /// Greatest common ancestor frontier.
        meet: Vec<Id>,
        /// Immediate children of meet toward subject.
        subject: Vec<Id>,
        /// Immediate children of meet toward other.
        other: Vec<Id>,
        /// Full forward chain from meet to subject tip (for merge).
        /// Events are in causal order: oldest first, newest last.
        subject_chain: Vec<Id>,
        /// Full forward chain from meet to other tip (for merge).
        /// Events are in causal order: oldest first, newest last.
        other_chain: Vec<Id>,
    },

    /// Proven different genesis events (single-root invariant violated).
    /// Action: reject per policy.
    Disjoint {
        /// Optional non-minimal common ancestors (if any were found).
        gca: Option<Vec<Id>>,
        /// Proven genesis of subject.
        subject_root: Id,
        /// Proven genesis of other.
        other_root: Id,
    },

    /// Recursion budget was exceeded before a determination could be made.
    /// Contains frontiers to resume later.
    BudgetExceeded { subject: BTreeSet<Id>, other: BTreeSet<Id> },
}
