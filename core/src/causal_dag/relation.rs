use std::collections::BTreeSet;

/// Causal relation between two clocks: `subject` vs `other`.
///
/// This is the in-memory working type for lineage comparison within ankurah-core.
/// It parallels `ankurah_proto::CausalRelation` (the wire protocol type) but must remain
/// distinct because:
/// - This type uses generic `Id` for testing with mock event stores
/// - The proto type uses concrete `EventId` and `Clock` for wire serialization
/// - Conversion methods (From/Into) bridge between them
///
/// The structures should generally match to enable clean conversions.
#[derive(Debug, PartialEq, Eq)]
pub enum CausalRelation<Id> {
    /// Clocks are identical
    Equal,

    /// Subject strictly descends from comparison (linear or branching history, all comparison events are ancestors)
    StrictDescends,

    /// Subject strictly ascends from comparison (comparison is newer, subject is older)
    StrictAscends,

    /// True concurrency: both sides have diverged from a common ancestor
    /// Both have events the other doesn't, requiring conflict resolution
    DivergedSince {
        /// GCA (greatest common ancestor) frontier where lineages diverged
        meet: Vec<Id>,
        /// Subject frontier after the meet
        subject: Vec<Id>,
        /// Other frontier after the meet  
        other: Vec<Id>,
    },

    /// No common ancestor whatsoever (different genesis events or empty clocks)
    /// Empty clocks (like SQL NULL) are always Disjoint from everything, including other empty clocks.
    Disjoint {
        /// Optional non-minimal common ancestors (if any were found during traversal)
        gca: Option<Vec<Id>>,
        /// Proven genesis of subject (None if subject is empty clock)
        subject_root: Option<Id>,
        /// Proven genesis of other (None if other is empty clock)
        other_root: Option<Id>,
    },

    /// Recursion budget was exceeded before a determination could be made
    BudgetExceeded {
        /// Current subject frontier for resumption
        subject: BTreeSet<Id>,
        /// Current other frontier for resumption
        other: BTreeSet<Id>,
    },
}
