use std::collections::BTreeSet;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AbstractCausalRelation<Id> {
    Equal,

    /// This means there exists a path in the DAG from each event in comparison to every event in subject
    StrictDescends,

    /// subject does not descend from comparison, but there exists a path to a common ancestor
    NotDescends {
        /// The greatest lower bound of the two sets
        meet: Vec<Id>,
    },

    /// subject and comparison conclusively have no common ancestor whatsoever
    Incomparable,

    /// subject partially descends from comparison (some but not all events in comparison are in subject's history)
    /// This means there exists a path from some events in comparison to some (but not all) events in subject
    PartiallyDescends {
        /// The greatest lower bound of the two sets
        meet: Vec<Id>,
        // LATER: The immediate descendents of the common ancestor that are in b's history but not in a's history
        // difference: Vec<Id>,
    },

    /// Recursion budget was exceeded before a determination could be made
    BudgetExceeded {
        // what exactly do we need to resume the comparison?
        subject_frontier: BTreeSet<Id>,
        other_frontier: BTreeSet<Id>,
    },
}
