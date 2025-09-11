use ankql::ast::Predicate;
use ankurah_core::property::PropertyValue;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// The fields and their directions in the index structure
    pub index_fields: Vec<IndexField>,
    /// Direction to scan the index (forward/backward)
    pub scan_direction: ScanDirection,
    /// Range bounds for the index scan
    pub range: Range,
    /// Original predicate minus consumed conjuncts
    pub remaining_predicate: ankql::ast::Predicate,
    /// True if ORDER BY doesn't match index (future-proofing)
    pub requires_sort: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexField {
    /// Field name
    pub name: String,
    /// Direction of this field in the index structure
    pub direction: IndexDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScanDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    pub from: Bound,
    pub to: Bound,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    Unbounded,
    Inclusive(Vec<PropertyValue>),
    Exclusive(Vec<PropertyValue>),
}

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Whether the storage backend supports descending indexes
    /// false for IndexedDB, true for engines with real DESC indexes
    pub supports_desc_indexes: bool,
}

impl PlannerConfig {
    pub fn new(supports_desc_indexes: bool) -> Self { Self { supports_desc_indexes } }

    /// IndexedDB configuration
    pub fn indexeddb() -> Self { Self::new(false) }

    /// Generic storage with full index support
    pub fn full_support() -> Self { Self::new(true) }
}

impl IndexField {
    pub fn new(name: String, direction: IndexDirection) -> Self { Self { name, direction } }
}

impl Range {
    pub fn new(from: Bound, to: Bound) -> Self { Self { from, to } }
    pub fn exact(values: Vec<PropertyValue>) -> Self { Self { from: Bound::Inclusive(values.clone()), to: Bound::Inclusive(values) } }
}

pub struct Planner {
    config: PlannerConfig,
}

impl Planner {
    pub fn new(config: PlannerConfig) -> Self { Self { config } }

    /// Generate all possible index scan plans for a query
    ///
    /// Input: Selection with predicate already containing __collection = collection_id
    /// Output: Vector of all viable plans
    pub fn plan(&self, _selection: &ankql::ast::Selection) -> Vec<Plan> {
        // TODO: Implement actual planning logic
        // For now, return empty vec so tests can run
        // see planner_tasks.md for more details
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core::property::PropertyValue;
    use ankurah_derive::selection;

    macro_rules! plan {
        ($($selection:tt)*) => {{
            let selection = selection!($($selection)*);
            let planner = Planner::new(PlannerConfig::indexeddb());
            planner.plan(&selection)
        }};
    }
    macro_rules! asc {
        ($name:expr) => {
            IndexField::new($name.to_string(), IndexDirection::Asc)
        };
    }

    macro_rules! incl {
        ($($val:expr),*) => {
            Bound::Inclusive(vec![$(PropertyValue::from($val)),*])
        };
    }
    macro_rules! excl {
        ($($val:expr),*) => {
            Bound::Exclusive(vec![$(PropertyValue::from($val)),*])
        };
    }
    macro_rules! range_exact {
        ($($val:expr),*) => {
            Range::exact(vec![$(PropertyValue::from($val)),*])
        };
    }

    // Test cases for ORDER BY scenarios
    mod order_by_tests {
        use super::*;

        #[test]
        fn basic_order_by() {
            assert_eq!(
                plan!("__collection = 'album' ORDER BY foo, bar"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("foo"), asc!("bar")],
                    scan_direction: ScanDirection::Asc,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }

        #[test]
        fn order_by_with_covered_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND foo > 10 ORDER BY foo, bar"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("foo"), asc!("bar")],
                    scan_direction: ScanDirection::Asc,
                    range: Range::new(excl!["album", 10], Bound::Unbounded),
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }

        #[test]
        fn test_order_by_with_equality_and_different_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND age = 30 ORDER BY foo, bar"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("age"), asc!("foo"), asc!("bar")],
                    scan_direction: ScanDirection::Asc,
                    range: range_exact!["album", 30],
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }
    }

    // Test cases for inequality scenarios
    mod inequality_tests {
        use super::*;

        #[test]
        fn test_single_inequality_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND age > 25"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("age")],
                    scan_direction: ScanDirection::Asc,
                    range: Range::new(excl!["album", 25], Bound::Unbounded),
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }

        #[test]
        fn test_multiple_inequalities_same_field_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND age < 50"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("age")],
                    scan_direction: ScanDirection::Asc,
                    range: Range::new(excl!["album", 25], excl!["album", 50]),
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }

        #[test]
        fn test_multiple_inequalities_different_fields_plan_structures() {
            // This should generate TWO plans (one for each inequality field)
            let plans = plan!("__collection = 'album' AND age > 25 AND score < 100");

            // Plan 1: Uses age index, score remains in predicate
            assert_eq!(
                Plan {
                    index_fields: vec![asc!("__collection"), asc!("age")],
                    scan_direction: ScanDirection::Asc,
                    range: Range::new(excl!["album", 25], Bound::Unbounded),
                    remaining_predicate: selection!("score < 100").predicate,
                    requires_sort: false
                },
                Plan {
                    index_fields: vec![asc!("__collection"), asc!("score")],
                    scan_direction: ScanDirection::Asc,
                    range: Range::new(incl!["album"], excl!["album", 100]), // or do we have to specify incl!["album", 0]? (which is dependent on the field type. signed/unsigned/string)
                    remaining_predicate: selection!("age > 25").predicate,
                    requires_sort: false
                }
            );
        }
    }

    // Test cases for equality scenarios
    mod equality_tests {
        use super::*;

        #[test]
        fn test_single_equality_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND name = 'Alice'"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("name")],
                    scan_direction: ScanDirection::Asc,
                    range: range_exact!["album", "Alice"],
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }

        #[test]
        fn test_multiple_equalities_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND name = 'Alice' AND age = 30"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("name"), asc!("age")],
                    scan_direction: ScanDirection::Asc,
                    range: range_exact!["album", "Alice", 30],
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }
    }

    // Test cases for mixed scenarios
    mod mixed_tests {
        use super::*;

        #[test]
        fn test_equality_with_inequality_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND name = 'Alice' AND age > 25"),
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("name"), asc!("age")],
                    scan_direction: ScanDirection::Asc,
                    range: Range::new(excl!["album", "Alice", 25], Bound::Unbounded),
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }

        #[test]
        fn test_equality_with_order_by_and_matching_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND score > 50 AND age = 30 ORDER BY score"), // inequality intentionally in the middle to test index_field sequencing
                vec![Plan {
                    index_fields: vec![asc!("__collection"), asc!("age"), asc!("score")], // equalities first, then inequalities, preserving order of appearance
                    scan_direction: ScanDirection::Asc,
                    range: Range::new(excl!["album", "Alice", 50], Bound::Unbounded),
                    remaining_predicate: Predicate::True,
                    requires_sort: false
                }]
            );
        }
    }
}
