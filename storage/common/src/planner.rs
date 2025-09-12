use crate::{
    index_spec::{IndexDirection, IndexField, IndexSpec},
    predicate::ConjunctFinder,
};
use ankql::ast::{ComparisonOperator, Expr, Identifier, Predicate};
use ankurah_core::property::PropertyValue;
use indexmap::IndexMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// The index specification for this plan
    pub index_spec: IndexSpec,
    /// Direction to scan the index (forward/backward)
    pub scan_direction: ScanDirection,
    /// Range bounds for the index scan
    pub range: Range,
    /// Original predicate minus consumed conjuncts
    pub remaining_predicate: ankql::ast::Predicate,
    /// ORDER BY fields that require in-memory sorting after index scan
    pub sort_fields: Vec<ankql::ast::OrderByItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    Forward,
    Reverse,
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
    pub fn new<T: Into<String>>(name: T, direction: IndexDirection) -> Self { Self { name: name.into(), direction } }
}

impl IndexSpec {
    /// Create a new IndexSpec with the given fields
    pub fn new(fields: Vec<IndexField>) -> Self { Self { fields } }

    /// Generate index name with custom prefix and delimiter
    pub fn name(&self, prefix: &str, delimiter: &str) -> String {
        let field_names: Vec<_> = self.fields.iter().map(|f| f.name.as_str()).collect();
        if prefix.is_empty() { field_names.join(delimiter) } else { format!("{}{}{}", prefix, delimiter, field_names.join(delimiter)) }
    }

    /// Get the index fields
    pub fn fields(&self) -> &[IndexField] { &self.fields }
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
    /// Input: Selection with predicate
    /// Output: Vector of all viable plans
    pub fn plan(&self, selection: &ankql::ast::Selection) -> Vec<Plan> {
        let conjuncts = ConjunctFinder::find(&selection.predicate);

        // Separate conjuncts into equalities and inequalities
        let (equalities, inequalities) = self.categorize_conjuncts(&conjuncts);

        let mut plans = Vec::new();

        // Generate ORDER BY plan if present
        if let Some(order_by) = &selection.order_by {
            if let Some(plan) = self.generate_order_by_plan(&equalities, &inequalities, order_by, &conjuncts) {
                plans.push(plan);
                return plans; // ORDER BY plan is the primary plan
            }
        }

        // If we have inequalities, generate plans for each inequality field
        if !inequalities.is_empty() {
            // IndexMap preserves insertion order, so iterate directly
            for (field, _) in &inequalities {
                if let Some(plan) = self.generate_inequality_plan(&equalities, field, &inequalities, &conjuncts) {
                    plans.push(plan);
                }
            }
        } else if !equalities.is_empty() {
            // Generate equality-only plan if we have equalities but no inequalities
            if let Some(plan) = self.generate_equality_plan(&equalities, &conjuncts) {
                plans.push(plan);
            }
        }

        // Deduplicate plans based on index_fields and scan_direction
        self.deduplicate_plans(plans)
    }

    /// Categorize conjuncts into equalities and inequalities
    fn categorize_conjuncts(
        &self,
        conjuncts: &[Predicate],
    ) -> (Vec<(String, PropertyValue)>, IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>>) {
        let mut equalities = Vec::new();
        let mut inequalities: IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>> = IndexMap::new();

        for conjunct in conjuncts {
            if let Some((field, op, value)) = self.extract_comparison(conjunct) {
                match op {
                    ComparisonOperator::Equal => {
                        equalities.push((field, value));
                    }
                    ComparisonOperator::GreaterThan
                    | ComparisonOperator::GreaterThanOrEqual
                    | ComparisonOperator::LessThan
                    | ComparisonOperator::LessThanOrEqual => {
                        inequalities.entry(field).or_insert_with(Vec::new).push((op, value));
                    }
                    _ => {
                        // NotEqual, In, Between - not supported for index ranges
                        // These remain in the remaining_predicate
                    }
                }
            }
        }

        (equalities, inequalities)
    }

    /// Extract field name, operator, and value from a comparison predicate
    fn extract_comparison(&self, predicate: &Predicate) -> Option<(String, ComparisonOperator, PropertyValue)> {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                // Extract field name from left side
                let field_name = match left.as_ref() {
                    Expr::Identifier(Identifier::Property(name)) => name.clone(),
                    _ => return None,
                };

                // Extract value from right side
                let value = match right.as_ref() {
                    Expr::Literal(literal) => self.literal_to_property_value(literal)?,
                    _ => return None,
                };

                Some((field_name, operator.clone(), value))
            }
            _ => None,
        }
    }

    /// Convert AST literal to PropertyValue
    fn literal_to_property_value(&self, literal: &ankql::ast::Literal) -> Option<PropertyValue> {
        match literal {
            ankql::ast::Literal::String(s) => Some(PropertyValue::from(s.clone())),
            ankql::ast::Literal::Integer(i) => {
                // Convert i64 to i32 if it fits, otherwise use i64
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    Some(PropertyValue::from(*i as i32))
                } else {
                    Some(PropertyValue::from(*i))
                }
            }
            ankql::ast::Literal::Float(f) => Some(PropertyValue::from(*f as i32)), // Convert f64 to i32
            ankql::ast::Literal::Boolean(b) => Some(PropertyValue::from(*b)),
        }
    }

    /// Generate plan for ORDER BY queries
    fn generate_order_by_plan(
        &self,
        equalities: &[(String, PropertyValue)],
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>>,
        order_by: &[ankql::ast::OrderByItem],
        conjuncts: &[Predicate],
    ) -> Option<Plan> {
        if order_by.is_empty() {
            return None;
        }

        // Build index fields: [...equalities, inequality_if_matches_first_orderby, ...order_by_fields]
        let mut index_fields = Vec::new();
        let mut sort_fields = Vec::new();

        // Add equality fields first
        for (field, _) in equalities {
            index_fields.push(IndexField::new(field.clone(), IndexDirection::Asc));
        }

        // Check if first ORDER BY field has an inequality - if so, add it before other ORDER BY fields
        let first_order_field = match &order_by[0].identifier {
            Identifier::Property(name) => name,
            _ => return None,
        };
        let has_inequality_on_first_order = inequalities.contains_key(first_order_field);

        if has_inequality_on_first_order {
            // Add the inequality field (it's the same as first ORDER BY field)
            index_fields.push(IndexField::new(first_order_field.clone(), IndexDirection::Asc));
        }

        // Process ORDER BY fields, handling direction changes
        if self.config.supports_desc_indexes {
            // Full support: can handle mixed directions in index
            if has_inequality_on_first_order {
                // Skip first field since it's already added as inequality field
                for item in &order_by[1..] {
                    if let Identifier::Property(name) = &item.identifier {
                        let direction = match item.direction {
                            ankql::ast::OrderDirection::Asc => IndexDirection::Asc,
                            ankql::ast::OrderDirection::Desc => IndexDirection::Desc,
                        };
                        index_fields.push(IndexField::new(name.clone(), direction));
                    }
                }
            } else {
                // Add all ORDER BY fields
                for item in order_by {
                    if let Identifier::Property(name) = &item.identifier {
                        let direction = match item.direction {
                            ankql::ast::OrderDirection::Asc => IndexDirection::Asc,
                            ankql::ast::OrderDirection::Desc => IndexDirection::Desc,
                        };
                        index_fields.push(IndexField::new(name.clone(), direction));
                    }
                }
            }
        } else {
            // IndexedDB: only ASC indexes, stop at first direction change
            let current_direction = order_by[0].direction.clone();
            let mut order_by_idx = if has_inequality_on_first_order { 1 } else { 0 };

            // If we don't have an inequality on first order field, add the first ORDER BY field
            if !has_inequality_on_first_order {
                if let Identifier::Property(name) = &order_by[0].identifier {
                    index_fields.push(IndexField::new(name.clone(), IndexDirection::Asc));
                    order_by_idx = 1;
                }
            }

            // Process remaining ORDER BY fields until direction changes
            for (i, item) in order_by[order_by_idx..].iter().enumerate() {
                if let Identifier::Property(name) = &item.identifier {
                    if item.direction == current_direction {
                        // Same direction, add to index
                        index_fields.push(IndexField::new(name.clone(), IndexDirection::Asc));
                    } else {
                        // Direction changed, add this and all remaining fields to sort_fields
                        for remaining_item in &order_by[order_by_idx + i..] {
                            sort_fields.push(remaining_item.clone());
                        }
                        break;
                    }
                }
            }
        }

        // Build range
        let range = self.build_range(
            equalities,
            if has_inequality_on_first_order { Some((first_order_field, inequalities.get(first_order_field)?)) } else { None },
        );

        // Calculate remaining predicate
        let remaining_predicate = self.calculate_remaining_predicate(
            conjuncts,
            equalities,
            if has_inequality_on_first_order { Some(first_order_field) } else { None },
        );

        // Determine scan direction
        let scan_direction = if self.config.supports_desc_indexes {
            // With full support, always scan forward (direction is in index)
            ScanDirection::Forward
        } else {
            // For IndexedDB, scan backwards if ORDER BY starts with DESC
            match order_by[0].direction {
                ankql::ast::OrderDirection::Desc => ScanDirection::Reverse,
                ankql::ast::OrderDirection::Asc => ScanDirection::Forward,
            }
        };

        let index_spec = IndexSpec::new(index_fields);
        Some(Plan { index_spec, scan_direction, range, remaining_predicate, sort_fields })
    }

    /// Generate plan for inequality-based queries
    fn generate_inequality_plan(
        &self,
        equalities: &[(String, PropertyValue)],
        inequality_field: &str,
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>>,
        conjuncts: &[Predicate],
    ) -> Option<Plan> {
        // Add equality fields first
        let mut index_fields = Vec::new();
        for (field, _) in equalities {
            index_fields.push(IndexField::new(field.clone(), IndexDirection::Asc));
        }

        // Add the inequality field
        index_fields.push(IndexField::new(inequality_field.to_string(), IndexDirection::Asc));

        // Build range
        let range = self.build_range(equalities, Some((inequality_field, inequalities.get(inequality_field)?)));

        // Calculate remaining predicate (exclude this inequality field)
        let remaining_predicate = self.calculate_remaining_predicate(conjuncts, equalities, Some(inequality_field));

        let index_spec = IndexSpec::new(index_fields);
        Some(Plan { index_spec, scan_direction: ScanDirection::Forward, range, remaining_predicate, sort_fields: vec![] })
    }

    /// Generate plan for equality-only queries
    fn generate_equality_plan(&self, equalities: &[(String, PropertyValue)], conjuncts: &[Predicate]) -> Option<Plan> {
        // Add all equality fields
        let mut index_fields = Vec::new();
        for (field, _) in equalities {
            index_fields.push(IndexField::new(field.clone(), IndexDirection::Asc));
        }

        // Build range (exact match on all equality values)
        let range = self.build_range(equalities, None);

        // Calculate remaining predicate
        let remaining_predicate = self.calculate_remaining_predicate(conjuncts, equalities, None);

        let index_spec = IndexSpec::new(index_fields);
        Some(Plan { index_spec, scan_direction: ScanDirection::Forward, range, remaining_predicate, sort_fields: vec![] })
    }

    /// Build range based on equalities and optional inequality
    fn build_range(
        &self,
        equalities: &[(String, PropertyValue)],
        inequality: Option<(&str, &Vec<(ComparisonOperator, PropertyValue)>)>,
    ) -> Range {
        let mut from_values = Vec::new();
        let mut to_values = Vec::new();

        // Add equality values to both bounds
        for (_, value) in equalities {
            from_values.push(value.clone());
            to_values.push(value.clone());
        }

        if let Some((_, inequalities)) = inequality {
            // Handle multiple inequalities on the same field
            // Currently uses last inequality for each direction - TODO: implement most restrictive selection
            let mut from_bound = Bound::Inclusive(from_values.clone()); // Default to equality prefix
            let mut to_bound = Bound::Inclusive(to_values.clone()); // Default to equality prefix

            for (op, value) in inequalities {
                match op {
                    ComparisonOperator::GreaterThan => {
                        let mut bound_values = from_values.clone();
                        bound_values.push(value.clone());
                        from_bound = Bound::Exclusive(bound_values);
                    }
                    ComparisonOperator::GreaterThanOrEqual => {
                        let mut bound_values = from_values.clone();
                        bound_values.push(value.clone());
                        from_bound = Bound::Inclusive(bound_values);
                    }
                    ComparisonOperator::LessThan => {
                        let mut bound_values = to_values.clone();
                        bound_values.push(value.clone());
                        to_bound = Bound::Exclusive(bound_values);
                    }
                    ComparisonOperator::LessThanOrEqual => {
                        let mut bound_values = to_values.clone();
                        bound_values.push(value.clone());
                        to_bound = Bound::Inclusive(bound_values);
                    }
                    _ => {}
                }
            }

            Range::new(from_bound, to_bound)
        } else {
            // Exact match on equality values
            Range::exact(from_values)
        }
    }

    /// Calculate remaining predicate by removing consumed conjuncts
    fn calculate_remaining_predicate(
        &self,
        conjuncts: &[Predicate],
        consumed_equalities: &[(String, PropertyValue)],
        consumed_inequality_field: Option<&str>,
    ) -> Predicate {
        let mut remaining_conjuncts = Vec::new();

        for conjunct in conjuncts {
            let mut consumed = false;

            // Check if this conjunct is consumed by equalities
            if let Some((field, _, _)) = self.extract_comparison(conjunct) {
                // Check if it's a consumed equality
                for (eq_field, _) in consumed_equalities {
                    if field == *eq_field {
                        consumed = true;
                        break;
                    }
                }

                // Check if it's a consumed inequality
                if !consumed {
                    if let Some(ineq_field) = consumed_inequality_field {
                        if field == ineq_field {
                            consumed = true;
                        }
                    }
                }
            }

            if !consumed {
                remaining_conjuncts.push(conjunct.clone());
            }
        }

        // Combine remaining conjuncts with AND
        if remaining_conjuncts.is_empty() {
            Predicate::True
        } else if remaining_conjuncts.len() == 1 {
            remaining_conjuncts.into_iter().next().unwrap()
        } else {
            // Build AND chain
            let mut result = remaining_conjuncts[0].clone();
            for conjunct in remaining_conjuncts.into_iter().skip(1) {
                result = Predicate::And(Box::new(result), Box::new(conjunct));
            }
            result
        }
    }

    /// Deduplicate plans based on index_fields and scan_direction
    fn deduplicate_plans(&self, plans: Vec<Plan>) -> Vec<Plan> {
        let mut unique_plans = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for plan in plans {
            // Create a key that owns the data we need for comparison
            let key = (plan.index_spec.fields.clone(), plan.scan_direction.clone());
            if seen.insert(key) {
                // insert returns true if the key was not already present
                unique_plans.push(plan);
            }
        }

        unique_plans
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core::property::PropertyValue;
    use ankurah_derive::selection;

    // FIX_ME: rename to plan_indexeddb
    macro_rules! plan {
        ($($selection:tt)*) => {{
            let selection = selection!($($selection)*);
            let planner = Planner::new(PlannerConfig::indexeddb());
            planner.plan(&selection)
        }};
    }
    macro_rules! plan_full_support {
        ($($selection:tt)*) => {{
            let selection = selection!($($selection)*);
            let planner = Planner::new(PlannerConfig::full_support());
            planner.plan(&selection)
        }};
    }
    macro_rules! asc {
        ($name:expr) => {
            IndexField::new($name.to_string(), IndexDirection::Asc)
        };
    }
    macro_rules! desc {
        ($name:expr) => {
            IndexField::new($name.to_string(), IndexDirection::Desc)
        };
    }
    macro_rules! oby_asc {
        ($name:expr) => {
            ankql::ast::OrderByItem {
                identifier: ankql::ast::Identifier::Property($name.to_string()),
                direction: ankql::ast::OrderDirection::Asc,
            }
        };
    }
    macro_rules! oby_desc {
        ($name:expr) => {
            ankql::ast::OrderByItem {
                identifier: ankql::ast::Identifier::Property($name.to_string()),
                direction: ankql::ast::OrderDirection::Desc,
            }
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
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn order_by_with_covered_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND foo > 10 ORDER BY foo, bar"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (foo) is > 10
                    // to is incl because there is no foo < ? in the predicate
                    range: Range::new(excl!["album", 10], incl!["album"]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn no_collection_field() {
            // the __collection field is not special for the planner
            assert_eq!(
                plan!("age = 30 ORDER BY foo, bar"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("age"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact![30],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        // was incorrectly named before
        #[test]
        fn test_order_by_with_equality() {
            assert_eq!(
                plan!("__collection = 'album' AND age = 30 ORDER BY foo, bar"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", 30],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_desc_single_field() {
            // Single DESC field - should scan backwards with ASC index
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Reverse,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_all_desc() {
            // All DESC fields - should scan backwards with ASC indexes
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC, year DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("year")]),
                    scan_direction: ScanDirection::Reverse,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_mixed_directions_asc_first() {
            // Mixed directions starting with ASC - should stop at first DESC, require sort
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name ASC, year DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![oby_desc!("year")]
                }]
            );
        }

        #[test]
        fn test_order_by_mixed_directions_desc_first() {
            // Mixed directions starting with DESC - should stop at first ASC, require sort
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC, year ASC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Reverse,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![oby_asc!("year")]
                }]
            );
        }

        #[test]
        fn test_order_by_mixed_three_fields() {
            // Three fields: ASC, DESC, DESC - should only include first field
            assert_eq!(
                plan!("__collection = 'album' ORDER BY foo ASC, bar DESC, baz DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("foo")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![oby_desc!("bar"), oby_desc!("baz")]
                }]
            );
        }

        #[test]
        fn test_order_by_with_equality_and_desc() {
            // Equality + DESC ORDER BY
            assert_eq!(
                plan!("__collection = 'album' AND status = 'active' ORDER BY name DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("status"), asc!("name")]),
                    scan_direction: ScanDirection::Reverse,
                    range: range_exact!["album", "active"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_with_inequality_and_desc() {
            // Inequality on ORDER BY field + DESC
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 ORDER BY age DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Reverse,
                    // from is excl because the inequality (age) is > 25
                    // to is incl because there is no age < ? in the predicate
                    range: Range::new(excl!["album", 25], incl!["album"]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }
    }

    // Test cases for full DESC index support (supports_desc_indexes = true)
    mod full_support_tests {
        use super::*;

        #[test]
        fn test_full_support_single_desc() {
            // With full support, DESC should create DESC index fields
            assert_eq!(
                plan_full_support!("__collection = 'album' ORDER BY name DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), desc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_full_support_mixed_directions() {
            // With full support, mixed directions should preserve all fields
            assert_eq!(
                plan_full_support!("__collection = 'album' ORDER BY name ASC, year DESC, score ASC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), desc!("year"), asc!("score")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_full_support_all_desc() {
            // With full support, all DESC should create DESC index fields
            assert_eq!(
                plan_full_support!("__collection = 'album' ORDER BY name DESC, year DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), desc!("name"), desc!("year")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_full_support_with_equality_and_mixed_order() {
            // Full support with equality and mixed ORDER BY directions
            assert_eq!(
                plan_full_support!("__collection = 'album' AND status = 'active' ORDER BY name ASC, year DESC"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("status"), asc!("name"), desc!("year")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", "active"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
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
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (age) is > 25
                    // to is incl because there is no age < ? in the predicate
                    range: Range::new(excl!["album", 25], incl!["album"]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_multiple_inequalities_same_field_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND age < 50"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (age) is > 25
                    // to is excl because the inequality (age) is < 50
                    range: Range::new(excl!["album", 25], excl!["album", 50]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_multiple_inequalities_different_fields_plan_structures() {
            // This should generate TWO plans (one for each inequality field)
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND score < 100"),
                vec![
                    // Plan 1: Uses age index, score remains in predicate
                    Plan {
                        index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (age) is > 25
                        // from is incl because there is no age < ? in the predicate
                        range: Range::new(excl!["album", 25], incl!["album"]),
                        remaining_predicate: selection!("score < 100").predicate,
                        sort_fields: vec![]
                    },
                    // Plan 2: Uses score index, age remains in predicate
                    Plan {
                        index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("score")]),
                        scan_direction: ScanDirection::Forward,
                        // from is incl because there is no score < ? in the predicate
                        // to is excl because the inequality (score) is < 100
                        range: Range::new(incl!["album"], excl!["album", 100]),
                        remaining_predicate: selection!("age > 25").predicate,
                        sort_fields: vec![]
                    }
                ]
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
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", "Alice"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_multiple_equalities_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND name = 'Alice' AND age = 30"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", "Alice", 30],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
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
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the final inequality (age) is > 25
                    // to is incl and the final component is omitted because there is no age < ? in the predicate
                    range: Range::new(excl!["album", "Alice", 25], incl!["album", "Alice"]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_equality_with_order_by_and_matching_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND score > 50 AND age = 30 ORDER BY score"), // inequality intentionally in the middle to test index_field sequencing
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age"), asc!("score")]), // equalities first, then inequalities, preserving order of appearance
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the final inequality (score) is > 50
                    // to is incl and the final component is omitted because there is no score < ? in the predicate
                    range: Range::new(excl!["album", 30, 50], incl!["album", 30]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }
    }

    // Test cases for edge cases and pathological scenarios
    mod edge_cases {
        use super::*;

        #[test]
        fn test_collection_only_query() {
            // Query with only __collection predicate
            assert_eq!(
                plan!("__collection = 'album'"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![],
                }]
            );
        }

        #[test]
        fn test_unsupported_operators() {
            // Queries with operators that can't be used for index ranges
            assert_eq!(
                plan!("__collection = 'album' AND name != 'Alice'"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    remaining_predicate: selection!("name != 'Alice'").predicate,
                    sort_fields: vec![],
                }]
            );

            // Mixed supported and unsupported
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND name != 'Alice'"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (age) is > 25
                    // to is incl and the final component is omitted because there is no age < ? in the predicate
                    range: Range::new(excl!["album", 25], incl!["album"]),
                    remaining_predicate: selection!("name != 'Alice'").predicate,
                    sort_fields: vec![],
                }]
            );
        }

        #[test]
        fn test_impossible_range() {
            // Conflicting inequalities that create impossible range
            // FIXME: determine if we want to have a no-op plan for this
            assert_eq!(
                plan!("__collection = 'album' AND age > 50 AND age < 30"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (age) is > 50
                    // to is excl because the inequality (age) is < 30
                    range: Range::new(excl!["album", 50], excl!["album", 30]), // Impossible but we generate it anyway
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![],
                }]
            );
        }

        #[test]
        fn test_or_only_predicate() {
            // Predicate with only OR - __collection conjunct should be extractable
            assert_eq!(
                plan!("__collection = 'album' AND (age > 25 OR name = 'Alice')"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album"],
                    // the OR-containing parenthetical is a disjunction, so it must remain in the predicate
                    remaining_predicate: selection!("age > 25 OR name = 'Alice'").predicate,
                    sort_fields: vec![],
                }]
            );
        }

        #[test]
        fn test_complex_nested_predicate() {
            // Complex nesting that should still extract some conjuncts
            assert_eq!(
                plan!("__collection = 'album' AND score = 100 AND (age > 25 OR name = 'Alice')"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("score")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", 100],
                    // the OR-containing parenthetical is a disjunction, so it must remain in the predicate
                    remaining_predicate: selection!("age > 25 OR name = 'Alice'").predicate,
                    sort_fields: vec![],
                }]
            );
        }

        #[test]
        fn test_order_by_with_no_matching_predicate() {
            // This might already be covered above
            // ORDER BY field that doesn't appear in WHERE clause
            assert_eq!(
                plan!("__collection = 'album' AND age = 30 ORDER BY name, score"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age"), asc!("name"), asc!("score")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", 30],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![],
                }]
            );
        }

        #[test]
        fn test_multiple_inequalities_same_field_complex() {
            // Multiple inequalities on same field with different operators
            // Current implementation: last inequality wins for each direction
            // TODO: Should ideally choose most restrictive bounds (>= 25 over > 20)
            assert_eq!(
                plan!("__collection = 'album' AND age >= 25 AND age <= 50 AND age > 20"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // age > 20 wins over age >= 25 because it comes last (last wins behavior)
                    // from: is excl because the final lower bound inequality (age) is > 20
                    // to: is incl because the upper bound inequality (age) is <= 50
                    range: Range::new(excl!["album", 20], incl!["album", 50]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![],
                }]
            );
        }

        #[test]
        fn test_large_numbers() {
            // Test with very large numbers
            assert_eq!(
                plan!("__collection = 'album' AND timestamp > 9223372036854775807"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("timestamp")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (timestamp) is > 9223372036854775807
                    // to is incl because there is no timestamp < ? in the predicate
                    range: Range::new(excl!["album", 9223372036854775807i64], incl!["album"]),
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_empty_string_equality() {
            // Test that empty strings are handled correctly in equality comparisons
            assert_eq!(
                plan!("__collection = 'album' AND name = ''"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", ""],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }

        #[test]
        fn test_empty_string_with_other_fields() {
            // Test empty string with other fields
            assert_eq!(
                plan!("__collection = 'album' AND name = '' AND year = '2000'"),
                vec![Plan {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("year")]),
                    scan_direction: ScanDirection::Forward,
                    range: range_exact!["album", "", "2000"],
                    remaining_predicate: Predicate::True,
                    sort_fields: vec![]
                }]
            );
        }
    }
}
