use crate::{
    index_spec::{IndexKeyPart, IndexSpec},
    predicate::ConjunctFinder,
    types::*,
};
use ankql::ast::{ComparisonOperator, Expr, Identifier, Predicate};
use ankurah_core::property::PropertyValue;
use indexmap::IndexMap;

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

        // New ORDER BY strategies
        if let Some(order_by) = &selection.order_by {
            if !order_by.is_empty() {
                if let Some(plan) = self.build_order_first_plan(&equalities, &inequalities, order_by, &conjuncts) {
                    plans.push(plan);
                }
                // If an ORDER BY field has inequalities (covered inequality), do NOT emit INEQ-FIRST
                let covered_ineq = order_by.iter().any(|item| match &item.identifier {
                    Identifier::Property(name) => inequalities.contains_key(name),
                    _ => false,
                });
                if !covered_ineq && !inequalities.is_empty() {
                    if let Some(plan) = self.build_ineq_first_plan(&equalities, &inequalities, order_by, &conjuncts) {
                        plans.push(plan);
                    }
                }
                return self.deduplicate_plans(plans);
            }
        }

        // If we have inequalities, generate plans for each inequality field
        if !inequalities.is_empty() {
            // IndexMap preserves insertion order, so iterate directly
            for (field, _) in &inequalities {
                if let Some(plan) = self.generate_inequality_plan_with_order_by(
                    &equalities,
                    field,
                    &inequalities,
                    &conjuncts,
                    selection.order_by.as_deref(),
                ) {
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

    // ORDER-FIRST: [EQ …] + maximal OB prefix (capability-aware). Bounds: EQ only.
    fn build_order_first_plan(
        &self,
        equalities: &[(String, PropertyValue)],
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>>,
        order_by: &[ankql::ast::OrderByItem],
        conjuncts: &[Predicate],
    ) -> Option<Plan> {
        if order_by.is_empty() {
            return None;
        }

        // Keyparts: EQ prefix
        let mut index_keyparts: Vec<IndexKeyPart> = equalities.iter().map(|(f, _)| IndexKeyPart::asc(f.clone())).collect();

        // Append ORDER BY fields per capability
        if self.config.supports_desc_indexes {
            for item in order_by {
                if let Identifier::Property(name) = &item.identifier {
                    index_keyparts.push(match item.direction {
                        ankql::ast::OrderDirection::Asc => IndexKeyPart::asc(name.clone()),
                        ankql::ast::OrderDirection::Desc => IndexKeyPart::desc(name.clone()),
                    });
                }
            }
        } else {
            // IndexedDB: ASC-only index parts, keep longest same-direction prefix
            let first_dir = order_by[0].direction.clone();
            let mut broke = false;
            for item in order_by {
                if let Identifier::Property(name) = &item.identifier {
                    if !broke && item.direction == first_dir {
                        index_keyparts.push(IndexKeyPart::asc(name.clone()));
                    } else {
                        broke = true;
                    }
                }
            }
        }

        // Bounds: equalities + (optional) bounds on the first ORDER BY field that has inequalities
        let applied_ineq = order_by.iter().find_map(|item| match &item.identifier {
            Identifier::Property(name) => inequalities.get_key_value(name).map(|(k, v)| (k.as_str(), v)),
            _ => None,
        });

        let bounds = match applied_ineq {
            Some((field, vec)) => self.build_bounds(equalities, Some((field, vec)), &index_keyparts)?,
            None => self.build_bounds(equalities, None, &index_keyparts)?,
        };
        if self.is_empty_bounds(&bounds) {
            return Some(Plan::EmptyScan);
        }

        // Remaining predicate excludes the applied OB inequality if any
        let remaining_predicate = self.calculate_remaining_predicate(conjuncts, equalities, applied_ineq.map(|(f, _)| f));

        // Scan direction
        let scan_direction = if self.config.supports_desc_indexes {
            ScanDirection::Forward
        } else {
            match order_by[0].direction {
                ankql::ast::OrderDirection::Desc => ScanDirection::Reverse,
                _ => ScanDirection::Forward,
            }
        };

        // Spill: any OB not covered by index (IndexedDB mixed directions)
        let mut order_by_spill = Vec::new();
        if !self.config.supports_desc_indexes {
            let first_dir = order_by[0].direction.clone();
            let mut broke = false;
            for item in order_by {
                if let Identifier::Property(_name) = &item.identifier {
                    if !broke && item.direction == first_dir {
                        continue;
                    } else {
                        broke = true;
                        order_by_spill.push(item.clone());
                    }
                }
            }
        }

        Some(Plan::Index { index_spec: IndexSpec::new(index_keyparts), scan_direction, bounds, remaining_predicate, order_by_spill })
    }

    // INEQ-FIRST: [EQ …] + primary INEQ (bounded). Do NOT append ORDER BY columns; always spill them.
    fn build_ineq_first_plan(
        &self,
        equalities: &[(String, PropertyValue)],
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>>,
        order_by: &[ankql::ast::OrderByItem],
        conjuncts: &[Predicate],
    ) -> Option<Plan> {
        // Pick primary inequality: prefer first OB field with ineq, else first ineq in map order
        let primary = order_by
            .iter()
            .find_map(|item| match &item.identifier {
                Identifier::Property(name) => inequalities.get_key_value(name).map(|(k, v)| (k.as_str(), v)),
                _ => None,
            })
            .or_else(|| inequalities.iter().next().map(|(k, v)| (k.as_str(), v)))?;

        // Keyparts: EQ + primary INEQ (do not append ORDER BY fields; they do not satisfy global order after a range)
        // NOTE (micro-optimization): Appending OB columns after the range could help spill comparator locality,
        // but it does not change correctness and the tests expect the simpler invariant-preserving form.
        let mut index_keyparts: Vec<IndexKeyPart> = equalities.iter().map(|(f, _)| IndexKeyPart::asc(f.clone())).collect();
        index_keyparts.push(IndexKeyPart::asc(primary.0.to_string()));

        // Bounds: EQ + primary INEQ (most-restrictive)
        let bounds = self.build_bounds(equalities, Some(primary), &index_keyparts)?;
        if self.is_empty_bounds(&bounds) {
            return Some(Plan::EmptyScan);
        }

        // Remaining predicate: all inequalities except the primary one
        let remaining_predicate = self.calculate_remaining_predicate(conjuncts, equalities, Some(primary.0));

        // Scan direction
        let scan_direction = if self.config.supports_desc_indexes {
            ScanDirection::Forward
        } else {
            match order_by[0].direction {
                ankql::ast::OrderDirection::Desc => ScanDirection::Reverse,
                _ => ScanDirection::Forward,
            }
        };

        // Spill: INEQ-FIRST always spills all ORDER BY fields not in EQ/pivot
        let mut covered = std::collections::HashSet::new();
        covered.extend(equalities.iter().map(|(f, _)| f.as_str()));
        covered.insert(primary.0);
        let mut order_by_spill = Vec::new();
        for item in order_by {
            if let Identifier::Property(name) = &item.identifier {
                if !covered.contains(name.as_str()) {
                    order_by_spill.push(item.clone());
                }
            }
        }

        Some(Plan::Index { index_spec: IndexSpec::new(index_keyparts), scan_direction, bounds, remaining_predicate, order_by_spill })
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

    // removed: legacy generate_order_by_plan (superseded by ORDER-FIRST/INEQ-FIRST)

    // removed: ad hoc ORDER BY-only builder (fold into ORDER-FIRST)

    /// Generate plan for inequality-based queries
    fn generate_inequality_plan(
        &self,
        equalities: &[(String, PropertyValue)],
        inequality_field: &str,
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>>,
        conjuncts: &[Predicate],
    ) -> Option<Plan> {
        self.generate_inequality_plan_with_order_by(equalities, inequality_field, inequalities, conjuncts, None)
    }

    fn generate_inequality_plan_with_order_by(
        &self,
        equalities: &[(String, PropertyValue)],
        inequality_field: &str,
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, PropertyValue)>>,
        conjuncts: &[Predicate],
        order_by: Option<&[ankql::ast::OrderByItem]>,
    ) -> Option<Plan> {
        // Add equality fields first
        let mut index_keyparts = Vec::new();
        for (field, _) in equalities {
            index_keyparts.push(IndexKeyPart::asc(field.clone()));
        }

        // Add the inequality field
        index_keyparts.push(IndexKeyPart::asc(inequality_field.to_string()));

        // Build bounds
        let bounds = self.build_bounds(equalities, Some((inequality_field, inequalities.get(inequality_field)?)), &index_keyparts);

        // Check for empty scan
        let bounds = match bounds {
            Some(bounds) => {
                if self.is_empty_bounds(&bounds) {
                    return Some(Plan::EmptyScan);
                }
                bounds
            }
            None => return Some(Plan::EmptyScan),
        };

        // Calculate remaining predicate (exclude this inequality field)
        let remaining_predicate = self.calculate_remaining_predicate(conjuncts, equalities, Some(inequality_field));

        // Calculate order_by_spill: any ORDER BY fields not covered by the index
        let mut order_by_spill = Vec::new();
        if let Some(order_by_items) = order_by {
            let covered_fields: std::collections::HashSet<&str> =
                equalities.iter().map(|(f, _)| f.as_str()).chain(std::iter::once(inequality_field)).collect();

            for item in order_by_items {
                if let Identifier::Property(name) = &item.identifier {
                    if !covered_fields.contains(name.as_str()) {
                        order_by_spill.push(item.clone());
                    }
                }
            }
        }

        let index_spec = IndexSpec::new(index_keyparts);
        Some(Plan::Index { index_spec, scan_direction: ScanDirection::Forward, bounds, remaining_predicate, order_by_spill })
    }

    /// Generate plan for equality-only queries
    fn generate_equality_plan(&self, equalities: &[(String, PropertyValue)], conjuncts: &[Predicate]) -> Option<Plan> {
        // Add all equality fields
        let mut index_keyparts = Vec::new();
        for (field, _) in equalities {
            index_keyparts.push(IndexKeyPart::asc(field.clone()));
        }

        // Build bounds (exact match on all equality values)
        let bounds = self.build_bounds(equalities, None, &index_keyparts);

        // Check for empty scan
        let bounds = match bounds {
            Some(bounds) => {
                if self.is_empty_bounds(&bounds) {
                    return Some(Plan::EmptyScan);
                }
                bounds
            }
            None => return Some(Plan::EmptyScan),
        };

        // Calculate remaining predicate
        let remaining_predicate = self.calculate_remaining_predicate(conjuncts, equalities, None);

        let index_spec = IndexSpec::new(index_keyparts);
        Some(Plan::Index { index_spec, scan_direction: ScanDirection::Forward, bounds, remaining_predicate, order_by_spill: vec![] })
    }

    // removed: specialized ORDER BY bounds builder (use unified per-strategy bounds)

    /// Build bounds based on equalities and optional inequality
    fn build_bounds(
        &self,
        equalities: &[(String, PropertyValue)],
        inequality: Option<(&str, &Vec<(ComparisonOperator, PropertyValue)>)>,
        index_keyparts: &[IndexKeyPart],
    ) -> Option<IndexBounds> {
        let mut keypart_bounds = Vec::new();

        // Build per-column bounds
        for keypart in index_keyparts {
            let column = &keypart.column;

            // Check if this column has an equality constraint
            let equality_value = equalities.iter().find(|(field, _)| field == column).map(|(_, value)| value);

            if let Some(value) = equality_value {
                // Equality constraint: both bounds are the same value, inclusive
                keypart_bounds.push(IndexColumnBound {
                    column: column.clone(),
                    low: Endpoint::incl(value.clone()),
                    high: Endpoint::incl(value.clone()),
                });
            } else if let Some((ineq_field, inequalities)) = inequality {
                if ineq_field == column {
                    // This column has inequality constraints
                    let mut low = Endpoint::UnboundedLow(ValueType::of(&inequalities[0].1));
                    let mut high = Endpoint::UnboundedHigh(ValueType::of(&inequalities[0].1));

                    // Process all inequalities for this column, choosing most restrictive bounds
                    for (op, value) in inequalities {
                        match op {
                            ComparisonOperator::GreaterThan => {
                                let candidate = Endpoint::excl(value.clone());
                                if self.is_more_restrictive_lower(&candidate, &low) {
                                    low = candidate;
                                }
                            }
                            ComparisonOperator::GreaterThanOrEqual => {
                                let candidate = Endpoint::incl(value.clone());
                                if self.is_more_restrictive_lower(&candidate, &low) {
                                    low = candidate;
                                }
                            }
                            ComparisonOperator::LessThan => {
                                let candidate = Endpoint::excl(value.clone());
                                if self.is_more_restrictive_upper(&candidate, &high) {
                                    high = candidate;
                                }
                            }
                            ComparisonOperator::LessThanOrEqual => {
                                let candidate = Endpoint::incl(value.clone());
                                if self.is_more_restrictive_upper(&candidate, &high) {
                                    high = candidate;
                                }
                            }
                            _ => {}
                        }
                    }

                    keypart_bounds.push(IndexColumnBound { column: column.clone(), low, high });
                    break; // Stop at first inequality column
                } else {
                    // No constraint on this column - stop here, don't add unbounded bounds
                    break;
                }
            } else {
                // No more constraints - stop here, don't add unbounded bounds
                break;
            }
        }

        Some(IndexBounds::new(keypart_bounds))
    }

    /// Check if candidate lower bound is more restrictive than current
    fn is_more_restrictive_lower(&self, candidate: &Endpoint, current: &Endpoint) -> bool {
        match (candidate, current) {
            // Any concrete value is more restrictive than unbounded
            (Endpoint::Value { .. }, Endpoint::UnboundedLow(_)) => true,

            // Unbounded is never more restrictive than concrete
            (Endpoint::UnboundedLow(_), Endpoint::Value { .. }) => false,

            // Compare two concrete values
            (Endpoint::Value { datum: cand_datum, inclusive: cand_incl }, Endpoint::Value { datum: curr_datum, inclusive: curr_incl }) => {
                match (cand_datum, curr_datum) {
                    (KeyDatum::Val(cand_val), KeyDatum::Val(curr_val)) => {
                        match cand_val.cmp(curr_val) {
                            std::cmp::Ordering::Greater => true, // Higher value is more restrictive for lower bound
                            std::cmp::Ordering::Equal => {
                                // Same value: exclusive is more restrictive than inclusive for lower bound
                                !cand_incl && *curr_incl
                            }
                            std::cmp::Ordering::Less => false, // Lower value is less restrictive
                        }
                    }
                    _ => false, // Don't handle infinity comparisons for now
                }
            }

            // Other cases: keep current
            _ => false,
        }
    }

    /// Check if candidate upper bound is more restrictive than current
    fn is_more_restrictive_upper(&self, candidate: &Endpoint, current: &Endpoint) -> bool {
        match (candidate, current) {
            // Any concrete value is more restrictive than unbounded
            (Endpoint::Value { .. }, Endpoint::UnboundedHigh(_)) => true,

            // Unbounded is never more restrictive than concrete
            (Endpoint::UnboundedHigh(_), Endpoint::Value { .. }) => false,

            // Compare two concrete values
            (Endpoint::Value { datum: cand_datum, inclusive: cand_incl }, Endpoint::Value { datum: curr_datum, inclusive: curr_incl }) => {
                match (cand_datum, curr_datum) {
                    (KeyDatum::Val(cand_val), KeyDatum::Val(curr_val)) => {
                        match cand_val.cmp(curr_val) {
                            std::cmp::Ordering::Less => true, // Lower value is more restrictive for upper bound
                            std::cmp::Ordering::Equal => {
                                // Same value: exclusive is more restrictive than inclusive for upper bound
                                !cand_incl && *curr_incl
                            }
                            std::cmp::Ordering::Greater => false, // Higher value is less restrictive
                        }
                    }
                    _ => false, // Don't handle infinity comparisons for now
                }
            }

            // Other cases: keep current
            _ => false,
        }
    }

    /// Check if bounds represent an empty range (impossible to satisfy)
    fn is_empty_bounds(&self, bounds: &IndexBounds) -> bool {
        for bound in &bounds.keyparts {
            // Check if any column has impossible bounds (low > high)
            match (&bound.low, &bound.high) {
                (
                    Endpoint::Value { datum: low_datum, inclusive: low_incl },
                    Endpoint::Value { datum: high_datum, inclusive: high_incl },
                ) => {
                    // Both are concrete values - check if low > high
                    match (low_datum, high_datum) {
                        (KeyDatum::Val(low_val), KeyDatum::Val(high_val)) => {
                            // Compare values using PropertyValue comparison
                            match low_val.cmp(high_val) {
                                std::cmp::Ordering::Greater => return true, // low > high = empty
                                std::cmp::Ordering::Equal => {
                                    // Equal values but both exclusive = empty
                                    if !low_incl && !high_incl {
                                        return true;
                                    }
                                }
                                std::cmp::Ordering::Less => {} // low < high = ok
                            }
                        }
                        _ => {} // Mixed with infinities - not empty
                    }
                }
                _ => {} // At least one unbounded - not empty
            }
        }
        false
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

    /// Deduplicate plans based on index_spec and scan_direction
    fn deduplicate_plans(&self, plans: Vec<Plan>) -> Vec<Plan> {
        let mut unique_plans = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for plan in plans {
            match &plan {
                Plan::Index { index_spec, scan_direction, .. } => {
                    // Create a key that owns the data we need for comparison
                    let key = (index_spec.keyparts.clone(), scan_direction.clone());
                    if seen.insert(key) {
                        // insert returns true if the key was not already present
                        unique_plans.push(plan);
                    }
                }
                Plan::EmptyScan => {
                    // Always include empty scans (they're rare and important)
                    unique_plans.push(plan);
                }
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
            IndexKeyPart::asc($name.to_string())
        };
    }
    macro_rules! desc {
        ($name:expr) => {
            IndexKeyPart::desc($name.to_string())
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

    use crate::{Endpoint, IndexBounds, IndexColumnBound, KeyDatum, ValueType};
    // ---- endpoint helpers (type-inferred via Into<PropertyValue>) ----
    fn ge<T: Into<PropertyValue>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: true } }
    fn gt<T: Into<PropertyValue>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: false } }
    fn le<T: Into<PropertyValue>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: true } }
    fn lt<T: Into<PropertyValue>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: false } }

    // ---- per-column range parser (RHS captured as `tt` to allow `..` / `..=`) ----
    macro_rules! col_range {
        // fat arrow syntax (for bounds_list usage)
        ($col:expr => $lo:tt .. $hi:tt) => {{
            let __lo: PropertyValue = ($lo).into();
            let __hi: PropertyValue = ($hi).into();
            IndexColumnBound { column: $col.to_string(), low: ge(__lo), high: lt(__hi) }
        }};
        ($col:expr => $lo:tt ..= $hi:tt) => {{
            let __lo: PropertyValue = ($lo).into();
            let __hi: PropertyValue = ($hi).into();
            IndexColumnBound { column: $col.to_string(), low: ge(__lo), high: le(__hi) }
        }};
        ($col:expr => .. $hi:tt) => {{
            let __hi: PropertyValue = ($hi).into();
            IndexColumnBound { column: $col.to_string(), low: Endpoint::UnboundedLow(ValueType::of(&__hi)), high: lt(__hi) }
        }};
        ($col:expr => $lo:tt ..) => {{
            let __lo: PropertyValue = ($lo).into();
            IndexColumnBound { column: $col.to_string(), low: ge(__lo.clone()), high: Endpoint::UnboundedHigh(ValueType::of(&__lo)) }
        }};

        // parenthesized ranges (to avoid parsing conflicts in bounds! macro)
        ($col:expr, ($lo:tt .. $hi:tt)) => {{
            let __lo: PropertyValue = ($lo).into();
            let __hi: PropertyValue = ($hi).into();
            IndexColumnBound { column: $col.to_string(), low: ge(__lo), high: lt(__hi) }
        }};
        ($col:expr, ($lo:tt ..= $hi:tt)) => {{
            let __lo: PropertyValue = ($lo).into();
            let __hi: PropertyValue = ($hi).into();
            IndexColumnBound { column: $col.to_string(), low: ge(__lo), high: le(__hi) }
        }};
        ($col:expr, (.. $hi:tt)) => {{
            let __hi: PropertyValue = ($hi).into();
            IndexColumnBound { column: $col.to_string(), low: Endpoint::UnboundedLow(ValueType::of(&__hi)), high: lt(__hi) }
        }};
        ($col:expr, ($lo:tt ..)) => {{
            let __lo: PropertyValue = ($lo).into();
            IndexColumnBound { column: $col.to_string(), low: ge(__lo.clone()), high: Endpoint::UnboundedHigh(ValueType::of(&__lo)) }
        }};

        // explicit equality: = v   (clear and unambiguous)
        ($col:expr, = $v:tt) => {{
            let __pv: PropertyValue = ($v).into();
            IndexColumnBound { column: $col.to_string(), low: ge(__pv.clone()), high: le(__pv) }
        }};
    }

    // ---- collect multiple columns into IndexBounds ----
    macro_rules! bounds {
        ( $( $col:expr => $spec:tt ),+ $(,)? ) => {
            IndexBounds::new(vec![ $( col_range!($col, $spec) ),+ ])
        };
    }

    // this one takes an array of IndexColumnBound
    macro_rules! bounds_list {
        ( $( $bound:expr ),+ $(,)? ) => {
            IndexBounds::new(vec![ $( $bound ),+ ])
        };
    }

    macro_rules! open_lower {
        ($col:expr => $lo:tt ..) => {{
            let mut bound = col_range!($col => $lo ..);
            // Modify the lower bound to be exclusive
            bound.low = match bound.low {
                Endpoint::Value { datum, inclusive: _ } => Endpoint::Value { datum, inclusive: false },
                other => other, // Keep unbounded as-is
            };
            bound
        }};
        ($col:expr => $lo:tt .. $hi:tt) => {{
            let mut bound = col_range!($col => $lo .. $hi);
            // Modify the lower bound to be exclusive (upper is already exclusive with ..)
            bound.low = match bound.low {
                Endpoint::Value { datum, inclusive: _ } => Endpoint::Value { datum, inclusive: false },
                other => other, // Keep unbounded as-is
            };
            bound
        }};
    }

    // Test cases for ORDER BY scenarios
    mod order_by_tests {
        use super::*;

        #[test]
        fn basic_order_by() {
            assert_eq!(
                plan!("__collection = 'album' ORDER BY foo, bar"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn order_by_with_covered_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND foo > 10 ORDER BY foo, bar"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (foo) is > 10
                    // to is incl because there is no foo < ? in the predicate
                    bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("foo" => 10..)),

                    // bounds: bounds!(__collection: album..album, foo: 10..)
                    // make_bounds_with_range(
                    //     &["__collection", "foo"],
                    //     &[PropertyValue::from("album")],
                    //     1,
                    //     Endpoint::excl(PropertyValue::from(10)),
                    //     Endpoint::UnboundedHigh(ValueType::I32)
                    // ),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }
        #[test]
        fn no_collection_field() {
            // the __collection field is not special for the planner
            assert_eq!(
                plan!("age = 30 ORDER BY foo, bar"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("age"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("age" => (30..=30)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        // was incorrectly named before
        #[test]
        fn test_order_by_with_equality() {
            assert_eq!(
                plan!("__collection = 'album' AND age = 30 ORDER BY foo, bar"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age"), asc!("foo"), asc!("bar")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "age" => (30..=30)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_desc_single_field() {
            // Single DESC field - should scan backwards with ASC index
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Reverse,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_all_desc() {
            // All DESC fields - should scan backwards with ASC indexes
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC, year DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("year")]),
                    scan_direction: ScanDirection::Reverse,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_mixed_directions_asc_first() {
            // Mixed directions starting with ASC - should stop at first DESC, require sort
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name ASC, year DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![oby_desc!("year")]
                }]
            );
        }

        #[test]
        fn test_order_by_mixed_directions_desc_first() {
            // Mixed directions starting with DESC - should stop at first ASC, require sort
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC, year ASC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Reverse,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![oby_asc!("year")]
                }]
            );
        }

        #[test]
        fn test_order_by_mixed_three_fields() {
            // Three fields: ASC, DESC, DESC - should only include first field
            assert_eq!(
                plan!("__collection = 'album' ORDER BY foo ASC, bar DESC, baz DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("foo")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![oby_desc!("bar"), oby_desc!("baz")]
                }]
            );
        }

        #[test]
        fn test_order_by_with_equality_and_desc() {
            // Equality + DESC ORDER BY
            assert_eq!(
                plan!("__collection = 'album' AND status = 'active' ORDER BY name DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("status"), asc!("name")]),
                    scan_direction: ScanDirection::Reverse,
                    bounds: bounds!("__collection" => ("album"..="album"), "status" => ("active"..="active")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_order_by_with_inequality_and_desc() {
            // Inequality on ORDER BY field + DESC
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 ORDER BY age DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Reverse,
                    // from is excl because the inequality (age) is > 25
                    // to is incl because there is no age < ? in the predicate
                    bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
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
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), desc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_full_support_mixed_directions() {
            // With full support, mixed directions should preserve all fields
            assert_eq!(
                plan_full_support!("__collection = 'album' ORDER BY name ASC, year DESC, score ASC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), desc!("year"), asc!("score")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_full_support_all_desc() {
            // With full support, all DESC should create DESC index fields
            assert_eq!(
                plan_full_support!("__collection = 'album' ORDER BY name DESC, year DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), desc!("name"), desc!("year")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_full_support_with_equality_and_mixed_order() {
            // Full support with equality and mixed ORDER BY directions
            assert_eq!(
                plan_full_support!("__collection = 'album' AND status = 'active' ORDER BY name ASC, year DESC"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("status"), asc!("name"), desc!("year")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "status" => ("active"..="active")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
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
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (age) is > 25
                    // to is incl because there is no age < ? in the predicate
                    bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_multiple_inequalities_same_field_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND age < 50"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (age) is > 25
                    // to is excl because the inequality (age) is < 50
                    bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..50)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
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
                    Plan::Index {
                        index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (age) is > 25
                        // from is incl because there is no age < ? in the predicate
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                        remaining_predicate: selection!("score < 100").predicate,
                        order_by_spill: vec![]
                    },
                    // Plan 2: Uses score index, age remains in predicate
                    Plan::Index {
                        index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("score")]),
                        scan_direction: ScanDirection::Forward,
                        // from is incl because there is no score < ? in the predicate
                        // to is excl because the inequality (score) is < 100
                        bounds: bounds!("__collection" => ("album"..="album"), "score" => (..100)),
                        remaining_predicate: selection!("age > 25").predicate,
                        order_by_spill: vec![]
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
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "name" => ("Alice"..="Alice")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_multiple_equalities_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND name = 'Alice' AND age = 30"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "name" => ("Alice"..="Alice"), "age" => (30..=30)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
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
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the final inequality (age) is > 25
                    // to is incl and the final component is omitted because there is no age < ? in the predicate
                    bounds: bounds_list!(
                        col_range!("__collection" => "album"..="album"),
                        col_range!("name" => "Alice"..="Alice"),
                        open_lower!("age" => 25..)
                    ),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_equality_with_order_by_and_matching_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND score > 50 AND age = 30 ORDER BY score"), // inequality intentionally in the middle to test index_field sequencing
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age"), asc!("score")]), // equalities first, then inequalities, preserving order of appearance
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the final inequality (score) is > 50
                    // to is incl and the final component is omitted because there is no score < ? in the predicate
                    bounds: bounds_list!(
                        col_range!("__collection" => "album"..="album"),
                        col_range!("age" => 30..=30),
                        open_lower!("score" => 50..)
                    ),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
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
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![],
                }]
            );
        }

        #[test]
        fn test_unsupported_operators() {
            // Queries with operators that can't be used for index ranges
            assert_eq!(
                plan!("__collection = 'album' AND name != 'Alice'"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: selection!("name != 'Alice'").predicate,
                    order_by_spill: vec![],
                }]
            );

            // Mixed supported and unsupported
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND name != 'Alice'"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (age) is > 25
                    // to is incl and the final component is omitted because there is no age < ? in the predicate
                    bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                    remaining_predicate: selection!("name != 'Alice'").predicate,
                    order_by_spill: vec![],
                }]
            );
        }

        #[test]
        fn test_impossible_range() {
            // Conflicting inequalities that create impossible range
            assert_eq!(plan!("__collection = 'album' AND age > 50 AND age < 30"), vec![Plan::EmptyScan]);
        }

        #[test]
        fn test_or_only_predicate() {
            // Predicate with only OR - __collection conjunct should be extractable
            assert_eq!(
                plan!("__collection = 'album' AND (age > 25 OR name = 'Alice')"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    // the OR-containing parenthetical is a disjunction, so it must remain in the predicate
                    remaining_predicate: selection!("age > 25 OR name = 'Alice'").predicate,
                    order_by_spill: vec![],
                }]
            );
        }

        #[test]
        fn test_complex_nested_predicate() {
            // Complex nesting that should still extract some conjuncts
            assert_eq!(
                plan!("__collection = 'album' AND score = 100 AND (age > 25 OR name = 'Alice')"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("score")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "score" => (100..=100)),
                    // the OR-containing parenthetical is a disjunction, so it must remain in the predicate
                    remaining_predicate: selection!("age > 25 OR name = 'Alice'").predicate,
                    order_by_spill: vec![],
                }]
            );
        }

        #[test]
        fn test_order_by_with_no_matching_predicate() {
            // This might already be covered above
            // ORDER BY field that doesn't appear in WHERE clause
            assert_eq!(
                plan!("__collection = 'album' AND age = 30 ORDER BY name, score"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age"), asc!("name"), asc!("score")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "age" => (30..=30)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![],
                }]
            );
        }

        #[test]
        fn test_inequality_different_field_than_order_by() {
            // Query: year >= '2001' ORDER BY name
            // Two correct strategies:
            // 1. Scan by NAME, filter by YEAR (ordering free, filtering costs)
            // 2. Scan by YEAR, sort by NAME (filtering free, sorting costs)
            assert_eq!(
                plan!("__collection = 'album' AND year >= '2001' ORDER BY name"),
                vec![
                    // Strategy 1: Scan by name, filter by year
                    Plan::Index {
                        index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::Comparison {
                            left: Box::new(Expr::Identifier(Identifier::Property("year".to_string()))),
                            operator: ComparisonOperator::GreaterThanOrEqual,
                            right: Box::new(Expr::Literal(ankql::ast::Literal::String("2001".to_string()))),
                        },
                        order_by_spill: vec![],
                    },
                    // Strategy 2: Scan by year, sort by name
                    Plan::Index {
                        index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("year")]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "year" => ("2001"..)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: vec![ankql::ast::OrderByItem {
                            identifier: ankql::ast::Identifier::Property("name".to_string()),
                            direction: ankql::ast::OrderDirection::Asc,
                        }],
                    }
                ]
            );
        }

        #[test]
        fn test_multiple_inequalities_same_field_complex() {
            // Multiple inequalities on same field with different operators
            // Implementation chooses most restrictive bounds (>= 25 over > 20)
            assert_eq!(
                plan!("__collection = 'album' AND age >= 25 AND age <= 50 AND age > 20"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("age")]),
                    scan_direction: ScanDirection::Forward,
                    // age >= 25 wins over age > 20 because it's more restrictive
                    // from: is incl because the most restrictive lower bound inequality (age) is >= 25
                    // to: is incl because the upper bound inequality (age) is <= 50
                    bounds: bounds!("__collection" => ("album"..="album"), "age" => (25..=50)),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![],
                }]
            );
        }

        #[test]
        fn test_large_numbers() {
            // Test with very large numbers
            assert_eq!(
                plan!("__collection = 'album' AND timestamp > 9223372036854775807"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("timestamp")]),
                    scan_direction: ScanDirection::Forward,
                    // from is excl because the inequality (timestamp) is > 9223372036854775807
                    // to is incl because there is no timestamp < ? in the predicate
                    // can't use closed lower bound via rust syntax - so we construct the bounds manually
                    bounds: bounds_list!(
                        col_range!("__collection" => "album"..="album"),
                        open_lower!("timestamp" => 9223372036854775807i64..)
                    ),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_empty_string_equality() {
            // Test that empty strings are handled correctly in equality comparisons
            assert_eq!(
                plan!("__collection = 'album' AND name = ''"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "name" => (""..="")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }

        #[test]
        fn test_empty_string_with_other_fields() {
            // Test empty string with other fields
            assert_eq!(
                plan!("__collection = 'album' AND name = '' AND year = '2000'"),
                vec![Plan::Index {
                    index_spec: IndexSpec::new(vec![asc!("__collection"), asc!("name"), asc!("year")]),
                    scan_direction: ScanDirection::Forward,
                    bounds: bounds!("__collection" => ("album"..="album"), "name" => (""..=""), "year" => ("2000"..="2000")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![]
                }]
            );
        }
    }
}
