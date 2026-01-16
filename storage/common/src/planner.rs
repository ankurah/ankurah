use crate::{KeyBounds, predicate::ConjunctFinder, types::*};
use ankql::ast::{ComparisonOperator, Expr, Predicate};
use ankurah_core::indexing::{IndexKeyPart, KeySpec};
use ankurah_core::value::{Value, ValueType};
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

    /// Generate all possible plans for a query
    ///
    /// Input: Selection with predicate, primary key field name
    /// Output: Vector of all viable plans (index plans + table scan fallback)
    pub fn plan(&self, selection: &ankql::ast::Selection, primary_key: &str) -> Vec<Plan> {
        let conjuncts = ConjunctFinder::find(&selection.predicate);

        // Separate conjuncts into equalities and inequalities, filtering out primary key predicates
        let (equalities, inequalities) = self.categorize_conjuncts_excluding_primary_key(&conjuncts, primary_key);

        // Check if we should skip index generation for primary key-only queries
        let has_primary_key_ranges = self.has_primary_key_range_predicates(&conjuncts, primary_key);
        let has_primary_key_order_by = self.has_primary_key_order_by(&selection.order_by, primary_key);
        let has_non_primary_predicates =
            conjuncts.iter().any(|pred| !matches!(pred, Predicate::True) && !self.is_primary_key_predicate(pred, primary_key));

        // If we have primary key predicates/ORDER BY but NO other meaningful predicates, skip index generation
        if (has_primary_key_ranges || has_primary_key_order_by) && !has_non_primary_predicates {
            let table_scan = self.build_table_scan_plan(&conjuncts, primary_key, &selection.order_by);
            return vec![table_scan];
        }

        let mut plans = Vec::new();

        // New ORDER BY strategies
        if let Some(order_by) = &selection.order_by
            && !order_by.is_empty()
        {
            if let Some(plan) = self.build_order_first_plan(&equalities, &inequalities, order_by, &conjuncts) {
                plans.push(plan);
            }
            // If an ORDER BY field has inequalities (covered inequality), do NOT emit INEQ-FIRST
            let covered_ineq =
                order_by.iter().any(|item| if item.path.is_simple() { inequalities.contains_key(item.path.first()) } else { false });
            if !covered_ineq
                && !inequalities.is_empty()
                && let Some(plan) = self.build_ineq_first_plan(&equalities, &inequalities, order_by, &conjuncts)
            {
                plans.push(plan);
            }
            // Apply the same TableScan fallback logic as the main path
            let deduplicated_plans = self.deduplicate_plans(plans);
            let has_empty_scan = deduplicated_plans.iter().any(|plan| matches!(plan, Plan::EmptyScan));
            if !has_empty_scan {
                let mut final_plans = deduplicated_plans;
                let table_scan = self.build_table_scan_plan(&conjuncts, primary_key, &selection.order_by);
                final_plans.push(table_scan);
                return final_plans;
            } else {
                return deduplicated_plans;
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
        let deduplicated_plans = self.deduplicate_plans(plans);

        // Add table scan as fallback ONLY if there's no EmptyScan
        let has_empty_scan = deduplicated_plans.iter().any(|plan| matches!(plan, Plan::EmptyScan));
        if !has_empty_scan {
            let mut final_plans = deduplicated_plans;
            let table_scan = self.build_table_scan_plan(&conjuncts, primary_key, &selection.order_by);
            final_plans.push(table_scan);
            final_plans
        } else {
            deduplicated_plans
        }
    }

    // ORDER-FIRST: [EQ …] + maximal OB prefix (capability-aware). Bounds: EQ only.
    fn build_order_first_plan(
        &self,
        equalities: &[(String, Value)],
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, Value)>>,
        order_by: &[ankql::ast::OrderByItem],
        conjuncts: &[Predicate],
    ) -> Option<Plan> {
        if order_by.is_empty() {
            return None;
        }

        // Keyparts: EQ prefix (using asc_path for multi-step path support)
        let mut index_keyparts: Vec<IndexKeyPart> = equalities.iter().map(|(f, v)| IndexKeyPart::asc_path(f, ValueType::of(v))).collect();

        // Append ORDER BY fields per capability
        if self.config.supports_desc_indexes {
            for item in order_by {
                if item.path.is_simple() {
                    let name = item.path.first();
                    index_keyparts.push(match item.direction {
                        ankql::ast::OrderDirection::Asc => IndexKeyPart::asc(name.to_string(), ValueType::String),
                        ankql::ast::OrderDirection::Desc => IndexKeyPart::desc(name.to_string(), ValueType::String),
                    });
                }
            }
        } else {
            // IndexedDB: ASC-only index parts, keep longest same-direction prefix
            let first_dir = order_by[0].direction.clone();
            let mut broke = false;
            for item in order_by {
                if item.path.is_simple() {
                    let name = item.path.first();
                    if !broke && item.direction == first_dir {
                        index_keyparts.push(IndexKeyPart::asc(name.to_string(), ValueType::String));
                    } else {
                        broke = true;
                    }
                }
            }
        }

        // Bounds: equalities + (optional) bounds on the first ORDER BY field that has inequalities
        let applied_ineq = order_by.iter().find_map(|item| {
            if item.path.is_simple() {
                let name = item.path.first();
                inequalities.get_key_value(name).map(|(k, v)| (k.as_str(), v))
            } else {
                None
            }
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

        // Build OrderByComponents: presort (satisfied by index) and spill (needs in-memory sort)
        let order_by = if !self.config.supports_desc_indexes {
            let first_dir = order_by[0].direction.clone();
            let mut presort = Vec::new();
            let mut spill = Vec::new();
            let mut broke = false;
            for item in order_by {
                if item.path.is_simple() {
                    if !broke && item.direction == first_dir {
                        presort.push(item.clone());
                    } else {
                        broke = true;
                        spill.push(item.clone());
                    }
                }
            }
            OrderByComponents::new(presort, spill)
        } else {
            // DESC indexes supported - entire ORDER BY satisfied by index
            OrderByComponents::new(order_by.to_vec(), vec![])
        };

        Some(Plan::Index {
            index_spec: KeySpec::new(index_keyparts),
            scan_direction,
            bounds,
            remaining_predicate,
            order_by_spill: order_by,
        })
    }

    // INEQ-FIRST: [EQ …] + primary INEQ (bounded). Do NOT append ORDER BY columns; always spill them.
    fn build_ineq_first_plan(
        &self,
        equalities: &[(String, Value)],
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, Value)>>,
        order_by: &[ankql::ast::OrderByItem],
        conjuncts: &[Predicate],
    ) -> Option<Plan> {
        // Pick primary inequality: prefer first OB field with ineq, else first ineq in map order
        let primary = order_by
            .iter()
            .find_map(|item| {
                if item.path.is_simple() {
                    let name = item.path.first();
                    inequalities.get_key_value(name).map(|(k, v)| (k.as_str(), v))
                } else {
                    None
                }
            })
            .or_else(|| inequalities.iter().next().map(|(k, v)| (k.as_str(), v)))?;

        // Keyparts: EQ + primary INEQ (do not append ORDER BY fields; they do not satisfy global order after a range)
        // NOTE (micro-optimization): Appending OB columns after the range could help spill comparator locality,
        // but it does not change correctness and the tests expect the simpler invariant-preserving form.
        let mut index_keyparts: Vec<IndexKeyPart> = equalities.iter().map(|(f, v)| IndexKeyPart::asc_path(f, ValueType::of(v))).collect();
        let primary_value = &primary.1[0].1; // Get Value from first inequality
        index_keyparts.push(IndexKeyPart::asc_path(primary.0, ValueType::of(primary_value))); // Use actual primary key value type

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

        // Build OrderByComponents: presort (EQ columns + pivot) and spill (everything else)
        // EQ columns are constant so ORDER BY direction doesn't matter for them.
        // Pivot column ordering depends on scan direction matching ORDER BY direction.
        let mut covered = std::collections::HashSet::new();
        covered.extend(equalities.iter().map(|(f, _)| f.as_str()));
        covered.insert(primary.0);
        let mut presort = Vec::new();
        let mut spill = Vec::new();
        for item in order_by {
            if item.path.is_simple() {
                let name = item.path.first();
                if covered.contains(name) {
                    presort.push(item.clone());
                } else {
                    spill.push(item.clone());
                }
            }
        }
        let order_by = OrderByComponents::new(presort, spill);

        Some(Plan::Index {
            index_spec: KeySpec::new(index_keyparts),
            scan_direction,
            bounds,
            remaining_predicate,
            order_by_spill: order_by,
        })
    }
    /// Categorize conjuncts into equalities and inequalities
    fn categorize_conjuncts_excluding_primary_key(
        &self,
        conjuncts: &[Predicate],
        primary_key: &str,
    ) -> (Vec<(String, Value)>, IndexMap<String, Vec<(ComparisonOperator, Value)>>) {
        let mut equalities = Vec::new();
        let mut inequalities: IndexMap<String, Vec<(ComparisonOperator, Value)>> = IndexMap::new();

        for conjunct in conjuncts {
            if let Some((field, op, value)) = self.extract_comparison(conjunct) {
                // Skip primary key predicates - they'll be handled by TableScan bounds
                if field == primary_key {
                    continue;
                }

                match op {
                    ComparisonOperator::Equal => {
                        equalities.push((field, value));
                    }
                    ComparisonOperator::GreaterThan
                    | ComparisonOperator::GreaterThanOrEqual
                    | ComparisonOperator::LessThan
                    | ComparisonOperator::LessThanOrEqual => {
                        inequalities.entry(field).or_default().push((op, value));
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

    /// Extract field path, operator, and value from a comparison predicate.
    /// Returns the full path as a dot-separated string (e.g., "context.session_id").
    fn extract_comparison(&self, predicate: &Predicate) -> Option<(String, ComparisonOperator, Value)> {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                // Extract field path from left side (supports multi-step paths)
                let field_path = match left.as_ref() {
                    Expr::Path(path) => path.steps.join("."),
                    _ => return None,
                };

                // Extract value from right side
                let value = match right.as_ref() {
                    Expr::Literal(literal) => literal.into(),
                    _ => return None,
                };

                Some((field_path, operator.clone(), value))
            }
            _ => None,
        }
    }

    // removed: legacy generate_order_by_plan (superseded by ORDER-FIRST/INEQ-FIRST)

    fn generate_inequality_plan_with_order_by(
        &self,
        equalities: &[(String, Value)],
        inequality_field: &str,
        inequalities: &IndexMap<String, Vec<(ComparisonOperator, Value)>>,
        conjuncts: &[Predicate],
        order_by: Option<&[ankql::ast::OrderByItem]>,
    ) -> Option<Plan> {
        // Add equality fields first
        let mut index_keyparts = Vec::new();
        for (field, value) in equalities {
            index_keyparts.push(IndexKeyPart::asc_path(field, ValueType::of(value)));
        }

        // Add the inequality field
        let inequality_values = inequalities.get(inequality_field)?;
        let first_inequality_value = &inequality_values[0].1; // Get Value from first inequality
        index_keyparts.push(IndexKeyPart::asc_path(inequality_field, ValueType::of(first_inequality_value)));

        // Build bounds
        let bounds = self.build_bounds(equalities, Some((inequality_field, inequality_values)), &index_keyparts);

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

        // Build OrderByComponents: presort (EQ + inequality) and spill (rest)
        let order_by_spill = if let Some(order_by_items) = order_by {
            let covered_fields: std::collections::HashSet<&str> =
                equalities.iter().map(|(f, _)| f.as_str()).chain(std::iter::once(inequality_field)).collect();
            let mut presort = Vec::new();
            let mut spill = Vec::new();
            for item in order_by_items {
                if item.path.is_simple() {
                    let name = item.path.first();
                    if covered_fields.contains(name) {
                        presort.push(item.clone());
                    } else {
                        spill.push(item.clone());
                    }
                }
            }
            OrderByComponents::new(presort, spill)
        } else {
            OrderByComponents::default()
        };

        let index_spec = KeySpec::new(index_keyparts);
        Some(Plan::Index { index_spec, scan_direction: ScanDirection::Forward, bounds, remaining_predicate, order_by_spill })
    }

    /// Generate plan for equality-only queries
    fn generate_equality_plan(&self, equalities: &[(String, Value)], conjuncts: &[Predicate]) -> Option<Plan> {
        // Add all equality fields
        let mut index_keyparts = Vec::new();
        for (field, value) in equalities {
            index_keyparts.push(IndexKeyPart::asc_path(field, ValueType::of(value)));
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

        let index_spec = KeySpec::new(index_keyparts);
        Some(Plan::Index {
            index_spec,
            scan_direction: ScanDirection::Forward,
            bounds,
            remaining_predicate,
            order_by_spill: OrderByComponents::default(),
        })
    }

    // removed: specialized ORDER BY bounds builder (use unified per-strategy bounds)

    /// Build bounds based on equalities and optional inequality
    fn build_bounds(
        &self,
        equalities: &[(String, Value)],
        inequality: Option<(&str, &Vec<(ComparisonOperator, Value)>)>,
        index_keyparts: &[IndexKeyPart],
    ) -> Option<KeyBounds> {
        let mut keypart_bounds = Vec::new();

        // Build per-column bounds (using full_path for multi-step path support)
        for keypart in index_keyparts {
            let full_path = keypart.full_path();

            // Check if this path has an equality constraint
            let equality_value = equalities.iter().find(|(field, _)| field == &full_path).map(|(_, value)| value);

            if let Some(value) = equality_value {
                // Equality constraint: both bounds are the same value, inclusive
                keypart_bounds.push(KeyBoundComponent {
                    column: full_path.clone(),
                    low: Endpoint::incl(value.clone()),
                    high: Endpoint::incl(value.clone()),
                });
            } else if let Some((ineq_field, inequalities)) = inequality {
                if ineq_field == full_path {
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

                    keypart_bounds.push(KeyBoundComponent { column: full_path.clone(), low, high });
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

        Some(KeyBounds::new(keypart_bounds))
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
                        match cand_val.partial_cmp(curr_val) {
                            Some(std::cmp::Ordering::Greater) => true, // Higher value is more restrictive for lower bound
                            Some(std::cmp::Ordering::Equal) => {
                                // Same value: exclusive is more restrictive than inclusive for lower bound
                                !cand_incl && *curr_incl
                            }
                            Some(std::cmp::Ordering::Less) => false, // Lower value is less restrictive
                            None => false,                           // Incomparable values (e.g., NaN) - keep current
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
                        match cand_val.partial_cmp(curr_val) {
                            Some(std::cmp::Ordering::Less) => true, // Lower value is more restrictive for upper bound
                            Some(std::cmp::Ordering::Equal) => {
                                // Same value: exclusive is more restrictive than inclusive for upper bound
                                !cand_incl && *curr_incl
                            }
                            Some(std::cmp::Ordering::Greater) => false, // Higher value is less restrictive
                            None => false,                              // Incomparable values (e.g., NaN) - keep current
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
    fn is_empty_bounds(&self, bounds: &KeyBounds) -> bool {
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
                            // Compare values using Value comparison
                            match low_val.partial_cmp(high_val) {
                                Some(std::cmp::Ordering::Greater) => return true, // low > high = empty
                                Some(std::cmp::Ordering::Equal) => {
                                    // Equal values but both exclusive = empty
                                    if !low_incl && !high_incl {
                                        return true;
                                    }
                                }
                                Some(std::cmp::Ordering::Less) => {} // low < high = ok
                                None => {}                           // Incomparable values (e.g., NaN) - assume not empty
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
        consumed_equalities: &[(String, Value)],
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
                if !consumed
                    && let Some(ineq_field) = consumed_inequality_field
                    && field == ineq_field
                {
                    consumed = true;
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
                    let key = (index_spec.keyparts.clone(), *scan_direction);
                    if seen.insert(key) {
                        // insert returns true if the key was not already present
                        unique_plans.push(plan);
                    }
                }
                Plan::EmptyScan => {
                    // Always include empty scans (they're rare and important)
                    unique_plans.push(plan);
                }
                Plan::TableScan { .. } => {
                    // Always include table scans (fallback plan)
                    unique_plans.push(plan);
                }
            }
        }

        unique_plans
    }

    /// Build a table scan plan with optional entity ID range extraction
    fn build_table_scan_plan(&self, conjuncts: &[Predicate], primary_key: &str, order_by: &Option<Vec<ankql::ast::OrderByItem>>) -> Plan {
        // Extract entity ID range from predicates on the primary key
        let bounds = self.extract_entity_id_range(conjuncts, primary_key);

        // All predicates remain (no index to satisfy any)
        let remaining_predicate = conjuncts.iter().fold(ankql::ast::Predicate::True, |acc, pred| {
            if matches!(acc, ankql::ast::Predicate::True) {
                pred.clone()
            } else {
                ankql::ast::Predicate::And(Box::new(acc), Box::new(pred.clone()))
            }
        });

        // Determine scan direction and ORDER BY components based on primary key ORDER BY
        let (scan_direction, order_by_spill) = if let Some(order_items) = order_by {
            if let Some(first_item) = order_items.first() {
                if first_item.path.is_simple() && first_item.path.first() == primary_key {
                    // Primary key ORDER BY is satisfied by scan direction
                    let direction = match first_item.direction {
                        ankql::ast::OrderDirection::Asc => ScanDirection::Forward,
                        ankql::ast::OrderDirection::Desc => ScanDirection::Reverse,
                    };
                    // First item is presort (satisfied by scan), rest is spill
                    let presort = vec![first_item.clone()];
                    let spill = order_items[1..].to_vec();
                    (direction, OrderByComponents::new(presort, spill))
                } else {
                    // Primary key not in ORDER BY, use forward scan and spill all
                    (ScanDirection::Forward, OrderByComponents::new(vec![], order_items.clone()))
                }
            } else {
                // Empty ORDER BY
                (ScanDirection::Forward, OrderByComponents::default())
            }
        } else {
            // No ORDER BY
            (ScanDirection::Forward, OrderByComponents::default())
        };

        Plan::TableScan { bounds, scan_direction, remaining_predicate, order_by_spill }
    }

    /// Extract entity ID range from predicates on the primary key field
    fn extract_entity_id_range(&self, conjuncts: &[Predicate], primary_key: &str) -> KeyBounds {
        let mut primary_key_bounds = Vec::new();

        // Extract all primary key constraints from conjuncts
        for predicate in conjuncts {
            if let Some(bound) = self.extract_primary_key_bound(predicate, primary_key) {
                primary_key_bounds.push(bound);
            }
        }

        if primary_key_bounds.is_empty() {
            return KeyBounds::empty();
        }

        // Combine multiple bounds into a single KeyBounds via intersection
        if primary_key_bounds.len() == 1 {
            KeyBounds { keyparts: primary_key_bounds }
        } else {
            // Intersect all bounds to get the most restrictive range
            let intersected_bound = self.intersect_primary_key_bounds(primary_key_bounds, primary_key);
            KeyBounds { keyparts: vec![intersected_bound] }
        }
    }

    /// Extract a single primary key bound from a predicate
    fn extract_primary_key_bound(&self, predicate: &Predicate, primary_key: &str) -> Option<KeyBoundComponent> {
        if let Predicate::Comparison { left, operator, right } = predicate {
            // Check if this is a primary key comparison
            let value = match (left.as_ref(), right.as_ref()) {
                (Expr::Path(path), Expr::Literal(literal)) if path.is_simple() && path.first() == primary_key => Value::from(literal),
                (Expr::Literal(literal), Expr::Path(path)) if path.is_simple() && path.first() == primary_key => Value::from(literal),
                _ => return None,
            };

            // Convert comparison operator to bounds
            let (low, high) = match operator {
                ComparisonOperator::Equal => (
                    Endpoint::Value { datum: KeyDatum::Val(value.clone()), inclusive: true },
                    Endpoint::Value { datum: KeyDatum::Val(value), inclusive: true },
                ),
                ComparisonOperator::GreaterThan => (
                    Endpoint::Value { datum: KeyDatum::Val(value.clone()), inclusive: false },
                    Endpoint::UnboundedHigh(ValueType::of(&value)),
                ),
                ComparisonOperator::GreaterThanOrEqual => (
                    Endpoint::Value { datum: KeyDatum::Val(value.clone()), inclusive: true },
                    Endpoint::UnboundedHigh(ValueType::of(&value)),
                ),
                ComparisonOperator::LessThan => (
                    Endpoint::UnboundedLow(ValueType::of(&value)),
                    Endpoint::Value { datum: KeyDatum::Val(value.clone()), inclusive: false },
                ),
                ComparisonOperator::LessThanOrEqual => (
                    Endpoint::UnboundedLow(ValueType::of(&value)),
                    Endpoint::Value { datum: KeyDatum::Val(value.clone()), inclusive: true },
                ),
                _ => return None, // Skip != and other operators
            };

            Some(KeyBoundComponent { column: primary_key.to_string(), low, high })
        } else {
            None
        }
    }

    /// Intersect multiple primary key bounds to get the most restrictive range
    fn intersect_primary_key_bounds(&self, bounds: Vec<KeyBoundComponent>, primary_key: &str) -> KeyBoundComponent {
        let mut result_low = Endpoint::UnboundedLow(ValueType::String); // Assume string for now
        let mut result_high = Endpoint::UnboundedHigh(ValueType::String);

        for bound in bounds {
            // Intersect lower bounds (take the maximum/most restrictive)
            result_low = self.intersect_lower_bounds(&result_low, &bound.low);

            // Intersect upper bounds (take the minimum/most restrictive)
            result_high = self.intersect_upper_bounds(&result_high, &bound.high);
        }

        KeyBoundComponent { column: primary_key.to_string(), low: result_low, high: result_high }
    }

    /// Intersect two lower bounds to get the most restrictive (maximum)
    fn intersect_lower_bounds(&self, left: &Endpoint, right: &Endpoint) -> Endpoint {
        match (left, right) {
            (Endpoint::UnboundedLow(_), other) | (other, Endpoint::UnboundedLow(_)) => other.clone(),
            (
                Endpoint::Value { datum: KeyDatum::Val(a), inclusive: inc_a },
                Endpoint::Value { datum: KeyDatum::Val(b), inclusive: inc_b },
            ) => {
                match a.partial_cmp(b) {
                    Some(std::cmp::Ordering::Greater) => left.clone(),
                    Some(std::cmp::Ordering::Less) => right.clone(),
                    Some(std::cmp::Ordering::Equal) => {
                        // Same value - use the more restrictive inclusivity (false is more restrictive for lower bounds)
                        Endpoint::Value { datum: KeyDatum::Val(a.clone()), inclusive: *inc_a && *inc_b }
                    }
                    None => left.clone(), // Can't compare, just pick one
                }
            }
            _ => left.clone(), // Fallback
        }
    }

    /// Intersect two upper bounds to get the most restrictive (minimum)
    fn intersect_upper_bounds(&self, left: &Endpoint, right: &Endpoint) -> Endpoint {
        match (left, right) {
            (Endpoint::UnboundedHigh(_), other) | (other, Endpoint::UnboundedHigh(_)) => other.clone(),
            (
                Endpoint::Value { datum: KeyDatum::Val(a), inclusive: inc_a },
                Endpoint::Value { datum: KeyDatum::Val(b), inclusive: inc_b },
            ) => {
                match a.partial_cmp(b) {
                    Some(std::cmp::Ordering::Less) => left.clone(),
                    Some(std::cmp::Ordering::Greater) => right.clone(),
                    Some(std::cmp::Ordering::Equal) => {
                        // Same value - use the more restrictive inclusivity (false is more restrictive for upper bounds)
                        Endpoint::Value { datum: KeyDatum::Val(a.clone()), inclusive: *inc_a && *inc_b }
                    }
                    None => left.clone(), // Can't compare, just pick one
                }
            }
            _ => left.clone(), // Fallback
        }
    }

    /// Check if a predicate is on the primary key field
    fn is_primary_key_predicate(&self, predicate: &Predicate, primary_key: &str) -> bool {
        if let Predicate::Comparison { left, operator: _, right: _ } = predicate {
            match left.as_ref() {
                Expr::Path(path) if path.is_simple() => path.first() == primary_key,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Check if ORDER BY is on the primary key (should skip index generation)
    fn has_primary_key_order_by(&self, order_by: &Option<Vec<ankql::ast::OrderByItem>>, primary_key: &str) -> bool {
        if let Some(order_items) = order_by
            && let Some(first_item) = order_items.first()
            && first_item.path.is_simple()
        {
            return first_item.path.first() == primary_key;
        }
        false
    }

    /// Check if conjuncts contain primary key range predicates that should skip index generation
    fn has_primary_key_range_predicates(&self, conjuncts: &[Predicate], primary_key: &str) -> bool {
        conjuncts.iter().any(|predicate| {
            if let Predicate::Comparison { left, operator, right: _ } = predicate {
                // Check if this is a primary key comparison with supported operators
                let is_primary_key_field = match left.as_ref() {
                    Expr::Path(path) if path.is_simple() => path.first() == primary_key,
                    _ => false,
                };

                if is_primary_key_field {
                    // Skip index generation for range operators (=, >, <, >=, <=)
                    // Keep index generation for != and other operators
                    matches!(
                        operator,
                        ComparisonOperator::Equal
                            | ComparisonOperator::GreaterThan
                            | ComparisonOperator::GreaterThanOrEqual
                            | ComparisonOperator::LessThan
                            | ComparisonOperator::LessThanOrEqual
                    )
                } else {
                    false
                }
            } else {
                false
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core::value::Value;
    use ankurah_derive::selection;

    // FIX_ME: rename to plan_indexeddb
    macro_rules! plan {
        ($($selection:tt)*) => {{
            let selection = selection!($($selection)*);
            let planner = Planner::new(PlannerConfig::indexeddb());
            planner.plan(&selection, "id")
        }};
    }
    macro_rules! plan_full_support {
        ($($selection:tt)*) => {{
            let selection = selection!($($selection)*);
            let planner = Planner::new(PlannerConfig::full_support());
            planner.plan(&selection, "id")
        }};
    }
    macro_rules! asc {
        ($name:expr, $ty:expr) => {
            IndexKeyPart::asc($name.to_string(), $ty)
        };
    }
    macro_rules! desc {
        ($name:expr, $ty:expr) => {
            IndexKeyPart::desc($name.to_string(), $ty)
        };
    }
    macro_rules! oby_asc {
        ($name:expr) => {
            ankql::ast::OrderByItem { path: ankql::ast::PathExpr::simple($name), direction: ankql::ast::OrderDirection::Asc }
        };
    }
    macro_rules! oby_desc {
        ($name:expr) => {
            ankql::ast::OrderByItem { path: ankql::ast::PathExpr::simple($name), direction: ankql::ast::OrderDirection::Desc }
        };
    }

    use crate::{Endpoint, KeyBoundComponent, KeyBounds, KeyDatum};
    use ankurah_core::value::ValueType;
    // ---- endpoint helpers (type-inferred via Into<Value>) ----
    fn ge<T: Into<Value>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: true } }
    fn gt<T: Into<Value>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: false } }
    fn le<T: Into<Value>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: true } }
    fn lt<T: Into<Value>>(v: T) -> Endpoint { Endpoint::Value { datum: KeyDatum::Val(v.into()), inclusive: false } }

    // ---- per-column range parser (RHS captured as `tt` to allow `..` / `..=`) ----
    macro_rules! col_range {
        // fat arrow syntax (for bounds_list usage)
        ($col:expr => $lo:tt .. $hi:tt) => {{
            let __lo: Value = ($lo).into();
            let __hi: Value = ($hi).into();
            KeyBoundComponent { column: $col.to_string(), low: ge(__lo), high: lt(__hi) }
        }};
        ($col:expr => $lo:tt ..= $hi:tt) => {{
            let __lo: Value = ($lo).into();
            let __hi: Value = ($hi).into();
            KeyBoundComponent { column: $col.to_string(), low: ge(__lo), high: le(__hi) }
        }};
        ($col:expr => .. $hi:tt) => {{
            let __hi: Value = ($hi).into();
            KeyBoundComponent { column: $col.to_string(), low: Endpoint::UnboundedLow(ValueType::of(&__hi)), high: lt(__hi) }
        }};
        ($col:expr => $lo:tt ..) => {{
            let __lo: Value = ($lo).into();
            KeyBoundComponent { column: $col.to_string(), low: ge(__lo.clone()), high: Endpoint::UnboundedHigh(ValueType::of(&__lo)) }
        }};

        // parenthesized ranges (to avoid parsing conflicts in bounds! macro)
        ($col:expr, ($lo:tt .. $hi:tt)) => {{
            let __lo: Value = ($lo).into();
            let __hi: Value = ($hi).into();
            KeyBoundComponent { column: $col.to_string(), low: ge(__lo), high: lt(__hi) }
        }};
        ($col:expr, ($lo:tt ..= $hi:tt)) => {{
            let __lo: Value = ($lo).into();
            let __hi: Value = ($hi).into();
            KeyBoundComponent { column: $col.to_string(), low: ge(__lo), high: le(__hi) }
        }};
        ($col:expr, (.. $hi:tt)) => {{
            let __hi: Value = ($hi).into();
            KeyBoundComponent { column: $col.to_string(), low: Endpoint::UnboundedLow(ValueType::of(&__hi)), high: lt(__hi) }
        }};
        ($col:expr, (..= $hi:tt)) => {{
            let __hi: Value = ($hi).into();
            KeyBoundComponent { column: $col.to_string(), low: Endpoint::UnboundedLow(ValueType::of(&__hi)), high: le(__hi) }
        }};
        ($col:expr, ($lo:tt ..)) => {{
            let __lo: Value = ($lo).into();
            KeyBoundComponent { column: $col.to_string(), low: ge(__lo.clone()), high: Endpoint::UnboundedHigh(ValueType::of(&__lo)) }
        }};

        // explicit equality: = v   (clear and unambiguous)
        ($col:expr, = $v:tt) => {{
            let __pv: Value = ($v).into();
            KeyBoundComponent { column: $col.to_string(), low: ge(__pv.clone()), high: le(__pv) }
        }};
    }

    // ---- collect multiple columns into KeyBounds ----
    macro_rules! bounds {
        ( $( $col:expr => $spec:tt ),+ $(,)? ) => {
            KeyBounds::new(vec![ $( col_range!($col, $spec) ),+ ])
        };
    }

    // this one takes an array of KeyBoundComponent
    macro_rules! bounds_list {
        ( $( $bound:expr ),+ $(,)? ) => {
            KeyBounds::new(vec![ $( $bound ),+ ])
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
        ($col:expr => $lo:tt ..= $hi:tt) => {{
            let mut bound = col_range!($col => $lo ..= $hi);
            // Modify the lower bound to be exclusive (upper stays inclusive with ..=)
            bound.low = match bound.low {
                Endpoint::Value { datum, inclusive: _ } => Endpoint::Value { datum, inclusive: false },
                other => other, // Keep unbounded as-is
            };
            bound
        }};
    }

    // Helper macro for OrderByComponents in tests
    macro_rules! order_by_components {
        // No ORDER BY
        () => {
            OrderByComponents::default()
        };
        // All presort, no spill (index satisfies entire ORDER BY)
        (presort: [$($presort:expr),* $(,)?]) => {
            OrderByComponents::new(vec![$($presort),*], vec![])
        };
        // All spill, no presort (global sort)
        (spill: [$($spill:expr),* $(,)?]) => {
            OrderByComponents::new(vec![], vec![$($spill),*])
        };
        // Both presort and spill
        (presort: [$($presort:expr),* $(,)?], spill: [$($spill:expr),* $(,)?]) => {
            OrderByComponents::new(vec![$($presort),*], vec![$($spill),*])
        };
    }

    // Test cases for ORDER BY scenarios
    mod order_by_tests {
        use super::*;

        #[test]
        fn basic_order_by() {
            assert_eq!(
                plan!("__collection = 'album' ORDER BY foo, bar"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("foo", ValueType::String),
                            asc!("bar", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("foo"), oby_asc!("bar")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("foo"), oby_asc!("bar")])
                    }
                ]
            );
        }

        #[test]
        fn order_by_with_covered_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND foo > 10 ORDER BY foo, bar"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("foo", ValueType::String),
                            asc!("bar", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (foo) is > 10
                        // to is incl because there is no foo < ? in the predicate
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("foo" => 10..)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("foo"), oby_asc!("bar")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND foo > 10").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("foo"), oby_asc!("bar")])
                    }
                ]
            );
        }
        #[test]
        fn no_collection_field() {
            // the __collection field is not special for the planner
            assert_eq!(
                plan!("age = 30 ORDER BY foo, bar"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("age", ValueType::I32),
                            asc!("foo", ValueType::String),
                            asc!("bar", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("age" => (30..=30)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("foo"), oby_asc!("bar")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("age = 30").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("foo"), oby_asc!("bar")])
                    }
                ]
            );
        }

        // was incorrectly named before
        #[test]
        fn test_order_by_with_equality() {
            assert_eq!(
                plan!("__collection = 'album' AND age = 30 ORDER BY foo, bar"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("age", ValueType::I32),
                            asc!("foo", ValueType::String),
                            asc!("bar", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (30..=30)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("foo"), oby_asc!("bar")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age = 30").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("foo"), oby_asc!("bar")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_desc_single_field() {
            // Single DESC field - should scan backwards with ASC index
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("name", ValueType::String)]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("name")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("name")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_all_desc() {
            // All DESC fields - should scan backwards with ASC indexes
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC, year DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("name", ValueType::String),
                            asc!("year", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("name"), oby_desc!("year")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("name"), oby_desc!("year")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_mixed_directions_asc_first() {
            // Mixed directions starting with ASC - should stop at first DESC, require sort
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name ASC, year DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("name", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("name")], spill: [oby_desc!("year")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("name"), oby_desc!("year")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_mixed_directions_desc_first() {
            // Mixed directions starting with DESC - should stop at first ASC, require sort
            assert_eq!(
                plan!("__collection = 'album' ORDER BY name DESC, year ASC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("name", ValueType::String)]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("name")], spill: [oby_asc!("year")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("name"), oby_asc!("year")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_three_asc_desc_desc() {
            // ASC, DESC, DESC: Forward scan satisfies first, rest spilled
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a ASC, b DESC, c DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("a", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("a")], spill: [oby_desc!("b"), oby_desc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("a"), oby_desc!("b"), oby_desc!("c")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_three_desc_desc_asc() {
            // DESC, DESC, ASC: Reverse scan satisfies first two, third spilled
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a DESC, b DESC, c ASC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("a", ValueType::String),
                            asc!("b", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("a"), oby_desc!("b")], spill: [oby_asc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("a"), oby_desc!("b"), oby_asc!("c")])
                    }
                ]
            );
        }

        // === Complete Three-Column Direction Pattern Tests ===
        // All 8 combinations of ASC/DESC for three columns

        #[test]
        fn test_order_by_three_asc_asc_asc() {
            // All ASC: Forward scan, all satisfied by index, no spill
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a ASC, b ASC, c ASC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("a", ValueType::String),
                            asc!("b", ValueType::String),
                            asc!("c", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("a"), oby_asc!("b"), oby_asc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("a"), oby_asc!("b"), oby_asc!("c")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_three_asc_asc_desc() {
            // ASC, ASC, DESC: Forward scan satisfies first two, third spilled
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a ASC, b ASC, c DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("a", ValueType::String),
                            asc!("b", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("a"), oby_asc!("b")], spill: [oby_desc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("a"), oby_asc!("b"), oby_desc!("c")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_three_asc_desc_asc() {
            // ASC, DESC, ASC: Forward scan satisfies first, rest spilled
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a ASC, b DESC, c ASC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("a", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("a")], spill: [oby_desc!("b"), oby_asc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("a"), oby_desc!("b"), oby_asc!("c")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_three_desc_asc_asc() {
            // DESC, ASC, ASC: Reverse scan satisfies first, rest spilled
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a DESC, b ASC, c ASC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("a", ValueType::String)]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("a")], spill: [oby_asc!("b"), oby_asc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("a"), oby_asc!("b"), oby_asc!("c")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_three_desc_asc_desc() {
            // DESC, ASC, DESC: Reverse scan satisfies first, rest spilled
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a DESC, b ASC, c DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("a", ValueType::String)]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("a")], spill: [oby_asc!("b"), oby_desc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("a"), oby_asc!("b"), oby_desc!("c")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_three_desc_desc_desc() {
            // All DESC: Reverse scan, all satisfied by index, no spill
            assert_eq!(
                plan!("__collection = 'album' ORDER BY a DESC, b DESC, c DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("a", ValueType::String),
                            asc!("b", ValueType::String),
                            asc!("c", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("a"), oby_desc!("b"), oby_desc!("c")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("a"), oby_desc!("b"), oby_desc!("c")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_with_equality_and_desc() {
            // Equality + DESC ORDER BY
            assert_eq!(
                plan!("__collection = 'album' AND status = 'active' ORDER BY name DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("status", ValueType::String),
                            asc!("name", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Reverse,
                        bounds: bounds!("__collection" => ("album"..="album"), "status" => ("active"..="active")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("name")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND status = 'active'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("name")])
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_with_inequality_and_desc() {
            // Inequality on ORDER BY field + DESC
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 ORDER BY age DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::String)]),
                        scan_direction: ScanDirection::Reverse,
                        // from is excl because the inequality (age) is > 25
                        // to is incl because there is no age < ? in the predicate
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("age")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age > 25").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("age")])
                    }
                ]
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
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), desc!("name", ValueType::String),]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("name")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("name")])
                    }
                ]
            );
        }

        #[test]
        fn test_full_support_mixed_directions() {
            // With full support, mixed directions should preserve all fields
            assert_eq!(
                plan_full_support!("__collection = 'album' ORDER BY name ASC, year DESC, score ASC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("name", ValueType::String),
                            desc!("year", ValueType::String),
                            asc!("score", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("name"), oby_desc!("year"), oby_asc!("score")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("name"), oby_desc!("year"), oby_asc!("score")])
                    }
                ]
            );
        }

        #[test]
        fn test_full_support_all_desc() {
            // With full support, all DESC should create DESC index fields
            assert_eq!(
                plan_full_support!("__collection = 'album' ORDER BY name DESC, year DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            desc!("name", ValueType::String),
                            desc!("year", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_desc!("name"), oby_desc!("year")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("name"), oby_desc!("year")])
                    }
                ]
            );
        }

        #[test]
        fn test_full_support_with_equality_and_mixed_order() {
            // Full support with equality and mixed ORDER BY directions
            assert_eq!(
                plan_full_support!("__collection = 'album' AND status = 'active' ORDER BY name ASC, year DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("status", ValueType::String),
                            asc!("name", ValueType::String),
                            desc!("year", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "status" => ("active"..="active")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("name"), oby_desc!("year")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND status = 'active'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("name"), oby_desc!("year")])
                    }
                ]
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
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (age) is > 25
                        // to is incl because there is no age < ? in the predicate
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age > 25").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_multiple_inequalities_same_field_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND age < 50"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (age) is > 25
                        // to is excl because the inequality (age) is < 50
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..50)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age > 25 AND age < 50").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
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
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (age) is > 25
                        // from is incl because there is no age < ? in the predicate
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                        remaining_predicate: selection!("score < 100").predicate,
                        order_by_spill: order_by_components!()
                    },
                    // Plan 2: Uses score index, age remains in predicate
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("score", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from is incl because there is no score < ? in the predicate
                        // to is excl because the inequality (score) is < 100
                        bounds: bounds!("__collection" => ("album"..="album"), "score" => (..100)),
                        remaining_predicate: selection!("age > 25").predicate,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age > 25 AND score < 100").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_greater_or_equal_inclusive_lower_bound() {
            // >= operator should produce INCLUSIVE lower bound
            assert_eq!(
                plan!("__collection = 'album' AND age >= 25"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from is INCLUSIVE because the inequality is >= 25
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (25..)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age >= 25").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_less_than_exclusive_upper_bound() {
            // < operator should produce EXCLUSIVE upper bound
            assert_eq!(
                plan!("__collection = 'album' AND age < 50"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // to is EXCLUSIVE because the inequality is < 50
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (..50)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age < 50").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_less_or_equal_inclusive_upper_bound() {
            // <= operator should produce INCLUSIVE upper bound
            assert_eq!(
                plan!("__collection = 'album' AND age <= 50"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // to is INCLUSIVE because the inequality is <= 50
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (..=50)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age <= 50").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_range_inclusive_both() {
            // >= AND <= should produce INCLUSIVE on both sides
            assert_eq!(
                plan!("__collection = 'album' AND age >= 25 AND age <= 50"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // Both bounds inclusive
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (25..=50)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age >= 25 AND age <= 50").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_range_mixed_gte_lt() {
            // >= AND < should produce inclusive lower, exclusive upper
            assert_eq!(
                plan!("__collection = 'album' AND age >= 25 AND age < 50"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from inclusive (>=), to exclusive (<)
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (25..50)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age >= 25 AND age < 50").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_range_mixed_gt_lte() {
            // > AND <= should produce exclusive lower, inclusive upper
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND age <= 50"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from exclusive (>), to inclusive (<=)
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..=50)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age > 25 AND age <= 50").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_gte_with_desc_order_by() {
            // >= with DESC ORDER BY should use Reverse scan on index
            assert_eq!(
                plan!("__collection = 'album' AND age >= 25 ORDER BY age DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::String)]),
                        scan_direction: ScanDirection::Reverse,
                        // from is INCLUSIVE because the inequality is >= 25
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (25..)),
                        remaining_predicate: Predicate::True,
                        // presort contains ORDER BY columns satisfied by index scan direction
                        order_by_spill: order_by_components!(presort: [oby_desc!("age")])
                    },
                    // TableScan uses Forward scan (default) and spills ORDER BY
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age >= 25").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("age")])
                    }
                ]
            );
        }

        #[test]
        fn test_lte_with_desc_order_by() {
            // <= with DESC ORDER BY should use Reverse scan on index
            assert_eq!(
                plan!("__collection = 'album' AND age <= 50 ORDER BY age DESC"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::String)]),
                        scan_direction: ScanDirection::Reverse,
                        // to is INCLUSIVE because the inequality is <= 50
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (..=50)),
                        remaining_predicate: Predicate::True,
                        // presort contains ORDER BY columns satisfied by index scan direction
                        order_by_spill: order_by_components!(presort: [oby_desc!("age")])
                    },
                    // TableScan uses Forward scan (default) and spills ORDER BY
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age <= 50").predicate,
                        order_by_spill: order_by_components!(spill: [oby_desc!("age")])
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
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("name", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "name" => ("Alice"..="Alice")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND name = 'Alice'").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_multiple_equalities_plan_structure() {
            assert_eq!(
                plan!("__collection = 'album' AND name = 'Alice' AND age = 30"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("name", ValueType::String),
                            asc!("age", ValueType::I32)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "name" => ("Alice"..="Alice"), "age" => (30..=30)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND name = 'Alice' AND age = 30").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_four_column_equality_prefix() {
            // Test a 4-column equality prefix to verify longer chains work correctly
            assert_eq!(
                plan!("__collection = 'album' AND artist = 'Queen' AND year = 1975 AND genre = 'Rock'"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("artist", ValueType::String),
                            asc!("year", ValueType::I32),
                            asc!("genre", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!(
                            "__collection" => ("album"..="album"),
                            "artist" => ("Queen"..="Queen"),
                            "year" => (1975..=1975),
                            "genre" => ("Rock"..="Rock")
                        ),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND artist = 'Queen' AND year = 1975 AND genre = 'Rock'")
                            .predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_three_equality_with_order_by() {
            // Test 3 equalities combined with ORDER BY
            assert_eq!(
                plan!("__collection = 'album' AND artist = 'Queen' AND year = 1975 ORDER BY title"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("artist", ValueType::String),
                            asc!("year", ValueType::I32),
                            asc!("title", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!(
                            "__collection" => ("album"..="album"),
                            "artist" => ("Queen"..="Queen"),
                            "year" => (1975..=1975)
                        ),
                        remaining_predicate: Predicate::True,
                        // ORDER BY title is satisfied by index (title is last column after eq prefix)
                        order_by_spill: order_by_components!(presort: [oby_asc!("title")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND artist = 'Queen' AND year = 1975").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("title")])
                    }
                ]
            );
        }

        #[test]
        fn test_three_equality_with_inequality() {
            // Test 3 equalities combined with inequality
            assert_eq!(
                plan!("__collection = 'album' AND artist = 'Queen' AND year = 1975 AND rating > 4"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("artist", ValueType::String),
                            asc!("year", ValueType::I32),
                            asc!("rating", ValueType::I32)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds_list!(
                            col_range!("__collection" => "album"..="album"),
                            col_range!("artist" => "Queen"..="Queen"),
                            col_range!("year" => 1975..=1975),
                            open_lower!("rating" => 4..)
                        ),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND artist = 'Queen' AND year = 1975 AND rating > 4")
                            .predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
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
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("name", ValueType::String),
                            asc!("age", ValueType::I32)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the final inequality (age) is > 25
                        // to is incl and the final component is omitted because there is no age < ? in the predicate
                        bounds: bounds_list!(
                            col_range!("__collection" => "album"..="album"),
                            col_range!("name" => "Alice"..="Alice"),
                            open_lower!("age" => 25..)
                        ),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND name = 'Alice' AND age > 25").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_equality_with_order_by_and_matching_inequality() {
            assert_eq!(
                plan!("__collection = 'album' AND score > 50 AND age = 30 ORDER BY score"), // inequality intentionally in the middle to test index_field sequencing
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("age", ValueType::I32),
                            asc!("score", ValueType::String)
                        ]), // equalities first, then inequalities, preserving order of appearance
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the final inequality (score) is > 50
                        // to is incl and the final component is omitted because there is no score < ? in the predicate
                        bounds: bounds_list!(
                            col_range!("__collection" => "album"..="album"),
                            col_range!("age" => 30..=30),
                            open_lower!("score" => 50..)
                        ),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("score")])
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND score > 50 AND age = 30").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("score")])
                    }
                ]
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
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album'").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_unsupported_operators() {
            // Queries with operators that can't be used for index ranges
            assert_eq!(
                plan!("__collection = 'album' AND name != 'Alice'"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: selection!("name != 'Alice'").predicate,
                        order_by_spill: order_by_components!(),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND name != 'Alice'").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );

            // Mixed supported and unsupported
            assert_eq!(
                plan!("__collection = 'album' AND age > 25 AND name != 'Alice'"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (age) is > 25
                        // to is incl and the final component is omitted because there is no age < ? in the predicate
                        bounds: bounds_list!(col_range!("__collection" => "album"..="album"), open_lower!("age" => 25..)),
                        remaining_predicate: selection!("name != 'Alice'").predicate,
                        order_by_spill: order_by_components!(),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age > 25 AND name != 'Alice'").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_impossible_range() {
            // Conflicting inequalities that create impossible range
            // EmptyScan is mutually exclusive with TableScan
            assert_eq!(plan!("__collection = 'album' AND age > 50 AND age < 30"), vec![Plan::EmptyScan]);
        }

        #[test]
        fn test_or_only_predicate() {
            // Predicate with only OR - __collection conjunct should be extractable
            assert_eq!(
                plan!("__collection = 'album' AND (age > 25 OR name = 'Alice')"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        // the OR-containing parenthetical is a disjunction, so it must remain in the predicate
                        remaining_predicate: selection!("age > 25 OR name = 'Alice'").predicate,
                        order_by_spill: order_by_components!(),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND (age > 25 OR name = 'Alice')").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_complex_nested_predicate() {
            // Complex nesting that should still extract some conjuncts
            assert_eq!(
                plan!("__collection = 'album' AND score = 100 AND (age > 25 OR name = 'Alice')"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("score", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "score" => (100..=100)),
                        // the OR-containing parenthetical is a disjunction, so it must remain in the predicate
                        remaining_predicate: selection!("age > 25 OR name = 'Alice'").predicate,
                        order_by_spill: order_by_components!(),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND score = 100 AND (age > 25 OR name = 'Alice')")
                            .predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_order_by_with_no_matching_predicate() {
            // This might already be covered above
            // ORDER BY field that doesn't appear in WHERE clause
            assert_eq!(
                plan!("__collection = 'album' AND age = 30 ORDER BY name, score"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("age", ValueType::I32),
                            asc!("name", ValueType::String),
                            asc!("score", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (30..=30)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(presort: [oby_asc!("name"), oby_asc!("score")]),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age = 30").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("name"), oby_asc!("score")])
                    }
                ]
            );
        }

        #[test]
        fn test_inequality_different_field_than_order_by() {
            // Query: year >= '2001' ORDER BY name
            // Two correct strategies:
            // 1. Scan by NAME, filter by YEAR (ordering free, filtering costs)
            // 2. Scan by YEAR, sort by NAME (filtering free, sorting costs)
            //
            // Note: For strategy 2, presort is empty because 'year' is NOT in the ORDER BY.
            // We can only presort by ORDER BY columns that the index satisfies.
            // Since ORDER BY is 'name ASC' and the index is on 'year', we need global sort.
            assert_eq!(
                plan!("__collection = 'album' AND year >= '2001' ORDER BY name"),
                vec![
                    // Strategy 1: Scan by name, filter by year
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("name", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album")),
                        remaining_predicate: Predicate::Comparison {
                            left: Box::new(Expr::Path(ankql::ast::PathExpr::simple("year"))),
                            operator: ComparisonOperator::GreaterThanOrEqual,
                            right: Box::new(Expr::Literal(ankql::ast::Literal::String("2001".to_string()))),
                        },
                        order_by_spill: order_by_components!(presort: [oby_asc!("name")]),
                    },
                    // Strategy 2: Scan by year, sort by name (global sort needed since 'year' not in ORDER BY)
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("year", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "year" => ("2001"..)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(spill: [oby_asc!("name")]),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND year >= '2001'").predicate,
                        order_by_spill: order_by_components!(spill: [oby_asc!("name")])
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
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("age", ValueType::I32)]),
                        scan_direction: ScanDirection::Forward,
                        // age >= 25 wins over age > 20 because it's more restrictive
                        // from: is incl because the most restrictive lower bound inequality (age) is >= 25
                        // to: is incl because the upper bound inequality (age) is <= 50
                        bounds: bounds!("__collection" => ("album"..="album"), "age" => (25..=50)),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!(),
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND age >= 25 AND age <= 50 AND age > 20").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_large_numbers() {
            // Test with very large numbers
            assert_eq!(
                plan!("__collection = 'album' AND timestamp > 9223372036854775807"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("timestamp", ValueType::I64)]),
                        scan_direction: ScanDirection::Forward,
                        // from is excl because the inequality (timestamp) is > 9223372036854775807
                        // to is incl because there is no timestamp < ? in the predicate
                        // can't use closed lower bound via rust syntax - so we construct the bounds manually
                        bounds: bounds_list!(
                            col_range!("__collection" => "album"..="album"),
                            open_lower!("timestamp" => 9223372036854775807i64..)
                        ),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND timestamp > 9223372036854775807").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_empty_string_equality() {
            // Test that empty strings are handled correctly in equality comparisons
            assert_eq!(
                plan!("__collection = 'album' AND name = ''"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("name", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "name" => (""..="")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND name = ''").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_empty_string_with_other_fields() {
            // Test empty string with other fields
            assert_eq!(
                plan!("__collection = 'album' AND name = '' AND year = '2000'"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![
                            asc!("__collection", ValueType::String),
                            asc!("name", ValueType::String),
                            asc!("year", ValueType::String)
                        ]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "name" => (""..=""), "year" => ("2000"..="2000")),
                        remaining_predicate: Predicate::True,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: KeyBounds::empty(),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!("__collection = 'album' AND name = '' AND year = '2000'").predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }

        #[test]
        fn test_primary_key_only_equality() {
            // Primary key-only queries should not generate index plans, only table scan with ID range
            assert_eq!(
                plan!("id = '12345678-1234-1234-1234-123456789abc'"),
                vec![Plan::TableScan {
                    bounds: bounds!("id" => ("12345678-1234-1234-1234-123456789abc"..="12345678-1234-1234-1234-123456789abc")),
                    scan_direction: ScanDirection::Forward,
                    remaining_predicate: selection!("id = '12345678-1234-1234-1234-123456789abc'").predicate,
                    order_by_spill: order_by_components!()
                }]
            );
        }

        #[test]
        fn test_primary_key_only_with_order_by() {
            // TODO: Primary key ORDER BY should be satisfied by table scan direction, not index
            // Currently the planner generates an index plan for 'id' field
            assert_eq!(
                plan!("id > '12345678-1234-1234-1234-123456789abc' ORDER BY id DESC"),
                vec![Plan::TableScan {
                    bounds: bounds_list!(open_lower!("id" => "12345678-1234-1234-1234-123456789abc"..)),
                    scan_direction: ScanDirection::Reverse, // DESC ORDER BY on primary key
                    remaining_predicate: selection!("id > '12345678-1234-1234-1234-123456789abc'").predicate,
                    order_by_spill: order_by_components!(presort: [oby_desc!("id")]) // Primary key ORDER BY satisfied by scan direction
                }]
            );
        }

        #[test]
        fn test_primary_key_with_non_primary_order_by() {
            // TODO: Primary key predicate should use table scan with ID range, not index
            // Currently the planner generates an index plan for 'id' field
            assert_eq!(
                plan!("id = '12345678-1234-1234-1234-123456789abc' ORDER BY name ASC"),
                vec![Plan::TableScan {
                    bounds: bounds!("id" => ("12345678-1234-1234-1234-123456789abc"..="12345678-1234-1234-1234-123456789abc")),
                    scan_direction: ScanDirection::Forward, // name ORDER BY not on primary key
                    // TODO this should be ::true because the id predicate is covered by the table scan
                    remaining_predicate: selection!("id = '12345678-1234-1234-1234-123456789abc'").predicate,
                    order_by_spill: order_by_components!(spill: [oby_asc!("name")]) // name ORDER BY must be sorted in-memory
                }]
            );
        }

        #[test]
        fn test_primary_key_not_equal() {
            // Primary key != should still generate index plans (can't use efficient ID ranges)
            // But no bounds can be extracted from !=, so it becomes a basic collection scan
            assert_eq!(
                plan!("id != '12345678-1234-1234-1234-123456789abc'"),
                vec![Plan::TableScan {
                    bounds: KeyBounds::empty(), // No range extraction for != operator
                    scan_direction: ScanDirection::Forward,
                    remaining_predicate: selection!("id != '12345678-1234-1234-1234-123456789abc'").predicate,
                    order_by_spill: order_by_components!()
                }]
            );
        }

        #[test]
        fn test_no_predicate_no_order_by() {
            // Query with no WHERE and no ORDER BY - pure table scan
            assert_eq!(
                plan!("true"),
                vec![Plan::TableScan {
                    bounds: KeyBounds::empty(),
                    scan_direction: ScanDirection::Forward,
                    remaining_predicate: ankql::ast::Predicate::True,
                    order_by_spill: order_by_components!()
                }]
            );
        }

        #[test]
        fn test_no_predicate_with_order_by() {
            // Query with no WHERE but ORDER BY - should use table scan with appropriate direction
            assert_eq!(
                plan!("true ORDER BY id DESC"),
                vec![Plan::TableScan {
                    bounds: KeyBounds::empty(),
                    scan_direction: ScanDirection::Reverse, // DESC ORDER BY on primary key
                    remaining_predicate: ankql::ast::Predicate::True,
                    order_by_spill: order_by_components!(presort: [oby_desc!("id")]) // Primary key ORDER BY satisfied by scan direction
                }]
            );
        }

        #[test]
        fn test_primary_key_range_intersection() {
            // Multiple primary key constraints should be intersected into most restrictive range
            assert_eq!(
                plan!("id >= '12345678-1234-1234-1234-123456789aaa' AND id <= '12345678-1234-1234-1234-123456789zzz'"),
                vec![Plan::TableScan {
                    bounds: bounds!("id" => ("12345678-1234-1234-1234-123456789aaa"..="12345678-1234-1234-1234-123456789zzz")),
                    scan_direction: ScanDirection::Forward,
                    remaining_predicate: selection!(
                        "id >= '12345678-1234-1234-1234-123456789aaa' AND id <= '12345678-1234-1234-1234-123456789zzz'"
                    )
                    .predicate,
                    order_by_spill: order_by_components!()
                }]
            );
        }

        #[test]
        fn test_mixed_primary_and_secondary_predicates() {
            // Mix of primary key and other predicates should generate both index and table scan plans
            assert_eq!(
                plan!("__collection = 'album' AND id > '12345678-1234-1234-1234-123456789abc' AND name = 'Alice'"),
                vec![
                    Plan::Index {
                        index_spec: KeySpec::new(vec![asc!("__collection", ValueType::String), asc!("name", ValueType::String)]),
                        scan_direction: ScanDirection::Forward,
                        bounds: bounds!("__collection" => ("album"..="album"), "name" => ("Alice"..="Alice")),
                        remaining_predicate: selection!("id > '12345678-1234-1234-1234-123456789abc'").predicate,
                        order_by_spill: order_by_components!()
                    },
                    Plan::TableScan {
                        bounds: bounds_list!(open_lower!("id" => "12345678-1234-1234-1234-123456789abc"..)),
                        scan_direction: ScanDirection::Forward,
                        remaining_predicate: selection!(
                            "__collection = 'album' AND id > '12345678-1234-1234-1234-123456789abc' AND name = 'Alice'"
                        )
                        .predicate,
                        order_by_spill: order_by_components!()
                    }
                ]
            );
        }
    }

    mod json_path_tests {
        use super::*;

        /// Test that multi-step paths (e.g., context.session_id) are correctly handled
        #[test]
        fn test_json_path_equality() {
            let planner = Planner::new(PlannerConfig::full_support());
            let selection = selection!("context.session_id = 'sess123'");
            let plans = planner.plan(&selection, "id");

            // Should generate an index plan with sub_path
            let index_plan = plans.iter().find(|p| matches!(p, Plan::Index { .. })).expect("Should generate index plan");

            if let Plan::Index { index_spec, bounds, .. } = index_plan {
                // Check that the keypart has the correct sub_path
                assert_eq!(index_spec.keyparts.len(), 1);
                let keypart = &index_spec.keyparts[0];
                assert_eq!(keypart.column, "context");
                assert_eq!(keypart.sub_path, Some(vec!["session_id".to_string()]));
                assert_eq!(keypart.full_path(), "context.session_id");

                // Check bounds use full path
                assert_eq!(bounds.keyparts.len(), 1);
                assert_eq!(bounds.keyparts[0].column, "context.session_id");
            } else {
                panic!("Expected Index plan");
            }
        }

        /// Test multi-step path with ORDER BY
        #[test]
        fn test_json_path_with_order_by() {
            let planner = Planner::new(PlannerConfig::full_support());
            let selection = selection!("context.user_id = 'user123' ORDER BY created DESC");
            let plans = planner.plan(&selection, "id");

            let index_plan = plans.iter().find(|p| matches!(p, Plan::Index { .. })).expect("Should generate index plan");

            if let Plan::Index { index_spec, .. } = index_plan {
                // First keypart should be the JSON path equality
                let first = &index_spec.keyparts[0];
                assert_eq!(first.column, "context");
                assert_eq!(first.sub_path, Some(vec!["user_id".to_string()]));

                // ORDER BY field should be second (simple path)
                if index_spec.keyparts.len() > 1 {
                    let second = &index_spec.keyparts[1];
                    assert_eq!(second.column, "created");
                    assert_eq!(second.sub_path, None);
                }
            }
        }

        /// Test deeper JSON paths
        #[test]
        fn test_deep_json_path() {
            let planner = Planner::new(PlannerConfig::full_support());
            let selection = selection!("data.nested.field = 'value'");
            let plans = planner.plan(&selection, "id");

            let index_plan = plans.iter().find(|p| matches!(p, Plan::Index { .. })).expect("Should generate index plan");

            if let Plan::Index { index_spec, .. } = index_plan {
                let keypart = &index_spec.keyparts[0];
                assert_eq!(keypart.column, "data");
                assert_eq!(keypart.sub_path, Some(vec!["nested".to_string(), "field".to_string()]));
                assert_eq!(keypart.full_path(), "data.nested.field");
            }
        }

        /// Test that JSON path equality has no remaining predicate (fully pushed down)
        #[test]
        fn test_json_path_full_pushdown() {
            let planner = Planner::new(PlannerConfig::full_support());
            let selection = selection!("context.session_id = 'sess123'");
            let plans = planner.plan(&selection, "id");

            let index_plan = plans.iter().find(|p| matches!(p, Plan::Index { .. })).expect("Should generate index plan");

            if let Plan::Index { remaining_predicate, .. } = index_plan {
                // Full pushdown: remaining_predicate should be True
                assert_eq!(*remaining_predicate, Predicate::True, "JSON path equality should be fully pushed down");
            } else {
                panic!("Expected Index plan");
            }
        }

        /// Test JSON path with inequality (partial pushdown for range)
        #[test]
        fn test_json_path_inequality() {
            let planner = Planner::new(PlannerConfig::full_support());
            let selection = selection!("context.count > 100");
            let plans = planner.plan(&selection, "id");

            // Should still generate an index plan
            let index_plan = plans.iter().find(|p| matches!(p, Plan::Index { .. })).expect("Should generate index plan for inequality");

            if let Plan::Index { index_spec, remaining_predicate, .. } = index_plan {
                let keypart = &index_spec.keyparts[0];
                assert_eq!(keypart.column, "context");
                assert_eq!(keypart.sub_path, Some(vec!["count".to_string()]));

                // Inequality should be fully pushed to bounds, remaining_predicate is True
                assert_eq!(*remaining_predicate, Predicate::True, "JSON path inequality should be fully pushed down");
            }
        }

        /// Test mixed: JSON path + regular field
        #[test]
        fn test_json_path_mixed_predicates() {
            let planner = Planner::new(PlannerConfig::full_support());
            let selection = selection!("status = 'active' AND context.user_id = 'user123'");
            let plans = planner.plan(&selection, "id");

            // Should have an index plan
            let index_plan = plans.iter().find(|p| matches!(p, Plan::Index { .. })).expect("Should generate index plan");

            if let Plan::Index { index_spec, remaining_predicate, .. } = index_plan {
                // Should have 2 keyparts
                assert_eq!(index_spec.keyparts.len(), 2, "Should have 2 keyparts for mixed query");

                // Find the JSON path keypart
                let json_keypart = index_spec.keyparts.iter().find(|kp| kp.sub_path.is_some());
                assert!(json_keypart.is_some(), "Should have a keypart with sub_path");

                let json_kp = json_keypart.unwrap();
                assert_eq!(json_kp.column, "context");
                assert_eq!(json_kp.sub_path, Some(vec!["user_id".to_string()]));

                // Both should be fully pushed, remaining is True
                assert_eq!(*remaining_predicate, Predicate::True, "Both predicates should be pushed down");
            }
        }
    }

    /// Tests for order_by_spill verification
    /// Verify correct column order, directions, and LIMIT interaction
    mod order_by_spill_tests {
        use super::*;

        #[test]
        fn test_spill_preserves_column_order() {
            // ORDER BY a ASC, b DESC, c ASC with index on (a) only
            // Spill should contain [b DESC, c ASC] in that order
            let plans = plan!("__collection = 'album' ORDER BY a ASC, b DESC, c ASC");
            let index_plan = &plans[0];

            if let Plan::Index { order_by_spill, .. } = index_plan {
                // First column (a) is satisfied by index, remaining are spilled
                assert_eq!(order_by_spill.presort.len(), 1);
                assert_eq!(order_by_spill.presort[0].path.property(), "a");

                assert_eq!(order_by_spill.spill.len(), 2);
                // Verify order: b comes before c
                assert_eq!(order_by_spill.spill[0].path.property(), "b");
                assert_eq!(order_by_spill.spill[1].path.property(), "c");
            } else {
                panic!("Expected Index plan");
            }
        }

        #[test]
        fn test_spill_preserves_directions() {
            // ORDER BY a ASC, b DESC, c ASC with index on (a) only
            // Spill should preserve directions: b DESC, c ASC
            let plans = plan!("__collection = 'album' ORDER BY a ASC, b DESC, c ASC");
            let index_plan = &plans[0];

            if let Plan::Index { order_by_spill, .. } = index_plan {
                assert_eq!(order_by_spill.spill.len(), 2);
                // Verify directions
                assert_eq!(order_by_spill.spill[0].direction, ankql::ast::OrderDirection::Desc);
                assert_eq!(order_by_spill.spill[1].direction, ankql::ast::OrderDirection::Asc);
            } else {
                panic!("Expected Index plan");
            }
        }

        #[test]
        fn test_spill_with_limit() {
            // LIMIT should not affect whether spill is needed
            // ORDER BY a ASC, b DESC LIMIT 10 with index on (a)
            // Spill should still contain [b DESC]
            let selection = selection!("__collection = 'album' ORDER BY a ASC, b DESC LIMIT 10");
            let planner = Planner::new(PlannerConfig::indexeddb());
            let plans = planner.plan(&selection, "id");
            let index_plan = &plans[0];

            if let Plan::Index { order_by_spill, .. } = index_plan {
                // a is presorted, b is spilled regardless of LIMIT
                assert_eq!(order_by_spill.presort.len(), 1);
                assert_eq!(order_by_spill.presort[0].path.property(), "a");
                assert_eq!(order_by_spill.spill.len(), 1);
                assert_eq!(order_by_spill.spill[0].path.property(), "b");
                assert_eq!(order_by_spill.spill[0].direction, ankql::ast::OrderDirection::Desc);
            } else {
                panic!("Expected Index plan");
            }
        }

        #[test]
        fn test_table_scan_spill_matches_full_order_by() {
            // TableScan should spill the full ORDER BY since it has no index
            let plans = plan!("__collection = 'album' ORDER BY x DESC, y ASC, z DESC");
            let table_plan = plans.iter().find(|p| matches!(p, Plan::TableScan { .. })).expect("Should have TableScan");

            if let Plan::TableScan { order_by_spill, .. } = table_plan {
                // TableScan has no presort, full spill
                assert!(order_by_spill.presort.is_empty());
                assert_eq!(order_by_spill.spill.len(), 3);
                // Verify all columns and directions
                assert_eq!(order_by_spill.spill[0].path.property(), "x");
                assert_eq!(order_by_spill.spill[0].direction, ankql::ast::OrderDirection::Desc);
                assert_eq!(order_by_spill.spill[1].path.property(), "y");
                assert_eq!(order_by_spill.spill[1].direction, ankql::ast::OrderDirection::Asc);
                assert_eq!(order_by_spill.spill[2].path.property(), "z");
                assert_eq!(order_by_spill.spill[2].direction, ankql::ast::OrderDirection::Desc);
            } else {
                panic!("Expected TableScan plan");
            }
        }

        #[test]
        fn test_no_spill_when_fully_satisfied() {
            // When index fully satisfies ORDER BY, spill should be empty
            // ORDER BY a ASC with index on (collection, a) ASC
            let plans = plan!("__collection = 'album' ORDER BY a");
            let index_plan = &plans[0];

            if let Plan::Index { order_by_spill, .. } = index_plan {
                // All ORDER BY satisfied by index
                assert_eq!(order_by_spill.presort.len(), 1);
                assert_eq!(order_by_spill.presort[0].path.property(), "a");
                assert!(order_by_spill.spill.is_empty(), "Spill should be empty when ORDER BY is fully satisfied");
            } else {
                panic!("Expected Index plan");
            }
        }

        #[test]
        fn test_equality_prefix_affects_spill() {
            // With equality prefix, ORDER BY columns after the equality should not spill
            // a = 'x' AND b = 'y' ORDER BY c ASC
            // Index on (a, b, c) satisfies the ORDER BY because a,b are fixed by equality
            let plans = plan!("__collection = 'album' AND category = 'rock' ORDER BY rating");
            let index_plan = &plans[0];

            if let Plan::Index { order_by_spill, .. } = index_plan {
                // rating ORDER BY is satisfied after equality prefix
                assert_eq!(order_by_spill.presort.len(), 1);
                assert_eq!(order_by_spill.presort[0].path.property(), "rating");
                assert!(order_by_spill.spill.is_empty(), "No spill needed with equality prefix + ORDER BY");
            } else {
                panic!("Expected Index plan");
            }
        }
    }
}
