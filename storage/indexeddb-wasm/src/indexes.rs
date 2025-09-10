/// Index specification for on-demand creation
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSpec {
    name: String,
    fields: Vec<IndexField>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexField {
    name: String,
    direction: IndexDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RangeConstraint {
    /// Start scanning from this key (inclusive)
    StartFrom(ConstraintValue),
    /// End scanning at this key (inclusive)  
    EndAt(ConstraintValue),
    /// Exact match
    Exact(ConstraintValue),
    /// Scan all records for this collection
    CollectionOnly,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

impl ConstraintValue {
    /// Convert to JsValue when needed for IndexedDB operations
    pub fn to_js_value(&self) -> wasm_bindgen::JsValue {
        match self {
            ConstraintValue::String(s) => wasm_bindgen::JsValue::from_str(s),
            ConstraintValue::Integer(i) => wasm_bindgen::JsValue::from_f64(*i as f64),
            ConstraintValue::Float(f) => wasm_bindgen::JsValue::from_f64(*f),
            ConstraintValue::Boolean(b) => wasm_bindgen::JsValue::from_bool(*b),
        }
    }
}

impl RangeConstraint {
    /// Convert RangeConstraint to IndexedDB IdbKeyRange
    /// The collection_id is always the first part of our composite index keys
    pub fn to_idb_key_range(&self, collection_id: &ankurah_proto::CollectionId) -> Result<web_sys::IdbKeyRange, wasm_bindgen::JsValue> {
        use js_sys::Array;
        use wasm_bindgen::JsValue;

        let collection_js = JsValue::from_str(&collection_id.to_string());

        match self {
            RangeConstraint::CollectionOnly => {
                // Exact match on collection only: [collection]
                web_sys::IdbKeyRange::only(&collection_js)
            }
            RangeConstraint::Exact(value) => {
                // Exact match on [collection, value]
                let key_array = Array::new_with_length(2);
                key_array.set(0, collection_js);
                key_array.set(1, value.to_js_value());
                web_sys::IdbKeyRange::only(&key_array)
            }
            RangeConstraint::StartFrom(value) => {
                // Range from [collection, value] (inclusive) to end of collection
                // We use lowerBound to start from [collection, value] and scan forward
                let lower_bound = Array::new_with_length(2);
                lower_bound.set(0, collection_js);
                lower_bound.set(1, value.to_js_value());

                web_sys::IdbKeyRange::lower_bound(&lower_bound)
            }
            RangeConstraint::EndAt(value) => {
                // Range from start of collection to [collection, value] (inclusive)
                // We use upperBound to end at [collection, value]
                let upper_bound = Array::new_with_length(2);
                upper_bound.set(0, collection_js);
                upper_bound.set(1, value.to_js_value());

                web_sys::IdbKeyRange::upper_bound(&upper_bound)
            }
        }
    }
}

impl IndexSpec {
    pub fn new(field_name: String, direction: IndexDirection) -> Self {
        // IndexedDB limitation: indexes are always ascending, direction only affects cursor scan
        // So we don't include direction in the index name - it's determined at query time
        let name = format!("__collection__{}", field_name);

        IndexSpec {
            name,
            fields: vec![
                IndexField { name: "__collection".to_string(), direction: IndexDirection::Asc },
                IndexField { name: field_name, direction }, // This direction is for cursor scanning, not index structure
            ],
        }
    }

    pub fn name(&self) -> &str { &self.name }

    pub fn fields(&self) -> &[IndexField] { &self.fields }
}

impl IndexField {
    pub fn name(&self) -> &str { &self.name }

    pub fn direction(&self) -> &IndexDirection { &self.direction }
}

pub struct Optimizer {}

impl Optimizer {
    pub fn new() -> Self { Self {} }

    /// Validate that the selection is compatible with IndexedDB limitations
    fn validate_indexeddb_compatibility(&self, selection: &ankql::ast::Selection) -> Result<(), anyhow::Error> {
        // IndexedDB limitation: We can scan indexes in reverse, but the index structure is always ascending
        // This is different from other storage engines like Sled that can have truly descending indexes

        // FIXME this does not seem correct. its ok to have multiple ORDER BY fields, as long as they are all the same direction
        if let Some(order_by) = &selection.order_by {
            for item in order_by {
                if matches!(item.direction, ankql::ast::OrderDirection::Desc) {
                    tracing::warn!("IndexedDB limitation: ORDER BY DESC uses reverse cursor scan, not descending index");
                }
            }
        }

        Ok(())
    }

    /// Select the best index for a given selection
    /// Returns (IndexSpec, scan_direction, range_constraint)
    pub fn pick_index(
        &self,
        selection: &ankql::ast::Selection,
    ) -> Result<(IndexSpec, IndexDirection, Option<RangeConstraint>), anyhow::Error> {
        // Validate IndexedDB compatibility
        self.validate_indexeddb_compatibility(selection)?;
        // Rule 1: Check ORDER BY clause first (takes priority)
        if let Some(order_by) = &selection.order_by {
            let (index_spec, direction) = self.handle_order_by(order_by)?;

            // Check if we can optimize with a range constraint from predicate
            let range_constraint = self.extract_range_constraint(&selection.predicate, index_spec.fields()[0].name(), &direction);

            return Ok((index_spec, direction, range_constraint));
        }

        // Rule 2: Look for top-level conjuncts (comparisons not in OR branches)
        if let Some((field_name, direction)) = self.find_top_level_conjunct(&selection.predicate) {
            let index_spec = IndexSpec::new(field_name.clone(), direction.clone());

            // Extract range constraint for the selected field
            let range_constraint = self.extract_range_constraint(&selection.predicate, &field_name, &direction);

            return Ok((index_spec, direction, range_constraint));
        }

        // Rule 3: Default to collection + id index
        Ok((IndexSpec::new("id".to_string(), IndexDirection::Asc), IndexDirection::Asc, None))
    }

    /// Handle ORDER BY clause validation and index selection
    fn handle_order_by(&self, order_by: &[ankql::ast::OrderByItem]) -> Result<(IndexSpec, IndexDirection), anyhow::Error> {
        if order_by.is_empty() {
            return Err(anyhow::anyhow!("Empty ORDER BY clause"));
        }

        // Check if all directions are the same
        let first_direction = &order_by[0].direction;
        let all_same_direction = order_by.iter().all(|item| &item.direction == first_direction);

        if !all_same_direction {
            return Err(anyhow::anyhow!("Mixed ORDER BY directions not supported (ASC + DESC)"));
        }

        // For now, only support single field ORDER BY (we can extend this later)
        if order_by.len() > 1 {
            return Err(anyhow::anyhow!("Multiple ORDER BY fields not yet supported"));
        }

        let order_item = &order_by[0];
        let field_name = match &order_item.identifier {
            ankql::ast::Identifier::Property(name) => name.clone(),
            ankql::ast::Identifier::CollectionProperty(_, name) => name.clone(),
        };

        let index_direction = match order_item.direction {
            ankql::ast::OrderDirection::Asc => IndexDirection::Asc,
            ankql::ast::OrderDirection::Desc => IndexDirection::Desc,
        };

        Ok((IndexSpec::new(field_name, index_direction.clone()), index_direction))
    }

    /// Find the first comparison that's a top-level conjunct (not in an OR branch)
    /// Returns (field_name, scan_direction)
    fn find_top_level_conjunct(&self, predicate: &ankql::ast::Predicate) -> Option<(String, IndexDirection)> {
        self.find_conjuncts_recursive(predicate, true)
    }

    /// Extract range constraint for a specific field from the predicate
    /// Returns None if no suitable constraint found or if constraint is incompatible with scan direction
    fn extract_range_constraint(
        &self,
        predicate: &ankql::ast::Predicate,
        target_field: &str,
        scan_direction: &IndexDirection,
    ) -> Option<RangeConstraint> {
        self.find_field_constraint(predicate, target_field, scan_direction, true)
    }

    /// Recursively search for constraints on the target field
    /// TODO - better define what "constraint" means here
    fn find_field_constraint(
        &self,
        predicate: &ankql::ast::Predicate,
        target_field: &str,
        scan_direction: &IndexDirection,
        is_top_level: bool,
    ) -> Option<RangeConstraint> {
        match predicate {
            ankql::ast::Predicate::Comparison { left, operator, right } => {
                if !is_top_level {
                    return None; // Skip comparisons in OR branches
                }

                // Check if this comparison is for our target field
                let field_name = match &**left {
                    ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property(name)) => name,
                    ankql::ast::Expr::Identifier(ankql::ast::Identifier::CollectionProperty(_, name)) => name,
                    _ => return None,
                };

                if field_name != target_field {
                    return None;
                }

                // Extract the comparison value
                let constraint_value = match &**right {
                    ankql::ast::Expr::Literal(literal) => self.literal_to_constraint_value(literal)?,
                    _ => return None, // Skip non-literal comparisons for now
                };

                // Check operator compatibility with scan direction
                match (operator, scan_direction) {
                    // Forward scan compatible operators
                    (ankql::ast::ComparisonOperator::Equal, _) => {
                        // Exact match works with any direction
                        Some(RangeConstraint::Exact(constraint_value))
                    }
                    (
                        ankql::ast::ComparisonOperator::GreaterThan | ankql::ast::ComparisonOperator::GreaterThanOrEqual,
                        IndexDirection::Asc,
                    ) => {
                        // Start from value, scan forward
                        Some(RangeConstraint::StartFrom(constraint_value))
                    }
                    (ankql::ast::ComparisonOperator::LessThan | ankql::ast::ComparisonOperator::LessThanOrEqual, IndexDirection::Desc) => {
                        // Start from value, scan backward
                        Some(RangeConstraint::StartFrom(constraint_value))
                    }
                    // Incompatible combinations
                    _ => None,
                }
            }
            ankql::ast::Predicate::And(left, right) => {
                // Check both sides, prefer the first valid constraint
                self.find_field_constraint(left, target_field, scan_direction, is_top_level)
                    .or_else(|| self.find_field_constraint(right, target_field, scan_direction, is_top_level))
            }
            ankql::ast::Predicate::Or(left, right) => {
                // Neither side is a top-level conjunct anymore
                self.find_field_constraint(left, target_field, scan_direction, false)
                    .or_else(|| self.find_field_constraint(right, target_field, scan_direction, false))
            }
            _ => None,
        }
    }

    /// Convert AST literal to ConstraintValue
    fn literal_to_constraint_value(&self, literal: &ankql::ast::Literal) -> Option<ConstraintValue> {
        match literal {
            ankql::ast::Literal::String(s) => Some(ConstraintValue::String(s.clone())),
            ankql::ast::Literal::Integer(i) => Some(ConstraintValue::Integer(*i)),
            ankql::ast::Literal::Float(f) => Some(ConstraintValue::Float(*f)),
            ankql::ast::Literal::Boolean(b) => Some(ConstraintValue::Boolean(*b)),
        }
    }

    /// Recursively search for comparisons, tracking whether we're in a top-level conjunct
    fn find_conjuncts_recursive(&self, predicate: &ankql::ast::Predicate, is_top_level: bool) -> Option<(String, IndexDirection)> {
        match predicate {
            ankql::ast::Predicate::Comparison { left, operator, right: _ } => {
                if !is_top_level {
                    return None; // Skip comparisons in OR branches
                }

                // Extract field name from left side (assuming left is always the field)
                let field_name = match &**left {
                    ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property(name)) => name.clone(),
                    ankql::ast::Expr::Identifier(ankql::ast::Identifier::CollectionProperty(_, name)) => name.clone(),
                    _ => return None, // Skip non-identifier comparisons
                };

                // Map comparison operator to scan direction
                let direction = match operator {
                    ankql::ast::ComparisonOperator::Equal => IndexDirection::Asc, // Direction doesn't matter for equality
                    ankql::ast::ComparisonOperator::GreaterThan | ankql::ast::ComparisonOperator::GreaterThanOrEqual => IndexDirection::Asc, // Scan forward
                    ankql::ast::ComparisonOperator::LessThan | ankql::ast::ComparisonOperator::LessThanOrEqual => IndexDirection::Desc, // Scan backward
                    ankql::ast::ComparisonOperator::In => IndexDirection::Asc, // Default to forward for IN
                    ankql::ast::ComparisonOperator::Between => IndexDirection::Asc, // Default to forward for BETWEEN
                    ankql::ast::ComparisonOperator::NotEqual => return None,   // Skip != as it's not range-friendly
                };

                Some((field_name, direction))
            }
            ankql::ast::Predicate::And(left, right) => {
                // Both sides are still top-level conjuncts
                self.find_conjuncts_recursive(left, is_top_level).or_else(|| self.find_conjuncts_recursive(right, is_top_level))
            }
            ankql::ast::Predicate::Or(left, right) => {
                // Neither side is a top-level conjunct anymore
                self.find_conjuncts_recursive(left, false).or_else(|| self.find_conjuncts_recursive(right, false))
            }
            ankql::ast::Predicate::Not(inner) => {
                // NOT inverts the logic, but we'll skip it for simplicity
                self.find_conjuncts_recursive(inner, false)
            }
            ankql::ast::Predicate::IsNull(_) => {
                // IS NULL comparisons aren't great for range queries
                None
            }
            ankql::ast::Predicate::True | ankql::ast::Predicate::False | ankql::ast::Predicate::Placeholder => None,
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use ankql::ast::Selection;
    use ankql::parser::parse_selection;

    fn parse_query(query: &str) -> Selection { parse_selection(query).expect("Failed to parse query") }

    #[test]
    fn test_find_conjuncts_simple_comparison() {
        let optimizer = Optimizer::new();

        // Simple comparison: age > 25
        let selection = parse_query("age > 25");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        assert_eq!(result, Some(("age".to_string(), IndexDirection::Asc)));
    }

    #[test]
    fn test_find_conjuncts_different_operators() {
        let optimizer = Optimizer::new();

        // Test different comparison operators
        let test_cases = vec![
            ("score = 100", IndexDirection::Asc),
            ("score > 100", IndexDirection::Asc),
            ("score >= 100", IndexDirection::Asc),
            ("score < 100", IndexDirection::Desc),
            ("score <= 100", IndexDirection::Desc),
            ("score IN (100, 200)", IndexDirection::Asc),
            // Note: BETWEEN is not supported by ankql parser yet
        ];

        for (query, expected_direction) in test_cases {
            let selection = parse_query(query);
            let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);
            assert_eq!(result, Some(("score".to_string(), expected_direction)), "Failed for query: {}", query);
        }
    }

    #[test]
    fn test_find_conjuncts_not_equal_skipped() {
        let optimizer = Optimizer::new();

        // NotEqual should be skipped as it's not range-friendly
        let selection = parse_query("status != 1");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        assert_eq!(result, None);
    }

    #[test]
    fn test_find_conjuncts_and_predicate() {
        let optimizer = Optimizer::new();

        // AND predicate: age > 25 AND score < 100
        let selection = parse_query("age > 25 AND score < 100");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return the first valid comparison (age > 25)
        assert_eq!(result, Some(("age".to_string(), IndexDirection::Asc)));
    }

    #[test]
    fn test_find_conjuncts_or_predicate_ignored() {
        let optimizer = Optimizer::new();

        // OR predicate: age > 25 OR score < 100
        let selection = parse_query("age > 25 OR score < 100");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return None because comparisons in OR are not top-level conjuncts
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_conjuncts_complex_predicate() {
        let optimizer = Optimizer::new();

        // Complex: (age > 25 AND status = 1) OR (score < 100)
        let selection = parse_query("(age > 25 AND status = 1) OR (score < 100)");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return None because the AND is inside an OR
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_conjuncts_top_level_and_with_nested_or() {
        let optimizer = Optimizer::new();

        // Top-level AND with nested OR: age > 25 AND (status = 1 OR status = 2)
        let selection = parse_query("age > 25 AND (status = 1 OR status = 2)");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return the first top-level conjunct (age > 25)
        assert_eq!(result, Some(("age".to_string(), IndexDirection::Asc)));
    }

    #[test]
    fn test_find_conjuncts_not_predicate() {
        let optimizer = Optimizer::new();

        // NOT predicate: NOT (age > 25)
        let selection = parse_query("NOT (age > 25)");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return None because NOT inverts the logic and we skip it
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_conjuncts_is_null_predicate() {
        let optimizer = Optimizer::new();

        // IS NULL predicate: age IS NULL
        let selection = parse_query("age IS NULL");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return None because IS NULL isn't great for range queries
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_conjuncts_constant_predicates() {
        let optimizer = Optimizer::new();

        let test_cases = vec!["TRUE", "FALSE"];

        for query in test_cases {
            let selection = parse_query(query);
            let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);
            assert_eq!(result, None, "Constant predicate should return None for: {}", query);
        }
    }

    #[test]
    fn test_find_conjuncts_collection_property() {
        let optimizer = Optimizer::new();

        // Test with CollectionProperty identifier (users.age > 25)
        let selection = parse_query("users.age > 25");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should extract the field name from CollectionProperty
        assert_eq!(result, Some(("age".to_string(), IndexDirection::Asc)));
    }

    #[test]
    fn test_find_conjuncts_non_identifier_left_side() {
        let optimizer = Optimizer::new();

        // Test with non-identifier on left side (25 > age - should be skipped)
        let selection = parse_query("25 > age");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return None because left side is not an identifier
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_conjuncts_precedence_in_and_chain() {
        let optimizer = Optimizer::new();

        // Chain of ANDs: age > 25 AND score < 100 AND status = 1
        let selection = parse_query("age > 25 AND score < 100 AND status = 1");
        let result = optimizer.find_conjuncts_recursive(&selection.predicate, true);

        // Should return the first valid comparison found (age > 25)
        assert_eq!(result, Some(("age".to_string(), IndexDirection::Asc)));
    }

    #[test]
    fn test_range_constraint_compatible_combinations() {
        let optimizer = Optimizer::new();

        // Test compatible operator + direction combinations
        let test_cases = vec![
            // Equal works with any direction
            ("age = 25", IndexDirection::Asc, true),
            ("age = 25", IndexDirection::Desc, true),
            // Greater than works with forward scan
            ("age > 25", IndexDirection::Asc, true),
            ("age >= 25", IndexDirection::Asc, true),
            // Less than works with backward scan
            ("age < 25", IndexDirection::Desc, true),
            ("age <= 25", IndexDirection::Desc, true),
            // Incompatible combinations
            ("age > 25", IndexDirection::Desc, false),
            ("age < 25", IndexDirection::Asc, false),
        ];

        for (query, direction, should_have_constraint) in test_cases {
            let selection = parse_query(query);
            let result = optimizer.extract_range_constraint(&selection.predicate, "age", &direction);

            if should_have_constraint {
                assert!(result.is_some(), "Should have constraint for {} + {:?}", query, direction);
            } else {
                assert!(result.is_none(), "Should NOT have constraint for {} + {:?}", query, direction);
            }
        }
    }

    #[test]
    fn test_range_constraint_types() {
        let optimizer = Optimizer::new();

        // Test exact match
        let selection = parse_query("age = 25");
        let result = optimizer.extract_range_constraint(&selection.predicate, "age", &IndexDirection::Asc);
        assert!(matches!(result, Some(RangeConstraint::Exact(ConstraintValue::Integer(25)))));

        // Test start from (forward)
        let selection = parse_query("age > 25");
        let result = optimizer.extract_range_constraint(&selection.predicate, "age", &IndexDirection::Asc);
        assert!(matches!(result, Some(RangeConstraint::StartFrom(ConstraintValue::Integer(25)))));

        // Test start from (backward)
        let selection = parse_query("age < 75");
        let result = optimizer.extract_range_constraint(&selection.predicate, "age", &IndexDirection::Desc);
        assert!(matches!(result, Some(RangeConstraint::StartFrom(ConstraintValue::Integer(75)))));
    }

    #[test]
    fn test_range_constraint_wrong_field_ignored() {
        let optimizer = Optimizer::new();

        // Constraint on different field should be ignored
        let selection = parse_query("score > 100");
        let result = optimizer.extract_range_constraint(&selection.predicate, "age", &IndexDirection::Asc);
        assert!(result.is_none());
    }

    #[test]
    fn test_range_constraint_in_or_branch_ignored() {
        let optimizer = Optimizer::new();

        // Constraint in OR branch should be ignored
        let selection = parse_query("age > 25 OR score < 100");
        let result = optimizer.extract_range_constraint(&selection.predicate, "age", &IndexDirection::Asc);
        assert!(result.is_none());
    }

    #[test]
    fn test_range_constraint_and_predicate() {
        let optimizer = Optimizer::new();

        // Should find constraint in AND predicate
        let selection = parse_query("age > 25 AND score < 100");
        let result = optimizer.extract_range_constraint(&selection.predicate, "age", &IndexDirection::Asc);
        assert!(matches!(result, Some(RangeConstraint::StartFrom(ConstraintValue::Integer(25)))));
    }
}
