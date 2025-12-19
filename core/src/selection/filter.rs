//! Filter items based on a predicate. This is necessary for cases where we are scanning over a set of data
//! which has not been pre-filtered by an index search - or to supplement/validate an index search with additional filtering.

use crate::value::Value;
use ankql::ast::{ComparisonOperator, Expr, Predicate};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("collection mismatch: expected {expected}, got {actual}")]
    CollectionMismatch { expected: String, actual: String },
    #[error("property not found: {0}")]
    PropertyNotFound(String),
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(&'static str),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(&'static str),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprOutput<T> {
    List(Vec<ExprOutput<T>>),
    Value(T),
    None,
}

impl<T: PartialEq> ExprOutput<T> {
    fn as_value(&self) -> Option<&T> {
        match self {
            ExprOutput::Value(v) => Some(v),
            _ => None,
        }
    }

    fn as_list(&self) -> Option<&Vec<ExprOutput<T>>> {
        match self {
            ExprOutput::List(l) => Some(l),
            _ => None,
        }
    }
}

impl ExprOutput<Value> {
    fn is_none(&self) -> bool { matches!(self, ExprOutput::None) }
}

/// Trait for items that can be filtered by predicate evaluation
///
/// Returns typed Values to enable proper comparison with casting
pub trait Filterable {
    fn collection(&self) -> &str;
    fn value(&self, name: &str) -> Option<Value>;
}

fn evaluate_expr<I: Filterable>(item: &I, expr: &Expr) -> Result<ExprOutput<Value>, Error> {
    match expr {
        Expr::Placeholder => Err(Error::PropertyNotFound("Placeholder values must be replaced before filtering".to_string())),
        Expr::Literal(lit) => Ok(ExprOutput::Value(lit.clone().into())),
        Expr::Path(path) => {
            // For simple paths, use the first step as the property name
            if path.is_simple() {
                let name = path.first();
                Ok(ExprOutput::Value(item.value(name).ok_or_else(|| Error::PropertyNotFound(name.to_string()))?))
            } else {
                // Multi-step path - could be:
                // 1. Collection.property (legacy, check if first step matches collection)
                // 2. property.nested.path (JSON traversal)

                let first = path.first();

                // First, check if it's a collection-qualified path
                if first == item.collection() {
                    // Treat remaining path as property access
                    let remaining = &path.steps[1..];
                    if remaining.len() == 1 {
                        // Simple collection.property
                        let name = &remaining[0];
                        return Ok(ExprOutput::Value(item.value(name).ok_or_else(|| Error::PropertyNotFound(name.to_string()))?));
                    }
                    // collection.property.nested... - get property and traverse JSON
                    let property_name = &remaining[0];
                    let json_path = &remaining[1..];
                    return evaluate_json_path(item, property_name, json_path);
                }

                // Not a collection qualifier - treat first step as property, rest as JSON path
                let property_name = first;
                let json_path: Vec<&str> = path.steps[1..].iter().map(|s| s.as_str()).collect();
                evaluate_json_path(item, property_name, &json_path)
            }
        }
        Expr::ExprList(exprs) => {
            let mut result = Vec::new();
            for expr in exprs {
                result.push(evaluate_expr(item, expr)?);
            }
            Ok(ExprOutput::List(result))
        }
        _ => Err(Error::UnsupportedExpression("Only literal, path, and list expressions are supported")),
    }
}

/// Evaluate a JSON path traversal: get property value, parse as JSON, walk path
fn evaluate_json_path<I: Filterable>(item: &I, property_name: &str, json_path: &[impl AsRef<str>]) -> Result<ExprOutput<Value>, Error> {
    let property_value = item.value(property_name).ok_or_else(|| Error::PropertyNotFound(property_name.to_string()))?;

    // The property must be Json or Binary (serialized JSON)
    let json_bytes = match property_value {
        Value::Json(bytes) | Value::Binary(bytes) => bytes,
        _ => {
            // Not a JSON property - can't traverse into it
            return Err(Error::PropertyNotFound(format!(
                "Cannot traverse into non-JSON property '{}' (path: {}.{})",
                property_name,
                property_name,
                json_path.iter().map(|s| s.as_ref()).collect::<Vec<_>>().join(".")
            )));
        }
    };

    // Parse JSON
    let json_value: serde_json::Value = serde_json::from_slice(&json_bytes)
        .map_err(|e| Error::PropertyNotFound(format!("Failed to parse JSON in property '{}': {}", property_name, e)))?;

    // Walk the JSON path
    let mut current = &json_value;
    for step in json_path {
        current = current.get(step.as_ref()).ok_or_else(|| {
            Error::PropertyNotFound(format!(
                "JSON path '{}' not found in property '{}'",
                json_path.iter().map(|s| s.as_ref()).collect::<Vec<_>>().join("."),
                property_name
            ))
        })?;
    }

    // Convert JSON value to our Value type
    Ok(ExprOutput::Value(json_to_value(current)))
}

/// Convert a serde_json::Value to our Value type
fn json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::String("null".to_string()), // Represent null as string for now
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::I64(i)
            } else if let Some(f) = n.as_f64() {
                Value::F64(f)
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            // For nested structures, serialize back to JSON bytes
            Value::Binary(serde_json::to_vec(json).unwrap_or_default())
        }
    }
}

/// Compare two values with automatic casting (for regular schema-typed fields).
/// If types don't match, attempts to cast right to left's type, then left to right's type.
fn compare_values_with_cast(left: &Value, right: &Value, op: impl Fn(&Value, &Value) -> bool) -> bool {
    use crate::value::ValueType;

    // If types match, compare directly
    if ValueType::of(left) == ValueType::of(right) {
        return op(left, right);
    }

    // Try casting right to left's type
    if let Ok(casted_right) = right.cast_to(ValueType::of(left)) {
        return op(left, &casted_right);
    }

    // Try casting left to right's type
    if let Ok(casted_left) = left.cast_to(ValueType::of(right)) {
        return op(&casted_left, right);
    }

    // No valid cast, types incompatible
    false
}

/// Compare two values with JSON-aware casting rules.
///
/// Unlike `compare_values_with_cast`, this function only allows casting within the
/// numeric family (I16, I32, I64, F64). Cross-family casting (e.g., string to number)
/// is not allowed - the comparison returns false if types are incompatible.
///
/// This is the correct semantic for JSON property values, where the actual type
/// can vary per-entity and we don't want to silently coerce mismatched types.
fn compare_json_values(left: &Value, right: &Value, op: impl Fn(&Value, &Value) -> bool) -> bool {
    use crate::value::ValueType;

    let left_type = ValueType::of(left);
    let right_type = ValueType::of(right);

    // If types match exactly, compare directly
    if left_type == right_type {
        return op(left, right);
    }

    // Allow casting within the numeric family only
    if is_numeric_type(left_type) && is_numeric_type(right_type) {
        // Try casting right to left's type
        if let Ok(casted_right) = right.cast_to(left_type) {
            return op(left, &casted_right);
        }
        // Try casting left to right's type
        if let Ok(casted_left) = left.cast_to(right_type) {
            return op(&casted_left, right);
        }
    }

    // Different type families (e.g., string vs number) - no casting, comparison fails
    false
}

/// Check if a ValueType is in the numeric family (can be cast between each other)
fn is_numeric_type(t: crate::value::ValueType) -> bool {
    use crate::value::ValueType;
    matches!(t, ValueType::I16 | ValueType::I32 | ValueType::I64 | ValueType::F64)
}

/// Check if an expression represents a JSON path traversal.
///
/// HACK: Currently we infer "JSON semantics" from multi-step paths (e.g., `data.field`).
/// This is a temporary approximation that works for Phase 1 where only Json properties
/// support nested traversal.
///
/// TODO(Phase 3 - Schema Registry): Replace this heuristic with proper property type lookup.
/// Once we have PropertyId and ModelSchema, we should check if the root property is of type
/// `Json` rather than inferring from path structure. This will be necessary when we add
/// Ref<T> traversal, where `artist.name` traverses a reference, not JSON.
fn is_json_path_expr(expr: &Expr) -> bool { matches!(expr, Expr::Path(path) if !path.is_simple()) }

pub fn evaluate_predicate<I: Filterable>(item: &I, predicate: &Predicate) -> Result<bool, Error> {
    match predicate {
        Predicate::Comparison { left, operator, right } => {
            let left_val = evaluate_expr(item, left)?;
            let right_val = evaluate_expr(item, right)?;

            // HACK: Determine if we should use JSON-aware comparison based on path structure.
            // If either side is a multi-step path (e.g., `data.field`), we assume it's traversing
            // into a Json property and use strict JSON casting rules (numeric family only).
            //
            // TODO(Phase 3 - Schema Registry): Replace this with proper property type lookup.
            // Once we have schema metadata, check if the path's root property is type `Json`
            // rather than inferring from path depth.
            let use_json_comparison = is_json_path_expr(left) || is_json_path_expr(right);

            // Select the appropriate comparison function
            let compare_fn: fn(&Value, &Value, fn(&Value, &Value) -> bool) -> bool =
                if use_json_comparison { compare_json_values } else { compare_values_with_cast };

            Ok(match operator {
                ComparisonOperator::Equal => {
                    left_val.as_value().zip(right_val.as_value()).map(|(l, r)| compare_fn(l, r, |a, b| a == b)).unwrap_or(false)
                }
                ComparisonOperator::NotEqual => {
                    left_val.as_value().zip(right_val.as_value()).map(|(l, r)| compare_fn(l, r, |a, b| a != b)).unwrap_or(false)
                }
                ComparisonOperator::GreaterThan => {
                    left_val.as_value().zip(right_val.as_value()).map(|(l, r)| compare_fn(l, r, |a, b| a > b)).unwrap_or(false)
                }
                ComparisonOperator::GreaterThanOrEqual => {
                    left_val.as_value().zip(right_val.as_value()).map(|(l, r)| compare_fn(l, r, |a, b| a >= b)).unwrap_or(false)
                }
                ComparisonOperator::LessThan => {
                    left_val.as_value().zip(right_val.as_value()).map(|(l, r)| compare_fn(l, r, |a, b| a < b)).unwrap_or(false)
                }
                ComparisonOperator::LessThanOrEqual => {
                    left_val.as_value().zip(right_val.as_value()).map(|(l, r)| compare_fn(l, r, |a, b| a <= b)).unwrap_or(false)
                }
                ComparisonOperator::In => {
                    let value =
                        left_val.as_value().ok_or_else(|| Error::PropertyNotFound("Expected single value for IN left operand".into()))?;
                    let list = right_val.as_list().ok_or_else(|| Error::PropertyNotFound("Expected list for IN right operand".into()))?;
                    list.iter().any(|item| item.as_value().map(|v| compare_fn(value, v, |a, b| a == b)).unwrap_or(false))
                }
                ComparisonOperator::Between => return Err(Error::UnsupportedOperator("BETWEEN operator not yet supported")),
            })
        }
        Predicate::And(left, right) => Ok(evaluate_predicate(item, left)? && evaluate_predicate(item, right)?),
        Predicate::Or(left, right) => Ok(evaluate_predicate(item, left)? || evaluate_predicate(item, right)?),
        Predicate::Not(pred) => Ok(!evaluate_predicate(item, pred)?),
        Predicate::IsNull(expr) => Ok(evaluate_expr(item, expr)?.is_none()),
        Predicate::True => Ok(true),
        Predicate::False => Ok(false),
        // Placeholder should be transformed to a comparison before filtering
        Predicate::Placeholder => Err(Error::PropertyNotFound("Placeholder must be transformed before filtering".to_string())),
    }
}

#[derive(Debug, PartialEq)]
pub enum FilterResult<R> {
    Pass(R),
    Skip(R),
    Error(R, Error),
}

pub struct FilterIterator<I> {
    iter: I,
    predicate: Predicate,
}

impl<I, R> FilterIterator<I>
where
    I: Iterator<Item = R>,
    R: Filterable,
{
    pub fn new(iter: I, predicate: Predicate) -> Self { Self { iter, predicate } }
}

impl<I, R> Iterator for FilterIterator<I>
where
    I: Iterator<Item = R>,
    R: Filterable,
{
    type Item = FilterResult<R>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| match evaluate_predicate(&item, &self.predicate) {
            Ok(true) => FilterResult::Pass(item),
            Ok(false) => FilterResult::Skip(item),
            Err(e) => FilterResult::Error(item, e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::parser::parse_selection;

    #[derive(Debug, Clone, PartialEq)]
    struct TestItem {
        name: String,
        age: String,
    }

    impl Filterable for TestItem {
        fn collection(&self) -> &str { "users" }

        fn value(&self, name: &str) -> Option<Value> {
            match name {
                "name" => Some(Value::String(self.name.clone())),
                "age" => Some(Value::String(self.age.clone())),
                _ => None,
            }
        }
    }

    impl TestItem {
        fn new(name: &str, age: &str) -> Self { Self { name: name.to_string(), age: age.to_string() } }
    }

    #[test]
    fn test_simple_equality() {
        let items = vec![TestItem::new("Alice", "30"), TestItem::new("Bob", "25"), TestItem::new("Charlie", "35")];

        let selection = parse_selection("name = 'Alice'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "30")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Skip(TestItem::new("Charlie", "35")),
            ]
        );
    }

    #[test]
    fn test_and_condition() {
        let items = vec![TestItem::new("Alice", "30"), TestItem::new("Bob", "30"), TestItem::new("Charlie", "35")];

        let selection = parse_selection("name = 'Alice' AND age = '30'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "30")),
                FilterResult::Skip(TestItem::new("Bob", "30")),
                FilterResult::Skip(TestItem::new("Charlie", "35")),
            ]
        );
    }

    #[test]
    fn test_complex_condition() {
        let items = vec![
            TestItem::new("Alice", "20"),
            TestItem::new("Bob", "25"),
            TestItem::new("Charlie", "30"),
            TestItem::new("David", "35"),
            TestItem::new("Eve", "40"),
        ];

        let selection = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= '30' AND age <= '40'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Skip(TestItem::new("Alice", "20")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Pass(TestItem::new("Charlie", "30")),
                FilterResult::Skip(TestItem::new("David", "35")),
                FilterResult::Skip(TestItem::new("Eve", "40")),
            ]
        );
    }

    #[test]
    fn test_in_operator() {
        let items = vec![
            TestItem::new("Alice", "20"),
            TestItem::new("Bob", "25"),
            TestItem::new("Charlie", "30"),
            TestItem::new("David", "35"),
            TestItem::new("Eve", "40"),
        ];

        // Test IN with names
        let selection = parse_selection("name IN ('Alice', 'Charlie', 'Eve')").unwrap();
        let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "20")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Pass(TestItem::new("Charlie", "30")),
                FilterResult::Skip(TestItem::new("David", "35")),
                FilterResult::Pass(TestItem::new("Eve", "40")),
            ]
        );

        // Test IN with ages
        let selection = parse_selection("age IN ('20', '30', '40')").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "20")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Pass(TestItem::new("Charlie", "30")),
                FilterResult::Skip(TestItem::new("David", "35")),
                FilterResult::Pass(TestItem::new("Eve", "40")),
            ]
        );
    }

    // JSON path traversal tests
    mod json_tests {
        use super::*;

        /// Test item with a JSON property for testing nested path queries
        #[derive(Debug, Clone, PartialEq)]
        struct TrackItem {
            name: String,
            licensing: Vec<u8>, // JSON stored as bytes
        }

        impl TrackItem {
            fn new(name: &str, licensing: serde_json::Value) -> Self {
                Self { name: name.to_string(), licensing: serde_json::to_vec(&licensing).unwrap() }
            }
        }

        impl Filterable for TrackItem {
            fn collection(&self) -> &str { "tracks" }

            fn value(&self, name: &str) -> Option<Value> {
                match name {
                    "name" => Some(Value::String(self.name.clone())),
                    "licensing" => Some(Value::Binary(self.licensing.clone())),
                    _ => None,
                }
            }
        }

        #[test]
        fn test_simple_json_path() {
            let items = vec![
                TrackItem::new(
                    "Track A",
                    serde_json::json!({
                        "territory": "US",
                        "rights": "exclusive"
                    }),
                ),
                TrackItem::new(
                    "Track B",
                    serde_json::json!({
                        "territory": "UK",
                        "rights": "non-exclusive"
                    }),
                ),
                TrackItem::new(
                    "Track C",
                    serde_json::json!({
                        "territory": "US",
                        "rights": "non-exclusive"
                    }),
                ),
            ];

            // Query: licensing.territory = 'US'
            let selection = parse_selection("licensing.territory = 'US'").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Skip(_)));
            assert!(matches!(results[2], FilterResult::Pass(_)));
        }

        #[test]
        fn test_nested_json_path() {
            let items = vec![
                TrackItem::new(
                    "Track A",
                    serde_json::json!({
                        "rights": {
                            "holder": "Label A",
                            "type": "exclusive"
                        }
                    }),
                ),
                TrackItem::new(
                    "Track B",
                    serde_json::json!({
                        "rights": {
                            "holder": "Label B",
                            "type": "non-exclusive"
                        }
                    }),
                ),
            ];

            // Query: licensing.rights.holder = 'Label A'
            let selection = parse_selection("licensing.rights.holder = 'Label A'").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Skip(_)));
        }

        #[test]
        fn test_json_path_with_numeric_value() {
            let items = vec![
                TrackItem::new(
                    "Track A",
                    serde_json::json!({
                        "duration": 180,
                        "bpm": 120
                    }),
                ),
                TrackItem::new(
                    "Track B",
                    serde_json::json!({
                        "duration": 240,
                        "bpm": 140
                    }),
                ),
            ];

            // Query: licensing.duration > 200
            let selection = parse_selection("licensing.duration > 200").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Skip(_)));
            assert!(matches!(results[1], FilterResult::Pass(_)));
        }

        #[test]
        fn test_json_path_with_boolean() {
            let items = vec![
                TrackItem::new(
                    "Track A",
                    serde_json::json!({
                        "active": true
                    }),
                ),
                TrackItem::new(
                    "Track B",
                    serde_json::json!({
                        "active": false
                    }),
                ),
            ];

            // Query: licensing.active = true
            let selection = parse_selection("licensing.active = true").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Skip(_)));
        }

        #[test]
        fn test_json_path_not_found() {
            let items = vec![TrackItem::new(
                "Track A",
                serde_json::json!({
                    "territory": "US"
                }),
            )];

            // Query for non-existent path
            let selection = parse_selection("licensing.nonexistent = 'value'").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Error(_, _)));
        }

        #[test]
        fn test_json_path_combined_with_regular_field() {
            let items = vec![
                TrackItem::new(
                    "Track A",
                    serde_json::json!({
                        "territory": "US"
                    }),
                ),
                TrackItem::new(
                    "Track B",
                    serde_json::json!({
                        "territory": "US"
                    }),
                ),
                TrackItem::new(
                    "Track C",
                    serde_json::json!({
                        "territory": "UK"
                    }),
                ),
            ];

            // Query: name = 'Track A' AND licensing.territory = 'US'
            let selection = parse_selection("name = 'Track A' AND licensing.territory = 'US'").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Skip(_)));
            assert!(matches!(results[2], FilterResult::Skip(_)));
        }

        #[test]
        fn test_traverse_into_non_json_property_errors() {
            let items = vec![TestItem::new("Alice", "30")];

            // Try to traverse into a non-JSON property
            let selection = parse_selection("name.nested = 'value'").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Error(_, Error::PropertyNotFound(_))));
        }

        #[test]
        fn test_json_path_with_or() {
            let items = vec![
                TrackItem::new("Track A", serde_json::json!({ "status": "active", "region": "US" })),
                TrackItem::new("Track B", serde_json::json!({ "status": "pending", "region": "UK" })),
                TrackItem::new("Track C", serde_json::json!({ "status": "archived", "region": "US" })),
            ];

            // Query: licensing.status = 'active' OR licensing.region = 'UK'
            let selection = parse_selection("licensing.status = 'active' OR licensing.region = 'UK'").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_))); // active
            assert!(matches!(results[1], FilterResult::Pass(_))); // UK
            assert!(matches!(results[2], FilterResult::Skip(_))); // neither
        }

        #[test]
        fn test_json_path_with_in_operator() {
            let items = vec![
                TrackItem::new("Track A", serde_json::json!({ "status": "active" })),
                TrackItem::new("Track B", serde_json::json!({ "status": "pending" })),
                TrackItem::new("Track C", serde_json::json!({ "status": "archived" })),
            ];

            // Query: licensing.status IN ('active', 'pending')
            let selection = parse_selection("licensing.status IN ('active', 'pending')").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Pass(_)));
            assert!(matches!(results[2], FilterResult::Skip(_)));
        }

        #[test]
        fn test_collection_qualified_json_path() {
            // Test: tracks.licensing.territory where "tracks" is the collection name
            // This tests the collection-qualified path handling in evaluate_expr
            let items = vec![
                TrackItem::new("Track A", serde_json::json!({ "territory": "US" })),
                TrackItem::new("Track B", serde_json::json!({ "territory": "UK" })),
            ];

            // Query with collection prefix: tracks.licensing.territory = 'US'
            let selection = parse_selection("tracks.licensing.territory = 'US'").unwrap();
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Skip(_)));
        }

        /// Tests for JSON-aware type casting behavior.
        ///
        /// HACK: We infer "JSON semantics" from multi-step paths.
        /// TODO(Phase 3 - Schema Registry): Replace with proper property type lookup.
        mod json_type_casting {
            use super::*;

            #[test]
            fn test_json_numeric_casting_same_type() {
                // JSON numbers matching literal numbers should work
                let items = vec![TrackItem::new("Track A", serde_json::json!({ "count": 42 }))];

                let selection = parse_selection("licensing.count = 42").unwrap();
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                assert!(matches!(results[0], FilterResult::Pass(_)));
            }

            #[test]
            fn test_json_numeric_casting_float_to_int() {
                // JSON float should match integer literal (numeric family casting)
                let items = vec![TrackItem::new(
                    "Track A",
                    serde_json::json!({ "count": 42.5 }), // Float in JSON
                )];

                // Query with integer - should match via numeric casting
                let selection = parse_selection("licensing.count > 42").unwrap();
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                assert!(matches!(results[0], FilterResult::Pass(_))); // 42.5 > 42
            }

            #[test]
            fn test_json_string_to_number_no_cast() {
                // JSON string "42" should NOT match integer literal 42
                // (JSON-aware casting only allows numeric family, not string->number)
                let items = vec![TrackItem::new(
                    "Track A",
                    serde_json::json!({ "count": "42" }), // String, not number
                )];

                let selection = parse_selection("licensing.count = 42").unwrap();
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                // Should NOT pass - no string->number casting for JSON
                assert!(matches!(results[0], FilterResult::Skip(_)));
            }

            #[test]
            fn test_json_number_to_string_no_cast() {
                // JSON number 42 should NOT match string literal '42'
                let items = vec![TrackItem::new(
                    "Track A",
                    serde_json::json!({ "count": 42 }), // Number, not string
                )];

                let selection = parse_selection("licensing.count = '42'").unwrap();
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                // Should NOT pass - no number->string casting for JSON
                assert!(matches!(results[0], FilterResult::Skip(_)));
            }

            #[test]
            fn test_json_string_equality_works() {
                // JSON string matching string literal should work
                let items = vec![TrackItem::new("Track A", serde_json::json!({ "status": "active" }))];

                let selection = parse_selection("licensing.status = 'active'").unwrap();
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                assert!(matches!(results[0], FilterResult::Pass(_)));
            }

            #[test]
            fn test_json_comparison_operators() {
                // Test numeric comparisons work correctly
                let items = vec![
                    TrackItem::new("A", serde_json::json!({ "score": 50 })),
                    TrackItem::new("B", serde_json::json!({ "score": 100 })),
                    TrackItem::new("C", serde_json::json!({ "score": 150 })),
                ];

                // > operator
                let selection = parse_selection("licensing.score > 100").unwrap();
                let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();
                assert!(matches!(results[0], FilterResult::Skip(_)));
                assert!(matches!(results[1], FilterResult::Skip(_)));
                assert!(matches!(results[2], FilterResult::Pass(_)));

                // >= operator
                let selection = parse_selection("licensing.score >= 100").unwrap();
                let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();
                assert!(matches!(results[0], FilterResult::Skip(_)));
                assert!(matches!(results[1], FilterResult::Pass(_)));
                assert!(matches!(results[2], FilterResult::Pass(_)));

                // < operator
                let selection = parse_selection("licensing.score < 100").unwrap();
                let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();
                assert!(matches!(results[0], FilterResult::Pass(_)));
                assert!(matches!(results[1], FilterResult::Skip(_)));
                assert!(matches!(results[2], FilterResult::Skip(_)));
            }

            #[test]
            fn test_regular_field_still_casts_string_to_number() {
                // Regular (non-JSON) fields should still use general casting.
                // This tests that we correctly choose compare_values_with_cast
                // for simple paths vs compare_json_values for multi-step paths.
                //
                // Note: This test uses the simple TestItem which stores age as string
                // but we query with a number literal - general casting allows this.
                let items = vec![TestItem::new("Alice", "30")];

                // Regular field with string value, queried with number
                let selection = parse_selection("age = 30").unwrap();
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                // Should pass - general casting allows string '30' to match integer 30
                assert!(matches!(results[0], FilterResult::Pass(_)));
            }
        }
    }
}
