//! Filter items based on a predicate. This is necessary for cases where we are scanning over a set of data
//! which has not been pre-filtered by an index search - or to supplement/validate an index search with additional filtering.

use crate::property::PropertyError;
use crate::value::Value;
use ankql::ast::{ComparisonOperator, Expr, Identifier, Predicate};
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
    /// A checked property read hit the RFC 5.4 sibling gate: a same-display-
    /// name retype lineage holds data on this item, so the read is fail-visible
    /// (a `TypeSkew` from `Filterable::value_checked`) rather than a silent
    /// NULL. Predicate evaluation surfaces it as an error, so the offending
    /// item is reported (`FilterResult::Error`) instead of silently
    /// matching/not-matching.
    #[error("property read error: {0}")]
    PropertyRead(String),
}

impl From<PropertyError> for Error {
    fn from(e: PropertyError) -> Self { Error::PropertyRead(e.to_string()) }
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

    /// Read a property under the RFC 5.4 sibling gate for RESOLVED-identifier
    /// evaluation. `Ok(Some)` = present, `Ok(None)` = absent (evaluated as NULL
    /// by the caller), `Err` = a checked-read failure such as `TypeSkew` (a
    /// same-display-name retype lineage holds data), surfaced as an evaluation
    /// error rather than a silent NULL.
    ///
    /// The default delegates to the lenient [`value`], so mock/test items and
    /// non-entity `Filterable`s are unaffected. `Entity` overrides it to route
    /// the checked lookup through the LWW backend's `get_checked`.
    fn value_checked(&self, name: &str) -> Result<Option<Value>, PropertyError> { Ok(self.value(name)) }
}

fn evaluate_expr<I: Filterable>(item: &I, expr: &Expr) -> Result<ExprOutput<Value>, Error> {
    match expr {
        Expr::Placeholder => Err(Error::PropertyNotFound("Placeholder values must be replaced before filtering".to_string())),
        Expr::Literal(lit) => Ok(ExprOutput::Value(lit.clone().into())),
        Expr::Path(path) => evaluate_path_steps(item, &path.steps),
        // A RESOLVED Identifier addresses exactly one property by its
        // resolved-at name plus an optional JSON sub-path. It does NOT share
        // the Path qualifier logic: the resolution pass already stripped any
        // collection qualifier and fixed the property, so a leading step that
        // happens to equal the collection name is the PROPERTY, never a
        // qualifier. Evaluate value(name) + subpath extraction only. (Phase A:
        // name-based lookup through the binding; id-based lookup is a
        // follow-up, Filterable::value_by_property_id.)
        Expr::Identifier(identifier) => evaluate_identifier(item, identifier),
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

/// Evaluate a RESOLVED [`Identifier`]: look up the property by its
/// resolved-at name, then extract any JSON sub-path. Unlike
/// [`evaluate_path_steps`] this does NO collection-qualifier handling -- the
/// resolution pass already bound the property, so `name` is authoritative
/// even when it equals the collection name (Phase A: name-based lookup).
fn evaluate_identifier<I: Filterable>(item: &I, identifier: &Identifier) -> Result<ExprOutput<Value>, Error> {
    let name = identifier.property_name();
    if identifier.is_simple() {
        // Bare column reference. RFC 5.4: a RESOLVED identifier that the item
        // does not hold evaluates as NULL (IsNull matches, comparisons are
        // false) instead of erroring PropertyNotFound -- this unifies the three
        // historical missing-property behaviors (filter hard-error, reactor
        // unwrap_or(false), SQL assume_null). The read is CHECKED, so a
        // same-display-name retype sibling holding data surfaces as an
        // evaluation error (`TypeSkew`) rather than a silent NULL.
        return Ok(match item.value_checked(name)? {
            Some(value) => ExprOutput::Value(value),
            None => ExprOutput::None,
        });
    }
    // A JSON sub-path on a resolved identifier: fetch the base property
    // (checked, so TypeSkew propagates), then traverse into it. An absent base
    // is NULL (consistent with the bare case), never PropertyNotFound.
    let Some(base) = item.value_checked(name)? else {
        return Ok(ExprOutput::None);
    };
    match base.extract_at_path(&identifier.subpath) {
        Some(value) => Ok(ExprOutput::Value(value)),
        // Present base, but the sub-path is absent within it: NULL, matching
        // the absent-as-NULL rule for resolved references.
        None => Ok(ExprOutput::None),
    }
}

/// Evaluate a sequence of path steps (as produced by `PathExpr::steps`)
/// against an item. Used by the `Expr::Path` arm (unresolved paths still
/// carry the legacy collection-qualifier ambiguity); the resolved
/// `Expr::Identifier` arm uses [`evaluate_identifier`] instead.
fn evaluate_path_steps<I: Filterable>(item: &I, steps: &[String]) -> Result<ExprOutput<Value>, Error> {
    // For simple paths, use the first step as the property name
    if steps.len() == 1 {
        let name = steps[0].as_str();
        return Ok(ExprOutput::Value(item.value(name).ok_or_else(|| Error::PropertyNotFound(name.to_string()))?));
    }

    // Multi-step path - could be:
    // 1. Collection.property (legacy, check if first step matches collection)
    // 2. property.nested.path (JSON traversal)

    let first = steps[0].as_str();

    // First, check if it's a collection-qualified path
    if first == item.collection() {
        // Treat remaining path as property access
        let remaining = &steps[1..];
        if remaining.len() == 1 {
            // Simple collection.property
            let name = &remaining[0];
            return Ok(ExprOutput::Value(item.value(name).ok_or_else(|| Error::PropertyNotFound(name.to_string()))?));
        }
        // collection.property.nested... - get property and traverse sub-path
        let property_name = &remaining[0];
        let sub_path = &remaining[1..];
        return evaluate_sub_path(item, property_name, sub_path);
    }

    // Not a collection qualifier - treat first step as property, rest as sub-path
    let property_name = first;
    let sub_path: Vec<&str> = steps[1..].iter().map(|s| s.as_str()).collect();
    evaluate_sub_path(item, property_name, &sub_path)
}

/// Evaluate a sub-path traversal: get property value, extract nested value at path
/// Delegates to Value::extract_at_path for the actual traversal.
fn evaluate_sub_path<I: Filterable>(item: &I, property_name: &str, sub_path: &[impl AsRef<str>]) -> Result<ExprOutput<Value>, Error> {
    let property_value = item.value(property_name).ok_or_else(|| Error::PropertyNotFound(property_name.to_string()))?;

    // Convert sub_path to Vec<String> for extract_at_path
    let path: Vec<String> = sub_path.iter().map(|s| s.as_ref().to_string()).collect();

    property_value.extract_at_path(&path).map(ExprOutput::Value).ok_or_else(|| {
        Error::PropertyNotFound(format!(
            "Sub-path '{}' not found in property '{}'",
            sub_path.iter().map(|s| s.as_ref()).collect::<Vec<_>>().join("."),
            property_name
        ))
    })
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

pub fn evaluate_predicate<I: Filterable>(item: &I, predicate: &Predicate) -> Result<bool, Error> {
    match predicate {
        Predicate::Comparison { left, operator, right } => {
            let left_val = evaluate_expr(item, left)?;
            let right_val = evaluate_expr(item, right)?;

            Ok(match operator {
                ComparisonOperator::Equal => left_val
                    .as_value()
                    .zip(right_val.as_value())
                    .map(|(l, r)| compare_values_with_cast(l, r, |a, b| a == b))
                    .unwrap_or(false),
                ComparisonOperator::NotEqual => left_val
                    .as_value()
                    .zip(right_val.as_value())
                    .map(|(l, r)| compare_values_with_cast(l, r, |a, b| a != b))
                    .unwrap_or(false),
                ComparisonOperator::GreaterThan => left_val
                    .as_value()
                    .zip(right_val.as_value())
                    .map(|(l, r)| compare_values_with_cast(l, r, |a, b| a > b))
                    .unwrap_or(false),
                ComparisonOperator::GreaterThanOrEqual => left_val
                    .as_value()
                    .zip(right_val.as_value())
                    .map(|(l, r)| compare_values_with_cast(l, r, |a, b| a >= b))
                    .unwrap_or(false),
                ComparisonOperator::LessThan => left_val
                    .as_value()
                    .zip(right_val.as_value())
                    .map(|(l, r)| compare_values_with_cast(l, r, |a, b| a < b))
                    .unwrap_or(false),
                ComparisonOperator::LessThanOrEqual => left_val
                    .as_value()
                    .zip(right_val.as_value())
                    .map(|(l, r)| compare_values_with_cast(l, r, |a, b| a <= b))
                    .unwrap_or(false),
                ComparisonOperator::In => {
                    let value =
                        left_val.as_value().ok_or_else(|| Error::PropertyNotFound("Expected single value for IN left operand".into()))?;
                    let list = right_val.as_list().ok_or_else(|| Error::PropertyNotFound("Expected list for IN right operand".into()))?;
                    list.iter().any(|item| item.as_value().map(|v| compare_values_with_cast(value, v, |a, b| a == b)).unwrap_or(false))
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
    fn test_identifier_evaluates_like_equivalent_path() {
        // A predicate containing Expr::Identifier (constructed programmatically)
        // evaluates identically to the same predicate written with Expr::Path.
        use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, PathExpr, Predicate};
        use ulid::Ulid;

        let items = vec![TestItem::new("Alice", "30"), TestItem::new("Bob", "25"), TestItem::new("Charlie", "35")];

        // Identifier { name: "name", subpath: [] } = 'Alice'
        let id_pred = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier {
                property: Ulid::from_bytes([9u8; 16]),
                name: "name".to_string(),
                subpath: vec![],
            })),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("Alice".to_string()))),
        };
        // The equivalent Path predicate.
        let path_pred = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("name"))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("Alice".to_string()))),
        };

        let id_results: Vec<_> = FilterIterator::new(items.clone().into_iter(), id_pred).collect();
        let path_results: Vec<_> = FilterIterator::new(items.into_iter(), path_pred).collect();

        assert_eq!(id_results, path_results);
        assert_eq!(
            id_results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "30")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Skip(TestItem::new("Charlie", "35")),
            ]
        );
    }

    /// The storage engines' post-filter shape, end to end: a RESOLVED
    /// Identifier predicate evaluated via a schema-blind [`TemporaryEntity`]
    /// over an id-keyed (0xA2) state buffer, exactly as
    /// `post_filter_states` does in the sqlite/postgres engines (and as a
    /// policy agent inspecting a raw state does). Regression: before the
    /// hint-projection regime of `get_checked`, every id-keyed property
    /// evaluated as absent (NULL) here, silently dropping matching rows.
    #[test]
    fn temporary_entity_post_filter_reads_id_keyed_state() {
        use crate::entity::TemporaryEntity;
        use crate::property::backend::{LWWBackend, PropertyBackend};
        use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, PathExpr, Predicate};
        use ankurah_proto as proto;
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use ulid::Ulid;

        // A bound id-keyed writer, as any post-epoch user entity is.
        let title_id = proto::EntityId::from_bytes([0x11; 16]);
        let binding = Arc::new(crate::property::backend::lww::SchemaBinding {
            to_id: BTreeMap::from([("title".to_string(), title_id)]),
            to_name: BTreeMap::from([(title_id, "title".to_string())]),
        });
        let writer = LWWBackend::new();
        writer.bind_schema(binding);
        writer.set_wire_mode(crate::property::backend::lww::WireMode::IdKeyedV2);
        writer.set("title".to_string(), Some(crate::value::Value::String("alpha".to_string())));
        let ops = writer.to_operations().unwrap().unwrap();
        writer.apply_operations_with_event(&ops, proto::EventId::from_bytes([3; 32])).unwrap();
        let buffer = writer.to_state_buffer().unwrap();

        let state =
            proto::State { state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_string(), buffer)])), head: Default::default() };
        let entity = TemporaryEntity::new(proto::EntityId::from_bytes([0x77; 16]), "album".into(), &state).unwrap();

        // Resolved-Identifier predicate (the checked read path).
        let id_pred = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier {
                property: Ulid::from_bytes(title_id.to_bytes()),
                name: "title".to_string(),
                subpath: vec![],
            })),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("alpha".to_string()))),
        };
        assert!(evaluate_predicate(&entity, &id_pred).unwrap(), "id-keyed value must be readable through its hint");

        // Path predicate (the lenient read path used for unresolved refs).
        let path_pred = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("title"))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("alpha".to_string()))),
        };
        assert!(evaluate_predicate(&entity, &path_pred).unwrap(), "lenient projection must also read the hint");

        // A property nothing claims evaluates as NULL: comparison false, no error.
        let absent_pred = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier {
                property: Ulid::from_bytes([8; 16]),
                name: "ghost".to_string(),
                subpath: vec![],
            })),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("alpha".to_string()))),
        };
        assert!(!evaluate_predicate(&entity, &absent_pred).unwrap());
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
        use crate::type_resolver::TypeResolver;

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

        /// Helper to parse and resolve types for JSON path queries
        fn parse_with_types(query: &str) -> ankql::ast::Selection {
            let selection = parse_selection(query).unwrap();
            TypeResolver::new().resolve_selection_types(selection)
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
        fn test_identifier_json_subpath_evaluates_like_path() {
            // An Identifier with a JSON subpath evaluates identically to the equivalent
            // multi-step Path (licensing.territory = 'US'), extracting the same nested value.
            use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, PathExpr, Predicate};
            use ulid::Ulid;

            let items = vec![
                TrackItem::new("Track A", serde_json::json!({ "territory": "US" })),
                TrackItem::new("Track B", serde_json::json!({ "territory": "UK" })),
                TrackItem::new("Track C", serde_json::json!({ "territory": "US" })),
            ];

            let id_pred = Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier {
                    property: Ulid::from_bytes([3u8; 16]),
                    name: "licensing".to_string(),
                    subpath: vec!["territory".to_string()],
                })),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("US".to_string()))),
            };
            let path_pred = Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr { steps: vec!["licensing".to_string(), "territory".to_string()] })),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("US".to_string()))),
            };

            let id_results: Vec<_> = FilterIterator::new(items.clone().into_iter(), id_pred).collect();
            let path_results: Vec<_> = FilterIterator::new(items.into_iter(), path_pred).collect();

            assert_eq!(id_results, path_results);
            assert!(matches!(id_results[0], FilterResult::Pass(_)));
            assert!(matches!(id_results[1], FilterResult::Skip(_)));
            assert!(matches!(id_results[2], FilterResult::Pass(_)));
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

                // Type resolver converts literal 42 to Json(42) for JSON path comparison
                let selection = parse_with_types("licensing.count = 42");
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

                // Type resolver converts literal '42' to Json("42") for JSON path comparison
                let selection = parse_with_types("licensing.count = '42'");
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                // Should NOT pass - no number->string casting for JSON
                assert!(matches!(results[0], FilterResult::Skip(_)));
            }

            #[test]
            fn test_json_string_equality_works() {
                // JSON string matching string literal should work
                let items = vec![TrackItem::new("Track A", serde_json::json!({ "status": "active" }))];

                // Type resolver converts literal to Json for JSON path comparison
                let selection = parse_with_types("licensing.status = 'active'");
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

                // > operator (type resolver converts literals to Json)
                let selection = parse_with_types("licensing.score > 100");
                let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();
                assert!(matches!(results[0], FilterResult::Skip(_)));
                assert!(matches!(results[1], FilterResult::Skip(_)));
                assert!(matches!(results[2], FilterResult::Pass(_)));

                // >= operator
                let selection = parse_with_types("licensing.score >= 100");
                let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();
                assert!(matches!(results[0], FilterResult::Skip(_)));
                assert!(matches!(results[1], FilterResult::Pass(_)));
                assert!(matches!(results[2], FilterResult::Pass(_)));

                // < operator
                let selection = parse_with_types("licensing.score < 100");
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
