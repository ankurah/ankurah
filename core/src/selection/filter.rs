//! Filter items based on a predicate. This is necessary for cases where we are scanning over a set of data
//! which has not been pre-filtered by an index search - or to supplement/validate an index search with additional filtering.

use crate::value::Value;
use ankql::ast::{ComparisonOperator, Expr, Predicate, PropertyId, PropertyPath};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("collection mismatch: expected {expected}, got {actual}")]
    CollectionMismatch { expected: String, actual: String },
    #[error("property not found: {0}")]
    PropertyNotFound(String),
    #[error("unresolved property path `{0}` reached filtering: a selection must be resolved to property ids before evaluation")]
    UnresolvedPath(String),
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
/// Returns typed Values to enable proper comparison with casting. Properties
/// are addressed by durable [`PropertyId`] only; the `id` pseudo-property
/// arrives as [`PropertyId::Id`] and implementors answer it with the item's
/// own entity id.
pub trait Filterable {
    fn collection(&self) -> &str;
    fn value(&self, property: &PropertyId) -> Option<Value>;
}

fn evaluate_expr<I: Filterable>(item: &I, expr: &Expr) -> Result<ExprOutput<Value>, Error> {
    match expr {
        Expr::Placeholder => Err(Error::PropertyNotFound("Placeholder values must be replaced before filtering".to_string())),
        Expr::Literal(lit) => Ok(ExprOutput::Value(lit.clone())),
        // A raw name reached evaluation without resolving to an identity.
        // Answering it by name would silently re-introduce name-keyed reads,
        // so this fails loudly instead (`Selection::resolve_names` is the
        // pass that should have run).
        Expr::Path(path) => Err(Error::UnresolvedPath(path.to_string())),
        Expr::PropertyPath(identifier) => {
            if identifier.is_simple() {
                Ok(ExprOutput::Value(item.value(&identifier.property_id()).ok_or_else(|| Error::PropertyNotFound(identifier.to_string()))?))
            } else {
                evaluate_sub_path(item, identifier)
            }
        }
        Expr::ExprList(exprs) => {
            let mut result = Vec::new();
            for expr in exprs {
                result.push(evaluate_expr(item, expr)?);
            }
            Ok(ExprOutput::List(result))
        }
        _ => Err(Error::UnsupportedExpression("Only literal, resolved-property, and list expressions are supported")),
    }
}

/// Evaluate a sub-path traversal: get the property's value by identity, then
/// extract the nested value at the identifier's JSON sub-path.
/// Delegates to Value::extract_at_path for the actual traversal.
fn evaluate_sub_path<I: Filterable>(item: &I, identifier: &PropertyPath) -> Result<ExprOutput<Value>, Error> {
    let property_value = item.value(&identifier.property_id()).ok_or_else(|| Error::PropertyNotFound(identifier.to_string()))?;

    property_value
        .extract_at_path(&identifier.subpath)
        .map(ExprOutput::Value)
        .ok_or_else(|| Error::PropertyNotFound(format!("Sub-path not found in property '{}'", identifier)))
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
    use crate::value::ValueType;
    use ankql::ast::{Selection, SystemProperty};
    use ankql::parser::parse_selection;
    use ankql::{NameResolutionError, NameResolver};
    use ankurah_proto::EntityId;
    use ankurah_proto::ModelId;

    /// A deterministic durable identity for a fixture field name.
    fn prop_id(name: &str) -> PropertyId {
        let mut bytes = [0u8; 32];
        let n = name.as_bytes();
        let len = n.len().min(32);
        bytes[..len].copy_from_slice(&n[..len]);
        PropertyId::EntityId(EntityId::from_bytes(bytes))
    }

    fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([0x77; 32])) }

    /// A static resolver over a fixture's field names and registered types:
    /// what the admission pass sees once the catalog holds these rows. The
    /// model qualifier "tracks" resolves, exercising qualified paths.
    struct FixtureResolver(&'static [(&'static str, ValueType)]);

    impl NameResolver for FixtureResolver {
        fn resolve_model_name(&self, name: &str) -> Result<Option<ModelId>, NameResolutionError> { Ok((name == "tracks").then(model)) }

        fn resolve_property(&self, _model: &ModelId, name: &str) -> Result<Option<PropertyId>, NameResolutionError> {
            Ok(self.0.iter().find(|(field, _)| *field == name).map(|(field, _)| prop_id(field)))
        }

        fn property_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, NameResolutionError> {
            self.0.iter().find(|(field, _)| prop_id(field) == *property).map(|(_, ty)| *ty).ok_or_else(|| {
                NameResolutionError::ValueTypeLookup { model: *model, property: *property, message: "not a fixture field".into() }
            })
        }
    }

    /// Parse and bind names to identities (no origin-side value casting, so
    /// eval-side casting stays under test).
    fn resolve(query: &str, fields: &'static [(&'static str, ValueType)]) -> Selection {
        parse_selection(query).unwrap().resolve_names(&model(), &FixtureResolver(fields)).unwrap()
    }

    /// Parse, bind, and canonicalize comparison values at origin -- the full
    /// admission treatment (sub-path comparisons coerce their literals to
    /// Json, replacing what the deleted TypeResolver heuristic did).
    fn resolve_cast(query: &str, fields: &'static [(&'static str, ValueType)]) -> Selection {
        let resolver = FixtureResolver(fields);
        let resolved = parse_selection(query).unwrap().resolve_names(&model(), &resolver).unwrap();
        let type_of = |path: &ankql::ast::PropertyPath| resolver.property_value_type(&model(), &path.property_id()).ok();
        resolved.cast_comparison_values(&type_of).unwrap()
    }

    const PEOPLE: &[(&str, ValueType)] = &[("name", ValueType::String), ("age", ValueType::String)];

    #[derive(Debug, Clone, PartialEq)]
    struct TestItem {
        name: String,
        age: String,
    }

    impl Filterable for TestItem {
        fn collection(&self) -> &str { "users" }

        fn value(&self, property: &PropertyId) -> Option<Value> {
            if *property == prop_id("name") {
                Some(Value::String(self.name.clone()))
            } else if *property == prop_id("age") {
                Some(Value::String(self.age.clone()))
            } else {
                None
            }
        }
    }

    impl TestItem {
        fn new(name: &str, age: &str) -> Self { Self { name: name.to_string(), age: age.to_string() } }
    }

    #[test]
    fn test_simple_equality() {
        let items = vec![TestItem::new("Alice", "30"), TestItem::new("Bob", "25"), TestItem::new("Charlie", "35")];

        let selection = resolve("name = 'Alice'", PEOPLE);
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

        let selection = resolve("name = 'Alice' AND age = '30'", PEOPLE);
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

        let selection = resolve("(name = 'Alice' OR name = 'Charlie') AND age >= '30' AND age <= '40'", PEOPLE);
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
        let selection = resolve("name IN ('Alice', 'Charlie', 'Eve')", PEOPLE);
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
        let selection = resolve("age IN ('20', '30', '40')", PEOPLE);
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

    #[test]
    fn unresolved_path_errors_at_evaluation() {
        // A raw (unresolved) reference reaching evaluation is the leak the
        // admission pass exists to prevent; answering it by name would
        // silently reintroduce name-keyed reads.
        let selection = parse_selection("name = 'Alice'").unwrap();
        let result = evaluate_predicate(&TestItem::new("Alice", "30"), &selection.predicate);
        assert!(matches!(result, Err(Error::UnresolvedPath(_))), "got {result:?}");
    }

    /// A row carrying a `Ref` field, whose value is an EntityId rather than a
    /// string. Policy filters compare such a field against a claim value, and
    /// the claim value need not be an id at all.
    #[derive(Debug, Clone, PartialEq)]
    struct OwnedItem {
        owner: ankurah_proto::EntityId,
    }

    const RECORDS: &[(&str, ValueType)] = &[("owner", ValueType::EntityId)];

    impl Filterable for OwnedItem {
        fn collection(&self) -> &str { "records" }

        fn value(&self, property: &PropertyId) -> Option<Value> {
            if *property == prop_id("owner") {
                Some(Value::EntityId(self.owner))
            } else {
                None
            }
        }
    }

    /// An EntityId-typed field compared against a string that is not an id
    /// answers false, and never an error. `evaluate_predicate` propagates an
    /// error out of the whole predicate, so an erroring comparison here would
    /// fail the caller's entire query — including the arms of an OR that would
    /// otherwise have admitted rows — where false merely denies this row.
    ///
    /// This is the row-side half of what a scope rule like `owner = $jwt.sub`
    /// does when the subject is a distinguished literal rather than a user's
    /// id: ankurah-jwt-auth substitutes such a subject as a String literal
    /// (its `variables::typed_expr`), and it arrives here. (Origin-side
    /// canonicalization declines to cast such a literal and leaves the
    /// comparison for this eval-side path; see `admit_selection`.)
    #[test]
    fn test_entity_id_field_never_equals_a_non_id_string() {
        let row = OwnedItem { owner: ankurah_proto::EntityId::random() };

        // 'guest' does not parse as an EntityId, so the cast toward the field's
        // type fails and the surviving comparison is the row's id rendered as
        // a string — which no non-id value equals.
        let selection = resolve("owner = 'guest'", RECORDS);
        assert_eq!(evaluate_predicate(&row, &selection.predicate), Ok(false), "a subject that is not an id must deny the row, not error");

        // The control that keeps the false above honest: the comparator does
        // cross this type pair, so the false is a comparison that answered no
        // and not a comparator that refuses EntityId-against-String outright —
        // which would answer false for every string, including the right one.
        let query = format!("owner = '{}'", row.owner.to_base64());
        let selection = parse_selection(&query).unwrap().resolve_names(&model(), &FixtureResolver(RECORDS)).unwrap();
        assert_eq!(evaluate_predicate(&row, &selection.predicate), Ok(true), "the row's own id, as a string, must still match");
    }

    #[test]
    fn id_pseudo_property_resolves_and_answers_the_entity_id() {
        // `id` resolves to PropertyId::Id, and Filterable implementors answer
        // it with the row's own identity.
        struct Row(EntityId);
        impl Filterable for Row {
            fn collection(&self) -> &str { "rows" }
            fn value(&self, property: &PropertyId) -> Option<Value> { (*property == PropertyId::Id).then(|| Value::EntityId(self.0)) }
        }
        let row = Row(EntityId::from_bytes([9u8; 32]));
        let query = format!("id = '{}'", row.0.to_base64());
        let selection = resolve_cast(Box::leak(query.into_boxed_str()), &[]);
        assert_eq!(evaluate_predicate(&row, &selection.predicate), Ok(true));
    }

    #[test]
    fn system_property_resolves_by_closed_name() {
        // On a system model there is no catalog row to resolve against; the
        // closed SystemProperty vocabulary is the identity.
        struct SysRow;
        impl Filterable for SysRow {
            fn collection(&self) -> &str { "_ankurah_property" }
            fn value(&self, property: &PropertyId) -> Option<Value> {
                (*property == PropertyId::System(SystemProperty::Label)).then(|| Value::String("album".into()))
            }
        }
        struct SystemResolver;
        impl NameResolver for SystemResolver {
            fn resolve_property(&self, _model: &ModelId, name: &str) -> Result<Option<PropertyId>, NameResolutionError> {
                Ok(SystemProperty::from_name(name).map(PropertyId::System))
            }
            fn property_value_type(&self, _model: &ModelId, property: &PropertyId) -> Result<ValueType, NameResolutionError> {
                let _ = property;
                Ok(ValueType::String)
            }
        }
        let selection = parse_selection("label = 'album'").unwrap().resolve_names(&model(), &SystemResolver).unwrap();
        assert_eq!(evaluate_predicate(&SysRow, &selection.predicate), Ok(true));
    }

    // JSON path traversal tests
    mod json_tests {
        use super::*;

        const TRACKS: &[(&str, ValueType)] = &[("name", ValueType::String), ("licensing", ValueType::Json)];

        /// Test item with a JSON property for testing nested path queries
        #[derive(Debug, Clone, PartialEq)]
        struct TrackItem {
            name: String,
            licensing: Vec<u8>, // JSON stored as binary
        }

        impl TrackItem {
            fn new(name: &str, licensing: serde_json::Value) -> Self {
                Self { name: name.to_string(), licensing: serde_json::to_vec(&licensing).unwrap() }
            }
        }

        impl Filterable for TrackItem {
            fn collection(&self) -> &str { "tracks" }

            fn value(&self, property: &PropertyId) -> Option<Value> {
                if *property == prop_id("name") {
                    Some(Value::String(self.name.clone()))
                } else if *property == prop_id("licensing") {
                    Some(Value::Binary(self.licensing.clone()))
                } else {
                    None
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
            let selection = resolve("licensing.territory = 'US'", TRACKS);
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
            let selection = resolve("licensing.rights.holder = 'Label A'", TRACKS);
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
            let selection = resolve("licensing.duration > 200", TRACKS);
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
            let selection = resolve("licensing.active = true", TRACKS);
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
            let selection = resolve("licensing.nonexistent = 'value'", TRACKS);
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
            let selection = resolve("name = 'Track A' AND licensing.territory = 'US'", TRACKS);
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Skip(_)));
            assert!(matches!(results[2], FilterResult::Skip(_)));
        }

        #[test]
        fn test_traverse_into_non_json_property_errors() {
            let items = vec![TestItem::new("Alice", "30")];

            // Try to traverse into a non-JSON property
            let selection = resolve("name.nested = 'value'", PEOPLE);
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
            let selection = resolve("licensing.status = 'active' OR licensing.region = 'UK'", TRACKS);
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
            let selection = resolve("licensing.status IN ('active', 'pending')", TRACKS);
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Pass(_)));
            assert!(matches!(results[2], FilterResult::Skip(_)));
        }

        #[test]
        fn test_collection_qualified_json_path() {
            // Test: tracks.licensing.territory where "tracks" is a model
            // qualifier the resolver recognizes; resolution strips it and
            // binds the remaining path.
            let items = vec![
                TrackItem::new("Track A", serde_json::json!({ "territory": "US" })),
                TrackItem::new("Track B", serde_json::json!({ "territory": "UK" })),
            ];

            // Query with collection prefix: tracks.licensing.territory = 'US'
            let selection = resolve("tracks.licensing.territory = 'US'", TRACKS);
            let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

            assert!(matches!(results[0], FilterResult::Pass(_)));
            assert!(matches!(results[1], FilterResult::Skip(_)));
        }

        /// Tests for JSON-aware type casting behavior: origin-side
        /// canonicalization types sub-path comparisons as Json
        /// (`cast_comparison_values`), and the eval-side comparator answers
        /// within the Json family.
        mod json_type_casting {
            use super::*;

            #[test]
            fn test_json_numeric_casting_same_type() {
                // JSON numbers matching literal numbers should work
                let items = vec![TrackItem::new("Track A", serde_json::json!({ "count": 42 }))];

                let selection = resolve("licensing.count = 42", TRACKS);
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
                let selection = resolve("licensing.count > 42", TRACKS);
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

                // Canonicalization converts literal 42 to Json(42) for the sub-path comparison
                let selection = resolve_cast("licensing.count = 42", TRACKS);
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

                // Canonicalization converts literal '42' to Json("42") for the sub-path comparison
                let selection = resolve_cast("licensing.count = '42'", TRACKS);
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                // Should NOT pass - no number->string casting for JSON
                assert!(matches!(results[0], FilterResult::Skip(_)));
            }

            #[test]
            fn test_json_string_equality_works() {
                // JSON string matching string literal should work
                let items = vec![TrackItem::new("Track A", serde_json::json!({ "status": "active" }))];

                // Canonicalization converts the literal to Json for the sub-path comparison
                let selection = resolve_cast("licensing.status = 'active'", TRACKS);
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

                // > operator (canonicalization converts literals to Json)
                let selection = resolve_cast("licensing.score > 100", TRACKS);
                let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();
                assert!(matches!(results[0], FilterResult::Skip(_)));
                assert!(matches!(results[1], FilterResult::Skip(_)));
                assert!(matches!(results[2], FilterResult::Pass(_)));

                // >= operator
                let selection = resolve_cast("licensing.score >= 100", TRACKS);
                let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();
                assert!(matches!(results[0], FilterResult::Skip(_)));
                assert!(matches!(results[1], FilterResult::Pass(_)));
                assert!(matches!(results[2], FilterResult::Pass(_)));

                // < operator
                let selection = resolve_cast("licensing.score < 100", TRACKS);
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
                let selection = resolve("age = 30", PEOPLE);
                let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

                // Should pass - general casting allows string '30' to match integer 30
                assert!(matches!(results[0], FilterResult::Pass(_)));
            }
        }
    }
}
