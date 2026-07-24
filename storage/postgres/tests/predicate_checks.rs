//! Predicate Checks: Postgres vs Filterable
//!
//! Verifies predicate evaluation consistency between Postgres storage and in-memory Filterable.
//! Test cases loaded from shared predicate_cases.json.

mod common;

use ankql::{NameResolutionError, NameResolver};
use ankurah::core::selection::filter::{evaluate_predicate, Filterable};
use ankurah::core::value::{Value, ValueType};
use ankurah::property::Json;
use ankurah::proto::PropertyId;
use ankurah::{policy::DEFAULT_CONTEXT as c, EntityId, Model, ModelId, Node, PermissiveAgent};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// =============================================================================
// TEST CASE LOADING
// =============================================================================

const PREDICATE_CASES_JSON: &str = include_str!("../../../tests/predicate_cases.json");
fn query_test_model_id() -> ModelId { ModelId::EntityId(EntityId::from_bytes([0x31; 16])) }
fn data_property_id() -> EntityId { EntityId::from_bytes([0x32; 16]) }

struct FilterResolver;

impl NameResolver for FilterResolver {
    fn resolve_property(&self, _model: &ModelId, name: &str) -> Result<Option<PropertyId>, NameResolutionError> {
        Ok((name == "data").then(|| PropertyId::EntityId(data_property_id())))
    }

    fn property_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, NameResolutionError> {
        (property == &PropertyId::EntityId(data_property_id())).then_some(ValueType::Json).ok_or_else(|| {
            NameResolutionError::ValueTypeLookup { model: *model, property: *property, message: "unknown test property".to_owned() }
        })
    }
}

#[derive(Debug, Deserialize)]
struct PredicateCases {
    suites: Vec<TestSuite>,
}
#[derive(Debug, Deserialize)]
struct TestSuite {
    name: String,
    cases: Vec<TestCase>,
}
#[derive(Debug, Deserialize)]
struct TestCase {
    name: String,
    entities: Vec<TestEntity>,
    expectations: Vec<Expectation>,
}
#[derive(Debug, Deserialize)]
struct TestEntity {
    label: String,
    data: serde_json::Value,
}
#[derive(Debug, Deserialize)]
struct Expectation {
    query: String,
    matches: Vec<String>,
}

fn all_test_cases() -> Vec<TestCase> {
    let cases: PredicateCases = serde_json::from_str(PREDICATE_CASES_JSON).expect("parse");
    cases.suites.into_iter().flat_map(|s| s.cases).collect()
}

// =============================================================================
// FILTERABLE REFERENCE
// =============================================================================

struct MockFilterable {
    collection: String,
    values: HashMap<String, Value>,
}
impl MockFilterable {
    fn new(collection: &str) -> Self { Self { collection: collection.to_string(), values: HashMap::new() } }
    fn with_json(mut self, name: &str, json: serde_json::Value) -> Self {
        self.values.insert(name.to_string(), Value::Json(json));
        self
    }
}
impl Filterable for MockFilterable {
    fn collection(&self) -> &str { &self.collection }
    fn value(&self, name: &str) -> Option<Value> { self.values.get(name).cloned() }
    fn value_by_id(&self, id: EntityId) -> Option<Value> { (id == data_property_id()).then(|| self.values.get("data").cloned()).flatten() }
}

fn verify_filterable(case: &TestCase) {
    for entity in &case.entities {
        let f = MockFilterable::new("QueryTest").with_json("data", entity.data.clone());
        for exp in &case.expectations {
            let sel = ankql::parser::parse_selection(&exp.query).expect("parse");
            let resolved_sel = sel.resolve_names(&query_test_model_id(), &FilterResolver).expect("resolve");
            let matches = evaluate_predicate(&f, &resolved_sel.predicate).unwrap_or(false);
            let should = exp.matches.contains(&entity.label);
            assert_eq!(matches, should, "[Filterable] case={} entity={} query='{}'", case.name, entity.label, exp.query);
        }
    }
}

// =============================================================================
// TEST MODEL
// =============================================================================

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct QueryTest {
    pub label: String,
    pub data: Json,
}

/// Regression model for #364. Values such as epoch seconds fit in an i32,
/// but the model's declared type materializes as a PostgreSQL bigint column.
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct TimestampedEvent {
    pub occurred_at: i64,
}

// =============================================================================
// POSTGRES PREDICATE CHECK
// =============================================================================

#[tokio::test]
async fn test_postgres_predicate_checks() -> Result<()> {
    let cases = all_test_cases();
    let (container, storage) = common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    for case in &cases {
        verify_filterable(case);

        let trx = ctx.begin();
        for entity in &case.entities {
            trx.create(&QueryTest { label: entity.label.clone(), data: Json::new(entity.data.clone()) }).await?;
        }
        trx.commit().await?;

        for exp in &case.expectations {
            let mut expected: Vec<String> = exp.matches.clone();
            expected.sort();
            let results: Vec<QueryTestView> = ctx.fetch(exp.query.as_str()).await?;
            let mut actual: Vec<String> = results.iter().map(|r| r.label().unwrap()).collect();
            actual.sort();
            assert_eq!(actual, expected, "[Postgres] case={} query='{}'", case.name, exp.query);
        }
    }

    drop(container);
    Ok(())
}

/// A small integer literal parses as `Value::I32`; catalog resolution must
/// widen it to the i64 property's canonical type before PostgreSQL binds the
/// parameter. Otherwise tokio-postgres rejects the i32 parameter for the
/// bigint column with `WrongType` (#364).
#[tokio::test]
async fn test_postgres_i64_field_accepts_small_integer_literal() -> Result<()> {
    use ankql::ast::{Expr, Predicate, Value};

    let raw = ankql::parser::parse_selection("occurred_at >= 0")?;
    assert!(
        matches!(
            raw.predicate,
            Predicate::Comparison { right, .. } if matches!(right.as_ref(), Expr::Literal(Value::I32(0)))
        ),
        "the regression requires a parser-narrowed i32 literal"
    );

    let (container, storage) = common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    let trx = ctx.begin();
    trx.create(&TimestampedEvent { occurred_at: 1_700_000_000 }).await?;
    trx.commit().await?;

    let results: Vec<TimestampedEventView> = ctx.fetch("occurred_at >= 0").await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].occurred_at()?, 1_700_000_000);

    drop(container);
    Ok(())
}
