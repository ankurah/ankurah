//! Predicate Checks: Postgres vs Filterable
//!
//! Verifies predicate evaluation consistency between Postgres storage and in-memory Filterable.
//! Test cases loaded from shared predicate_cases.json.

mod common;

use ankql::ast::PropertyId;
use ankurah::core::schema::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty};
use ankurah::core::selection::filter::{evaluate_predicate, Filterable};
use ankurah::core::value::Value;
use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// =============================================================================
// TEST CASE LOADING
// =============================================================================

const PREDICATE_CASES_JSON: &str = include_str!("../../../tests/predicate_cases.json");

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
    /// The optional `nickname` property; a case leaves it out to test a missing property.
    #[serde(default)]
    nickname: Option<String>,
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
    values: HashMap<PropertyId, Value>,
}
impl MockFilterable {
    fn new(collection: &str) -> Self { Self { collection: collection.to_string(), values: HashMap::new() } }
    fn with_json(mut self, name: &str, json: serde_json::Value) -> Self {
        self.values.insert(fixture_prop(name), Value::Json(json));
        self
    }
    fn with_text(mut self, name: &str, text: Option<String>) -> Self {
        if let Some(text) = text {
            self.values.insert(fixture_prop(name), Value::String(text));
        }
        self
    }
}
impl Filterable for MockFilterable {
    fn value(&self, property: &PropertyId) -> Option<Value> { self.values.get(property).cloned() }
}

/// A deterministic durable identity for a fixture field name.
fn fixture_prop(name: &str) -> PropertyId {
    let mut bytes = [0u8; 32];
    let n = name.as_bytes();
    let len = n.len().min(32);
    bytes[..len].copy_from_slice(&n[..len]);
    PropertyId::EntityId(ankurah::proto::EntityId::from_bytes(bytes))
}

/// Bind a case query's names to the fixture identities: `nickname` is the String-typed optional property,
/// and every other name is the Json-typed `data` property.
struct FixtureResolver;
impl ModelResolver for FixtureResolver {
    fn resolve_property(&self, _model: &ankurah::proto::ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        use ankurah::core::value::ValueType;
        let value_type = if name == "nickname" { ValueType::String } else { ValueType::Json };
        Ok(Some(ResolvedProperty { id: fixture_prop(name), value_type }))
    }
}

fn verify_filterable(case: &TestCase) {
    let model = ankurah::proto::ModelId::EntityId(ankurah::proto::EntityId::from_bytes([0x77; 32]));
    for entity in &case.entities {
        let f = MockFilterable::new("QueryTest").with_json("data", entity.data.clone()).with_text("nickname", entity.nickname.clone());
        for exp in &case.expectations {
            let sel = ankql::parser::parse_selection(&exp.query).expect("parse");
            // Bind names and canonicalize literals for JSON path comparisons
            let resolved_sel = resolve_selection(&model, &FixtureResolver, sel).expect("resolve");
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
    pub nickname: Option<String>,
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
    let ctx = node.context_async(c).await.unwrap();

    for case in &cases {
        verify_filterable(case);

        let trx = ctx.begin();
        for entity in &case.entities {
            trx.create(&QueryTest { label: entity.label.clone(), data: Json::new(entity.data.clone()), nickname: entity.nickname.clone() })
                .await?;
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
