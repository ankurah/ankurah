//! IndexedDB Predicate Checks
//!
//! Verifies predicate evaluation consistency between IndexedDB and in-memory Filterable.
//! Test cases loaded from `tests/predicate_cases.json`.
//!
//! Run with: ./scripts/run-wasm-tests.sh indexeddb_wasm

mod common;

use ankurah::core::selection::filter::{evaluate_predicate, Filterable};
use ankurah::core::type_resolver::TypeResolver;
use ankurah::core::value::Value;
use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT, Model, Node, PermissiveAgent};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::setup;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

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
}

fn verify_filterable(case: &TestCase) {
    let type_resolver = TypeResolver::new();
    for entity in &case.entities {
        let f = MockFilterable::new("QueryTest").with_json("data", entity.data.clone());
        for exp in &case.expectations {
            let sel = ankql::parser::parse_selection(&exp.query).expect("parse");
            // Apply type resolver to convert literals for JSON path comparisons
            let resolved_sel = type_resolver.resolve_selection_types(sel);
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

// =============================================================================
// INDEXEDDB PREDICATE CHECK
// =============================================================================

#[wasm_bindgen_test]
async fn test_indexeddb_predicate_checks() -> Result<(), anyhow::Error> {
    setup();
    let cases = all_test_cases();
    let db_name = format!("test_predicate_{}", ulid::Ulid::new());
    let storage = IndexedDBStorageEngine::open(&db_name).await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

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
            assert_eq!(actual, expected, "[IndexedDB] case={} query='{}'", case.name, exp.query);
        }
    }

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
