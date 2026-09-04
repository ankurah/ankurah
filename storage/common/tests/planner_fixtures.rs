//! Planner fixture tests for ankurah-storage-common.
//!
//! Nothing in `storage/common` implements `Serialize`. The planner IR never crosses a
//! wire or a storage boundary, so there is no encoding to prove. What these fixtures
//! prove instead is **planner agreement**: given the same query, primary key and
//! engine capability config, does a port choose the same plans, in the same order?
//!
//! Order matters. `Planner::plan` returns a candidate list, deduplicated, with the
//! table scan appended last as a fallback, and callers pick from that list. A port that
//! produces the same set in a different order is not equivalent.
//!
//! ## The projection
//!
//! Because the IR has no `Serialize`, everything below `Plan` is projected into JSON by
//! hand, in the `proj_*` functions. That is deliberate: a `{:?}` snapshot would be a
//! contract on Rust's derive output, would churn whenever a field is added, and would
//! ask a TypeScript port to reproduce Rust's `Debug` rendering. A hand-written schema is
//! something a port can implement. The schema is documented field by field in
//! `storage/common/test_fixtures/README.md`.
//!
//! Two things are *not* hand-projected: `ankql::ast::Predicate` and
//! `ankql::ast::OrderByItem` pass through their real `Serialize`, so the AST JSON here
//! is the same shape as in `proto/test_fixtures/ankql_ast.json` and
//! `ankql/test_fixtures/parse_cases.json`. Likewise `ankurah_core::value::Value` and
//! `ValueType` use their real `Serialize`, matching `core/test_fixtures`. One canonical
//! shape per type across the whole fixture suite.
//!
//! - If `OVERWRITE_FIXTURES` env var is set: write the fixture.
//! - If NOT set: read it and assert it matches exactly.
//!
//! Run with `OVERWRITE_FIXTURES=1 cargo test -p ankurah-storage-common --test planner_fixtures` to regenerate.
use std::fs;
use std::path::PathBuf;

use ankql::parser::parse_selection;
use ankurah_core::indexing::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder};
use ankurah_core::value::{Value, ValueType};
use ankurah_storage_common::bounds::normalize;
use ankurah_storage_common::{
    CanonicalRange, Endpoint, KeyBoundComponent, KeyBounds, KeyDatum, OrderByComponents, Plan,
    Planner, PlannerConfig, ScanDirection,
};

use serde_json::{json, Value as J};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_fixtures")
        .join(name)
}

fn check_or_write_bytes(name: &str, data: &[u8]) {
    let path = fixture_path(name);
    let overwrite = std::env::var("OVERWRITE_FIXTURES").is_ok();

    if overwrite {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!("Wrote fixture: {} ({} bytes)", path.display(), data.len());
    } else if !path.exists() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!(
            "Generated missing fixture: {} ({} bytes)",
            path.display(),
            data.len()
        );
    } else {
        let expected = fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "Failed to read fixture {}: {}. Run with OVERWRITE_FIXTURES=1 to generate.",
                path.display(),
                e
            )
        });
        assert_eq!(data, &expected[..], "Fixture mismatch for {}", name);
    }
}

// ---- Hand-written projection of the planner IR --------------------------------
//
// Every function here defines one node of the published schema. Changing one of them
// changes the contract, so they are written out explicitly rather than derived.

fn proj_value(v: &Value) -> J {
    // Value's own Serialize, so this matches core/test_fixtures exactly.
    serde_json::to_value(v).expect("Value has a JSON form")
}

fn proj_value_type(t: &ValueType) -> J {
    serde_json::to_value(t).expect("ValueType has a JSON form")
}

fn proj_direction(d: &IndexDirection) -> J {
    match d {
        IndexDirection::Asc => json!("Asc"),
        IndexDirection::Desc => json!("Desc"),
    }
}

fn proj_nulls(n: &Option<NullsOrder>) -> J {
    match n {
        None => J::Null,
        Some(NullsOrder::First) => json!("First"),
        Some(NullsOrder::Last) => json!("Last"),
    }
}

fn proj_index_key_part(p: &IndexKeyPart) -> J {
    json!({
        "column": p.column,
        "sub_path": p.sub_path,
        "direction": proj_direction(&p.direction),
        "value_type": proj_value_type(&p.value_type),
        "nulls": proj_nulls(&p.nulls),
        "collation": p.collation,
    })
}

/// A KeySpec is projected as a bare array of keyparts - the wrapper struct carries no
/// information beyond its one field.
fn proj_key_spec(spec: &KeySpec) -> J {
    J::Array(spec.keyparts.iter().map(proj_index_key_part).collect())
}

fn proj_key_datum(d: &KeyDatum) -> J {
    match d {
        KeyDatum::Val(v) => json!({ "kind": "Val", "value": proj_value(v) }),
        KeyDatum::NegInfinity(t) => {
            json!({ "kind": "NegInfinity", "value_type": proj_value_type(t) })
        }
        KeyDatum::PosInfinity(t) => {
            json!({ "kind": "PosInfinity", "value_type": proj_value_type(t) })
        }
    }
}

fn proj_endpoint(e: &Endpoint) -> J {
    match e {
        Endpoint::UnboundedLow(t) => {
            json!({ "kind": "UnboundedLow", "value_type": proj_value_type(t) })
        }
        Endpoint::UnboundedHigh(t) => {
            json!({ "kind": "UnboundedHigh", "value_type": proj_value_type(t) })
        }
        Endpoint::Value { datum, inclusive } => json!({
            "kind": "Value",
            "datum": proj_key_datum(datum),
            "inclusive": inclusive,
        }),
    }
}

fn proj_key_bound_component(c: &KeyBoundComponent) -> J {
    json!({
        "column": c.column,
        "low": proj_endpoint(&c.low),
        "high": proj_endpoint(&c.high),
    })
}

/// KeyBounds is projected as a bare array, same reasoning as KeySpec.
fn proj_key_bounds(b: &KeyBounds) -> J {
    J::Array(b.keyparts.iter().map(proj_key_bound_component).collect())
}

fn proj_scan_direction(d: &ScanDirection) -> J {
    match d {
        ScanDirection::Forward => json!("Forward"),
        ScanDirection::Reverse => json!("Reverse"),
    }
}

fn proj_order_by_components(o: &OrderByComponents) -> J {
    json!({
        "presort": o.presort.iter().map(|i| serde_json::to_value(i).unwrap()).collect::<Vec<_>>(),
        "spill": o.spill.iter().map(|i| serde_json::to_value(i).unwrap()).collect::<Vec<_>>(),
        "is_satisfied": o.is_satisfied(),
        "is_global_spill": o.is_global_spill(),
    })
}

fn proj_plan(plan: &Plan) -> J {
    match plan {
        Plan::Index {
            index_spec,
            scan_direction,
            bounds,
            remaining_predicate,
            order_by_spill,
        } => json!({
            "kind": "Index",
            "index_spec": proj_key_spec(index_spec),
            "scan_direction": proj_scan_direction(scan_direction),
            "bounds": proj_key_bounds(bounds),
            "remaining_predicate": serde_json::to_value(remaining_predicate).unwrap(),
            "order_by_spill": proj_order_by_components(order_by_spill),
        }),
        Plan::TableScan {
            bounds,
            scan_direction,
            remaining_predicate,
            order_by_spill,
        } => json!({
            "kind": "TableScan",
            "bounds": proj_key_bounds(bounds),
            "scan_direction": proj_scan_direction(scan_direction),
            "remaining_predicate": serde_json::to_value(remaining_predicate).unwrap(),
            "order_by_spill": proj_order_by_components(order_by_spill),
        }),
        Plan::EmptyScan => json!({ "kind": "EmptyScan" }),
    }
}

fn proj_canonical_range(r: &CanonicalRange) -> J {
    fn side(s: &Option<(Vec<Value>, bool)>) -> J {
        match s {
            None => J::Null,
            Some((tuple, open)) => json!({
                "tuple": tuple.iter().map(proj_value).collect::<Vec<_>>(),
                "open": open,
            }),
        }
    }
    json!({ "lower": side(&r.lower), "upper": side(&r.upper) })
}

// ---- Plan cases ---------------------------------------------------------------

struct PlanCase {
    query: &'static str,
    primary_key: &'static str,
    /// Which capability configs to run. Both, when the config is what the case is about.
    configs: &'static [&'static str],
    note: &'static str,
}

const BOTH: &[&str] = &["indexeddb", "full_support"];
const IDB: &[&str] = &["indexeddb"];

const PLAN_CASES: &[PlanCase] = &[
    PlanCase {
        query: "status = 'active'",
        primary_key: "id",
        configs: IDB,
        note: "equality-only, single column",
    },
    PlanCase {
        query: "status = 'active' AND kind = 'album'",
        primary_key: "id",
        configs: IDB,
        note: "equality-only, composite - keypart order follows conjunct order, not name order",
    },
    PlanCase {
        query: "score > 5",
        primary_key: "id",
        configs: IDB,
        note: "one inequality, no ORDER BY",
    },
    PlanCase {
        query: "score > 5 AND rank < 10",
        primary_key: "id",
        configs: IDB,
        note: "two inequalities on different fields - one index plan per inequality field",
    },
    PlanCase {
        query: "score > 5 AND score < 10",
        primary_key: "id",
        configs: IDB,
        note: "two inequalities on the SAME field - one bounded range, not two plans",
    },
    PlanCase {
        query: "status = 'active' AND score > 5",
        primary_key: "id",
        configs: IDB,
        note: "equality prefix plus a range on the next keypart",
    },
    PlanCase {
        query: "status = 'active' ORDER BY score DESC",
        primary_key: "id",
        configs: BOTH,
        note: "ORDER-FIRST with a single DESC key. Under indexeddb the index is ASC-only \
               and the scan reverses; under full_support the keypart itself is DESC",
    },
    PlanCase {
        query: "status = 'active' ORDER BY a ASC, b DESC",
        primary_key: "id",
        configs: BOTH,
        note: "mixed-direction ORDER BY - the case the two configs disagree on. indexeddb \
               keeps the longest same-direction prefix and spills the rest; full_support \
               satisfies the whole ordering in the index",
    },
    PlanCase {
        query: "status = 'active' ORDER BY a DESC, b DESC",
        primary_key: "id",
        configs: BOTH,
        note: "uniform DESC ORDER BY - indexeddb can serve it all by reversing the scan, \
               so nothing spills even without DESC index support",
    },
    PlanCase {
        query: "score > 5 ORDER BY score DESC",
        primary_key: "id",
        configs: IDB,
        note: "covered inequality: the ORDER BY field is the one with the range, so \
               INEQ-FIRST is deliberately suppressed and only ORDER-FIRST is emitted",
    },
    PlanCase {
        query: "score > 5 ORDER BY name ASC",
        primary_key: "id",
        configs: IDB,
        note: "uncovered inequality: the ORDER BY field has no range, so both ORDER-FIRST \
               and INEQ-FIRST are emitted",
    },
    PlanCase {
        query: "status = 'active' AND score > 5 ORDER BY name ASC",
        primary_key: "id",
        configs: IDB,
        note: "equality plus an uncovered inequality plus an unrelated ORDER BY",
    },
    PlanCase {
        query: "id = 'x'",
        primary_key: "id",
        configs: IDB,
        note: "primary-key equality only - index generation is skipped and a table scan is \
               returned directly",
    },
    PlanCase {
        query: "id > 5",
        primary_key: "id",
        configs: IDB,
        note: "primary-key range only - short-circuits to a single table scan with primary \
               key bounds",
    },
    PlanCase {
        query: "id > 5 ORDER BY id DESC",
        primary_key: "id",
        configs: IDB,
        note: "primary-key range plus primary-key ORDER BY - short-circuits, and the scan \
               direction comes from the ORDER BY",
    },
    PlanCase {
        query: "status = 'active' ORDER BY id DESC",
        primary_key: "id",
        configs: IDB,
        note: "primary-key ORDER BY alongside a non-primary predicate - the short-circuit \
               does NOT apply because there is another meaningful predicate",
    },
    PlanCase {
        query: "score > 10 AND score < 5",
        primary_key: "id",
        configs: IDB,
        note: "contradictory range on one field - the bounds are empty, so EmptyScan, and \
               no table-scan fallback is appended",
    },
    PlanCase {
        query: "status = 'a' OR status = 'b'",
        primary_key: "id",
        configs: IDB,
        note: "OR breaks the conjunct chain: ConjunctFinder yields the whole OR as one \
               opaque conjunct, so no index is usable and the whole predicate is residual",
    },
    PlanCase {
        query: "status = 'active' AND (a = 1 OR b = 2)",
        primary_key: "id",
        configs: IDB,
        note: "an OR nested inside an AND - the equality is still indexable, the OR stays \
               in remaining_predicate",
    },
    PlanCase {
        query: "status IS NULL",
        primary_key: "id",
        configs: IDB,
        note: "IS NULL is not an equality or a range, so it cannot drive an index",
    },
    PlanCase {
        query: "",
        primary_key: "id",
        configs: IDB,
        note: "empty query - the parser yields Predicate::True, which is not indexable; \
               expect a bare table scan",
    },
    PlanCase {
        query: "status = 'active' LIMIT 10",
        primary_key: "id",
        configs: IDB,
        note: "LIMIT is carried on the Selection but the planner ignores it - the plans \
               must be identical to the bare equality case",
    },
    PlanCase {
        query: "licensing.territory = 'US'",
        primary_key: "id",
        configs: IDB,
        note: "a dotted path as the indexed column - watch whether sub_path is populated",
    },
];

fn config_for(name: &str) -> PlannerConfig {
    match name {
        "indexeddb" => PlannerConfig::indexeddb(),
        "full_support" => PlannerConfig::full_support(),
        other => panic!("unknown planner config {other}"),
    }
}

// ---- Bounds normalization cases -----------------------------------------------

fn val_str(s: &str) -> Value {
    Value::String(s.to_string())
}

fn incl(v: Value) -> Endpoint {
    Endpoint::Value {
        datum: KeyDatum::Val(v),
        inclusive: true,
    }
}

fn excl(v: Value) -> Endpoint {
    Endpoint::Value {
        datum: KeyDatum::Val(v),
        inclusive: false,
    }
}

fn part(column: &str, low: Endpoint, high: Endpoint) -> KeyBoundComponent {
    KeyBoundComponent {
        column: column.to_string(),
        low,
        high,
    }
}

/// `(label, note, bounds)` triples fed straight to `bounds::normalize`.
fn bounds_cases() -> Vec<(&'static str, &'static str, KeyBounds)> {
    vec![
        (
            "empty",
            "no keyparts at all",
            KeyBounds::empty(),
        ),
        (
            "single_keypart_equality",
            "one keypart, low == high, both inclusive. normalize() has an explicit special \
             case for exactly this shape at the end of the function - it returns an \
             open-ended range rather than a closed one",
            KeyBounds::new(vec![part(
                "status",
                incl(val_str("active")),
                incl(val_str("active")),
            )]),
        ),
        (
            "two_keypart_equality",
            "two equality keyparts - the equality prefix should absorb both",
            KeyBounds::new(vec![
                part("status", incl(val_str("active")), incl(val_str("active"))),
                part("kind", incl(val_str("album")), incl(val_str("album"))),
            ]),
        ),
        (
            "equality_prefix_then_closed_range",
            "one equality then a bounded range - the prefix is collapsed and the range \
             materializes on both sides",
            KeyBounds::new(vec![
                part("status", incl(val_str("active")), incl(val_str("active"))),
                part("score", incl(Value::I32(5)), incl(Value::I32(10))),
            ]),
        ),
        (
            "equality_prefix_then_open_range",
            "same, but the range endpoints are exclusive - watch the open flags",
            KeyBounds::new(vec![
                part("status", incl(val_str("active")), incl(val_str("active"))),
                part("score", excl(Value::I32(5)), excl(Value::I32(10))),
            ]),
        ),
        (
            "unbounded_high",
            "a range with no upper endpoint - normalize returns early with upper: None",
            KeyBounds::new(vec![part(
                "score",
                incl(Value::I32(5)),
                Endpoint::UnboundedHigh(ValueType::I32),
            )]),
        ),
        (
            "unbounded_low",
            "a range with no lower endpoint - the lower tuple stays short",
            KeyBounds::new(vec![part(
                "score",
                Endpoint::UnboundedLow(ValueType::I32),
                incl(Value::I32(10)),
            )]),
        ),
        (
            "equality_prefix_then_unbounded_high",
            "equality prefix followed by a half-open range",
            KeyBounds::new(vec![
                part("status", incl(val_str("active")), incl(val_str("active"))),
                part(
                    "score",
                    incl(Value::I32(5)),
                    Endpoint::UnboundedHigh(ValueType::I32),
                ),
            ]),
        ),
        (
            "both_unbounded",
            "a keypart with no endpoints on either side",
            KeyBounds::new(vec![part(
                "score",
                Endpoint::UnboundedLow(ValueType::I32),
                Endpoint::UnboundedHigh(ValueType::I32),
            )]),
        ),
        (
            "infinity_datum",
            "a KeyDatum sentinel rather than an Endpoint sentinel - normalize's match arms \
             do not treat KeyDatum::PosInfinity as a value, so this exits early",
            KeyBounds::new(vec![part(
                "score",
                Endpoint::Value {
                    datum: KeyDatum::NegInfinity(ValueType::I32),
                    inclusive: true,
                },
                Endpoint::Value {
                    datum: KeyDatum::PosInfinity(ValueType::I32),
                    inclusive: true,
                },
            )]),
        ),
    ]
}

// ---- The fixture --------------------------------------------------------------

#[test]
fn test_planner_fixture() {
    let mut plan_cases = Vec::new();

    for case in PLAN_CASES {
        let selection = parse_selection(case.query).unwrap_or_else(|e| {
            panic!(
                "planner fixture query {:?} must parse (see ankql/test_fixtures for what \
                 the parser accepts): {}",
                case.query, e
            )
        });

        for config_name in case.configs {
            let planner = Planner::new(config_for(config_name));
            let plans = planner.plan(&selection, case.primary_key);

            plan_cases.push(json!({
                "query": case.query,
                "primary_key": case.primary_key,
                "config": {
                    "name": config_name,
                    "supports_desc_indexes": *config_name == "full_support",
                },
                "note": case.note,
                "selection_ast": serde_json::to_value(&selection).unwrap(),
                "plan_count": plans.len(),
                "plans": plans.iter().map(proj_plan).collect::<Vec<_>>(),
            }));
        }
    }

    let mut bounds_json = Vec::new();
    for (label, note, bounds) in bounds_cases() {
        let (range, eq_prefix_len, eq_prefix_values) = normalize(&bounds);
        bounds_json.push(json!({
            "label": label,
            "note": note,
            "bounds": proj_key_bounds(&bounds),
            "normalized": {
                "canonical_range": proj_canonical_range(&range),
                "eq_prefix_len": eq_prefix_len,
                "eq_prefix_values": eq_prefix_values.iter().map(proj_value).collect::<Vec<_>>(),
            },
        }));
    }

    let mut root = serde_json::Map::new();
    root.insert("fixture".into(), json!("plans.json"));
    root.insert(
        "produced_by".into(),
        json!("ankurah_storage_common::Planner::plan and ankurah_storage_common::bounds::normalize"),
    );
    root.insert(
        "schema".into(),
        json!("hand-written projection defined in storage/common/tests/planner_fixtures.rs; \
               documented field by field in storage/common/test_fixtures/README.md"),
    );
    root.insert("plan_case_count".into(), json!(plan_cases.len()));
    root.insert("bounds_case_count".into(), json!(bounds_json.len()));
    root.insert("plan_cases".into(), J::Array(plan_cases));
    root.insert("bounds_cases".into(), J::Array(bounds_json));

    let mut text = serde_json::to_string_pretty(&J::Object(root)).unwrap();
    text.push('\n');
    check_or_write_bytes("plans.json", text.as_bytes());
}
