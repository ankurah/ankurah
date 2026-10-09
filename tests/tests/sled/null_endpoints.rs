//! NULL and absent values never become finite index-range endpoints. The
//! planner bounds only comparisons with literal values, IS NULL stays in
//! the residual predicate, and the conversion treats an infinite datum as
//! the unbounded side of its part, not as a key.

use ankurah::core::indexing::{IndexKeyPart, KeySpec};
use ankurah::{Value, ValueType};
use ankurah_storage_common::{ColumnPath, Endpoint, EngineColumns, KeyBoundComponent, KeyBounds, KeyDatum, Plan, Planner, PlannerConfig};
use ankurah_storage_sled::planner_integration::key_bounds_to_sled_range;

fn lower(query: &str) -> ankql::ast::Selection<EngineColumns> {
    ankql::selection::map_references(
        &ankql::parser::parse_selection(query).unwrap(),
        &|path| ColumnPath::new(path.first(), path.steps[1..].to_vec()),
        &|model| *model.as_id().expect("physical-column fixture"),
    )
}

fn plans(query: &str) -> Vec<Plan> { Planner::new(PlannerConfig::full_support()).plan(&lower(query), "id") }

#[test]
fn is_null_bounds_nothing_and_stays_residual() {
    let chosen = plans("name = 'a' AND status IS NULL").into_iter().next().unwrap();
    let Plan::Index { bounds, remaining_predicate, index_spec, .. } = chosen else { panic!("expected an index plan, got {chosen:?}") };
    assert_eq!(bounds.keyparts.iter().map(|part| part.column.as_str()).collect::<Vec<_>>(), ["name"]);
    assert_eq!(
        (&bounds.keyparts[0].low, &bounds.keyparts[0].high),
        (&Endpoint::incl(Value::String("a".into())), &Endpoint::incl(Value::String("a".into())))
    );
    assert!(index_spec.keyparts.iter().all(|part| part.key != "status"), "{index_spec:?}");
    assert_eq!(remaining_predicate, lower("status IS NULL").predicate);

    for plan in plans("status IS NULL") {
        if let Plan::Index { bounds, .. } = plan {
            assert!(bounds.keyparts.iter().all(|part| part.column != "status"), "{bounds:?}");
        }
    }
}

/// There is no NULL literal to compare against: `= NULL` does not parse, so
/// no comparison can carry a null into a bound; IS NULL is the only null
/// test, and it is its own predicate rather than a comparison.
#[test]
fn equals_null_has_no_literal_form() {
    assert!(ankql::parser::parse_selection("name = NULL").is_err());
    assert!(matches!(ankql::parser::parse_selection("name IS NULL").unwrap().predicate, ankql::ast::Predicate::IsNull(_)));
}

/// `build_bounds` in the planner constructs only `Endpoint::Value` from
/// literal values and the two unbounded ends; should an infinite datum ever
/// arrive, it is the unbounded side, never a key.
#[test]
fn infinite_endpoints_are_unbounded_sides_not_keys() {
    let spec = KeySpec::new(vec![IndexKeyPart::asc("name", ValueType::String)]);
    let component = |low, high| KeyBounds::new(vec![KeyBoundComponent { column: "name".into(), low, high }]);
    let infinite = component(
        Endpoint::Value { datum: KeyDatum::NegInfinity(ValueType::String), inclusive: true },
        Endpoint::Value { datum: KeyDatum::PosInfinity(ValueType::String), inclusive: true },
    );
    let unbounded = component(Endpoint::UnboundedLow(ValueType::String), Endpoint::UnboundedHigh(ValueType::String));
    let (infinite, unbounded) = (key_bounds_to_sled_range(&infinite, &spec).unwrap(), key_bounds_to_sled_range(&unbounded, &spec).unwrap());
    assert_eq!((infinite.start, infinite.end), (unbounded.start, unbounded.end));
}
