use super::*;

fn lower(query: &str) -> ankql::ast::Selection<EngineColumns> {
    ankql::selection::map_references(
        &ankql::parser::parse_selection(query).unwrap(),
        &|path| ColumnPath::new(path.first(), path.steps[1..].to_vec()),
        &|model| *model.as_id().expect("physical-column fixture"),
    )
}

#[test]
fn bounded_column_keeps_its_other_conditions_with_full_index_support() {
    assert_eq!(
        plan_full_support!("age > 25 AND age != 30"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![asc!("age", ValueType::I32)]),
                scan_direction: ScanDirection::Forward,
                bounds: bounds_list!(open_lower!("age" => 25..)),
                remaining_predicate: selection!("age != 30").predicate,
                order_by_spill: order_by_components!(),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("age > 25 AND age != 30").predicate,
                order_by_spill: order_by_components!(),
            },
        ]
    );
}

#[test]
fn bounded_column_keeps_its_other_conditions_with_indexeddb_capabilities() {
    assert_eq!(
        plan!("age > 25 AND age != 30"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![asc!("age", ValueType::I32)]),
                scan_direction: ScanDirection::Forward,
                bounds: bounds_list!(open_lower!("age" => 25..)),
                remaining_predicate: selection!("age != 30").predicate,
                order_by_spill: order_by_components!(),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("age > 25 AND age != 30").predicate,
                order_by_spill: order_by_components!(),
            },
        ]
    );
}

#[test]
fn residuals_contain_only_conditions_not_enforced_by_bounds() {
    for config in [PlannerConfig::full_support(), PlannerConfig::indexeddb()] {
        for (query, residual) in [
            ("owner = 'bob' AND owner = 'alice'", Some("owner = 'alice'")),
            ("owner = 'bob' AND owner = 'bob'", None),
            ("owner = 'a' AND owner = 'b' AND owner = 'c'", Some("owner = 'b' AND owner = 'c'")),
            ("owner = 'bob' AND status = 'open' AND owner = 'alice'", Some("owner = 'alice'")),
            ("owner = 'bob' AND owner = 'alice' ORDER BY owner, label", Some("owner = 'alice'")),
            ("age = 5 AND age > 10", Some("age > 10")),
            ("age > 10 AND age = 5", Some("age > 10")),
            ("age = 5 AND age > 10 ORDER BY label", Some("age > 10")),
            ("age = 5 AND age > 3 AND age <= 5", None),
            ("age = 5 AND age >= 5 AND age < 10", None),
            ("age = 5 AND age > 5", Some("age > 5")),
            ("age = 5 AND age < 5", Some("age < 5")),
            ("age = 5 AND age != 5", Some("age != 5")),
            ("age > 3 AND age < 10", None),
            ("age >= 3 AND age > 3 AND age > 1", None),
            ("age <= 10 AND age < 10 AND age < 20", None),
            ("age > 5 AND age > 'abc'", Some("age > 'abc'")),
            ("age < 5 AND age < 'abc'", Some("age < 'abc'")),
            ("age > 25 AND age IN (30, 40)", Some("age IN (30, 40)")),
            ("age > 25 AND NOT (age = 30)", Some("NOT (age = 30)")),
            ("(owner = 'a' OR reviewer = 'a') AND owner = 'b'", Some("owner = 'a' OR reviewer = 'a'")),
            ("data.age > 25 AND data.age != 30", Some("data.age != 30")),
            // An unbounded earlier ORDER BY column prevents a bound on age.
            ("age > 25 ORDER BY name, age", Some("age > 25")),
        ] {
            let selection = lower(query);
            let expected = match residual {
                Some(residual) => lower(residual).predicate,
                None => ankql::ast::Predicate::True,
            };
            let plans = Planner::new(config.clone()).plan(&selection, "id");
            let mut index_plans = 0;
            for plan in &plans {
                match plan {
                    Plan::Index { remaining_predicate, index_spec, .. } => {
                        index_plans += 1;
                        assert_eq!(remaining_predicate, &expected, "{query}: {plan:?}");
                        let paths: std::collections::HashSet<_> = index_spec.keyparts.iter().map(|part| part.full_path()).collect();
                        assert_eq!(paths.len(), index_spec.keyparts.len(), "duplicate index columns: {plan:?}");
                    }
                    Plan::TableScan { remaining_predicate, .. } => {
                        assert_eq!(remaining_predicate, &selection.predicate, "{query}: {plan:?}");
                    }
                    Plan::EmptyScan => panic!("these cases require bounds plus a residual: {query}"),
                }
            }
            assert!(index_plans > 0, "expected an index plan for {query}");
        }
    }
}
