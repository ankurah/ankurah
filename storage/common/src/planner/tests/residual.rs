use super::*;

#[test]
fn remaining_predicate_respects_endpoint_inclusivity_and_strength() {
    let planner = Planner::new(PlannerConfig::full_support());
    // Supply bounds directly: the planner normally tightens them to match the query.
    for (predicate, bounds, expected) in [
        (selection!("age > 5").predicate, bounds!("age" => (5..)), selection!("age > 5").predicate),
        (selection!("age >= 5").predicate, bounds!("age" => (5..)), Predicate::True),
        (selection!("age > 5").predicate, bounds_list!(open_lower!("age" => 5..)), Predicate::True),
        (selection!("age >= 5").predicate, bounds_list!(open_lower!("age" => 5..)), Predicate::True),
        (selection!("age > 5").predicate, bounds!("age" => (6..)), Predicate::True),
        (selection!("age >= 5").predicate, bounds!("age" => (4..)), selection!("age >= 5").predicate),
        (selection!("age < 5").predicate, bounds!("age" => (..=5)), selection!("age < 5").predicate),
        (selection!("age <= 5").predicate, bounds!("age" => (..=5)), Predicate::True),
        (selection!("age < 5").predicate, bounds!("age" => (..5)), Predicate::True),
        (selection!("age <= 5").predicate, bounds!("age" => (..5)), Predicate::True),
        (selection!("age < 5").predicate, bounds!("age" => (..=4)), Predicate::True),
        (selection!("age <= 5").predicate, bounds!("age" => (..=6)), selection!("age <= 5").predicate),
    ] {
        assert_eq!(planner.calculate_remaining_predicate(std::slice::from_ref(&predicate), &bounds), expected, "{predicate:?}, {bounds:?}");
    }
}

#[test]
fn remaining_predicate_removes_equality_only_for_matching_point_bounds() {
    let planner = Planner::new(PlannerConfig::full_support());
    for (bounds, expected) in [
        (bounds!("age" => (5..=5)), Predicate::True),
        (bounds!("age" => (4..=4)), selection!("age = 5").predicate),
        (bounds!("age" => (4..=5)), selection!("age = 5").predicate),
        (bounds!("age" => (5..=6)), selection!("age = 5").predicate),
        (bounds!("age" => (5..)), selection!("age = 5").predicate),
        (bounds!("age" => (..=5)), selection!("age = 5").predicate),
    ] {
        assert_eq!(planner.calculate_remaining_predicate(&[selection!("age = 5").predicate], &bounds), expected, "{bounds:?}");
    }
}

#[test]
fn remaining_predicate_keeps_conditions_without_enforcing_bounds() {
    let planner = Planner::new(PlannerConfig::full_support());
    for (predicate, bounds) in [
        (selection!("age > 5").predicate, KeyBounds::empty()),
        (selection!("age > 5").predicate, bounds!("other" => (5..))),
        (selection!("age > 5").predicate, bounds!("age" => (..10))),
        (selection!("age < 5").predicate, bounds!("age" => (0..))),
        (selection!("age > 5").predicate, bounds!("age" => ("abc"..))),
        (selection!("age < 5").predicate, bounds!("age" => (.."abc"))),
        (selection!("age != 5").predicate, bounds!("age" => (0..10))),
        (selection!("age IN (3, 5)").predicate, bounds!("age" => (0..10))),
        (selection!("NOT (age > 5 AND age < 10)").predicate, bounds!("age" => (6..9))),
        (selection!("age > 5 OR other = 1").predicate, bounds!("age" => (6..))),
    ] {
        assert_eq!(planner.calculate_remaining_predicate(std::slice::from_ref(&predicate), &bounds), predicate, "{bounds:?}");
    }
}

#[test]
fn remaining_predicate_recombines_only_unenforced_conjuncts() {
    let planner = Planner::new(PlannerConfig::full_support());
    assert_eq!(planner.calculate_remaining_predicate(&[], &KeyBounds::empty()), Predicate::True);
    assert_eq!(
        planner.calculate_remaining_predicate(
            &[selection!("age >= 5").predicate, selection!("age <= 10").predicate],
            &bounds!("age" => (5..=10)),
        ),
        Predicate::True
    );
    assert_eq!(
        planner.calculate_remaining_predicate(
            &[selection!("age > 5").predicate, selection!("age >= 5").predicate, selection!("age != 8").predicate],
            &bounds!("age" => (5..)),
        ),
        selection!("age > 5 AND age != 8").predicate
    );
}

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

#[test]
fn equality_prefix_ignores_redundant_desc_with_full_support() {
    assert_eq!(
        plan_full_support!("foo = 5 ORDER BY foo DESC, bar DESC"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![asc!("foo", ValueType::I32), desc!("bar", ValueType::String)]),
                scan_direction: ScanDirection::Forward,
                bounds: bounds!("foo" => (5..=5)),
                remaining_predicate: Predicate::True,
                order_by_spill: order_by_components!(presort: [oby_desc!("foo"), oby_desc!("bar")]),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("foo = 5").predicate,
                order_by_spill: order_by_components!(spill: [oby_desc!("foo"), oby_desc!("bar")]),
            },
        ]
    );
}

#[test]
fn equality_prefix_ignores_redundant_desc_with_indexeddb() {
    assert_eq!(
        plan!("foo = 5 ORDER BY foo DESC, bar DESC"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![asc!("foo", ValueType::I32), asc!("bar", ValueType::String)]),
                scan_direction: ScanDirection::Reverse,
                bounds: bounds!("foo" => (5..=5)),
                remaining_predicate: Predicate::True,
                order_by_spill: order_by_components!(presort: [oby_desc!("foo"), oby_desc!("bar")]),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("foo = 5").predicate,
                order_by_spill: order_by_components!(spill: [oby_desc!("foo"), oby_desc!("bar")]),
            },
        ]
    );
}

#[test]
fn contradictory_ranges_are_empty_in_either_direction() {
    assert_eq!(plan!("foo > 10 AND foo < 0"), vec![Plan::EmptyScan]);
    assert_eq!(plan!("foo > 10 AND foo < 0 ORDER BY foo ASC"), vec![Plan::EmptyScan]);
    assert_eq!(plan!("foo > 10 AND foo < 0 ORDER BY foo DESC"), vec![Plan::EmptyScan]);
    assert_eq!(plan_full_support!("foo > 10 AND foo < 0"), vec![Plan::EmptyScan]);
    assert_eq!(plan_full_support!("foo > 10 AND foo < 0 ORDER BY foo ASC"), vec![Plan::EmptyScan]);
    assert_eq!(plan_full_support!("foo > 10 AND foo < 0 ORDER BY foo DESC"), vec![Plan::EmptyScan]);
}

#[test]
fn bounded_range_asc_with_full_support() {
    assert_eq!(
        plan_full_support!("foo > 10 AND foo < 20 ORDER BY foo ASC"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![asc!("foo", ValueType::String)]),
                scan_direction: ScanDirection::Forward,
                bounds: bounds_list!(open_lower!("foo" => 10..20)),
                remaining_predicate: Predicate::True,
                order_by_spill: order_by_components!(presort: [oby_asc!("foo")]),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("foo > 10 AND foo < 20").predicate,
                order_by_spill: order_by_components!(spill: [oby_asc!("foo")]),
            },
        ]
    );
}

#[test]
fn bounded_range_desc_with_full_support() {
    assert_eq!(
        plan_full_support!("foo > 10 AND foo < 20 ORDER BY foo DESC"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![desc!("foo", ValueType::String)]),
                scan_direction: ScanDirection::Forward,
                bounds: bounds_list!(open_lower!("foo" => 10..20)),
                remaining_predicate: Predicate::True,
                order_by_spill: order_by_components!(presort: [oby_desc!("foo")]),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("foo > 10 AND foo < 20").predicate,
                order_by_spill: order_by_components!(spill: [oby_desc!("foo")]),
            },
        ]
    );
}

#[test]
fn bounded_range_asc_with_indexeddb() {
    assert_eq!(
        plan!("foo > 10 AND foo < 20 ORDER BY foo ASC"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![asc!("foo", ValueType::String)]),
                scan_direction: ScanDirection::Forward,
                bounds: bounds_list!(open_lower!("foo" => 10..20)),
                remaining_predicate: Predicate::True,
                order_by_spill: order_by_components!(presort: [oby_asc!("foo")]),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("foo > 10 AND foo < 20").predicate,
                order_by_spill: order_by_components!(spill: [oby_asc!("foo")]),
            },
        ]
    );
}

#[test]
fn bounded_range_desc_with_indexeddb() {
    assert_eq!(
        plan!("foo > 10 AND foo < 20 ORDER BY foo DESC"),
        vec![
            Plan::Index {
                index_spec: KeySpec::new(vec![asc!("foo", ValueType::String)]),
                scan_direction: ScanDirection::Reverse,
                bounds: bounds_list!(open_lower!("foo" => 10..20)),
                remaining_predicate: Predicate::True,
                order_by_spill: order_by_components!(presort: [oby_desc!("foo")]),
            },
            Plan::TableScan {
                bounds: KeyBounds::empty(),
                scan_direction: ScanDirection::Forward,
                remaining_predicate: selection!("foo > 10 AND foo < 20").predicate,
                order_by_spill: order_by_components!(spill: [oby_desc!("foo")]),
            },
        ]
    );
}
