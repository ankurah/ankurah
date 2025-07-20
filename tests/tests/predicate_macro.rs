use ankurah::predicate;

#[test]
fn test_predicate_macro_unquoted_syntax() {
    // Test unquoted syntax with different operators and types
    let name = "Alice";
    let age = 25;
    let active = true;

    // Test basic equality
    assert_eq!(
        predicate!(name = { name }),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string(),))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("Alice".to_string(),))),
        }
    );

    // Test multiple operators and mixed types
    assert_eq!(
        predicate!(name = {name} AND age > {age} AND active = {active}),
        ankql::ast::Predicate::And(
            Box::new(ankql::ast::Predicate::And(
                Box::new(ankql::ast::Predicate::Comparison {
                    left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string(),))),
                    operator: ankql::ast::ComparisonOperator::Equal,
                    right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("Alice".to_string(),))),
                }),
                Box::new(ankql::ast::Predicate::Comparison {
                    left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string(),))),
                    operator: ankql::ast::ComparisonOperator::GreaterThan,
                    right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(25))),
                }),
            )),
            Box::new(ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("active".to_string(),))),
                operator: ankql::ast::ComparisonOperator::Equal,
                right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Boolean(true))),
            }),
        )
    );
}

#[test]
fn test_predicate_macro_in_clause() {
    // Test IN clause with multiple values
    let status1 = "active";
    let status2 = "pending";

    assert_eq!(
        predicate!(status IN ({status1}, {status2})),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("status".to_string(),))),
            operator: ankql::ast::ComparisonOperator::In,
            right: Box::new(ankql::ast::Expr::ExprList(vec![
                ankql::ast::Expr::Literal(ankql::ast::Literal::String("active".to_string())),
                ankql::ast::Expr::Literal(ankql::ast::Literal::String("pending".to_string())),
            ])),
        }
    );
}

#[test]
fn test_predicate_macro_quoted_syntax() {
    // Test quoted syntax with positional arguments and mixed types
    let name = "Bob";
    let age = 30;
    let active = true;

    assert_eq!(
        predicate!("name = {} AND age = {} AND active = {}", name, age, active),
        ankql::ast::Predicate::And(
            Box::new(ankql::ast::Predicate::And(
                Box::new(ankql::ast::Predicate::Comparison {
                    left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string(),))),
                    operator: ankql::ast::ComparisonOperator::Equal,
                    right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("Bob".to_string(),))),
                }),
                Box::new(ankql::ast::Predicate::Comparison {
                    left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string(),))),
                    operator: ankql::ast::ComparisonOperator::Equal,
                    right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(30))),
                }),
            )),
            Box::new(ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("active".to_string(),))),
                operator: ankql::ast::ComparisonOperator::Equal,
                right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Boolean(true))),
            }),
        )
    );
}

#[test]
fn test_predicate_macro_shorthand_syntax() {
    // Test shorthand syntax where {identifier} expands to identifier = {identifier}
    let name = "Alice";
    let age = 25;

    // Test single variable shorthand
    assert_eq!(
        predicate!({ name }),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string(),))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("Alice".to_string(),))),
        }
    );

    // Test multiple variables shorthand with AND
    assert_eq!(
        predicate!({name} AND {age}),
        ankql::ast::Predicate::And(
            Box::new(ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string(),))),
                operator: ankql::ast::ComparisonOperator::Equal,
                right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("Alice".to_string(),))),
            }),
            Box::new(ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string(),))),
                operator: ankql::ast::ComparisonOperator::Equal,
                right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(25))),
            }),
        )
    );

    // Verify that {name} is identical to name = {name}
    assert_eq!(predicate!({ name }), predicate!(name = { name }));
}

#[test]
fn test_predicate_macro_operator_shorthand() {
    // Test operator shorthand syntax: {>age} expands to age > {age}
    let age = 25;
    let count = 10;
    let status = "active";
    let score = 95.5;

    // Test greater than operator
    assert_eq!(
        predicate!({>age}),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string()))),
            operator: ankql::ast::ComparisonOperator::GreaterThan,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(25))),
        }
    );

    // Test less than or equal operator
    assert_eq!(
        predicate!({<=count}),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("count".to_string()))),
            operator: ankql::ast::ComparisonOperator::LessThanOrEqual,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(10))),
        }
    );

    // Test not equal operator
    assert_eq!(
        predicate!({!=status}),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("status".to_string()))),
            operator: ankql::ast::ComparisonOperator::NotEqual,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("active".to_string()))),
        }
    );

    // Test combined operators
    assert_eq!(
        predicate!({>age} AND {<=count}),
        ankql::ast::Predicate::And(
            Box::new(ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string()))),
                operator: ankql::ast::ComparisonOperator::GreaterThan,
                right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(25))),
            }),
            Box::new(ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("count".to_string()))),
                operator: ankql::ast::ComparisonOperator::LessThanOrEqual,
                right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(10))),
            }),
        )
    );

    // Test that {>=score} expands to score >= {score}
    assert_eq!(
        predicate!({>=score}),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("score".to_string()))),
            operator: ankql::ast::ComparisonOperator::GreaterThanOrEqual,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Float(95.5))),
        }
    );

    // Test that {=age} is equivalent to {age} (explicit equality)
    assert_eq!(predicate!({=age}), predicate!({ age }));

    // Test <> operator (alternative not equal)
    assert_eq!(
        predicate!({<>status}),
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("status".to_string()))),
            operator: ankql::ast::ComparisonOperator::NotEqual,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("active".to_string()))),
        }
    );
}

#[test]
fn test_predicate_macro_syntax_comparison() {
    let name = "Alice";
    let age = 25;

    // Test that quoted and unquoted syntax produce equivalent results
    let expected = ankql::ast::Predicate::And(
        Box::new(ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string()))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("Alice".to_string()))),
        }),
        Box::new(ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string()))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(25))),
        }),
    );

    // These should all produce the same result
    assert_eq!(predicate!("name = {} AND age = {}", name, age), expected);
    assert_eq!(predicate!("{name} AND age = {}", age), expected);
    assert_eq!(predicate!("name = {} AND {age}", name), expected);
    assert_eq!(predicate!("{name} AND age = {age}"), expected);

    assert_eq!(predicate!(name = {name} AND age = {age}), expected);
    assert_eq!(predicate!({name} AND age = {age}), expected);
    assert_eq!(predicate!(name = {name} AND {age}), expected);
    assert_eq!(predicate!({name} AND {age}), expected);
}

#[test]
fn test_predicate_macro_pure_syntax_forms() {
    // Test that both pure quoted and pure unquoted syntax work correctly
    // Mixed syntax is not supported because it would be ambiguous

    let foo_value = "test";
    let bar = "bar_value";

    // Expected AST for foo = "test" AND bar = "bar_value"
    let expected = ankql::ast::Predicate::And(
        Box::new(ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("foo".to_string()))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("test".to_string()))),
        }),
        Box::new(ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("bar".to_string()))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String("bar_value".to_string()))),
        }),
    );

    // Pure quoted syntax - uses positional arguments
    let quoted_result = predicate!("foo = {} AND bar = {}", foo_value, "bar_value");
    assert_eq!(quoted_result, expected);

    // Pure unquoted syntax - uses variable names directly
    let unquoted_result = predicate!(foo = {foo_value} AND bar = {bar});
    assert_eq!(unquoted_result, expected);

    // Both forms should produce identical AST
    assert_eq!(quoted_result, unquoted_result);
}

#[test]
fn test_predicate_macro_edge_cases() {
    let age_value = 25;

    // Test mixed literal/placeholder syntax - {name} is literal text "name", {} gets replaced
    let mixed_expected = ankql::ast::Predicate::And(
        Box::new(ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string()))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string()))),
        }),
        Box::new(ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string()))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(25))),
        }),
    );

    // Mixed literal and placeholder - {name} is literal "name", {} gets replaced with age_value
    assert_eq!(predicate!("name = name AND age = {}", age_value), mixed_expected);
}

#[test]
fn test_predicate_macro_list_expansion() {
    // Test list expansion in IN clauses with different collection types
    // This uses the Into<Expr> implementations that create ExprList for collections

    // Test with Vec<&str>
    let names = vec!["Alice", "Bob", "Charlie"];
    let vector_result = predicate!("name IN {names}");
    assert_eq!(
        vector_result,
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("name".to_string()))),
            operator: ankql::ast::ComparisonOperator::In,
            right: Box::new(ankql::ast::Expr::ExprList(vec![
                ankql::ast::Expr::Literal(ankql::ast::Literal::String("Alice".to_string())),
                ankql::ast::Expr::Literal(ankql::ast::Literal::String("Bob".to_string())),
                ankql::ast::Expr::Literal(ankql::ast::Literal::String("Charlie".to_string())),
            ])),
        }
    );

    // Test with array [i32; N]
    let ages = [25, 30, 35];
    let array_result = predicate!("age IN {ages}");
    assert_eq!(
        array_result,
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("age".to_string()))),
            operator: ankql::ast::ComparisonOperator::In,
            right: Box::new(ankql::ast::Expr::ExprList(vec![
                ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(25)),
                ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(30)),
                ankql::ast::Expr::Literal(ankql::ast::Literal::Integer(35)),
            ])),
        }
    );

    // Test with slice &[&str]
    let statuses = &["active", "pending"];
    let slice_result = predicate!("status IN {statuses}");
    assert_eq!(
        slice_result,
        ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Identifier(ankql::ast::Identifier::Property("status".to_string()))),
            operator: ankql::ast::ComparisonOperator::In,
            right: Box::new(ankql::ast::Expr::ExprList(vec![
                ankql::ast::Expr::Literal(ankql::ast::Literal::String("active".to_string())),
                ankql::ast::Expr::Literal(ankql::ast::Literal::String("pending".to_string())),
            ])),
        }
    );

    // Note: This list expansion works with the quoted syntax using {variable_name}.
    // True unquoted syntax like predicate!(name IN {names}) would require more complex
    // parsing to distinguish between single values and collections in the token stream
    // processing. The current quoted syntax with {} works perfectly for list expansion.
}
