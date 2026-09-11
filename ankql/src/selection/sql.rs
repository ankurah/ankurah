use crate::ast::{ComparisonOperator, Expr, Predicate, Stage};
use crate::error::SqlGenerationError;
use ankurah_core_types::{Path, Value};

pub(crate) fn write_path<S: Stage>(path: &S::Path, output: &mut impl std::fmt::Write) -> std::fmt::Result {
    for (i, step) in path.display_steps().enumerate() {
        if i > 0 {
            output.write_char('.')?;
        }
        write!(output, "\"{}\"", step.replace('"', "\"\""))?;
    }
    Ok(())
}

fn write_string(value: &str, buffer: &mut String) {
    buffer.push('\'');
    buffer.push_str(&value.replace('\'', "''"));
    buffer.push('\'');
}

fn generate_expr_sql<S: Stage>(
    expr: &Expr<S>,
    placeholder_count: &mut Option<usize>,
    found_placeholders: &mut usize,
    buffer: &mut String,
) -> Result<(), SqlGenerationError> {
    match expr {
        Expr::Placeholder => {
            *found_placeholders += 1;

            // Check if we're exceeding the expected count
            if let Some(expected) = placeholder_count {
                if *found_placeholders > *expected {
                    return Err(SqlGenerationError::PlaceholderCountMismatch { expected: *expected, found: *found_placeholders });
                }
            }

            buffer.push('?');
        }
        Expr::Literal(lit) => match lit {
            Value::I16(i) => {
                buffer.push_str(&i.to_string());
            }
            Value::I32(i) => {
                buffer.push_str(&i.to_string());
            }
            Value::I64(i) => {
                buffer.push_str(&i.to_string());
            }
            Value::F64(f) => {
                if !f.is_finite() {
                    return Err(SqlGenerationError::InvalidExpression("Non-finite numbers have no AnkQL literal syntax".to_string()));
                }
                buffer.push_str(&format!("{f:?}"));
            }
            Value::Bool(b) => {
                buffer.push_str(if *b { "true" } else { "false" });
            }
            Value::String(s) => {
                write_string(s, buffer);
            }
            Value::EntityId(id) => {
                write_string(&id.to_base64(), buffer);
            }
            Value::Object(_) | Value::Binary(_) => {
                return Err(SqlGenerationError::InvalidExpression("Object and Binary values have no AnkQL literal syntax".to_string()));
            }
            Value::Json(value) => {
                write_string(&value.to_string(), buffer);
            }
        },
        Expr::Path(path) => {
            let _ = write_path::<S>(path, buffer);
        }
        Expr::ExprList(exprs) => {
            if exprs.is_empty() {
                return Err(SqlGenerationError::InvalidExpression("Empty lists have no AnkQL literal syntax".to_string()));
            }
            buffer.push('(');
            for (i, expr) in exprs.iter().enumerate() {
                if i > 0 {
                    buffer.push_str(", ");
                }
                match expr {
                    Expr::Placeholder | Expr::Literal(_) => generate_expr_sql(expr, placeholder_count, found_placeholders, buffer)?,
                    _ => {
                        return Err(SqlGenerationError::InvalidExpression(
                            "Only literal expressions and placeholders are supported in IN lists".to_string(),
                        ))
                    }
                }
            }
            buffer.push(')');
        }
        _ => return Err(SqlGenerationError::InvalidExpression("Only literal, identifier, and list expressions are supported".to_string())),
    }
    Ok(())
}

fn comparison_op_to_sql(op: &ComparisonOperator) -> Result<&'static str, SqlGenerationError> {
    Ok(match op {
        ComparisonOperator::Equal => "=",
        ComparisonOperator::NotEqual => "<>",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
        ComparisonOperator::In => "IN",
        ComparisonOperator::Between => return Err(SqlGenerationError::UnsupportedOperator("BETWEEN operator is not yet supported")),
    })
}

/// Render AnkQL source; resolved paths use source labels rather than durable ids.
pub fn generate_selection_sql<S: Stage>(
    predicate: &Predicate<S>,
    expected_placeholders: Option<usize>,
) -> Result<String, SqlGenerationError> {
    let mut placeholder_count = expected_placeholders;
    let mut found_placeholders = 0;
    let mut buffer = String::new();
    generate_selection_sql_inner(predicate, &mut placeholder_count, &mut found_placeholders, &mut buffer)?;

    // Check if we have the expected number of placeholders
    if let Some(expected) = expected_placeholders {
        if found_placeholders != expected {
            return Err(SqlGenerationError::PlaceholderCountMismatch { expected, found: found_placeholders });
        }
    }

    Ok(buffer)
}

fn generate_selection_sql_inner<S: Stage>(
    predicate: &Predicate<S>,
    placeholder_count: &mut Option<usize>,
    found_placeholders: &mut usize,
    buffer: &mut String,
) -> Result<(), SqlGenerationError> {
    match predicate {
        Predicate::Comparison { left, operator, right } => {
            generate_expr_sql(left, placeholder_count, found_placeholders, buffer)?;
            buffer.push(' ');
            buffer.push_str(comparison_op_to_sql(operator)?);
            buffer.push(' ');
            generate_expr_sql(right, placeholder_count, found_placeholders, buffer)?;
        }
        Predicate::And(left, right) | Predicate::Or(left, right) => {
            let is_or = matches!(predicate, Predicate::Or(..));
            let group_right = matches!(right.as_ref(), Predicate::And(..));
            if is_or {
                buffer.push('(');
            }
            generate_selection_sql_inner(left, placeholder_count, found_placeholders, buffer)?;
            buffer.push_str(if is_or { " OR " } else { " AND " });
            if group_right {
                buffer.push('(');
            }
            generate_selection_sql_inner(right, placeholder_count, found_placeholders, buffer)?;
            if group_right {
                buffer.push(')');
            }
            if is_or {
                buffer.push(')');
            }
        }
        Predicate::Not(pred) => {
            buffer.push_str("(NOT (");
            generate_selection_sql_inner(pred, placeholder_count, found_placeholders, buffer)?;
            buffer.push_str("))");
        }
        Predicate::IsNull(expr) => {
            buffer.push('(');
            generate_expr_sql(expr, placeholder_count, found_placeholders, buffer)?;
            buffer.push_str(" IS NULL)");
        }
        Predicate::True => buffer.push_str("TRUE"),
        Predicate::False => buffer.push_str("FALSE"),
        // Placeholder should be transformed to a comparison before SQL generation
        Predicate::Placeholder => {
            return Err(SqlGenerationError::InvalidExpression("Placeholder must be transformed before SQL generation".to_string()))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ComparisonOperator, Expr, PathExpr, Predicate};
    use crate::error::SqlGenerationError;
    use crate::parser::parse_selection;
    use ankurah_core_types::Value;
    use anyhow::Result;

    #[test]
    fn test_simple_equality() -> Result<()> {
        let selection = parse_selection("name = 'Alice'").unwrap();
        let sql = generate_selection_sql(&selection.predicate, None)?;
        assert_eq!(sql, r#""name" = 'Alice'"#);
        Ok(())
    }

    #[test]
    fn test_and_condition() -> Result<()> {
        let selection = parse_selection("name = 'Alice' AND age = '30'").unwrap();
        let sql = generate_selection_sql(&selection.predicate, None)?;
        assert_eq!(sql, r#""name" = 'Alice' AND "age" = '30'"#);
        Ok(())
    }

    #[test]
    fn test_complex_condition() -> Result<()> {
        let selection = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= '30' AND age <= '40'").unwrap();
        let sql = generate_selection_sql(&selection.predicate, None)?;
        assert_eq!(sql, r#"("name" = 'Alice' OR "name" = 'Charlie') AND "age" >= '30' AND "age" <= '40'"#);
        Ok(())
    }

    #[test]
    fn test_including_collection_identifier() -> Result<()> {
        let selection = parse_selection("person.name = 'Alice'").unwrap();
        let sql = generate_selection_sql(&selection.predicate, None)?;
        assert_eq!(sql, r#""person"."name" = 'Alice'"#);
        Ok(())
    }

    #[test]
    fn test_in_operator() -> Result<()> {
        let selection = parse_selection("name IN ('Alice', 'Bob', 'Charlie')").unwrap();
        let sql = generate_selection_sql(&selection.predicate, None)?;
        assert_eq!(sql, r#""name" IN ('Alice', 'Bob', 'Charlie')"#);
        Ok(())
    }

    #[test]
    fn test_placeholder_with_none_count() -> Result<()> {
        let query = "user_id = ?";
        let selection = parse_selection(query).unwrap();
        let sql = generate_selection_sql(&selection.predicate, None)?;
        assert_eq!(sql, r#""user_id" = ?"#);
        Ok(())
    }

    #[test]
    fn test_placeholder_with_exact_count() -> Result<()> {
        let query = "user_id = ? AND status = ?";
        let selection = parse_selection(query).unwrap();
        let sql = generate_selection_sql(&selection.predicate, Some(2))?;
        assert_eq!(sql, r#""user_id" = ? AND "status" = ?"#);
        Ok(())
    }

    #[test]
    fn test_placeholder_count_mismatch_too_few() -> Result<()> {
        let selection = parse_selection("user_id = ? AND status = ?")?;
        match generate_selection_sql(&selection.predicate, Some(1)) {
            Err(SqlGenerationError::PlaceholderCountMismatch { expected, found }) => {
                assert_eq!(expected, 1);
                assert_eq!(found, 2);
            }
            _ => panic!("Expected PlaceholderCountMismatch error"),
        }
        Ok(())
    }

    #[test]
    fn test_placeholder_count_mismatch_too_many() -> Result<()> {
        let selection = parse_selection("user_id = ?")?;
        match generate_selection_sql(&selection.predicate, Some(2)) {
            Err(SqlGenerationError::PlaceholderCountMismatch { expected, found }) => {
                assert_eq!(expected, 2);
                assert_eq!(found, 1);
            }
            _ => panic!("Expected PlaceholderCountMismatch error"),
        }
        Ok(())
    }

    #[test]
    fn test_placeholder_in_lists() -> Result<()> {
        let query = "status IN (?, ?, ?)";
        let selection = parse_selection(query).unwrap();
        let sql = generate_selection_sql(&selection.predicate, Some(3))?;
        assert_eq!(sql, r#""status" IN (?, ?, ?)"#);
        Ok(())
    }

    #[test]
    fn test_placeholder_with_zero_count() -> Result<()> {
        let query = "user_id = 123";
        let selection = parse_selection(query).unwrap();
        let sql = generate_selection_sql(&selection.predicate, Some(0))?;
        assert_eq!(sql, r#""user_id" = 123"#);
        Ok(())
    }

    #[test]
    fn test_string_escaping() -> Result<()> {
        // Create a predicate with a string containing single quotes directly
        let predicate: Predicate<crate::ast::Parsed> = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("name"))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::String("O'Brien".to_string()))),
        };
        let sql = generate_selection_sql(&predicate, None)?;
        assert_eq!(sql, r#""name" = 'O''Brien'"#);
        assert_eq!(parse_selection(&sql)?.predicate, predicate);
        Ok(())
    }

    #[test]
    fn test_null_byte_handling() -> Result<()> {
        let predicate: Predicate<crate::ast::Parsed> = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("data"))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::String("test\0data".to_string()))),
        };
        let sql = generate_selection_sql(&predicate, None)?;
        assert_eq!(sql, "\"data\" = 'test\0data'");
        assert_eq!(parse_selection(&sql)?.predicate, predicate);
        Ok(())
    }

    #[test]
    fn parsed_selections_round_trip() -> Result<()> {
        for source in [
            r#""true" = true ORDER BY "null" DESC, "first.name" ASC LIMIT 2"#,
            r#""first name"."a.b" = 'O''Brien' ORDER BY "say ""hi""""#,
            r#""  spaced  " = '' OR "from" = ''''"#,
            "name IN ('O''Brien', 'Alice', '')",
            "name IN ('O''Brien') AND id IN (?)",
            "name = ? AND status IN (?, ?)",
            "(NOT (a = 1)) OR b = 2",
            "a = 1 AND (b = 2 AND c = 3)",
            "(a IS NULL) AND (b IS NOT NULL)",
            "TRUE AND (FALSE OR TRUE)",
            "n IN (-1, +2, 1.0, -0.0, 1e20, -2.5e-10)",
        ] {
            let parsed = parse_selection(source)?;
            let rendered = parsed.to_string();
            assert_eq!(parse_selection(&rendered)?, parsed, "source: {source}; rendered: {rendered}");
        }
        Ok(())
    }

    #[test]
    fn boolean_groupings_round_trip() -> Result<()> {
        let atoms = ["a = 1", "TRUE", "(a IS NULL)", "(NOT (a = 1))"].map(|source| parse_selection(source).unwrap().predicate);
        let mut branches = atoms.to_vec();
        for left in &atoms {
            for right in &atoms {
                branches.push(Predicate::And(Box::new(left.clone()), Box::new(right.clone())));
                branches.push(Predicate::Or(Box::new(left.clone()), Box::new(right.clone())));
            }
        }
        for left in &branches {
            for right in &branches {
                for predicate in [
                    Predicate::And(Box::new(left.clone()), Box::new(right.clone())),
                    Predicate::Or(Box::new(left.clone()), Box::new(right.clone())),
                ] {
                    let rendered = generate_selection_sql(&predicate, None)?;
                    assert_eq!(parse_selection(&rendered)?.predicate, predicate, "rendered: {rendered}");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn quoted_names_are_decoded() -> Result<()> {
        let parsed = parse_selection(r#""true"."a.b" = 'O''Brien' ORDER BY "say ""hi""""#)?;
        assert_eq!(
            parsed.predicate,
            Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr { steps: vec!["true".into(), "a.b".into()] })),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Value::String("O'Brien".into()))),
            }
        );
        assert_eq!(parsed.order_by.unwrap()[0].path, PathExpr::simple("say \"hi\""));
        Ok(())
    }

    #[test]
    fn resolved_selection_renders_source_names() -> Result<()> {
        use crate::ast::{OrderByItem, OrderDirection, PropertyPath, Resolved, Selection};
        use ankurah_core_types::EntityId;
        let path = PropertyPath::registered(EntityId::from_bytes([7; 32]), "true", vec!["a.b".into()]);
        let selection: Selection<Resolved> = Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Path(path.clone())),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Value::String("O'Brien".into()))),
            },
            order_by: Some(vec![OrderByItem {
                path: PropertyPath::registered(EntityId::from_bytes([8; 32]), "sort key", vec![]),
                direction: OrderDirection::Desc,
            }]),
            limit: Some(3),
        };
        let reparsed = parse_selection(&selection.to_string())?;
        assert_eq!(reparsed, parse_selection(r#""true"."a.b" = 'O''Brien' ORDER BY "sort key" DESC LIMIT 3"#)?);
        assert_eq!(path.to_string(), "true.a.b", "standalone path display remains unquoted");
        Ok(())
    }

    #[test]
    fn unsupported_literals_fail_without_lossy_rendering() {
        for value in [Value::Object(vec![0, 255]), Value::Binary(vec![0, 255]), Value::F64(f64::NAN), Value::F64(f64::INFINITY)] {
            let predicate: Predicate<crate::ast::Parsed> = Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("value"))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(value)),
            };
            assert!(matches!(generate_selection_sql(&predicate, None), Err(SqlGenerationError::InvalidExpression(_))));
        }
        let predicate: Predicate<crate::ast::Parsed> = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("value"))),
            operator: ComparisonOperator::In,
            right: Box::new(Expr::ExprList(vec![])),
        };
        assert!(matches!(generate_selection_sql(&predicate, None), Err(SqlGenerationError::InvalidExpression(_))));
    }

    #[test]
    fn test_placeholder_with_zero_count_but_has_placeholder() -> Result<()> {
        let selection = parse_selection("user_id = ?")?;
        match generate_selection_sql(&selection.predicate, Some(0)) {
            Err(SqlGenerationError::PlaceholderCountMismatch { expected, found }) => {
                assert_eq!(expected, 0);
                assert_eq!(found, 1);
            }
            _ => panic!("Expected PlaceholderCountMismatch error"),
        }
        Ok(())
    }
}
