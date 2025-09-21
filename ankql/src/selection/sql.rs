use crate::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use crate::error::SqlGenerationError;
use base64::{engine::general_purpose, Engine as _};

fn generate_expr_sql(
    expr: &Expr,
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
            Literal::I16(i) => {
                buffer.push_str(&i.to_string());
            }
            Literal::I32(i) => {
                buffer.push_str(&i.to_string());
            }
            Literal::I64(i) => {
                buffer.push_str(&i.to_string());
            }
            Literal::F64(f) => {
                buffer.push_str(&f.to_string());
            }
            Literal::Bool(b) => {
                buffer.push_str(if *b { "true" } else { "false" });
            }
            Literal::String(s) => {
                buffer.push('\'');
                // Escape problematic characters for SQL safety
                for c in s.chars() {
                    match c {
                        '\'' => buffer.push_str("''"), // Single quote -> double quote (SQL standard)
                        '\0' => {
                            // Null bytes can cause string truncation in C-based drivers
                            // Skip them entirely for safety
                            continue;
                        }
                        _ => buffer.push(c),
                    }
                }
                buffer.push('\'');
            }
            Literal::EntityId(ulid) => {
                buffer.push('\'');
                buffer.push_str(&general_purpose::URL_SAFE_NO_PAD.encode(ulid.to_bytes()));
                buffer.push('\'');
            }
            Literal::Object(bytes) | Literal::Binary(bytes) => {
                buffer.push('\'');
                buffer.push_str(&String::from_utf8_lossy(bytes));
                buffer.push('\'');
            }
        },
        Expr::Identifier(id) => match id {
            Identifier::Property(name) => {
                buffer.push('"');
                buffer.push_str(name);
                buffer.push('"');
            }
            Identifier::CollectionProperty(collection, name) => {
                buffer.push('"');
                buffer.push_str(collection);
                buffer.push('"');
                buffer.push('.');
                buffer.push('"');
                buffer.push_str(name);
                buffer.push('"');
            }
        },
        Expr::ExprList(exprs) => {
            buffer.push('(');
            for (i, expr) in exprs.iter().enumerate() {
                if i > 0 {
                    buffer.push_str(", ");
                }
                match expr {
                    Expr::Placeholder => {
                        *found_placeholders += 1;

                        // Check if we're exceeding the expected count
                        if let Some(expected) = placeholder_count {
                            if *found_placeholders > *expected {
                                return Err(SqlGenerationError::PlaceholderCountMismatch {
                                    expected: *expected,
                                    found: *found_placeholders,
                                });
                            }
                        }

                        buffer.push('?');
                    }
                    Expr::Literal(lit) => match lit {
                        Literal::I16(i) => {
                            buffer.push_str(&i.to_string());
                        }
                        Literal::I32(i) => {
                            buffer.push_str(&i.to_string());
                        }
                        Literal::I64(i) => {
                            buffer.push_str(&i.to_string());
                        }
                        Literal::F64(f) => {
                            buffer.push_str(&f.to_string());
                        }
                        Literal::String(s) => {
                            buffer.push('\'');
                            // Escape problematic characters for SQL safety
                            for c in s.chars() {
                                match c {
                                    '\'' => buffer.push_str("''"), // Single quote -> double quote (SQL standard)
                                    '\0' => {
                                        // Null bytes can cause string truncation in C-based drivers
                                        // Skip them entirely for safety
                                        continue;
                                    }
                                    _ => buffer.push(c),
                                }
                            }
                            buffer.push('\'');
                        }
                        Literal::Bool(b) => {
                            buffer.push_str(if *b { "true" } else { "false" });
                        }
                        Literal::EntityId(ulid) => {
                            buffer.push('\'');
                            buffer.push_str(&general_purpose::URL_SAFE_NO_PAD.encode(ulid.to_bytes()));
                            buffer.push('\'');
                        }
                        Literal::Object(bytes) | Literal::Binary(bytes) => {
                            todo!("Object and Binary literals");
                            // buffer.push('\'');
                            // buffer.push_str(&String::from_utf8_lossy(bytes));
                            // buffer.push('\'');
                        }
                    },
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

pub fn generate_selection_sql(predicate: &Predicate, expected_placeholders: Option<usize>) -> Result<String, SqlGenerationError> {
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

fn generate_selection_sql_inner(
    predicate: &Predicate,
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
        Predicate::And(left, right) => {
            generate_selection_sql_inner(left, placeholder_count, found_placeholders, buffer)?;
            buffer.push_str(" AND ");
            generate_selection_sql_inner(right, placeholder_count, found_placeholders, buffer)?;
        }
        Predicate::Or(left, right) => {
            buffer.push('(');
            generate_selection_sql_inner(left, placeholder_count, found_placeholders, buffer)?;
            buffer.push_str(" OR ");
            generate_selection_sql_inner(right, placeholder_count, found_placeholders, buffer)?;
            buffer.push(')');
        }
        Predicate::Not(pred) => {
            buffer.push_str("NOT (");
            generate_selection_sql_inner(pred, placeholder_count, found_placeholders, buffer)?;
            buffer.push(')');
        }
        Predicate::IsNull(expr) => {
            generate_expr_sql(expr, placeholder_count, found_placeholders, buffer)?;
            buffer.push_str(" IS NULL");
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
    use crate::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
    use crate::error::SqlGenerationError;
    use crate::parser::parse_selection;
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
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier::Property("name".to_string()))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("O'Brien".to_string()))),
        };
        let sql = generate_selection_sql(&predicate, None)?;
        assert_eq!(sql, r#""name" = 'O''Brien'"#);
        Ok(())
    }

    #[test]
    fn test_null_byte_handling() -> Result<()> {
        // Test that null bytes are removed for safety
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier::Property("data".to_string()))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("test\0data".to_string()))),
        };
        let sql = generate_selection_sql(&predicate, None)?;
        assert_eq!(sql, r#""data" = 'testdata'"#);
        Ok(())
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
