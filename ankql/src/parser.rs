use crate::ast;
use crate::error::ParseError;
use crate::grammar;
use pest::iterators::{Pair, Pairs};
use pest::Parser;

/// Print a parse tree node and its children recursively
#[cfg(test)]
fn print_tree(pair: Pair<grammar::Rule>, indent: usize) {
    if matches!(pair.as_rule(), grammar::Rule::EOI) {
        return;
    }
    println!("{:indent$}{:?}: '{}'", "", pair.as_rule(), pair.as_str().trim(), indent = indent);
    for inner in pair.into_inner() {
        print_tree(inner, indent + 2);
    }
}

/// Debug print a sequence of parse tree nodes
#[cfg(test)]
fn debug_print_pairs(pairs: Pairs<grammar::Rule>) {
    println!("Parse tree:");
    for pair in pairs {
        print_tree(pair, 0);
    }
}

/// Parse a selection expression into a Selection AST.
/// The selection includes a predicate and optional ORDER BY and LIMIT clauses.
pub fn parse_selection(input: &str) -> Result<ast::Selection, ParseError> {
    // TODO: Improve grammar to handle these cases more elegantly
    if input.trim().is_empty() {
        return Ok(ast::Selection { predicate: ast::Predicate::True, order_by: None, limit: None });
    }

    let pairs = grammar::AnkqlParser::parse(grammar::Rule::Selection, input).map_err(|e| ParseError::SyntaxError(format!("{}", e)))?;

    #[cfg(test)]
    debug_print_pairs(pairs.clone());

    let mut predicate = None;
    let mut order_by = None;
    let mut limit = None;

    for pair in pairs {
        match pair.as_rule() {
            grammar::Rule::Expr => {
                predicate = Some(parse_expr(pair)?);
            }
            grammar::Rule::OrderByClause => {
                order_by = Some(parse_order_by_clause(pair)?);
            }
            grammar::Rule::LimitClause => {
                limit = Some(parse_limit_clause(pair)?);
            }
            grammar::Rule::EOI => {} // End of input, ignore
            _ => {
                return Err(ParseError::UnexpectedRule { expected: "Expr, OrderByClause, or LimitClause", got: pair.as_rule() });
            }
        }
    }

    let predicate = predicate.ok_or(ParseError::EmptyExpression)?;

    Ok(ast::Selection { predicate, order_by, limit })
}

/// Parse a boolean expression, which can be a comparison, AND, or OR expression
fn parse_expr(pair: Pair<grammar::Rule>) -> Result<ast::Predicate, ParseError> {
    assert_eq!(pair.as_rule(), grammar::Rule::Expr, "Expected Expr rule");
    let mut pairs = pair.into_inner();

    // Parse the first value
    let first = pairs.next().ok_or(ParseError::MissingOperand("first"))?;

    // handle unary operators which have precedence over infix operators
    if first.as_rule() == grammar::Rule::UnaryNot {
        let next: Pair<'_, grammar::Rule> = pairs.next().ok_or(ParseError::EmptyExpression)?;

        return Ok(ast::Predicate::Not(Box::new(match next.as_rule() {
            grammar::Rule::ExpressionInParentheses => {
                //
                parse_expr(next.into_inner().next().ok_or(ParseError::EmptyExpression)?)?
            }
            _ => {
                // TODO
                return Err(ParseError::UnexpectedRule { expected: "ExpressionInParentheses", got: next.as_rule() });
            }
        })));
    }

    let mut result = parse_atomic_expr(first)?;

    // Handle postfix and infix operators
    while let Some(op) = pairs.next() {
        match op.as_rule() {
            grammar::Rule::IsNullPostfix => {
                // Check if this is "IS NULL" or "IS NOT NULL" by examining the text
                let is_not = op.as_str().to_uppercase().contains("NOT");

                let is_null = ast::Expr::Predicate(ast::Predicate::IsNull(Box::new(result)));
                result = if is_not { ast::Expr::Predicate(ast::Predicate::Not(Box::new(is_null.try_into()?))) } else { is_null };
            }
            _ => {
                // infix operators DO have a right operand
                let right = pairs.next().ok_or(ParseError::MissingOperand("right"))?;
                result = match op.as_rule() {
                    grammar::Rule::Eq
                    | grammar::Rule::GtEq
                    | grammar::Rule::Gt
                    | grammar::Rule::LtEq
                    | grammar::Rule::Lt
                    | grammar::Rule::NotEq
                    | grammar::Rule::In => create_comparison(result, op.as_rule(), right)?,
                    grammar::Rule::And | grammar::Rule::Or => create_logical_op(op.as_rule(), result, right, &mut pairs)?,
                    _ => {
                        return Err(ParseError::UnexpectedRule { expected: "comparison operator, And, or Or", got: op.as_rule() });
                    }
                }
            }
        };
    }

    result.try_into()
}

/// Create a comparison predicate from a left expression and a right pair
fn create_comparison(left: ast::Expr, op: grammar::Rule, right: Pair<grammar::Rule>) -> Result<ast::Expr, ParseError> {
    let right_expr = parse_atomic_expr(right)?;
    let operator = match op {
        grammar::Rule::Eq => ast::ComparisonOperator::Equal,
        grammar::Rule::GtEq => ast::ComparisonOperator::GreaterThanOrEqual,
        grammar::Rule::Gt => ast::ComparisonOperator::GreaterThan,
        grammar::Rule::LtEq => ast::ComparisonOperator::LessThanOrEqual,
        grammar::Rule::Lt => ast::ComparisonOperator::LessThan,
        grammar::Rule::NotEq => ast::ComparisonOperator::NotEqual,
        grammar::Rule::In => ast::ComparisonOperator::In,
        _ => {
            return Err(ParseError::UnexpectedRule { expected: "comparison operator", got: op });
        }
    };
    Ok(ast::Expr::Predicate(ast::Predicate::Comparison { left: Box::new(left), operator, right: Box::new(right_expr) }))
}

/// Create a logical operation (AND/OR) from a left expression and a right pair
fn create_logical_op(
    op: grammar::Rule,
    left: ast::Expr,
    right: Pair<grammar::Rule>,
    rest: &mut Pairs<grammar::Rule>,
) -> Result<ast::Expr, ParseError> {
    let left_pred = left.try_into()?;

    // Parse the right side, which might be part of a comparison
    let right_expr = parse_atomic_expr(right)?;
    let right_pred = if let Some(next_op) = rest.next() {
        match next_op.as_rule() {
            grammar::Rule::Eq
            | grammar::Rule::GtEq
            | grammar::Rule::Gt
            | grammar::Rule::LtEq
            | grammar::Rule::Lt
            | grammar::Rule::NotEq
            | grammar::Rule::In => {
                let next_right = rest.next().ok_or(ParseError::MissingOperand("comparison right"))?;
                let next_right_expr = parse_atomic_expr(next_right)?;
                ast::Predicate::Comparison {
                    left: Box::new(right_expr),
                    operator: match next_op.as_rule() {
                        grammar::Rule::Eq => ast::ComparisonOperator::Equal,
                        grammar::Rule::GtEq => ast::ComparisonOperator::GreaterThanOrEqual,
                        grammar::Rule::Gt => ast::ComparisonOperator::GreaterThan,
                        grammar::Rule::LtEq => ast::ComparisonOperator::LessThanOrEqual,
                        grammar::Rule::Lt => ast::ComparisonOperator::LessThan,
                        grammar::Rule::NotEq => ast::ComparisonOperator::NotEqual,
                        grammar::Rule::In => ast::ComparisonOperator::In,
                        _ => unreachable!(),
                    },
                    right: Box::new(next_right_expr),
                }
            }
            _ => {
                return Err(ParseError::UnexpectedRule { expected: "comparison operator", got: next_op.as_rule() });
            }
        }
    } else {
        right_expr.try_into()?
    };

    Ok(ast::Expr::Predicate(match op {
        grammar::Rule::And => ast::Predicate::And(Box::new(left_pred), Box::new(right_pred)),
        grammar::Rule::Or => ast::Predicate::Or(Box::new(left_pred), Box::new(right_pred)),
        _ => unreachable!(),
    }))
}

/// Parse an atomic expression, which can be an identifier, literal, or parenthesized expression
fn parse_atomic_expr(pair: Pair<grammar::Rule>) -> Result<ast::Expr, ParseError> {
    match pair.as_rule() {
        grammar::Rule::IdentifierWithOptionalContinuation => parse_identifier(pair),
        grammar::Rule::SingleQuotedString => parse_string_literal(pair),
        grammar::Rule::True => Ok(ast::Expr::Literal(ast::Literal::Boolean(true))),
        grammar::Rule::False => Ok(ast::Expr::Literal(ast::Literal::Boolean(false))),
        grammar::Rule::Unsigned => parse_number(pair),
        grammar::Rule::QuestionParameter => Ok(ast::Expr::Placeholder),
        grammar::Rule::ExpressionInParentheses => {
            let inner = pair.into_inner().next().ok_or(ParseError::EmptyExpression)?;
            let pred = parse_expr(inner)?;
            Ok(ast::Expr::Predicate(pred))
        }
        grammar::Rule::Row => {
            let mut exprs = Vec::new();
            for expr_pair in pair.into_inner() {
                if expr_pair.as_rule() == grammar::Rule::Expr {
                    let expr = parse_atomic_expr(expr_pair.into_inner().next().ok_or(ParseError::EmptyExpression)?)?;
                    exprs.push(expr);
                } else {
                    exprs.push(parse_atomic_expr(expr_pair)?);
                }
            }
            Ok(ast::Expr::ExprList(exprs))
        }
        _ => Err(ParseError::UnexpectedRule { expected: "atomic expression", got: pair.as_rule() }),
    }
}

/// Parse an identifier, which can be a simple name or a dotted path
fn parse_identifier(pair: Pair<grammar::Rule>) -> Result<ast::Expr, ParseError> {
    if pair.as_rule() != grammar::Rule::IdentifierWithOptionalContinuation {
        return Err(ParseError::UnexpectedRule { expected: "IdentifierWithOptionalContinuation", got: pair.as_rule() });
    }

    let mut ident_parts = pair.into_inner();
    let ident = ident_parts.next().ok_or(ParseError::InvalidPredicate("Empty identifier parts".into()))?;

    if ident.as_rule() != grammar::Rule::Identifier {
        return Err(ParseError::UnexpectedRule { expected: "Identifier", got: ident.as_rule() });
    }

    let collection = ident.as_str().trim().to_string();

    // Check if we have a ReferenceContinuation
    if let Some(ref_cont) = ident_parts.next() {
        if ref_cont.as_rule() != grammar::Rule::ReferenceContinuation {
            return Err(ParseError::UnexpectedRule { expected: "ReferenceContinuation", got: ref_cont.as_rule() });
        }

        // Get the property name from the ReferenceContinuation
        let property = ref_cont.into_inner().next().ok_or(ParseError::InvalidPredicate("Empty reference continuation".into()))?;

        if property.as_rule() != grammar::Rule::Identifier {
            return Err(ParseError::UnexpectedRule { expected: "Identifier", got: property.as_rule() });
        }

        Ok(ast::Expr::Identifier(ast::Identifier::CollectionProperty(collection, property.as_str().trim().to_string())))
    } else {
        Ok(ast::Expr::Identifier(ast::Identifier::Property(collection)))
    }
}

/// Parse a string literal, removing the surrounding quotes
fn parse_string_literal(pair: Pair<grammar::Rule>) -> Result<ast::Expr, ParseError> {
    if pair.as_rule() != grammar::Rule::SingleQuotedString {
        return Err(ParseError::UnexpectedRule { expected: "SingleQuotedString", got: pair.as_rule() });
    }

    let s = pair.as_str();
    if !s.starts_with('\'') || !s.ends_with('\'') {
        return Err(ParseError::InvalidPredicate("String literal must be quoted".into()));
    }
    let s = &s[1..s.len() - 1];

    Ok(ast::Expr::Literal(ast::Literal::String(s.to_string())))
}

/// Parse a number literal
fn parse_number(pair: Pair<grammar::Rule>) -> Result<ast::Expr, ParseError> {
    if pair.as_rule() != grammar::Rule::Unsigned {
        return Err(ParseError::UnexpectedRule { expected: "Unsigned", got: pair.as_rule() });
    }

    let num = pair.as_str().trim().parse::<i64>().map_err(|e| ParseError::InvalidPredicate(format!("Failed to parse number: {}", e)))?;

    Ok(ast::Expr::Literal(ast::Literal::Integer(num)))
}

/// Parse a LIMIT clause
fn parse_limit_clause(pair: Pair<grammar::Rule>) -> Result<u64, ParseError> {
    if pair.as_rule() != grammar::Rule::LimitClause {
        return Err(ParseError::UnexpectedRule { expected: "LimitClause", got: pair.as_rule() });
    }

    // Since LimitClause is compound atomic ($), we can access the inner Unsigned token directly
    let unsigned_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == grammar::Rule::Unsigned)
        .ok_or(ParseError::InvalidPredicate("Missing limit value".into()))?;

    let limit =
        unsigned_pair.as_str().trim().parse::<u64>().map_err(|e| ParseError::InvalidPredicate(format!("Failed to parse limit: {}", e)))?;

    Ok(limit)
}

fn parse_order_by_clause(pair: Pair<grammar::Rule>) -> Result<Vec<ast::OrderByItem>, ParseError> {
    if pair.as_rule() != grammar::Rule::OrderByClause {
        return Err(ParseError::UnexpectedRule { expected: "OrderByClause", got: pair.as_rule() });
    }

    // Since OrderByClause is compound atomic ($), we can access the inner tokens directly
    let inner_pairs: Vec<_> = pair.into_inner().collect();

    let identifier_pair = inner_pairs
        .iter()
        .find(|p| p.as_rule() == grammar::Rule::Identifier)
        .ok_or(ParseError::InvalidPredicate("Missing column name in ORDER BY".into()))?;

    let identifier_str = identifier_pair.as_str().trim();

    // Only simple identifiers are supported in ORDER BY (no dotted identifiers)
    if identifier_str.contains('.') {
        return Err(ParseError::InvalidPredicate("Dotted identifiers are not supported in ORDER BY clauses".into()));
    }

    let identifier = ast::Identifier::Property(identifier_str.to_string());

    let direction = inner_pairs
        .iter()
        .find(|p| p.as_rule() == grammar::Rule::OrderDirection)
        .map(|p| match p.as_str().trim().to_uppercase().as_str() {
            "ASC" => Ok(ast::OrderDirection::Asc),
            "DESC" => Ok(ast::OrderDirection::Desc),
            _ => Err(ParseError::InvalidPredicate(format!("Invalid order direction: {}", p.as_str()))),
        })
        .transpose()?
        .unwrap_or(ast::OrderDirection::Asc); // Default

    Ok(vec![ast::OrderByItem { identifier, direction }])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_selection_status_active() {
        let input = r#"status = 'active'"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection,
            ast::Selection {
                predicate: ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Literal(ast::Literal::String("active".to_string())))
                },
                order_by: None,
                limit: None,
            }
        );
    }

    #[test]
    fn test_parse_selection_user_and_status() {
        let input = r#"user = 123 AND status = 'active'"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection,
            ast::Selection {
                predicate: ast::Predicate::And(
                    Box::new(ast::Predicate::Comparison {
                        left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("user".to_string()))),
                        operator: ast::ComparisonOperator::Equal,
                        right: Box::new(ast::Expr::Literal(ast::Literal::Integer(123)))
                    }),
                    Box::new(ast::Predicate::Comparison {
                        left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                        operator: ast::ComparisonOperator::Equal,
                        right: Box::new(ast::Expr::Literal(ast::Literal::String("active".to_string())))
                    })
                ),
                order_by: None,
                limit: None,
            }
        );
    }

    #[test]
    fn test_parse_selection_user_or_and_status() {
        let input = r#"(user = 123 OR user = 456) AND status = 'active'"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::And(
                Box::new(ast::Predicate::Or(
                    Box::new(ast::Predicate::Comparison {
                        left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("user".to_string()))),
                        operator: ast::ComparisonOperator::Equal,
                        right: Box::new(ast::Expr::Literal(ast::Literal::Integer(123)))
                    }),
                    Box::new(ast::Predicate::Comparison {
                        left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("user".to_string()))),
                        operator: ast::ComparisonOperator::Equal,
                        right: Box::new(ast::Expr::Literal(ast::Literal::Integer(456)))
                    })
                )),
                Box::new(ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Literal(ast::Literal::String("active".to_string())))
                })
            )
        );
    }

    #[test]
    fn test_parse_selection_status_is_null() {
        let input = r#"status IS NULL"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::IsNull(Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))))
        );
    }

    #[test]
    fn test_parse_selection_status_is_not_null() {
        let input = r#"status IS NOT NULL"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::Not(Box::new(ast::Predicate::IsNull(Box::new(ast::Expr::Identifier(ast::Identifier::Property(
                "status".to_string()
            ))))))
        );
    }

    #[test]
    fn unary_not_parenthesized() {
        let input = r#"NOT (status = 'active')"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::Not(Box::new(ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                operator: ast::ComparisonOperator::Equal,
                right: Box::new(ast::Expr::Literal(ast::Literal::String("active".to_string())))
            }))
        );
    }

    #[test]
    fn unary_not_unparenthesized() {
        // currently we don't support this - mostly because I'm not totally sure of the precedence rules, or how to parse it. lol
        let input = r#"NOT status = 'active'"#;
        matches!(
            parse_selection(input),
            Err(ParseError::UnexpectedRule { expected: "ExpressionInParentheses", got: grammar::Rule::ExpressionInParentheses })
        );
    }

    #[test]
    fn test_parse_empty_string() {
        let input = "";
        let selection = parse_selection(input).unwrap();
        assert_eq!(selection.predicate, ast::Predicate::True);
    }

    #[test]
    fn test_parse_true_literal() {
        let input = "true";
        let selection = parse_selection(input).unwrap();
        assert_eq!(selection.predicate, ast::Predicate::True);
    }

    #[test]
    fn test_parse_selection_in_clause() {
        let input = r#"status IN ('active', 'pending')"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                operator: ast::ComparisonOperator::In,
                right: Box::new(ast::Expr::ExprList(vec![
                    ast::Expr::Literal(ast::Literal::String("active".to_string())),
                    ast::Expr::Literal(ast::Literal::String("pending".to_string())),
                ]))
            }
        );
    }

    #[test]
    fn test_parse_selection_in_clause_numbers() {
        let input = r#"user_id IN (1, 2, 3)"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("user_id".to_string()))),
                operator: ast::ComparisonOperator::In,
                right: Box::new(ast::Expr::ExprList(vec![
                    ast::Expr::Literal(ast::Literal::Integer(1)),
                    ast::Expr::Literal(ast::Literal::Integer(2)),
                    ast::Expr::Literal(ast::Literal::Integer(3)),
                ]))
            }
        );
    }

    #[test]
    fn test_comparison_to_true() {
        let input = r#"bool_field = true"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("bool_field".to_string()))),
                operator: ast::ComparisonOperator::Equal,
                right: Box::new(ast::Expr::Literal(ast::Literal::Boolean(true)))
            }
        );
    }

    #[test]
    fn test_comparison_to_false() {
        let input = r#"bool_field <> false"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("bool_field".to_string()))),
                operator: ast::ComparisonOperator::NotEqual,
                right: Box::new(ast::Expr::Literal(ast::Literal::Boolean(false)))
            }
        );
    }

    #[test]
    fn test_comparison_to_left_operand_boolean() {
        let input = r#"false <> bool_field"#;
        let selection = parse_selection(input).unwrap();
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Literal(ast::Literal::Boolean(false))),
                operator: ast::ComparisonOperator::NotEqual,
                right: Box::new(ast::Expr::Identifier(ast::Identifier::Property("bool_field".to_string())))
            }
        );
    }

    #[test]
    fn test_placeholders() -> anyhow::Result<()> {
        // Single literal placeholder in comparison
        assert_eq!(
            parse_selection("user_id = ?")?.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("user_id".to_string()))),
                operator: ast::ComparisonOperator::Equal,
                right: Box::new(ast::Expr::Placeholder)
            }
        );

        // Multiple literal placeholders in AND expression
        assert_eq!(
            parse_selection("user_id = ? AND status = ?")?.predicate,
            ast::Predicate::And(
                Box::new(ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("user_id".to_string()))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Placeholder)
                }),
                Box::new(ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Placeholder)
                })
            )
        );

        // Literal placeholders in IN clause
        assert_eq!(
            parse_selection("status IN (?, ?, ?)")?.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                operator: ast::ComparisonOperator::In,
                right: Box::new(ast::Expr::ExprList(vec![ast::Expr::Placeholder, ast::Expr::Placeholder, ast::Expr::Placeholder,]))
            }
        );

        // predicate placeholders connected by AND
        assert_eq!(
            parse_selection("? AND ?")?.predicate,
            ast::Predicate::And(Box::new(ast::Predicate::Placeholder), Box::new(ast::Predicate::Placeholder))
        );

        // predicate placeholders connected by OR
        assert_eq!(
            parse_selection("? OR ?")?.predicate,
            ast::Predicate::Or(Box::new(ast::Predicate::Placeholder), Box::new(ast::Predicate::Placeholder))
        );

        // Test a single predicate placeholder
        assert_eq!(parse_selection("?")?.predicate, ast::Predicate::Placeholder);

        // test a mix of predicate and literal placeholders
        assert_eq!(
            parse_selection("? AND foo = ?")?.predicate,
            ast::Predicate::And(
                Box::new(ast::Predicate::Placeholder),
                Box::new(ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("foo".to_string()))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Placeholder),
                })
            )
        );

        Ok(())
    }

    #[test]
    fn test_boolean_literals() -> anyhow::Result<()> {
        // Test that "true" parses correctly through the TryFrom path
        assert_eq!(parse_selection("true")?.predicate, ast::Predicate::True);

        // Test that "false" parses correctly through the TryFrom path
        assert_eq!(parse_selection("false")?.predicate, ast::Predicate::False);

        Ok(())
    }

    #[test]
    fn test_order_by_basic() -> anyhow::Result<()> {
        let selection = parse_selection("status = 'active' ORDER BY name")?;
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                operator: ast::ComparisonOperator::Equal,
                right: Box::new(ast::Expr::Literal(ast::Literal::String("active".to_string())))
            }
        );
        assert_eq!(
            selection.order_by,
            Some(vec![ast::OrderByItem { identifier: ast::Identifier::Property("name".to_string()), direction: ast::OrderDirection::Asc }])
        );
        assert_eq!(selection.limit, None);
        Ok(())
    }

    #[test]
    fn test_order_by_with_direction() -> anyhow::Result<()> {
        let selection = parse_selection("true ORDER BY created_at DESC")?;
        assert_eq!(selection.predicate, ast::Predicate::True);
        assert_eq!(
            selection.order_by,
            Some(vec![ast::OrderByItem {
                identifier: ast::Identifier::Property("created_at".to_string()),
                direction: ast::OrderDirection::Desc
            }])
        );
        Ok(())
    }

    #[test]
    fn test_order_by_dotted_identifier_not_supported() -> anyhow::Result<()> {
        // Dotted identifiers are intentionally not supported in ORDER BY
        let result = parse_selection("true ORDER BY user.name ASC");
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_limit_basic() -> anyhow::Result<()> {
        let selection = parse_selection("status = 'active' LIMIT 10")?;
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("status".to_string()))),
                operator: ast::ComparisonOperator::Equal,
                right: Box::new(ast::Expr::Literal(ast::Literal::String("active".to_string())))
            }
        );
        assert_eq!(selection.order_by, None);
        assert_eq!(selection.limit, Some(10));
        Ok(())
    }

    #[test]
    fn test_order_by_and_limit() -> anyhow::Result<()> {
        let selection = parse_selection("user_id > 100 ORDER BY created_at DESC LIMIT 5")?;
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("user_id".to_string()))),
                operator: ast::ComparisonOperator::GreaterThan,
                right: Box::new(ast::Expr::Literal(ast::Literal::Integer(100)))
            }
        );
        assert_eq!(
            selection.order_by,
            Some(vec![ast::OrderByItem {
                identifier: ast::Identifier::Property("created_at".to_string()),
                direction: ast::OrderDirection::Desc
            }])
        );
        assert_eq!(selection.limit, Some(5));
        Ok(())
    }

    #[test]
    fn test_limit_only() -> anyhow::Result<()> {
        let selection = parse_selection("true LIMIT 100")?;
        assert_eq!(selection.predicate, ast::Predicate::True);
        assert_eq!(selection.order_by, None);
        assert_eq!(selection.limit, Some(100));
        Ok(())
    }

    #[test]
    fn test_order_by_only() -> anyhow::Result<()> {
        let selection = parse_selection("true ORDER BY score")?;
        assert_eq!(selection.predicate, ast::Predicate::True);
        assert_eq!(
            selection.order_by,
            Some(vec![ast::OrderByItem {
                identifier: ast::Identifier::Property("score".to_string()),
                direction: ast::OrderDirection::Asc
            }])
        );
        assert_eq!(selection.limit, None);
        Ok(())
    }

    #[test]
    fn test_pathological_keyword_cases() -> anyhow::Result<()> {
        // Test that keywords can be used as identifiers in appropriate contexts
        let selection = parse_selection("limit = 1")?;
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("limit".to_string()))),
                operator: ast::ComparisonOperator::Equal,
                right: Box::new(ast::Expr::Literal(ast::Literal::Integer(1)))
            }
        );

        let selection = parse_selection("order = 2 ORDER BY name")?;
        assert_eq!(
            selection.predicate,
            ast::Predicate::Comparison {
                left: Box::new(ast::Expr::Identifier(ast::Identifier::Property("order".to_string()))),
                operator: ast::ComparisonOperator::Equal,
                right: Box::new(ast::Expr::Literal(ast::Literal::Integer(2)))
            }
        );
        assert_eq!(
            selection.order_by,
            Some(vec![ast::OrderByItem { identifier: ast::Identifier::Property("name".to_string()), direction: ast::OrderDirection::Asc }])
        );

        Ok(())
    }

    #[test]
    fn test_raw_parsing() {
        println!("=== Testing Raw Grammar Parsing ===\n");

        let test_cases = vec![
            "true",
            "true or false",
            "true and false",
            "true LIMIT 10",
            "status = 'active'",
            "status = 'active' LIMIT 5",
            "limit = 1",          // pathological case - limit as identifier
            "limit = 1 LIMIT 10", // pathological case
            "foo = 1 order by name",
            "true ORDER BY name ASC",
            "true ORDER BY name DESC",
            "true ORDER BY name LIMIT 10",
            "order = 1",               // pathological case - order as identifier
            "by = 2",                  // pathological case - by as identifier
            "order = 1 ORDER BY name", // pathological case
        ];

        for (i, input) in test_cases.iter().enumerate() {
            println!("Test {}: '{}'", i + 1, input);

            // Just try to parse with pest, don't interpret
            match grammar::AnkqlParser::parse(grammar::Rule::Selection, input) {
                Ok(pairs) => {
                    println!("✅ Grammar parsed successfully!");
                    for pair in pairs {
                        print_parse_tree(pair, 0);
                    }
                }
                Err(e) => {
                    panic!("❌ Grammar parse error: {}", e);
                }
            }
        }
    }

    // Helper function to print the parse tree
    fn print_parse_tree(pair: pest::iterators::Pair<grammar::Rule>, indent: usize) {
        let indent_str = "  ".repeat(indent);
        println!("{}Rule::{:?} -> '{}'", indent_str, pair.as_rule(), pair.as_str());

        for inner_pair in pair.into_inner() {
            print_parse_tree(inner_pair, indent + 1);
        }
    }
}
