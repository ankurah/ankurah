use std::error::Error;

use crate::ast;
use crate::grammar;
use pest::Parser;
use pest::iterators::{Pair, Pairs};

/// Custom error type for parsing errors
#[derive(Debug)]
enum ParseError {
    EmptyExpression,
    UnexpectedRule {
        expected: &'static str,
        got: grammar::Rule,
    },
    InvalidPredicate(String),
    MissingOperand(&'static str),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyExpression => write!(f, "Empty expression"),
            Self::UnexpectedRule { expected, got } => {
                write!(f, "Expected {}, got {:?}", expected, got)
            }
            Self::InvalidPredicate(msg) => write!(f, "Invalid predicate: {}", msg),
            Self::MissingOperand(side) => write!(f, "Missing {} operand", side),
        }
    }
}

impl Error for ParseError {}

/// Print a parse tree node and its children recursively
#[cfg(test)]
fn print_tree(pair: Pair<grammar::Rule>, indent: usize) {
    if matches!(pair.as_rule(), grammar::Rule::EOI) {
        return;
    }
    println!(
        "{:indent$}{:?}: '{}'",
        "",
        pair.as_rule(),
        pair.as_str().trim(),
        indent = indent
    );
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

/// Parse a selection expression into a predicate AST.
/// The selection must be a valid boolean expression using AND, OR, and comparison operators.
pub fn parse_selection(input: &str) -> Result<ast::Predicate, Box<dyn Error>> {
    let pairs = grammar::AnkqlParser::parse(grammar::Rule::Selection, input)?;

    #[cfg(test)]
    debug_print_pairs(pairs.clone());

    // Since Selection is a silent rule (_), we get the Expr directly
    let expr = pairs
        .into_iter()
        .next()
        .ok_or(ParseError::EmptyExpression)?;
    if expr.as_rule() != grammar::Rule::Expr {
        return Err(Box::new(ParseError::UnexpectedRule {
            expected: "Expr",
            got: expr.as_rule(),
        }));
    }

    parse_expr(expr)
}

/// Parse a boolean expression, which can be a comparison, AND, or OR expression
fn parse_expr(pair: Pair<grammar::Rule>) -> Result<ast::Predicate, Box<dyn Error>> {
    assert_eq!(pair.as_rule(), grammar::Rule::Expr, "Expected Expr rule");
    let mut pairs = pair.into_inner();

    // Parse the first value
    let first = pairs.next().ok_or(ParseError::MissingOperand("first"))?;
    let mut result = parse_atomic_expr(first)?;

    // Process operator-value pairs
    while let Some(op) = pairs.next() {
        let right = pairs.next().ok_or(ParseError::MissingOperand("right"))?;
        result = match op.as_rule() {
            grammar::Rule::Eq
            | grammar::Rule::GtEq
            | grammar::Rule::Gt
            | grammar::Rule::LtEq
            | grammar::Rule::Lt
            | grammar::Rule::NotEq => create_comparison(result, op.as_rule(), right)?,
            grammar::Rule::And | grammar::Rule::Or => {
                create_logical_op(op.as_rule(), result, right, &mut pairs)?
            }
            _ => {
                return Err(Box::new(ParseError::UnexpectedRule {
                    expected: "comparison operator, And, or Or",
                    got: op.as_rule(),
                }));
            }
        };
    }

    result.into_predicate()
}

/// Create a comparison predicate from a left expression and a right pair
fn create_comparison(
    left: ast::Expr,
    op: grammar::Rule,
    right: Pair<grammar::Rule>,
) -> Result<ast::Expr, Box<dyn Error>> {
    let right_expr = parse_atomic_expr(right)?;
    let operator = match op {
        grammar::Rule::Eq => ast::ComparisonOperator::Equal,
        grammar::Rule::GtEq => ast::ComparisonOperator::GreaterThanOrEqual,
        grammar::Rule::Gt => ast::ComparisonOperator::GreaterThan,
        grammar::Rule::LtEq => ast::ComparisonOperator::LessThanOrEqual,
        grammar::Rule::Lt => ast::ComparisonOperator::LessThan,
        grammar::Rule::NotEq => ast::ComparisonOperator::NotEqual,
        _ => {
            return Err(Box::new(ParseError::UnexpectedRule {
                expected: "comparison operator",
                got: op,
            }));
        }
    };
    Ok(ast::Expr::Predicate(ast::Predicate::Comparison {
        left: Box::new(left),
        operator,
        right: Box::new(right_expr),
    }))
}

/// Create a logical operation (AND/OR) from a left expression and a right pair
fn create_logical_op(
    op: grammar::Rule,
    left: ast::Expr,
    right: Pair<grammar::Rule>,
    rest: &mut Pairs<grammar::Rule>,
) -> Result<ast::Expr, Box<dyn Error>> {
    let left_pred = left.into_predicate()?;

    // Parse the right side, which might be part of a comparison
    let right_expr = parse_atomic_expr(right)?;
    let right_pred = if let Some(next_op) = rest.next() {
        match next_op.as_rule() {
            grammar::Rule::Eq
            | grammar::Rule::GtEq
            | grammar::Rule::Gt
            | grammar::Rule::LtEq
            | grammar::Rule::Lt
            | grammar::Rule::NotEq => {
                let next_right = rest
                    .next()
                    .ok_or(ParseError::MissingOperand("comparison right"))?;
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
                        _ => unreachable!(),
                    },
                    right: Box::new(next_right_expr),
                }
            }
            _ => {
                return Err(Box::new(ParseError::UnexpectedRule {
                    expected: "comparison operator",
                    got: next_op.as_rule(),
                }));
            }
        }
    } else {
        right_expr.into_predicate()?
    };

    Ok(ast::Expr::Predicate(match op {
        grammar::Rule::And => ast::Predicate::And(Box::new(left_pred), Box::new(right_pred)),
        grammar::Rule::Or => ast::Predicate::Or(Box::new(left_pred), Box::new(right_pred)),
        _ => unreachable!(),
    }))
}

/// Parse an atomic expression, which can be an identifier, literal, or parenthesized expression
fn parse_atomic_expr(pair: Pair<grammar::Rule>) -> Result<ast::Expr, Box<dyn Error>> {
    match pair.as_rule() {
        grammar::Rule::IdentifierWithOptionalContinuation => parse_identifier(pair),
        grammar::Rule::SingleQuotedString => parse_string_literal(pair),
        grammar::Rule::Unsigned => parse_number(pair),
        grammar::Rule::ExpressionInParentheses => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or(ParseError::EmptyExpression)?;
            let pred = parse_expr(inner)?;
            Ok(ast::Expr::Predicate(pred))
        }
        _ => Err(Box::new(ParseError::UnexpectedRule {
            expected: "atomic expression",
            got: pair.as_rule(),
        })),
    }
}

/// Parse an identifier, which can be a simple name or a dotted path
fn parse_identifier(pair: Pair<grammar::Rule>) -> Result<ast::Expr, Box<dyn Error>> {
    if pair.as_rule() != grammar::Rule::IdentifierWithOptionalContinuation {
        return Err(Box::new(ParseError::UnexpectedRule {
            expected: "IdentifierWithOptionalContinuation",
            got: pair.as_rule(),
        }));
    }

    let mut ident_parts = pair.into_inner();
    let ident = ident_parts.next().ok_or(ParseError::InvalidPredicate(
        "Empty identifier parts".into(),
    ))?;

    if ident.as_rule() != grammar::Rule::Identifier {
        return Err(Box::new(ParseError::UnexpectedRule {
            expected: "Identifier",
            got: ident.as_rule(),
        }));
    }

    let collection = ident.as_str().trim().to_string();

    // Check if we have a ReferenceContinuation
    if let Some(ref_cont) = ident_parts.next() {
        if ref_cont.as_rule() != grammar::Rule::ReferenceContinuation {
            return Err(Box::new(ParseError::UnexpectedRule {
                expected: "ReferenceContinuation",
                got: ref_cont.as_rule(),
            }));
        }

        // Get the property name from the ReferenceContinuation
        let property = ref_cont
            .into_inner()
            .next()
            .ok_or(ParseError::InvalidPredicate(
                "Empty reference continuation".into(),
            ))?;

        if property.as_rule() != grammar::Rule::Identifier {
            return Err(Box::new(ParseError::UnexpectedRule {
                expected: "Identifier",
                got: property.as_rule(),
            }));
        }

        Ok(ast::Expr::Identifier(ast::Identifier::CollectionProperty(
            collection,
            property.as_str().trim().to_string(),
        )))
    } else {
        Ok(ast::Expr::Identifier(ast::Identifier::Property(collection)))
    }
}

/// Parse a string literal, removing the surrounding quotes
fn parse_string_literal(pair: Pair<grammar::Rule>) -> Result<ast::Expr, Box<dyn Error>> {
    if pair.as_rule() != grammar::Rule::SingleQuotedString {
        return Err(Box::new(ParseError::UnexpectedRule {
            expected: "SingleQuotedString",
            got: pair.as_rule(),
        }));
    }

    let s = pair.as_str();
    if !s.starts_with('\'') || !s.ends_with('\'') {
        return Err(Box::new(ParseError::InvalidPredicate(
            "String literal must be quoted".into(),
        )));
    }
    let s = &s[1..s.len() - 1];

    Ok(ast::Expr::Literal(ast::Literal::String(s.to_string())))
}

/// Parse a number literal
fn parse_number(pair: Pair<grammar::Rule>) -> Result<ast::Expr, Box<dyn Error>> {
    if pair.as_rule() != grammar::Rule::Unsigned {
        return Err(Box::new(ParseError::UnexpectedRule {
            expected: "Unsigned",
            got: pair.as_rule(),
        }));
    }

    let num = pair
        .as_str()
        .trim()
        .parse::<i64>()
        .map_err(|e| ParseError::InvalidPredicate(format!("Failed to parse number: {}", e)))?;

    Ok(ast::Expr::Literal(ast::Literal::Integer(num)))
}

/// Helper trait to convert expressions to predicates
trait IntoPredicate {
    fn into_predicate(self) -> Result<ast::Predicate, Box<dyn Error>>;
}

impl IntoPredicate for ast::Expr {
    fn into_predicate(self) -> Result<ast::Predicate, Box<dyn Error>> {
        match self {
            ast::Expr::Predicate(p) => Ok(p),
            _ => Err(Box::new(ParseError::InvalidPredicate(
                "Expression is not a predicate".into(),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_selection_status_active() {
        let input = r#"status = 'active'"#;
        let predicate = parse_selection(input).unwrap();
        assert_eq!(predicate, ast::Predicate::Comparison {
            left: Box::new(ast::Expr::Identifier(ast::Identifier::Property(
                "status".to_string()
            ))),
            operator: ast::ComparisonOperator::Equal,
            right: Box::new(ast::Expr::Literal(ast::Literal::String(
                "active".to_string()
            )))
        });
    }

    #[test]
    fn test_parse_selection_user_and_status() {
        let input = r#"user = 123 AND status = 'active'"#;
        let predicate = parse_selection(input).unwrap();
        assert_eq!(
            predicate,
            ast::Predicate::And(
                Box::new(ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property(
                        "user".to_string()
                    ))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Literal(ast::Literal::Integer(123)))
                }),
                Box::new(ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property(
                        "status".to_string()
                    ))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Literal(ast::Literal::String(
                        "active".to_string()
                    )))
                })
            )
        );
    }

    #[test]
    fn test_parse_selection_user_or_and_status() {
        let input = r#"(user = 123 OR user = 456) AND status = 'active'"#;
        let predicate = parse_selection(input).unwrap();
        assert_eq!(
            predicate,
            ast::Predicate::And(
                Box::new(ast::Predicate::Or(
                    Box::new(ast::Predicate::Comparison {
                        left: Box::new(ast::Expr::Identifier(ast::Identifier::Property(
                            "user".to_string()
                        ))),
                        operator: ast::ComparisonOperator::Equal,
                        right: Box::new(ast::Expr::Literal(ast::Literal::Integer(123)))
                    }),
                    Box::new(ast::Predicate::Comparison {
                        left: Box::new(ast::Expr::Identifier(ast::Identifier::Property(
                            "user".to_string()
                        ))),
                        operator: ast::ComparisonOperator::Equal,
                        right: Box::new(ast::Expr::Literal(ast::Literal::Integer(456)))
                    })
                )),
                Box::new(ast::Predicate::Comparison {
                    left: Box::new(ast::Expr::Identifier(ast::Identifier::Property(
                        "status".to_string()
                    ))),
                    operator: ast::ComparisonOperator::Equal,
                    right: Box::new(ast::Expr::Literal(ast::Literal::String(
                        "active".to_string()
                    )))
                })
            )
        );
    }
}
