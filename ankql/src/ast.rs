use crate::selection::sql::generate_selection_sql;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Literal),
    Identifier(Identifier),
    Predicate(Predicate),
    InfixExpr { left: Box<Expr>, operator: InfixOperator, right: Box<Expr> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Identifier {
    Property(String),
    CollectionProperty(String, String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    Comparison { left: Box<Expr>, operator: ComparisonOperator, right: Box<Expr> },
    IsNull(Box<Expr>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    True,
    False,
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", generate_selection_sql(self)) }
}

impl Predicate {
    /// Clones the predicate tree and evaluates comparisons involving missing columns as if they were NULL
    pub fn assume_null(&self, columns: &[String]) -> Self {
        match self {
            Predicate::Comparison { left, operator, right } => {
                // Check if either side is an identifier that's in our null list
                let has_null_identifier = match (&**left, &**right) {
                    (Expr::Identifier(id), _) | (_, Expr::Identifier(id)) => match id {
                        Identifier::Property(name) => columns.contains(name),
                        Identifier::CollectionProperty(_, name) => columns.contains(name),
                    },
                    _ => false,
                };

                if has_null_identifier {
                    match operator {
                        // NULL = anything is false
                        ComparisonOperator::Equal => Predicate::False,
                        // NULL != anything is false (NULL comparisons always return NULL in SQL)
                        ComparisonOperator::NotEqual => Predicate::False,
                        // NULL > anything is false
                        ComparisonOperator::GreaterThan => Predicate::False,
                        // NULL >= anything is false
                        ComparisonOperator::GreaterThanOrEqual => Predicate::False,
                        // NULL < anything is false
                        ComparisonOperator::LessThan => Predicate::False,
                        // NULL <= anything is false
                        ComparisonOperator::LessThanOrEqual => Predicate::False,
                        // NULL IN (...) is false
                        ComparisonOperator::In => Predicate::False,
                        // NULL BETWEEN ... is false
                        ComparisonOperator::Between => Predicate::False,
                    }
                } else {
                    // No NULL identifiers, keep the comparison as is
                    Predicate::Comparison { left: left.clone(), operator: operator.clone(), right: right.clone() }
                }
            }
            Predicate::IsNull(expr) => {
                // If we're explicitly checking for NULL and the identifier is in our null list,
                // then this evaluates to true
                match &**expr {
                    Expr::Identifier(id) => {
                        let is_null = match id {
                            Identifier::Property(name) => columns.contains(name),
                            Identifier::CollectionProperty(_, name) => columns.contains(name),
                        };
                        if is_null {
                            Predicate::True
                        } else {
                            Predicate::IsNull(expr.clone())
                        }
                    }
                    _ => Predicate::IsNull(expr.clone()),
                }
            }
            Predicate::And(left, right) => {
                let left = left.assume_null(columns);
                let right = right.assume_null(columns);

                // Optimize
                match (&left, &right) {
                    // if either side is false, the whole thing is false
                    (Predicate::False, _) | (_, Predicate::False) => Predicate::False,
                    // if both sides are true, the whole thing is true
                    (Predicate::True, Predicate::True) => Predicate::True,
                    // if one side is true, the whole thing is the other side
                    (Predicate::True, p) | (p, Predicate::True) => p.clone(),
                    _ => Predicate::And(Box::new(left), Box::new(right)),
                }
            }
            Predicate::Or(left, right) => {
                let left = left.assume_null(columns);
                let right = right.assume_null(columns);

                // Optimize
                match (&left, &right) {
                    // if either side is true, the whole thing is true
                    (Predicate::True, _) | (_, Predicate::True) => Predicate::True,
                    // if both sides are false, the whole thing is false
                    (Predicate::False, Predicate::False) => Predicate::False,
                    // if one side is false, the whole thing is the other side
                    (Predicate::False, p) | (p, Predicate::False) => p.clone(),
                    // otherwise, keep the original
                    _ => Predicate::Or(Box::new(left), Box::new(right)),
                }
            }
            Predicate::Not(pred) => {
                let inner = pred.assume_null(columns);
                match inner {
                    Predicate::True => Predicate::False,
                    Predicate::False => Predicate::True,
                    _ => Predicate::Not(Box::new(inner)),
                }
            }
            // These are constants, just clone them
            Predicate::True => Predicate::True,
            Predicate::False => Predicate::False,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,              // =
    NotEqual,           // <> or !=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    In,                 // IN
    Between,            // BETWEEN
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InfixOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ParseError;
    use crate::parser::parse_selection;

    fn nullify_columns(input: &str, null_columns: &[&str]) -> Result<String, ParseError> {
        let pred = parse_selection(input)?;
        let result = pred.assume_null(&null_columns.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        Ok(generate_selection_sql(&result))
    }

    #[test]
    fn test_single_comparison_null_handling() {
        assert_eq!(nullify_columns("status = 'active'", &["status"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("age > 30", &["age"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("count >= 100", &["count"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("name < 'Z'", &["name"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("score <= 90", &["score"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("status IS NULL", &["status"]).unwrap(), "TRUE");
        assert_eq!(nullify_columns("role = 'admin'", &["other"]).unwrap(), r#""role" = 'admin'"#);
    }

    #[test]
    fn nested_predicate_null_handling() {
        let input = "alpha = 1 AND (beta = 2 OR charlie = 3)";
        assert_eq!(nullify_columns(input, &["charlie"]).unwrap(), r#""alpha" = 1 AND "beta" = 2"#);
        assert_eq!(nullify_columns(input, &["beta", "charlie"]).unwrap(), r#"FALSE"#);
        assert_eq!(nullify_columns(input, &["alpha"]).unwrap(), r#"FALSE"#);
        assert_eq!(nullify_columns(input, &["other"]).unwrap(), r#""alpha" = 1 AND ("beta" = 2 OR "charlie" = 3)"#);
    }
}
