use crate::error::ParseError;
use crate::selection::sql::generate_selection_sql;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Literal),
    Identifier(Identifier),
    Predicate(Predicate),
    InfixExpr { left: Box<Expr>, operator: InfixOperator, right: Box<Expr> },
    ExprList(Vec<Expr>), // New variant for handling lists like (1,2,3) in IN clauses
    Placeholder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    // Id(EntityId), // TODO consolidate ast into proto crate so we can directly reference EntityId
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Identifier {
    Property(String),
    CollectionProperty(String, String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Selection {
    pub predicate: Predicate,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub identifier: Identifier,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl std::fmt::Display for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.predicate) }
}

// Backward compatibility
impl From<Predicate> for Selection {
    fn from(predicate: Predicate) -> Self { Selection { predicate, order_by: None, limit: None } }
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
    Placeholder,
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match generate_selection_sql(self, None) {
            Ok(sql) => write!(f, "{}", sql),
            Err(e) => write!(f, "SQL Error: {}", e),
        }
    }
}

impl Selection {
    pub fn assume_null(&self, columns: &[String]) -> Self {
        Self { predicate: self.predicate.assume_null(columns), order_by: self.order_by.clone(), limit: self.limit.clone() }
    }
}

impl Predicate {
    /// Recursively walk a predicate tree and accumulate results using a closure
    pub fn walk<T, F>(&self, accumulator: T, visitor: &mut F) -> T
    where F: FnMut(T, &Predicate) -> T {
        let accumulator = visitor(accumulator, self);
        match self {
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                let accumulator = left.walk(accumulator, visitor);
                right.walk(accumulator, visitor)
            }
            Predicate::Not(inner) => inner.walk(accumulator, visitor),
            _ => accumulator,
        }
    }

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
            Predicate::Placeholder => Predicate::Placeholder,
        }
    }

    /// Populate placeholders in the predicate with actual values
    pub fn populate<I, V>(self, values: I) -> Result<Predicate, ParseError>
    where
        I: IntoIterator<Item = V>,
        V: Into<Expr>,
    {
        let mut values_iter = values.into_iter();
        let result = self.populate_recursive(&mut values_iter)?;

        // Check if there are any unused values
        if values_iter.next().is_some() {
            return Err(ParseError::InvalidPredicate("Too many values provided for placeholders".to_string()));
        }

        Ok(result)
    }

    fn populate_recursive<I, V>(self, values: &mut I) -> Result<Predicate, ParseError>
    where
        I: Iterator<Item = V>,
        V: Into<Expr>,
    {
        match self {
            Predicate::Comparison { left, operator, right } => Ok(Predicate::Comparison {
                left: Box::new(left.populate_recursive(values)?),
                operator,
                right: Box::new(right.populate_recursive(values)?),
            }),
            Predicate::And(left, right) => {
                Ok(Predicate::And(Box::new(left.populate_recursive(values)?), Box::new(right.populate_recursive(values)?)))
            }
            Predicate::Or(left, right) => {
                Ok(Predicate::Or(Box::new(left.populate_recursive(values)?), Box::new(right.populate_recursive(values)?)))
            }
            Predicate::Not(pred) => Ok(Predicate::Not(Box::new(pred.populate_recursive(values)?))),
            Predicate::IsNull(expr) => Ok(Predicate::IsNull(Box::new(expr.populate_recursive(values)?))),
            Predicate::True => Ok(Predicate::True),
            Predicate::False => Ok(Predicate::False),
            // Placeholder should be transformed to a comparison before population
            Predicate::Placeholder => Err(ParseError::InvalidPredicate("Placeholder must be transformed before population".to_string())),
        }
    }
}

impl Expr {
    fn populate_recursive<I, V>(self, values: &mut I) -> Result<Expr, ParseError>
    where
        I: Iterator<Item = V>,
        V: Into<Expr>,
    {
        match self {
            Expr::Placeholder => match values.next() {
                Some(value) => Ok(value.into()),
                None => Err(ParseError::InvalidPredicate("Not enough values provided for placeholders".to_string())),
            },
            Expr::Literal(lit) => Ok(Expr::Literal(lit)),
            Expr::Identifier(id) => Ok(Expr::Identifier(id)),
            Expr::Predicate(pred) => Ok(Expr::Predicate(pred.populate_recursive(values)?)),
            Expr::InfixExpr { left, operator, right } => Ok(Expr::InfixExpr {
                left: Box::new(left.populate_recursive(values)?),
                operator,
                right: Box::new(right.populate_recursive(values)?),
            }),
            Expr::ExprList(exprs) => {
                let mut populated_exprs = Vec::new();
                for expr in exprs {
                    populated_exprs.push(expr.populate_recursive(values)?);
                }
                Ok(Expr::ExprList(populated_exprs))
            }
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
    use crate::parser::parse_selection;

    fn nullify_columns(input: &str, null_columns: &[&str]) -> Result<String, ParseError> {
        let selection = parse_selection(input)?;
        let result = selection.predicate.assume_null(&null_columns.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        generate_selection_sql(&result, None).map_err(|_| ParseError::InvalidPredicate("SQL generation failed".to_string()))
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

    #[test]
    fn test_populate_single_placeholder() {
        let selection = parse_selection("name = ?").unwrap();
        let populated = selection.predicate.populate(vec!["Alice"]).unwrap();

        let expected = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier::Property("name".to_string()))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("Alice".to_string()))),
        };

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_multiple_placeholders() {
        let selection = parse_selection("age > ? AND name = ?").unwrap();
        let values: Vec<Expr> = vec![25i64.into(), "Bob".into()];
        let populated = selection.predicate.populate(values).unwrap();

        let expected = Predicate::And(
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier::Property("age".to_string()))),
                operator: ComparisonOperator::GreaterThan,
                right: Box::new(Expr::Literal(Literal::Integer(25))),
            }),
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier::Property("name".to_string()))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("Bob".to_string()))),
            }),
        );

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_in_clause() {
        let selection = parse_selection("status IN (?, ?, ?)").unwrap();
        let populated = selection.predicate.populate(vec!["active", "pending", "review"]).unwrap();

        let expected = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier::Property("status".to_string()))),
            operator: ComparisonOperator::In,
            right: Box::new(Expr::ExprList(vec![
                Expr::Literal(Literal::String("active".to_string())),
                Expr::Literal(Literal::String("pending".to_string())),
                Expr::Literal(Literal::String("review".to_string())),
            ])),
        };

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_mixed_types() {
        let selection = parse_selection("active = ? AND score > ? AND name = ?").unwrap();
        let values: Vec<Expr> = vec![true.into(), 95.5f64.into(), "Charlie".into()];
        let populated = selection.predicate.populate(values).unwrap();

        // Verify the structure is correct
        if let Predicate::And(left, right) = populated {
            if let Predicate::And(inner_left, inner_right) = *left {
                // Check boolean value
                if let Predicate::Comparison { right: val, .. } = *inner_left {
                    assert_eq!(*val, Expr::Literal(Literal::Boolean(true)));
                }
                // Check float value
                if let Predicate::Comparison { right: val, .. } = *inner_right {
                    assert_eq!(*val, Expr::Literal(Literal::Float(95.5)));
                }
            }
            // Check string value
            if let Predicate::Comparison { right: val, .. } = *right {
                assert_eq!(*val, Expr::Literal(Literal::String("Charlie".to_string())));
            }
        }
    }

    #[test]
    fn test_populate_too_few_values() {
        let selection = parse_selection("name = ? AND age = ?").unwrap();
        let result = selection.predicate.populate(vec!["Alice"]);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Not enough values"));
    }

    #[test]
    fn test_populate_too_many_values() {
        let selection = parse_selection("name = ?").unwrap();
        let result = selection.predicate.populate(vec!["Alice", "Bob"]);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Too many values"));
    }

    #[test]
    fn test_populate_no_placeholders() {
        let selection = parse_selection("name = 'Alice'").unwrap();
        let populated = selection.clone().predicate.populate(Vec::<String>::new()).unwrap();

        // Should be unchanged
        assert_eq!(populated, selection.predicate);
    }
}

// From implementations for single values that wrap them in Expr::Literal
impl From<String> for Expr {
    fn from(s: String) -> Expr { Expr::Literal(Literal::String(s)) }
}

impl From<&str> for Expr {
    fn from(s: &str) -> Expr { Expr::Literal(Literal::String(s.to_string())) }
}

impl From<i64> for Expr {
    fn from(i: i64) -> Expr { Expr::Literal(Literal::Integer(i)) }
}

impl From<f64> for Expr {
    fn from(f: f64) -> Expr { Expr::Literal(Literal::Float(f)) }
}

impl From<bool> for Expr {
    fn from(b: bool) -> Expr { Expr::Literal(Literal::Boolean(b)) }
}

impl From<Literal> for Expr {
    fn from(lit: Literal) -> Expr { Expr::Literal(lit) }
}

// These create Expr::ExprList for use in IN clauses
impl<T> From<Vec<T>> for Expr
where T: Into<Expr>
{
    fn from(vec: Vec<T>) -> Self { Expr::ExprList(vec.into_iter().map(|item| item.into()).collect()) }
}

impl<T, const N: usize> From<[T; N]> for Expr
where T: Into<Expr>
{
    fn from(arr: [T; N]) -> Self { Expr::ExprList(arr.into_iter().map(|item| item.into()).collect()) }
}

impl<T> From<&[T]> for Expr
where T: Into<Expr> + Clone
{
    fn from(slice: &[T]) -> Self { Expr::ExprList(slice.iter().map(|item| item.clone().into()).collect()) }
}

impl<T, const N: usize> From<&[T; N]> for Expr
where T: Into<Expr> + Clone
{
    fn from(arr: &[T; N]) -> Self { Expr::ExprList(arr.iter().map(|item| item.clone().into()).collect()) }
}
