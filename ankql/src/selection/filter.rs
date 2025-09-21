//! Filter items based on a predicate. This is necessary for cases where we are scanning over a set of data
//! which has not been pre-filtered by an index search - or to supplement/validate an index search with additional filtering.

use crate::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use base64::{engine::general_purpose, Engine as _};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("collection mismatch: expected {expected}, got {actual}")]
    CollectionMismatch { expected: String, actual: String },
    #[error("property not found: {0}")]
    PropertyNotFound(String),
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(&'static str),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(&'static str),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprOutput<T> {
    List(Vec<ExprOutput<T>>),
    Value(T),
    None,
}

impl<T: PartialEq> ExprOutput<T> {
    fn as_value(&self) -> Option<&T> {
        match self {
            ExprOutput::Value(v) => Some(v),
            _ => None,
        }
    }

    fn as_list(&self) -> Option<&Vec<ExprOutput<T>>> {
        match self {
            ExprOutput::List(l) => Some(l),
            _ => None,
        }
    }
}

impl ExprOutput<String> {
    fn is_none(&self) -> bool {
        match self {
            ExprOutput::None => true,
            _ => false,
        }
    }
}

// Just to make this fast, lets assume all values are strings.
pub trait Filterable {
    fn collection(&self) -> &str;
    // TODO figure out how to make this generic so we can perform typecast eligibity checking
    // and perform the actual typecast for comparisons
    fn value(&self, name: &str) -> Option<String>;
}

fn evaluate_expr<I: Filterable>(item: &I, expr: &Expr) -> Result<ExprOutput<String>, Error> {
    match expr {
        Expr::Placeholder => Err(Error::PropertyNotFound("Placeholder values must be replaced before filtering".to_string())),
        Expr::Literal(lit) => Ok(ExprOutput::Value(match lit {
            Literal::I16(i) => i.to_string(),
            Literal::I32(i) => i.to_string(),
            Literal::I64(i) => i.to_string(),
            Literal::F64(f) => f.to_string(),
            Literal::Bool(b) => b.to_string(),
            Literal::String(s) => s.clone(),
            Literal::EntityId(ulid) => general_purpose::URL_SAFE_NO_PAD.encode(ulid.to_bytes()),
            Literal::Object(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Literal::Binary(bytes) => String::from_utf8_lossy(bytes).to_string(),
        })),
        Expr::Identifier(id) => match id {
            Identifier::Property(name) => Ok(ExprOutput::Value(item.value(name).ok_or_else(|| Error::PropertyNotFound(name.clone()))?)),
            Identifier::CollectionProperty(collection, name) => {
                if collection != item.collection() {
                    return Err(Error::CollectionMismatch { expected: collection.clone(), actual: item.collection().to_string() });
                }
                Ok(ExprOutput::Value(item.value(name).ok_or_else(|| Error::PropertyNotFound(name.clone()))?))
            }
        },
        Expr::ExprList(exprs) => {
            let mut result = Vec::new();
            for expr in exprs {
                result.push(evaluate_expr(item, expr)?);
            }
            Ok(ExprOutput::List(result))
        }
        _ => Err(Error::UnsupportedExpression("Only literal, identifier, and list expressions are supported")),
    }
}

pub fn evaluate_predicate<I: Filterable>(item: &I, predicate: &Predicate) -> Result<bool, Error> {
    match predicate {
        Predicate::Comparison { left, operator, right } => {
            let left_val = evaluate_expr(item, left)?;
            let right_val = evaluate_expr(item, right)?;

            Ok(match operator {
                ComparisonOperator::Equal => left_val.as_value().zip(right_val.as_value()).map(|(l, r)| l == r).unwrap_or(false),
                ComparisonOperator::NotEqual => left_val.as_value().zip(right_val.as_value()).map(|(l, r)| l != r).unwrap_or(false),
                ComparisonOperator::GreaterThan => left_val.as_value().zip(right_val.as_value()).map(|(l, r)| l > r).unwrap_or(false),
                ComparisonOperator::GreaterThanOrEqual => {
                    left_val.as_value().zip(right_val.as_value()).map(|(l, r)| l >= r).unwrap_or(false)
                }
                ComparisonOperator::LessThan => left_val.as_value().zip(right_val.as_value()).map(|(l, r)| l < r).unwrap_or(false),
                ComparisonOperator::LessThanOrEqual => left_val.as_value().zip(right_val.as_value()).map(|(l, r)| l <= r).unwrap_or(false),
                ComparisonOperator::In => {
                    let value =
                        left_val.as_value().ok_or_else(|| Error::PropertyNotFound("Expected single value for IN left operand".into()))?;
                    let list = right_val.as_list().ok_or_else(|| Error::PropertyNotFound("Expected list for IN right operand".into()))?;
                    list.iter().any(|item| item.as_value().map(|v| v == value).unwrap_or(false))
                }
                ComparisonOperator::Between => return Err(Error::UnsupportedOperator("BETWEEN operator not yet supported")),
            })
        }
        Predicate::And(left, right) => Ok(evaluate_predicate(item, left)? && evaluate_predicate(item, right)?),
        Predicate::Or(left, right) => Ok(evaluate_predicate(item, left)? || evaluate_predicate(item, right)?),
        Predicate::Not(pred) => Ok(!evaluate_predicate(item, pred)?),
        Predicate::IsNull(expr) => Ok(evaluate_expr(item, expr)?.is_none()),
        Predicate::True => Ok(true),
        Predicate::False => Ok(false),
        // Placeholder should be transformed to a comparison before filtering
        Predicate::Placeholder => Err(Error::PropertyNotFound("Placeholder must be transformed before filtering".to_string())),
    }
}

#[derive(Debug, PartialEq)]
pub enum FilterResult<R> {
    Pass(R),
    Skip(R),
    Error(R, Error),
}

pub struct FilterIterator<I> {
    iter: I,
    predicate: Predicate,
}

impl<I, R> FilterIterator<I>
where
    I: Iterator<Item = R>,
    R: Filterable,
{
    pub fn new(iter: I, predicate: Predicate) -> Self { Self { iter, predicate } }
}

impl<I, R> Iterator for FilterIterator<I>
where
    I: Iterator<Item = R>,
    R: Filterable,
{
    type Item = FilterResult<R>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| match evaluate_predicate(&item, &self.predicate) {
            Ok(true) => FilterResult::Pass(item),
            Ok(false) => FilterResult::Skip(item),
            Err(e) => FilterResult::Error(item, e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_selection;

    #[derive(Debug, Clone, PartialEq)]
    struct TestItem {
        name: String,
        age: String,
    }

    impl Filterable for TestItem {
        fn collection(&self) -> &str { "users" }

        fn value(&self, name: &str) -> Option<String> {
            match name {
                "name" => Some(self.name.clone()),
                "age" => Some(self.age.clone()),
                _ => None,
            }
        }
    }

    impl TestItem {
        fn new(name: &str, age: &str) -> Self { Self { name: name.to_string(), age: age.to_string() } }
    }

    #[test]
    fn test_simple_equality() {
        let items = vec![TestItem::new("Alice", "30"), TestItem::new("Bob", "25"), TestItem::new("Charlie", "35")];

        let selection = parse_selection("name = 'Alice'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "30")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Skip(TestItem::new("Charlie", "35")),
            ]
        );
    }

    #[test]
    fn test_and_condition() {
        let items = vec![TestItem::new("Alice", "30"), TestItem::new("Bob", "30"), TestItem::new("Charlie", "35")];

        let selection = parse_selection("name = 'Alice' AND age = '30'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "30")),
                FilterResult::Skip(TestItem::new("Bob", "30")),
                FilterResult::Skip(TestItem::new("Charlie", "35")),
            ]
        );
    }

    #[test]
    fn test_complex_condition() {
        let items = vec![
            TestItem::new("Alice", "20"),
            TestItem::new("Bob", "25"),
            TestItem::new("Charlie", "30"),
            TestItem::new("David", "35"),
            TestItem::new("Eve", "40"),
        ];

        let selection = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= '30' AND age <= '40'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Skip(TestItem::new("Alice", "20")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Pass(TestItem::new("Charlie", "30")),
                FilterResult::Skip(TestItem::new("David", "35")),
                FilterResult::Skip(TestItem::new("Eve", "40")),
            ]
        );
    }

    #[test]
    fn test_in_operator() {
        let items = vec![
            TestItem::new("Alice", "20"),
            TestItem::new("Bob", "25"),
            TestItem::new("Charlie", "30"),
            TestItem::new("David", "35"),
            TestItem::new("Eve", "40"),
        ];

        // Test IN with names
        let selection = parse_selection("name IN ('Alice', 'Charlie', 'Eve')").unwrap();
        let results: Vec<_> = FilterIterator::new(items.clone().into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "20")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Pass(TestItem::new("Charlie", "30")),
                FilterResult::Skip(TestItem::new("David", "35")),
                FilterResult::Pass(TestItem::new("Eve", "40")),
            ]
        );

        // Test IN with ages
        let selection = parse_selection("age IN ('20', '30', '40')").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), selection.predicate).collect();

        assert_eq!(
            results,
            vec![
                FilterResult::Pass(TestItem::new("Alice", "20")),
                FilterResult::Skip(TestItem::new("Bob", "25")),
                FilterResult::Pass(TestItem::new("Charlie", "30")),
                FilterResult::Skip(TestItem::new("David", "35")),
                FilterResult::Pass(TestItem::new("Eve", "40")),
            ]
        );
    }
}
