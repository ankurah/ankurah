//! Filter items based on a predicate. This is necessary for cases where we are scanning over a set of data
//! which has not been pre-filtered by an index search - or to supplement/validate an index search with additional filtering.

use crate::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("collection mismatch: expected {expected}, got {actual}")]
    CollectionMismatch { expected: String, actual: String },
    #[error("property not found: {0}")]
    PropertyNotFound(String),
}

// Just to make this fast, lets assume all values are strings.
pub trait Filterable {
    fn collection(&self) -> &str;
    // TODO figure out how to make this generic so we can perform typecast eligibity checking
    // and perform the actual typecast for comparisons
    fn value(&self, name: &str) -> Option<String>;
}

fn evaluate_expr<I: Filterable>(item: &I, expr: &Expr) -> Result<Option<String>, Error> {
    match expr {
        Expr::Literal(lit) => Ok(match lit {
            Literal::String(s) => Some(s.clone()),
            Literal::Integer(i) => Some(i.to_string()),
            Literal::Float(f) => Some(f.to_string()),
            Literal::Boolean(b) => Some(b.to_string()),
        }),
        Expr::Identifier(id) => match id {
            Identifier::Property(name) => Ok(item.value(name)),
            Identifier::CollectionProperty(collection, name) => {
                if collection != item.collection() {
                    return Err(Error::CollectionMismatch { expected: collection.clone(), actual: item.collection().to_string() });
                }
                Ok(item.value(name))
            }
        },
        _ => unimplemented!("Only literal and identifier expressions are supported"),
    }
}

pub fn evaluate_predicate<I: Filterable>(item: &I, predicate: &Predicate) -> Result<bool, Error> {
    match predicate {
        Predicate::Comparison { left, operator, right } => {
            let left_val = evaluate_expr(item, left)?;
            let right_val = evaluate_expr(item, right)?;

            Ok(match operator {
                ComparisonOperator::Equal => left_val == right_val,
                ComparisonOperator::NotEqual => left_val != right_val,
                ComparisonOperator::GreaterThan => left_val > right_val,
                ComparisonOperator::GreaterThanOrEqual => left_val >= right_val,
                ComparisonOperator::LessThan => left_val < right_val,
                ComparisonOperator::LessThanOrEqual => left_val <= right_val,
                _ => unimplemented!("Only basic comparison operators are supported"),
            })
        }
        Predicate::And(left, right) => Ok(evaluate_predicate(item, left)? && evaluate_predicate(item, right)?),
        Predicate::Or(left, right) => Ok(evaluate_predicate(item, left)? || evaluate_predicate(item, right)?),
        Predicate::Not(pred) => Ok(!evaluate_predicate(item, pred)?),
        Predicate::IsNull(expr) => Ok(evaluate_expr(item, expr)?.is_none()),
        Predicate::True => Ok(true),
        Predicate::False => Ok(false),
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

        let predicate = parse_selection("name = 'Alice'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), predicate).collect();

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

        let predicate = parse_selection("name = 'Alice' AND age = '30'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), predicate).collect();

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

        let predicate = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= '30' AND age <= '40'").unwrap();
        let results: Vec<_> = FilterIterator::new(items.into_iter(), predicate).collect();

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
}
