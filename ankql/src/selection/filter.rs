//! Filter records based on a predicate. This is necessary for cases where we are scanning over a set of data
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
    // and perform the actual typecast for comparisions
    fn value(&self, name: &str) -> Option<String>;
}

fn evaluate_expr<R: Filterable>(record: &R, expr: &Expr) -> Result<String, Error> {
    match expr {
        Expr::Literal(lit) => Ok(match lit {
            Literal::String(s) => s.clone(),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
        }),
        Expr::Identifier(id) => match id {
            Identifier::Property(name) => record
                .value(name)
                .ok_or_else(|| Error::PropertyNotFound(name.clone())),
            Identifier::CollectionProperty(collection, name) => {
                if collection != record.collection() {
                    return Err(Error::CollectionMismatch {
                        expected: collection.clone(),
                        actual: record.collection().to_string(),
                    });
                }
                record
                    .value(name)
                    .ok_or_else(|| Error::PropertyNotFound(name.clone()))
            }
        },
        _ => unimplemented!("Only literal and identifier expressions are supported"),
    }
}

pub fn evaluate_predicate<R: Filterable>(record: &R, predicate: &Predicate) -> Result<bool, Error> {
    match predicate {
        Predicate::Comparison {
            left,
            operator,
            right,
        } => {
            let left_val = evaluate_expr(record, left)?;
            let right_val = evaluate_expr(record, right)?;

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
        Predicate::And(left, right) => {
            Ok(evaluate_predicate(record, left)? && evaluate_predicate(record, right)?)
        }
        Predicate::Or(left, right) => {
            Ok(evaluate_predicate(record, left)? || evaluate_predicate(record, right)?)
        }
        Predicate::Not(pred) => Ok(!evaluate_predicate(record, pred)?),
        Predicate::IsNull(expr) => Ok(evaluate_expr(record, expr).is_err()),
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
    pub fn new(iter: I, predicate: Predicate) -> Self {
        Self { iter, predicate }
    }
}

impl<I, R> Iterator for FilterIterator<I>
where
    I: Iterator<Item = R>,
    R: Filterable,
{
    type Item = FilterResult<R>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(
            |record| match evaluate_predicate(&record, &self.predicate) {
                Ok(true) => FilterResult::Pass(record),
                Ok(false) => FilterResult::Skip(record),
                Err(e) => FilterResult::Error(record, e),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_selection;

    #[derive(Debug, Clone, PartialEq)]
    struct TestRecord {
        name: String,
        age: String,
    }

    impl Filterable for TestRecord {
        fn collection(&self) -> &str {
            "users"
        }

        fn value(&self, name: &str) -> Option<String> {
            match name {
                "name" => Some(self.name.clone()),
                "age" => Some(self.age.clone()),
                _ => None,
            }
        }
    }

    impl TestRecord {
        fn new(name: &str, age: &str) -> Self {
            Self {
                name: name.to_string(),
                age: age.to_string(),
            }
        }
    }

    #[test]
    fn test_simple_equality() {
        let records = vec![
            TestRecord::new("Alice", "30"),
            TestRecord::new("Bob", "25"),
            TestRecord::new("Charlie", "35"),
        ];

        let predicate = parse_selection("name = 'Alice'").unwrap();
        let results: Vec<_> = FilterIterator::new(records.into_iter(), predicate).collect();

        assert_eq!(results, vec![
            FilterResult::Pass(TestRecord::new("Alice", "30")),
            FilterResult::Skip(TestRecord::new("Bob", "25")),
            FilterResult::Skip(TestRecord::new("Charlie", "35")),
        ]);
    }

    #[test]
    fn test_and_condition() {
        let records = vec![
            TestRecord::new("Alice", "30"),
            TestRecord::new("Bob", "30"),
            TestRecord::new("Charlie", "35"),
        ];

        let predicate = parse_selection("name = 'Alice' AND age = '30'").unwrap();
        let results: Vec<_> = FilterIterator::new(records.into_iter(), predicate).collect();

        assert_eq!(results, vec![
            FilterResult::Pass(TestRecord::new("Alice", "30")),
            FilterResult::Skip(TestRecord::new("Bob", "30")),
            FilterResult::Skip(TestRecord::new("Charlie", "35")),
        ]);
    }

    #[test]
    fn test_complex_condition() {
        let records = vec![
            TestRecord::new("Alice", "20"),
            TestRecord::new("Bob", "25"),
            TestRecord::new("Charlie", "30"),
            TestRecord::new("David", "35"),
            TestRecord::new("Eve", "40"),
        ];

        let predicate =
            parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= '30' AND age <= '40'")
                .unwrap();
        let results: Vec<_> = FilterIterator::new(records.into_iter(), predicate).collect();

        assert_eq!(results, vec![
            FilterResult::Skip(TestRecord::new("Alice", "20")),
            FilterResult::Skip(TestRecord::new("Bob", "25")),
            FilterResult::Pass(TestRecord::new("Charlie", "30")),
            FilterResult::Skip(TestRecord::new("David", "35")),
            FilterResult::Skip(TestRecord::new("Eve", "40")),
        ]);
    }
}
