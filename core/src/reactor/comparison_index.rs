use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;

use crate::collation::Collatable;
use crate::value::{CastError, Value, ValueType};
use ankql::ast;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ComparisonIndexError {
    #[error(transparent)]
    Cast(#[from] CastError),
    #[error("comparison index does not support operator {0:?}")]
    UnsupportedOperator(ast::ComparisonOperator),
}

/// An in-memory watcher index for one property with one canonical value type.
///
/// This is the fallback for storage engines that do not expose watchable
/// indexes. It uses separate maps for each comparison direction; if production
/// workloads outgrow it, a B+ tree with subscriptions on intermediate nodes
/// would be a better implementation.
#[derive(Debug)]
pub(crate) struct ComparisonIndex<T> {
    pub(crate) eq: HashMap<Vec<u8>, Vec<T>>,
    pub(crate) ne: HashMap<Vec<u8>, Vec<T>>,
    pub(crate) gt: BTreeMap<Vec<u8>, Vec<T>>,
    pub(crate) lt: BTreeMap<Vec<u8>, Vec<T>>,
    /// Inclusive comparisons at the domain boundary (`x >= MIN` or
    /// `x <= MAX`) admit every representable value and have no finite
    /// threshold to place in either range map.
    always: Vec<T>,
    value_type: ValueType,
}

impl<T: Clone + Eq + Hash + Ord> ComparisonIndex<T> {
    pub fn new(value_type: ValueType) -> Self {
        Self { eq: HashMap::new(), ne: HashMap::new(), gt: BTreeMap::new(), lt: BTreeMap::new(), always: Vec::new(), value_type }
    }

    fn for_entry<F, V>(&mut self, value: V, op: ast::ComparisonOperator, f: F) -> Result<(), ComparisonIndexError>
    where
        F: FnOnce(&mut Vec<T>),
        V: Collatable,
    {
        match op {
            ast::ComparisonOperator::Equal => f(self.eq.entry(value.to_bytes()).or_default()),
            ast::ComparisonOperator::NotEqual => f(self.ne.entry(value.to_bytes()).or_default()),
            ast::ComparisonOperator::GreaterThan => f(self.gt.entry(value.to_bytes()).or_default()),
            ast::ComparisonOperator::LessThan => f(self.lt.entry(value.to_bytes()).or_default()),
            ast::ComparisonOperator::GreaterThanOrEqual => {
                // x >= 5 is equivalent to x > predecessor(5). The minimum
                // value has no predecessor and therefore admits everything.
                match value.predecessor_bytes() {
                    Some(threshold) => f(self.gt.entry(threshold).or_default()),
                    None => f(&mut self.always),
                }
            }
            ast::ComparisonOperator::LessThanOrEqual => {
                // x <= 5 is equivalent to x < successor(5). The maximum
                // value has no successor and therefore admits everything.
                match value.successor_bytes() {
                    Some(threshold) => f(self.lt.entry(threshold).or_default()),
                    None => f(&mut self.always),
                }
            }
            ast::ComparisonOperator::In | ast::ComparisonOperator::Between => {
                return Err(ComparisonIndexError::UnsupportedOperator(op));
            }
        }
        Ok(())
    }

    pub fn add(&mut self, value: Value, op: ast::ComparisonOperator, watcher_id: T) -> Result<(), ComparisonIndexError> {
        let value = value.cast_to(self.value_type)?;
        self.for_entry(value, op, |entries| entries.push(watcher_id))
    }

    pub fn remove(&mut self, value: Value, op: ast::ComparisonOperator, watcher_id: T) -> Result<(), ComparisonIndexError> {
        let value = value.cast_to(self.value_type)?;
        self.for_entry(value, op, |entries| {
            if let Some(pos) = entries.iter().position(|id| *id == watcher_id) {
                entries.remove(pos);
            }
        })
    }

    /// Find watchers whose thresholds admit `value`.
    ///
    /// Every lookup casts into this index's canonical collation type. Callers
    /// decide how to fall back when malformed stored data cannot be cast.
    pub fn find_matching(&self, value: Value) -> Result<std::collections::btree_set::IntoIter<T>, ComparisonIndexError> {
        let value = value.cast_to(self.value_type)?;
        let mut result: BTreeSet<_> = self.always.iter().cloned().collect();
        let bytes = value.to_bytes();

        if let Some(subs) = self.eq.get(&bytes) {
            result.extend(subs.iter().cloned());
        }

        for (stored_bytes, subs) in &self.ne {
            if bytes != *stored_bytes {
                result.extend(subs.iter().cloned());
            }
        }

        for (_, subs) in self.gt.range(..bytes.clone()) {
            result.extend(subs.iter().cloned());
        }

        if let Some(successor) = value.successor_bytes() {
            for (_, subs) in self.lt.range(successor..) {
                result.extend(subs.iter().cloned());
            }
        }

        Ok(result.into_iter())
    }

    pub fn all_watchers(&self) -> BTreeSet<T> {
        self.eq
            .values()
            .chain(self.ne.values())
            .chain(self.gt.values())
            .chain(self.lt.values())
            .flatten()
            .chain(self.always.iter())
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::ComparisonIndex;
    use crate::value::{Value, ValueType};
    use ankql::ast;
    use ankurah_proto as proto;

    #[test]
    fn test_field_index() {
        let mut index = ComparisonIndex::new(ValueType::I64);

        let sub0 = proto::QueryId::test(0);
        index.add(Value::I64(8), ast::ComparisonOperator::LessThan, sub0).unwrap();
        assert_eq!(index.find_matching(Value::I64(8)).unwrap().collect::<Vec<_>>(), vec![]);
        assert_eq!(index.find_matching(Value::I64(7)).unwrap().collect::<Vec<_>>(), vec![sub0]);

        let sub1 = proto::QueryId::test(1);
        index.add(Value::I64(20), ast::ComparisonOperator::GreaterThan, sub1).unwrap();
        assert_eq!(index.find_matching(Value::I64(20)).unwrap().collect::<Vec<_>>(), vec![]);
        assert_eq!(index.find_matching(Value::I64(21)).unwrap().collect::<Vec<_>>(), vec![sub1]);

        index.add(Value::I64(5), ast::ComparisonOperator::Equal, sub0).unwrap();
        assert_eq!(index.find_matching(Value::I64(5)).unwrap().collect::<Vec<_>>(), vec![sub0]);

        index.add(Value::I64(25), ast::ComparisonOperator::LessThan, sub0).unwrap();
        assert_eq!(index.find_matching(Value::I64(22)).unwrap().collect::<Vec<_>>(), vec![sub0, sub1]);
        assert_eq!(index.find_matching(Value::I64(25)).unwrap().collect::<Vec<_>>(), vec![sub1]);
        assert_eq!(index.find_matching(Value::I64(26)).unwrap().collect::<Vec<_>>(), vec![sub1]);
    }

    #[test]
    fn test_field_index_not_equal() {
        let mut index = ComparisonIndex::<proto::QueryId>::new(ValueType::I64);
        let sub0 = proto::QueryId::test(0);
        index.add(Value::I64(8), ast::ComparisonOperator::NotEqual, sub0).unwrap();
        assert_eq!(index.find_matching(Value::I64(8)).unwrap().collect::<Vec<_>>(), vec![]);
        assert_eq!(index.find_matching(Value::I64(9)).unwrap().collect::<Vec<_>>(), vec![sub0]);
    }

    #[test]
    fn typed_index_recasts_values() {
        let mut index = ComparisonIndex::new(ValueType::I64);
        let subscription = proto::QueryId::test(0);
        index.add(Value::I32(8), ast::ComparisonOperator::Equal, subscription).unwrap();
        assert_eq!(index.find_matching(Value::String("8".to_owned())).unwrap().collect::<Vec<_>>(), vec![subscription]);
    }

    #[test]
    fn uncastable_value_is_an_error_and_callers_can_wake_every_watcher() {
        let mut index = ComparisonIndex::new(ValueType::I64);
        let equal = proto::QueryId::test(0);
        let not_equal = proto::QueryId::test(1);
        index.add(Value::I64(8), ast::ComparisonOperator::Equal, equal).unwrap();
        index.add(Value::I64(8), ast::ComparisonOperator::NotEqual, not_equal).unwrap();

        assert!(index.find_matching(Value::String("not-a-number".to_owned())).is_err());
        assert_eq!(index.all_watchers().into_iter().collect::<Vec<_>>(), vec![equal, not_equal]);
    }

    #[test]
    fn inclusive_domain_boundaries_match_every_value_and_can_be_removed() {
        let mut index = ComparisonIndex::new(ValueType::I64);
        let above_minimum = proto::QueryId::test(0);
        let below_maximum = proto::QueryId::test(1);
        index.add(Value::I64(i64::MIN), ast::ComparisonOperator::GreaterThanOrEqual, above_minimum).unwrap();
        index.add(Value::I64(i64::MAX), ast::ComparisonOperator::LessThanOrEqual, below_maximum).unwrap();

        assert_eq!(index.find_matching(Value::I64(0)).unwrap().collect::<Vec<_>>(), vec![above_minimum, below_maximum]);
        index.remove(Value::I64(i64::MIN), ast::ComparisonOperator::GreaterThanOrEqual, above_minimum).unwrap();
        index.remove(Value::I64(i64::MAX), ast::ComparisonOperator::LessThanOrEqual, below_maximum).unwrap();
        assert!(index.find_matching(Value::I64(0)).unwrap().next().is_none());
    }
}
