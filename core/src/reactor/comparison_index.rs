use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;

use crate::collation::Collatable;
use crate::value::{Value, ValueType};
use ankql::ast;

/// An index for a specific field and comparison operator
/// Used for storage engines that don't offer watchable indexes
/// This is a naive implementation that uses a BTreeMap for each operator
/// This is not efficient for large datasets. If this ends up being used in production
/// we should consider using a more efficient index structure like a B+ tree with subscription
/// registrations on intermediate nodes for range comparisons.
#[derive(Debug)]
pub(crate) struct ComparisonIndex<T> {
    pub(crate) eq: HashMap<Vec<u8>, Vec<T>>,
    pub(crate) ne: HashMap<Vec<u8>, Vec<T>>,
    pub(crate) gt: BTreeMap<Vec<u8>, Vec<T>>,
    pub(crate) lt: BTreeMap<Vec<u8>, Vec<T>>,
    target_type: Option<ValueType>,
}

impl<T> Default for ComparisonIndex<T> {
    fn default() -> Self { Self { eq: HashMap::new(), ne: HashMap::new(), gt: BTreeMap::new(), lt: BTreeMap::new(), target_type: None } }
}

impl<T: Clone + Eq + Hash + Ord> ComparisonIndex<T> {
    #[allow(unused)]
    pub fn new() -> Self { Self::default() }

    pub fn with_target_type(target_type: ValueType) -> Self { Self { target_type: Some(target_type), ..Self::default() } }

    fn for_entry<F, V>(&mut self, value: V, op: ast::ComparisonOperator, f: F)
    where
        F: FnOnce(&mut Vec<T>),
        V: Collatable,
    {
        match op {
            ast::ComparisonOperator::Equal => {
                let entry = self.eq.entry(value.to_bytes()).or_default();
                f(entry);
            }
            ast::ComparisonOperator::NotEqual => {
                let entry = self.ne.entry(value.to_bytes()).or_default();
                f(entry);
            }
            ast::ComparisonOperator::GreaterThan => {
                let entry = self.gt.entry(value.to_bytes()).or_default();
                f(entry);
            }
            ast::ComparisonOperator::LessThan => {
                let entry = self.lt.entry(value.to_bytes()).or_default();
                f(entry);
            }
            ast::ComparisonOperator::GreaterThanOrEqual => {
                // x >= 5 is equivalent to x > 4
                if let Some(pred) = value.predecessor_bytes() {
                    let entry = self.gt.entry(pred).or_default();
                    f(entry);
                } else {
                    // If there is no predecessor (e.g., value is minimum), then this matches everything
                    let entry = self.gt.entry(Vec::new()).or_default();
                    f(entry);
                }
            }
            ast::ComparisonOperator::LessThanOrEqual => {
                // x <= 5 is equivalent to x < 6
                if let Some(succ) = value.successor_bytes() {
                    let entry = self.lt.entry(succ).or_default();
                    f(entry);
                }
            }
            _ => panic!("Unsupported operator: {:?}", op),
        }
    }

    pub fn add(&mut self, value: Value, op: ast::ComparisonOperator, watcher_id: T) {
        let Some(value) = self.normalize(value) else { return };
        self.for_entry(value, op, |entries| entries.push(watcher_id));
    }

    pub fn remove(&mut self, value: Value, op: ast::ComparisonOperator, watcher_id: T) {
        let Some(value) = self.normalize(value) else { return };
        self.for_entry(value, op, |entries| {
            if let Some(pos) = entries.iter().position(|id| *id == watcher_id) {
                entries.remove(pos);
            }
        });
    }

    /// Find watchers whose thresholds admit `value` after re-casting at the
    /// entity-change boundary. If a malformed stored value cannot enter the
    /// registered collation domain, return every watcher conservatively; the
    /// reactor's exact predicate evaluation then decides membership.
    pub fn find_matching(&self, value: Value) -> std::collections::btree_set::IntoIter<T> {
        let Some(value) = self.normalize(value) else { return self.all_watchers().into_iter() };
        let mut result = BTreeSet::new();
        let bytes = value.to_bytes();

        // Check exact matches
        if let Some(subs) = self.eq.get(&bytes) {
            result.extend(subs.iter().cloned());
        }

        // Check not equal - iterate through all != conditions
        for (stored_bytes, subs) in &self.ne {
            if bytes != *stored_bytes {
                // query_value != stored_value
                result.extend(subs.iter().cloned());
            }
        }

        // Check greater than matches (x > threshold)
        for (_threshold, subs) in self.gt.range(..bytes.clone()) {
            result.extend(subs.iter().cloned());
        }

        // Check less than matches (x < threshold)
        if let Some(pred) = value.successor_bytes() {
            for (_threshold, subs) in self.lt.range(pred..) {
                result.extend(subs.iter().cloned());
            }
        }

        // Should just return the BTreeSet but this sucks for test cases
        result.into_iter()
    }

    fn normalize(&self, value: Value) -> Option<Value> { self.target_type.map_or(Some(value.clone()), |target| value.cast_to(target).ok()) }

    fn all_watchers(&self) -> BTreeSet<T> {
        self.eq.values().chain(self.ne.values()).chain(self.gt.values()).chain(self.lt.values()).flatten().cloned().collect()
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
        let mut index = ComparisonIndex::new();

        // Less than 8 ------------------------------------------------------------
        let sub0 = proto::QueryId::test(0);
        index.add(Value::I64(8), ast::ComparisonOperator::LessThan, sub0);

        // 8 should match nothing
        assert_eq!(index.find_matching(Value::I64(8)).collect::<Vec<_>>(), vec![]);

        // 7 should match sub0
        assert_eq!(index.find_matching(Value::I64(7)).collect::<Vec<_>>(), vec![sub0]);

        let sub1 = proto::QueryId::test(1);

        // Greater than 20 ------------------------------------------------------------
        index.add(Value::I64(20), ast::ComparisonOperator::GreaterThan, sub1);

        // 20 should match nothing
        assert_eq!(index.find_matching(Value::I64(20)).collect::<Vec<_>>(), vec![]);

        // 21 should match sub1
        assert_eq!(index.find_matching(Value::I64(21)).collect::<Vec<_>>(), vec![sub1]);

        // // Add subscriptions for various numeric comparisons
        index.add(Value::I64(5), ast::ComparisonOperator::Equal, sub0);

        // // Test exact match (5)
        assert_eq!(index.find_matching(Value::I64(5)).collect::<Vec<_>>(), vec![sub0]);

        // Less than 25 ------------------------------------------------------------
        index.add(Value::I64(25), ast::ComparisonOperator::LessThan, sub0);

        // 22 should match sub0 (< 25) and sub1 (> 20)
        assert_eq!(index.find_matching(Value::I64(22)).collect::<Vec<_>>(), vec![sub0, sub1]);

        // 25 should match sub1 because > 20
        assert_eq!(index.find_matching(Value::I64(25)).collect::<Vec<_>>(), vec![sub1]);

        // 26 should match sub1 because > 20
        assert_eq!(index.find_matching(Value::I64(26)).collect::<Vec<_>>(), vec![sub1]);
    }

    #[test]
    fn test_field_index_not_equal() {
        let mut index = ComparisonIndex::<proto::QueryId>::new();

        let sub0 = proto::QueryId::test(0);
        index.add(Value::I64(8), ast::ComparisonOperator::NotEqual, sub0);

        assert_eq!(index.find_matching(Value::I64(8)).collect::<Vec<_>>(), vec![]);
        assert_eq!(index.find_matching(Value::I64(9)).collect::<Vec<_>>(), vec![sub0]);
    }

    #[test]
    fn typed_index_recasts_changed_entity_values() {
        let mut index = ComparisonIndex::with_target_type(ValueType::I64);
        let subscription = proto::QueryId::test(0);
        index.add(Value::I32(8), ast::ComparisonOperator::Equal, subscription);

        assert_eq!(index.find_matching(Value::String("8".to_owned())).collect::<Vec<_>>(), vec![subscription]);
    }

    #[test]
    fn uncastable_changed_value_conservatively_wakes_every_watcher() {
        let mut index = ComparisonIndex::with_target_type(ValueType::I64);
        let equal = proto::QueryId::test(0);
        let not_equal = proto::QueryId::test(1);
        index.add(Value::I64(8), ast::ComparisonOperator::Equal, equal);
        index.add(Value::I64(8), ast::ComparisonOperator::NotEqual, not_equal);

        assert_eq!(index.find_matching(Value::String("not-a-number".to_owned())).collect::<Vec<_>>(), vec![equal, not_equal]);
    }
}
