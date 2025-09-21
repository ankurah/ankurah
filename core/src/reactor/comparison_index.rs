use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;

use crate::collation::Collatable;
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
}

impl<T> Default for ComparisonIndex<T> {
    fn default() -> Self { Self { eq: HashMap::new(), ne: HashMap::new(), gt: BTreeMap::new(), lt: BTreeMap::new() } }
}

impl<T: Clone + Eq + Hash + Ord> ComparisonIndex<T> {
    #[allow(unused)]
    pub fn new() -> Self { Self::default() }

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

    pub fn add<V: Collatable>(&mut self, value: V, op: ast::ComparisonOperator, watcher_id: T) {
        self.for_entry(value, op, |entries| entries.push(watcher_id));
    }

    pub fn remove<V: Collatable>(&mut self, value: V, op: ast::ComparisonOperator, watcher_id: T) {
        self.for_entry(value, op, |entries| {
            if let Some(pos) = entries.iter().position(|id| *id == watcher_id) {
                entries.remove(pos);
            }
        });
    }

    pub fn find_matching<V: Collatable>(&self, value: V) -> std::collections::btree_set::IntoIter<T> {
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
}

#[cfg(test)]
mod tests {
    use super::ComparisonIndex;
    use crate::value::Value;
    use ankql::ast;
    use ankurah_proto as proto;

    #[test]
    fn test_field_index() {
        let mut index = ComparisonIndex::new();

        // Less than 8 ------------------------------------------------------------
        let sub0 = proto::QueryId::test(0);
        index.add(ast::Literal::I64(8), ast::ComparisonOperator::LessThan, sub0);

        // 8 should match nothing
        assert_eq!(index.find_matching(Value::I64(8)).collect::<Vec<_>>(), vec![]);

        // 7 should match sub0
        assert_eq!(index.find_matching(Value::I64(7)).collect::<Vec<_>>(), vec![sub0]);

        let sub1 = proto::QueryId::test(1);

        // Greater than 20 ------------------------------------------------------------
        index.add(ast::Literal::I64(20), ast::ComparisonOperator::GreaterThan, sub1);

        // 20 should match nothing
        assert_eq!(index.find_matching(Value::I64(20)).collect::<Vec<_>>(), vec![]);

        // 21 should match sub1
        assert_eq!(index.find_matching(Value::I64(21)).collect::<Vec<_>>(), vec![sub1]);

        // // Add subscriptions for various numeric comparisons
        index.add(ast::Literal::I64(5), ast::ComparisonOperator::Equal, sub0);

        // // Test exact match (5)
        assert_eq!(index.find_matching(Value::I64(5)).collect::<Vec<_>>(), vec![sub0]);

        // Less than 25 ------------------------------------------------------------
        index.add(ast::Literal::I64(25), ast::ComparisonOperator::LessThan, sub0);

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
        index.add(ast::Literal::I64(8), ast::ComparisonOperator::NotEqual, sub0);

        assert_eq!(index.find_matching(Value::I64(8)).collect::<Vec<_>>(), vec![]);
        assert_eq!(index.find_matching(Value::I64(9)).collect::<Vec<_>>(), vec![sub0]);
    }
}
