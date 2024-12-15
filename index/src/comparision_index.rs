use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::{collation::Collatable, reactor::SubscriptionId};
use ankql::ast;

/// An index for a specific field and comparison operator
/// Used for storage engines that don't offer watchable indexes
/// This is a naive implementation that uses a BTreeMap for each operator
/// This is not efficient for large datasets. If this ends up being used in production
/// we should consider using a more efficient index structure like a B+ tree with subscription
/// registrations on intermediate nodes for range comparisons.
#[derive(Debug, Default)]
pub(crate) struct ComparisonIndex {
    pub(crate) eq: HashMap<Vec<u8>, Vec<SubscriptionId>>,
    pub(crate) gt: BTreeMap<Vec<u8>, Vec<SubscriptionId>>,
    pub(crate) lt: BTreeMap<Vec<u8>, Vec<SubscriptionId>>,
}

impl ComparisonIndex {
    pub(crate) fn new() -> Self {
        Self {
            eq: HashMap::new(),
            gt: BTreeMap::new(),
            lt: BTreeMap::new(),
        }
    }

    fn for_entry<F, V>(&mut self, value: V, op: ast::ComparisonOperator, f: F)
    where
        F: FnOnce(&mut Vec<SubscriptionId>),
        V: Collatable,
    {
        match op {
            ast::ComparisonOperator::Equal => {
                let entry = self.eq.entry(value.to_bytes()).or_default();
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

    pub fn add<V: Collatable>(
        &mut self,
        value: V,
        op: ast::ComparisonOperator,
        sub_id: SubscriptionId,
    ) {
        self.for_entry(value, op, |entries| entries.push(sub_id));
    }

    pub fn remove<V: Collatable>(
        &mut self,
        value: V,
        op: ast::ComparisonOperator,
        sub_id: SubscriptionId,
    ) {
        self.for_entry(value, op, |entries| {
            if let Some(pos) = entries.iter().position(|id| *id == sub_id) {
                entries.remove(pos);
            }
        });
    }
    pub fn find_matching_subscriptions<V: Collatable>(&self, value: V) -> Vec<SubscriptionId> {
        let mut result = BTreeSet::new();
        let bytes = value.to_bytes();

        // Check exact matches
        if let Some(subs) = self.eq.get(&bytes) {
            println!("eq: {:?}", subs);
            result.extend(subs.iter().cloned());
        }

        // Check greater than matches (value > threshold)
        // We want all thresholds strictly less than our value
        for (_, subs) in self.gt.range(..bytes.clone()) {
            println!("gt: {:?}", subs);
            result.extend(subs);
        }

        // Check less than matches (value < threshold)
        // We want all thresholds strictly greater than our value
        for (threshold, subs) in self.lt.range(bytes.clone()..) {
            println!("lt: {:?} {:?}", threshold, bytes);
            if threshold > &bytes {
                result.extend(subs.iter().cloned());
            }
        }
        // Should just return the BTreeSet but this sucks for test cases
        result.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_index() {
        let mut index = ComparisonIndex::default();

        // Add some subscriptions
        index.add(25, ast::ComparisonOperator::GreaterThanOrEqual, 1.into()); // age >= 25
        index.add(90, ast::ComparisonOperator::LessThanOrEqual, 2.into()); // age <= 90
        index.add(30, ast::ComparisonOperator::Equal, 3.into()); // age == 30

        // Test some values
        assert_eq!(
            index.find_matching_subscriptions(24),
            vec![2],
            "24 should match <= 90"
        );
        assert_eq!(
            index.find_matching_subscriptions(25),
            vec![1, 2],
            "25 should match >= 25 and <= 90"
        );
        assert_eq!(
            index.find_matching_subscriptions(30),
            vec![1, 2, 3],
            "30 should match >= 25 and <= 90 and == 30"
        );
        assert_eq!(
            index.find_matching_subscriptions(90),
            vec![1, 2],
            "90 should match >= 25 and <= 90"
        );
        assert_eq!(
            index.find_matching_subscriptions(91),
            vec![1],
            "91 should match >= 25"
        );
    }
}
