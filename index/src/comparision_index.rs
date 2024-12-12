use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::{collation::Collation, reactor::SubscriptionId};

/// An index for a specific field and comparison operator
#[derive(Debug, Default)]
pub(crate) struct ComparisonIndex {
    eq: HashMap<Vec<u8>, Vec<SubscriptionId>>,
    gt: BTreeMap<Vec<u8>, Vec<SubscriptionId>>,
    lt: BTreeMap<Vec<u8>, Vec<SubscriptionId>>,
}

impl ComparisonIndex {
    fn add_subscription(&mut self, value: &str, op: &str, sub_id: SubscriptionId) {
        let bytes = crate::collation::Collation::to_bytes(value);
        match op {
            "=" => {
                self.eq.entry(bytes).or_default().push(sub_id);
            }
            ">" => {
                self.gt.entry(bytes).or_default().push(sub_id);
            }
            "<" => {
                self.lt.entry(bytes).or_default().push(sub_id);
            }
            ">=" => {
                // x >= 5 is equivalent to x > 4
                if let Some(pred) = crate::collation::Collation::predecessor_bytes(value) {
                    self.gt.entry(pred).or_default().push(sub_id);
                } else {
                    // If there is no predecessor (e.g., value is minimum), then this matches everything
                    self.gt.entry(Vec::new()).or_default().push(sub_id);
                }
            }
            "<=" => {
                // x <= 5 is equivalent to x < 6
                if let Some(succ) = crate::collation::Collation::successor_bytes(value) {
                    self.lt.entry(succ).or_default().push(sub_id);
                }
            }
            _ => panic!("Unsupported operator: {}", op),
        }
    }

    fn find_matching_subscriptions(&self, value: &str) -> Vec<SubscriptionId> {
        let mut result = BTreeSet::new();
        let bytes = value.to_bytes();

        // Check exact matches
        if let Some(subs) = self.eq.get(&bytes) {
            result.extend(subs.iter().cloned());
        }

        // Check greater than matches (value > threshold)
        // We want all thresholds strictly less than our value
        for (_, subs) in self.gt.range(..bytes.clone()) {
            result.extend(subs);
        }

        // Check less than matches (value < threshold)
        // We want all thresholds strictly greater than our value
        for (threshold, subs) in self.lt.range(bytes.clone()..) {
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
        index.add_subscription("25", ">=", 1.into()); // age >= 25
        index.add_subscription("90", "<=", 1.into()); // age <= 90
        index.add_subscription("30", "=", 2.into()); // age == 30

        // Test some values
        assert!(
            index.find_matching_subscriptions("24").is_empty(),
            "24 should not match"
        );
        assert_eq!(
            index.find_matching_subscriptions("25"),
            vec![1],
            "25 should match >= 25"
        );
        let mut matches_30 = index.find_matching_subscriptions("30");
        matches_30.sort_unstable();
        assert_eq!(
            matches_30,
            vec![1, 2],
            "30 should match both >= 25 and == 30"
        );
        assert_eq!(
            index.find_matching_subscriptions("90"),
            vec![1],
            "90 should match <= 90"
        );
        assert!(
            index.find_matching_subscriptions("91").is_empty(),
            "91 should not match"
        );

        // Test edge cases
        assert_eq!(
            index.find_matching_subscriptions("26"),
            vec![1],
            "26 should match >= 25"
        );
        assert_eq!(
            index.find_matching_subscriptions("89"),
            vec![1],
            "89 should match both >= 25 and <= 90"
        );
    }

    #[test]
    fn test_range_queries() {
        let mut index = ComparisonIndex::default();

        // Test greater than
        index.add_subscription("25", ">", 1.into());
        assert!(
            index.find_matching_subscriptions("24").is_empty(),
            "> 25: 24 should not match"
        );
        assert!(
            index.find_matching_subscriptions("25").is_empty(),
            "> 25: 25 should not match"
        );
        assert_eq!(
            index.find_matching_subscriptions("26"),
            vec![1],
            "> 25: 26 should match"
        );

        // Test less than
        index.add_subscription("25", "<", 2.into());
        assert_eq!(
            index.find_matching_subscriptions("24"),
            vec![2],
            "< 25: 24 should match"
        );
        assert!(
            index.find_matching_subscriptions("25").is_empty(),
            "< 25: 25 should not match"
        );
        assert!(
            index.find_matching_subscriptions("26").is_empty(),
            "< 25: 26 should not match"
        );

        // Test greater than or equal
        index.add_subscription("25", ">=", 3.into());
        assert!(
            index.find_matching_subscriptions("24").is_empty(),
            ">= 25: 24 should not match"
        );
        assert_eq!(
            index.find_matching_subscriptions("25"),
            vec![3],
            ">= 25: 25 should match"
        );
        assert_eq!(
            index.find_matching_subscriptions("26"),
            vec![1, 3],
            ">= 25: 26 should match"
        );

        // Test less than or equal
        index.add_subscription("25", "<=", 4.into());
        assert_eq!(
            index.find_matching_subscriptions("24"),
            vec![2, 4],
            "<= 25: 24 should match"
        );
        assert_eq!(
            index.find_matching_subscriptions("25"),
            vec![4],
            "<= 25: 25 should match"
        );
        assert!(
            index.find_matching_subscriptions("26").is_empty(),
            "<= 25: 26 should not match"
        );
    }
}
