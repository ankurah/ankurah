use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use ankql::parser::parse_selection;
use ankql::selection::filter::{FilterIterator, FilterResult, Filterable};
use std::collections::{BTreeMap, Bound, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, AtomicUsize as AtomicId};
use std::sync::{Arc, Mutex};

/// A callback function that receives subscription updates
type Callback = Box<dyn Fn(Op, Vec<TestRecord>) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
struct Subscription {
    id: usize,
    predicate: Predicate,
    callbacks: Arc<Vec<Callback>>,
    fields: HashSet<String>, // Fields this subscription depends on
    matching_records: Arc<Mutex<HashSet<usize>>>, // Records currently matching this subscription
}

/// An index for a specific field and comparison operator
#[derive(Debug, Default)]
struct FieldIndex {
    // Map from value to subscription IDs that match that value
    eq: HashMap<String, Vec<usize>>,
    // Ordered list of (threshold, subscription IDs) for range queries
    // TODO: Consider using skip lists or B-trees for better range query performance
    gt: BTreeMap<String, Vec<usize>>,
    lt: BTreeMap<String, Vec<usize>>,
}

impl FieldIndex {
    fn add_subscription(&mut self, value: &str, op: &str, sub_id: usize) {
        match op {
            "=" => {
                self.eq.entry(value.to_string()).or_default().push(sub_id);
            }
            ">" => {
                self.gt.entry(value.to_string()).or_default().push(sub_id);
            }
            "<" => {
                self.lt.entry(value.to_string()).or_default().push(sub_id);
            }
            ">=" => {
                // x >= 5 is the same as x > 4
                if let Some(pred) = predecessor(value) {
                    self.gt.entry(pred).or_default().push(sub_id);
                } else {
                    // If there is no predecessor (e.g., value is ""), then this matches everything
                    self.gt.entry("".to_string()).or_default().push(sub_id);
                }
            }
            "<=" => {
                // x <= 5 is the same as x < 6
                self.lt.entry(successor(value)).or_default().push(sub_id);
            }
            _ => panic!("Unsupported operator: {}", op),
        }
    }

    fn find_matching_subscriptions(&self, value: &str) -> Vec<usize> {
        let mut result = HashSet::new();

        // Check exact matches
        if let Some(subs) = self.eq.get(value) {
            result.extend(subs);
        }

        // Check greater than matches (value > threshold)
        // We want all thresholds strictly less than our value
        for (_, subs) in self.gt.range(..value.to_string()) {
            result.extend(subs);
        }

        // Check less than matches (value < threshold)
        // We want all thresholds strictly greater than our value
        for (_, subs) in self
            .lt
            .range((Bound::Excluded(value.to_string()), Bound::Unbounded))
        {
            result.extend(subs);
        }

        let mut result: Vec<_> = result.into_iter().collect();
        result.sort_unstable(); // Make the order deterministic for testing
        result
    }
}

/// Represents a change in the record set
#[derive(Debug)]
pub enum Op {
    /// A record has been added or now matches the subscription query
    Add {
        id: usize,
        record: TestRecord,
        updates: Vec<Update>, // What changes caused this record to be added to the subscription
    },
    /// A record no longer matches the subscription query
    Remove {
        id: usize,
        old_record: TestRecord,
        current_record: TestRecord,
        updates: Vec<Update>, // What changes caused this record to be removed from the subscription
    },
    /// A record was updated and still matches the subscription query
    Edit {
        id: usize,
        old_record: TestRecord,
        new_record: TestRecord,
        updates: Vec<Update>,
    },
}

#[derive(Debug, Clone)]
struct Update(String, String);

#[derive(Debug, Clone)]
pub struct TestRecord {
    pub name: String,
    pub age: String,
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

/// A server that maintains a set of records and notifies subscribers of changes
pub struct FakeServer {
    state: HashMap<usize, TestRecord>,
    next_id: AtomicId,
    next_sub_id: AtomicId,
    field_indexes: HashMap<String, FieldIndex>,
    subscriptions: HashMap<usize, Arc<Subscription>>,
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle<'a> {
    predicate: Predicate,
    fields: HashSet<String>,
    server: &'a mut FakeServer,
}

impl FakeServer {
    /// Creates a new server with initial records
    pub fn new(initial_records: Vec<TestRecord>) -> Self {
        let mut state = HashMap::new();
        for record in initial_records {
            state.insert(0, record);
        }
        Self {
            state,
            next_id: AtomicId::new(1),
            next_sub_id: AtomicId::new(0),
            field_indexes: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    /// Creates a new subscription for records matching the given query
    pub fn subscribe(&mut self, query: &str) -> SubscriptionHandle {
        let predicate = parse_selection(query).unwrap();
        let fields = Self::extract_fields(&predicate);
        println!("Subscription depends on fields: {:?}", fields);
        SubscriptionHandle {
            predicate,
            fields,
            server: self,
        }
    }

    fn extract_field_comparisons(
        predicate: &Predicate,
    ) -> Vec<(&str, &ComparisonOperator, &Literal)> {
        let mut comparisons = Vec::new();
        match predicate {
            Predicate::Comparison {
                left,
                operator,
                right,
            } => {
                if let (Expr::Identifier(field), Expr::Literal(value)) =
                    (left.as_ref(), right.as_ref())
                {
                    match field {
                        Identifier::Property(name) => {
                            comparisons.push((name.as_str(), operator, value));
                        }
                        Identifier::CollectionProperty(_, name) => {
                            comparisons.push((name.as_str(), operator, value));
                        }
                    }
                }
            }
            Predicate::And(left, right) => {
                comparisons.extend(Self::extract_field_comparisons(left));
                comparisons.extend(Self::extract_field_comparisons(right));
            }
            _ => {}
        }
        comparisons
    }

    fn extract_fields(predicate: &Predicate) -> HashSet<String> {
        let mut fields = HashSet::new();
        for (field, _, _) in Self::extract_field_comparisons(predicate) {
            fields.insert(field.to_string());
        }
        fields
    }

    fn add_to_indexes(&mut self, sub_id: usize, predicate: &str) {
        // Parse predicate to extract field comparisons
        // For this example, we'll just handle simple age comparisons
        if predicate.contains("age") {
            let field_index = self.field_indexes.entry("age".to_string()).or_default();

            if predicate.contains(">=") {
                let value = predicate.split(">=").nth(1).unwrap().trim();
                field_index.add_subscription(value, ">=", sub_id);
            } else if predicate.contains("<=") {
                let value = predicate.split("<=").nth(1).unwrap().trim();
                field_index.add_subscription(value, "<=", sub_id);
            }
        }
    }

    fn find_matching_subscriptions(&self, record: &TestRecord) -> Vec<Arc<Subscription>> {
        let mut result = HashSet::new();

        // Check field indexes
        if let Some(index) = self.field_indexes.get("age") {
            result.extend(index.find_matching_subscriptions(&record.age));
        }

        result
            .into_iter()
            .filter_map(|id| self.subscriptions.get(&id))
            .cloned()
            .collect()
    }

    /// Inserts a new record and notifies relevant subscribers
    pub fn insert(&mut self, record: TestRecord) -> usize {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create updates for all fields since this is a new record
        let updates = vec![
            Update("name".to_string(), record.name.clone()),
            Update("age".to_string(), record.age.clone()),
        ];

        // Insert the record
        self.state.insert(id, record.clone());
        println!("➕ Inserted record {id}: {record:?}");

        // Find and notify affected subscriptions
        let affected_subs: Vec<_> = self
            .subscriptions
            .iter()
            .filter(|(_, sub)| {
                FilterIterator::new(vec![record.clone()].into_iter(), sub.predicate.clone())
                    .next()
                    .map(|r| matches!(r, FilterResult::Pass(_)))
                    .unwrap_or(false)
            })
            .collect();

        if !affected_subs.is_empty() {
            println!("   Notifying {} subscription(s)", affected_subs.len());
        }

        // Notify affected subscriptions
        for (_sub_id, sub) in affected_subs {
            let mut matching_records = sub.matching_records.lock().unwrap();
            matching_records.insert(id);

            let state: Vec<_> = matching_records
                .iter()
                .filter_map(|id| self.state.get(id))
                .cloned()
                .collect();

            for callback in sub.callbacks.iter() {
                callback(
                    Op::Add {
                        id,
                        record: record.clone(),
                        updates: updates.clone(),
                    },
                    state.clone(),
                );
            }
        }

        id
    }

    /// Updates an existing record and notifies relevant subscribers
    pub fn edit(&mut self, id: usize, updates: Vec<Update>) {
        if let Some(old_record) = self.state.get(&id).cloned() {
            // Apply updates to create new record
            let mut new_record = old_record.clone();
            let mut changed_fields = HashSet::new();

            for Update(field, value) in &updates {
                changed_fields.insert(field.clone());
                match field.as_str() {
                    "name" => new_record.name = value.clone(),
                    "age" => new_record.age = value.clone(),
                    _ => panic!("Unknown field: {}", field),
                }
            }

            // Update state
            self.state.insert(id, new_record.clone());
            println!("✏️  Editing record {id}:");
            println!("   Before: {old_record:?}");
            println!("   After:  {new_record:?}");
            println!(
                "   Changes: {}",
                updates
                    .iter()
                    .map(|Update(f, v)| format!("{f}={v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            // Find affected subscriptions
            let affected_subs: Vec<_> = self
                .subscriptions
                .iter()
                .filter(|(_, sub)| {
                    // A subscription is affected if:
                    // 1. Any of its dependent fields changed OR
                    // 2. The record currently matches the subscription
                    let fields = &sub.fields;
                    fields.intersection(&changed_fields).next().is_some()
                        || sub.matching_records.lock().unwrap().contains(&id)
                })
                .collect();

            if !affected_subs.is_empty() {
                println!("   Notifying {} subscription(s)", affected_subs.len());
            }

            // Apply updates to each affected subscription
            for (sub_id, sub) in affected_subs {
                let was_matching = {
                    let matching = sub.matching_records.lock().unwrap();
                    matching.contains(&id)
                };

                let now_matching = FilterIterator::new(
                    vec![new_record.clone()].into_iter(),
                    sub.predicate.clone(),
                )
                .next()
                .map(|r| matches!(r, FilterResult::Pass(_)))
                .unwrap_or(false);

                if was_matching != now_matching {
                    let mut matching_records = sub.matching_records.lock().unwrap();
                    if now_matching {
                        println!("   Sub {sub_id}: Record now matches subscription");
                        matching_records.insert(id);
                    } else {
                        println!("   Sub {sub_id}: Record no longer matches subscription");
                        matching_records.remove(&id);
                    }
                } else if was_matching {
                    println!("   Sub {sub_id}: Record updated (still matches)");
                }

                // Get current state for callback
                let state: Vec<TestRecord> = sub
                    .matching_records
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|id| self.state.get(id))
                    .cloned()
                    .collect();

                // Notify callbacks
                for callback in sub.callbacks.iter() {
                    let op = match (was_matching, now_matching) {
                        (true, false) => Op::Remove {
                            id,
                            old_record: old_record.clone(),
                            current_record: new_record.clone(),
                            updates: updates.clone(),
                        },
                        (false, true) => Op::Add {
                            id,
                            record: new_record.clone(),
                            updates: updates.clone(),
                        },
                        (true, true) => Op::Edit {
                            id,
                            old_record: old_record.clone(),
                            new_record: new_record.clone(),
                            updates: updates.clone(),
                        },
                        (false, false) => continue,
                    };
                    callback(op, state.clone());
                }
            }
        }
    }

    fn get_matching_records(&self, _record: &TestRecord) -> Vec<TestRecord> {
        self.state.values().cloned().collect()
    }
}

impl<'a> SubscriptionHandle<'a> {
    pub fn subscribe<F>(self, callback: F) -> Self
    where
        F: Fn(Op, Vec<TestRecord>) + Send + Sync + 'static,
    {
        let sub_id = self
            .server
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create subscription with empty matching records
        let subscription = Arc::new(Subscription {
            id: sub_id,
            predicate: self.predicate.clone(),
            callbacks: Arc::new(vec![Box::new(callback)]),
            fields: self.fields.clone(),
            matching_records: Arc::new(Mutex::new(HashSet::new())),
        });

        // Store subscription
        self.server
            .subscriptions
            .insert(sub_id, subscription.clone());

        // Find initial matching records
        let mut matching_records = subscription.matching_records.lock().unwrap();
        for (id, record) in &self.server.state {
            if FilterIterator::new(vec![record.clone()].into_iter(), self.predicate.clone())
                .next()
                .map(|r| matches!(r, FilterResult::Pass(_)))
                .unwrap_or(false)
            {
                matching_records.insert(*id);
            }
        }

        // Send initial state
        let initial_records: Vec<_> = matching_records
            .iter()
            .filter_map(|id| self.server.state.get(id))
            .cloned()
            .collect();

        if !initial_records.is_empty() {
            for callback in subscription.callbacks.iter() {
                callback(
                    Op::Add {
                        id: *matching_records.iter().next().unwrap(),
                        record: initial_records[0].clone(),
                        updates: vec![],
                    },
                    initial_records.clone(),
                );
            }
        }

        self
    }
}

/// Example usage
fn main() {
    println!("🔄 Starting pubsub example...\n");

    // Create a server with one initial record
    let mut server = FakeServer::new(vec![TestRecord {
        name: "Alice".to_string(),
        age: "36".to_string(),
    }]);

    // Subscribe to records for people who can rent a car (25-90)
    println!("👀 Creating subscription for rental age (25-90)");
    let subscription = server.subscribe("age >= 25 AND age <= 90");
    let expected = Arc::new(AtomicUsize::new(1));
    let expected_for_closure = expected.clone();

    let _subscription = subscription.subscribe(move |op, state| {
        match &op {
            Op::Add {
                record, updates, ..
            } => {
                println!("   ➕ Added: {record:?}");
                if !updates.is_empty() {
                    println!(
                        "      Due to: {}",
                        updates
                            .iter()
                            .map(|Update(f, v)| format!("{f}={v}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
            Op::Remove {
                old_record,
                updates,
                ..
            } => {
                println!("   ➖ Removed: {old_record:?}");
                println!(
                    "      Due to: {}",
                    updates
                        .iter()
                        .map(|Update(f, v)| format!("{f}={v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            Op::Edit {
                old_record,
                new_record,
                updates,
                ..
            } => {
                println!("   ✏️  Updated:");
                println!("      From: {old_record:?}");
                println!("      To:   {new_record:?}");
                println!(
                    "      Changes: {}",
                    updates
                        .iter()
                        .map(|Update(f, v)| format!("{f}={v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
        println!("   Current matches: {state:?}\n");
        assert_eq!(
            state.len(),
            expected_for_closure.load(std::sync::atomic::Ordering::Relaxed)
        );
    });

    println!("\n📝 Running test sequence...\n");

    // Add Bob (too young to rent)
    let id = server.insert(TestRecord {
        name: "Bob".to_string(),
        age: "22".to_string(),
    });

    // Change Bob's name while too young - should see no updates
    server.edit(id, vec![Update("name".to_string(), "Robert".to_string())]);

    // Bob/Robert turns 25 (can now rent)
    expected.store(2, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, vec![Update("age".to_string(), "25".to_string())]);

    // Robert changes name back to Bob while age matches - should see an edit
    server.edit(id, vec![Update("name".to_string(), "Bob".to_string())]);

    // Bob turns 26 (still can rent)
    server.edit(id, vec![Update("age".to_string(), "26".to_string())]);

    // Bob changes name to Robert while age matches - should see an edit
    server.edit(id, vec![Update("name".to_string(), "Robert".to_string())]);

    // Robert turns 91 (too old to rent)
    expected.store(1, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, vec![Update("age".to_string(), "91".to_string())]);

    // Changes name back to Bob while too old - should see no updates
    server.edit(id, vec![Update("name".to_string(), "Bob".to_string())]);

    println!("✅ Test sequence complete!");
}

// TODO: Implement proper binary collation that handles:
// - Full UTF-8 range
// - Correct lexicographic ordering
// - Efficient successor/predecessor without string scanning
// For now, we use simple string operations that work for ASCII

fn successor(s: &str) -> String {
    format!("{}.", s) // Simple hack: append a dot which is greater than most chars
}

fn predecessor(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s[..s.len() - 1].to_string()) // Simple hack: remove last char
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_successor() {
        assert!(successor("abc") > "abc".to_string());
        assert!(successor("") > "".to_string());
    }

    #[test]
    fn test_predecessor() {
        assert!(predecessor("abc").unwrap() < "abc".to_string());
        assert_eq!(predecessor(""), None);
    }

    #[test]
    fn test_field_index() {
        let mut index = FieldIndex::default();

        // Add some subscriptions
        index.add_subscription("25", ">=", 1); // age >= 25
        index.add_subscription("90", "<=", 1); // age <= 90
        index.add_subscription("30", "=", 2); // age == 30

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
        let mut index = FieldIndex::default();

        // Test greater than
        index.add_subscription("25", ">", 1);
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
        index.add_subscription("25", "<", 2);
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
        index.add_subscription("25", ">=", 3);
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
        index.add_subscription("25", "<=", 4);
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
