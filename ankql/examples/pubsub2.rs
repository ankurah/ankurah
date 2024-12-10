use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use ankql::parser::parse_selection;
use ankql::selection::filter::{FilterIterator, FilterResult, Filterable};
use std::collections::{HashMap, HashSet};
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
#[derive(Default)]
struct FieldIndex {
    // Map from value to subscription IDs that match that value
    eq: HashMap<String, Vec<usize>>,
    // Ordered list of (threshold, subscription IDs) for range queries
    gt: Vec<(String, Vec<usize>)>,
    lt: Vec<(String, Vec<usize>)>,
    gte: Vec<(String, Vec<usize>)>,
    lte: Vec<(String, Vec<usize>)>,
}

impl FieldIndex {
    fn add_subscription(&mut self, value: &str, op: &ComparisonOperator, sub_id: usize) {
        match op {
            ComparisonOperator::Equal => {
                self.eq.entry(value.to_string()).or_default().push(sub_id);
            }
            ComparisonOperator::GreaterThan => {
                self.gt.push((value.to_string(), vec![sub_id]));
                self.gt.sort_by(|a, b| a.0.cmp(&b.0));
            }
            ComparisonOperator::LessThan => {
                self.lt.push((value.to_string(), vec![sub_id]));
                self.lt.sort_by(|a, b| a.0.cmp(&b.0));
            }
            ComparisonOperator::GreaterThanOrEqual => {
                self.gte.push((value.to_string(), vec![sub_id]));
                self.gte.sort_by(|a, b| a.0.cmp(&b.0));
            }
            ComparisonOperator::LessThanOrEqual => {
                self.lte.push((value.to_string(), vec![sub_id]));
                self.lte.sort_by(|a, b| a.0.cmp(&b.0));
            }
            _ => unimplemented!("Only basic comparison operators are supported"),
        }
    }

    fn find_matching_subscriptions(&self, field: &str, value: &str) -> Vec<usize> {
        let mut matches = Vec::new();

        // For age field, convert strings to integers for comparison
        if field == "age" {
            if let (Ok(value_int), true) = (
                value.parse::<i32>(),
                !self.eq.is_empty()
                    || !self.gt.is_empty()
                    || !self.lt.is_empty()
                    || !self.gte.is_empty()
                    || !self.lte.is_empty(),
            ) {
                println!("  Comparing age as integer: {}", value_int);

                // Check exact matches
                for (threshold, subs) in &self.eq {
                    if let Ok(threshold_int) = threshold.parse::<i32>() {
                        if value_int == threshold_int {
                            matches.extend(subs);
                        }
                    }
                }

                // Check range matches
                for (threshold, subs) in &self.gt {
                    if let Ok(threshold_int) = threshold.parse::<i32>() {
                        if value_int > threshold_int {
                            matches.extend(subs);
                        }
                    }
                }
                for (threshold, subs) in &self.lt {
                    if let Ok(threshold_int) = threshold.parse::<i32>() {
                        if value_int < threshold_int {
                            matches.extend(subs);
                        }
                    }
                }
                for (threshold, subs) in &self.gte {
                    if let Ok(threshold_int) = threshold.parse::<i32>() {
                        if value_int >= threshold_int {
                            matches.extend(subs);
                        }
                    }
                }
                for (threshold, subs) in &self.lte {
                    if let Ok(threshold_int) = threshold.parse::<i32>() {
                        if value_int <= threshold_int {
                            matches.extend(subs);
                        }
                    }
                }
            }
        } else {
            // For non-age fields, use string comparison
            if let Some(subs) = self.eq.get(value) {
                matches.extend(subs);
            }

            for (threshold, subs) in &self.gt {
                if value > threshold {
                    matches.extend(subs);
                }
            }
            for (threshold, subs) in &self.lt {
                if value < threshold {
                    matches.extend(subs);
                }
            }
            for (threshold, subs) in &self.gte {
                if value >= threshold {
                    matches.extend(subs);
                }
            }
            for (threshold, subs) in &self.lte {
                if value <= threshold {
                    matches.extend(subs);
                }
            }
        }

        matches
    }
}

/// Represents a change in the record set
#[derive(Debug)]
pub enum Op {
    /// A record has been added or now matches the subscription query
    Add { id: usize, record: TestRecord },
    /// A record no longer matches the subscription query
    Remove {
        id: usize,
        old_record: TestRecord,     // The last record that matched
        current_record: TestRecord, // The current record that doesn't match
    },
    /// A record was updated and still matches the subscription query
    Edit {
        id: usize,
        old_record: TestRecord,
        new_record: TestRecord,
    },
}

#[derive(Debug, Clone)]
struct Update(&'static str, &'static str);

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

    fn add_to_indexes(&mut self, sub_id: usize, predicate: &Predicate) {
        println!("Adding subscription {} to indexes", sub_id);
        for (field, op, value) in Self::extract_field_comparisons(predicate) {
            println!("  Field comparison: {} {:?} {:?}", field, op, value);
            let index = self.field_indexes.entry(field.to_string()).or_default();
            let value_str = match value {
                Literal::String(s) => s.clone(),
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::Boolean(b) => b.to_string(),
            };
            index.add_subscription(&value_str, op, sub_id);
        }
    }

    fn find_matching_subscriptions(&self, record: &TestRecord) -> Vec<Arc<Subscription>> {
        println!("Finding matches for record: {:?}", record);
        let mut candidate_ids = HashSet::new();
        let mut first = true;

        // Check each field's index
        for (field, value) in [("name", &record.name), ("age", &record.age)] {
            if let Some(index) = self.field_indexes.get(field) {
                let ids = index.find_matching_subscriptions(field, value);
                println!(
                    "  Field '{}' with value '{}' matched subscription IDs: {:?}",
                    field, value, ids
                );
                if first {
                    candidate_ids = ids.into_iter().collect();
                    first = false;
                } else {
                    candidate_ids.retain(|id| ids.contains(id));
                }
            }
        }

        println!("  Candidate IDs after field matching: {:?}", candidate_ids);

        // Get the actual subscriptions and verify with full predicate
        let result: Vec<Arc<Subscription>> = candidate_ids
            .into_iter()
            .filter_map(|id| self.subscriptions.get(&id))
            .filter(|sub| {
                let matches =
                    FilterIterator::new(vec![record.clone()].into_iter(), sub.predicate.clone())
                        .next()
                        .map(|r| matches!(r, FilterResult::Pass(_)))
                        .unwrap_or(false);
                println!(
                    "  Subscription {} full predicate match: {}",
                    sub.id, matches
                );
                matches
            })
            .cloned()
            .collect();

        println!("  Final matching subscriptions: {}", result.len());
        result
    }

    /// Inserts a new record and notifies relevant subscribers
    pub fn insert(&mut self, record: TestRecord) -> usize {
        println!("\nInserting record: {:?}", record);
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.state.insert(id, record.clone());

        // Check all subscriptions (for insert we need to check all since it's new)
        for sub in self.subscriptions.values() {
            let matches =
                FilterIterator::new(vec![record.clone()].into_iter(), sub.predicate.clone())
                    .next()
                    .map(|r| matches!(r, FilterResult::Pass(_)))
                    .unwrap_or(false);

            if matches {
                let mut matching_records = sub.matching_records.lock().unwrap();
                matching_records.insert(id);

                let current_records: Vec<_> = matching_records
                    .iter()
                    .filter_map(|id| self.state.get(id))
                    .cloned()
                    .collect();

                for callback in sub.callbacks.iter() {
                    callback(
                        Op::Add {
                            id,
                            record: record.clone(),
                        },
                        current_records.clone(),
                    );
                }
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

            for Update(field, value) in updates {
                changed_fields.insert(field.to_string());
                match field {
                    "name" => new_record.name = value.to_string(),
                    "age" => new_record.age = value.to_string(),
                    _ => panic!("Unknown field: {}", field),
                }
            }

            // Update state
            self.state.insert(id, new_record.clone());

            println!("\nEditing record {id}: {new_record:?}");
            println!("Changed fields: {:?}", changed_fields);

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

            println!("Affected subscriptions: {}", affected_subs.len());

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

                println!(
                    "  Subscription {sub_id}: was_matching={was_matching}, now_matching={now_matching}"
                );

                if was_matching != now_matching {
                    let mut matching_records = sub.matching_records.lock().unwrap();
                    if now_matching {
                        matching_records.insert(id);
                    } else {
                        matching_records.remove(&id);
                    }
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
                        },
                        (false, true) => Op::Add {
                            id,
                            record: new_record.clone(),
                        },
                        (true, true) => Op::Edit {
                            id,
                            old_record: old_record.clone(),
                            new_record: new_record.clone(),
                        },
                        (false, false) => continue,
                    };
                    callback(op, state.clone());
                }
            }
        }
    }

    fn get_matching_records(&self, record: &TestRecord) -> Vec<TestRecord> {
        let mut result = Vec::new();

        // Add all records that match the subscription's predicate
        for r in self.state.values() {
            if self.find_matching_subscriptions(r).len() > 0 {
                result.push(r.clone());
            }
        }

        result
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
    // Create a server with one initial record
    let mut server = FakeServer::new(vec![TestRecord {
        name: "Alice".to_string(),
        age: "36".to_string(),
    }]);

    // Subscribe to records for people who can rent a car (25-90)
    let subscription = server.subscribe("age >= 25 AND age <= 90");
    let expected = Arc::new(AtomicUsize::new(1));
    let expected_for_closure = expected.clone();

    let _subscription = subscription.subscribe(move |op, state| {
        println!("op: {op:?}");
        println!("people who can rent a car: {:?}", state);
        assert_eq!(
            state.len(),
            expected_for_closure.load(std::sync::atomic::Ordering::Relaxed)
        );
    });

    // Add Bob (too young to rent)
    let id = server.insert(TestRecord {
        name: "Bob".to_string(),
        age: "22".to_string(),
    });

    // Change Bob's name while too young - should see no updates
    server.edit(id, vec![Update("name", "Robert")]);

    // Bob/Robert turns 25 (can now rent)
    expected.store(2, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, vec![Update("age", "25")]);

    // Robert changes name back to Bob while age matches - should see an edit
    server.edit(id, vec![Update("name", "Bob")]);

    // Bob turns 26 (still can rent)
    server.edit(id, vec![Update("age", "26")]);

    // Bob changes name to Robert while age matches - should see an edit
    server.edit(id, vec![Update("name", "Robert")]);

    // Robert turns 91 (too old to rent)
    expected.store(1, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, vec![Update("age", "91")]);

    // Changes name back to Bob while too old - should see no updates
    server.edit(id, vec![Update("name", "Bob")]);
}
