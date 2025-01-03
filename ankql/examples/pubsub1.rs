use ankql::ast::Predicate;
use ankql::parser::parse_selection;
use ankql::selection::filter::{FilterIterator, FilterResult, Filterable};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, AtomicUsize as AtomicId};
use std::sync::Arc;

/// A callback function that receives subscription updates
type Callback = Box<dyn Fn(Op, Vec<TestRecord>)>;

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

/// A simple record type for demonstration
#[derive(Debug, Clone, PartialEq)]
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
    subscriptions: Vec<(Predicate, Vec<Callback>)>,
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle<'a> {
    predicate: Predicate,
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
            subscriptions: Vec::new(),
        }
    }

    /// Creates a new subscription for records matching the given query
    pub fn subscribe(&mut self, query: &str) -> SubscriptionHandle {
        let predicate = parse_selection(query).unwrap();
        SubscriptionHandle {
            predicate,
            server: self,
        }
    }

    /// Inserts a new record and notifies relevant subscribers
    pub fn insert(&mut self, record: TestRecord) -> usize {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.state.insert(id, record.clone());

        self.notify_matching_subscribers(id, &record, |id, record| Op::Add { id, record });
        id
    }

    /// Updates an existing record and notifies relevant subscribers
    pub fn edit(&mut self, id: usize, new_record: TestRecord) {
        if let Some(old_record) = self.state.insert(id, new_record.clone()) {
            self.handle_record_change(id, old_record, new_record);
        }
    }

    // Helper methods
    fn notify_matching_subscribers<F>(&self, id: usize, record: &TestRecord, op_fn: F)
    where
        F: Fn(usize, TestRecord) -> Op,
    {
        for (predicate, callbacks) in &self.subscriptions {
            if self.matches_predicate(record, predicate) {
                let matching = self.get_matching_records(predicate);
                for callback in callbacks {
                    callback(op_fn(id, record.clone()), matching.clone());
                }
            }
        }
    }

    fn handle_record_change(&self, id: usize, old_record: TestRecord, new_record: TestRecord) {
        for (predicate, callbacks) in &self.subscriptions {
            let old_matches = self.matches_predicate(&old_record, predicate);
            let new_matches = self.matches_predicate(&new_record, predicate);

            let matching = self.get_matching_records(predicate);
            for callback in callbacks {
                let op = match (old_matches, new_matches) {
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
                callback(op, matching.clone());
            }
        }
    }

    fn matches_predicate(&self, record: &TestRecord, predicate: &Predicate) -> bool {
        FilterIterator::new(vec![record.clone()].into_iter(), predicate.clone())
            .next()
            .map(|r| matches!(r, FilterResult::Pass(_)))
            .unwrap_or(false)
    }

    fn get_matching_records(&self, predicate: &Predicate) -> Vec<TestRecord> {
        FilterIterator::new(self.state.values().cloned(), predicate.clone())
            .filter_map(|r| match r {
                FilterResult::Pass(record) => Some(record),
                _ => None,
            })
            .collect()
    }
}

impl SubscriptionHandle<'_> {
    /// Registers a callback to be called when matching records change
    pub fn subscribe<F>(self, callback: F) -> Self
    where
        F: Fn(Op, Vec<TestRecord>) + 'static,
    {
        // Send initial state if any records match
        let matching = self.server.get_matching_records(&self.predicate);
        if !matching.is_empty() {
            callback(
                Op::Add {
                    id: 0,
                    record: matching[0].clone(),
                },
                matching.clone(),
            );
        }

        // Register for future updates
        for (pred, callbacks) in &mut self.server.subscriptions {
            if *pred == self.predicate {
                callbacks.push(Box::new(callback));
                return self;
            }
        }
        self.server
            .subscriptions
            .push((self.predicate.clone(), vec![Box::new(callback)]));
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

    // Bob turns 25 (can now rent)
    expected.store(2, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, TestRecord {
        name: "Bob".to_string(),
        age: "25".to_string(),
    });

    // Bob turns 26 (still can rent)
    server.edit(id, TestRecord {
        name: "Bob".to_string(),
        age: "26".to_string(),
    });

    // Bob turns 91 (too old to rent)
    expected.store(1, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, TestRecord {
        name: "Bob".to_string(),
        age: "91".to_string(),
    });
}
