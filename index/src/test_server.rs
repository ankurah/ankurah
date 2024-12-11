use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

use ankql::selection::filter::FilterIterator;

use crate::{
    operation::Operation,
    reactor::{Reactor, StorageEngine, SubscriptionHandle},
};

#[derive(Debug, Clone)]
struct Update(String, String);

/// A super basic record that we can use for test cases
#[derive(Debug, Clone)]
pub struct TestModel {
    pub name: String,
    pub age: String,
}
#[derive(Debug, Clone)]
pub struct TestRecord {
    pub id: usize,
    pub model: TestModel,
}

impl crate::reactor::Record<TestRecord> for TestRecord {
    type Id = usize;
    fn id(&self) -> Self::Id {
        self.id
    }
}

impl ankql::selection::filter::Filterable for &TestRecord {
    fn collection(&self) -> &str {
        "users"
    }

    fn value(&self, name: &str) -> Option<String> {
        match name {
            "name" => Some(self.model.name.clone()),
            "age" => Some(self.model.age.clone()),
            _ => None,
        }
    }
}

/// A very basic server that we can use for test cases
pub struct TestServer {
    // Storing the record id redundantly for now
    storage: Arc<TestStorageEngine>,
    reactor: Reactor<TestStorageEngine, TestRecord>,
}

pub struct TestStorageEngine {
    records: Mutex<HashMap<usize, TestRecord>>,
    next_record_id: AtomicUsize,
}

impl StorageEngine<TestRecord> for TestStorageEngine {
    type Id = usize;
    type Update = Update;

    fn fetch_records(&self, predicate: &ankql::ast::Predicate) -> Vec<TestRecord> {
        self.fetch_records(predicate)
    }
}

impl TestStorageEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::new(HashMap::new()),
            next_record_id: AtomicUsize::new(1),
        })
    }
    pub fn insert(&self, model: TestModel) -> usize {
        let id = self
            .next_record_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.records
            .lock()
            .unwrap()
            .insert(id, TestRecord { id, model });
        id
    }

    /// just brute force for now - we don't need indexes for a test server
    fn fetch_records(&self, predicate: &ankql::ast::Predicate) -> Vec<TestRecord> {
        use ankql::selection::filter::FilterResult;
        let records = self.records.lock().unwrap();
        let matching_records = FilterIterator::new(records.values(), predicate.clone());
        matching_records
            .filter_map(|r| {
                if let FilterResult::Pass(e) = r {
                    Some(e.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl TestServer {
    /// Creates a new server with initial records
    pub fn new(initial: Vec<TestModel>) -> Arc<Self> {
        let storage = TestStorageEngine::new();

        for model in initial {
            storage.insert(model);
        }

        let reactor = Reactor::new(storage.clone());
        Arc::new(Self { storage, reactor })
    }
    pub fn insert(&mut self, model: TestModel) -> usize {
        self.storage.insert(model)
    }
    /// Creates a new subscription for records matching the given query
    pub fn subscribe(
        &mut self,
        query: &str,
        callback: impl Fn(Operation<TestRecord, Update>, Vec<TestRecord>) + Send + Sync + 'static,
    ) -> SubscriptionHandle<TestStorageEngine, TestRecord> {
        let predicate = ankql::parser::parse_selection(query).unwrap();
        self.reactor.subscribe(&predicate, callback).unwrap()
    }

    // LEFT OFF HERE
    /// Inserts a new record and notifies relevant subscribers
    pub fn insert(&mut self, record: TestModel) -> usize {
        let id = self
            .next_record_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create updates for all fields since this is a new record
        let updates = vec![
            Update("name".to_string(), record.name.clone()),
            Update("age".to_string(), record.age.clone()),
        ];

        // Insert the record
        self.records.insert(id, record.clone());
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
                .filter_map(|id| self.records.get(id))
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
        if let Some(old_record) = self.records.get(&id).cloned() {
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
            self.records.insert(id, new_record.clone());
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
                let state: Vec<TestModel> = sub
                    .matching_records
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|id| self.records.get(id))
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
}
