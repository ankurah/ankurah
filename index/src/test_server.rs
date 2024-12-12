use std::{collections::HashMap, sync::atomic::AtomicUsize};

use crate::reactor::{Reactor, SubscriptionHandle};

#[derive(Debug, Clone)]
struct Update(String, String);

#[derive(Debug, Clone)]
pub struct TestRecord {
    pub name: String,
    pub age: String,
}

impl ankql::selection::filter::Filterable for TestRecord {
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
    records: HashMap<usize, TestRecord>,
    next_record_id: AtomicUsize,
    reactor: Reactor,
}

impl FakeServer {
    /// Creates a new server with initial records
    pub fn new(initial_records: Vec<TestRecord>) -> Self {
        let mut state = HashMap::new();
        for record in initial_records {
            state.insert(0, record);
        }
        Self {
            records: state,
            next_record_id: AtomicId::new(1),
            next_sub_id: AtomicId::new(0),
            reactor,
        }
    }

    /// Creates a new subscription for records matching the given query
    pub fn subscribe(&mut self, query: &str) -> SubscriptionHandle {
        let predicate = ankql::parser::parse_selection(query).unwrap();
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
                let state: Vec<TestRecord> = sub
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

    fn get_matching_records(&self, _record: &TestRecord) -> Vec<TestRecord> {
        self.records.values().cloned().collect()
    }
}
