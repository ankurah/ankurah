use ankql::ast::Predicate;
use ankql::parser::parse_selection;
use ankql::selection::filter::{FilterIterator, FilterResult, Filterable};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, AtomicUsize as AtomicId};
use std::sync::Arc;

/// A callback function that receives subscription updates
type Callback = Box<dyn Fn(Op, Vec<TestItem>)>;

/// Represents a change in the result set
#[derive(Debug)]
pub enum Op {
    /// An item has been added or now matches the subscription query
    Add { id: usize, item: TestItem },
    /// An item no longer matches the subscription query
    Remove {
        id: usize,
        old_item: TestItem,     // The last item that matched
        current_item: TestItem, // The current item that doesn't match
    },
    /// An item was updated and still matches the subscription query
    Edit { id: usize, old_item: TestItem, new_item: TestItem },
}

/// A simple item type for demonstration
#[derive(Debug, Clone, PartialEq)]
pub struct TestItem {
    pub name: String,
    pub age: String,
}

impl Filterable for TestItem {
    fn collection(&self) -> &str { "users" }

    fn value(&self, name: &str) -> Option<String> {
        match name {
            "name" => Some(self.name.clone()),
            "age" => Some(self.age.clone()),
            _ => None,
        }
    }
}

/// A server that maintains a set of items and notifies subscribers of changes
pub struct FakeServer {
    state: HashMap<usize, TestItem>,
    next_id: AtomicId,
    subscriptions: Vec<(Predicate, Vec<Callback>)>,
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle<'a> {
    predicate: Predicate,
    server: &'a mut FakeServer,
}

impl FakeServer {
    /// Creates a new server with initial items
    pub fn new(initial_items: Vec<TestItem>) -> Self {
        let mut state = HashMap::new();
        for item in initial_items {
            state.insert(0, item);
        }
        Self { state, next_id: AtomicId::new(1), subscriptions: Vec::new() }
    }

    /// Creates a new subscription for items matching the given query
    pub fn subscribe(&mut self, query: &str) -> SubscriptionHandle {
        let predicate = parse_selection(query).unwrap();
        SubscriptionHandle { predicate, server: self }
    }

    /// Inserts a new item and notifies relevant subscribers
    pub fn insert(&mut self, item: TestItem) -> usize {
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.state.insert(id, item.clone());

        self.notify_matching_subscribers(id, &item, |id, item| Op::Add { id, item });
        id
    }

    /// Updates an existing item and notifies relevant subscribers
    pub fn edit(&mut self, id: usize, new_item: TestItem) {
        if let Some(old_item) = self.state.insert(id, new_item.clone()) {
            self.handle_item_change(id, old_item, new_item);
        }
    }

    // Helper methods
    fn notify_matching_subscribers<F>(&self, id: usize, item: &TestItem, op_fn: F)
    where F: Fn(usize, TestItem) -> Op {
        for (predicate, callbacks) in &self.subscriptions {
            if self.matches_predicate(item, predicate) {
                let matching = self.get_matching_items(predicate);
                for callback in callbacks {
                    callback(op_fn(id, item.clone()), matching.clone());
                }
            }
        }
    }

    fn handle_item_change(&self, id: usize, old_item: TestItem, new_item: TestItem) {
        for (predicate, callbacks) in &self.subscriptions {
            let old_matches = self.matches_predicate(&old_item, predicate);
            let new_matches = self.matches_predicate(&new_item, predicate);

            let matching = self.get_matching_items(predicate);
            for callback in callbacks {
                let op = match (old_matches, new_matches) {
                    (true, false) => Op::Remove { id, old_item: old_item.clone(), current_item: new_item.clone() },
                    (false, true) => Op::Add { id, item: new_item.clone() },
                    (true, true) => Op::Edit { id, old_item: old_item.clone(), new_item: new_item.clone() },
                    (false, false) => continue,
                };
                callback(op, matching.clone());
            }
        }
    }

    fn matches_predicate(&self, item: &TestItem, predicate: &Predicate) -> bool {
        FilterIterator::new(vec![item.clone()].into_iter(), predicate.clone())
            .next()
            .map(|r| matches!(r, FilterResult::Pass(_)))
            .unwrap_or(false)
    }

    fn get_matching_items(&self, predicate: &Predicate) -> Vec<TestItem> {
        FilterIterator::new(self.state.values().cloned(), predicate.clone())
            .filter_map(|r| match r {
                FilterResult::Pass(item) => Some(item),
                _ => None,
            })
            .collect()
    }
}

impl SubscriptionHandle<'_> {
    /// Registers a callback to be called when matching items change
    pub fn subscribe<F>(self, callback: F) -> Self
    where F: Fn(Op, Vec<TestItem>) + 'static {
        // Send initial state if any items match
        let matching = self.server.get_matching_items(&self.predicate);
        if !matching.is_empty() {
            callback(Op::Add { id: 0, item: matching[0].clone() }, matching.clone());
        }

        // Register for future updates
        for (pred, callbacks) in &mut self.server.subscriptions {
            if *pred == self.predicate {
                callbacks.push(Box::new(callback));
                return self;
            }
        }
        self.server.subscriptions.push((self.predicate.clone(), vec![Box::new(callback)]));
        self
    }
}

/// Example usage
fn main() {
    // Create a server with one initial item
    let mut server = FakeServer::new(vec![TestItem { name: "Alice".to_string(), age: "36".to_string() }]);

    // Subscribe to items for people who can rent a car (25-90)
    let subscription = server.subscribe("age >= 25 AND age <= 90");
    let expected = Arc::new(AtomicUsize::new(1));
    let expected_for_closure = expected.clone();

    let _subscription = subscription.subscribe(move |op, state| {
        println!("op: {op:?}");
        println!("people who can rent a car: {:?}", state);
        assert_eq!(state.len(), expected_for_closure.load(std::sync::atomic::Ordering::Relaxed));
    });

    // Add Bob (too young to rent)
    let id = server.insert(TestItem { name: "Bob".to_string(), age: "22".to_string() });

    // Bob turns 25 (can now rent)
    expected.store(2, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, TestItem { name: "Bob".to_string(), age: "25".to_string() });

    // Bob turns 26 (still can rent)
    server.edit(id, TestItem { name: "Bob".to_string(), age: "26".to_string() });

    // Bob turns 91 (too old to rent)
    expected.store(1, std::sync::atomic::Ordering::Relaxed);
    server.edit(id, TestItem { name: "Bob".to_string(), age: "91".to_string() });
}
