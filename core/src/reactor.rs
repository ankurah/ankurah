use super::collation::Collatable;
use super::comparision_index::ComparisonIndex;
use crate::value::Value;
use ankql::ast;
use ankql::selection::filter::Filterable;
use anyhow::Result;
use dashmap::DashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);
impl From<&str> for FieldId {
    fn from(val: &str) -> Self {
        FieldId(val.to_string())
    }
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of records
pub struct Reactor<S, R>
where
    R: Record,
    S: TestStorageEngine<R>,
{
    /// Current subscriptions
    subscriptions: DashMap<SubscriptionId, Arc<Subscription<R, S::Update>>>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as changes)
    index_watchers: DashMap<FieldId, ComparisonIndex>,
    /// Index of subscriptions that presently match each record.
    /// This is used to quickly find all subscriptions that need to be notified when a record changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    record_watchers: DashMap<R::Id, Vec<SubscriptionId>>,
    /// Next subscription id
    next_sub_id: AtomicUsize,
    /// Weak reference to the server, which we need to perform the initial subscription setup
    storage: Arc<S>,
}

pub trait TestStorageEngine<R>
where
    R: Record,
{
    type Id: std::hash::Hash + std::cmp::Eq;
    type Update: Clone;
    fn fetch_records(&self, predicate: &ast::Predicate) -> Vec<R>;
}

pub trait Record: Filterable + Clone {
    type Id: std::hash::Hash + std::cmp::Eq + Copy + Clone;
    type Model;
    fn id(&self) -> Self::Id;
    fn value(&self, name: &str) -> Option<Value>;
}

impl<S, R> Reactor<S, R>
where
    R: Record,
    S: TestStorageEngine<R>,
{
    pub fn new(storage: Arc<S>) -> Arc<Self> {
        Arc::new(Self {
            subscriptions: DashMap::new(),
            index_watchers: DashMap::new(),
            record_watchers: DashMap::new(),
            next_sub_id: AtomicUsize::new(0),
            storage,
        })
    }
    pub fn subscribe<F>(
        self: &Arc<Self>,
        predicate: &ast::Predicate,
        callback: F,
    ) -> Result<SubscriptionHandle<S, R>>
    where
        F: Fn(ChangeSet<R, S::Update>) + Send + Sync + 'static,
    {
        let sub_id: SubscriptionId = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .into();

        // Start watching the relevant indexes
        self.add_index_watchers(sub_id, predicate);

        // Find initial matching records
        let matching_records = self.storage.fetch_records(predicate);

        // Set up record watchers
        for record in matching_records.iter() {
            self.record_watchers
                .entry(record.id())
                .or_default()
                .push(sub_id);
        }

        // Create subscription with initial matching records
        let subscription = Arc::new(Subscription {
            id: sub_id,
            predicate: predicate.clone(),
            callback: Arc::new(Box::new(callback)),
            matching_records: Mutex::new(matching_records.clone()),
        });

        // Store subscription
        self.subscriptions.insert(sub_id, subscription.clone());

        // Call callback with initial state
        (subscription.callback)(ChangeSet {
            changes: matching_records
                .into_iter()
                .map(|r| RecordChange {
                    record: r,
                    updates: vec![],
                    kind: RecordChangeKind::Add,
                })
                .collect(),
        });

        Ok(SubscriptionHandle {
            id: sub_id,
            reactor: self.clone(),
        })
    }

    fn recurse_predicate<F>(&self, predicate: &ast::Predicate, f: F)
    where
        F: Fn(
                dashmap::Entry<FieldId, ComparisonIndex>,
                FieldId,
                &ast::Literal,
                &ast::ComparisonOperator,
            ) + Copy,
    {
        use ankql::ast::{Expr, Identifier, Predicate};
        match predicate {
            Predicate::Comparison {
                left,
                operator,
                right,
            } => {
                if let (Expr::Identifier(field), Expr::Literal(literal))
                | (Expr::Literal(literal), Expr::Identifier(field)) = (&**left, &**right)
                {
                    let field_name = match field {
                        Identifier::Property(name) => name.clone(),
                        Identifier::CollectionProperty(_, name) => name.clone(),
                    };

                    let field_id = FieldId(field_name);
                    let entry = self.index_watchers.entry(field_id.clone());
                    f(entry, field_id, literal, operator);
                } else {
                    // warn!("Unsupported predicate: {:?}", predicate);
                }
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.recurse_predicate(left, f);
                self.recurse_predicate(right, f);
            }
            Predicate::Not(pred) => {
                self.recurse_predicate(pred, f);
            }
            Predicate::IsNull(_) => {}
        }
    }

    fn add_index_watchers(&self, sub_id: SubscriptionId, predicate: &ast::Predicate) {
        self.recurse_predicate(predicate, |entry, _field_id, literal, operator| {
            entry
                .or_default()
                .add((*literal).clone(), operator.clone(), sub_id);
        });
    }

    fn remove_index_watchers(&self, sub_id: SubscriptionId, predicate: &ast::Predicate) {
        self.recurse_predicate(
            predicate,
            |entry: dashmap::Entry<FieldId, ComparisonIndex>,
             _field_id,
             literal: &ast::Literal,
             operator| {
                if let dashmap::Entry::Occupied(mut index) = entry {
                    // use crate::collation::Collatable;
                    let literal = (*literal).clone();
                    index.get_mut().remove(literal, operator.clone(), sub_id);
                }
            },
        );
    }

    /// Remove a subscription and clean up its watchers
    fn unsubscribe(&self, sub_id: SubscriptionId) {
        if let Some((_, subscription)) = self.subscriptions.remove(&sub_id) {
            // Remove from index watchers
            self.remove_index_watchers(sub_id, &subscription.predicate);

            // Remove from record watchers using subscription's matching_records
            let matching = subscription.matching_records.lock().unwrap();
            for record in matching.iter() {
                if let Some(mut watchers) = self.record_watchers.get_mut(&record.id()) {
                    watchers.retain(|&id| id != sub_id);
                }
            }
        }
    }

    /// Update record watchers when a record's matching status changes
    fn update_record_watchers(&self, record: &R, matching: bool, sub_id: SubscriptionId) {
        if let Some(subscription) = self.subscriptions.get(&sub_id) {
            let mut records = subscription.matching_records.lock().unwrap();
            let mut watchers = self.record_watchers.entry(record.id()).or_default();

            // TODO - we can't just use the matching flag, because we need to know if the record was in the set before
            // or after calling notify_change
            let did_match = records.iter().any(|r| r.id() == record.id());
            match (did_match, matching) {
                (false, true) => {
                    records.push(record.clone());
                    watchers.push(sub_id);
                }
                (true, false) => {
                    records.retain(|r| r.id() != record.id());
                    watchers.retain(|&id| id != sub_id);
                }
                _ => {} // No change needed
            }
        }
    }

    /// Notify subscriptions about a record change
    pub fn notify_change(&self, record: R, updates: Vec<S::Update>, kind: RecordChangeKind) {
        let mut possibly_interested_subs = HashSet::new();

        // Find subscriptions that might be interested based on index watchers
        for index_ref in self.index_watchers.iter() {
            // Get the field value from the record
            if let Some(field_value) = Record::value(&record, &index_ref.key().0) {
                possibly_interested_subs.extend(index_ref.find_matching(field_value));
            }
        }

        // Check each possibly interested subscription with full predicate evaluation
        for sub_id in possibly_interested_subs {
            if let Some(subscription) = self.subscriptions.get(&sub_id) {
                // Use evaluate_predicate directly on the record instead of fetch_records
                let matches =
                    ankql::selection::filter::evaluate_predicate(&record, &subscription.predicate)
                        .unwrap_or(false);

                let did_match = subscription
                    .matching_records
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|r| r.id() == record.id());

                // Only update watchers and notify if matching status changed
                if matches != did_match {
                    self.update_record_watchers(&record, matches, sub_id);

                    (subscription.callback)(ChangeSet {
                        changes: vec![RecordChange {
                            record: record.clone(),
                            updates: updates.clone(),
                            kind: if matches {
                                RecordChangeKind::Add
                            } else {
                                RecordChangeKind::Remove
                            },
                        }],
                    });
                } else if matches && kind == RecordChangeKind::Edit {
                    // Record still matches but was edited
                    (subscription.callback)(ChangeSet {
                        changes: vec![RecordChange {
                            record: record.clone(),
                            updates: updates.clone(),
                            kind: RecordChangeKind::Edit,
                        }],
                    });
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecordChangeKind {
    Add,
    Remove,
    Edit,
}
/// Represents a change in the record set
#[derive(Debug, Clone)]
pub struct RecordChange<R, U> {
    #[allow(unused)]
    record: R,
    #[allow(unused)]
    updates: Vec<U>,
    #[allow(unused)]
    kind: RecordChangeKind,
}

/// A set of changes to the record set
#[derive(Debug, Clone)]
pub struct ChangeSet<R, U> {
    #[allow(unused)]
    changes: Vec<RecordChange<R, U>>,
    // other stuff later
}

/// A callback function that receives subscription updates
type Callback<R, U> = Box<dyn Fn(ChangeSet<R, U>) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription<R, U> {
    #[allow(unused)]
    id: SubscriptionId,
    predicate: ankql::ast::Predicate,
    callback: Arc<Callback<R, U>>,
    // Track which records currently match this subscription
    matching_records: Mutex<Vec<R>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(usize);
impl Deref for SubscriptionId {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl PartialEq<usize> for SubscriptionId {
    fn eq(&self, other: &usize) -> bool {
        self.0 == *other
    }
}
impl From<usize> for SubscriptionId {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle<S, R>
where
    S: TestStorageEngine<R>,
    R: Record,
{
    id: SubscriptionId,
    reactor: Arc<Reactor<S, R>>,
}

impl<S, R> Drop for SubscriptionHandle<S, R>
where
    S: TestStorageEngine<R>,
    R: Record,
{
    fn drop(&mut self) {
        self.reactor.unsubscribe(self.id);
    }
}

#[cfg(test)]
mod tests {
    use crate as ankurah_core;
    use crate::property::YrsString;
    use crate::Model;

    use super::*;
    use ankql;
    use std::sync::{Arc, Mutex, Weak};

    #[derive(Debug, Clone, PartialEq)]
    pub enum PetUpdate {
        #[allow(unused)]
        Name(String),
        Age(i64),
    }

    use crate::derive_deps::wasm_bindgen::prelude::wasm_bindgen;
    #[derive(Debug, Clone, Model)]
    pub struct Pet {
        #[active_value(YrsString)]
        pub name: String,
        #[active_value(YrsString)]
        pub age: String,
    }

    #[derive(Clone, Debug)]
    pub struct ActivePet {
        model: Arc<Mutex<Pet>>,
        engine: Weak<DummyEngine>,
    }

    impl ActivePet {
        fn new(pet: Pet, engine: &Arc<DummyEngine>) -> Self {
            Self {
                model: Arc::new(Mutex::new(pet)),
                engine: Arc::downgrade(engine),
            }
        }

        #[allow(unused)]
        pub fn set_name(&self, name: String) {
            let mut pet = self.model.lock().unwrap();
            pet.name = name.clone();
            if let Some(engine) = self.engine.upgrade() {
                engine.notify_change(
                    self.clone(),
                    vec![PetUpdate::Name(name)],
                    RecordChangeKind::Edit,
                );
            }
        }

        pub fn set_age(&self, age: i64) {
            {
                let mut pet = self.model.lock().unwrap();
                pet.age = age.to_string();
            }

            if let Some(engine) = self.engine.upgrade() {
                engine.notify_change(
                    self.clone(),
                    vec![PetUpdate::Age(age)],
                    RecordChangeKind::Edit,
                );
            }
        }
    }

    // impl Record for ActivePet {
    //     type Id = i64;
    //     type Model = Pet;
    //     fn id(&self) -> Self::Id {
    //         self.model.lock().unwrap().id
    //     }

    //     fn value(&self, name: &str) -> Option<Value> {
    //         match name {
    //             "id" => Some(Value::Integer(self.id())),
    //             "name" => Some(Value::String(self.model.lock().unwrap().name.clone())),
    //             "age" => Some(Value::Integer(
    //                 self.model.lock().unwrap().age.parse::<i64>().unwrap(),
    //             )),
    //             _ => None,
    //         }
    //     }
    // }

    // LEFT OFF HERE - NEXT STEPS:
    // [X] implement value<T>(name: &str) -> Option<T> for RecordInner and PropertyBackends such that we can get a typecasted value
    //     Filterable is already implemented for RecordInner
    // [X] implement Filterable trait for RecordInner
    // [ ] Update the reactor code and these tests to operate on Record/RecordInner and make Pet Model with a derived PetRecord
    // [ ] get the tests passing here
    // [ ] integrate reactor into node
    // [ ] adapt the tests here to use node directly (and reactor indirectly)
    // [ ] proper typecasting of values
    //  * Continue using ComparisonIndex for now in the interest of expedience
    //  * Later we will replace the ComparisonIndex functionality into the storage engine implementations

    impl ankql::selection::filter::Filterable for ActivePet {
        fn collection(&self) -> &str {
            "pets"
        }

        fn value(&self, name: &str) -> Option<String> {
            let pet = self.model.lock().unwrap();
            match name {
                "id" => Some(pet.id.to_string()),
                "name" => Some(pet.name.clone()),
                "age" => Some(pet.age.to_string()),
                _ => None,
            }
        }
    }

    struct DummyEngine {
        records: Mutex<Vec<ActivePet>>,
        reactor: Mutex<Option<Weak<Reactor<Self, ActivePet>>>>,
    }

    impl DummyEngine {
        fn new(records: Vec<Pet>) -> Arc<Self> {
            let me = Arc::new(Self {
                records: Mutex::new(vec![]),
                reactor: Mutex::new(None),
            });
            {
                let mut r = me.records.lock().unwrap();
                for record in records {
                    r.push(ActivePet::new(record, &me));
                }
            }
            me
        }

        fn set_reactor(&self, reactor: &Arc<Reactor<Self, ActivePet>>) {
            *self.reactor.lock().unwrap() = Some(Arc::downgrade(reactor));
        }

        fn notify_change(
            &self,
            record: ActivePet,
            updates: Vec<PetUpdate>,
            kind: RecordChangeKind,
        ) {
            if let Some(reactor) = self
                .reactor
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade)
            {
                reactor.notify_change(record, updates, kind);
            }
        }

        fn get(&self, id: i64) -> Option<ActivePet> {
            self.records
                .lock()
                .unwrap()
                .iter()
                .find(|p| p.id() == id)
                .cloned()
        }
    }

    // Use StorageEngine instead
    // impl TestStorageEngine<ActivePet> for DummyEngine {
    //     type Id = usize;
    //     type Update = PetUpdate;

    //     fn fetch_records(&self, predicate: &ast::Predicate) -> Vec<ActivePet> {
    //         use ankql::selection::filter::{FilterIterator, FilterResult};

    //         let records = self.records.lock().unwrap();
    //         FilterIterator::new(records.iter().cloned(), predicate.clone())
    //             .filter_map(|result| match result {
    //                 FilterResult::Pass(record) => Some(record),
    //                 _ => None,
    //             })
    //             .collect()
    //     }
    // }

    #[test]
    pub fn test_watch_index() {
        let server = DummyEngine::new(vec![
            Pet {
                name: "Rex".to_string(),
                age: "1".to_string(),
            },
            Pet {
                name: "Snuffy".to_string(),
                age: "2".to_string(),
            },
            Pet {
                name: "Jasper".to_string(),
                age: "4".to_string(),
            },
        ]);
        let reactor = Arc::new(Reactor::new(server.clone()));

        let predicate =
            ankql::parser::parse_selection("name = 'Rex' OR age > 2 and age < 5").unwrap();

        let sub_id = SubscriptionId::from(0);
        reactor.add_index_watchers(sub_id, &predicate);

        println!("watchers: {:#?}", reactor.index_watchers);

        // Check to see if the index watchers are populated correctly
        let iw = &reactor.index_watchers;
        let name = FieldId("name".to_string());
        let age = FieldId("age".to_string());

        // Check name = 'Rex'
        assert_eq!(
            iw.get(&name).map(|i| {
                assert_eq!(i.eq.len(), 1);
                i.find_matching(Value::String("Rex".to_string()))
            }),
            Some(vec![sub_id])
        );

        // Check age > 2
        assert_eq!(
            iw.get(&age).map(|i| {
                assert_eq!(i.gt.len(), 1);
                i.find_matching(Value::Integer(3))
            }),
            Some(vec![sub_id])
        );

        // Check age < 5
        assert_eq!(
            iw.get(&age).map(|i| {
                assert_eq!(i.lt.len(), 1);
                i.find_matching(Value::Integer(4))
            }),
            Some(vec![sub_id])
        );
    }

    #[test]
    fn test_subscription_and_notification() {
        println!("MARK 1: Creating server");
        let server = DummyEngine::new(vec![
            Pet {
                name: "Rex".to_string(),
                age: "1".to_string(),
            },
            Pet {
                name: "Snuffy".to_string(),
                age: "2".to_string(),
            },
            Pet {
                name: "Jasper".to_string(),
                age: "6".to_string(),
            },
        ]);
        println!("MARK 2: Creating reactor");
        let reactor = Reactor::new(server.clone());
        server.set_reactor(&reactor);

        println!("MARK 3: Setting up changesets");
        let received_changesets = Arc::new(Mutex::new(Vec::new()));
        let received_changesets_clone = received_changesets.clone();
        let check_received = move || {
            let mut changesets = received_changesets.lock().unwrap();
            let result: Vec<(Vec<PetUpdate>, RecordChangeKind)> = (*changesets)
                .iter()
                .map(|c: &ChangeSet<ActivePet, PetUpdate>| {
                    (c.changes[0].updates.clone(), c.changes[0].kind.clone())
                })
                .collect();
            changesets.clear();
            result
        };

        println!("MARK 4: Creating subscription");
        let predicate =
            ankql::parser::parse_selection("name = 'Rex' OR (age > 2 and age < 5)").unwrap();
        let _handle = reactor
            .subscribe(&predicate, move |changeset| {
                println!("MARK 5: Callback received");
                let mut received = received_changesets_clone.lock().unwrap();
                received.push(changeset);
            })
            .unwrap();

        println!("MARK 6: Getting Rex's record");
        let rex = server.get(1).expect("Rex should exist");
        assert_eq!(
            Record::value(&rex, "name").unwrap(),
            Value::String("Rex".to_string())
        );

        let snuffy = server.get(2).expect("Snuffy should exist");
        assert_eq!(
            Record::value(&snuffy, "name").unwrap(),
            Value::String("Snuffy".to_string())
        );

        let jasper = server.get(3).expect("Jasper should exist");
        assert_eq!(
            Record::value(&jasper, "name").unwrap(),
            Value::String("Jasper".to_string())
        );

        println!("MARK 7: Verifying initial state");
        {
            let received = check_received();
            assert_eq!(received, vec![(vec![], RecordChangeKind::Add)]);
        }

        println!("MARK 9: Updating Rex's age");
        rex.set_age(7);

        // should have recieved one changeset with updates [PetUpdate::Age(7)] and kind RecordChangeKind::Edit
        assert_eq!(
            check_received(),
            vec![(
                vec![PetUpdate::Age(7)],
                RecordChangeKind::Edit // it's an edit because it was already matching
                                       // rex always shows up because he's an OR predicate by name
            )]
        );

        // snuffy turns 3
        snuffy.set_age(3);
        assert_eq!(
            check_received(),
            vec![(
                vec![PetUpdate::Age(3)],
                RecordChangeKind::Add // It's an add because it formerly didn't match the predicate
            )]
        );

        // turns out we had Jasper's age wrong, he's actually 4
        jasper.set_age(4);
        assert_eq!(
            check_received(),
            vec![(
                vec![PetUpdate::Age(4)],
                RecordChangeKind::Add // It's an add because it formerly didn't match the predicate
            )]
        );

        // snuffy and jasper turn 5 and 6
        snuffy.set_age(5);
        jasper.set_age(6);

        // we should have received two changesets with a remove and an add
        assert_eq!(
            check_received(),
            vec![
                (vec![PetUpdate::Age(5)], RecordChangeKind::Remove),
                (vec![PetUpdate::Age(6)], RecordChangeKind::Remove)
            ]
        );
    }
}
