use super::collation::Collatable;
use super::comparision_index::ComparisonIndex;
use ankql::ast;
use anyhow::Result;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, Weak};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);
impl Into<FieldId> for &str {
    fn into(self) -> FieldId {
        FieldId(self.to_string())
    }
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of records
pub struct Reactor<S, R>
where
    R: Record,
    S: StorageEngine<R>,
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

pub(crate) trait StorageEngine<R>
where
    R: Record,
{
    type Id: std::hash::Hash + std::cmp::Eq;
    type Update;
    fn fetch_records(&self, predicate: &ast::Predicate) -> Vec<R>;
}
pub(crate) trait Record: Clone {
    type Id: std::hash::Hash + std::cmp::Eq + Copy + Clone;
    type Model;
    fn id(&self) -> Self::Id;
}

impl<S, R> Reactor<S, R>
where
    R: Record,
    S: StorageEngine<R>,
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

        // Store subscription
        self.subscriptions.insert(sub_id, subscription.clone());

        // Find initial matching records
        let matching_records = self.storage.fetch_records(predicate);

        // Update subscription's matching records and record watchers
        for record in matching_records.iter() {
            self.record_watchers
                .entry(record.id())
                .or_default()
                .push(sub_id);
        }

        let subscription = Arc::new(Subscription {
            id: sub_id,
            predicate: predicate.clone(),
            callback: Arc::new(Box::new(callback)),
            matching_records: Mutex::new(matching_records),
        });

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
            entry.or_insert_with(ComparisonIndex::new).add(
                (*literal).clone(),
                operator.clone(),
                sub_id,
            );
        });
    }

    fn remove_index_watchers(&self, sub_id: SubscriptionId, predicate: &ast::Predicate) {
        self.recurse_predicate(
            predicate,
            |entry: dashmap::Entry<FieldId, ComparisonIndex>,
             field_id,
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
        // Find all subscriptions that care about this record's fields
        let mut interested_subs = HashSet::new();

        // Check index watchers for fields that changed
        for (field_id, index) in self.index_watchers.iter() {
            // TODO: Extract changed value for this field from updates
            // For now, assume all fields changed
            interested_subs.extend(index.find_matching_subscriptions(&record));
        }

        // For each interested subscription, check if record matches predicate
        for sub_id in interested_subs {
            if let Some(subscription) = self.subscriptions.get(&sub_id) {
                let matches = self
                    .storage
                    .fetch_records(&subscription.predicate)
                    .iter()
                    .any(|r| r.id() == record.id());

                // Update record watchers
                self.update_record_watchers(&record, matches, sub_id);

                // Notify subscription if needed
                if matches {
                    (subscription.callback)(ChangeSet {
                        changes: vec![RecordChange {
                            record: record.clone(),
                            updates: updates.clone(),
                            kind: kind.clone(),
                        }],
                    });
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum RecordChangeKind {
    Add,
    Remove,
    Edit,
}
/// Represents a change in the record set
pub struct RecordChange<R, U> {
    record: R,
    updates: Vec<U>,
    kind: RecordChangeKind,
}

/// A set of changes to the record set
pub struct ChangeSet<R, U> {
    changes: Vec<RecordChange<R, U>>,
    // other stuff later
}

/// A callback function that receives subscription updates
type Callback<R, U> = Box<dyn Fn(ChangeSet<R, U>) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription<R, U> {
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
    S: StorageEngine<R>,
    R: Record,
{
    id: SubscriptionId,
    reactor: Arc<Reactor<S, R>>,
}

impl<S, R> Drop for SubscriptionHandle<S, R>
where
    S: StorageEngine<R>,
    R: Record,
{
    fn drop(&mut self) {
        self.reactor.unsubscribe(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql;
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    pub enum PetUpdate {
        Name(String),
        Age(i64),
    }

    #[derive(Debug, Clone)]
    pub struct Pet {
        id: i64,
        name: String,
        age: i64,
    }

    #[derive(Clone)]
    pub struct ActivePet {
        inner: Arc<Mutex<Pet>>,
        engine: Weak<DummyEngine>,
    }

    impl ActivePet {
        fn new(pet: Pet, engine: &Arc<DummyEngine>) -> Self {
            Self {
                inner: Arc::new(Mutex::new(pet)),
                engine: Arc::downgrade(engine),
            }
        }

        pub fn set_name(&self, name: String) {
            let mut pet = self.inner.lock().unwrap();
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
            let mut pet = self.inner.lock().unwrap();
            pet.age = age;
            if let Some(engine) = self.engine.upgrade() {
                engine.notify_change(
                    self.clone(),
                    vec![PetUpdate::Age(age)],
                    RecordChangeKind::Edit,
                );
            }
        }
    }

    impl Record for ActivePet {
        type Id = i64;
        type Model = Pet;
        fn id(&self) -> Self::Id {
            self.inner.lock().unwrap().id
        }
    }

    impl ankql::selection::filter::Filterable for ActivePet {
        fn collection(&self) -> &str {
            "pets"
        }

        fn value(&self, name: &str) -> Option<String> {
            let pet = self.inner.lock().unwrap();
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

    impl StorageEngine<ActivePet> for DummyEngine {
        type Id = usize;
        type Update = PetUpdate;

        fn fetch_records(&self, predicate: &ast::Predicate) -> Vec<ActivePet> {
            use ankql::selection::filter::{FilterIterator, FilterResult};

            let records = self.records.lock().unwrap();
            FilterIterator::new(records.iter().cloned(), predicate.clone())
                .filter_map(|result| match result {
                    FilterResult::Pass(record) => Some(record),
                    _ => None,
                })
                .collect()
        }
    }

    #[test]
    pub fn test_watch_index() {
        let server = DummyEngine::new(vec![
            Pet {
                id: 1,
                name: "Rex".to_string(),
                age: 1,
            },
            Pet {
                id: 2,
                name: "Snuffy".to_string(),
                age: 2,
            },
            Pet {
                id: 3,
                name: "Jasper".to_string(),
                age: 4,
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
        let lattitude = FieldId("lattitude".to_string());
        assert_eq!(
            iw.get(&name).map(|i| {
                assert_eq!(i.eq.len(), 1);
                i.find_matching_subscriptions("Rex")
            }),
            Some(vec![sub_id])
        );
        assert_eq!(
            iw.get(&age).map(|i| {
                assert_eq!(i.gt.len(), 1);
                i.find_matching_subscriptions(3)
            }),
            Some(vec![sub_id])
        );
        assert_eq!(
            iw.get(&lattitude).map(|i| {
                assert_eq!(i.lt.len(), 1);
                i.find_matching_subscriptions(7)
            }),
            Some(vec![sub_id])
        );
    }

    #[test]
    fn test_subscription_and_notification() {
        let server = DummyEngine::new(vec![
            Pet {
                id: 1,
                name: "Rex".to_string(),
                age: 1,
            },
            Pet {
                id: 2,
                name: "Snuffy".to_string(),
                age: 2,
            },
            Pet {
                id: 3,
                name: "Jasper".to_string(),
                age: 6,
            },
        ]);
        let reactor = Reactor::new(server.clone());
        server.set_reactor(&reactor);

        let received_changesets = Arc::new(Mutex::new(Vec::new()));
        let received_changesets_clone = received_changesets.clone();

        // Subscribe to pets named Rex OR pets between 2 and 5 years old
        let predicate =
            ankql::parser::parse_selection("name = 'Rex' OR (age > 2 and age < 5)").unwrap();
        let handle = reactor
            .subscribe(&predicate, move |changeset| {
                let mut received = received_changesets_clone.lock().unwrap();
                received.push(changeset);
            })
            .unwrap();

        // Get Rex's record
        let rex = server.get(1).expect("Rex should exist");

        // Verify initial state (Should see Rex only)
        {
            let received = received_changesets.lock().unwrap();
            assert_eq!(received.len(), 1, "Should receive initial changeset");
            let changeset = &received[0];
            assert_eq!(changeset.changes.len(), 1, "Should have one change");
            assert!(matches!(changeset.changes[0].kind, RecordChangeKind::Add));
            assert!(changeset.changes[0].updates.is_empty());
        }

        // Clear the changesets
        received_changesets.lock().unwrap().clear();

        // Make Rex too old to match the age criteria
        rex.set_age(7);

        // Verify Rex was removed with age update
        {
            let received = received_changesets.lock().unwrap();
            assert_eq!(received.len(), 1, "Should receive one changeset");
            let changeset = &received[0];
            assert_eq!(changeset.changes.len(), 1, "Should have one change");
            assert!(matches!(
                changeset.changes[0].kind,
                RecordChangeKind::Remove
            ));
            assert_eq!(changeset.changes[0].updates, vec![PetUpdate::Age(7)]);
        }
    }
}
