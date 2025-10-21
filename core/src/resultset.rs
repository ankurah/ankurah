use crate::indexing::{encode_tuple_values_with_key_spec, KeySpec};
use crate::{entity::Entity, model::View, reactor::AbstractEntity};
use ankurah_proto as proto;
use ankurah_signals::{
    broadcast::{Broadcast, BroadcastId},
    signal::{Listener, ListenerGuard},
    subscribe::IntoSubscribeListener,
    CurrentObserver, Get, Peek, Signal, Subscribe, SubscriptionGuard,
};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// Efficient storage for sort keys - uses fixed array for small keys, Vec for larger ones
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IVec {
    /// Keys <= 16 bytes stored in zero-padded fixed array
    Small([u8; 16]),
    /// Keys > 16 bytes stored in Vec
    Large(Vec<u8>),
}

impl IVec {
    /// Create from byte slice
    fn from_slice(bytes: &[u8]) -> Self {
        if bytes.len() <= 16 {
            let mut data = [0u8; 16];
            data[..bytes.len()].copy_from_slice(bytes);
            Self::Small(data)
        } else {
            Self::Large(bytes.to_vec())
        }
    }
}

impl From<Vec<u8>> for IVec {
    fn from(vec: Vec<u8>) -> Self { Self::from_slice(&vec) }
}

#[derive(Debug, Clone)]
pub struct EntityResultSet<E: AbstractEntity = Entity>(Arc<Inner<E>>);

/// View-typed ResultSet
#[derive(Debug)]
pub struct ResultSet<R: View>(EntityResultSet<Entity>, std::marker::PhantomData<R>);

impl<R: View> Deref for ResultSet<R> {
    type Target = EntityResultSet<Entity>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<R: View> ResultSet<R> {
    pub fn by_id(&self, id: &proto::EntityId) -> Option<R> { self.0.by_id(id).map(|e| R::from_entity(e)) }
}

#[derive(Debug)]
struct Inner<E: AbstractEntity> {
    // Order preserving set of entities
    state: std::sync::Mutex<State<E>>,
    loaded: AtomicBool,
    broadcast: Broadcast<()>,
}

#[derive(Debug)]
struct State<E: AbstractEntity> {
    order: Vec<EntityEntry<E>>,
    index: HashMap<proto::EntityId, usize>,
    // Ordering configuration
    key_spec: Option<KeySpec>,
    limit: Option<usize>,
    gap_dirty: bool, // Set when we remove entities and go from =LIMIT to < LIMIT
}

#[derive(Debug, Clone)]
struct EntityEntry<E: AbstractEntity> {
    entity: E,
    sort_key: Option<IVec>,
    dirty: bool,
}
// TODO - figure out how to maintain ordering of entities

/// A write guard for making atomic changes to a ResultSet
/// Holds the mutex guard to ensure all changes happen atomically
/// Sends a single notification when dropped (if any changes were made)
pub struct ResultSetWrite<'a, E: AbstractEntity = Entity> {
    resultset: &'a EntityResultSet<E>,
    changed: bool,
    guard: Option<std::sync::MutexGuard<'a, State<E>>>,
}

/// A read guard for read-only access to a ResultSet
/// Holds the mutex guard to ensure consistent reads
pub struct ResultSetRead<'a, E: AbstractEntity = Entity> {
    guard: std::sync::MutexGuard<'a, State<E>>,
}

// TODO - build unit tests for this
impl<'a, E: AbstractEntity> ResultSetWrite<'a, E> {
    /// Add an entity to the result set
    pub fn add(&mut self, entity: E) -> bool {
        let guard = self.guard.as_mut().expect("write guard already dropped");
        let id = *entity.id();
        if guard.index.contains_key(&id) {
            return false; // Already present
        }

        // Compute sort key if ordering is configured
        let sort_key = guard.key_spec.as_ref().map(|key_spec| Self::compute_sort_key(&entity, key_spec));

        let entry = EntityEntry { entity, sort_key, dirty: false };

        // Insert in correct position (always sort by entity ID, with optional key spec first)
        let pos = guard
            .order
            .binary_search_by(|existing| {
                match (&existing.sort_key, &entry.sort_key) {
                    (Some(existing_key), Some(entry_key)) => {
                        // Both have sort keys - compare keys first, then entity ID for tie-breaking
                        existing_key.cmp(entry_key).then_with(|| existing.entity.id().cmp(entry.entity.id()))
                    }
                    (Some(_), None) => std::cmp::Ordering::Less, // Keyed entries sort before unkeyed
                    (None, Some(_)) => std::cmp::Ordering::Greater, // Unkeyed entries sort after keyed
                    (None, None) => existing.entity.id().cmp(entry.entity.id()), // Both unkeyed - sort by entity ID
                }
            })
            .unwrap_or_else(|pos| pos);

        guard.order.insert(pos, entry);
        guard.index.insert(id, pos);

        // Fix indices for all entries after the insertion point
        for i in (pos + 1)..guard.order.len() {
            let entry_id = *guard.order[i].entity.id();
            guard.index.insert(entry_id, i);
        }

        // Apply limit if configured
        if let Some(limit) = guard.limit {
            if guard.order.len() > limit {
                // Remove the last entry (beyond limit)
                if let Some(removed_entry) = guard.order.pop() {
                    let removed_id = *removed_entry.entity.id();
                    guard.index.remove(&removed_id);
                    // TODO: Return the evicted entity ID for the caller to handle
                }
            }
        }

        self.changed = true;
        true
    }

    /// Remove an entity from the result set
    pub fn remove(&mut self, id: proto::EntityId) -> bool {
        let guard = self.guard.as_mut().expect("write guard already dropped");
        if let Some(idx) = guard.index.remove(&id) {
            // Check if we were at limit before removal
            if guard.limit.is_some_and(|limit| guard.order.len() == limit) {
                guard.gap_dirty = true;
            }

            guard.order.remove(idx);
            if idx < guard.order.len() {
                fix_from(guard, idx);
            }

            self.changed = true;
            true
        } else {
            false
        }
    }

    /// Check if an entity exists
    pub fn contains(&self, id: &proto::EntityId) -> bool {
        self.guard.as_ref().expect("write guard already dropped").index.contains_key(id)
    }

    /// Iterate over all entities
    /// Returns an iterator over (entity_id, entity) pairs
    pub fn iter_entities(&self) -> impl Iterator<Item = (proto::EntityId, &E)> {
        let guard = self.guard.as_ref().expect("write guard already dropped");
        guard.order.iter().map(|entry| (*entry.entity.id(), &entry.entity))
    }

    /// Mark all entities as dirty for re-evaluation
    pub fn mark_all_dirty(&mut self) {
        let guard = self.guard.as_mut().expect("write guard already dropped");
        for entry in &mut guard.order {
            entry.dirty = true;
        }
        self.changed = true;
    }

    /// Retain only dirty entities that pass the closure, removing those that don't
    pub fn retain_dirty<F>(&mut self, mut should_retain: F) -> Vec<proto::EntityId>
    where F: FnMut(&E) -> bool {
        let guard = self.guard.as_mut().expect("write guard already dropped");
        let mut removed_ids = Vec::new();
        let mut i = 0;

        // Check if we were at limit before any removals
        let was_at_limit = guard.limit.is_some_and(|limit| guard.order.len() == limit);

        while i < guard.order.len() {
            if guard.order[i].dirty {
                let should_keep = should_retain(&guard.order[i].entity);
                if should_keep {
                    // Entity should be retained - recompute sort key and mark clean
                    let key_spec = guard.key_spec.clone();
                    if let Some(key_spec) = key_spec {
                        guard.order[i].sort_key = Some(Self::compute_sort_key(&guard.order[i].entity, &key_spec));
                    }
                    guard.order[i].dirty = false;
                    i += 1;
                } else {
                    // Entity should be removed
                    let removed_entry = guard.order.remove(i);
                    let removed_id = *removed_entry.entity.id();
                    guard.index.remove(&removed_id);
                    removed_ids.push(removed_id);
                    // Don't increment i since we removed an element
                }
            } else {
                i += 1;
            }
        }

        // Fix indices after removals (no re-sorting needed)
        guard.index.clear();
        let index_updates: Vec<_> = guard.order.iter().enumerate().map(|(i, entry)| (*entry.entity.id(), i)).collect();
        for (id, i) in index_updates {
            guard.index.insert(id, i);
        }

        if !removed_ids.is_empty() {
            self.changed = true;

            // Set gap_dirty if we went from LIMIT to < LIMIT
            if (!guard.gap_dirty) && was_at_limit && guard.limit.is_some_and(|limit| guard.order.len() < limit) {
                guard.gap_dirty = true;
            }
        }

        removed_ids
    }

    /// Replace all entities in the result set with proper sorting
    pub fn replace_all(&mut self, entities: Vec<E>) {
        let guard = self.guard.as_mut().expect("write guard already dropped");

        // Clear existing data
        guard.order.clear();
        guard.index.clear();

        // Add all entities with proper sorting
        for entity in entities {
            // Compute sort key if ordering is configured
            let sort_key = guard.key_spec.as_ref().map(|key_spec| Self::compute_sort_key(&entity, key_spec));

            let entry = EntityEntry { entity, sort_key, dirty: false };
            guard.order.push(entry);
        }

        // Sort all entries if we have ordering configured
        if guard.key_spec.is_some() {
            guard.order.sort_by(|a, b| {
                match (&a.sort_key, &b.sort_key) {
                    (Some(key_a), Some(key_b)) => {
                        // Compare keys first, then entity ID for tie-breaking
                        key_a.cmp(key_b).then_with(|| a.entity.id().cmp(b.entity.id()))
                    }
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => a.entity.id().cmp(b.entity.id()),
                }
            });
        } else {
            // Sort by entity ID only if no key spec
            guard.order.sort_by(|a, b| a.entity.id().cmp(b.entity.id()));
        }

        // Apply limit if configured
        if let Some(limit) = guard.limit {
            if guard.order.len() > limit {
                guard.order.truncate(limit);
            }
        }

        // Rebuild index
        let index_updates: Vec<_> = guard.order.iter().enumerate().map(|(i, entry)| (*entry.entity.id(), i)).collect();
        for (id, i) in index_updates {
            guard.index.insert(id, i);
        }

        self.changed = true;
    }

    /// Compute sort key for an entity using the current key spec
    fn compute_sort_key(entity: &E, key_spec: &KeySpec) -> IVec {
        let mut values = Vec::new();

        // Extract values for each key part
        for keypart in &key_spec.keyparts {
            let value = AbstractEntity::value(entity, &keypart.column);
            // TODO: Handle NULLs properly - for now we'll get encoding errors on NULLs
            // which will cause unwrap_or_default() to return empty key (sorts first)
            if let Some(v) = value {
                values.push(v);
            } else {
                // Skip this entity for now if any field is NULL
                return IVec::from_slice(&[]); // Empty key sorts first
            }
        }

        // Encode the tuple - if this fails, return empty key (will sort first)
        let encoded = encode_tuple_values_with_key_spec(&values, key_spec).unwrap_or_default();
        IVec::from(encoded)
    }
}

impl<'a, E: AbstractEntity> Drop for ResultSetWrite<'a, E> {
    fn drop(&mut self) {
        if self.changed {
            // Drop the guard first to release the lock before broadcasting
            drop(self.guard.take());
            self.resultset.0.broadcast.send(());
        }
    }
}

impl<'a, E: AbstractEntity> ResultSetRead<'a, E> {
    /// Check if an entity exists
    pub fn contains(&self, id: &proto::EntityId) -> bool { self.guard.index.contains_key(id) }

    /// Iterate over all entities
    /// Returns an iterator over (entity_id, entity) pairs
    pub fn iter_entities(&self) -> impl Iterator<Item = (proto::EntityId, &E)> {
        self.guard.order.iter().map(|entity| (*entity.entity.id(), &entity.entity))
    }

    /// Get the number of entities
    pub fn len(&self) -> usize { self.guard.order.len() }

    /// Check if the result set is empty
    pub fn is_empty(&self) -> bool { self.guard.order.is_empty() }
}

impl<E: AbstractEntity> EntityResultSet<E> {
    pub fn from_vec(entities: Vec<E>, loaded: bool) -> Self {
        let mut index = HashMap::new();
        let mut order = Vec::new();

        for (i, entity) in entities.into_iter().enumerate() {
            index.insert(*entity.id(), i);
            order.push(EntityEntry { entity, sort_key: None, dirty: false });
        }

        let state = State { order, index, key_spec: None, limit: None, gap_dirty: false };
        Self(Arc::new(Inner { state: std::sync::Mutex::new(state), loaded: AtomicBool::new(loaded), broadcast: Broadcast::new() }))
    }
    pub fn empty() -> Self {
        let state = State { order: Vec::new(), index: HashMap::new(), key_spec: None, limit: None, gap_dirty: false };
        Self(Arc::new(Inner { state: std::sync::Mutex::new(state), loaded: AtomicBool::new(false), broadcast: Broadcast::new() }))
    }
    pub fn single(entity: E) -> Self {
        let entry = EntityEntry { entity: entity.clone(), sort_key: None, dirty: false };
        let mut state = State { order: vec![entry], index: HashMap::new(), key_spec: None, limit: None, gap_dirty: false };
        state.index.insert(*entity.id(), 0);
        Self(Arc::new(Inner { state: std::sync::Mutex::new(state), loaded: AtomicBool::new(false), broadcast: Broadcast::new() }))
    }

    /// Begin a write operation for atomic changes to the resultset
    /// All mutations happen through the returned write guard
    /// A single notification is sent when the guard is dropped (if changes were made)
    pub fn write(&self) -> ResultSetWrite<'_, E> {
        let guard = self.0.state.lock().unwrap();
        ResultSetWrite { resultset: self, changed: false, guard: Some(guard) }
    }

    /// Get a read guard for consistent read-only access to the resultset
    pub fn read(&self) -> ResultSetRead<'_, E> {
        let guard = self.0.state.lock().unwrap();
        ResultSetRead { guard }
    }
    pub fn set_loaded(&self, loaded: bool) {
        self.0.loaded.store(loaded, Ordering::Relaxed);
        self.0.broadcast.send(());
    }
    pub fn is_loaded(&self) -> bool {
        CurrentObserver::track(&self);
        self.0.loaded.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let mut st = self.0.state.lock().unwrap();
        st.order.clear();
        st.index.clear();
        drop(st);
        self.0.broadcast.send(());
    }

    /// Get an iterator over entity IDs without cloning entities
    pub fn keys(&self) -> EntityResultSetKeyIterator {
        // TODO make a signal trait for tracked keys
        CurrentObserver::track(&self);
        let st = self.0.state.lock().unwrap();
        let keys: Vec<proto::EntityId> = st.order.iter().map(|e| *e.entity.id()).collect();
        EntityResultSetKeyIterator::new(keys)
    }

    /// Check if an entity with the given ID exists
    pub fn contains_key(&self, id: &proto::EntityId) -> bool {
        // TODO make a signal trait for tracked contains_key
        CurrentObserver::track(&self);
        let st = self.0.state.lock().unwrap();
        st.index.contains_key(id)
    }

    pub fn by_id(&self, id: &proto::EntityId) -> Option<E> {
        // TODO make a signal trait for tracked by_id
        CurrentObserver::track(self);
        let st = self.0.state.lock().unwrap();
        st.index.get(id).map(|&i| st.order[i].entity.clone())
    }

    pub fn len(&self) -> usize {
        CurrentObserver::track(&self);
        let st = self.0.state.lock().unwrap();
        st.order.len()
    }

    /// Check if this result set needs gap filling
    pub(crate) fn is_gap_dirty(&self) -> bool {
        let st = self.0.state.lock().unwrap();
        st.gap_dirty
    }

    /// Clear the gap_dirty flag (called after gap filling is complete)
    pub(crate) fn clear_gap_dirty(&self) {
        let mut st = self.0.state.lock().unwrap();
        st.gap_dirty = false;
    }

    /// Get the current limit for this result set
    pub fn get_limit(&self) -> Option<usize> {
        let st = self.0.state.lock().unwrap();
        st.limit
    }

    /// Get the last entity for gap filling continuation
    pub(crate) fn last_entity(&self) -> Option<E> {
        let st = self.0.state.lock().unwrap();
        st.order.last().map(|entry| entry.entity.clone())
    }

    /// Configure ordering for this result set
    pub(crate) fn order_by(&self, key_spec: Option<KeySpec>) {
        let mut st = self.0.state.lock().unwrap();

        // Check if the key spec actually changed
        if st.key_spec == key_spec {
            return; // No change, no-op
        }

        st.key_spec = key_spec.clone();

        // Recompute sort keys for all entries
        for entry in &mut st.order {
            entry.sort_key = if let Some(ref ks) = key_spec {
                Some(ResultSetWrite::compute_sort_key(&entry.entity, ks))
            } else {
                None // No ORDER BY, sort by entity ID only
            };
        }

        // Sort by the new keys
        st.order.sort_by(|a, b| {
            match (&a.sort_key, &b.sort_key) {
                (Some(key_a), Some(key_b)) => {
                    // First compare by sort key
                    match key_a.cmp(key_b) {
                        std::cmp::Ordering::Equal => a.entity.id().cmp(b.entity.id()), // Tie-break by entity ID
                        other => other,
                    }
                }
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => a.entity.id().cmp(b.entity.id()),
            }
        });

        // Rebuild index after sorting
        st.index.clear();
        let index_updates: Vec<_> = st.order.iter().enumerate().map(|(i, entry)| (*entry.entity.id(), i)).collect();
        for (id, i) in index_updates {
            st.index.insert(id, i);
        }

        drop(st);
        self.0.broadcast.send(());
    }

    /// Set the limit for this result set
    pub(crate) fn limit(&self, limit: Option<usize>) {
        let mut st = self.0.state.lock().unwrap();

        // Check if the limit actually changed
        if st.limit == limit {
            return; // No change, no-op
        }

        st.limit = limit;

        // Apply the new limit by truncating if necessary
        let mut entities_removed = false;
        if let Some(limit) = limit {
            if st.order.len() > limit {
                st.order.truncate(limit);
                entities_removed = true;

                // Rebuild index after truncation
                st.index.clear();
                let index_updates: Vec<_> = st.order.iter().enumerate().map(|(i, entry)| (*entry.entity.id(), i)).collect();
                for (id, i) in index_updates {
                    st.index.insert(id, i);
                }
            }
        }

        drop(st);

        // Only broadcast if entities were actually removed
        if entities_removed {
            self.0.broadcast.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexing::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder};
    use crate::value::{Value, ValueType};
    use ankurah_proto as proto;
    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    struct TestEntity {
        id: proto::EntityId,
        collection: proto::CollectionId,
        properties: HashMap<String, Value>,
    }

    impl TestEntity {
        fn new(id: u8, properties: HashMap<String, Value>) -> Self {
            let mut id_bytes = [0u8; 16];
            id_bytes[15] = id;
            Self { id: proto::EntityId::from_bytes(id_bytes), collection: proto::CollectionId::fixed_name("test"), properties }
        }
    }

    impl AbstractEntity for TestEntity {
        fn collection(&self) -> proto::CollectionId { self.collection.clone() }

        fn id(&self) -> &proto::EntityId { &self.id }

        fn value(&self, field: &str) -> Option<Value> {
            if field == "id" {
                Some(Value::EntityId(self.id.clone()))
            } else {
                self.properties.get(field).cloned()
            }
        }
    }

    #[test]
    fn test_entity_id_ordering() {
        let resultset = EntityResultSet::empty();
        let mut write = resultset.write();

        // Create entities with different IDs (bytes sort chronologically)
        let entity1 = TestEntity::new(1, HashMap::new());
        let entity2 = TestEntity::new(2, HashMap::new());
        let entity3 = TestEntity::new(3, HashMap::new());

        // Add in reverse order
        write.add(entity3.clone());
        write.add(entity1.clone());
        write.add(entity2.clone());

        drop(write);

        // Should be sorted by entity ID
        let read_guard = resultset.read();
        let entities: Vec<_> = read_guard.iter_entities().collect();
        assert_eq!(entities.len(), 3);
        assert_eq!(entities[0].0, entity1.id);
        assert_eq!(entities[1].0, entity2.id);
        assert_eq!(entities[2].0, entity3.id);
    }

    #[test]
    fn test_order_by_with_tie_breaking() {
        let resultset = EntityResultSet::empty();

        // Create entities with same name but different IDs
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("Alice".to_string()));
        let entity1 = TestEntity::new(1, props1);

        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), Value::String("Alice".to_string()));
        let entity2 = TestEntity::new(2, props2);

        let mut props3 = HashMap::new();
        props3.insert("name".to_string(), Value::String("Bob".to_string()));
        let entity3 = TestEntity::new(3, props3);

        // Set up ordering by name
        let key_spec = KeySpec {
            keyparts: vec![IndexKeyPart {
                column: "name".to_string(),
                direction: IndexDirection::Asc,
                nulls: Some(NullsOrder::Last),
                collation: None,
                value_type: ValueType::String,
            }],
        };
        resultset.order_by(Some(key_spec));

        let mut write = resultset.write();
        write.add(entity2.clone());
        write.add(entity3.clone());
        write.add(entity1.clone());
        drop(write);

        // Should be sorted by name, then by entity ID for tie-breaking
        let read_guard = resultset.read();
        let entities: Vec<_> = read_guard.iter_entities().collect();
        assert_eq!(entities.len(), 3);
        // Both Alice entities should come first (sorted by ID), then Bob
        assert_eq!(entities[0].0, entity1.id); // Alice (earlier ID)
        assert_eq!(entities[1].0, entity2.id); // Alice (later ID)
        assert_eq!(entities[2].0, entity3.id); // Bob
    }

    #[test]
    fn test_limit_functionality() {
        let resultset = EntityResultSet::empty();

        // Add some entities
        let mut write = resultset.write();
        for i in 0..5u8 {
            let mut props = HashMap::new();
            props.insert("value".to_string(), Value::I32(i as i32));
            let entity = TestEntity::new(i, props);
            write.add(entity);
        }
        drop(write);

        assert_eq!(resultset.len(), 5);

        // Apply limit
        resultset.limit(Some(3));
        assert_eq!(resultset.len(), 3);

        // Remove limit
        resultset.limit(None);
        assert_eq!(resultset.len(), 3); // Should stay truncated
    }

    #[test]
    fn test_dirty_tracking() {
        let resultset = EntityResultSet::empty();

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        let entity1 = TestEntity::new(1, props);

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(false));
        let entity2 = TestEntity::new(2, props);

        let mut write = resultset.write();
        write.add(entity1.clone());
        write.add(entity2.clone());

        // Mark all dirty
        write.mark_all_dirty();

        // Retain only active entities
        let removed = write.retain_dirty(|entity| entity.value("active") == Some(Value::Bool(true)));

        drop(write);

        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], entity2.id);
        assert_eq!(resultset.len(), 1);
        assert_eq!(resultset.read().iter_entities().next().unwrap().0, entity1.id);
    }

    #[test]
    fn test_write_guard_atomic_operations() {
        let resultset = EntityResultSet::empty();

        // Multiple operations in one write guard should be atomic
        {
            let mut write = resultset.write();
            let entity1 = TestEntity::new(1, HashMap::new());
            let entity2 = TestEntity::new(2, HashMap::new());

            write.add(entity1);
            write.add(entity2);

            // Operations are visible within the same write guard
            assert_eq!(write.iter_entities().count(), 2);
            // Notification sent when write guard is dropped
        }

        // Operations should be visible after guard is dropped
        assert_eq!(resultset.len(), 2);
    }

    #[test]
    fn test_ivec_small_keys() {
        // Test that small keys (<=16 bytes) use the Small variant
        let small_key = IVec::from_slice(b"hello");
        let another_small = IVec::from_slice(b"world");
        let empty_key = IVec::from_slice(b"");

        // Verify ordering works correctly with zero padding
        assert!(small_key < another_small); // "hello" < "world"
        assert!(empty_key < small_key); // empty sorts first

        // Test that zero padding doesn't affect comparison
        let key_ab = IVec::from_slice(b"ab");
        let key_abc = IVec::from_slice(b"abc");
        assert!(key_ab < key_abc); // "ab" < "abc" even with zero padding
    }

    #[test]
    fn test_ivec_large_keys() {
        // Test that large keys (>16 bytes) use the Large variant
        let large_key = IVec::from_slice(&[1u8; 20]); // 20 bytes
        let small_key = IVec::from_slice(&[1u8; 10]); // 10 bytes

        // Verify they can be compared
        assert!(small_key < large_key); // smaller array should sort first
    }

    #[test]
    fn test_ivec_boundary() {
        // Test the 16-byte boundary
        let exactly_16 = IVec::from_slice(&[1u8; 16]);
        let exactly_17 = IVec::from_slice(&[1u8; 17]);

        // Both should work and be comparable
        assert!(exactly_16 < exactly_17);

        // Verify 16-byte keys use Small variant (this is implicit in the implementation)
        match exactly_16 {
            IVec::Small(_) => (), // Expected
            IVec::Large(_) => panic!("16-byte key should use Small variant"),
        }

        match exactly_17 {
            IVec::Large(_) => (), // Expected
            IVec::Small(_) => panic!("17-byte key should use Large variant"),
        }
    }
}

fn fix_from<E: AbstractEntity>(st: &mut State<E>, start: usize) {
    // Recompute indices for shifted tail
    for i in start..st.order.len() {
        let id = *st.order[i].entity.id();
        st.index.insert(id, i);
    }
}

impl<E: View> ResultSet<E> {
    pub fn iter(&self) -> ResultSetIter<E> { ResultSetIter::new(self.clone()) }
}

impl<E: View> Clone for ResultSet<E> {
    fn clone(&self) -> Self { Self(self.0.clone(), std::marker::PhantomData) }
}

impl<E: View> Default for ResultSet<E> {
    fn default() -> Self {
        let entity_resultset = EntityResultSet::empty();
        Self(entity_resultset, std::marker::PhantomData)
    }
}

impl<E: AbstractEntity> Signal for EntityResultSet<E> {
    fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.0.broadcast.reference().listen(listener)) }
    fn broadcast_id(&self) -> BroadcastId { self.0.broadcast.id() }
}

impl<R: View> Signal for ResultSet<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.0 .0.broadcast.reference().listen(listener)) }

    fn broadcast_id(&self) -> BroadcastId { self.0 .0.broadcast.id() }
}

impl<E: View + Clone + 'static> Get<Vec<E>> for ResultSet<E> {
    fn get(&self) -> Vec<E> {
        use ankurah_signals::CurrentObserver;
        CurrentObserver::track(self);
        self.0 .0.state.lock().unwrap().order.iter().map(|e| E::from_entity(e.entity.clone())).collect()
    }
}

impl<E: View + Clone + 'static> Peek<Vec<E>> for ResultSet<E> {
    fn peek(&self) -> Vec<E> { self.0 .0.state.lock().unwrap().order.iter().map(|e| E::from_entity(e.entity.clone())).collect() }
}

impl<E: View + Clone + 'static> Subscribe<Vec<E>> for ResultSet<E> {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<Vec<E>> {
        let listener = listener.into_subscribe_listener();
        let me = self.clone();
        let guard: ankurah_signals::broadcast::ListenerGuard<()> = self.0 .0.broadcast.reference().listen(move |_| {
            let entities: Vec<E> = me.0 .0.state.lock().unwrap().order.iter().map(|e| E::from_entity(e.entity.clone())).collect();
            listener(entities);
        });
        SubscriptionGuard::new(ListenerGuard::new(guard))
    }
}

#[derive(Debug)]
pub struct ResultSetIter<E: View> {
    resultset: ResultSet<E>,
    index: usize,
}

impl<E: View> ResultSetIter<E> {
    fn new(resultset: ResultSet<E>) -> Self { Self { resultset, index: 0 } }
}

impl<E: View + Clone> Iterator for ResultSetIter<E> {
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        // Track the underlying resultset using the CurrentObserver when iterating
        use ankurah_signals::CurrentObserver;
        CurrentObserver::track(&self.resultset);

        let state = self.resultset.0 .0.state.lock().unwrap();
        if self.index < state.order.len() {
            let entity = &state.order[self.index].entity;
            let view = E::from_entity(entity.clone());
            self.index += 1;
            Some(view)
        } else {
            None
        }
    }
}

impl<E: View + Clone> IntoIterator for ResultSet<E> {
    type Item = E;
    type IntoIter = ResultSetIter<E>;

    fn into_iter(self) -> Self::IntoIter { ResultSetIter::new(self) }
}

impl<E: View + Clone> IntoIterator for &ResultSet<E> {
    type Item = E;
    type IntoIter = ResultSetIter<E>;

    fn into_iter(self) -> Self::IntoIter { ResultSetIter::new(self.clone()) }
}

#[derive(Debug)]
pub struct EntityResultSetKeyIterator {
    keys: Vec<proto::EntityId>,
    index: usize,
}

impl EntityResultSetKeyIterator {
    fn new(keys: Vec<proto::EntityId>) -> Self { Self { keys, index: 0 } }
}

impl Iterator for EntityResultSetKeyIterator {
    type Item = proto::EntityId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.keys.len() {
            let key = self.keys[self.index];
            self.index += 1;
            Some(key)
        } else {
            None
        }
    }
}

// Specific implementation for EntityResultSet<Entity> to provide map method
impl EntityResultSet<Entity> {
    pub fn wrap<R: View>(&self) -> ResultSet<R> { ResultSet(self.clone(), std::marker::PhantomData) }
}
