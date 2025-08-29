use crate::{entity::Entity, model::View, reactor::AbstractEntity};
use ankurah_proto as proto;
use ankurah_signals::{
    broadcast::{Broadcast, BroadcastId, Listener, ListenerGuard},
    subscribe::IntoSubscribeListener,
    Get, Peek, Signal, Subscribe, SubscriptionGuard,
};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Debug, Clone)]
pub struct EntityResultSet<E: AbstractEntity = Entity>(Arc<Inner<E>>);

/// View-typed ResultSet
#[derive(Debug)]
pub struct ResultSet<R: View>(EntityResultSet<Entity>, std::marker::PhantomData<R>);

impl<R: View> Deref for ResultSet<R> {
    type Target = EntityResultSet<Entity>;
    fn deref(&self) -> &Self::Target { &self.0 }
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
    order: Vec<E>,
    index: HashMap<proto::EntityId, usize>,
}
// TODO - figure out how to maintain ordering of entities

impl<E: AbstractEntity> EntityResultSet<E> {
    pub fn from_vec(order: Vec<E>, loaded: bool) -> Self {
        let mut index = HashMap::new();
        for (i, entity) in order.iter().enumerate() {
            index.insert(entity.id(), i);
        }
        let state = State { order, index };
        Self(Arc::new(Inner { state: std::sync::Mutex::new(state), loaded: AtomicBool::new(loaded), broadcast: Broadcast::new() }))
    }
    pub fn empty() -> Self {
        let state = State { order: Vec::new(), index: HashMap::new() };
        Self(Arc::new(Inner { state: std::sync::Mutex::new(state), loaded: AtomicBool::new(false), broadcast: Broadcast::new() }))
    }
    pub fn set_loaded(&self, loaded: bool) {
        self.0.loaded.store(loaded, Ordering::Relaxed);
        self.0.broadcast.send(());
    }
    pub fn is_loaded(&self) -> bool { self.0.loaded.load(Ordering::Relaxed) }

    pub fn clear(&self) {
        let mut st = self.0.state.lock().unwrap();
        st.order.clear();
        st.index.clear();
        drop(st);
        self.0.broadcast.send(());
    }

    pub fn replace_all(&self, entities: Vec<E>) {
        let mut st = self.0.state.lock().unwrap();
        st.order.clear();
        st.index.clear();
        for (i, entity) in entities.into_iter().enumerate() {
            st.index.insert(AbstractEntity::id(&entity), i);
            st.order.push(entity);
        }
        drop(st);
        self.0.broadcast.send(());
    }
    /// Append entity to the end; returns false if already present.
    pub fn push(&self, entity: E) -> bool {
        let mut st = self.0.state.lock().unwrap();
        let id = AbstractEntity::id(&entity);
        if st.index.contains_key(&id) {
            return false;
        }
        let pos = st.order.len();
        st.order.push(entity);
        st.index.insert(id, pos);
        drop(st);
        self.0.broadcast.send(());
        true
    }

    /// Insert entity immediately after `after_id`.  
    /// If `after_id` not present, appends to end.  
    /// Returns false if entity already present.
    pub fn insert_after(&self, after_id: proto::EntityId, entity: E) -> bool {
        let mut st = self.0.state.lock().unwrap();
        let id = AbstractEntity::id(&entity);
        if st.index.contains_key(&id) {
            return false;
        }

        let insert_pos = st.index.get(&after_id).map(|&i| i + 1).unwrap_or(st.order.len());
        st.order.insert(insert_pos, entity);
        fix_from(&mut st, insert_pos);
        drop(st);

        self.0.broadcast.send(());
        true
    }

    /// Remove entity by id; returns true if removed.
    pub fn remove(&self, id: &proto::EntityId) -> bool {
        let mut st = self.0.state.lock().unwrap();
        let Some(idx) = st.index.remove(id) else { return false };
        st.order.remove(idx);
        if idx < st.order.len() {
            fix_from(&mut st, idx);
        }
        drop(st);
        self.0.broadcast.send(());
        true
    }

    /// Get an iterator over entity IDs without cloning entities
    pub fn keys(&self) -> EntityResultSetKeyIterator {
        let st = self.0.state.lock().unwrap();
        let keys: Vec<proto::EntityId> = st.order.iter().map(|e| e.id()).collect();
        EntityResultSetKeyIterator::new(keys)
    }

    /// Check if an entity with the given ID exists
    pub fn contains_key(&self, id: &proto::EntityId) -> bool {
        let st = self.0.state.lock().unwrap();
        st.index.contains_key(id)
    }

    pub fn by_id(&self, id: &proto::EntityId) -> Option<E> {
        let st = self.0.state.lock().unwrap();
        st.index.get(id).map(|&i| st.order[i].clone())
    }

    pub fn len(&self) -> usize {
        let st = self.0.state.lock().unwrap();
        st.order.len()
    }
}

fn fix_from<E: AbstractEntity>(st: &mut State<E>, start: usize) {
    // Recompute indices for shifted tail
    for i in start..st.order.len() {
        let id = st.order[i].id();
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
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0.broadcast.reference().listen(listener) }
    fn broadcast_id(&self) -> BroadcastId { self.0.broadcast.id() }
}

impl<R: View> Signal for ResultSet<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0 .0.broadcast.reference().listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0 .0.broadcast.id() }
}

impl<E: View + Clone + 'static> Get<Vec<E>> for ResultSet<E> {
    fn get(&self) -> Vec<E> {
        use ankurah_signals::CurrentObserver;
        CurrentObserver::track(self);
        self.0 .0.state.lock().unwrap().order.iter().map(|e| E::from_entity(e.clone())).collect()
    }
}

impl<E: View + Clone + 'static> Peek<Vec<E>> for ResultSet<E> {
    fn peek(&self) -> Vec<E> { self.0 .0.state.lock().unwrap().order.iter().map(|e| E::from_entity(e.clone())).collect() }
}

impl<E: View + Clone + 'static> Subscribe<Vec<E>> for ResultSet<E> {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<Vec<E>> {
        let listener = listener.into_subscribe_listener();
        let me = self.clone();
        let guard: ListenerGuard<()> = self.0 .0.broadcast.reference().listen(move |_| {
            let entities: Vec<E> = me.0 .0.state.lock().unwrap().order.iter().map(|e| E::from_entity(e.clone())).collect();
            listener(entities);
        });
        SubscriptionGuard::new(guard)
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
            let entity = &state.order[self.index];
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
    pub fn map<R: View>(&self) -> ResultSet<R> { ResultSet(self.clone(), std::marker::PhantomData) }
}
