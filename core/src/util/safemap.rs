use std::{collections::hash_map::Entry, collections::HashMap, hash::Hash};

use super::safeset::SafeSet;

/// A very basic concurrent hashmap that is hard to misuse in an async context.
/// The number one rule is that a lock can only be held very briefly - with no calls into
/// other functions that might block.
#[derive(Default)]
pub struct SafeMap<K: Hash + Eq, V>(std::sync::RwLock<HashMap<K, V>>);

impl<K: Hash + Eq, V> SafeMap<K, V> {
    pub fn new() -> Self { Self(std::sync::RwLock::new(HashMap::new())) }

    pub fn insert(&self, key: K, value: V) { self.0.write().expect("Failed to lock the map").insert(key, value); }

    pub fn remove(&self, key: &K) -> Option<V> { self.0.write().expect("Failed to lock the map").remove(key) }

    pub fn is_empty(&self) -> bool { self.0.read().expect("Failed to lock the map").is_empty() }
    pub fn len(&self) -> usize { self.0.read().expect("Failed to lock the map").len() }
}

impl<K: Hash + Eq, V> SafeMap<K, V>
where V: Clone
{
    pub fn get(&self, k: &K) -> Option<V> { self.0.read().expect("Failed to lock the map").get(k).cloned() }
}

impl<K: Hash + Eq, V> SafeMap<K, V>
where V: Clone + Default
{
    /// get a value from the map. If the key is not present, insert a default value and return a clone of that.
    pub fn get_or_default(&self, k: K) -> V {
        match self.0.write().expect("Failed to lock the map").entry(k) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => v.insert(Default::default()).clone(),
        }
    }
}

impl<K: Hash + Eq, V> SafeMap<K, V>
where
    K: Clone,
    V: Clone,
{
    pub fn to_vec(&self) -> Vec<(K, V)> {
        self.0.read().expect("Failed to lock the map").iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

impl<K: Hash + Eq, H: PartialEq> SafeMap<K, Vec<H>> {
    pub fn push(&self, key: K, value: H) {
        //
        self.0.write().expect("Failed to lock the map").entry(key).or_default().push(value);
    }
    /// Retain only the elements specified by the predicate. NoOp if the key is not present.
    pub fn remove_eq(&self, key: &K, value: &H) {
        match self.0.write().expect("Failed to lock the map").get_mut(key) {
            Some(v) => v.retain(|h| h != value),
            None => (),
        }
    }
}

impl<K: Hash + Eq, H: Hash + Eq> SafeMap<K, std::collections::HashSet<H>> {
    pub fn set_insert(&self, key: K, value: H) { self.0.write().expect("Failed to lock the map").entry(key).or_default().insert(value); }
    pub fn set_remove(&self, key: &K, value: &H) -> bool {
        match self.0.write().expect("Failed to lock the map").get_mut(key) {
            Some(v) => v.remove(value),
            None => false,
        }
    }
}
