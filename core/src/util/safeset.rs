use std::{collections::HashSet, hash::Hash};

/// A very basic concurrent hashset that is hard to misuse in an async context.
/// The number one rule is that a lock can only be held very briefly - with no calls into
/// other functions that might block.
pub struct SafeSet<T: Hash + Eq>(std::sync::RwLock<HashSet<T>>);

impl<T: Hash + Eq> SafeSet<T> {
    pub fn new() -> Self { Self(std::sync::RwLock::new(HashSet::new())) }

    pub fn insert(&self, value: T) -> bool { self.0.write().expect("Failed to lock the set").insert(value) }

    pub fn remove(&self, value: &T) -> bool { self.0.write().expect("Failed to lock the set").remove(value) }

    pub fn contains(&self, value: &T) -> bool { self.0.read().expect("Failed to lock the set").contains(value) }

    pub fn is_empty(&self) -> bool { self.0.read().expect("Failed to lock the set").is_empty() }

    pub fn len(&self) -> usize { self.0.read().expect("Failed to lock the set").len() }
}

impl<T: Hash + Eq + Clone> SafeSet<T> {
    pub fn to_vec(&self) -> Vec<T> { self.0.read().expect("Failed to lock the set").iter().cloned().collect() }
}

impl<T: Hash + Eq + std::fmt::Debug> std::fmt::Debug for SafeSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SafeSet {{ {:?} }}", self.0.read().expect("Failed to lock the set"))
    }
}

impl<T: Hash + Eq> Default for SafeSet<T> {
    fn default() -> Self { Self::new() }
}
