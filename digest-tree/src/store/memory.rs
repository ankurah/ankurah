//! The in-memory reference store.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

use super::{Locked, OrderedMap, WriteItem};
use crate::span::holds_nothing;
use crate::{ScanDirection, StoreError};

/// A store that keeps one tree's rows wholly in memory, for tests: a
/// [`MemoryMap`] behind the locks of a [`Locked`] store. Clones share the
/// rows.
pub type MemoryStore<K, V> = Locked<MemoryMap<K, V>>;

impl<K, V> Locked<MemoryMap<K, V>> {
    /// An empty store.
    pub fn new() -> Self { Self::over(MemoryMap::new()) }
}

/// One tree's rows in memory, in a `BTreeMap`: the map of a [`MemoryStore`].
/// A batch applies whole under the map's lock, so every read sees all of it or
/// none of it, and no lock is held across an await.
#[derive(Debug)]
pub struct MemoryMap<K, V> {
    rows: RwLock<BTreeMap<K, V>>,
}

impl<K, V> MemoryMap<K, V> {
    /// An empty map.
    pub fn new() -> Self { Self { rows: RwLock::new(BTreeMap::new()) } }
}

/// The lock is poisoned only when a key's or row's own code panicked while a
/// batch held it, which may have left the batch half applied.
fn poisoned<T>(_: T) -> StoreError { StoreError::permanent("a panic during a write left the memory store's rows undefined") }

impl<K, V> OrderedMap for MemoryMap<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    type Key = K;
    type Value = V;

    async fn get(&self, key: &K) -> Result<Option<V>, StoreError> { Ok(self.rows.read().map_err(poisoned)?.get(key).cloned()) }

    async fn range(&self, start: Bound<&K>, end: Bound<&K>, direction: ScanDirection, limit: usize) -> Result<Vec<(K, V)>, StoreError> {
        let rows = self.rows.read().map_err(poisoned)?;

        // `BTreeMap::range` panics on bounds that hold nothing.
        if holds_nothing(start, end) {
            return Ok(Vec::new());
        }

        let range = rows.range((start, end)).map(|(key, row)| (key.clone(), row.clone()));
        Ok(match direction {
            ScanDirection::Forward => range.take(limit).collect(),
            ScanDirection::Reverse => range.rev().take(limit).collect(),
        })
    }

    async fn apply(&self, batch: Vec<WriteItem<K, V>>) -> Result<(), StoreError> {
        let mut rows = self.rows.write().map_err(poisoned)?;
        for item in batch {
            match item {
                WriteItem::Put(key, row) => rows.insert(key, row),
                WriteItem::Delete(key) => rows.remove(&key),
            };
        }

        Ok(())
    }
}
