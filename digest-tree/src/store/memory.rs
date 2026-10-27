//! The in-memory reference store.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, RwLock};

use super::{Store, WriteItem};
use crate::span::holds_nothing;
use crate::{ScanDirection, StoreError};

/// A reference store that keeps one tree's rows in memory, in a `BTreeMap`
/// behind a lock. Clones share the rows.
///
/// A write applies its whole batch under the lock, so every read sees all of
/// it or none of it. No lock is held across an await.
#[derive(Debug)]
pub struct MemoryStore<K, V> {
    rows: Arc<RwLock<BTreeMap<K, V>>>,
}

impl<K, V> MemoryStore<K, V> {
    /// An empty store.
    pub fn new() -> Self { Self { rows: Arc::new(RwLock::new(BTreeMap::new())) } }
}

impl<K, V> Default for MemoryStore<K, V> {
    fn default() -> Self { Self::new() }
}

impl<K, V> Clone for MemoryStore<K, V> {
    fn clone(&self) -> Self { Self { rows: Arc::clone(&self.rows) } }
}

/// The lock is poisoned only when a key's or row's own code panicked while a
/// write held it, which may have left the batch half applied.
fn poisoned<T>(_: T) -> StoreError { StoreError::permanent("a panic during a write left the memory store's rows undefined") }

impl<K, V> Store<K, V> for MemoryStore<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
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

    async fn write(&self, items: Vec<WriteItem<K, V>>) -> Result<(), StoreError> {
        let mut rows = self.rows.write().map_err(poisoned)?;
        for item in items {
            match item {
                WriteItem::Put(key, row) => rows.insert(key, row),
                WriteItem::Delete(key) => rows.remove(&key),
            };
        }

        Ok(())
    }
}
