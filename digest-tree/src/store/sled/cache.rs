//! The rows a sled store keeps in memory, as their keys' residency asks.

use std::collections::BTreeMap;

use crate::store::{Residency, RowKey, WriteItem};

/// The rows a store keeps in memory, and how many writes have landed.
pub(super) struct Cache<V> {
    /// Rows whose keys ask to stay in memory while the tree is open.
    always: BTreeMap<RowKey, V>,

    /// Rows kept while recently used.
    recent: Recent<RowKey, V>,

    /// How many writes have landed. A read notes it before going to disk and
    /// fills the cache only if no write landed meanwhile, since the rows it
    /// read may be ones that write replaced.
    writes: u64,
}

impl<V: Clone> Cache<V> {
    /// An empty cache that keeps at most `capacity` rows while recently used.
    pub(super) fn new(capacity: usize) -> Self { Self { always: BTreeMap::new(), recent: Recent::new(capacity), writes: 0 } }

    /// The row under `key`, if the cache holds it; a recently used row
    /// becomes the most recently used.
    pub(super) fn get(&mut self, key: &RowKey) -> Option<V> { self.always.get(key).cloned().or_else(|| self.recent.get(key)) }

    /// The row under `key`, if the cache holds it, leaving recency alone: for
    /// a scan, which passes rows its reader won't come back to.
    pub(super) fn peek(&self, key: &RowKey) -> Option<V> { self.always.get(key).or_else(|| self.recent.peek(key)).cloned() }

    /// How many writes have landed: what a read notes before going to disk.
    pub(super) fn writes(&self) -> u64 { self.writes }

    /// Keep rows a read found on disk, unless a write has landed since the
    /// read noted `writes`.
    pub(super) fn fill(&mut self, writes: u64, rows: impl IntoIterator<Item = (RowKey, V)>) {
        if writes == self.writes {
            rows.into_iter().for_each(|(key, row)| self.keep(key, row));
        }
    }

    /// Keep the rows a range read found on disk that stay in memory always.
    /// A scan passes rows its reader won't come back to, so the rows kept
    /// while recently used are left to the reads that ask for them one by
    /// one, rather than pushed out by the scan.
    pub(super) fn fill_scanned(&mut self, writes: u64, rows: &[(RowKey, V)]) {
        let always = rows.iter().filter(|(key, _)| key.residency() == Residency::Always).cloned();
        self.fill(writes, always);
    }

    /// Reflect a write that landed on disk. Rows that stay in memory always
    /// are kept; rows kept while recently used are updated where the cache
    /// holds them, and otherwise left to the reads that ask for them, so a
    /// build doesn't push every recently used row out.
    pub(super) fn apply(&mut self, items: Vec<WriteItem<RowKey, V>>) {
        for item in items {
            match item {
                WriteItem::Put(key, row) => match key.residency() {
                    Residency::Always => {
                        self.always.insert(key, row);
                    }
                    Residency::Recent => self.recent.update(&key, row),
                },

                WriteItem::Delete(key) => {
                    self.always.remove(&key);
                    self.recent.remove(&key);
                }
            }
        }

        self.writes += 1;
    }

    fn keep(&mut self, key: RowKey, row: V) {
        match key.residency() {
            Residency::Always => {
                self.always.insert(key, row);
            }
            Residency::Recent => self.recent.put(key, row),
        }
    }
}

/// Rows kept while recently used: at most `capacity`, the least recently
/// used leaving first.
struct Recent<K, V> {
    capacity: usize,

    /// Each row, with the moment it was last used.
    rows: BTreeMap<K, (V, u64)>,

    /// Each moment of use, with its row's key, the oldest first.
    uses: BTreeMap<u64, K>,

    /// The next moment of use.
    clock: u64,
}

impl<K: Ord + Clone, V: Clone> Recent<K, V> {
    fn new(capacity: usize) -> Self { Self { capacity, rows: BTreeMap::new(), uses: BTreeMap::new(), clock: 0 } }

    fn get(&mut self, key: &K) -> Option<V> {
        let (row, used) = self.rows.get_mut(key)?;
        self.uses.remove(used);
        *used = self.clock;
        self.uses.insert(self.clock, key.clone());
        self.clock += 1;

        Some(row.clone())
    }

    fn peek(&self, key: &K) -> Option<&V> { self.rows.get(key).map(|(row, _)| row) }

    fn put(&mut self, key: K, row: V) {
        self.remove(&key);
        self.rows.insert(key.clone(), (row, self.clock));
        self.uses.insert(self.clock, key);
        self.clock += 1;

        while self.rows.len() > self.capacity {
            let (_, oldest) = self.uses.pop_first().expect("every row has a moment of use");
            self.rows.remove(&oldest);
        }
    }

    /// Replace the row under `key`, if the cache holds one.
    fn update(&mut self, key: &K, row: V) {
        if let Some((held, _)) = self.rows.get_mut(key) {
            *held = row;
        }
    }

    fn remove(&mut self, key: &K) {
        if let Some((_, used)) = self.rows.remove(key) {
            self.uses.remove(&used);
        }
    }
}

#[cfg(test)]
mod tests;
