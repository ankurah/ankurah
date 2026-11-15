//! Metadata and trie records cached by the engine-backed stores, within a
//! shared byte budget. Each tree's records remain in its own namespace.
//!
//! Leaves are never kept: a bucket's members are read by range, and the
//! leaves a commit checks are read once, with the tree's lock, so a cache by
//! key would never answer for them. The crate marks the rows it reads by key
//! [`Residency::Always`], and the cache admits those. Nothing needs a tree's
//! rows to be complete in the cache: a row it lacks is read through storage.
//!
//! Each tree's rows are kept under its rows map, which names its incarnation,
//! so a tree dropped and created again never meets the old tree's rows. They
//! are true at one generation, and are used only by a commit or a read that
//! found the tree at exactly that generation, under its lock or in its
//! snapshot, so no read mixes rows of two generations.
//!
//! The cache takes in only landed states. Each change to a tree's rows,
//! catching up from the change log, forgetting them, filling a row a read
//! found, or taking in a commit that landed, is read against the tree's
//! generation in the cache, and is made only if that generation is still the
//! one: otherwise it is dropped. A storage transaction's own writes enter
//! only once it has committed, when its owner says they landed. A tree's
//! generation in the cache can fall back, its rows evicted and refilled by a
//! read older than they were; its rows are true at whichever it is.
//!
//! Rows stay as their bytes until first read: catching up brings in every row
//! the other nodes' commits wrote, of which one read uses few, and decoding a
//! digest isn't free. Over budget, the rows used least recently go. A dropped
//! tree's rows go that way too: kept under the rows map of its incarnation,
//! they are never read again.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Mutex;

use ankurah_digest_tree::{Residency, RowKey};
use lru::LruCache;

use crate::storage::MapId;
use crate::sync::storage_adapter::layout::log::Changed;

/// How many encoded record bytes the storage adapters' shared cache may keep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheSettings {
    /// The most bytes of rows it keeps, keys and rows together, counted as
    /// they are stored; a row it has decoded takes more memory than that.
    /// Zero keeps none, and every row is read through storage.
    pub bytes: usize,
}

/// A shared record-byte budget for stores using the same storage engine.
/// Records remain scoped to each tree's rows map. Adapters supply those map
/// identities; the cache has no storage-engine dependency.
pub struct StorageCache {
    state: Mutex<CacheState>,
}

impl StorageCache {
    pub fn new(settings: CacheSettings) -> Self { Self { state: Mutex::new(CacheState::new(settings)) } }

    pub(super) fn generation(&self, rows: &MapId) -> Option<u64> { self.state.lock().unwrap().generation(rows) }

    pub(super) fn get_many<R, E>(
        &self,
        rows: &MapId,
        generation: u64,
        keys: impl IntoIterator<Item = Vec<u8>>,
        decode: impl Fn(&[u8]) -> Result<R, E>,
    ) -> Vec<Option<R>>
    where
        R: Clone + Send + Sync + 'static,
        E: Display,
    {
        let mut state = self.state.lock().unwrap();
        keys.into_iter().map(|key| state.get(rows, generation, &key, &decode)).collect()
    }

    pub(super) fn fill_many<R: Send + Sync + 'static>(
        &self,
        rows: &MapId,
        generation: u64,
        values: impl IntoIterator<Item = (Vec<u8>, R, usize)>,
    ) {
        let mut state = self.state.lock().unwrap();
        for (key, row, stored) in values {
            state.fill(rows, generation, key, row, stored);
        }
    }

    pub(super) fn land(&self, rows: &MapId, generation: u64, changed: Changed) {
        self.state.lock().unwrap().land(rows, generation, changed);
    }

    pub(super) fn catch_up(&self, rows: &MapId, from: u64, to: u64, changed: Changed) {
        self.state.lock().unwrap().catch_up(rows, from, to, changed);
    }

    pub(super) fn forget(&self, rows: &MapId, from: u64) { self.state.lock().unwrap().forget(rows, from); }
}

/// Cached records from the stores sharing this byte budget.
struct CacheState {
    budget: usize,

    /// The bytes of the rows kept, keys and rows together.
    used: usize,

    /// Each tree that has rows kept, by its rows map: a tree with none has
    /// no generation in the cache.
    trees: HashMap<MapId, CachedTree>,

    /// Every kept row, by the number it was kept under, with its tree and
    /// key: the least recently used first.
    recency: LruCache<u64, (MapId, Vec<u8>)>,

    /// The number the next row kept is kept under.
    next: u64,
}

/// One tree's kept rows, true at one generation.
struct CachedTree {
    generation: u64,
    rows: HashMap<Vec<u8>, CachedRow>,
}

/// A kept row: its bytes, decoded once it is first read.
struct CachedRow {
    /// What the row costs of the budget: its key's and its bytes' lengths.
    cost: usize,

    /// The number it is kept under in the recency list.
    number: u64,

    row: Kept,
}

enum Kept {
    Encoded(Vec<u8>),

    /// The row as the crate's type, for a tree of whatever types.
    Decoded(Box<dyn Any + Send + Sync>),
}

/// Whether the cache keeps the row under `key`: the crate reads it by key on
/// almost every operation.
pub(in crate::sync) fn admits(key: &RowKey) -> bool { key.residency() == Residency::Always }

impl CacheState {
    fn new(settings: CacheSettings) -> Self {
        Self { budget: settings.bytes, used: 0, trees: HashMap::new(), recency: LruCache::unbounded(), next: 0 }
    }

    /// The generation the tree whose rows map is `rows` has rows kept at, if
    /// it has any kept.
    fn generation(&self, rows: &MapId) -> Option<u64> { self.trees.get(rows).map(|tree| tree.generation) }

    /// The row under `key`, of the tree whose rows map is `rows`, if the cache
    /// keeps it at exactly `generation`: decoded by `decode` the first time it
    /// is read. A row decoded as another type than `R` is not answered.
    ///
    /// Nor is a row whose bytes `decode` refuses, as a damaged change-log entry
    /// can leave one: the cache logs it and forgets every row it kept of the
    /// tree, which is then read through storage.
    fn get<R, E>(&mut self, rows: &MapId, generation: u64, key: &[u8], decode: impl FnOnce(&[u8]) -> Result<R, E>) -> Option<R>
    where
        R: Clone + Send + Sync + 'static,
        E: Display,
    {
        let tree = self.trees.get_mut(rows).filter(|tree| tree.generation == generation)?;
        let cached = tree.rows.get_mut(key)?;

        let row = match &cached.row {
            Kept::Decoded(row) => row.downcast_ref::<R>()?.clone(),

            Kept::Encoded(bytes) => {
                let row = match decode(bytes) {
                    Ok(row) => row,

                    Err(damage) => {
                        tracing::warn!("{damage}, as the record cache kept it: the cache forgets the records it kept of the tree");
                        self.forget(rows, generation);
                        return None;
                    }
                };

                cached.row = Kept::Decoded(Box::new(row.clone()));
                row
            }
        };

        self.recency.promote(&cached.number);
        Some(row)
    }

    /// Keep `row`, which a read of the tree whose rows map is `rows` found
    /// under `key` at `generation`, whose stored bytes are `stored` long: if
    /// the tree's kept rows are at that generation, or it has none kept.
    fn fill<R: Send + Sync + 'static>(&mut self, rows: &MapId, generation: u64, key: Vec<u8>, row: R, stored: usize) {
        if !self.may_change(rows, |kept| kept.is_none_or(|kept| kept == generation)) {
            return;
        }

        self.tree(rows, generation);
        let cost = key.len() + stored;
        self.keep(rows, key, cost, Kept::Decoded(Box::new(row)));
        self.evict();
    }

    /// Take in the rows a commit of the tree whose rows map is `rows` wrote,
    /// which landed at `generation`: if the tree's kept rows are at the
    /// generation before, or it has none kept. A creation lands at generation
    /// 0, and only on a tree with none kept.
    fn land(&mut self, rows: &MapId, generation: u64, changed: Changed) {
        let before = generation.checked_sub(1);
        if !self.may_change(rows, |kept| kept.is_none() || kept == before) {
            return;
        }

        self.change(rows, generation, changed);
    }

    /// Bring the kept rows of the tree whose rows map is `rows` from
    /// generation `from` to `to`, with the rows the change log says changed
    /// between: if they are still at `from`.
    fn catch_up(&mut self, rows: &MapId, from: u64, to: u64, changed: Changed) {
        if self.generation(rows) == Some(from) {
            self.change(rows, to, changed);
        }
    }

    /// Forget every kept row of the tree whose rows map is `rows`, if they
    /// are still at generation `from`: the change log can't bring them on.
    fn forget(&mut self, rows: &MapId, from: u64) {
        if self.generation(rows) != Some(from) {
            return;
        }

        let Some(tree) = self.trees.remove(rows) else { return };
        for row in tree.rows.into_values() {
            self.used -= row.cost;
            self.recency.pop(&row.number);
        }
    }

    /// Whether the cache keeps anything, and the generation the tree's rows
    /// are kept at, if any, passes `check`.
    fn may_change(&self, rows: &MapId, check: impl FnOnce(Option<u64>) -> bool) -> bool { self.budget > 0 && check(self.generation(rows)) }

    /// Put and delete `changed` among the kept rows of the tree whose rows
    /// map is `rows`, and take the rows to be at `generation`.
    fn change(&mut self, rows: &MapId, generation: u64, changed: Changed) {
        self.tree(rows, generation);

        for (key, row) in changed {
            match row {
                Some(bytes) => {
                    let cost = key.len() + bytes.len();
                    self.keep(rows, key, cost, Kept::Encoded(bytes));
                }

                None => self.remove(rows, &key),
            }
        }

        self.drop_if_empty(rows);
        self.evict();
    }

    /// The tree's kept rows, taken to be at `generation`.
    fn tree(&mut self, rows: &MapId, generation: u64) {
        self.trees.entry(rows.clone()).or_insert_with(|| CachedTree { generation, rows: HashMap::new() }).generation = generation;
    }

    /// Keep `row` under `key` among the rows of the tree whose rows map is
    /// `rows`, at `cost` of the budget. The recency list keeps the tree and
    /// key too, to find the row by when it evicts it.
    fn keep(&mut self, rows: &MapId, key: Vec<u8>, cost: usize, row: Kept) {
        let number = self.next;
        self.next += 1;

        self.used += cost;
        self.recency.push(number, (rows.clone(), key.clone()));

        let tree = self.trees.get_mut(rows).expect("a tree is kept before its rows");
        if let Some(replaced) = tree.rows.insert(key, CachedRow { cost, number, row }) {
            self.used -= replaced.cost;
            self.recency.pop(&replaced.number);
        }
    }

    fn remove(&mut self, rows: &MapId, key: &[u8]) {
        let tree = self.trees.get_mut(rows).expect("a tree is kept before its rows");

        if let Some(removed) = tree.rows.remove(key) {
            self.used -= removed.cost;
            self.recency.pop(&removed.number);
        }
    }

    /// A tree with no rows kept has no generation in the cache.
    fn drop_if_empty(&mut self, rows: &MapId) {
        if self.trees.get(rows).is_some_and(|tree| tree.rows.is_empty()) {
            self.trees.remove(rows);
        }
    }

    /// Drop the rows used least recently until the rest fit the budget.
    fn evict(&mut self) {
        while self.used > self.budget {
            let Some((_, (rows, key))) = self.recency.pop_lru() else { return };
            self.remove(&rows, &key);
            self.drop_if_empty(&rows);
        }
    }
}

#[cfg(test)]
mod tests;
