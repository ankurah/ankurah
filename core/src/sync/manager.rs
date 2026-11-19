//! Residency and maintenance of core's entity trees.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::{DigestGroup, Ristretto255};
use lru::LruCache;

use super::member::{EntityHead, EntityMemberId, TreeProperty};
use super::{CacheSettings, EngineAdapter, StorageCache};
use crate::error::RetrievalError;
use crate::storage::StorageEngine;

mod continuation;
mod limited;
mod opening;
mod preparation;

/// When a managed tree persists the changes already reflected by its reads.
#[derive(Clone, Copy, Debug)]
pub struct WriteBehind {
    /// Flush after this many changed entities. Zero writes every maintained batch.
    pub max_changes: usize,
    /// Maximum time a completed maintenance batch may remain unflushed.
    pub max_delay: Duration,
}

impl Default for WriteBehind {
    fn default() -> Self { Self { max_changes: 1024, max_delay: Duration::from_millis(250) } }
}

pub(super) type DigestTree<SE, G> =
    ankurah_digest_tree::DigestTree<TreeProperty, EntityMemberId, EntityHead, G, EngineAdapter<SE, TreeProperty, EntityHead, G>>;
type WeakDigestTree<SE, G> =
    ankurah_digest_tree::WeakDigestTree<TreeProperty, EntityMemberId, EntityHead, G, EngineAdapter<SE, TreeProperty, EntityHead, G>>;

/// One node's resident trees. Each tree's store owns record caching; this
/// type owns residency, exact Selection lookup and canonical-state continuation.
///
/// The owner supplies the publication lock shared with its entity commits.
/// Opening holds it through reconciliation and registration. Writers invalidate
/// views before committing canonical state while holding the same lock; reads
/// resume after reconciliation publishes a complete tree version. Storage read
/// boundaries and conditional flushes coordinate independent Nodes sharing an
/// engine, without relying on this process-local lock.
pub struct TreeManager<SE, G: DigestGroup = Ristretto255> {
    storage: Arc<SE>,
    catalog: Weak<crate::schema::catalog::CatalogManager>,
    cache: Arc<StorageCache>,
    publication: Arc<tokio::sync::Mutex<()>>,
    epoch: Arc<AtomicU64>,
    write_behind: Mutex<WriteBehind>,
    // Hold this lock before `retained` when changing registration or recency.
    // Exact Selection encoding includes LIMIT.
    trees: Mutex<HashMap<Vec<u8>, (Selection<Resolved>, WeakDigestTree<SE, G>)>>,
    // None disables idle retention; external owners still keep trees managed.
    retained: Mutex<Option<LruCache<Vec<u8>, DigestTree<SE, G>>>>,
}

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeManager<SE, G> {
    /// Retain at most `capacity` recently used trees for reuse. Callers can keep
    /// additional trees alive; every live tree continues to receive maintenance.
    /// `cache` independently bounds the record bytes cached by the stores.
    pub fn new(storage: Arc<SE>, publication: Arc<tokio::sync::Mutex<()>>, cache: CacheSettings, capacity: usize) -> Self {
        Self {
            storage,
            catalog: Weak::new(),
            publication,
            epoch: Arc::new(AtomicU64::new(0)),
            write_behind: Mutex::new(WriteBehind::default()),
            cache: Arc::new(StorageCache::new(cache)),
            trees: Mutex::new(HashMap::new()),
            retained: Mutex::new(NonZeroUsize::new(capacity).map(LruCache::new)),
        }
    }

    /// Use validated registration types while their canonical definitions are in transit.
    pub(crate) fn with_catalog(mut self, catalog: &Arc<crate::schema::catalog::CatalogManager>) -> Self {
        self.catalog = Arc::downgrade(catalog);
        self
    }

    /// The storage engine used by this manager's trees.
    pub fn storage(&self) -> &Arc<SE> { &self.storage }

    /// Prevent existing views from presenting the pre-commit tree as current.
    /// Call before awaiting a canonical-state commit while holding publication.
    /// Reconciliation is required even if that commit's outcome becomes unknown.
    pub fn invalidate(&self) { self.epoch.fetch_add(1, Ordering::AcqRel); }

    pub fn set_write_behind(&self, policy: WriteBehind) { *self.write_behind.lock().unwrap() = policy; }

    /// The next pending flush deadline, if any managed tree has dirty records.
    pub fn flush_after(&self) -> Option<Duration> {
        let policy = *self.write_behind.lock().unwrap();
        self.trees()
            .iter()
            .filter_map(|tree| {
                let version = tree.store().buffered();
                version.dirty_since.map(|since| {
                    if version.changes >= policy.max_changes {
                        Duration::ZERO
                    } else {
                        policy.max_delay.saturating_sub(since.elapsed())
                    }
                })
            })
            .min()
    }

    /// The tree for this Selection key. Any clone, view, stream or transaction
    /// derived from it keeps the tree alive and receiving maintenance.
    fn tree(&self, key: &[u8]) -> Option<DigestTree<SE, G>> {
        let (tree, evicted) = {
            let mut trees = self.trees.lock().unwrap();
            match trees.get(key).and_then(|(_, digest)| digest.upgrade()) {
                Some(tree) => {
                    let evicted = self.retain(key.to_vec(), tree.clone());
                    (Some(tree), evicted)
                }
                None => {
                    trees.remove(key);
                    (None, None)
                }
            }
        };
        drop(evicted);
        tree
    }

    // Called synchronously after the rebuild commits, while publication is held.
    fn remember(&self, selection: Selection<Resolved>, key: Vec<u8>, tree: DigestTree<SE, G>) {
        let (evicted, live) = {
            let mut trees = self.trees.lock().unwrap();
            let live = Self::live(&mut trees);
            trees.insert(key.clone(), (selection, tree.downgrade()));
            (self.retain(key, tree), live)
        };
        drop(evicted);
        drop(live);
    }

    /// Every live tree, including retained idle trees and trees kept alive by
    /// callers after LRU eviction. Enumeration does not change request recency.
    pub fn trees(&self) -> Vec<DigestTree<SE, G>> {
        Self::live(&mut self.trees.lock().unwrap()).into_iter().map(|(_, tree)| tree).collect()
    }

    /// Upgrade every live entry and prune expired weak references. Holding the
    /// returned trees also keeps the maintenance set alive outside the lock.
    fn live(trees: &mut HashMap<Vec<u8>, (Selection<Resolved>, WeakDigestTree<SE, G>)>) -> Vec<(Selection<Resolved>, DigestTree<SE, G>)> {
        let mut live = Vec::new();
        trees.retain(|_, (selection, digest)| {
            if let Some(tree) = digest.upgrade() {
                live.push((selection.clone(), tree));
                true
            } else {
                false
            }
        });
        live
    }

    /// Retain a requested tree and return references to release outside the lock.
    /// Releasing one never removes a live tree from the weak registry.
    fn retain(&self, key: Vec<u8>, tree: DigestTree<SE, G>) -> Option<DigestTree<SE, G>> {
        match self.retained.lock().unwrap().as_mut() {
            Some(retained) => retained.push(key, tree).map(|(_, removed)| removed),
            None => Some(tree),
        }
    }
}

fn selection_key(selection: &Selection<Resolved>) -> Result<Vec<u8>, RetrievalError> {
    // Display labels are not property identity. Keep the predicate structure,
    // literal bit patterns, ordering and LIMIT, without those incidental names.
    let selection = ankql::selection::map_references::<Resolved, Resolved>(
        selection,
        &|path| {
            let mut identity = ankql::ast::PropertyPath::from(path.property_id());
            identity.subpath = path.subpath.clone();
            identity
        },
        &|model| *model,
    );
    // Selection's float literals preclude Eq/Hash.
    bincode::serialize(&selection).map_err(RetrievalError::storage)
}

#[cfg(test)]
mod tests;
