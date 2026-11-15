//! Residency and maintenance of core's entity trees.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::{DigestGroup, Ristretto255};
use lru::LruCache;

use super::member::{EntityHead, EntityMemberId, TreeProperty};
use super::{CacheSettings, EngineAdapter, StorageCache};
use crate::error::RetrievalError;
use crate::storage::StorageEngine;

mod maintenance;
mod opening;

pub(super) type DigestTree<SE, G> =
    ankurah_digest_tree::DigestTree<TreeProperty, EntityMemberId, EntityHead, G, EngineAdapter<SE, TreeProperty, EntityHead, G>>;
type WeakDigestTree<SE, G> =
    ankurah_digest_tree::WeakDigestTree<TreeProperty, EntityMemberId, EntityHead, G, EngineAdapter<SE, TreeProperty, EntityHead, G>>;

/// One node's resident trees. Each tree's store owns record caching; this
/// type owns residency, exact Selection lookup and entity-change routing.
///
/// The owner supplies the publication lock shared with its entity commits.
/// Opening holds it through the initial scan, rebuild and registration; writers
/// hold it from before maintenance through storage commit and publication.
/// Coordination of entity writes from other Nodes belongs to the integration.
pub struct TreeManager<SE, G: DigestGroup = Ristretto255> {
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    publication: Arc<tokio::sync::Mutex<()>>,
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
            publication,
            cache: Arc::new(StorageCache::new(cache)),
            trees: Mutex::new(HashMap::new()),
            retained: Mutex::new(NonZeroUsize::new(capacity).map(LruCache::new)),
        }
    }

    /// The storage engine used by this manager's trees.
    pub fn storage(&self) -> &Arc<SE> { &self.storage }

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
