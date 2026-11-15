//! A digest-tree Store over StorageEngine. Reads use engine snapshots;
//! mutations use the caller's transaction, with cache publication after commit.

use std::marker::PhantomData;
use std::sync::Arc;

use ankurah_digest_tree::{DigestGroup, Encode, Row, RowKey, Store, StoreError};

use self::layout::entry::{self, TreeEntry};
use self::layout::log::{self, Changed};
use self::layout::names::Incarnation;
use super::error::snapshot_store_failure;
use super::Error;
use crate::storage::{LockName, MapId, MapRows, MapSnapshot, StorageEngine};

mod apply;
mod by_key;
mod cache;
mod guard;
mod layout;
mod lifecycle;
mod read;
mod rows;
mod trx;

pub use apply::{apply, AppliedTrees, TreeChange};
pub use cache::{CacheSettings, StorageCache};
pub use layout::names::TreeId;
pub use lifecycle::{create, drop, open, rebuild};
pub use read::StorageEngineSnapshot;
pub use trx::StorageEngineTrx;

/// A Store for one tree at a fixed incarnation. Clones share the same store.
/// Each handle selects only that tree's maps, so store operations need no tree
/// id and unbounded ranges stay within that tree. Stores can share a
/// StorageCache to pool their byte budget; records remain scoped to the tree.
pub struct EngineAdapter<SE, K, L, G> {
    inner: Arc<Inner<SE>>,
    _rows: PhantomData<fn() -> (K, L, G)>,
}

/// Shared adapter state for one tree incarnation: storage, cache, identity,
/// and the names of its lock and maps.
pub(super) struct Inner<SE> {
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    id: TreeId,
    incarnation: Incarnation,
    lock: LockName,
    rows: MapId,
    log: MapId,
}

/// A tree write's generation and cached row changes, published after the
/// owner's storage transaction commits. An empty write keeps nothing.
pub struct TreeLanding {
    written: Option<(u64, Changed)>,
}

impl TreeLanding {
    /// The landing of a write that wrote nothing.
    fn nothing() -> Self { Self { written: None } }

    /// The landing of a write that brought the tree to `generation`, changing
    /// `changed`.
    fn of(generation: u64, changed: Changed) -> Self { Self { written: Some((generation, changed)) } }
}

impl<SE: StorageEngine> Inner<SE> {
    fn new(storage: Arc<SE>, cache: Arc<StorageCache>, id: TreeId, incarnation: Incarnation) -> Self {
        Self { storage, cache, lock: id.lock(), rows: incarnation.rows(), log: incarnation.log(), id, incarnation }
    }

    /// The generation the tree's rows are kept at in the cache, if any are.
    fn cached_generation(&self) -> Option<u64> { self.cache.generation(&self.rows) }

    /// The generation the tree's rows are at, from `answer`, the answer to a
    /// read of its entry: a tree retired since this handle was opened fails
    /// the read.
    fn generation_found(&self, answer: Option<MapRows>) -> Result<u64, StoreError> {
        let found = entry::found(&[&self.id], answer).and_then(|mut entries| entries.pop().expect("an entry for the one id"));
        let entry = found.map_err(StoreError::permanent)?;
        let current = TreeEntry::current(entry, self.incarnation).ok_or_else(|| StoreError::permanent(Error::Retired(self.id.clone())))?;

        Ok(current.generation)
    }

    /// Bring the tree's rows in the cache from generation `from`, where they
    /// were kept when a read or a commit began, to `to`, the generation it
    /// found the tree at, from `answer`, the answer to a read of the change
    /// log after `from`: row by row where the log reaches from one to the
    /// other, and otherwise by forgetting them.
    ///
    /// A cache past `to` is forgotten too. Every generation the cache takes
    /// the rows to had landed, and a read or a commit sees every commit that
    /// landed before it began, so a cache ahead of what one found can only
    /// hold rows its owner said landed and didn't.
    fn catch_up(&self, from: u64, to: u64, answer: Option<MapRows>) -> Result<(), Error> {
        if from == to {
            return Ok(());
        }

        if from > to {
            self.cache.forget(&self.rows, from);
            return Ok(());
        }

        let Some(MapRows::Ranged(entries)) = answer else { return Err(Error::AnswerMisfit) };
        let changed = log::changes(&self.id, from, to, entries);

        let cache = &self.cache;
        match changed {
            Some(changed) => cache.catch_up(&self.rows, from, to, changed),
            None => cache.forget(&self.rows, from),
        }

        Ok(())
    }
}

impl<SE: StorageEngine, K, L, G> EngineAdapter<SE, K, L, G> {
    fn new(inner: Inner<SE>) -> Self { Self { inner: Arc::new(inner), _rows: PhantomData } }

    /// The adapter state shared by its clones.
    pub(super) fn inner(&self) -> &Inner<SE> { &self.inner }

    /// Whether this store uses the given engine instance.
    pub(in crate::sync) fn uses_storage(&self, storage: &Arc<SE>) -> bool { Arc::ptr_eq(&self.inner.storage, storage) }

    /// Whether both stores address the same incarnation on the same engine.
    pub(in crate::sync) fn same_tree(&self, other: &Self) -> bool {
        self.uses_storage(&other.inner.storage) && self.inner.rows == other.inner.rows
    }

    /// The id of the tree whose rows this store holds, by which an apply
    /// names the trees it retired and those that refused.
    pub fn id(&self) -> &TreeId { &self.inner.id }
}

impl<SE: StorageEngine, K, L, G> Clone for EngineAdapter<SE, K, L, G> {
    fn clone(&self) -> Self { Self { inner: Arc::clone(&self.inner), _rows: PhantomData } }
}

impl<SE, K, L, G> Store<RowKey, Row<K, L, G>> for EngineAdapter<SE, K, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    type Read = StorageEngineSnapshot<SE, K, L, G>;
    type Trx<'a> = StorageEngineTrx<'a, SE, K, L, G>;
    type Pending = TreeLanding;

    /// A snapshot of the engine's maps under the tree's lock, whose first
    /// request reads the tree's entry, and the change log after the
    /// generation the cache keeps its rows at, to bring them up to date. A
    /// tree dropped, created again or marked invalid since this handle was
    /// opened fails the read.
    async fn begin_read(&self) -> Result<StorageEngineSnapshot<SE, K, L, G>, StoreError> {
        let tree = self.inner();
        // Sampled before the snapshot begins, so a cache ahead of what it
        // finds can only hold rows said to have landed that didn't.
        let cached = tree.cached_generation();
        let mut snapshot = tree.storage.map_snapshot(std::slice::from_ref(&tree.lock)).await.map_err(snapshot_store_failure)?;

        let mut reads = vec![entry::read([&tree.id])];
        reads.extend(cached.map(|from| log::read_after(tree.log.clone(), from)));
        let mut answers = snapshot.map_read(reads).await.map_err(snapshot_store_failure)?.into_iter();

        let generation = tree.generation_found(answers.next())?;
        if let Some(from) = cached {
            tree.catch_up(from, generation, answers.next()).map_err(StoreError::permanent)?;
        }

        Ok(StorageEngineSnapshot::new(self.clone(), snapshot, generation))
    }

    /// A transaction on the same incarnation of the same tree, in the same
    /// engine, whichever handle it came from.
    fn owns(&self, trx: &StorageEngineTrx<'_, SE, K, L, G>) -> bool { self.same_tree(trx.store()) }

    fn landed(&self, landing: TreeLanding) {
        if let Some((generation, changed)) = landing.written {
            self.inner().cache.land(&self.inner().rows, generation, changed);
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-helpers"))]
pub mod conformance;
