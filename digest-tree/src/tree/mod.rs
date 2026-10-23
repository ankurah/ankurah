//! The digest tree: its spec and the memory-resident maps that hold its rows,
//! and the transactions that carry its operations.
//!
//! Every write is conditional on what its transaction read, so two changes to
//! one leaf, or to one tree node's base row, never overwrite each other: the
//! second meets [`TreeError::Conflict`], which fails its transaction whole, and
//! retries from fresh reads.
//!
//! A tree node's base row is written when the tree is built. Every later
//! change to the tree node is a delta row appended beside it, under a key of
//! its own, which readers sum, so no change rewrites a hot tree node's row and
//! changes do not serialize on the root; compaction folds the deltas into the
//! base, a bounded batch per transaction.

use std::marker::PhantomData;

use crate::digest::{DigestGroup, Ristretto255};
use crate::error::TreeError;
use crate::selection::TreeSpec;
use crate::store::{MapAccess, MapTransaction, MemoryMaps, MemoryTransaction};

mod audit;
mod build;
mod change;
#[cfg(test)]
mod fixtures;
mod leaves;
mod nodes;
mod scope;

pub use audit::Audit;
pub use build::BUCKET_CAPACITY;
pub use change::{LeafChange, LeafData};
pub use scope::{LeafCursor, LeafPage, TreeScope};

/// A digest tree: the spec that defines which leaves it admits and the order
/// it keeps them in, and the memory-resident maps that hold its rows, with its
/// leaves summed in the group `G`, ristretto255 unless the tree names another.
///
/// Its operations run in transactions: [`DigestTree::begin`] gives one.
#[derive(Debug)]
pub struct DigestTree<G: DigestGroup = Ristretto255> {
    spec: TreeSpec,
    maps: MemoryMaps,
    group: PhantomData<G>,
}

impl<G: DigestGroup> DigestTree<G> {
    /// An empty tree defined by `spec`.
    pub fn new(spec: TreeSpec) -> Self { Self { spec, maps: MemoryMaps::new(), group: PhantomData } }

    /// The spec that defines the tree.
    pub fn spec(&self) -> &TreeSpec { &self.spec }

    /// Begin a transaction over the tree's maps as they stand now, with the
    /// tree's operations.
    pub fn begin(&self) -> TreeTransaction<G> { TreeTransaction::bind(&self.spec, self.maps.begin()) }
}

/// One transaction over a tree's maps, with the tree's operations, its leaves
/// summed in the group `G`.
///
/// Every operation runs within the transaction: its reads observe the state
/// the transaction began with and the transaction's own earlier writes, and
/// its writes land, all together, only when the owning transaction commits.
/// [`TreeTransaction::commit`] is available when this view owns its maps.
/// A borrowed view leaves commit to its outer owner.
/// A [`TreeError::Conflict`] from any operation or from the commit
/// fails the transaction whole, and the caller retries from fresh reads in a
/// new one. The operations below are the only writers of the maps' contents;
/// the layouts of the keys and values are in [`layout`].
#[derive(Debug)]
pub struct TreeTransaction<G: DigestGroup = Ristretto255, T = MemoryTransaction> {
    pub(crate) transaction: T,
    group: PhantomData<G>,
    pub(crate) spec: TreeSpec,
    pub(crate) failed: bool,
    pub(crate) generation: u64,
    pub(crate) snapshot: std::sync::Arc<()>,
}

impl<G: DigestGroup, T: MapTransaction> TreeTransaction<G, T> {
    /// Commit the owned maps together, if every condition still holds.
    /// Failure publishes nothing. Borrowed map views have no commit method.
    pub async fn commit(self) -> Result<(), TreeError> {
        if self.failed {
            return Err(TreeError::Conflict);
        }
        self.transaction.commit().await
    }
}

/// Keeps partial work from escaping through an outer transaction's commit,
/// including when a mutation future is dropped before returning a result.
pub(crate) struct Mutation<'a, G: DigestGroup, T: MapAccess> {
    tree: &'a mut TreeTransaction<G, T>,
    completed: bool,
}

impl<G: DigestGroup, T: MapAccess> Mutation<'_, G, T> {
    pub(crate) fn finish<R>(mut self, result: Result<R, TreeError>) -> Result<R, TreeError> {
        self.completed = result.is_ok();
        result
    }
}

impl<G: DigestGroup, T: MapAccess> std::ops::Deref for Mutation<'_, G, T> {
    type Target = TreeTransaction<G, T>;
    fn deref(&self) -> &Self::Target { self.tree }
}

impl<G: DigestGroup, T: MapAccess> std::ops::DerefMut for Mutation<'_, G, T> {
    fn deref_mut(&mut self) -> &mut Self::Target { self.tree }
}

impl<G: DigestGroup, T: MapAccess> Drop for Mutation<'_, G, T> {
    fn drop(&mut self) {
        if !self.completed {
            self.tree.failed = true;
            self.tree.transaction.poison();
        }
    }
}

/// How many entries a read of a whole range takes at a time.
const PAGE: usize = 1024;

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// Use these transaction-bound maps as the tree described by `spec`.
    /// The caller supplies this tree's maps and matching spec. Admission is
    /// still the caller's responsibility; the tree validates canonical keys.
    /// Passing a borrowed adapter leaves commit with the outer transaction.
    /// Scopes and cursors belong to this view and must be resolved again when
    /// a new view is opened, even within the same outer transaction.
    ///
    /// ```
    /// # async fn example() -> Result<(), ankurah_digest_tree::TreeError> {
    /// use ankurah_digest_tree::{MapTransaction, MemoryMaps, Ristretto255, TreeSpec, TreeTransaction};
    /// use ankurah_core_types::KeySpec;
    /// let spec = TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True);
    /// let maps = MemoryMaps::new();
    /// let mut owner = maps.begin();
    /// {
    ///     let mut tree = TreeTransaction::<Ristretto255, _>::bind(&spec, &mut owner);
    ///     tree.build([]).await?;
    /// }
    /// owner.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Borrowing maps does not grant permission to commit them:
    ///
    /// ```compile_fail
    /// # async fn example(spec: &ankurah_digest_tree::TreeSpec) {
    /// use ankurah_digest_tree::{MemoryMaps, Ristretto255, TreeTransaction};
    /// let maps = MemoryMaps::new();
    /// let mut owner = maps.begin();
    /// let tree = TreeTransaction::<Ristretto255, _>::bind(spec, &mut owner);
    /// tree.commit().await.unwrap();
    /// # }
    /// ```
    pub fn bind(spec: &TreeSpec, maps: T) -> Self {
        Self { transaction: maps, group: PhantomData, spec: spec.clone(), failed: false, generation: 0, snapshot: std::sync::Arc::new(()) }
    }

    pub(crate) fn mutation(&mut self) -> Mutation<'_, G, T> { Mutation { tree: self, completed: false } }
}
