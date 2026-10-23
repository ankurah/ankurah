//! The digest tree: its spec and the memory-resident maps that hold its rows,
//! and the transactions that carry its operations.
//!
//! Every write is conditional on what its transaction read, so two changes to
//! one leaf, or to one tree node's base row, never overwrite each other: the
//! second meets [`TreeError::Conflict`], which fails its transaction whole, and
//! retries from fresh reads.
//!
//! A tree node's base row is written where the topology is made: by a build
//! or a rebuild, a bucket split or merge, and a branch insertion. A single
//! change appends a delta row beside each base it changes instead, under a key
//! of its own, which readers sum, so changes running at once neither rewrite a
//! hot tree node's row nor serialize on the root. The delta rows stay until
//! [`TreeTransaction::compact_node`] folds them into the base, or a batch of
//! changes, which writes each base it changes, folds them on the way.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::digest::{DigestGroup, Ristretto255};
use crate::error::TreeError;
use crate::selection::TreeSpec;
use crate::store::{Failure, MapAccess, MapTransaction, MemoryMaps, MemoryTransaction};

mod audit;
mod build;
mod change;
#[cfg(test)]
mod fixtures;
mod leaves;
mod nodes;
mod scope;

pub use audit::AuditCounts;
pub use build::BUCKET_CAPACITY;
pub use change::{LeafChange, LeafInput};
pub use scope::{LeafCursor, LeafPage, TreeScope};

/// A digest tree: the spec that defines which leaves it admits and the order
/// it keeps them in, and the memory-resident maps that hold its rows, with its
/// leaves summed in the group `G`, ristretto255 unless the tree names another.
///
/// Its operations run in transactions: [`DigestTree::begin`] gives one.
#[derive(Debug)]
pub struct DigestTree<G: DigestGroup = Ristretto255> {
    spec: Arc<TreeSpec>,
    maps: MemoryMaps,
    group: PhantomData<G>,
}

impl<G: DigestGroup> DigestTree<G> {
    /// An empty tree defined by `spec`.
    pub fn new(spec: TreeSpec) -> Self { Self { spec: Arc::new(spec), maps: MemoryMaps::new(), group: PhantomData } }

    /// The spec that defines the tree.
    pub fn spec(&self) -> &TreeSpec { &self.spec }

    /// Begin a transaction over the tree's maps as they stand now, with the
    /// tree's operations.
    pub fn begin(&self) -> TreeTransaction<G> { TreeTransaction::over(self.spec.clone(), self.maps.begin()) }
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
/// new one. The tree's operations are the only writers of the maps' contents,
/// every key and value in the tree's own byte layout.
#[derive(Debug)]
pub struct TreeTransaction<G: DigestGroup = Ristretto255, T = MemoryTransaction> {
    pub(crate) transaction: T,
    group: PhantomData<G>,
    pub(crate) spec: Arc<TreeSpec>,
    pub(crate) failed: bool,
    pub(crate) generation: u64,
    /// Identifies the transaction view a scope belongs to.
    pub(crate) view: Arc<()>,
}

impl<G: DigestGroup, T: MapTransaction> TreeTransaction<G, T> {
    /// Commit the owned maps together, if every condition still holds and
    /// no operation failed. Failure publishes nothing. Borrowed map views
    /// have no commit method.
    pub async fn commit(self) -> Result<(), TreeError> { self.transaction.commit().await }
}

/// Keeps partial work from escaping through an outer transaction's commit,
/// including when a mutation's future is dropped before returning a result.
pub(crate) struct MutationGuard<'a, G: DigestGroup, T: MapAccess> {
    tree: &'a mut TreeTransaction<G, T>,
    /// How the mutation failed, which poisons the transaction when the guard
    /// drops: `None` once it succeeds, and poisoned until it finishes, so that
    /// cancellation and panics poison too.
    failure: Option<Failure>,
}

impl<G: DigestGroup, T: MapAccess> MutationGuard<'_, G, T> {
    pub(crate) fn finish<R>(mut self, result: Result<R, TreeError>) -> Result<R, TreeError> {
        self.failure = match &result {
            Ok(_) => None,
            Err(TreeError::Conflict) => Some(Failure::Conflict),
            Err(_) => Some(Failure::Poisoned),
        };
        result
    }
}

impl<G: DigestGroup, T: MapAccess> std::ops::Deref for MutationGuard<'_, G, T> {
    type Target = TreeTransaction<G, T>;
    fn deref(&self) -> &Self::Target { self.tree }
}

impl<G: DigestGroup, T: MapAccess> std::ops::DerefMut for MutationGuard<'_, G, T> {
    fn deref_mut(&mut self) -> &mut Self::Target { self.tree }
}

impl<G: DigestGroup, T: MapAccess> Drop for MutationGuard<'_, G, T> {
    fn drop(&mut self) {
        if let Some(failure) = self.failure {
            self.tree.failed = true;
            self.tree.transaction.poison(failure);
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
    pub fn bind(spec: &TreeSpec, maps: T) -> Self { Self::over(Arc::new(spec.clone()), maps) }

    /// A transaction over `maps` as the tree `spec` defines, sharing the spec.
    fn over(spec: Arc<TreeSpec>, maps: T) -> Self {
        Self { transaction: maps, group: PhantomData, spec, failed: false, generation: 0, view: Arc::new(()) }
    }

    pub(crate) fn mutation_guard(&mut self) -> MutationGuard<'_, G, T> { MutationGuard { tree: self, failure: Some(Failure::Poisoned) } }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::fixtures::empty_tree;
    use crate::{HeadHash, LeafChange, LeafInput};
    use ankurah_core_types::EntityId;

    fn input(n: u8) -> LeafInput { LeafInput { key: vec![], entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[n; 32]]) } }

    /// A commit reports why its transaction cannot commit: a conflict when an
    /// operation found what it expected changed, after which the writer
    /// retries from fresh reads, and otherwise a poisoned transaction, whose
    /// failure a retry can meet again.
    #[tokio::test]
    async fn a_commit_tells_a_conflict_from_other_failures() {
        let tree = empty_tree::<Ristretto255>();
        let mut stale = tree.begin();
        assert!(matches!(stale.apply_change(LeafChange { before: Some(input(1)), after: None }).await, Err(TreeError::Conflict)));
        assert!(matches!(stale.commit().await, Err(TreeError::Conflict)));

        let mut duplicate = tree.begin();
        assert!(matches!(duplicate.build([input(1), input(1)]).await, Err(TreeError::DuplicateLeaf)));
        assert!(matches!(duplicate.commit().await, Err(TreeError::Poisoned)));
    }
}
