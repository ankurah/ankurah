//! A transaction's commit: prepared without storage I/O, applied through the
//! transaction its owner lends it, and landed once that transaction commits.

use super::commit::{self, Ready};
use crate::tree::rows::TreeRows;
use crate::tree::DigestTree;
use crate::{DigestGroup, Error, MemberId, Payload, ReadTrx, Row, RowKey, Store, StoreTrx};

/// A transaction's changes, hashed and ready to apply in the owner's
/// transaction. It keeps the tree alive but has read nothing and holds no
/// storage transaction, so it may wait as long as its owner likes. Dropped,
/// it changes nothing.
#[must_use = "a prepared commit writes nothing until it is applied"]
pub struct Prepared<K, I, L, G: DigestGroup, S> {
    tree: DigestTree<K, I, L, G, S>,
    changes: Vec<Ready<L, G>>,
    leaves_checked: Vec<RowKey>,
}

impl<K, I, L, G, S> Prepared<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    pub(super) fn new(tree: DigestTree<K, I, L, G, S>, changes: Vec<Ready<L, G>>) -> Self {
        let leaves_checked = commit::leaves_checked(&changes);
        Self { tree, changes, leaves_checked }
    }

    /// The tree the changes are to.
    pub fn tree(&self) -> &DigestTree<K, I, L, G, S> { &self.tree }

    /// The keys of the leaf rows the commit's checks read, each once and in
    /// key order: the leaf each replaced version sits at, and each new slot a
    /// member is put in. [`apply`](Self::apply) reads them in one read through
    /// its transaction, so an owner that reads them itself, as it keeps other
    /// writers off the tree, can answer that read without another request. A
    /// split reads further leaves, which no commit can name before it
    /// applies.
    pub fn leaves_checked(&self) -> &[RowKey] { &self.leaves_checked }

    /// Apply the changes through `trx`, the owner's transaction on the tree's
    /// store, in one write.
    ///
    /// Each change is checked against the leaves it touches, which it reads
    /// through `trx` in one read: an insert's slot must be empty, as must the
    /// slot an update moves its member to, or it is [`Error::AlreadyHeld`],
    /// and an update's or a removal's leaf must hold the version it names, or
    /// it is [`Error::NotHeld`]. Either refuses the proposed transition and
    /// writes nothing; the owner decides how to reconcile the mismatch. A
    /// transaction on another store than the tree's is [`Error::OtherTree`],
    /// and writes nothing either.
    ///
    /// The commit works its rows out in memory, reading through `trx`, and
    /// then writes them through it in one batch, possibly empty: they land
    /// when the owner commits `trx`, all of them or none. This does not
    /// publish this write's changes to the store's caches; after the owner's
    /// transaction commits, the owner calls [`Applied::landed`].
    pub async fn apply(self, mut trx: S::Trx<'_>) -> Result<Applied<K, I, L, G, S>, Error> {
        if !self.tree.store().owns(&trx) {
            return Err(Error::OtherTree);
        }

        let mut writes = Vec::new();
        if !self.changes.is_empty() {
            let checked = commit::checked(&self.leaves_checked, trx.get_many(&self.leaves_checked).await?)?;
            let mut rows = TreeRows::new(&mut trx);
            commit::work_out_rows(&mut rows, self.changes, checked).await?;

            writes = rows.into_writes();
        }

        let pending = trx.write(writes).await?;

        Ok(Applied { tree: self.tree, pending })
    }
}

/// A commit written in its owner's transaction, whose rows land when that
/// transaction commits.
#[must_use = "an applied commit's rows land with its owner's transaction, and then `landed` tells the store"]
pub struct Applied<K, I, L, G: DigestGroup, S: Store<RowKey, Row<K, L, G>>> {
    tree: DigestTree<K, I, L, G, S>,
    pending: S::Pending,
}

impl<K, I, L, G: DigestGroup, S: Store<RowKey, Row<K, L, G>>> Applied<K, I, L, G, S> {
    /// The owner's transaction the commit was applied in has committed: the
    /// store takes in what it kept of the rows. A commit whose transaction
    /// didn't commit is dropped instead, which costs nothing but what the
    /// store would have kept in memory.
    pub fn landed(self) { self.tree.inner.store.landed(self.pending) }
}
