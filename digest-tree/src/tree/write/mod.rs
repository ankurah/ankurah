//! Writing a tree: creating it, and committing transactions to it, either in
//! a transaction its owner lends it or, on a store with no other owner, in
//! one of the store's own.

mod commit;
mod create;
mod hashing;
#[cfg(any(test, feature = "test-support"))]
mod locked;
mod maintain;
mod prepared;
mod transaction;

use std::ops::Bound;

use ankurah_core_types::key_spec::KeySpec;

pub use prepared::{Applied, Prepared};
pub use transaction::Transaction;

use super::{DigestTree, ScanDirection};
use crate::key::check_spec;
use crate::{DigestGroup, Error, Member, MemberId, Payload, ReadTrx, Row, RowKey, Store, StoreTrx};

/// A member's leaf, hashed and ready to file: its address, its payload and
/// its element.
struct Filed<L, G: DigestGroup> {
    address: Vec<u8>,
    payload: L,
    element: G::Element,
}

impl<K, I, L, G, S> DigestTree<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// A new tree keyed by `key_spec`, holding exactly `members`, written
    /// through `trx`, its owner's transaction on `store`, which holds no rows
    /// yet. The tree exists once the owner commits that transaction, and then
    /// [`Created::landed`] gives it.
    ///
    /// The members' keys are made and their leaves hashed a chunk at a time,
    /// yielding to the executor between chunks, and every row is written in
    /// one batch. A member that can't give a value is [`Error::Key`], one that
    /// gives a value of another type than its part's is
    /// [`Error::WrongValueType`], and the same id twice is
    /// [`Error::DuplicateMember`]: a caller's bug that keeping either member
    /// would hide. A spec that names a collation is [`Error::Collation`], a
    /// store whose transaction finds any row, a tree's or others, is
    /// [`Error::StoreNotEmpty`], and a transaction on another store than
    /// `store` is [`Error::OtherTree`]. Each of these writes nothing.
    pub async fn create_in<M: Member<K, Id = I, Payload = L>>(
        store: S,
        mut trx: S::Trx<'_>,
        key_spec: KeySpec<K>,
        members: impl IntoIterator<Item = M>,
    ) -> Result<Created<K, I, L, G, S>, Error> {
        // Collected before the first await, so that the caller's iterator is
        // never held across one.
        let members: Vec<M> = members.into_iter().collect();

        if !store.owns(&trx) {
            return Err(Error::OtherTree);
        }

        check_spec(&key_spec)?;
        require_empty(&mut trx).await?;

        let rows = create::rows::<K, I, L, G, M>(&key_spec, members).await?;
        let pending = trx.write(rows).await?;

        Ok(Created { tree: Self::holding(store, key_spec), pending })
    }

    /// A transaction on this tree: changes collected as the owner makes them
    /// and committed together, all of them or none.
    pub fn transaction(&self) -> Transaction<K, I, L, G, S> { Transaction::new(self.clone()) }
}

/// A tree created in its owner's transaction, which exists once that
/// transaction commits.
#[must_use = "a created tree exists once its owner's transaction commits, and then `landed` gives it"]
pub struct Created<K, I, L, G: DigestGroup, S: Store<RowKey, Row<K, L, G>>> {
    tree: DigestTree<K, I, L, G, S>,
    pending: S::Pending,
}

impl<K, I, L, G: DigestGroup, S: Store<RowKey, Row<K, L, G>>> Created<K, I, L, G, S> {
    /// The tree, once the owner's transaction its rows were written in has
    /// committed: the store takes in what it kept of them.
    pub fn landed(self) -> DigestTree<K, I, L, G, S> {
        self.tree.inner.store.landed(self.pending);
        self.tree
    }
}

/// Refuse a store whose transaction finds any row, a tree's or another's,
/// which a new tree's rows would mix with.
async fn require_empty<K, V>(trx: &mut impl ReadTrx<K, V>) -> Result<(), Error> {
    match trx.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 1).await?.is_empty() {
        true => Ok(()),
        false => Err(Error::StoreNotEmpty),
    }
}
