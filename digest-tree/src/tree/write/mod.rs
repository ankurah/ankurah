//! Writing a tree: creating it empty, building it whole, and committing
//! transactions to it.

mod build;
mod commit;
mod hashing;
mod maintain;
mod transaction;

use std::ops::Bound;

use ankurah_core_types::key_spec::KeySpec;

pub use transaction::{StagedCommit, Transaction};

use super::{DigestTree, ScanDirection};
use crate::key::check_spec;
use crate::store::Meta;
use crate::{Digest, DigestGroup, Error, Member, MemberId, NodePrefix, Payload, Row, RowKey, Store, WriteItem};

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
    /// An empty tree keyed by `key_spec`, in a store that holds no rows yet:
    /// [`Error::StoreNotEmpty`] for one that holds any, a tree's or others,
    /// and [`Error::Collation`] for a spec that names a collation.
    ///
    /// Open one tree per store and clone it to share it. Trees opened
    /// separately on one store hold separate locks, so unless the store keeps
    /// [what a tree relies on](Store#what-a-tree-relies-on) itself, two commits
    /// could each read a node's old digest and write their own, losing one
    /// update.
    pub async fn create(store: S, key_spec: KeySpec<K>) -> Result<Self, Error> {
        check_spec(&key_spec)?;
        require_empty(&store).await?;

        // Every tree has a root, the empty one a root of no members.
        store
            .write(vec![
                WriteItem::Put(RowKey::Meta, Row::Meta(Meta { key_spec: key_spec.clone(), group: G::ROW_VERSION })),
                WriteItem::Put(RowKey::Node(NodePrefix::root()), Row::Node(Digest::empty())),
            ])
            .await?;

        Ok(Self::holding(store, key_spec))
    }

    /// A tree holding exactly `members`, built in one pass, in a store that
    /// holds no rows yet: [`Error::StoreNotEmpty`] for one that holds any.
    ///
    /// The members' keys are made and their leaves hashed a chunk at a time,
    /// yielding to the executor between chunks, and every row is written in
    /// one batch, all of it or none: a build whose write fails leaves the
    /// store empty, and can run again. A member that can't give a value is
    /// [`Error::Key`], one that gives a value of another type than its part's
    /// is [`Error::WrongValueType`], and the same id twice is
    /// [`Error::DuplicateMember`]: a caller's bug that keeping either member
    /// would hide. A spec that names a collation is [`Error::Collation`].
    ///
    /// Open one tree per store and clone it to share it, as
    /// [`create`](Self::create) explains.
    pub async fn build<M>(store: S, key_spec: KeySpec<K>, members: impl IntoIterator<Item = M>) -> Result<Self, Error>
    where M: Member<K, Id = I, Payload = L> {
        // Collected before the first await, so that the caller's iterator is
        // never held across one.
        let members: Vec<M> = members.into_iter().collect();
        check_spec(&key_spec)?;
        require_empty(&store).await?;

        let rows = build::rows::<K, I, L, G, M>(&key_spec, members).await?;
        store.write(rows).await?;

        Ok(Self::holding(store, key_spec))
    }

    /// A transaction on this tree: changes collected as the owner makes them
    /// and committed together, all of them or none.
    pub fn transaction(&self) -> Transaction<K, I, L, G, S> { Transaction::new(self.clone()) }
}

/// Refuse a store that holds any row, a tree's or another's, which a new
/// tree's rows would mix with.
async fn require_empty<K, V>(store: &impl Store<K, V>) -> Result<(), Error> {
    match store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 1).await?.is_empty() {
        true => Ok(()),
        false => Err(Error::StoreNotEmpty),
    }
}
