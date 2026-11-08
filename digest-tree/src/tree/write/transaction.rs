//! Transactions: a batch of changes to a tree, collected as its owner makes
//! them and committed in one write, all of them or none.
//!
//! Every change names the version of the member it replaces, as an index's
//! upkeep reads an entity's old values to remove its old key: an insert names
//! none, and an update or a removal the version the tree holds. A change's
//! keys are made at once, so a member that can't give a value for a key
//! part, or gives one of another type than the part's, fails at its own call.
//! Applying the transaction checks each change against the leaves it touches
//! (see `commit`).

use std::collections::{BTreeSet, HashMap};

use super::{commit, Prepared};
use crate::key::member_key;
use crate::tree::DigestTree;
use crate::{DigestGroup, Error, Leaf, Member, MemberId, Payload, Row, RowKey, Store};

/// A batch of changes to a tree, committed together: all of them, or none.
///
/// Each change names the version of the member it replaces: none for an
/// insert, and for an update or a removal the version the tree holds, as its
/// owner gave it, or as a read returned it, through
/// [`update_leaf`](Self::update_leaf) and [`remove_leaf`](Self::remove_leaf).
/// Changes to one member chain: each starts from the version the one before
/// it left. Nothing is written until the transaction is
/// [prepared](Self::prepare) and [applied](Prepared::apply) in its owner's
/// transaction on the store, which then commits; a transaction dropped before
/// then changes nothing.
pub struct Transaction<K, I, L, G: DigestGroup, S> {
    tree: DigestTree<K, I, L, G, S>,

    /// Each member the transaction changes, in the order of its first change.
    changes: Vec<MemberChange<L>>,

    /// Where each member's change is in `changes`, by its id's bytes.
    by_id: HashMap<Vec<u8>, usize>,
}

/// One member's versions across a transaction: the one the tree holds before
/// it, if any, and the one the tree is to hold after it, if any.
pub(super) struct MemberChange<L> {
    pub(super) id: Vec<u8>,
    pub(super) before: Option<Version<L>>,
    pub(super) after: Option<Version<L>>,

    /// The keys of the slots the transaction's changes put the member in,
    /// besides the slot of the version the tree holds before it: an
    /// insert's, and each one an update moves the member to. The tree must
    /// hold no version of the member in any of them, though a later change
    /// moves the member on, or the tree would hold the member twice.
    pub(super) new_slots: BTreeSet<Vec<u8>>,
}

/// A version of a member: its key and its payload.
pub(super) struct Version<L> {
    pub(super) key: Vec<u8>,
    pub(super) payload: L,
}

impl<K, I, L, G, S> Transaction<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    pub(super) fn new(tree: DigestTree<K, I, L, G, S>) -> Self { Self { tree, changes: Vec::new(), by_id: HashMap::new() } }

    /// Add `member`, which the tree doesn't hold. A member that can't give a
    /// value is [`Error::Key`], one that gives a value of another type than
    /// its part's is [`Error::WrongValueType`], and one this transaction
    /// already holds is [`Error::AlreadyHeld`].
    ///
    /// The commit checks only the slot under the member's own key. A member
    /// the tree holds under another key goes unnoticed, and the tree then
    /// holds it twice: finding a member by id alone would take a row per
    /// member, which the tree doesn't keep.
    pub fn insert<M: Member<K, Id = I, Payload = L>>(&mut self, member: M) -> Result<(), Error> {
        let (id, version) = self.version_of(&member)?;
        self.change(id, None, Some(version))
    }

    /// Replace `old`, the version of a member the tree holds, with `new`, a
    /// version of the same member: [`Error::IdChanged`] for one of another.
    /// An `old` that isn't the version this transaction last left the member
    /// in is [`Error::NotHeld`].
    pub fn update<O, N>(&mut self, old: O, new: N) -> Result<(), Error>
    where
        O: Member<K, Id = I, Payload = L>,
        N: Member<K, Id = I, Payload = L>,
    {
        let (id, before) = self.version_of(&old)?;
        self.replace(id, before, &new)
    }

    /// Replace `old`, a leaf a read of this tree returned, with `new`, as
    /// [`update`](Self::update) does: for an owner that holds the version it
    /// replaces only as the tree held it.
    pub fn update_leaf<N: Member<K, Id = I, Payload = L>>(&mut self, old: Leaf<I, L>, new: N) -> Result<(), Error> {
        let (id, before) = version_of_leaf(old);
        self.replace(id, before, &new)
    }

    /// Remove `old`, the version of a member the tree holds. An `old` that
    /// isn't the version this transaction last left the member in is
    /// [`Error::NotHeld`].
    pub fn remove<O: Member<K, Id = I, Payload = L>>(&mut self, old: O) -> Result<(), Error> {
        let (id, before) = self.version_of(&old)?;
        self.change(id, Some(before), None)
    }

    /// Remove `old`, a leaf a read of this tree returned, as
    /// [`remove`](Self::remove) does.
    pub fn remove_leaf(&mut self, old: Leaf<I, L>) -> Result<(), Error> {
        let (id, before) = version_of_leaf(old);
        self.change(id, Some(before), None)
    }

    /// The tree the changes are to.
    pub fn tree(&self) -> &DigestTree<K, I, L, G, S> { &self.tree }

    /// The changes, ready to [apply](Prepared::apply) in the owner's
    /// transaction: each new version placed at its address and hashed, a
    /// chunk at a time with a yield between chunks, and the leaves the
    /// commit's checks read named ([`Prepared::leaves_checked`]). It reads no
    /// store, refuses nothing and can't fail.
    pub async fn prepare(self) -> Prepared<K, I, L, G, S> {
        let changes = commit::hash::<L, G>(self.changes).await;
        Prepared::new(self.tree, changes)
    }

    /// A member's id's bytes, and its version under the tree's spec.
    fn version_of<M: Member<K, Id = I, Payload = L>>(&self, member: &M) -> Result<(Vec<u8>, Version<L>), Error> {
        let key = member_key(&self.tree.inner.key_spec, member)?;
        Ok((member.id().to_bytes().into_owned(), Version { key, payload: member.payload() }))
    }

    /// Record a change of the member with id `id` from `before` to `new`,
    /// which must be a version of the same member.
    fn replace<N: Member<K, Id = I, Payload = L>>(&mut self, id: Vec<u8>, before: Version<L>, new: &N) -> Result<(), Error> {
        let (new_id, after) = self.version_of(new)?;

        if new_id != id {
            return Err(Error::IdChanged { old: id, new: new_id });
        }

        self.change(id, Some(before), Some(after))
    }

    /// Record a change of the member with id `id` from `from` to `to`. A
    /// member changed earlier in the transaction must change on from the
    /// version that change left it in.
    fn change(&mut self, id: Vec<u8>, from: Option<Version<L>>, to: Option<Version<L>>) -> Result<(), Error> {
        // The slot the change puts the member in, unless it leaves the member
        // where it was.
        let entered = to.as_ref().filter(|to| from.as_ref().is_none_or(|from| from.key != to.key)).map(|to| to.key.clone());

        let Some(&at) = self.by_id.get(&id) else {
            self.by_id.insert(id.clone(), self.changes.len());
            self.changes.push(MemberChange { id, before: from, after: to, new_slots: entered.into_iter().collect() });
            return Ok(());
        };

        let change = &mut self.changes[at];
        if !same_version(change.after.as_ref(), from.as_ref()) {
            return Err(match from {
                Some(_) => Error::NotHeld { member: id },
                None => Error::AlreadyHeld { member: id },
            });
        }

        // The slot of the version the tree holds before the transaction is
        // empty once a change has moved the member out of it, so a change
        // moving the member back there puts it in no new slot.
        if let Some(key) = entered.filter(|key| change.before.as_ref().is_none_or(|before| before.key != *key)) {
            change.new_slots.insert(key);
        }

        change.after = to;
        Ok(())
    }
}

/// A leaf's id's bytes, and its version as the tree held it.
fn version_of_leaf<I: MemberId, L>(leaf: Leaf<I, L>) -> (Vec<u8>, Version<L>) {
    (leaf.id.to_bytes().into_owned(), Version { key: leaf.key, payload: leaf.payload })
}

/// Whether two versions, either absent, are the same for the tree: both
/// absent, or equal keys with payloads whose digests cover equal bytes.
fn same_version<L: Payload>(a: Option<&Version<L>>, b: Option<&Version<L>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.key == b.key && a.payload.digest_bytes() == b.payload.digest_bytes(),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
