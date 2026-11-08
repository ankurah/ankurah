//! Working out a transaction's commit: each change checked against the leaves
//! it touches, its leaf rows put, and its edits filed in the trie.

use std::collections::{BTreeMap, BTreeSet};

use super::hashing::hash_leaves;
use super::maintain::{self, Edit};
use super::transaction::{MemberChange, Version};
use super::Filed;
use crate::digest::leaf_encoding;
use crate::span::address_of;
use crate::store::LeafRow;
use crate::tree::rows::{wrong_kind, TreeRows};
use crate::tree::trie::Trie;
use crate::{DigestGroup, Error, Payload, ReadTrx, Row, RowKey, StoreError};

/// The fewest edits a transaction must make to the trie for the tree to be
/// laid out anew rather than updated node by node, which must also number at
/// least a quarter of the tree's members: then one pass over every member
/// costs less than walking the trie once per edit.
const RELAYOUT_EDITS: usize = 128;

/// A member's change ready to apply: where the version it replaces sits and
/// the payload it holds there, where the new version goes, with its element,
/// and the address of every new slot the transaction put the member in.
pub(super) struct Ready<L, G: DigestGroup> {
    id: Vec<u8>,
    before: Option<(Vec<u8>, L)>,
    after: Option<Filed<L, G>>,
    new_slots: Vec<Vec<u8>>,
}

/// The changes with every version placed at its address and every new
/// version hashed, a chunk at a time with a yield between chunks.
pub(super) async fn hash<L: Payload, G: DigestGroup>(changes: Vec<MemberChange<L>>) -> Vec<Ready<L, G>> {
    let encodings: Vec<Vec<u8>> = changes
        .iter()
        .filter_map(|change| change.after.as_ref().map(|after| leaf_encoding(&after.key, &change.id, &after.payload.digest_bytes())))
        .collect();

    let mut elements = hash_leaves::<G>(&encodings).await.into_iter();

    changes
        .into_iter()
        .map(|MemberChange { id, before, after, new_slots }| Ready {
            before: before.map(|Version { key, payload }| (address_of(&key, &id), payload)),
            after: after.map(|Version { key, payload }| Filed {
                address: address_of(&key, &id),
                payload,
                element: elements.next().expect("an element for every new version"),
            }),
            new_slots: new_slots.iter().map(|key| address_of(key, &id)).collect(),
            id,
        })
        .collect()
}

/// The leaves applying `changes` checks, each once, in key order: the leaf
/// each replaced version sits at, and each new slot a member was put in,
/// among them every slot a new version goes to. A commit reads them in one
/// read through its transaction, so that a store far from its rows fetches
/// them together.
pub(super) fn leaves_checked<L, G: DigestGroup>(changes: &[Ready<L, G>]) -> Vec<RowKey> {
    let before = changes.iter().filter_map(|change| change.before.as_ref().map(|(address, _)| address));
    let new_slots = changes.iter().flat_map(|change| &change.new_slots);

    let leaves: BTreeSet<&Vec<u8>> = before.chain(new_slots).collect();
    leaves.into_iter().map(|address| RowKey::Leaf(address.clone())).collect()
}

/// The leaves a commit's checks read, by address, as one read found them:
/// each leaf the tree holds there, or `None` for an empty slot.
pub(super) type Checked<L, G> = BTreeMap<Vec<u8>, Option<LeafRow<L, G>>>;

/// The leaves the checks read, from what a read of `leaves`, the keys
/// [`leaves_checked`] named, found: one row per key, in order.
pub(super) fn checked<K, L, G: DigestGroup>(leaves: &[RowKey], found: Vec<Option<Row<K, L, G>>>) -> Result<Checked<L, G>, Error> {
    if found.len() != leaves.len() {
        return Err(Error::Store(StoreError::permanent("a store answered a read of several keys with another number of rows")));
    }

    leaves
        .iter()
        .zip(found)
        .map(|(key, row)| match (key, row) {
            (RowKey::Leaf(address), None) => Ok((address.clone(), None)),
            (RowKey::Leaf(address), Some(Row::Leaf(leaf))) => Ok((address.clone(), Some(leaf))),
            _ => Err(wrong_kind()),
        })
        .collect()
}

/// The leaf at `address`, as the commit's one read of its checked leaves
/// found it. Every leaf a change checks is among them, so one missing means
/// the commit named its leaves wrong.
fn checked_leaf<L, G: DigestGroup>(checked: &mut Checked<L, G>, address: &[u8]) -> Result<Option<LeafRow<L, G>>, Error> {
    checked.remove(address).ok_or(Error::Inconsistent("a commit checks a leaf it didn't read"))
}

/// Check every change against the leaves it touches, as `checked` holds
/// them, and work out in `rows` the rows the changes leave: each change's leaf
/// rows, and its edits filed in the trie, node by node or, for a transaction
/// large against the tree, by laying the tree out anew.
pub(super) async fn work_out_rows<K, L, G, R>(
    rows: &mut TreeRows<K, L, G, R>,
    changes: Vec<Ready<L, G>>,
    mut checked: Checked<L, G>,
) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
{
    let mut edits = Vec::with_capacity(changes.len());

    for Ready { id, before, after, new_slots } in changes {
        // The leaf the version a transaction first replaces sits at must
        // hold that version.
        let replaced = match before {
            Some((address, payload)) => match checked_leaf(&mut checked, &address)? {
                Some(held) if held.payload.digest_bytes() == payload.digest_bytes() => Some((address, held.element)),
                _ => return Err(Error::NotHeld { member: id }),
            },

            None => None,
        };

        // Every new slot the transaction put the member in must be empty,
        // though a later change moved the member on: a leaf already there
        // means the tree holds the member there, and would count it twice.
        for address in &new_slots {
            if checked_leaf(&mut checked, address)?.is_some() {
                return Err(Error::AlreadyHeld { member: id });
            }
        }

        match (replaced, after) {
            // A member inserted and removed within the transaction.
            (None, None) => {}

            (None, Some(after)) => file(rows, &mut edits, after),

            // The payload may differ in what its digest doesn't cover, so
            // the row is replaced even where the element is the same.
            (Some((address, element)), Some(after)) if after.address == address => {
                rows.put(RowKey::Leaf(address.clone()), Row::Leaf(LeafRow { payload: after.payload, element: after.element }));

                if element != after.element {
                    edits.push(Edit::Replace { address, old: element, new: after.element });
                }
            }

            (Some((address, element)), after) => {
                rows.delete(RowKey::Leaf(address.clone()));
                edits.push(Edit::Remove { address, element });

                if let Some(after) = after {
                    file(rows, &mut edits, after);
                }
            }
        }
    }

    if edits.is_empty() {
        return Ok(());
    }

    // Only a transaction that makes enough edits reads the tree's count of
    // members, from its root, which a walk node by node reads for itself.
    if edits.len() >= RELAYOUT_EDITS && edits.len() as u64 * 4 >= Trie::new(rows).root().await?.row.digest.count() {
        return maintain::lay_out_anew(rows).await;
    }

    maintain::edit(rows, edits).await
}

/// Put a new version's leaf row, and note its insertion into the trie. Its
/// slot is a new one the transaction put the member in, which the checks found
/// empty.
fn file<K, L, G, R>(rows: &mut TreeRows<K, L, G, R>, edits: &mut Vec<Edit<G>>, Filed { address, payload, element }: Filed<L, G>)
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    R: ReadTrx<RowKey, Row<K, L, G>>,
{
    rows.put(RowKey::Leaf(address.clone()), Row::Leaf(LeafRow { payload, element }));
    edits.push(Edit::Insert { address, element });
}

#[cfg(test)]
mod tests;
