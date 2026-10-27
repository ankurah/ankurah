//! Staging a transaction's commit: each change checked against the leaves it
//! touches, its rows staged, and its edits filed in the trie.

use super::hashing::hash_leaves;
use super::maintain::{self, Edit};
use super::transaction::{MemberChange, Version};
use super::Filed;
use crate::digest::leaf_encoding;
use crate::span::address_of;
use crate::store::LeafRow;
use crate::tree::rows::StagedRows;
use crate::tree::trie::Trie;
use crate::{DigestGroup, Error, Payload, Row, RowKey, Store};

/// The fewest edits a transaction must make to the trie for the tree to be
/// laid out anew rather than updated node by node, which must also number at
/// least a quarter of the tree's members: then one pass over every member
/// costs less than walking the trie once per edit.
const RELAYOUT_EDITS: usize = 128;

/// A member's change ready to stage: where the version it replaces sits and
/// the payload it holds there, and where the new version goes, with its
/// element.
pub(super) struct Ready<L, G: DigestGroup> {
    id: Vec<u8>,
    before: Option<(Vec<u8>, L)>,
    after: Option<Filed<L, G>>,
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
        .map(|MemberChange { id, before, after }| Ready {
            before: before.map(|Version { key, payload }| (address_of(&key, &id), payload)),
            after: after.map(|Version { key, payload }| Filed {
                address: address_of(&key, &id),
                payload,
                element: elements.next().expect("an element for every new version"),
            }),
            id,
        })
        .collect()
}

/// Check every change against the leaves it touches, stage its rows in `rows`,
/// and file its edits in the trie: node by node, or, for a transaction large
/// against the tree, by laying the tree out anew.
pub(super) async fn stage<K, L, G, S>(rows: &mut StagedRows<'_, K, L, G, S>, changes: Vec<Ready<L, G>>) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    let members = Trie::new(rows).root().await?.digest.count();
    let mut edits = Vec::with_capacity(changes.len());

    for Ready { id, before, after } in changes {
        match (before, after) {
            // A member inserted and removed within the transaction.
            (None, None) => {}

            (None, Some(after)) => file(rows, &mut edits, id, after).await?,

            // The leaf an update or a removal replaces must hold the version
            // it names.
            (Some((address, payload)), after) => {
                let element = match rows.leaf(&address).await? {
                    Some(held) if held.payload.digest_bytes() == payload.digest_bytes() => held.element,
                    _ => return Err(Error::NotHeld { member: id }),
                };

                match after {
                    // The payload may differ in what its digest doesn't
                    // cover, so the row is replaced even where the element is
                    // the same.
                    Some(after) if after.address == address => {
                        rows.put(RowKey::Leaf(address.clone()), Row::Leaf(LeafRow { payload: after.payload, element: after.element }));

                        if element != after.element {
                            edits.push(Edit::Replace { address, old: element, new: after.element });
                        }
                    }

                    after => {
                        rows.delete(RowKey::Leaf(address.clone()));
                        edits.push(Edit::Remove { address, element });

                        if let Some(after) = after {
                            file(rows, &mut edits, id, after).await?;
                        }
                    }
                }
            }
        }
    }

    match edits.len() {
        0 => Ok(()),
        count if count >= RELAYOUT_EDITS && count as u64 * 4 >= members => maintain::lay_out_anew(rows).await,
        _ => maintain::edit(rows, edits).await,
    }
}

/// Stage a new version's leaf row and its insertion into the trie, for the
/// member with id `id`. The slot must be empty: a leaf already there, for an
/// insert or for an update moving the member to another key, means the tree
/// holds the member there, and would count it twice.
async fn file<K, L, G, S>(
    rows: &mut StagedRows<'_, K, L, G, S>,
    edits: &mut Vec<Edit<G>>,
    id: Vec<u8>,
    Filed { address, payload, element }: Filed<L, G>,
) -> Result<(), Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    if rows.leaf(&address).await?.is_some() {
        return Err(Error::AlreadyHeld { member: id });
    }

    rows.put(RowKey::Leaf(address.clone()), Row::Leaf(LeafRow { payload, element }));
    edits.push(Edit::Insert { address, element });

    Ok(())
}
