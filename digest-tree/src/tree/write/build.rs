//! Building a tree whole from its members: every row, the nodes laid out
//! bottom-up in one pass.

use ankurah_core_types::key_spec::KeySpec;

use super::hashing::{hash_chunk, yield_now, LEAVES_PER_CHUNK};
use super::Filed;
use crate::digest::leaf_encoding;
use crate::key::member_key;
use crate::span::address_of;
use crate::store::{LeafRow, Meta};
use crate::tree::trie::lay_out;
use crate::{DigestGroup, Error, Member, MemberId, NodePrefix, Payload, Row, RowKey, WriteItem};

/// A member with its key made and its id's bytes, not yet hashed.
struct Encoded<L> {
    key: Vec<u8>,
    id: Vec<u8>,
    payload: L,
}

/// Every row of a tree that holds exactly `members` under `key_spec`.
///
/// Every member's key is made before any is hashed, so that a member without
/// a key, or an id given twice, is refused before the costly part. Both
/// passes go a chunk at a time, yielding to the executor between chunks, so a
/// large build holds no executor thread for long.
pub(super) async fn rows<K, I, L, G, M>(key_spec: &KeySpec<K>, members: Vec<M>) -> Result<Vec<WriteItem<RowKey, Row<K, L, G>>>, Error>
where
    K: Clone,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    M: Member<K, Id = I, Payload = L>,
{
    let mut encoded = Vec::with_capacity(members.len());
    for (index, member) in members.iter().enumerate() {
        if index > 0 && index % LEAVES_PER_CHUNK == 0 {
            yield_now().await;
        }

        let key = member_key(key_spec, member)?;
        encoded.push(Encoded { key, id: member.id().to_bytes().into_owned(), payload: member.payload() });
    }

    refuse_duplicate_ids(&encoded)?;

    let mut filed: Vec<Filed<L, G>> = Vec::with_capacity(encoded.len());
    let mut unhashed = encoded.into_iter();

    loop {
        let chunk: Vec<Encoded<L>> = unhashed.by_ref().take(LEAVES_PER_CHUNK).collect();
        if chunk.is_empty() {
            break;
        }

        if !filed.is_empty() {
            yield_now().await;
        }

        let encodings: Vec<Vec<u8>> =
            chunk.iter().map(|member| leaf_encoding(&member.key, &member.id, &member.payload.digest_bytes())).collect();

        let elements = hash_chunk::<G>(&encodings);
        filed.extend(chunk.into_iter().zip(elements).map(|(Encoded { key, id, payload }, element)| Filed {
            address: address_of(&key, &id),
            payload,
            element,
        }));
    }

    // Addresses delimit the key and the id, so distinct ids give distinct
    // addresses.
    filed.sort_unstable_by(|a, b| a.address.cmp(&b.address));
    let placed: Vec<(&[u8], &G::Element)> = filed.iter().map(|leaf| (leaf.address.as_slice(), &leaf.element)).collect();
    let nodes = lay_out::<G>(&NodePrefix::root(), &placed)?;

    let mut rows = Vec::with_capacity(1 + nodes.len() + filed.len());
    rows.push(WriteItem::Put(RowKey::Meta, Row::Meta(Meta { key_spec: key_spec.clone(), group: G::ROW_VERSION })));
    rows.extend(nodes.into_iter().map(|node| WriteItem::Put(RowKey::Node(node.prefix), Row::Node(node.digest))));

    for Filed { address, payload, element } in filed {
        rows.push(WriteItem::Put(RowKey::Leaf(address), Row::Leaf(LeafRow { payload, element })));
    }

    Ok(rows)
}

/// The same id twice is the caller's mistake, which keeping either member
/// would hide.
fn refuse_duplicate_ids<L>(encoded: &[Encoded<L>]) -> Result<(), Error> {
    let mut ids: Vec<&[u8]> = encoded.iter().map(|member| member.id.as_slice()).collect();
    ids.sort_unstable();

    match ids.windows(2).find(|pair| pair[0] == pair[1]) {
        Some(pair) => Err(Error::DuplicateMember { member: pair[0].to_vec() }),
        None => Ok(()),
    }
}
