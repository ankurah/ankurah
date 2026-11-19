//! The byte encodings of row keys and rows, for stores that keep bytes.
//!
//! A row key is a tag byte for its kind, then its body: nothing for the
//! metadata, a node prefix's order-preserving encoding, or an address's raw
//! bytes. The tags rise in the order the kinds are declared, so encoded
//! keys sort as the keys do. Each row's own tag also identifies its layout;
//! current metadata has a distinct tag so legacy keys remain recognizable.

mod key_spec;

use ankurah_core_types::key_spec::KeySpec;

use super::{Encode, LeafRow, Meta, NodeRow, Row, RowKey};
use crate::digest::{decode_stored_element, encode_stored_element, stored_element_len};
use crate::{Digest, DigestGroup, NodePrefix, StoreError};

/// Row-key tags. The original metadata value also used META; new metadata
/// values use VERSIONED_META to distinguish their additional version field.
const META: u8 = 0;
const NODE: u8 = 1;
const LEAF: u8 = 2;
const VERSIONED_META: u8 = 3;

/// The byte after a node row's digest that says whether the node is a bucket
/// or branches, and so whether a prefix follows. The root's children may
/// share the empty prefix, whose encoding is no bytes at all, so the prefix
/// alone couldn't tell a bucket from a branch.
const BUCKET: u8 = 0;
const BRANCH: u8 = 1;

impl Encode for RowKey {
    fn encode(&self) -> Vec<u8> {
        match self {
            RowKey::Meta => vec![META],
            RowKey::Node(prefix) => tagged(NODE, &prefix.to_bytes()),
            RowKey::Leaf(address) => tagged(LEAF, address),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        match bytes.split_first() {
            Some((&META, [])) => Ok(RowKey::Meta),
            Some((&NODE, prefix)) => NodePrefix::from_bytes(prefix).map(RowKey::Node).map_err(StoreError::permanent),
            Some((&LEAF, address)) => Ok(RowKey::Leaf(address.to_vec())),
            _ => Err(StoreError::permanent("bytes that are no row key")),
        }
    }
}

/// `body` behind `tag`.
fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + body.len());
    bytes.push(tag);
    bytes.extend_from_slice(body);

    bytes
}

/// A row's layout after its tag:
///
/// - metadata: the group's identifier, key-encoding version, then key spec;
/// - a node: the digest's stored form, then a byte that says whether the node
///   is a bucket or branches, and for a branch the encoding of the prefix its
///   children share;
/// - a leaf: the element's stored form, then the payload's encoding.
impl<K: Encode, L: Encode, G: DigestGroup> Encode for Row<K, L, G> {
    fn encode(&self) -> Vec<u8> {
        match self {
            Row::Meta(meta) => {
                let mut bytes = vec![VERSIONED_META, meta.group, meta.key_encoding];
                bytes.extend_from_slice(&meta.key_spec.encode());

                bytes
            }

            Row::Node(node) => {
                let mut bytes = Vec::with_capacity(2 + Digest::<G>::STORED_LEN);
                bytes.push(NODE);
                node.digest.encode_into(&mut bytes);

                match &node.branches_at {
                    None => bytes.push(BUCKET),

                    Some(prefix) => {
                        bytes.push(BRANCH);
                        bytes.extend_from_slice(&prefix.to_bytes());
                    }
                }

                bytes
            }

            Row::Leaf(leaf) => {
                let mut bytes = vec![LEAF];
                encode_stored_element::<G>(&leaf.element, &mut bytes);
                bytes.extend_from_slice(&leaf.payload.encode());

                bytes
            }
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        match bytes.split_first() {
            Some((&META, body)) => decode_meta(body, false).map(Row::Meta),
            Some((&VERSIONED_META, body)) => decode_meta(body, true).map(Row::Meta),

            Some((&NODE, body)) => decode_node(body).map(Row::Node),

            Some((&LEAF, body)) => {
                let (element, payload) =
                    body.split_at_checked(stored_element_len::<G>()).ok_or_else(|| StoreError::permanent("a truncated leaf row"))?;

                let element = decode_stored_element::<G>(element).map_err(StoreError::permanent)?;
                Ok(Row::Leaf(LeafRow { payload: L::decode(payload)?, element }))
            }

            _ => Err(StoreError::permanent("bytes that are no row")),
        }
    }
}

fn decode_node<G: DigestGroup>(body: &[u8]) -> Result<NodeRow<G>, StoreError> {
    let (digest, record) = body.split_at_checked(Digest::<G>::STORED_LEN).ok_or_else(|| StoreError::permanent("a truncated node row"))?;
    let digest = Digest::decode(digest).map_err(StoreError::permanent)?;

    let branches_at = match record.split_first() {
        Some((&BUCKET, [])) => None,
        Some((&BRANCH, prefix)) => Some(NodePrefix::from_bytes(prefix).map_err(StoreError::permanent)?),
        _ => return Err(StoreError::permanent("a node row that says neither bucket nor branch")),
    };

    Ok(NodeRow { digest, branches_at })
}

fn decode_meta<K: Encode>(body: &[u8], versioned: bool) -> Result<Meta<K>, StoreError> {
    let (&group, key_spec) = body.split_first().ok_or_else(|| StoreError::permanent("a truncated metadata row"))?;
    let (key_encoding, key_spec) = if versioned {
        let (&version, key_spec) = key_spec.split_first().ok_or_else(|| StoreError::permanent("a truncated metadata row"))?;
        (version, key_spec)
    } else {
        (1, key_spec)
    };
    Ok(Meta { key_spec: KeySpec::decode(key_spec)?, group, key_encoding })
}

#[cfg(test)]
mod tests;
