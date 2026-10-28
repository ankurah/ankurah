//! The byte encodings of row keys and rows, for stores that keep bytes.
//!
//! A row key is a tag byte for its kind, then its body: nothing for the
//! metadata, a node prefix's order-preserving encoding, or an address's raw
//! bytes. The tags rise in the order the kinds are declared, so encoded
//! keys sort as the keys do. A row begins with the same tag as its key, so a
//! row decodes without its key.

mod key_spec;

use ankurah_core_types::key_spec::KeySpec;

use super::{Encode, LeafRow, Meta, Row, RowKey};
use crate::digest::{decode_stored_element, encode_stored_element, stored_element_len};
use crate::{Digest, DigestGroup, NodePrefix, StoreError};

/// The tag of each kind of row, which begins its key's encoding and its own.
const META: u8 = 0;
const NODE: u8 = 1;
const LEAF: u8 = 2;

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
/// - metadata: the group's identifier, then the key spec's encoding;
/// - a node: the digest's stored form;
/// - a leaf: the element's stored form, then the payload's encoding.
impl<K: Encode, L: Encode, G: DigestGroup> Encode for Row<K, L, G> {
    fn encode(&self) -> Vec<u8> {
        match self {
            Row::Meta(meta) => {
                let mut bytes = vec![META, meta.group];
                bytes.extend_from_slice(&meta.key_spec.encode());

                bytes
            }

            Row::Node(digest) => {
                let mut bytes = Vec::with_capacity(1 + Digest::<G>::STORED_LEN);
                bytes.push(NODE);
                digest.encode_into(&mut bytes);

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
            Some((&META, body)) => decode_meta(body).map(Row::Meta),

            Some((&NODE, digest)) => Digest::decode(digest).map(Row::Node).map_err(StoreError::permanent),

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

fn decode_meta<K: Encode>(body: &[u8]) -> Result<Meta<K>, StoreError> {
    let (&group, key_spec) = body.split_first().ok_or_else(|| StoreError::permanent("a truncated metadata row"))?;
    Ok(Meta { key_spec: KeySpec::decode(key_spec)?, group })
}

#[cfg(test)]
mod tests;
