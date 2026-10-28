//! Addresses: where a member sits in a tree's order.
//!
//! An address is the member's key and then its id's bytes, each with every
//! zero byte followed by `0xFF` and then ended by `0x00 0x00`, which sorts
//! before any escaped byte. So addresses sort by key and then by id, whatever
//! bytes the owner's key and id hold; no address begins another, so the trie
//! can always part two; and the tree splits an address back into its key and
//! id without knowing how the key was made.

use crate::{Error, MemberId};

/// What ends a key or an id within an address.
const END: [u8; 2] = [0x00, 0x00];

/// The address of the member with this key and id.
pub fn leaf_address(key: &[u8], id: &impl MemberId) -> Vec<u8> { address_of(key, &id.to_bytes()) }

/// The address of the member with this key and these id bytes.
pub(crate) fn address_of(key: &[u8], id: &[u8]) -> Vec<u8> {
    let mut address = Vec::with_capacity(key.len() + id.len() + 2 * END.len());
    for field in [key, id] {
        frame_into(&mut address, field);
    }

    address
}

/// Append `field` escaped and then ended, as an address holds its key and its
/// id: framed fields sort as their bytes do, and none begins another.
pub(crate) fn frame_into(out: &mut Vec<u8>, field: &[u8]) {
    escape_into(out, field);
    out.extend_from_slice(&END);
}

/// What every address begins with whose key begins with `prefix`.
pub(crate) fn key_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(prefix.len());
    escape_into(&mut bytes, prefix);

    bytes
}

/// A stored address split into its key and its id's bytes. The tree stores
/// only addresses it formed, so one that doesn't split means its rows are
/// damaged.
pub(crate) fn split_address(address: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let mut rest = address;
    let key = unescape(&mut rest).ok_or_else(not_an_address)?;
    let id = unescape(&mut rest).ok_or_else(not_an_address)?;

    match rest.is_empty() {
        true => Ok((key, id)),
        false => Err(not_an_address()),
    }
}

/// Append `field` with every zero byte followed by `0xFF`.
fn escape_into(out: &mut Vec<u8>, field: &[u8]) {
    for &byte in field {
        out.push(byte);

        if byte == 0 {
            out.push(0xFF);
        }
    }
}

/// The field `rest` begins with, unescaped, with `rest` moved past its end;
/// `None` if `rest` doesn't begin with a whole field.
fn unescape(rest: &mut &[u8]) -> Option<Vec<u8>> {
    let mut field = Vec::new();
    let mut at = 0;

    loop {
        let byte = *rest.get(at)?;
        if byte != 0 {
            field.push(byte);
            at += 1;
            continue;
        }

        match *rest.get(at + 1)? {
            0xFF => field.push(0),

            0x00 => {
                *rest = &rest[at + 2..];
                return Some(field);
            }

            _ => return None,
        }

        at += 2;
    }
}

/// A stored address that isn't a key and an id: the rows are damaged.
fn not_an_address() -> Error { Error::Inconsistent("a stored address is not a key followed by an id") }

#[cfg(test)]
mod tests;
