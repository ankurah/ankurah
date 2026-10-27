//! A member's leaf: the canonical bytes its element is hashed from.

/// The canonical encoding of a member's leaf: its key, its id's bytes and its
/// payload's digest bytes, in that order, each preceded by its length as an
/// unsigned 64-bit big-endian integer.
///
/// The length prefixes make the encoding injective, so no two distinct leaves
/// share an encoding, even where one key or id is a prefix of another. The
/// leaf binds its key as well as its member, so moving a member to another key
/// replaces its element.
pub(crate) fn leaf_encoding(key: &[u8], id: &[u8], payload: &[u8]) -> Vec<u8> {
    let fields = [key, id, payload];
    let mut encoding = Vec::with_capacity(fields.iter().map(|field| 8 + field.len()).sum());

    for field in fields {
        encoding.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoding.extend_from_slice(field);
    }

    encoding
}

#[cfg(test)]
mod tests;
