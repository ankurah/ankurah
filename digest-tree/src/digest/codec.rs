//! The stored forms of digests and leaf elements: each group's canonical
//! encoding of the element behind the group's version byte, so that a row of
//! one group never decodes in another.

use super::{Digest, DigestGroup};

/// Why stored bytes are not a digest or a leaf element.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The bytes have the wrong length for the layout.
    #[error("expected {expected} bytes, found {found}")]
    Length {
        /// The layout's length.
        expected: usize,

        /// The length given.
        found: usize,
    },

    /// The bytes begin with another version byte than the group's: a row
    /// written in another group, or in a layout this build cannot read.
    #[error("unknown digest version {0}")]
    Version(u8),

    /// The element bytes are not the canonical encoding of an element of the
    /// group. Only a group that can tell reports it; LtHash16 never does.
    #[error("not the canonical encoding of a group element")]
    Element,
}

const COUNT_LEN: usize = 8;

impl<G: DigestGroup> Digest<G> {
    /// The length of a stored digest: the version byte, the element, then the
    /// count.
    pub(crate) const STORED_LEN: usize = 1 + G::ELEMENT_LEN + COUNT_LEN;

    /// Append the stored form: the group's version byte, the element's
    /// canonical encoding, then the count as an unsigned 64-bit big-endian
    /// integer.
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(G::ROW_VERSION);
        encode_element::<G>(&self.element, out);
        out.extend_from_slice(&self.count.to_be_bytes());
    }

    /// Decode the stored form, refusing another group's version byte and any
    /// element bytes the group can tell are not a canonical encoding.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let wrong_length = DecodeError::Length { expected: Self::STORED_LEN, found: bytes.len() };
        let (element, count) = versioned::<G>(bytes, Self::STORED_LEN)?.split_last_chunk::<COUNT_LEN>().ok_or(wrong_length)?;
        Ok(Self { element: G::decode(element)?, count: u64::from_be_bytes(*count) })
    }
}

/// The length of a stored leaf element: the version byte, then the element.
pub(crate) const fn stored_element_len<G: DigestGroup>() -> usize { 1 + G::ELEMENT_LEN }

/// Append a leaf element's stored form: the group's version byte, then the
/// element's canonical encoding.
pub(crate) fn encode_stored_element<G: DigestGroup>(element: &G::Element, out: &mut Vec<u8>) {
    out.push(G::ROW_VERSION);
    encode_element::<G>(element, out);
}

/// Decode a leaf element's stored form, refusing another group's version byte
/// and any bytes the group can tell are not a canonical encoding.
pub(crate) fn decode_stored_element<G: DigestGroup>(bytes: &[u8]) -> Result<G::Element, DecodeError> {
    G::decode(versioned::<G>(bytes, stored_element_len::<G>())?)
}

/// Append the canonical encoding of `element`.
fn encode_element<G: DigestGroup>(element: &G::Element, out: &mut Vec<u8>) {
    let start = out.len();
    out.resize(start + G::ELEMENT_LEN, 0);
    G::encode(element, &mut out[start..]);
}

/// `bytes`, if they are exactly `len` long.
pub(crate) fn exact(bytes: &[u8], len: usize) -> Result<&[u8], DecodeError> {
    match bytes.len() == len {
        true => Ok(bytes),
        false => Err(DecodeError::Length { expected: len, found: bytes.len() }),
    }
}

/// The bytes after the version byte, when they are `len` long in all and the
/// version byte is `G`'s. The version is checked first, so a row of another
/// group, whose length differs, is refused as an unknown version.
fn versioned<G: DigestGroup>(bytes: &[u8], len: usize) -> Result<&[u8], DecodeError> {
    match bytes.first() {
        Some(&version) if version != G::ROW_VERSION => Err(DecodeError::Version(version)),
        _ => Ok(&exact(bytes, len)?[1..]),
    }
}

#[cfg(test)]
mod tests;
