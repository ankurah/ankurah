//! What a digest tree relies on of its group: the arithmetic, the hash from
//! leaves into the group, and the canonical encoding of an element.

use super::DecodeError;

/// The operations that make a type one of the crate's digest groups. It sits
/// in a private module, so no other crate can implement
/// [`DigestGroup`](super::DigestGroup), whose supertrait it is.
///
/// The group must be commutative and associative, so that a set of leaves has
/// one digest whatever order its leaves arrived in and however a tree groups
/// them; invertible, so that a change can subtract the old leaf's element; and
/// collision resistant for sets of distinct leaves. The module documentation
/// states the security each group gives.
pub trait GroupOperations<E> {
    /// The group's identifier: the version byte that begins every stored
    /// digest and leaf element of this group, and the group a store records
    /// for its tree. No two groups share one.
    const ROW_VERSION: u8;

    /// The length of an element's canonical encoding, in bytes.
    const ELEMENT_LEN: usize;

    /// The identity element: the sum of no elements.
    fn zero() -> E;

    /// Add `other` to `sum`. The operations work in place because a digest is
    /// a running sum and an element can be kilobytes.
    fn add(sum: &mut E, other: &E);

    /// Subtract `other` from `sum`.
    fn sub(sum: &mut E, other: &E);

    /// The element of the leaf whose canonical encoding is `encoded_leaf`.
    ///
    /// The collision resistance of sums rests on leaves' elements behaving as
    /// independent random ones, so the hash behaves as a random oracle into the
    /// group, under a domain tag of the group's own that keeps its elements
    /// apart from every other use of the same hash.
    fn hash_leaf(encoded_leaf: &[u8]) -> E;

    /// Write the canonical encoding of `element` into `out`, which holds
    /// exactly [`ELEMENT_LEN`](Self::ELEMENT_LEN) bytes. Equal elements have
    /// equal encodings, and different elements different ones.
    fn encode(element: &E, out: &mut [u8]);

    /// The element `bytes` encode. Bytes of the wrong length are refused, and
    /// so are bytes that are not a canonical encoding wherever the group can
    /// tell: ristretto255 refuses them, while every 2,048 bytes are an
    /// LtHash16 element.
    fn decode(bytes: &[u8]) -> Result<E, DecodeError>;
}

#[cfg(test)]
mod tests;
