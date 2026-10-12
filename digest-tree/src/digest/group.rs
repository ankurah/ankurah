//! The group a digest sums in: what a digest tree relies on of its
//! cryptography.

use std::fmt;

use super::codec::DecodeError;

/// The abelian group a digest sums leaves' elements in, with the hash that
/// takes a leaf into it.
///
/// A tree finds the leaves two trees hold differently by comparing their
/// digests, and keeps each tree node current by adding and subtracting
/// elements rather than recomputing it, so the group must be:
///
/// - commutative and associative, so that a set of leaves has one digest
///   whatever order its leaves arrived in and however a tree groups them;
/// - invertible, so that a change can subtract the old leaf's element, and can
///   travel as a difference;
/// - collision resistant for sets of unique leaves: finding two different sets
///   of distinct leaves whose elements have the same sum must be infeasible.
///   Sums over multisets need not be, since adding one element often enough
///   can cancel it; the module documentation explains why a tree's leaves are
///   unique, and states the security each group gives.
///
/// An element has one canonical encoding of [`ELEMENT_LEN`](Self::ELEMENT_LEN)
/// bytes, which rows store behind the group's
/// [version byte](Self::ROW_VERSION).
///
/// [`Ristretto255`](super::Ristretto255) is the default group;
/// [`LtHash16`](super::LtHash16) is the second. A type implementing the trait
/// only names its group as a type parameter; it is `Copy`, `Eq`, `Debug`,
/// `Send` and `Sync` so that the types generic over a group can derive those
/// traits and cross threads.
pub trait DigestGroup: Copy + Eq + fmt::Debug + Send + Sync {
    /// A group element: a leaf's element, or the sum of the elements beneath a
    /// tree node.
    type Element: Copy + Eq + Send + Sync;

    /// The codec version byte that begins every row of this group's digests
    /// and leaf elements. No two groups share one, nor two row layouts of one
    /// group, so a row written in another group decodes as an unknown version
    /// and its tree is rebuilt from its leaves.
    const ROW_VERSION: u8;

    /// The length of an element's canonical encoding, in bytes.
    const ELEMENT_LEN: usize;

    /// The identity element: the sum of no elements.
    fn zero() -> Self::Element;

    /// Add `other` to `sum`. The operations work in place because a digest is
    /// a running sum and an element can be kilobytes.
    fn add(sum: &mut Self::Element, other: &Self::Element);

    /// Subtract `other` from `sum`.
    fn sub(sum: &mut Self::Element, other: &Self::Element);

    /// Replace `element` with its inverse, the element whose sum with it is
    /// zero.
    fn neg(element: &mut Self::Element);

    /// The element of the leaf whose canonical encoding
    /// ([`Leaf::encode`](super::Leaf::encode)) is `encoded_leaf`.
    ///
    /// The collision resistance of sums rests on leaves' elements behaving as
    /// independent random ones, so the hash must behave as a random oracle
    /// into the group, and runs under a domain tag of the group's own that
    /// keeps its elements apart from every other use of the same hash.
    fn hash_leaf(encoded_leaf: &[u8]) -> Self::Element;

    /// Write the canonical encoding of `element` into `out`, which holds
    /// exactly [`ELEMENT_LEN`](Self::ELEMENT_LEN) bytes. Equal elements have
    /// equal encodings, and different elements different ones.
    fn encode(element: &Self::Element, out: &mut [u8]);

    /// The element `bytes` encode. Bytes of the wrong length are refused, and
    /// so are bytes that are not a canonical encoding wherever the group can
    /// tell: ristretto255 refuses them, while every 2,048 bytes are an
    /// LtHash16 element.
    fn decode(bytes: &[u8]) -> Result<Self::Element, DecodeError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::{HeadHash, Leaf, LtHash16, Ristretto255};
    use ankurah_core_types::EntityId;

    fn element<G: DigestGroup>(n: u8) -> G::Element {
        Leaf::new(&[n], EntityId::from_bytes([n; 32]), HeadHash::of([[n; 32]])).element::<G>().0
    }

    fn sum<G: DigestGroup>(a: G::Element, b: G::Element) -> G::Element {
        let mut sum = a;
        G::add(&mut sum, &b);
        sum
    }

    /// What the trait promises, for one group: a leaf hashes to one element and
    /// different leaves to different elements; addition has zero for its
    /// identity, commutes and associates; subtraction undoes it and an
    /// element's inverse sums with it to zero; and every element decodes from
    /// its encoding, which no other length passes for. Elements need not be
    /// `Debug`, so the checks compare them with `==`.
    fn the_contract_holds<G: DigestGroup>() {
        let (a, b, c) = (element::<G>(1), element::<G>(2), element::<G>(3));
        assert!(element::<G>(1) == a, "a leaf hashes to one element");
        assert!(a != b, "different leaves hash to different elements");

        assert!(sum::<G>(a, G::zero()) == a, "zero is the identity");
        assert!(sum::<G>(a, b) == sum::<G>(b, a), "addition commutes");
        assert!(sum::<G>(sum::<G>(a, b), c) == sum::<G>(a, sum::<G>(b, c)), "addition associates");

        let mut difference = sum::<G>(a, b);
        G::sub(&mut difference, &b);
        assert!(difference == a, "subtraction undoes addition");
        let mut inverse = a;
        G::neg(&mut inverse);
        assert!(sum::<G>(a, inverse) == G::zero(), "an element and its inverse sum to zero");

        for element in [G::zero(), a, sum::<G>(a, b), inverse] {
            let mut encoding = vec![0u8; G::ELEMENT_LEN];
            G::encode(&element, &mut encoding);
            assert!(G::decode(&encoding) == Ok(element), "an element decodes from its encoding");
            let longer = [&encoding[..], &[0]].concat();
            for wrong in [&encoding[1..], &longer[..]] {
                assert_eq!(G::decode(wrong).err(), Some(DecodeError::Length { expected: G::ELEMENT_LEN, found: wrong.len() }));
            }
        }
    }

    #[test]
    fn ristretto255_keeps_the_contract() { the_contract_holds::<Ristretto255>() }

    #[test]
    fn lthash16_keeps_the_contract() { the_contract_holds::<LtHash16>() }
}
