//! The cryptography: the groups a tree sums members in, and the digest.
//!
//! A digest stands for every member beneath a tree node, so two trees that
//! hold equal digests over a span hold the same members within it, and a
//! comparison can skip the whole span.
//!
//! # Construction
//!
//! - A member's **leaf** binds its key, its id's bytes and its payload's
//!   digest bytes. Its canonical encoding is the three, in that order, each
//!   preceded by its length as an unsigned 64-bit big-endian integer, so no
//!   two distinct leaves share an encoding.
//! - Digests sum in a [`DigestGroup`]: an abelian group, with a hash from
//!   leaves into it. [`Ristretto255`] is the default; [`LtHash16`] is a second,
//!   with faster arithmetic and weaker guarantees.
//! - A leaf's **element** is the group's hash of its canonical encoding. In
//!   ristretto255 (RFC 9496) that is `hash_to_ristretto255` from RFC 9380,
//!   under the domain separation tag `org.ankurah.digest.leaf.v0`.
//! - A [`Digest`] is the sum of the elements of a set of leaves together with
//!   the number of leaves summed. Adding a member to a tree node adds its
//!   element, removing one subtracts it, and a change to a member subtracts the
//!   old leaf's element and adds the new one. Addition commutes and
//!   associates, so a set of members has one digest whatever order they
//!   arrived in and however a tree groups them.
//!
//! # Security
//!
//! Equal digests stand for equal sets of leaves because producing two different
//! sets of leaves with the same sum is as hard as computing a discrete
//! logarithm in ristretto255. That is Theorem 1 of Maitin-Shepard, Tibouchi and
//! Aranha, "Elliptic Curve Multiset Hash" (2017), under its assumptions: the
//! hash into the group is a random oracle over an efficiently samplable
//! encoding, the model RFC 9380's `hash_to_ristretto255` is built for; the
//! group has prime order, as ristretto255 does; and every multiplicity in the
//! difference of the two multisets is below the group order. The order is about
//! 2^252, so generic attacks need about 2^126 group operations: roughly the
//! 128-bit classical security level, under those assumptions. LtHash16 is not
//! here for its security, as [`LtHash16`] explains.
//!
//! The guarantee is for sets. A multiset may repeat a leaf, and adding one leaf
//! as many times as the group order cancels it, so sums over unrestricted
//! multisets are not collision resistant. A tree's leaves are unique by
//! construction: a leaf names its member's id, and a tree holds one leaf per
//! member, so set-collision resistance is all a comparison needs.
//!
//! # Encodings
//!
//! A stored digest or leaf element carries its group's canonical encoding, 32
//! bytes in ristretto255 (RFC 9496 section 4.3.2) and 2,048 in LtHash16,
//! behind the group's version byte, so a row written in one group never
//! decodes in the other.
//!
//! The canonical encoding is portable across targets and library versions,
//! keeps a ristretto255 digest at 41 bytes, and is refused on read when it is
//! invalid, since decompression accepts only canonical encodings of group
//! elements. A damaged row whose bytes are still valid, such as one with a
//! changed count or with another valid element in place of the right one,
//! decodes all the same; only an audit, which recomputes a tree from its
//! members, catches it. Every 2,048 bytes are an LtHash16 element, so nothing
//! validates an LtHash16 row but its length.

mod codec;
mod group;
mod leaf;
mod lthash16;
mod ristretto255;

use std::fmt;

pub(crate) use codec::{decode_stored_element, encode_stored_element, stored_element_len, DecodeError};
pub(crate) use leaf::leaf_encoding;
pub use lthash16::LtHash16;
pub use ristretto255::Ristretto255;

use crate::Error;

/// The abelian group a digest sums members' elements in, with the hash that
/// takes a member's leaf into it.
///
/// A tree finds the members two trees hold differently by comparing their
/// digests, and keeps each tree node current by adding and subtracting
/// elements rather than recomputing it, so the group is commutative,
/// associative and invertible, and collision resistant for sets of distinct
/// leaves.
///
/// The crate implements it for its two groups only, [`Ristretto255`] and
/// [`LtHash16`]: a store records which group its tree sums in, and
/// [`DigestTree::open`](crate::DigestTree::open) refuses a tree of another,
/// which only a fixed set of groups with distinct identifiers can promise.
pub trait DigestGroup: Copy + Eq + fmt::Debug + Send + Sync + 'static + group::GroupOperations<<Self as DigestGroup>::Element> {
    /// A group element: a member's element, or the sum beneath a subtree.
    type Element: Copy + Eq + fmt::Debug + Send + Sync;
}

/// The digest of a set of members: the sum of their elements, and how many
/// there are. Two digests are equal when both their sums and their counts
/// are.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Digest<G: DigestGroup> {
    element: G::Element,
    count: u64,
}

impl<G: DigestGroup> Digest<G> {
    /// The digest of no members.
    pub fn empty() -> Self { Self { element: G::zero(), count: 0 } }

    /// How many members the digest covers.
    pub fn count(&self) -> u64 { self.count }

    /// The digest of the one leaf whose element this is.
    pub(crate) fn of_element(element: G::Element) -> Self { Self { element, count: 1 } }

    /// Add `other` in place, as the group does, since an element can be
    /// kilobytes. A count past `u64` comes only from a damaged row.
    pub(crate) fn add(&mut self, other: &Self) -> Result<(), Error> {
        self.count = self.count.checked_add(other.count).ok_or(Error::Inconsistent("a digest's count overflows"))?;
        G::add(&mut self.element, &other.element);
        Ok(())
    }

    /// Add one leaf's element in place.
    pub(crate) fn add_element(&mut self, element: &G::Element) -> Result<(), Error> {
        self.count = self.count.checked_add(1).ok_or(Error::Inconsistent("a digest's count overflows"))?;
        G::add(&mut self.element, element);
        Ok(())
    }

    /// Subtract one leaf's element in place. A count below zero means a member
    /// was taken from a digest that never held it: the tree's rows disagree.
    pub(crate) fn sub_element(&mut self, element: &G::Element) -> Result<(), Error> {
        self.count = self.count.checked_sub(1).ok_or(Error::Inconsistent("a digest's count falls below zero"))?;
        G::sub(&mut self.element, element);
        Ok(())
    }
}

impl<G: DigestGroup> fmt::Debug for Digest<G> {
    /// The element prints as its canonical encoding in hexadecimal, which is
    /// the same for equal elements, as a group's own representation need not
    /// be.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoding = vec![0u8; G::ELEMENT_LEN];
        G::encode(&self.element, &mut encoding);
        write!(f, "Digest {{ element: ")?;
        encoding.iter().try_for_each(|byte| write!(f, "{byte:02x}"))?;

        write!(f, ", count: {} }}", self.count)
    }
}

#[cfg(test)]
mod tests;
