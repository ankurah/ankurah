//! Digests: the sums a digest tree compares, and the groups they sum in.
//!
//! A digest stands for every leaf beneath a tree node, so two trees that hold
//! equal digests at a tree node hold the same leaves beneath it, and a
//! comparison can skip that tree node's whole subtree. This module holds the
//! cryptography behind that comparison: the leaves, the groups a tree sums
//! them in, and the digests themselves.
//!
//! # Construction
//!
//! - A [`Leaf`] is one entity filed under one key: the canonical key bytes
//!   (empty where the entity id alone is the key), the entity id, and the
//!   [`HeadHash`] of the entity's head.
//! - Digests sum in a [`DigestGroup`]: an abelian group, with a hash from
//!   leaves into it. [`Ristretto255`] is the default, so no caller names it;
//!   [`LtHash16`] is a second, kept to compare with it.
//! - [`Leaf::element`] hashes the leaf's canonical encoding ([`Leaf::encode`])
//!   to an element of the group. In ristretto255 (RFC 9496) that is
//!   `hash_to_ristretto255` from RFC 9380, under the domain separation tag
//!   [`LEAF_DST`].
//! - A [`Digest`] is the sum of the elements of a multiset of leaves together
//!   with the number of leaves summed. Adding a leaf to a tree node adds its
//!   element, removing one subtracts it, and a change to an entity's head
//!   subtracts the old leaf's element and adds the new one. Addition commutes
//!   and associates, so a set of leaves has one digest whatever order the
//!   leaves arrived in and however a tree groups them.
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
//! multisets are not collision resistant. Leaves are unique by construction: a
//! leaf names its key and its entity, and a tree holds one leaf per distinct
//! key and entity. Applying a change to a tree must keep that uniqueness; with
//! it, set-collision resistance is all a comparison needs. An entity filed
//! under two keys is two distinct leaves with distinct elements, not two
//! copies of one leaf.
//!
//! # Encodings
//!
//! A stored digest or leaf element carries its group's canonical encoding, 32
//! bytes in ristretto255 (RFC 9496 section 4.3.2) and 2,048 in LtHash16,
//! behind the group's codec version byte: see [`Digest::to_row_bytes`] and
//! [`LeafElement::to_row_bytes`].
//!
//! The other candidate for ristretto255's rows was the curve library's internal
//! coordinates, copied as they lie in memory. With them a tree node update
//! costs what adding a difference in memory costs, about 70 ns in this
//! module's benchmark, where the canonical encoding costs about 4.9 µs: 2.4 µs
//! to decompress, the addition, and 2.4 µs to compress. But curve25519-dalek
//! keeps those coordinates private and lays them out differently on 32-bit and
//! 64-bit targets, so storing them would mean unsafe copies of a private layout
//! that any release of the library may change, and a damaged row could be
//! caught only by an audit. The canonical encoding is portable across targets
//! and library versions, keeps a digest row at 41 bytes rather than 169, and
//! rejects invalid encodings on read, since decompression accepts only
//! canonical encodings of group elements. A damaged row whose bytes are still
//! valid, such as one with a changed count byte or with another valid element
//! in place of the right one, decodes all the same, so the audit, which
//! recomputes a tree from its leaves, remains what catches it.
//!
//! LtHash16's canonical encoding is its lanes as a little-endian target holds
//! them, so its codec costs a copy, but nothing validates it: every 2,048 bytes
//! are an element, so only the audit catches a damaged LtHash16 row. Its digest
//! rows are 2,057 bytes against ristretto255's 41.
//!
//! A head-only change decodes the old leaf's stored element and encodes two:
//! the new leaf's element for its leaf row, and one difference reused under
//! each ancestor prefix. It reads topology keys without decoding tree node
//! digests. An insertion, removal or key move may also split or merge buckets;
//! that work reads bounded leaf sets or a parent summary to establish the new
//! summaries. Readers and compaction decode tree node rows, a tree
//! node's base row and each delta appended since: reading a tree node with
//! eight deltas costs about 20 µs in ristretto255 in this module's benchmark,
//! and about 1 µs in LtHash16. If measurements call for a faster layout, it
//! takes a new version byte; a tree is derived from its leaves, so a tree whose
//! rows carry a version this build cannot read is rebuilt from them.

mod codec;
mod group;
mod leaf;
mod lthash16;
mod ristretto255;

use std::borrow::Borrow;
use std::fmt;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

pub use codec::DecodeError;
pub use group::DigestGroup;
pub use leaf::{HeadHash, Leaf, HEAD_TAG};
pub use lthash16::{LtHash16, LTHASH16_LEAF_CONTEXT};
pub use ristretto255::{Ristretto255, LEAF_DST};

/// One leaf's element of the group `G`, made by
/// [`Leaf::element`].
///
/// A digest gains it when the leaf enters a tree and loses it when the leaf
/// leaves or is replaced.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LeafElement<G: DigestGroup = Ristretto255>(pub(crate) G::Element);

/// The sum of the elements of a multiset of leaves in the group `G`, with the
/// number of leaves summed.
///
/// A tree node's digest is the digest of the leaves beneath it, and the digest
/// of a range is the sum of the digests that tile it. Two digests are equal
/// when both their elements and their counts are.
///
/// The count is signed so that the difference of two digests, such as the
/// change one leaf change makes to every tree node on its path, is a digest
/// too. A tree node's own digest never has a negative count; one that does
/// records a removal of a leaf that was never added.
///
/// # Count overflow
///
/// The count is a number of leaves, so arithmetic never wraps it. The checked
/// operations ([`Digest::checked_add`], [`Digest::checked_sub`],
/// [`Digest::checked_neg`] and [`Digest::checked_sum`]) return
/// [`CountOverflow`] when the count would leave the range of `i64`; use them
/// whenever a digest decoded from a row takes part, since its count may be any
/// `i64`. The operators (`+`, `-`, unary `-`,
/// `+=`, `-=` and [`Sum`]) are for digests built from leaves this process
/// hashed, whose counts cannot come near the limit: they panic on overflow, in
/// debug and release builds alike, rather than wrap.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Digest<G: DigestGroup = Ristretto255> {
    pub(crate) element: G::Element,
    pub(crate) count: i64,
}

impl<G: DigestGroup> Digest<G> {
    /// The digest of no leaves: the group identity and a count of zero.
    pub fn empty() -> Self { Self { element: G::zero(), count: 0 } }

    /// The number of leaves summed; negative for a difference that removes
    /// more leaves than it adds.
    pub fn count(&self) -> i64 { self.count }

    /// `self + other`, or [`CountOverflow`] if the count would leave the range
    /// of `i64`.
    pub fn checked_add(mut self, other: Digest<G>) -> Result<Digest<G>, CountOverflow> {
        self.add_in_place(&other)?;
        Ok(self)
    }

    /// `self - other`, or [`CountOverflow`] if the count would leave the range
    /// of `i64`.
    pub fn checked_sub(mut self, other: Digest<G>) -> Result<Digest<G>, CountOverflow> {
        self.sub_in_place(&other)?;
        Ok(self)
    }

    /// `-self`, or [`CountOverflow`] for a count of `i64::MIN`, whose negation
    /// `i64` cannot hold.
    pub fn checked_neg(mut self) -> Result<Digest<G>, CountOverflow> {
        self.count = self.count.checked_neg().ok_or(CountOverflow)?;
        G::neg(&mut self.element);
        Ok(self)
    }

    /// The sum of `digests`, or [`CountOverflow`] if the running count leaves
    /// the range of `i64` at any step.
    pub fn checked_sum<D: Borrow<Digest<G>>>(digests: impl IntoIterator<Item = D>) -> Result<Digest<G>, CountOverflow> {
        digests.into_iter().try_fold(Digest::empty(), |sum, digest| sum.checked_add(*digest.borrow()))
    }

    /// Add `other` to `self` in place, as the group does, since an element can
    /// be kilobytes; `self` is unchanged if the count would overflow.
    fn add_in_place(&mut self, other: &Digest<G>) -> Result<(), CountOverflow> {
        self.count = self.count.checked_add(other.count).ok_or(CountOverflow)?;
        G::add(&mut self.element, &other.element);
        Ok(())
    }

    /// Subtract `other` from `self` in place; `self` is unchanged if the count
    /// would overflow.
    fn sub_in_place(&mut self, other: &Digest<G>) -> Result<(), CountOverflow> {
        self.count = self.count.checked_sub(other.count).ok_or(CountOverflow)?;
        G::sub(&mut self.element, &other.element);
        Ok(())
    }
}

impl<G: DigestGroup> Default for Digest<G> {
    fn default() -> Self { Self::empty() }
}

impl<G: DigestGroup> From<LeafElement<G>> for Digest<G> {
    /// The digest of the one leaf whose element this is.
    fn from(leaf: LeafElement<G>) -> Self { Self { element: leaf.0, count: 1 } }
}

/// The count of a digest would leave the range of `i64`.
///
/// A count is a number of leaves, so it never wraps: the checked operations on
/// [`Digest`] return this error, and the operators panic with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a digest's count overflows i64")]
pub struct CountOverflow;

/// The operators' overflow policy: panic, in every build, rather than wrap.
fn panic_on_overflow<T>(result: Result<T, CountOverflow>) -> T { result.unwrap_or_else(|overflow| panic!("{overflow}")) }

impl<G: DigestGroup> Add for Digest<G> {
    type Output = Digest<G>;
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_add`].
    fn add(self, other: Digest<G>) -> Digest<G> { panic_on_overflow(self.checked_add(other)) }
}

impl<G: DigestGroup> Sub for Digest<G> {
    type Output = Digest<G>;
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_sub`].
    fn sub(self, other: Digest<G>) -> Digest<G> { panic_on_overflow(self.checked_sub(other)) }
}

impl<G: DigestGroup> Neg for Digest<G> {
    type Output = Digest<G>;
    /// Panics for a count of `i64::MIN`, in every build: see
    /// [`Digest::checked_neg`].
    fn neg(self) -> Digest<G> { panic_on_overflow(self.checked_neg()) }
}

impl<G: DigestGroup> AddAssign for Digest<G> {
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_add`].
    fn add_assign(&mut self, other: Digest<G>) { panic_on_overflow(self.add_in_place(&other)) }
}

impl<G: DigestGroup> SubAssign for Digest<G> {
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_sub`].
    fn sub_assign(&mut self, other: Digest<G>) { panic_on_overflow(self.sub_in_place(&other)) }
}

impl<G: DigestGroup> Sum for Digest<G> {
    /// Panics if the running count overflows `i64`, in every build: see
    /// [`Digest::checked_sum`].
    fn sum<I: Iterator<Item = Digest<G>>>(digests: I) -> Digest<G> { panic_on_overflow(Digest::checked_sum(digests)) }
}

impl<'a, G: DigestGroup> Sum<&'a Digest<G>> for Digest<G> {
    /// Panics if the running count overflows `i64`, in every build: see
    /// [`Digest::checked_sum`].
    fn sum<I: Iterator<Item = &'a Digest<G>>>(digests: I) -> Digest<G> { panic_on_overflow(Digest::checked_sum(digests)) }
}

impl<G: DigestGroup> fmt::Debug for LeafElement<G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LeafElement(")?;
        write_encoded::<G>(f, &self.0)?;
        write!(f, ")")
    }
}

impl<G: DigestGroup> fmt::Debug for Digest<G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest {{ element: ")?;
        write_encoded::<G>(f, &self.element)?;
        write!(f, ", count: {} }}", self.count)
    }
}

/// Elements print as their canonical encoding in hexadecimal, which is the same
/// for equal elements, as a group's own representation of an element need not
/// be.
fn write_encoded<G: DigestGroup>(f: &mut fmt::Formatter<'_>, element: &G::Element) -> fmt::Result {
    let mut encoding = vec![0u8; G::ELEMENT_LEN];
    G::encode(element, &mut encoding);
    encoding.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core_types::EntityId;

    fn leaf_element(n: u8) -> LeafElement { Leaf::new(&[n], EntityId::from_bytes([n; 32]), HeadHash::of([[n; 32]])).element() }

    /// A digest with this count; the element plays no part in count overflow.
    fn counted(count: i64) -> Digest { Digest { element: leaf_element(1).0, count } }

    #[test]
    fn the_empty_digest_is_the_identity_and_a_digest_minus_itself_is_empty() {
        let digest = Digest::from(leaf_element(1)) + Digest::from(leaf_element(2));
        assert_eq!(digest + Digest::empty(), digest);
        assert_eq!(Digest::empty() + digest, digest);
        assert_eq!(digest - Digest::empty(), digest);
        assert_eq!(-Digest::<Ristretto255>::empty(), Digest::empty());
        assert_eq!(std::iter::empty::<Digest>().sum::<Digest>(), Digest::empty());
        assert_eq!(digest - digest, Digest::empty());
        assert_eq!(digest + -digest, Digest::empty());
    }

    #[test]
    fn a_difference_that_removes_more_leaves_than_it_adds_has_a_negative_count() {
        let (a, b) = (Digest::from(leaf_element(1)), Digest::from(leaf_element(2)));
        let removal = Digest::empty() - a;
        assert_eq!(removal.count(), -1);
        assert_eq!(removal, -a);
        assert_eq!((removal - b).count(), -2);
        assert_eq!(removal + a, Digest::empty());
        // Replacing one leaf with another removes one and adds one.
        assert_eq!((b - a).count(), 0);
        assert_eq!(a + (b - a), b);
    }

    #[test]
    fn checked_operations_refuse_counts_beyond_i64() {
        assert_eq!(counted(i64::MAX).checked_add(counted(1)), Err(CountOverflow));
        assert_eq!(counted(i64::MIN).checked_add(counted(-1)), Err(CountOverflow));
        assert_eq!(counted(i64::MIN).checked_sub(counted(1)), Err(CountOverflow));
        assert_eq!(counted(i64::MAX).checked_sub(counted(-1)), Err(CountOverflow));
        assert_eq!(counted(i64::MIN).checked_neg(), Err(CountOverflow));
        assert_eq!(Digest::checked_sum([counted(i64::MAX), counted(1)]), Err(CountOverflow));
        assert_eq!(Digest::checked_sum([counted(i64::MIN), counted(-1)]), Err(CountOverflow));
        assert_eq!(Digest::checked_sum([counted(i64::MAX), counted(1)].iter()), Err(CountOverflow));
        assert_eq!(Digest::checked_sum([counted(i64::MIN), counted(-1)].iter()), Err(CountOverflow));

        // Up to the limits, they succeed.
        let count = |result: Result<Digest, CountOverflow>| result.map(|digest| digest.count());
        assert_eq!(count(counted(i64::MAX - 1).checked_add(counted(1))), Ok(i64::MAX));
        assert_eq!(count(counted(i64::MIN + 1).checked_sub(counted(1))), Ok(i64::MIN));
        assert_eq!(count(counted(i64::MAX).checked_neg()), Ok(-i64::MAX));
        assert_eq!(count(Digest::checked_sum([counted(i64::MAX), counted(i64::MIN)])), Ok(-1));
    }

    // The operators check the count explicitly rather than through the
    // build's overflow checks, so these panic under `cargo test --release`
    // too.

    #[test]
    #[should_panic(expected = "count overflows")]
    fn adding_past_i64_max_panics() { let _ = counted(i64::MAX) + counted(1); }

    #[test]
    #[should_panic(expected = "count overflows")]
    fn subtracting_past_i64_min_panics() { let _ = counted(i64::MIN) - counted(1); }

    #[test]
    #[should_panic(expected = "count overflows")]
    fn negating_i64_min_panics() { let _ = -counted(i64::MIN); }

    #[test]
    #[should_panic(expected = "count overflows")]
    fn add_assign_past_i64_max_panics() {
        let mut digest = counted(i64::MAX);
        digest += counted(1);
    }

    #[test]
    #[should_panic(expected = "count overflows")]
    fn sub_assign_past_i64_min_panics() {
        let mut digest = counted(i64::MIN);
        digest -= counted(1);
    }

    #[test]
    #[should_panic(expected = "count overflows")]
    fn an_owned_sum_past_i64_max_panics() { let _: Digest = [counted(i64::MAX), counted(1)].into_iter().sum(); }

    #[test]
    #[should_panic(expected = "count overflows")]
    fn a_borrowed_sum_past_i64_min_panics() { let _: Digest = [counted(i64::MIN), counted(-1)].iter().sum(); }
}
