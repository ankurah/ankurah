use std::borrow::Borrow;
use std::fmt;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

use curve25519_dalek::ristretto::RistrettoPoint;
use curve25519_dalek::traits::Identity;

/// One leaf's point in ristretto255, made by [`Leaf::point`](crate::Leaf::point).
///
/// A digest gains it when the leaf enters a tree and loses it when the leaf
/// leaves or is replaced.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LeafPoint(pub(crate) RistrettoPoint);

/// The sum of the points of a multiset of leaves, with the number of leaves
/// summed.
///
/// A node's digest is the digest of the leaves beneath it, and the digest of a
/// range is the sum of the digests that tile it. Two digests are equal when
/// both their points and their counts are.
///
/// The count is signed so that the difference of two digests, such as the
/// change one commit makes to every node on its path, is a digest too. A
/// node's own digest never has a negative count; one that does records a
/// removal of a leaf that was never added.
///
/// # Count overflow
///
/// The count is a number of leaves, so arithmetic never wraps it. The checked
/// operations ([`Digest::checked_add`], [`Digest::checked_sub`],
/// [`Digest::checked_neg`] and [`Digest::checked_sum`]) return
/// [`CountOverflow`] when the count would leave the range of `i64`; use them
/// whenever a digest decoded from a row or received from another member takes
/// part, since its count may be any `i64`. The operators (`+`, `-`, unary `-`,
/// `+=`, `-=` and [`Sum`]) are for digests built from leaves this process
/// hashed, whose counts cannot come near the limit: they panic on overflow, in
/// debug and release builds alike, rather than wrap.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Digest {
    pub(crate) point: RistrettoPoint,
    pub(crate) count: i64,
}

impl Digest {
    /// The digest of no leaves: the group identity and a count of zero.
    pub fn empty() -> Self { Self { point: RistrettoPoint::identity(), count: 0 } }

    /// The number of leaves summed; negative for a difference that removes
    /// more leaves than it adds.
    pub fn count(&self) -> i64 { self.count }

    /// `self + other`, or [`CountOverflow`] if the count would leave the range
    /// of `i64`.
    pub fn checked_add(self, other: Digest) -> Result<Digest, CountOverflow> {
        let count = self.count.checked_add(other.count).ok_or(CountOverflow)?;
        Ok(Digest { point: self.point + other.point, count })
    }

    /// `self - other`, or [`CountOverflow`] if the count would leave the range
    /// of `i64`.
    pub fn checked_sub(self, other: Digest) -> Result<Digest, CountOverflow> {
        let count = self.count.checked_sub(other.count).ok_or(CountOverflow)?;
        Ok(Digest { point: self.point - other.point, count })
    }

    /// `-self`, or [`CountOverflow`] for a count of `i64::MIN`, whose negation
    /// `i64` cannot hold.
    pub fn checked_neg(self) -> Result<Digest, CountOverflow> {
        let count = self.count.checked_neg().ok_or(CountOverflow)?;
        Ok(Digest { point: -self.point, count })
    }

    /// The sum of `digests`, or [`CountOverflow`] if the running count leaves
    /// the range of `i64` at any step.
    pub fn checked_sum<D: Borrow<Digest>>(digests: impl IntoIterator<Item = D>) -> Result<Digest, CountOverflow> {
        digests.into_iter().try_fold(Digest::empty(), |sum, digest| sum.checked_add(*digest.borrow()))
    }
}

impl Default for Digest {
    fn default() -> Self { Self::empty() }
}

impl From<LeafPoint> for Digest {
    /// The digest of the one leaf whose point this is.
    fn from(leaf: LeafPoint) -> Self { Self { point: leaf.0, count: 1 } }
}

/// The count of a digest would leave the range of `i64`.
///
/// A count is a number of leaves, so it never wraps: the checked operations on
/// [`Digest`] return this error, and the operators panic with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a digest's count overflows i64")]
pub struct CountOverflow;

/// The operators' overflow policy: panic, in every build, rather than wrap.
fn panic_on_overflow(result: Result<Digest, CountOverflow>) -> Digest { result.unwrap_or_else(|overflow| panic!("{overflow}")) }

impl Add for Digest {
    type Output = Digest;
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_add`].
    fn add(self, other: Digest) -> Digest { panic_on_overflow(self.checked_add(other)) }
}

impl Sub for Digest {
    type Output = Digest;
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_sub`].
    fn sub(self, other: Digest) -> Digest { panic_on_overflow(self.checked_sub(other)) }
}

impl Neg for Digest {
    type Output = Digest;
    /// Panics for a count of `i64::MIN`, in every build: see
    /// [`Digest::checked_neg`].
    fn neg(self) -> Digest { panic_on_overflow(self.checked_neg()) }
}

impl AddAssign for Digest {
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_add`].
    fn add_assign(&mut self, other: Digest) { *self = *self + other }
}

impl SubAssign for Digest {
    /// Panics if the count overflows `i64`, in every build: see
    /// [`Digest::checked_sub`].
    fn sub_assign(&mut self, other: Digest) { *self = *self - other }
}

impl Sum for Digest {
    /// Panics if the running count overflows `i64`, in every build: see
    /// [`Digest::checked_sum`].
    fn sum<I: Iterator<Item = Digest>>(digests: I) -> Digest { panic_on_overflow(Digest::checked_sum(digests)) }
}

impl<'a> Sum<&'a Digest> for Digest {
    /// Panics if the running count overflows `i64`, in every build: see
    /// [`Digest::checked_sum`].
    fn sum<I: Iterator<Item = &'a Digest>>(digests: I) -> Digest { panic_on_overflow(Digest::checked_sum(digests)) }
}

impl fmt::Debug for LeafPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LeafPoint(")?;
        write_compressed(f, &self.0)?;
        write!(f, ")")
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest {{ point: ")?;
        write_compressed(f, &self.point)?;
        write!(f, ", count: {} }}", self.count)
    }
}

/// Points print as their canonical encoding in hexadecimal; the curve
/// library's own `Debug` prints internal coordinates, which differ between
/// equal points.
fn write_compressed(f: &mut fmt::Formatter<'_>, point: &RistrettoPoint) -> fmt::Result {
    point.compress().as_bytes().iter().try_for_each(|byte| write!(f, "{byte:02x}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HeadHash, Leaf};
    use ankurah_core_types::EntityId;

    fn leaf_point(n: u8) -> LeafPoint { Leaf::new(&[n], EntityId::from_bytes([n; 32]), HeadHash::of([[n; 32]])).point() }

    /// A digest with this count; the point plays no part in count overflow.
    fn counted(count: i64) -> Digest { Digest { point: leaf_point(1).0, count } }

    #[test]
    fn the_empty_digest_is_the_identity_and_a_digest_minus_itself_is_empty() {
        let digest = Digest::from(leaf_point(1)) + Digest::from(leaf_point(2));
        assert_eq!(digest + Digest::empty(), digest);
        assert_eq!(Digest::empty() + digest, digest);
        assert_eq!(digest - Digest::empty(), digest);
        assert_eq!(-Digest::empty(), Digest::empty());
        assert_eq!(std::iter::empty::<Digest>().sum::<Digest>(), Digest::empty());
        assert_eq!(digest - digest, Digest::empty());
        assert_eq!(digest + -digest, Digest::empty());
    }

    #[test]
    fn a_difference_that_removes_more_leaves_than_it_adds_has_a_negative_count() {
        let (a, b) = (Digest::from(leaf_point(1)), Digest::from(leaf_point(2)));
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
