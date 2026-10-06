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
}

impl Default for Digest {
    fn default() -> Self { Self::empty() }
}

impl From<LeafPoint> for Digest {
    /// The digest of the one leaf whose point this is.
    fn from(leaf: LeafPoint) -> Self { Self { point: leaf.0, count: 1 } }
}

impl Add for Digest {
    type Output = Digest;
    fn add(self, other: Digest) -> Digest { Digest { point: self.point + other.point, count: self.count + other.count } }
}

impl Sub for Digest {
    type Output = Digest;
    fn sub(self, other: Digest) -> Digest { Digest { point: self.point - other.point, count: self.count - other.count } }
}

impl Neg for Digest {
    type Output = Digest;
    fn neg(self) -> Digest { Digest { point: -self.point, count: -self.count } }
}

impl AddAssign for Digest {
    fn add_assign(&mut self, other: Digest) { *self = *self + other }
}

impl SubAssign for Digest {
    fn sub_assign(&mut self, other: Digest) { *self = *self - other }
}

impl Sum for Digest {
    fn sum<I: Iterator<Item = Digest>>(digests: I) -> Digest { digests.fold(Digest::empty(), Add::add) }
}

impl<'a> Sum<&'a Digest> for Digest {
    fn sum<I: Iterator<Item = &'a Digest>>(digests: I) -> Digest { digests.copied().sum() }
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
