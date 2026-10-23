//! Spans of leaf addresses: the runs of leaves a scope reads and a span
//! digest sums, with the interval operations they share with the
//! encoded-value intervals of a request's clauses.

use std::cmp::Ordering;
use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::address::NodePrefix;
use crate::digest::{Digest, DigestGroup, Ristretto255};
use crate::error::TreeError;
use crate::store::ByteBounds;

/// A span of leaf addresses. Endpoints are complete addresses, not suffixes
/// relative to a node prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeSpan {
    /// The lower bound.
    pub start: Bound<Vec<u8>>,
    /// The upper bound.
    pub end: Bound<Vec<u8>>,
}

impl TreeSpan {
    /// The range between two bounds.
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self { Self { start, end } }

    /// Every address.
    pub fn all() -> Self { Self::new(Bound::Unbounded, Bound::Unbounded) }

    /// Whether this span contains no addresses.
    pub fn is_empty(&self) -> bool { empty(self) }

    /// Whether every address in this span also belongs to `other`.
    pub fn is_subset_of(&self, other: &Self) -> bool { subset(self, other) }

    /// Compare lower bounds in address order, including open and unbounded ends.
    pub fn cmp_start(&self, other: &Self) -> std::cmp::Ordering { lower_cmp(&self.start, &other.start) }

    /// Every address beneath `prefix`: the leaves of a bucket, or of any tree
    /// node.
    pub fn under(prefix: &NodePrefix) -> Self {
        let (start, end) = prefix.address_bounds();
        Self::new(Bound::Included(start), end.map_or(Bound::Unbounded, Bound::Excluded))
    }

    /// The rest of this range after `address`, to read the next page.
    pub fn after(&self, address: &[u8]) -> Self { Self::new(Bound::Excluded(address.to_vec()), self.end.clone()) }

    /// The bounds as a range scan takes them.
    pub fn bounds(&self) -> ByteBounds<'_> { (self.start.as_ref().map(Vec::as_slice), self.end.as_ref().map(Vec::as_slice)) }

    /// Whether `address` lies within the range.
    pub fn contains(&self, address: &[u8]) -> bool {
        let above_start = match &self.start {
            Bound::Included(start) => address >= start.as_slice(),
            Bound::Excluded(start) => address > start.as_slice(),
            Bound::Unbounded => true,
        };
        let below_end = match &self.end {
            Bound::Included(end) => address <= end.as_slice(),
            Bound::Excluded(end) => address < end.as_slice(),
            Bound::Unbounded => true,
        };
        above_start && below_end
    }
}

pub(crate) fn lower_cmp(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => a.cmp(b),
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Less),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Greater),
    }
}
pub(crate) fn upper_cmp(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => a.cmp(b),
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Greater),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Less),
    }
}
pub(crate) fn empty(range: &TreeSpan) -> bool {
    match (&range.start, &range.end) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a >= b,
    }
}
pub(crate) fn intersect(a: &TreeSpan, b: &TreeSpan) -> Option<TreeSpan> {
    let start = if lower_cmp(&a.start, &b.start).is_ge() { &a.start } else { &b.start };
    let end = if upper_cmp(&a.end, &b.end).is_le() { &a.end } else { &b.end };
    let range = TreeSpan::new(start.clone(), end.clone());
    (!empty(&range)).then_some(range)
}

/// The portions of `range` outside a sorted, disjoint set of covered ranges.
/// Keeping these gaps matters when the other peer has leaves absent locally.
pub(crate) fn gaps(range: &TreeSpan, covered: &[TreeSpan]) -> Vec<TreeSpan> {
    let mut start = range.start.clone();
    let mut gaps = Vec::new();
    for covered in covered {
        let Some(covered) = intersect(range, covered) else { continue };
        let end = match covered.start {
            Bound::Included(value) => Bound::Excluded(value),
            Bound::Excluded(value) => Bound::Included(value),
            Bound::Unbounded => {
                start = match covered.end {
                    Bound::Included(value) => Bound::Excluded(value),
                    Bound::Excluded(value) => Bound::Included(value),
                    Bound::Unbounded => return gaps,
                };
                continue;
            }
        };
        let gap = TreeSpan::new(start, end);
        if !empty(&gap) {
            gaps.push(gap);
        }
        start = match covered.end {
            Bound::Included(value) => Bound::Excluded(value),
            Bound::Excluded(value) => Bound::Included(value),
            Bound::Unbounded => return gaps,
        };
    }
    let tail = TreeSpan::new(start, range.end.clone());
    if !empty(&tail) {
        gaps.push(tail);
    }
    gaps
}
pub(crate) fn subset(a: &TreeSpan, b: &TreeSpan) -> bool {
    empty(a) || (lower_cmp(&a.start, &b.start).is_ge() && upper_cmp(&a.end, &b.end).is_le())
}
pub(crate) fn singleton(range: &TreeSpan) -> Option<&[u8]> {
    match (&range.start, &range.end) {
        (Bound::Included(a), Bound::Included(b)) if a == b => Some(a),
        _ => None,
    }
}
pub(crate) fn normalize(mut ranges: Vec<TreeSpan>) -> Vec<TreeSpan> {
    ranges.retain(|range| !empty(range));
    ranges.sort_by(|a, b| lower_cmp(&a.start, &b.start));
    let mut out: Vec<TreeSpan> = Vec::new();
    for range in ranges {
        if let Some(previous) = out.last_mut() {
            let touches = match (&previous.end, &range.start) {
                (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Excluded(b)) => a > b,
                (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a >= b,
            };
            if touches {
                if upper_cmp(&range.end, &previous.end).is_gt() {
                    previous.end = range.end;
                }
                continue;
            }
        }
        out.push(range);
    }
    out
}

/// First byte string after every string starting with `prefix`.
pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.pop() {
        if byte != 255 {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}

pub(crate) fn prefix_range(prefix: &[u8]) -> TreeSpan {
    TreeSpan::new(Bound::Included(prefix.to_vec()), prefix_end(prefix).map_or(Bound::Unbounded, Bound::Excluded))
}

pub(crate) fn intersections(a: &[TreeSpan], b: &[TreeSpan]) -> Vec<TreeSpan> {
    normalize(a.iter().flat_map(|a| b.iter().filter_map(move |b| intersect(a, b))).collect())
}

/// A tree span and the digest of its leaves, cached or computed for clipped bounds.
/// Bounds are complete addresses, so another tree can compute the same span independently.
/// A peer must also establish compatible keys, digest groups and admitted
/// membership; a span digest alone does not carry those proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeSpanDigest<G: DigestGroup = Ristretto255> {
    /// The containing subtree from which refinement continues. Bounds alone
    /// do not identify this ancestor. A clipped or empty span need not have
    /// a stored node at this prefix on the answering peer.
    pub prefix: NodePrefix,
    /// The exact span within that subtree, independent of the prefix encoding.
    pub bounds: TreeSpan,
    /// Digest of precisely the leaves within these bounds.
    pub digest: Digest<G>,
}

impl<G: DigestGroup> Serialize for TreeSpanDigest<G> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { self.to_bytes().serialize(serializer) }
}

impl<'de, G: DigestGroup> Deserialize<'de> for TreeSpanDigest<G> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}
impl<G: DigestGroup> TreeSpanDigest<G> {
    /// Encode a versioned span digest for a peer, independent of a local snapshot.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&(1u8, self.prefix.to_bytes(), &self.bounds.start, &self.bounds.end, self.digest.to_row_bytes())).unwrap()
    }
    /// Decode a span digest and validate its prefix, bounds, count and group encoding.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TreeError> {
        use bincode::Options;
        type EncodedSpanDigest = (u8, Vec<u8>, std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>, Vec<u8>);
        let (version, prefix, start, end, digest): EncodedSpanDigest = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes()
            .deserialize(bytes)
            .map_err(|e| TreeError::unreadable("tree span digest", e))?;
        if version != 1 {
            return Err(TreeError::InvalidKey("unknown tree span digest version"));
        }
        let prefix = NodePrefix::from_bytes(&prefix).map_err(|e| TreeError::unreadable("span digest prefix", e))?;
        let bounds = TreeSpan::new(start, end);
        let digest = Digest::from_row_bytes(&digest).map_err(|e| TreeError::unreadable("span digest", e))?;
        let head = Self { prefix, bounds, digest };
        head.validate()?;
        Ok(head)
    }

    /// Validate a typed span digest without encoding and decoding it again.
    /// Prefix and group encodings are already checked by their constructors.
    pub fn validate(&self) -> Result<(), TreeError> {
        if self.bounds.is_empty() || !self.bounds.is_subset_of(&TreeSpan::under(&self.prefix)) || self.digest.count() < 0 {
            return Err(TreeError::InvalidKey("invalid tree span digest bounds or count"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    #[test]
    fn contains_honours_each_kind_of_bound() {
        let included = TreeSpan::new(Bound::Included(vec![2]), Bound::Included(vec![4]));
        let excluded = TreeSpan::new(Bound::Excluded(vec![2]), Bound::Excluded(vec![4]));
        for address in [vec![1], vec![2], vec![3], vec![4], vec![4, 0], vec![5]] {
            let inside = address >= vec![2] && address <= vec![4];
            assert_eq!(included.contains(&address), inside, "{address:?} within the closed range");
            let inside = address > vec![2] && address < vec![4];
            assert_eq!(excluded.contains(&address), inside, "{address:?} within the open range");
            assert!(TreeSpan::all().contains(&address));
        }
    }

    #[test]
    fn the_next_page_starts_past_the_last_address_read_and_keeps_the_end() {
        let range = TreeSpan::new(Bound::Included(vec![1]), Bound::Excluded(vec![9]));
        let next = range.after(&[3]);
        assert_eq!(next, TreeSpan::new(Bound::Excluded(vec![3]), Bound::Excluded(vec![9])));
        assert_eq!(next.bounds(), (Bound::Excluded(&[3][..]), Bound::Excluded(&[9][..])));
        assert!(!next.contains(&[3]) && next.contains(&[3, 0]) && !next.contains(&[9]));
    }

    /// The range under a prefix holds exactly the addresses that begin with
    /// its bits, judged by the prefix itself.
    #[test]
    fn the_range_under_a_prefix_holds_exactly_the_addresses_it_contains() {
        let mut rng = SmallRng::seed_from_u64(17);
        for _ in 0..200 {
            let len = rng.gen_range(0..=40);
            let from: Vec<u8> = (0..5).map(|_| rng.gen::<u8>() & 0xC3).collect();
            let prefix = NodePrefix::of(&from, len);
            let range = TreeSpan::under(&prefix);
            for _ in 0..50 {
                let address: Vec<u8> = (0..rng.gen_range(0..=6)).map(|_| rng.gen::<u8>() & 0xC3).collect();
                assert_eq!(range.contains(&address), prefix.contains_address(&address), "{prefix:?} against {address:?}");
            }
        }
        assert_eq!(TreeSpan::under(&NodePrefix::root()), TreeSpan::new(Bound::Included(vec![]), Bound::Unbounded));
    }

    #[test]
    fn gaps_partition_each_kind_of_boundary() {
        let mut endpoints = vec![Bound::Unbounded];
        for value in [vec![], vec![1], vec![1, 0], vec![2]] {
            endpoints.push(Bound::Included(value.clone()));
            endpoints.push(Bound::Excluded(value));
        }
        let ranges: Vec<_> = endpoints
            .iter()
            .flat_map(|start| endpoints.iter().map(move |end| TreeSpan::new(start.clone(), end.clone())))
            .filter(|range| !empty(range))
            .collect();
        let samples = [vec![], vec![0], vec![1], vec![1, 0], vec![1, 0, 0], vec![1, 1], vec![2], vec![2, 0], vec![255]];
        for range in &ranges {
            for a in &ranges {
                for b in &ranges {
                    let covered = normalize(vec![a.clone(), b.clone()]);
                    let missing = gaps(range, &covered);
                    for address in &samples {
                        let uncovered = range.contains(address) && !covered.iter().any(|part| part.contains(address));
                        assert_eq!(
                            missing.iter().filter(|part| part.contains(address)).count(),
                            usize::from(uncovered),
                            "range={range:?}, covered={covered:?}, address={address:?}"
                        );
                    }
                }
            }
        }
    }
}
