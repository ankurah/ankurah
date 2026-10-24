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

/// A span of leaf addresses: every address from its start bound to its end
/// bound, in byte order. Endpoints are complete addresses, not suffixes
/// relative to a tree node's prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeSpan {
    /// The lower bound.
    pub start: Bound<Vec<u8>>,
    /// The upper bound.
    pub end: Bound<Vec<u8>>,
}

impl TreeSpan {
    /// The span between two bounds.
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self { Self { start, end } }

    /// Every address: the span under the root prefix, from the empty byte
    /// string on, which a span digest at the root carries.
    pub fn all() -> Self { Self::under(&NodePrefix::root()) }

    /// Every address beneath `prefix`: the leaves of a bucket, or of any tree
    /// node.
    pub fn under(prefix: &NodePrefix) -> Self {
        let (start, end) = prefix.address_bounds();
        Self::new(Bound::Included(start), end.map_or(Bound::Unbounded, Bound::Excluded))
    }

    /// Every byte string that begins with `bytes`.
    pub(crate) fn starting_with(bytes: &[u8]) -> Self {
        Self::new(Bound::Included(bytes.to_vec()), prefix_end(bytes).map_or(Bound::Unbounded, Bound::Excluded))
    }

    /// The rest of this span after `address`, to read the next page.
    pub fn after(&self, address: &[u8]) -> Self { Self::new(Bound::Excluded(address.to_vec()), self.end.clone()) }

    /// The bounds as a range scan takes them.
    pub fn bounds(&self) -> ByteBounds<'_> { (self.start.as_ref().map(Vec::as_slice), self.end.as_ref().map(Vec::as_slice)) }

    /// Whether `address` lies within the span.
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

    /// Whether the bounds admit no address: the start lies past the end, or
    /// on it with either bound excluding it. A span between two adjacent byte
    /// strings, such as from `a` excluded to `a` followed by a zero byte
    /// excluded, holds no address either but is not reported empty.
    pub fn is_empty(&self) -> bool {
        match (&self.start, &self.end) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Included(start), Bound::Included(end)) => start > end,
            (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start >= end,
        }
    }

    /// Whether every address in this span also belongs to `other`, judged by
    /// the bounds: an empty span belongs to every span.
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.is_empty() || (cmp_starts(&self.start, &other.start).is_ge() && cmp_ends(&self.end, &other.end).is_le())
    }

    /// Compare this span's start with `other`'s in address order, excluded and
    /// unbounded starts included.
    pub fn cmp_start(&self, other: &Self) -> Ordering { cmp_starts(&self.start, &other.start) }

    /// The addresses in both this span and `other`; `None` when the
    /// intersection is empty.
    pub fn intersect(&self, other: &Self) -> Option<TreeSpan> {
        let start = if cmp_starts(&self.start, &other.start).is_ge() { &self.start } else { &other.start };
        let end = if cmp_ends(&self.end, &other.end).is_le() { &self.end } else { &other.end };
        let span = TreeSpan::new(start.clone(), end.clone());
        (!span.is_empty()).then_some(span)
    }

    /// The one byte string this span holds, when both its bounds include it.
    pub(crate) fn single(&self) -> Option<&[u8]> {
        match (&self.start, &self.end) {
            (Bound::Included(start), Bound::Included(end)) if start == end => Some(start),
            _ => None,
        }
    }

    /// The parts of this span outside `covered`, whose spans must be sorted
    /// by start and must not overlap. Keeping these gaps matters when the
    /// other peer has leaves absent locally.
    pub(crate) fn gaps(&self, covered: &[TreeSpan]) -> Vec<TreeSpan> {
        let mut start = self.start.clone();
        let mut gaps = Vec::new();
        for covered in covered {
            let Some(covered) = self.intersect(covered) else { continue };
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
            if !gap.is_empty() {
                gaps.push(gap);
            }
            start = match covered.end {
                Bound::Included(value) => Bound::Excluded(value),
                Bound::Excluded(value) => Bound::Included(value),
                Bound::Unbounded => return gaps,
            };
        }
        let tail = TreeSpan::new(start, self.end.clone());
        if !tail.is_empty() {
            gaps.push(tail);
        }
        gaps
    }

    /// The addresses of `spans` as spans in address order that neither
    /// overlap nor touch: empty spans dropped, the rest sorted by start and
    /// merged where they meet.
    pub(crate) fn normalize(mut spans: Vec<TreeSpan>) -> Vec<TreeSpan> {
        spans.retain(|span| !span.is_empty());
        spans.sort_by(TreeSpan::cmp_start);
        let mut out: Vec<TreeSpan> = Vec::new();
        for span in spans {
            if let Some(previous) = out.last_mut() {
                let touches = match (&previous.end, &span.start) {
                    (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
                    (Bound::Excluded(end), Bound::Excluded(start)) => end > start,
                    (Bound::Included(end) | Bound::Excluded(end), Bound::Included(start) | Bound::Excluded(start)) => end >= start,
                };
                if touches {
                    if cmp_ends(&span.end, &previous.end).is_gt() {
                        previous.end = span.end;
                    }
                    continue;
                }
            }
            out.push(span);
        }
        out
    }

    /// The addresses in both a span of `left` and a span of `right`,
    /// normalized.
    pub(crate) fn intersect_all(left: &[TreeSpan], right: &[TreeSpan]) -> Vec<TreeSpan> {
        TreeSpan::normalize(left.iter().flat_map(|left| right.iter().filter_map(move |right| left.intersect(right))).collect())
    }
}

/// Two lower bounds in address order: an unbounded start comes first, and an
/// included start before an excluded one of the same bytes.
fn cmp_starts(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => a.cmp(b),
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Less),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Greater),
    }
}

/// Two upper bounds in address order: an unbounded end comes last, and an
/// included end after an excluded one of the same bytes.
fn cmp_ends(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => a.cmp(b),
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Greater),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Less),
    }
}

/// The first byte string after every string that begins with `prefix`; `None`
/// when no string follows them all, as for a prefix of only 255 bytes.
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

/// A span of a tree and the digest of its leaves, which two peers compare:
/// the stored digest of a tree node the span covers whole, or else the sum of
/// the leaves within the span. The bounds are complete addresses, so another
/// tree can compute the digest of the same span independently. A peer must
/// also establish compatible keys, digest groups and admitted membership; a
/// span digest alone does not carry those proofs.
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
    /// Encode the span digest for a peer, behind a version byte.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&(1u8, self.prefix.to_bytes(), &self.bounds.start, &self.bounds.end, self.digest.to_row_bytes())).unwrap()
    }

    /// Decode a span digest a peer encoded, validating its prefix, bounds,
    /// count and group encoding.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, TreeError> {
        use bincode::Options;
        type EncodedSpanDigest = (u8, Vec<u8>, std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>, Vec<u8>);
        let (version, prefix, start, end, digest): EncodedSpanDigest = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes()
            .deserialize(bytes)
            .map_err(|e| TreeError::unreadable("tree span digest", e))?;
        if version != 1 {
            return Err(TreeError::InvalidSpan("unknown tree span digest version"));
        }
        let prefix = NodePrefix::from_bytes(&prefix).map_err(|e| TreeError::unreadable("span digest prefix", e))?;
        let bounds = TreeSpan::new(start, end);
        let digest = Digest::from_row_bytes(&digest).map_err(|e| TreeError::unreadable("span digest", e))?;
        let span_digest = Self { prefix, bounds, digest };
        span_digest.validate()?;
        Ok(span_digest)
    }

    /// Validate a typed span digest without encoding and decoding it again.
    /// Prefix and group encodings are already checked by their constructors.
    pub fn validate(&self) -> Result<(), TreeError> {
        if self.bounds.is_empty() || !self.bounds.is_subset_of(&TreeSpan::under(&self.prefix)) || self.digest.count() < 0 {
            return Err(TreeError::InvalidSpan("invalid tree span digest bounds or count"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::{HeadHash, Leaf, LtHash16};
    use ankurah_core_types::EntityId;
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    #[test]
    fn contains_honours_each_kind_of_bound() {
        let included = TreeSpan::new(Bound::Included(vec![2]), Bound::Included(vec![4]));
        let excluded = TreeSpan::new(Bound::Excluded(vec![2]), Bound::Excluded(vec![4]));
        for address in [vec![1], vec![2], vec![3], vec![4], vec![4, 0], vec![5]] {
            let inside = address >= vec![2] && address <= vec![4];
            assert_eq!(included.contains(&address), inside, "{address:?} within the closed span");
            let inside = address > vec![2] && address < vec![4];
            assert_eq!(excluded.contains(&address), inside, "{address:?} within the open span");
            assert!(TreeSpan::all().contains(&address));
        }
    }

    /// The span of every address is the span under the root prefix, so a
    /// span digest at the root carries it.
    #[test]
    fn every_address_is_the_span_under_the_root() {
        assert_eq!(TreeSpan::all(), TreeSpan::under(&NodePrefix::root()));
        let root = TreeSpanDigest::<Ristretto255> { prefix: NodePrefix::root(), bounds: TreeSpan::all(), digest: Digest::empty() };
        root.validate().unwrap();
    }

    #[test]
    fn the_next_page_starts_past_the_last_address_read_and_keeps_the_end() {
        let span = TreeSpan::new(Bound::Included(vec![1]), Bound::Excluded(vec![9]));
        let next = span.after(&[3]);
        assert_eq!(next, TreeSpan::new(Bound::Excluded(vec![3]), Bound::Excluded(vec![9])));
        assert_eq!(next.bounds(), (Bound::Excluded(&[3][..]), Bound::Excluded(&[9][..])));
        assert!(!next.contains(&[3]) && next.contains(&[3, 0]) && !next.contains(&[9]));
    }

    /// The span under a prefix holds exactly the addresses that begin with
    /// its bits, judged by the prefix itself.
    #[test]
    fn the_span_under_a_prefix_holds_exactly_the_addresses_it_contains() {
        let mut rng = SmallRng::seed_from_u64(17);
        for _ in 0..200 {
            let len = rng.gen_range(0..=40);
            let from: Vec<u8> = (0..5).map(|_| rng.gen::<u8>() & 0xC3).collect();
            let prefix = NodePrefix::of(&from, len);
            let span = TreeSpan::under(&prefix);
            for _ in 0..50 {
                let address: Vec<u8> = (0..rng.gen_range(0..=6)).map(|_| rng.gen::<u8>() & 0xC3).collect();
                assert_eq!(span.contains(&address), prefix.contains_address(&address), "{prefix:?} against {address:?}");
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
        let spans: Vec<_> = endpoints
            .iter()
            .flat_map(|start| endpoints.iter().map(move |end| TreeSpan::new(start.clone(), end.clone())))
            .filter(|span| !span.is_empty())
            .collect();
        let samples = [vec![], vec![0], vec![1], vec![1, 0], vec![1, 0, 0], vec![1, 1], vec![2], vec![2, 0], vec![255]];
        for span in &spans {
            for a in &spans {
                for b in &spans {
                    let covered = TreeSpan::normalize(vec![a.clone(), b.clone()]);
                    let missing = span.gaps(&covered);
                    for address in &samples {
                        let uncovered = span.contains(address) && !covered.iter().any(|part| part.contains(address));
                        assert_eq!(
                            missing.iter().filter(|part| part.contains(address)).count(),
                            usize::from(uncovered),
                            "span={span:?}, covered={covered:?}, address={address:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn typed_and_decoded_span_digests_reject_the_same_invalid_bounds_and_counts() {
        let prefix = NodePrefix::of(&[0x80], 1);
        let valid = TreeSpanDigest::<Ristretto255> { prefix: prefix.clone(), bounds: TreeSpan::under(&prefix), digest: Digest::empty() };
        valid.validate().unwrap();
        assert_eq!(TreeSpanDigest::from_bytes(&valid.to_bytes()).unwrap(), valid);
        let leaf = Digest::from(Leaf::new(&[], EntityId::from_bytes([1; 32]), HeadHash::of([[1; 32]])).element());
        let invalid = [
            TreeSpanDigest { bounds: TreeSpan::new(Bound::Excluded(vec![0x80]), Bound::Included(vec![0x80])), ..valid.clone() },
            TreeSpanDigest { bounds: TreeSpan::new(Bound::Included(vec![0x90]), Bound::Included(vec![0x80])), ..valid.clone() },
            TreeSpanDigest { bounds: TreeSpan::all(), ..valid.clone() },
            TreeSpanDigest { digest: -leaf, ..valid },
        ];
        for span_digest in invalid {
            assert!(span_digest.validate().is_err());
            assert!(TreeSpanDigest::<Ristretto255>::from_bytes(&span_digest.to_bytes()).is_err());
        }
    }

    /// A span digest that crosses the wire is validated as it decodes: an
    /// unknown version, a prefix that does not decode, a negative count, a
    /// digest of another group, empty bounds, bounds outside the prefix and
    /// trailing bytes are each refused.
    #[test]
    fn span_digest_deserialization_preserves_the_tree_crates_validation() {
        let root = NodePrefix::root();
        let bounds = TreeSpan::under(&NodePrefix::root());
        let mut negative = Digest::<Ristretto255>::empty().to_row_bytes();
        let count_offset = negative.len() - 8;
        negative[count_offset..].copy_from_slice(&(-1i64).to_be_bytes());
        let encode = |version, prefix: Vec<u8>, bounds: &TreeSpan, digest| {
            bincode::serialize(&(version, prefix, &bounds.start, &bounds.end, digest)).unwrap()
        };
        let empty_digest = Digest::<Ristretto255>::empty().to_row_bytes();
        let valid = encode(1u8, root.to_bytes(), &bounds, empty_digest.clone());
        let mut trailing = valid.clone();
        trailing.push(0);
        for bytes in [
            encode(2u8, root.to_bytes(), &bounds, empty_digest.clone()),
            encode(1u8, vec![255], &bounds, empty_digest.clone()),
            encode(1u8, root.to_bytes(), &bounds, negative),
            encode(1u8, root.to_bytes(), &bounds, Digest::<LtHash16>::empty().to_row_bytes()),
            encode(1u8, root.to_bytes(), &TreeSpan::new(Bound::Included(vec![2]), Bound::Excluded(vec![1])), empty_digest.clone()),
            encode(1u8, NodePrefix::of(&[0], 1).to_bytes(), &bounds, empty_digest),
            trailing,
        ] {
            let framed = bincode::serialize(&bytes).unwrap();
            assert!(bincode::deserialize::<TreeSpanDigest>(&framed).is_err());
        }
        let decoded: TreeSpanDigest = bincode::deserialize(&bincode::serialize(&valid).unwrap()).unwrap();
        assert_eq!(decoded.digest.count(), 0);
        assert_eq!(decoded.bounds, bounds);
    }
}
