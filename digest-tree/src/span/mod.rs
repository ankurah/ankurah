//! Spans of addresses, the subtrees that name them, and their digests: the
//! values peers exchange.

mod address;
mod digest;
mod prefix;

use std::cmp::Ordering;
use std::ops::Bound;

use serde::{Deserialize, Serialize};

pub use address::leaf_address;
pub(crate) use address::{address_of, key_prefix, split_address};
pub use digest::SpanDigest;
pub use prefix::NodePrefix;

/// Whether bounds hold no key: the start lies past the end, or on it with
/// either bound excluding it.
pub(crate) fn holds_nothing<K: Ord + ?Sized>(start: Bound<&K>, end: Bound<&K>) -> bool {
    match (start, end) {
        (Bound::Included(start), Bound::Included(end)) => start > end,
        (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start >= end,
        _ => false,
    }
}

/// A range of addresses: the unit peers ask about. Deserializing one checks
/// nothing; a receiver validates spans explicitly.
///
/// Bounds compare by their bytes, so a span between two adjacent byte strings,
/// such as from `a` excluded to `a` followed by a zero byte excluded, holds no
/// address yet is not reported empty: such a span only costs a comparison that
/// finds nothing.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Span {
    /// Where the span starts.
    pub start: Bound<Vec<u8>>,

    /// Where the span ends.
    pub end: Bound<Vec<u8>>,
}

impl Span {
    /// The span between these bounds.
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self { Self { start, end } }

    /// Every address.
    pub fn all() -> Self { Self::new(Bound::Unbounded, Bound::Unbounded) }

    /// The addresses of the members whose keys lie between these bounds. Each
    /// bound is a key prefix, as the owner's collation encodes a key's leading
    /// parts, and is widened to every key it begins: an included lower bound
    /// starts at the first of them and an excluded one after the last, and an
    /// included upper bound ends after the last of them and an excluded one
    /// before the first.
    pub fn for_key_range(lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let start = match lower {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included([]) => Bound::Unbounded,
            Bound::Included(prefix) => Bound::Included(key_prefix(prefix)),

            Bound::Excluded(prefix) => match prefix_end(&key_prefix(prefix)) {
                Some(after) => Bound::Included(after),

                // Every key begins with the empty prefix.
                None => return Self::holding_nothing(),
            },
        };

        let end = match upper {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(prefix) => prefix_end(&key_prefix(prefix)).map_or(Bound::Unbounded, Bound::Excluded),
            Bound::Excluded([]) => return Self::holding_nothing(),
            Bound::Excluded(prefix) => Bound::Excluded(key_prefix(prefix)),
        };

        Self::new(start, end)
    }

    /// A span that holds no address.
    fn holding_nothing() -> Self { Self::new(Bound::Excluded(Vec::new()), Bound::Excluded(Vec::new())) }

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

    /// Whether no address lies within the span: its start lies past its end,
    /// or on it with either bound excluding it.
    pub fn is_empty(&self) -> bool { holds_nothing(self.start.as_ref(), self.end.as_ref()) }

    /// Whether every address within the span lies within `other`, judged by
    /// the bounds: an empty span lies within every span.
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.is_empty() || (cmp_starts(&self.start, &other.start).is_ge() && cmp_ends(&self.end, &other.end).is_le())
    }

    /// The order of the two spans' starts.
    pub fn cmp_start(&self, other: &Self) -> Ordering { cmp_starts(&self.start, &other.start) }

    /// The addresses within both spans, if any.
    pub fn intersect(&self, other: &Self) -> Option<Self> {
        let start = if cmp_starts(&self.start, &other.start).is_ge() { &self.start } else { &other.start };
        let end = if cmp_ends(&self.end, &other.end).is_le() { &self.end } else { &other.end };
        let span = Self::new(start.clone(), end.clone());

        (!span.is_empty()).then_some(span)
    }

    /// The parts of the span that none of `covered` reaches, in order.
    /// `covered` must be disjoint and ordered.
    pub fn gaps(&self, covered: &[Self]) -> Vec<Self> {
        let mut start = self.start.clone();
        let mut gaps = Vec::new();

        for covered in covered {
            let Some(covered) = self.intersect(covered) else { continue };
            if let Some(end) = flip(covered.start) {
                let gap = Self::new(start, end);
                if !gap.is_empty() {
                    gaps.push(gap);
                }
            }

            match flip(covered.end) {
                Some(after) => start = after,
                None => return gaps,
            }
        }

        let tail = Self::new(start, self.end.clone());
        if !tail.is_empty() {
            gaps.push(tail);
        }

        gaps
    }

    /// The addresses of `spans` as spans in address order that neither
    /// overlap nor touch: empty spans dropped, the rest sorted by start and
    /// merged where they meet.
    pub(crate) fn normalize(mut spans: Vec<Self>) -> Vec<Self> {
        spans.retain(|span| !span.is_empty());
        spans.sort_by(Self::cmp_start);
        let mut out: Vec<Self> = Vec::with_capacity(spans.len());

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
}

/// The bound on the far side of `bound`: where a gap ends that the bound
/// starts, or starts that it ends. `None` for an unbounded bound, which leaves
/// no gap.
fn flip(bound: Bound<Vec<u8>>) -> Option<Bound<Vec<u8>>> {
    match bound {
        Bound::Included(at) => Some(Bound::Excluded(at)),
        Bound::Excluded(at) => Some(Bound::Included(at)),
        Bound::Unbounded => None,
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
/// when no string follows them all, as for the empty prefix or one made only
/// of `0xFF` bytes.
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

#[cfg(test)]
mod tests;
