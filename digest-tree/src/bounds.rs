//! Operations on exact leaf-address and encoded-value intervals.
use crate::TreeSpan;
use std::cmp::Ordering;
use std::ops::Bound;

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

#[cfg(test)]
mod tests {
    use super::*;

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
