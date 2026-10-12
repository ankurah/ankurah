//! Operations on exact leaf-address and encoded-value intervals.
use crate::AddressRange;
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
pub(crate) fn empty(range: &AddressRange) -> bool {
    match (&range.start, &range.end) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a >= b,
    }
}
pub(crate) fn intersect(a: &AddressRange, b: &AddressRange) -> Option<AddressRange> {
    let start = if lower_cmp(&a.start, &b.start).is_ge() { &a.start } else { &b.start };
    let end = if upper_cmp(&a.end, &b.end).is_le() { &a.end } else { &b.end };
    let range = AddressRange::new(start.clone(), end.clone());
    (!empty(&range)).then_some(range)
}
pub(crate) fn subset(a: &AddressRange, b: &AddressRange) -> bool {
    empty(a) || (lower_cmp(&a.start, &b.start).is_ge() && upper_cmp(&a.end, &b.end).is_le())
}
pub(crate) fn singleton(range: &AddressRange) -> Option<&[u8]> {
    match (&range.start, &range.end) {
        (Bound::Included(a), Bound::Included(b)) if a == b => Some(a),
        _ => None,
    }
}
pub(crate) fn normalize(mut ranges: Vec<AddressRange>) -> Vec<AddressRange> {
    ranges.retain(|range| !empty(range));
    ranges.sort_by(|a, b| lower_cmp(&a.start, &b.start));
    let mut out: Vec<AddressRange> = Vec::new();
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
