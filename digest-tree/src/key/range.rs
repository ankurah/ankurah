//! Key ranges: which members a span holds, by their values, and the span of
//! addresses a key range lowers to.

use std::ops::{Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

use ankurah_core_types::key_spec::{IndexDirection, KeySpec};
use ankurah_core_types::Value;

use super::{check_value, encode_part, present_tag};
use crate::{Error, Span};

// A key range keeps the key's parts apart where a key-value store takes a
// range of key bytes, because the lowering an index does one layer up, part
// by part, swapping a descending part's ends, happens here: the tree encodes
// the parts itself, and it holds members without values, which a range over a
// part must leave out and `..` must keep.

/// Which members a span holds, by their values: the values the key's leading
/// parts hold, and at most one range of values for the part after them, in
/// that part's own order.
///
/// A Rust range of values ranges over the key's first part: `2010..`, or a
/// pair of [`Bound`]s for a range that excludes its start. A range leaves out
/// the members that hold no value for its part, as a comparison does; `..`
/// keeps them. A key range with nothing in it holds every member.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyRange {
    /// The values of the leading parts, first part first: `None` for a part
    /// that holds no value.
    prefix: Vec<Option<Value>>,

    /// The range of values of the part after the prefix, if any.
    range: Option<(Bound<Value>, Bound<Value>)>,
}

impl KeyRange {
    /// Every member.
    fn all() -> Self { Self { prefix: Vec::new(), range: None } }

    /// The members whose leading parts hold `prefix`, first part first,
    /// `None` meaning no value, and whose next part holds a value within
    /// `range`; with no range, or one unbounded both ways, any part after
    /// them holds anything.
    pub fn new(prefix: Vec<Option<Value>>, range: Option<(Bound<Value>, Bound<Value>)>) -> Self {
        let free = Self { prefix, range: None };
        match range {
            Some((start, end)) => free.ranged(start, end),
            None => free,
        }
    }

    /// The members whose leading parts hold `values`, first part first.
    pub fn prefix(values: impl IntoIterator<Item = impl Into<Value>>) -> Self {
        Self { prefix: values.into_iter().map(|value| Some(value.into())).collect(), range: None }
    }

    /// These members, of those whose next part, after the prefix, holds a
    /// value within `range`. An unbounded `range` keeps every member, as
    /// `..` does.
    pub fn and<T: Into<Value> + Clone>(self, range: impl RangeBounds<T>) -> Self {
        let bound = |bound: Bound<&T>| bound.cloned().map(Into::into);
        self.ranged(bound(range.start_bound()), bound(range.end_bound()))
    }

    /// These members, with the part after the prefix ranging from `start` to
    /// `end`: none of a range unbounded both ways, which leaves the part free.
    fn ranged(mut self, start: Bound<Value>, end: Bound<Value>) -> Self {
        self.range = match (&start, &end) {
            (Bound::Unbounded, Bound::Unbounded) => None,
            _ => Some((start, end)),
        };

        self
    }
}

impl From<RangeFull> for KeyRange {
    fn from(_: RangeFull) -> Self { Self::all() }
}

impl<T: Into<Value>> From<Range<T>> for KeyRange {
    fn from(values: Range<T>) -> Self { Self::all().ranged(Bound::Included(values.start.into()), Bound::Excluded(values.end.into())) }
}

impl<T: Into<Value>> From<RangeInclusive<T>> for KeyRange {
    fn from(values: RangeInclusive<T>) -> Self {
        let (start, end) = values.into_inner();
        Self::all().ranged(Bound::Included(start.into()), Bound::Included(end.into()))
    }
}

impl<T: Into<Value>> From<RangeFrom<T>> for KeyRange {
    fn from(values: RangeFrom<T>) -> Self { Self::all().ranged(Bound::Included(values.start.into()), Bound::Unbounded) }
}

impl<T: Into<Value>> From<RangeTo<T>> for KeyRange {
    fn from(values: RangeTo<T>) -> Self { Self::all().ranged(Bound::Unbounded, Bound::Excluded(values.end.into())) }
}

impl<T: Into<Value>> From<RangeToInclusive<T>> for KeyRange {
    fn from(values: RangeToInclusive<T>) -> Self { Self::all().ranged(Bound::Unbounded, Bound::Included(values.end.into())) }
}

impl<T: Into<Value>> From<(Bound<T>, Bound<T>)> for KeyRange {
    fn from((start, end): (Bound<T>, Bound<T>)) -> Self { Self::all().ranged(start.map(Into::into), end.map(Into::into)) }
}

/// The span of the addresses of the members that `range` holds, under
/// `spec`: [`Error::KeyLength`] for values for more parts than the key has,
/// and [`Error::KeyValueType`] for a value of another type than its part's.
pub(crate) fn span_for<K>(spec: &KeySpec<K>, range: &KeyRange) -> Result<Span, Error> {
    let given = range.prefix.len() + usize::from(range.range.is_some());
    if given > spec.keyparts.len() {
        return Err(Error::KeyLength { parts: spec.keyparts.len(), given });
    }

    let mut prefix = Vec::new();
    for (index, (part, value)) in spec.keyparts.iter().zip(&range.prefix).enumerate() {
        if let Some(value) = value {
            check_value(part, index, value)?;
        }

        encode_part(part, value.as_ref(), &mut prefix);
    }

    let Some((low, high)) = &range.range else {
        // Every key the prefix begins.
        return Ok(Span::for_key_range(prefix.clone()..=prefix));
    };

    let index = range.prefix.len();
    let part = &spec.keyparts[index];

    // A bound on the part's values, as a prefix of keys.
    let key_bound = |bound: &Bound<Value>| -> Result<Bound<Vec<u8>>, Error> {
        let holding = |value: &Value| -> Result<Vec<u8>, Error> {
            check_value(part, index, value)?;

            let mut key = prefix.clone();
            encode_part(part, Some(value), &mut key);
            Ok(key)
        };

        Ok(match bound {
            Bound::Included(value) => Bound::Included(holding(value)?),
            Bound::Excluded(value) => Bound::Excluded(holding(value)?),

            // An open side still ends where the part's values do, keeping out
            // the members that hold none.
            Bound::Unbounded => Bound::Included([prefix.as_slice(), &[present_tag(part)]].concat()),
        })
    };

    // A descending part's bytes run against its values, so its highest value
    // bounds the span's start.
    let (start, end) = match part.direction {
        IndexDirection::Asc => (key_bound(low)?, key_bound(high)?),
        IndexDirection::Desc => (key_bound(high)?, key_bound(low)?),
    };

    Ok(Span::for_key_range((start, end)))
}

/// Names a span of a tree's addresses: a [`Span`] itself, or a [`KeyRange`]
/// of the members' values, lowered under the tree's key spec. Every read
/// that takes a span takes either: [`select`](crate::DigestTree::select),
/// [`count`](crate::DigestTreeView::count),
/// [`span_digest`](crate::DigestTreeView::span_digest) and a
/// [range read](crate::DigestTreeView::range).
pub trait IntoSpan {
    /// The span, under `spec`. For a key range, values for more parts than
    /// the key has are [`Error::KeyLength`], and a value of another type than
    /// its part's is [`Error::KeyValueType`].
    fn into_span<K>(self, spec: &KeySpec<K>) -> Result<Span, Error>;
}

impl IntoSpan for Span {
    fn into_span<K>(self, _: &KeySpec<K>) -> Result<Span, Error> { Ok(self) }
}

impl IntoSpan for &Span {
    fn into_span<K>(self, _: &KeySpec<K>) -> Result<Span, Error> { Ok(self.clone()) }
}

/// A key range, and every Rust range of values that converts into one.
impl<R: Into<KeyRange>> IntoSpan for R {
    fn into_span<K>(self, spec: &KeySpec<K>) -> Result<Span, Error> { span_for(spec, &self.into()) }
}

/// Names the spans a view covers ([`select`](crate::DigestTree::select)): one
/// thing that names a span ([`IntoSpan`]), or an array or vector of them.
pub trait IntoSpans {
    /// The spans, under `spec`, with a key range's errors
    /// ([`into_span`](IntoSpan::into_span)).
    fn into_spans<K>(self, spec: &KeySpec<K>) -> Result<Vec<Span>, Error>;
}

impl<S: IntoSpan> IntoSpans for S {
    fn into_spans<K>(self, spec: &KeySpec<K>) -> Result<Vec<Span>, Error> { Ok(vec![self.into_span(spec)?]) }
}

impl<S: IntoSpan, const N: usize> IntoSpans for [S; N] {
    fn into_spans<K>(self, spec: &KeySpec<K>) -> Result<Vec<Span>, Error> { self.into_iter().map(|span| span.into_span(spec)).collect() }
}

impl<S: IntoSpan> IntoSpans for Vec<S> {
    fn into_spans<K>(self, spec: &KeySpec<K>) -> Result<Vec<Span>, Error> { self.into_iter().map(|span| span.into_span(spec)).collect() }
}
