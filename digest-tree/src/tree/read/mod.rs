//! Reading a tree: views of the whole tree or of the part within some spans,
//! the digests they sum over spans, the range reads that stream a view's
//! members, and the comparison of a view with another tree's.

mod comparison;
mod member_stream;
mod view;

pub use comparison::{ComparisonError, Difference, Page, PageGetter, PageMisfit};
pub use member_stream::{IntoScanRange, MemberStream, ScanRange};
pub use view::{DigestTreeView, Leaf};

use super::DigestTree;
use crate::key::span_for;
use crate::{DigestGroup, Error, IntoSpans, KeyRange, MemberId, Payload, Row, RowKey, Span, Store};

impl<K, I, L, G, S> DigestTree<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// A view of the whole tree.
    pub fn view(&self) -> DigestTreeView<K, I, L, G, S> { DigestTreeView::new(self.clone(), vec![Span::all()]) }

    /// A view of the part of the tree within `spans`: a span of addresses or
    /// a key range of values ([`IntoSpan`](crate::IntoSpan)), such as
    /// `2010..` for the members whose first key part holds 2010 or more, or an
    /// array or vector of them ([`IntoSpans`]). They may come in any order,
    /// and may overlap or touch: the view covers every address any of them
    /// holds, and an empty span adds nothing. A key range that doesn't fit the
    /// tree's key spec is [`Error::KeyLength`] or [`Error::KeyValueType`].
    /// Making the view reads nothing.
    pub fn select(&self, spans: impl IntoSpans) -> Result<DigestTreeView<K, I, L, G, S>, Error> {
        Ok(DigestTreeView::new(self.clone(), Span::normalize(spans.into_spans(&self.inner.key_spec)?)))
    }

    /// The span of the addresses of the members `range` holds, a
    /// [`KeyRange`] or a Rust range of values for the key's first part, for
    /// when a span is needed in hand, as to keep one; every read takes the
    /// range itself ([`IntoSpan`](crate::IntoSpan)). Values for
    /// more parts than the key has are [`Error::KeyLength`], and a value of
    /// another type than its part's is [`Error::KeyValueType`]. Making the
    /// span reads nothing.
    pub fn span_for(&self, range: impl Into<KeyRange>) -> Result<Span, Error> { span_for(&self.inner.key_spec, &range.into()) }
}
