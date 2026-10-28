//! Reading a tree: views of the whole tree or of the part within some spans,
//! the digests they sum over spans, and the range reads that stream a view's
//! members.

mod members;
mod span_digests;
mod view;

pub use members::{Members, ScanRange};
pub use view::{DigestTreeView, Leaf};

use super::DigestTree;
use crate::key::span_for;
use crate::{DigestGroup, Error, KeyRange, MemberId, Payload, Row, RowKey, Span, Store};

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

    /// A view of the part of the tree within `spans`. The spans may come in
    /// any order, and may overlap or touch: the view covers every address
    /// any of them holds, and an empty span adds nothing. Making the view
    /// reads nothing.
    pub fn select(&self, spans: impl IntoIterator<Item = Span>) -> DigestTreeView<K, I, L, G, S> {
        DigestTreeView::new(self.clone(), Span::normalize(spans.into_iter().collect()))
    }

    /// The span of the addresses of the members `range` holds: a
    /// [`KeyRange`], or a Rust range of values for the key's first part. Values
    /// for more parts than the key has are [`Error::KeyLength`], and a value
    /// of another type than its part's is [`Error::KeyValueType`]. Making the
    /// span reads nothing.
    pub fn span_for(&self, range: impl Into<KeyRange>) -> Result<Span, Error> { span_for(&self.inner.key_spec, &range.into()) }
}
