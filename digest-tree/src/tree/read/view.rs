//! Reading a tree.

use serde::{Deserialize, Serialize};

use super::{IntoScanRange, MemberStream, Page, PageGetter, ScanRange};
use crate::span::split_address;
use crate::store::LeafRow;
use crate::tree::rows::TreeRows;
use crate::tree::trie::{Node, Trie};
use crate::tree::DigestTree;
use crate::{Digest, DigestGroup, Error, IntoSpan, MemberId, Payload, Row, RowKey, ScanDirection, Span, SpanDigest, Store};

/// One member as the tree holds it, as reads return it: what a change names,
/// through [`update_leaf`](crate::Transaction::update_leaf) and
/// [`remove_leaf`](crate::Transaction::remove_leaf), as the version it
/// replaces, and what a page of members ([`Page`]) carries
/// across the wire, so a leaf serializes when its id and payload do.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Leaf<I, L> {
    /// The member's key, as the tree made it from the member's values.
    pub key: Vec<u8>,

    /// The member's id.
    pub id: I,

    /// The member's payload.
    pub payload: L,
}

impl<I: MemberId, L> Leaf<I, L> {
    /// The member at `address`, as its row holds it. The tree stores only
    /// addresses it formed from a key and an id, so one that doesn't split
    /// into them, or holds no id, means its rows are damaged.
    pub(super) fn from_row<G: DigestGroup>(address: &[u8], row: LeafRow<L, G>) -> Result<Self, Error> {
        let (key, id) = split_address(address)?;
        let id = I::from_bytes(&id).ok_or(Error::Inconsistent("a stored address holds no member id"))?;

        Ok(Self { key, id, payload: row.payload })
    }
}

/// A read-only, live view of a tree: the whole tree, or the part within some
/// spans ([`select`](DigestTree::select)). Each read sees a complete state of
/// the tree, and so does each batch of a [range](Self::range) read; two reads
/// may see different ones. The view's spans are fixed when it is made.
///
/// Every read is limited to the view's spans: a member outside them is
/// neither counted, summed nor returned.
pub struct DigestTreeView<K, I, L, G: DigestGroup, S> {
    tree: DigestTree<K, I, L, G, S>,

    /// Disjoint and in address order.
    spans: Vec<Span>,
}

impl<K, I, L, G, S> DigestTreeView<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// A view of `tree` over `spans`, which are disjoint and in address order.
    pub(super) fn new(tree: DigestTree<K, I, L, G, S>, spans: Vec<Span>) -> Self { Self { tree, spans } }

    /// Every member of the view within `range`, as a stream that reads them
    /// in its direction a batch at a time, through the view's spans in that
    /// direction. `range` is a [`ScanRange`], or anything that names a span
    /// ([`IntoSpan`]), which reads forward: `view.range(..)` reads every
    /// member, `view.range(2013..)` those whose first key part holds 2013 or
    /// more, and a [`Span`] the addresses it holds. A key range that doesn't
    /// fit the tree's key spec is [`Error::KeyLength`] or
    /// [`Error::KeyValueType`], before anything is read.
    ///
    /// The stream reads nothing until it is first polled, and reads each
    /// batch only once its caller has taken every member of the one before,
    /// so a stream stopped early reads no further. Each batch begins a read of
    /// the tree's store of its own, which ends once the batch is read: commits
    /// may land between batches, and the stream holds no read while its
    /// caller holds a member or once it is dropped. The first batch yields as
    /// many members as a bucket holds, so that a range within one bucket is
    /// read in one read, and a batch that reads to the end of the range ends
    /// the stream without another read.
    ///
    /// A batch under way keeps its read open until it is read, even while its
    /// caller stops polling, as a caller does that gives up on `next()` after
    /// a timeout. A store's read may hold off a commit, as `MemoryStore`'s
    /// holds off the batch a commit applies, so such a caller polls the stream
    /// on, or drops it, before it awaits a commit to the same store.
    ///
    /// Each batch sees one committed state of the tree, and later batches may
    /// see later states. The stream yields members strictly in the order of
    /// their addresses in the range's direction, so never one address twice.
    /// A member that keeps its address throughout is yielded exactly once;
    /// one inserted, removed or moved while the stream runs may be missed,
    /// and one that moved may be yielded at both addresses.
    ///
    /// A failed read yields its error, and then the stream ends.
    pub fn range(&self, range: impl IntoScanRange) -> Result<MemberStream<K, I, L, G, S>, Error> {
        let ScanRange { range, direction } = range.into_scan_range();
        let span = range.into_span(&self.tree.inner.key_spec)?;

        Ok(MemberStream::new(self.tree.clone(), self.within(&span).collect(), direction))
    }

    /// How many members the view covers within `span`: a span, or a key range
    /// ([`IntoSpan`]); `..` counts them all.
    pub async fn count(&self, span: impl IntoSpan) -> Result<u64, Error> {
        let span = span.into_span(&self.tree.inner.key_spec)?;
        let mut rows = self.tree.read().await?;

        Ok(self.digest_in_view(&mut Trie::new(&mut rows), &span).await?.count())
    }

    /// The digest of the members the view covers within `span`, a span or a
    /// key range ([`IntoSpan`]).
    pub async fn span_digest(&self, span: impl IntoSpan) -> Result<SpanDigest<G>, Error> {
        let span = span.into_span(&self.tree.inner.key_spec)?;
        let mut rows = self.tree.read().await?;
        let digest = self.digest_in_view(&mut Trie::new(&mut rows), &span).await?;

        Ok(SpanDigest { bounds: span, digest })
    }

    /// The tree's page at each of `spans`, as a node reads it to answer
    /// another node's comparison ([`PageGetter::get_pages`]): where the tree
    /// has structure beneath a span, the span's subdivision one level down,
    /// and where it has none, the members within it. One read: every page
    /// comes from the same state of the tree, and the spans share the walks
    /// that answer them, so a node is read twice at most, however many of the
    /// spans lie beneath it.
    pub async fn pages(&self, spans: Vec<Span>) -> Result<Vec<Page<I, L, G>>, Error> {
        let mut rows = self.tree.read().await?;
        let mut trie = Trie::new(&mut rows);

        let children = trie.children_of_smallest_containing(&spans).await?;
        let subdivisions = self.subdivisions(&mut trie, &spans, children).await?;

        let mut pages = Vec::with_capacity(spans.len());
        for (span, subspans) in spans.iter().zip(subdivisions) {
            pages.push(match subspans.is_empty() {
                false => Page::Digests(subspans),
                true => Page::Members(self.members_within(&mut rows, span).await?),
            });
        }

        Ok(pages)
    }

    /// The view's own digest of each of `spans`, in one read: every span is
    /// digested in the same state of the tree, in one walk of it.
    pub(super) async fn span_digests(&self, spans: Vec<Span>) -> Result<Vec<SpanDigest<G>>, Error> {
        if spans.is_empty() {
            return Ok(Vec::new());
        }

        let mut rows = self.tree.read().await?;
        let digests = self.digests_in_view(&mut Trie::new(&mut rows), &spans).await?;

        Ok(spans.into_iter().zip(digests).map(|(bounds, digest)| SpanDigest { bounds, digest }).collect())
    }

    /// Every member the view covers within each of `spans`, in one read.
    pub(super) async fn members_within_each(&self, spans: &[Span]) -> Result<Vec<Vec<Leaf<I, L>>>, Error> {
        if spans.is_empty() {
            return Ok(Vec::new());
        }

        let mut rows = self.tree.read().await?;

        let mut members = Vec::with_capacity(spans.len());
        for span in spans {
            members.push(self.members_within(&mut rows, span).await?);
        }

        Ok(members)
    }

    /// The spans the view covers, disjoint and in address order.
    pub(super) fn spans(&self) -> &[Span] { &self.spans }

    /// The span digests one level beneath the smallest subtree that contains
    /// each of `spans`, whose `children` they are, all digested in one walk:
    /// each child digests, within the view, the part of its span it holds,
    /// and the gaps neither child reaches, which hold none of this tree's
    /// members but may hold another tree's, give empty digests. A part
    /// outside the view digests nothing, as every read within the view does.
    /// Every subspan is narrower than its span, so the page beneath it makes
    /// progress. Empty for a span whose smallest subtree is a bucket, or
    /// whose children neither reach it: there the page is the members within
    /// the span.
    async fn subdivisions(
        &self,
        trie: &mut Trie<'_, K, L, G, S::Read>,
        spans: &[Span],
        children: Vec<Option<[Node<G>; 2]>>,
    ) -> Result<Vec<Vec<SpanDigest<G>>>, Error> {
        let mut subdivisions: Vec<Vec<SpanDigest<G>>> = spans.iter().map(|_| Vec::new()).collect();

        // Every part of a span its subtree's children hold, all digested
        // together, and which span each is a part of.
        let (mut parts, mut part_of) = (Vec::new(), Vec::new());
        for (index, (span, children)) in spans.iter().zip(children).enumerate() {
            let Some(children) = children else { continue };
            let covered = children.map(|child| child.prefix.span());
            let reached: Vec<Span> = covered.iter().filter_map(|subtree| subtree.intersect(span)).collect();

            if reached.is_empty() {
                continue;
            }

            subdivisions[index].extend(span.gaps(&covered).into_iter().map(|gap| SpanDigest { bounds: gap, digest: Digest::empty() }));
            part_of.extend(std::iter::repeat_n(index, reached.len()));
            parts.extend(reached);
        }

        let digests = self.digests_in_view(trie, &parts).await?;
        for ((bounds, index), digest) in parts.into_iter().zip(part_of).zip(digests) {
            subdivisions[index].push(SpanDigest { bounds, digest });
        }

        for subspans in &mut subdivisions {
            subspans.sort_by(|a, b| a.bounds.cmp_start(&b.bounds));
        }

        Ok(subdivisions)
    }

    /// Every member the view covers within `span`, read through `rows`.
    async fn members_within(&self, rows: &mut TreeRows<K, L, G, S::Read>, span: &Span) -> Result<Vec<Leaf<I, L>>, Error> {
        let within: Vec<Span> = self.within(span).collect();
        let leaves = rows.first_leaves(&within, ScanDirection::Forward, usize::MAX).await?;

        leaves.into_iter().map(|(address, row)| Leaf::from_row(&address, row)).collect()
    }

    /// The digest of the members the view covers within `span`.
    async fn digest_in_view(&self, trie: &mut Trie<'_, K, L, G, S::Read>, span: &Span) -> Result<Digest<G>, Error> {
        let regions: Vec<Span> = self.within(span).collect();

        let mut digest = Digest::empty();
        for part in trie.digests_within(&regions).await? {
            digest.add(&part)?;
        }

        Ok(digest)
    }

    /// The digest of the members the view covers within each of `spans`, in
    /// one walk of the trie, which the spans share.
    async fn digests_in_view(&self, trie: &mut Trie<'_, K, L, G, S::Read>, spans: &[Span]) -> Result<Vec<Digest<G>>, Error> {
        // Each span's parts within the view's spans, and which span each is
        // a part of.
        let (regions, region_of): (Vec<Span>, Vec<usize>) =
            spans.iter().enumerate().flat_map(|(index, span)| self.within(span).map(move |region| (region, index))).unzip();

        let mut digests = vec![Digest::empty(); spans.len()];
        for (index, part) in region_of.into_iter().zip(trie.digests_within(&regions).await?) {
            digests[index].add(&part)?;
        }

        Ok(digests)
    }

    /// The view's spans cut to `span`, in address order: the addresses a read
    /// within `span` covers. Every read is limited to the view's spans here.
    fn within<'a>(&'a self, span: &'a Span) -> impl Iterator<Item = Span> + 'a {
        self.spans.iter().filter_map(move |covered| span.intersect(covered))
    }
}

/// A view gets its own tree's pages ([`pages`](DigestTreeView::pages)), so
/// that two trees in one process compare directly, as tests do; across the
/// wire, a connection to the node that holds the tree gets them.
impl<K, I, L, G, S> PageGetter<I, L, G> for DigestTreeView<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    type Error = Error;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<I, L, G>>, Error> { self.pages(spans).await }
}

#[cfg(test)]
mod tests;
