//! Reading a tree.

use std::convert::Infallible;

use super::{Members, ScanRange};
use crate::span::address_of;
use crate::tree::rows::StagedRows;
use crate::tree::trie::{Node, Trie};
use crate::tree::DigestTree;
use crate::{Digest, DigestGroup, Error, Member, MemberId, Payload, Row, RowKey, Span, SpanDigest, Store};

/// One member as the tree holds it, as reads return it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leaf<I, L> {
    /// The member's key, as the member made it.
    pub key: Vec<u8>,

    /// The member's id.
    pub id: I,

    /// The member's payload.
    pub payload: L,
}

/// A leaf is a member as the tree holds it, whatever the key spec: what a
/// caller that read it from the tree names as the version a change replaces.
impl<K, I: MemberId, L: Payload> Member<K> for Leaf<I, L> {
    type Id = I;
    type Payload = L;
    type KeyError = Infallible;

    fn id(&self) -> I { self.id.clone() }

    fn key(&self, _: &K) -> Result<Vec<u8>, Infallible> { Ok(self.key.clone()) }

    fn payload(&self) -> L { self.payload.clone() }
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

    /// How many members the view covers.
    #[allow(clippy::len_without_is_empty, reason = "the count is a read of the store, which a caller compares with zero as cheaply")]
    pub async fn len(&self) -> Result<u64, Error> {
        let _read = self.tree.inner.lock.read().await;
        let rows = StagedRows::new(&self.tree.inner.store);
        let trie = Trie::new(&rows);
        let root = trie.root().await?;

        Ok(self.digest_in_view(&trie, &root, &Span::all()).await?.count())
    }

    /// The member with this key and id, if the view covers it.
    pub async fn member(&self, key: &[u8], id: &I) -> Result<Option<Leaf<I, L>>, Error> {
        let address = address_of(key, &id.to_bytes());
        if !self.spans.iter().any(|span| span.contains(&address)) {
            return Ok(None);
        }

        let _read = self.tree.inner.lock.read().await;
        let leaf = StagedRows::new(&self.tree.inner.store).leaf(&address).await?;

        Ok(leaf.map(|leaf| Leaf { key: key.to_vec(), id: id.clone(), payload: leaf.payload }))
    }

    /// Every member of the view within `range.span`, as a stream that reads
    /// them in `range.direction` a batch at a time, through the view's spans
    /// in that direction.
    ///
    /// Each batch takes the tree's read lock for its own read alone. The
    /// stream holds no lock between batches, and so none while its caller
    /// holds a member or once it is dropped; commits may land between
    /// batches. The first batch reads 64 members, as many as a bucket holds,
    /// so that a span refined down to one bucket is read in one hold of the
    /// lock; each batch after it reads twice as many as the one before, up to
    /// 1,024. Each also reads one member past those, which tells the stream
    /// whether any remain, so that it never takes the lock again for a batch
    /// that finds nothing. Nothing is read until the stream is first polled.
    ///
    /// Each batch sees one complete state of the tree, and later batches may
    /// see later states. The stream yields members strictly in the order of
    /// their addresses in the range's direction, so never one address twice.
    /// A member that keeps its address throughout is yielded exactly once;
    /// one inserted, removed or moved while the stream runs may be missed,
    /// and one that moved may be yielded at both addresses.
    ///
    /// A failed read yields its error, and then the stream ends.
    pub fn range(&self, range: ScanRange) -> Members<K, I, L, G, S> { Members::new(self.tree.clone(), &self.spans, range) }

    /// How many members within `span` the view covers.
    pub async fn count(&self, span: &Span) -> Result<u64, Error> {
        let _read = self.tree.inner.lock.read().await;
        let rows = StagedRows::new(&self.tree.inner.store);
        let trie = Trie::new(&rows);
        let root = trie.root().await?;

        Ok(self.digest_in_view(&trie, &root, span).await?.count())
    }

    /// The digest of the members within `span` the view covers, named with the
    /// smallest subtree that contains it.
    pub async fn span_digest(&self, span: &Span) -> Result<SpanDigest<G>, Error> {
        let _read = self.tree.inner.lock.read().await;
        let rows = StagedRows::new(&self.tree.inner.store);
        let trie = Trie::new(&rows);
        let named = trie.smallest_containing(span).await?;
        let digest = self.digest_in_view(&trie, &named, span).await?;

        Ok(SpanDigest { prefix: named.prefix, bounds: span.clone(), digest })
    }

    /// The span digests of the subtrees just beneath `digest`, within the
    /// view.
    ///
    /// The nearest subtrees beneath `digest.prefix` in this tree each give
    /// the digest of their part of its bounds, and the parts no subtree
    /// reaches, which hold none of this tree's members but may hold a peer's,
    /// give empty digests named with `digest.prefix`; parts outside the view
    /// are left out. Every part is named with a deeper prefix or covers less,
    /// so refining again makes progress. Empty where no subtree beneath
    /// `digest.prefix` reaches its bounds, as beneath a bucket: there the
    /// members within the bounds are compared directly, or a peer with more
    /// structure there refines them. A span digest whose span leaves its
    /// prefix's subtree is [`Error::MalformedSpanDigest`].
    pub async fn refine(&self, digest: &SpanDigest<G>) -> Result<Vec<SpanDigest<G>>, Error> {
        digest.validate()?;
        let _read = self.tree.inner.lock.read().await;

        let rows = StagedRows::new(&self.tree.inner.store);
        let trie = Trie::new(&rows);
        let beneath = trie.nearest_beneath(&digest.prefix).await?;
        let covered: Vec<Span> = beneath.iter().map(|node| node.prefix.span()).collect();

        let reached: Vec<_> =
            beneath.into_iter().zip(&covered).filter_map(|(node, subtree)| Some((node, subtree.intersect(&digest.bounds)?))).collect();

        if reached.is_empty() {
            return Ok(Vec::new());
        }

        let mut refined = Vec::new();
        for (node, bounds) in reached {
            if self.overlaps(&bounds) {
                let sum = self.digest_in_view(&trie, &node, &bounds).await?;
                refined.push(SpanDigest { prefix: node.prefix, bounds, digest: sum });
            }
        }

        for gap in digest.bounds.gaps(&covered) {
            if self.overlaps(&gap) {
                refined.push(SpanDigest { prefix: digest.prefix.clone(), bounds: gap, digest: Digest::empty() });
            }
        }

        refined.sort_by(|a, b| a.bounds.cmp_start(&b.bounds));
        Ok(refined)
    }

    /// The digest of the members within `span` the view covers, beneath the
    /// node `from`.
    async fn digest_in_view(&self, trie: &Trie<'_, '_, K, L, G, S>, from: &Node<G>, span: &Span) -> Result<Digest<G>, Error> {
        let mut digest = Digest::empty();
        for covered in &self.spans {
            if let Some(region) = span.intersect(covered) {
                digest.add(&trie.digest_within(from.clone(), &region).await?)?;
            }
        }

        Ok(digest)
    }

    /// Whether any address of `span` lies within the view.
    fn overlaps(&self, span: &Span) -> bool { self.spans.iter().any(|covered| covered.intersect(span).is_some()) }
}

#[cfg(test)]
mod tests;
