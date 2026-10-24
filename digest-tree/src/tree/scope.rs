//! Scoped reads: a scope binds a request's spans to one transaction view,
//! and the reads over it count, sum, refine and page through its leaves.

use std::sync::Arc;

use ankurah_core_types::IndexDirection;

use super::TreeTransaction;
use crate::address::{NodePrefix, TreeSpan, TreeSpanDigest};
use crate::digest::{Digest, DigestGroup, Ristretto255};
use crate::error::TreeError;
use crate::selection::{order_cut, ReusePlan, TierWalk, TreeRequest};
use crate::store::layout;
use crate::store::{LeafRow, MapAccess, MapName};

/// What a read of a tree covers: exact, disjoint spans of leaf addresses and
/// the order to walk them in, bound to the transaction view that made them.
/// A mutation in that view invalidates the scope, and no other view can use
/// it.
#[derive(Debug, Clone)]
pub struct TreeScope {
    spans: Vec<TreeSpan>,
    directions: Vec<IndexDirection>,
    id_direction: IndexDirection,
    /// The transaction view the scope belongs to.
    view: Arc<()>,
    generation: u64,
}

impl TreeScope {
    /// The disjoint spans of leaf addresses left once membership and limit
    /// are applied. A constraint the serving tree's admission already
    /// guarantees may be absent from them, so reusing these bounds on another
    /// tree requires proving that both trees admit the same members within
    /// them.
    pub fn spans(&self) -> &[TreeSpan] { &self.spans }
}

/// An opaque continuation within a scope's order and transaction view.
#[derive(Debug, Clone)]
pub struct LeafCursor {
    remaining: Vec<TreeSpan>,
    scope: TreeScope,
}

/// One page of leaves and, when more remain, the continuation to use next.
#[derive(Debug)]
pub struct LeafPage<G: DigestGroup = Ristretto255> {
    /// Leaves in the scope's traversal order.
    pub leaves: Vec<LeafRow<G>>,
    /// Pass this to the next page read; `None` means exhausted.
    pub next: Option<LeafCursor>,
}

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// A scope of exact spans of leaf addresses, such as a subtree's
    /// [`TreeSpan::under`]. Unlike a Selection's scope, it counts leaves, an
    /// entity filed under several keys once for each, and walks them in
    /// address order.
    pub fn scope_spans(&self, spans: impl IntoIterator<Item = TreeSpan>) -> TreeScope {
        TreeScope {
            spans: TreeSpan::normalize(spans.into_iter().collect()),
            directions: vec![IndexDirection::Asc; self.spec.key_spec().keyparts.len()],
            id_direction: IndexDirection::Asc,
            view: self.view.clone(),
            generation: self.generation,
        }
    }

    /// The scope of `request`: the spans of this tree that serve it, with its
    /// limit resolved against this transaction view. A positive limit the
    /// leaves do not fill keeps the membership's spans whole, so that a peer
    /// can supply the missing members, even when this tree is empty. Under a
    /// limit, the walk takes each key part's tier in the request's direction,
    /// and breaks ties by ascending entity id unless the request orders it.
    /// [`TreeError::UnsupportedSelection`] when the tree does not serve the
    /// request, and [`TreeError::MultipleKeys`] when it files an entity under
    /// more than one key.
    pub async fn scope(&mut self, request: &TreeRequest) -> Result<TreeScope, TreeError> {
        let spec = self.spec.clone();
        let ReusePlan { mut spans, directions, id_direction, .. } = spec.plan(request).ok_or(TreeError::UnsupportedSelection)?;
        // Selection counts are entity counts. Multi-key raw trees need a
        // distinct-entity projection before they can serve this scalar API.
        if !spans.is_empty() && self.multiple_key_entities().await? != 0 {
            return Err(TreeError::MultipleKeys);
        }
        if let Some(limit) = request.limit {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            if limit == 0 {
                spans.clear();
            } else {
                let rows = self.walk(&spans, &directions, id_direction, limit).await?;
                if rows.len() == limit {
                    let last = rows.last().expect("a positive limit was filled");
                    let cut = order_cut(spec.key_spec(), &directions, id_direction, &last.address(), true)?;
                    spans = TreeSpan::intersect_all(&spans, &cut);
                }
            }
        }
        Ok(TreeScope { spans, directions, id_direction, view: self.view.clone(), generation: self.generation })
    }

    fn check_scope(&self, scope: &TreeScope) -> Result<(), TreeError> {
        if self.failed || !Arc::ptr_eq(&scope.view, &self.view) || scope.generation != self.generation {
            return Err(TreeError::StaleScope);
        }
        Ok(())
    }

    /// Span digests that tile the scope without overlap: a tree node the scope
    /// covers whole contributes its stored digest, and a bucket the scope cuts
    /// the sum of its leaves within the scope. Spans that hold no leaf here
    /// stay in the cover, since a peer may have leaves there.
    pub async fn span_digests(&mut self, scope: &TreeScope) -> Result<Vec<TreeSpanDigest<G>>, TreeError> {
        self.check_scope(scope)?;
        let mut span_digests = Vec::new();
        for span in &scope.spans {
            span_digests.extend(self.span_digests_in(&NodePrefix::root(), span).await?);
        }
        Ok(span_digests)
    }

    /// Exact leaf count; Selection scopes require one leaf per entity.
    pub async fn count(&mut self, scope: &TreeScope) -> Result<u64, TreeError> {
        let digest = self.digest(scope).await?;
        u64::try_from(digest.count()).map_err(|_| TreeError::Inconsistent("negative scoped count"))
    }

    /// Sum the exact span digests of a scope.
    pub async fn digest(&mut self, scope: &TreeScope) -> Result<Digest<G>, TreeError> {
        Digest::checked_sum(self.span_digests(scope).await?.iter().map(|span_digest| span_digest.digest)).map_err(Into::into)
    }

    /// Refine a span digest into the span digests of the nearest tree nodes
    /// beneath its prefix and of the gaps between them, within its bounds; a
    /// tree node the bounds cut gives the span digests that tile its part. At
    /// a bucket, which has no tree node beneath it, the span digest of its
    /// leaves within the bounds.
    pub async fn refine(&mut self, span_digest: &TreeSpanDigest<G>) -> Result<Vec<TreeSpanDigest<G>>, TreeError> {
        let bounds =
            TreeSpan::under(&span_digest.prefix).intersect(&span_digest.bounds).ok_or(TreeError::InvalidSpan("empty tree span"))?;
        let children = self.child_prefixes(&span_digest.prefix).await?;
        if children.is_empty() {
            return self.span_digests_in(&span_digest.prefix, &bounds).await;
        }
        let mut span_digests = empty_gaps(&span_digest.prefix, &bounds, &children);
        for prefix in children {
            span_digests.extend(self.span_digests_in(&prefix, &bounds).await?);
        }
        span_digests.sort_by(|a, b| a.bounds.cmp_start(&b.bounds));
        Ok(span_digests)
    }

    async fn span_digests_in(&mut self, prefix: &NodePrefix, span: &TreeSpan) -> Result<Vec<TreeSpanDigest<G>>, TreeError> {
        let mut pending = vec![prefix.clone()];
        let mut span_digests = Vec::new();
        while let Some(prefix) = pending.pop() {
            let entire = TreeSpan::under(&prefix);
            let Some(bounds) = entire.intersect(span) else {
                continue;
            };
            if entire.is_subset_of(span) {
                if let Some(digest) = self.node(&prefix).await? {
                    if digest.count() < 0 {
                        return Err(TreeError::Inconsistent("negative node count"));
                    }
                    span_digests.push(TreeSpanDigest { prefix, bounds, digest });
                    continue;
                }
            }
            let children = self.child_prefixes(&prefix).await?;
            if children.is_empty() {
                let leaves = self.all_leaves(&bounds).await?;
                let digest = leaves.iter().map(|leaf| Digest::from(leaf.row().element)).sum();
                span_digests.push(TreeSpanDigest { prefix, bounds, digest });
            } else {
                span_digests.extend(empty_gaps(&prefix, &bounds, &children));
                pending.extend(children.into_iter().rev());
            }
        }
        span_digests.sort_by(|a, b| a.bounds.cmp_start(&b.bounds));
        Ok(span_digests)
    }

    /// The next page of at most `limit` leaves in the scope's order: from the
    /// start, or from `cursor`, which an earlier page returned. A cursor
    /// belongs to its scope and transaction view, and a mutation invalidates
    /// both.
    pub async fn page(&mut self, scope: &TreeScope, cursor: Option<&LeafCursor>, limit: usize) -> Result<LeafPage<G>, TreeError> {
        self.check_scope(scope)?;
        if limit == 0 {
            return Err(TreeError::InvalidSpan("page size must be positive"));
        }
        let spans = if let Some(cursor) = cursor {
            self.check_scope(&cursor.scope)?;
            if cursor.scope.spans != scope.spans
                || cursor.scope.directions != scope.directions
                || cursor.scope.id_direction != scope.id_direction
            {
                return Err(TreeError::StaleScope);
            }
            &cursor.remaining
        } else {
            &scope.spans
        };
        let mut rows = self.walk(spans, &scope.directions, scope.id_direction, limit.saturating_add(1)).await?;
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next = if more {
            let spec = &self.spec;
            let cut = order_cut(spec.key_spec(), &scope.directions, scope.id_direction, &rows.last().unwrap().address(), false)?;
            Some(LeafCursor { remaining: TreeSpan::intersect_all(spans, &cut), scope: scope.clone() })
        } else {
            None
        };
        Ok(LeafPage { leaves: rows, next })
    }

    async fn walk(
        &mut self,
        spans: &[TreeSpan],
        directions: &[IndexDirection],
        id_direction: IndexDirection,
        limit: usize,
    ) -> Result<Vec<LeafRow<G>>, TreeError> {
        let spec = self.spec.clone();
        let mut walk = TierWalk::new(spec.key_spec(), directions, id_direction, limit, spans);
        let mut leaves = Vec::new();
        while let Some(scan) = walk.next_scan() {
            let rows = match scan.direction {
                IndexDirection::Asc => self.transaction.scan(MapName::Leaves, scan.bounds.bounds(), scan.limit).await?,
                IndexDirection::Desc => self.transaction.scan_reverse(MapName::Leaves, scan.bounds.bounds(), scan.limit).await?,
            };
            walk.advance(rows.first().map(|(address, _)| address.as_slice()), rows.len())?;
            for (key, value) in rows {
                let row = layout::leaf_row(&key, &value)?;
                if scan.leaves {
                    leaves.push(row);
                }
            }
        }
        Ok(leaves)
    }
}

/// The parts of `bounds` that no child of the tree node at `prefix` covers,
/// each as a span digest of no leaves: the children hold every stored leaf,
/// but path compression can leave addresses of their parent outside them.
fn empty_gaps<G: DigestGroup>(prefix: &NodePrefix, bounds: &TreeSpan, children: &[NodePrefix]) -> Vec<TreeSpanDigest<G>> {
    let covered: Vec<_> = children.iter().map(TreeSpan::under).collect();
    bounds.gaps(&covered).into_iter().map(|bounds| TreeSpanDigest { prefix: prefix.clone(), bounds, digest: Digest::empty() }).collect()
}
