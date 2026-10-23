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

/// Exact disjoint address spans belonging to one transaction view and its
/// current leaf state. A mutation invalidates the scope; another transaction cannot use it.
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
    /// The disjoint spans of leaf addresses left after membership and limit
    /// have been applied.
    /// Constraints already guaranteed by the serving tree's admission may
    /// be absent here. Reusing these bounds on another tree requires proving
    /// that both trees describe the same admitted selection in these bounds.
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
    /// Read exact spans of leaf addresses directly, including a subtree via
    /// `TreeSpan::under`. Unlike Selection scopes these count leaves,
    /// including multiple keys for one entity, and walk in address order.
    pub fn scope_spans(&self, spans: impl IntoIterator<Item = TreeSpan>) -> TreeScope {
        TreeScope {
            spans: TreeSpan::normalize(spans.into_iter().collect()),
            directions: vec![IndexDirection::Asc; self.spec.key_spec().keyparts.len()],
            id_direction: IndexDirection::Asc,
            view: self.view.clone(),
            generation: self.generation,
        }
    }

    /// Match a request and resolve its limit against this transaction view. An
    /// underfilled positive limit retains the membership bounds so a peer
    /// can supply the missing members, including when this tree is empty.
    /// Limited traversal walks each tuple tier in its requested direction; ties use
    /// ascending entity identity unless explicitly ordered otherwise.
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

    /// Span digests that tile the scope without overlap. Fully covered cached
    /// nodes contribute their stored digest; boundary buckets are clipped by
    /// reading only their leaves inside the requested address bounds. Empty
    /// regions remain in the cover: a peer may have leaves there.
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
        u64::try_from(digest.count()).map_err(|_| TreeError::Audit("negative scoped count"))
    }

    /// Sum the exact span digests of a scope.
    pub async fn digest(&mut self, scope: &TreeScope) -> Result<Digest<G>, TreeError> {
        Digest::checked_sum(self.span_digests(scope).await?.iter().map(|span_digest| span_digest.digest)).map_err(Into::into)
    }

    /// Refine a span digest into the nearest cached descendants, keeping its address
    /// clip. At a terminal bucket, returns its exact clipped span digest again.
    pub async fn refine(&mut self, span_digest: &TreeSpanDigest<G>) -> Result<Vec<TreeSpanDigest<G>>, TreeError> {
        let bounds = TreeSpan::under(&span_digest.prefix).intersect(&span_digest.bounds).ok_or(TreeError::InvalidKey("empty tree span"))?;
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
                if let Some(node) = self.node(&prefix).await? {
                    if node.digest.count() < 0 {
                        return Err(TreeError::Audit("negative node count"));
                    }
                    span_digests.push(TreeSpanDigest { prefix, bounds, digest: node.digest });
                    continue;
                }
            }
            let children = self.child_prefixes(&prefix).await?;
            if children.is_empty() {
                let rows = self.all_leaves(&bounds).await?;
                span_digests.push(TreeSpanDigest { prefix, bounds, digest: rows.iter().map(|row| Digest::from(row.element)).sum() });
            } else {
                span_digests.extend(empty_gaps(&prefix, &bounds, &children));
                pending.extend(children.into_iter().rev());
            }
        }
        span_digests.sort_by(|a, b| a.bounds.cmp_start(&b.bounds));
        Ok(span_digests)
    }

    /// Read a page without restarting at earlier leaves. A cursor belongs to
    /// this scope and transaction; mutations invalidate it along with the scope.
    pub async fn page(&mut self, scope: &TreeScope, cursor: Option<&LeafCursor>, limit: usize) -> Result<LeafPage<G>, TreeError> {
        self.check_scope(scope)?;
        if limit == 0 {
            return Err(TreeError::InvalidKey("page size must be positive"));
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
        let spec = self.spec.key_spec().clone();
        let mut walk = TierWalk::new(&spec, directions, id_direction, limit, spans);
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

/// Children cover the locally stored leaves, but compressed prefixes need
/// not cover their parent's addresses. Represent the rest explicitly.
fn empty_gaps<G: DigestGroup>(prefix: &NodePrefix, bounds: &TreeSpan, children: &[NodePrefix]) -> Vec<TreeSpanDigest<G>> {
    let covered: Vec<_> = children.iter().map(TreeSpan::under).collect();
    bounds.gaps(&covered).into_iter().map(|bounds| TreeSpanDigest { prefix: prefix.clone(), bounds, digest: Digest::empty() }).collect()
}
