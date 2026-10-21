//! Snapshot-bound query scopes, tier-ordered traversal and span digests.

use crate::bounds::{gaps, intersect, lower_cmp, normalize, subset};
use crate::key::prefix_end;
use crate::layout;
use crate::map::{MapAccess, MapName};
use crate::request::{prefix_range, ReusePlan};
use crate::{CanonicalCodec, Digest, DigestGroup, LeafRow, NodePrefix, Ristretto255, TreeError, TreeRequest, TreeSpan, TreeTransaction};
use ankurah_core_types::{IndexDirection, KeySpec, PropertyId};
use serde::{Deserialize, Serialize};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

/// Exact disjoint address spans belonging to one transaction and its current
/// leaf state. A mutation invalidates the scope; another transaction cannot use it.
#[derive(Debug, Clone)]
pub struct TreeScope {
    ranges: Vec<TreeSpan>,
    directions: Vec<IndexDirection>,
    id_direction: IndexDirection,
    snapshot: Arc<()>,
    generation: u64,
}
impl TreeScope {
    /// Disjoint address bounds after membership and limit have been applied.
    /// Constraints already guaranteed by the serving tree's admission may
    /// be absent here. Reusing these bounds on another tree requires proving
    /// that both trees describe the same admitted selection in these bounds.
    pub fn ranges(&self) -> &[TreeSpan] { &self.ranges }
}

/// A tree span and the digest of its leaves, cached or computed for clipped bounds.
/// Bounds are complete addresses, so another tree can compute the same span independently.
/// A peer must also establish compatible keys, digest groups and admitted
/// membership; a span digest alone does not carry those proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeSpanDigest<G: DigestGroup = Ristretto255> {
    /// The containing subtree from which refinement continues. Bounds alone
    /// do not identify this ancestor. A clipped or empty span need not have
    /// a stored node at this prefix on the answering peer.
    pub prefix: NodePrefix,
    /// The exact span within that subtree, independent of the prefix encoding.
    pub bounds: TreeSpan,
    /// Digest of precisely the leaves within these bounds.
    pub digest: Digest<G>,
}

impl<G: DigestGroup> Serialize for TreeSpanDigest<G> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { self.to_bytes().serialize(serializer) }
}

impl<'de, G: DigestGroup> Deserialize<'de> for TreeSpanDigest<G> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}
impl<G: DigestGroup> TreeSpanDigest<G> {
    /// Encode a versioned span digest for a peer, independent of a local snapshot.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&(1u8, self.prefix.to_bytes(), &self.bounds.start, &self.bounds.end, self.digest.to_row_bytes())).unwrap()
    }
    /// Decode a span digest and validate its prefix, bounds, count and group encoding.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TreeError> {
        use bincode::Options;
        type EncodedSpanDigest = (u8, Vec<u8>, std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>, Vec<u8>);
        let (version, prefix, start, end, digest): EncodedSpanDigest = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes()
            .deserialize(bytes)
            .map_err(|e| TreeError::unreadable("tree span digest", e))?;
        if version != 1 {
            return Err(TreeError::InvalidKey("unknown tree span digest version"));
        }
        let prefix = NodePrefix::from_bytes(&prefix).map_err(|e| TreeError::unreadable("span digest prefix", e))?;
        let bounds = TreeSpan::new(start, end);
        let digest = Digest::from_row_bytes(&digest).map_err(|e| TreeError::unreadable("span digest", e))?;
        let head = Self { prefix, bounds, digest };
        head.validate()?;
        Ok(head)
    }

    /// Validate a typed span digest without encoding and decoding it again.
    /// Prefix and group encodings are already checked by their constructors.
    pub fn validate(&self) -> Result<(), TreeError> {
        if self.bounds.is_empty() || !self.bounds.is_subset_of(&TreeSpan::under(&self.prefix)) || self.digest.count() < 0 {
            return Err(TreeError::InvalidKey("invalid tree span digest bounds or count"));
        }
        Ok(())
    }
}

/// An opaque continuation within a scope's order and snapshot.
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
    /// Read exact leaf-address ranges directly, including a subtree via
    /// `TreeSpan::under`. Unlike Selection scopes these count leaves,
    /// including multiple keys for one entity, and walk in address order.
    pub fn scope_ranges(&self, ranges: impl IntoIterator<Item = TreeSpan>) -> TreeScope {
        TreeScope {
            ranges: normalize(ranges.into_iter().collect()),
            directions: vec![IndexDirection::Asc; self.spec.key_spec().keyparts.len()],
            id_direction: IndexDirection::Asc,
            snapshot: self.snapshot.clone(),
            generation: self.generation,
        }
    }

    /// Match a request and resolve its limit against this snapshot. An
    /// underfilled positive limit retains the membership bounds so a peer
    /// can supply the missing members, including when this tree is empty.
    /// Limited traversal walks each tuple tier in its requested direction; ties use
    /// ascending entity identity unless explicitly ordered otherwise.
    pub async fn scope(&mut self, request: &TreeRequest) -> Result<TreeScope, TreeError> {
        let spec = self.spec.clone();
        let ReusePlan { mut ranges, directions, id_direction, .. } = spec.plan(request).ok_or(TreeError::UnsupportedSelection)?;
        // Selection counts are entity counts. Multi-key raw trees need a
        // distinct-entity projection before they can serve this scalar API.
        if !ranges.is_empty() && self.multiple_key_entities().await? != 0 {
            return Err(TreeError::MultipleKeys);
        }
        if let Some(limit) = request.limit {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            if limit == 0 {
                ranges.clear();
            } else {
                let rows = self.walk(&ranges, &directions, id_direction, limit).await?;
                if rows.len() == limit {
                    let last = rows.last().expect("a positive limit was filled");
                    let cut = order_cut(spec.key_spec(), &directions, id_direction, last, true)?;
                    ranges = intersections(&ranges, &cut);
                }
            }
        }
        Ok(TreeScope { ranges, directions, id_direction, snapshot: self.snapshot.clone(), generation: self.generation })
    }

    fn check_scope(&self, scope: &TreeScope) -> Result<(), TreeError> {
        if self.failed || !Arc::ptr_eq(&scope.snapshot, &self.snapshot) || scope.generation != self.generation {
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
        let mut heads = Vec::new();
        for range in &scope.ranges {
            heads.extend(self.span_digests_in(&NodePrefix::root(), range).await?);
        }
        Ok(heads)
    }

    /// Exact leaf count; Selection scopes require one leaf per entity.
    pub async fn count(&mut self, scope: &TreeScope) -> Result<u64, TreeError> {
        let digest = self.digest(scope).await?;
        u64::try_from(digest.count()).map_err(|_| TreeError::Audit("negative scoped count"))
    }

    /// Sum the exact span digests of a scope.
    pub async fn digest(&mut self, scope: &TreeScope) -> Result<Digest<G>, TreeError> {
        Digest::checked_sum(self.span_digests(scope).await?.iter().map(|head| head.digest)).map_err(Into::into)
    }

    /// Refine a span digest into the nearest cached descendants, keeping its address
    /// clip. At a terminal bucket, returns its exact clipped span digest again.
    pub async fn refine(&mut self, head: &TreeSpanDigest<G>) -> Result<Vec<TreeSpanDigest<G>>, TreeError> {
        let bounds = intersect(&TreeSpan::under(&head.prefix), &head.bounds).ok_or(TreeError::InvalidKey("empty tree span"))?;
        let children = self.child_prefixes(&head.prefix).await?;
        if children.is_empty() {
            return self.span_digests_in(&head.prefix, &bounds).await;
        }
        let mut heads = empty_gaps(&head.prefix, &bounds, &children);
        for prefix in children {
            heads.extend(self.span_digests_in(&prefix, &bounds).await?);
        }
        heads.sort_by(|a, b| lower_cmp(&a.bounds.start, &b.bounds.start));
        Ok(heads)
    }

    async fn span_digests_in(&mut self, prefix: &NodePrefix, range: &TreeSpan) -> Result<Vec<TreeSpanDigest<G>>, TreeError> {
        let mut pending = vec![prefix.clone()];
        let mut heads = Vec::new();
        while let Some(prefix) = pending.pop() {
            let entire = TreeSpan::under(&prefix);
            let Some(bounds) = intersect(&entire, range) else {
                continue;
            };
            if subset(&entire, range) {
                if let Some(node) = self.node(&prefix).await? {
                    if node.digest.count() < 0 {
                        return Err(TreeError::Audit("negative node count"));
                    }
                    heads.push(TreeSpanDigest { prefix, bounds, digest: node.digest });
                    continue;
                }
            }
            let children = self.child_prefixes(&prefix).await?;
            if children.is_empty() {
                let rows = self.all_leaves(&bounds).await?;
                heads.push(TreeSpanDigest { prefix, bounds, digest: rows.iter().map(|row| Digest::from(row.element)).sum() });
            } else {
                heads.extend(empty_gaps(&prefix, &bounds, &children));
                pending.extend(children.into_iter().rev());
            }
        }
        heads.sort_by(|a, b| lower_cmp(&a.bounds.start, &b.bounds.start));
        Ok(heads)
    }

    /// Read a page without restarting at earlier leaves. A cursor belongs to
    /// this scope and transaction; mutations invalidate it along with the scope.
    pub async fn page(&mut self, scope: &TreeScope, cursor: Option<&LeafCursor>, limit: usize) -> Result<LeafPage<G>, TreeError> {
        self.check_scope(scope)?;
        if limit == 0 {
            return Err(TreeError::InvalidKey("page size must be positive"));
        }
        let ranges = if let Some(cursor) = cursor {
            self.check_scope(&cursor.scope)?;
            if cursor.scope.ranges != scope.ranges
                || cursor.scope.directions != scope.directions
                || cursor.scope.id_direction != scope.id_direction
            {
                return Err(TreeError::StaleScope);
            }
            &cursor.remaining
        } else {
            &scope.ranges
        };
        let mut rows = self.walk(ranges, &scope.directions, scope.id_direction, limit.saturating_add(1)).await?;
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next = if more {
            let spec = &self.spec;
            let cut = order_cut(spec.key_spec(), &scope.directions, scope.id_direction, rows.last().unwrap(), false)?;
            Some(LeafCursor { remaining: intersections(ranges, &cut), scope: scope.clone() })
        } else {
            None
        };
        Ok(LeafPage { leaves: rows, next })
    }

    async fn walk(
        &mut self,
        ranges: &[TreeSpan],
        directions: &[IndexDirection],
        id_direction: IndexDirection,
        limit: usize,
    ) -> Result<Vec<LeafRow<G>>, TreeError> {
        let spec = self.spec.key_spec().clone();
        let mut walk = LeafWalk { spec, directions, id_direction, limit, leaves: Vec::new() };
        walk.read(self, ranges.to_vec(), 0).await?;
        Ok(walk.leaves)
    }
}

/// Children cover the locally stored leaves, but compressed prefixes need
/// not cover their parent's addresses. Represent the rest explicitly.
fn empty_gaps<G: DigestGroup>(prefix: &NodePrefix, bounds: &TreeSpan, children: &[NodePrefix]) -> Vec<TreeSpanDigest<G>> {
    let covered: Vec<_> = children.iter().map(TreeSpan::under).collect();
    gaps(bounds, &covered).into_iter().map(|bounds| TreeSpanDigest { prefix: prefix.clone(), bounds, digest: Digest::empty() }).collect()
}

/// A walk's fixed order and accumulated page, shared by all tuple tiers.
struct LeafWalk<'a, G: DigestGroup> {
    spec: KeySpec<PropertyId>,
    directions: &'a [IndexDirection],
    id_direction: IndexDirection,
    limit: usize,
    leaves: Vec<LeafRow<G>>,
}

impl<G: DigestGroup> LeafWalk<'_, G> {
    async fn read<T: MapAccess>(
        &mut self,
        transaction: &mut TreeTransaction<G, T>,
        mut ranges: Vec<TreeSpan>,
        depth: usize,
    ) -> Result<(), TreeError> {
        let direction = self.directions.get(depth).copied().unwrap_or(self.id_direction);
        while !ranges.is_empty() && self.leaves.len() < self.limit {
            let indices: Vec<_> = match direction {
                IndexDirection::Asc => (0..ranges.len()).collect(),
                IndexDirection::Desc => (0..ranges.len()).rev().collect(),
            };
            let mut seed = None;
            for index in indices {
                let take = if depth == self.spec.keyparts.len() { self.limit - self.leaves.len() } else { 1 };
                let rows = match direction {
                    IndexDirection::Asc => transaction.transaction.scan(MapName::Leaves, ranges[index].bounds(), take).await?,
                    IndexDirection::Desc => transaction.transaction.scan_reverse(MapName::Leaves, ranges[index].bounds(), take).await?,
                };
                if depth == self.spec.keyparts.len() {
                    for (key, value) in rows {
                        self.leaves.push(layout::leaf_row(&key, &value)?);
                    }
                    if self.leaves.len() == self.limit {
                        return Ok(());
                    }
                } else if let Some((key, value)) = rows.into_iter().next() {
                    seed = Some(layout::leaf_row::<G>(&key, &value)?);
                    break;
                }
            }
            if depth == self.spec.keyparts.len() {
                return Ok(());
            }
            let Some(seed) = seed else { return Ok(()) };
            let parts = CanonicalCodec.split(&self.spec, &seed.key)?;
            let prefix = parts[..=depth].concat();
            let inside = intersections(&ranges, &[prefix_range(&prefix)]);
            Box::pin(self.read(transaction, inside, depth + 1)).await?;
            let rest = match direction {
                IndexDirection::Asc => match prefix_end(&prefix) {
                    Some(end) => TreeSpan::new(Included(end), Unbounded),
                    None => break,
                },
                IndexDirection::Desc => TreeSpan::new(Unbounded, Excluded(prefix)),
            };
            ranges = intersections(&ranges, &[rest]);
        }
        Ok(())
    }
}

fn intersections(a: &[TreeSpan], b: &[TreeSpan]) -> Vec<TreeSpan> {
    normalize(a.iter().flat_map(|a| b.iter().filter_map(move |b| intersect(a, b))).collect())
}

// A lexicographic cut has one interval for each earlier differing tuple part
// and one for the entity-id tie. All intervals use ascending physical bytes.
fn order_cut<G: DigestGroup>(
    spec: &KeySpec<PropertyId>,
    directions: &[IndexDirection],
    id_direction: IndexDirection,
    pivot: &LeafRow<G>,
    before: bool,
) -> Result<Vec<TreeSpan>, TreeError> {
    let parts = CanonicalCodec.split(spec, &pivot.key)?;
    let mut prefix = Vec::new();
    let mut ranges = Vec::new();
    for (part, direction) in parts.iter().zip(directions) {
        let value = [prefix.as_slice(), part].concat();
        let tier = prefix_range(&prefix);
        let smaller = before == (*direction == IndexDirection::Asc);
        let range = if smaller {
            Some(TreeSpan::new(tier.start, Excluded(value.clone())))
        } else {
            prefix_end(&value).map(|start| TreeSpan::new(Included(start), tier.end))
        };
        if let Some(range) = range {
            ranges.push(range);
        }
        prefix = value;
    }
    let tier = prefix_range(&prefix);
    let address = pivot.address();
    let smaller = before == (id_direction == IndexDirection::Asc);
    let edge = if smaller {
        TreeSpan::new(tier.start, if before { Included(address) } else { Excluded(address) })
    } else {
        TreeSpan::new(if before { Included(address) } else { Excluded(address) }, tier.end)
    };
    ranges.push(edge);
    Ok(normalize(ranges))
}
