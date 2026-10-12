//! Snapshot-bound query scopes, tier-ordered traversal and clipped summaries.

use crate::bounds::{gaps, intersect, lower_cmp, normalize, subset};
use crate::key::prefix_end;
use crate::layout;
use crate::map::{MapAccess, MapName};
use crate::request::{prefix_range, ReusePlan};
use crate::{AddressRange, Digest, DigestGroup, KeyCodec, LeafRow, NodePrefix, Ristretto255, TreeError, TreeRequest, TreeTransaction};
use ankurah_core_types::{IndexDirection, KeySpec, PropertyId};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

/// Exact disjoint address spans belonging to one transaction and its current
/// leaf state. A mutation invalidates the scope; another transaction cannot use it.
#[derive(Debug, Clone)]
pub struct TreeScope {
    ranges: Vec<AddressRange>,
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
    pub fn ranges(&self) -> &[AddressRange] { &self.ranges }
}

/// A cached or clipped subtree summary. Bounds are addresses, never offsets
/// into a bucket, so another tree can compute the same region independently.
/// A peer must also establish compatible keys, digest groups and admitted
/// membership; a head alone does not carry those proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeHead<G: DigestGroup = Ristretto255> {
    /// The prefix containing this region. A clipped or empty region need not
    /// have a stored node at this prefix on the answering peer.
    pub prefix: NodePrefix,
    /// Exact address bounds within that node.
    pub bounds: AddressRange,
    /// Digest of precisely the leaves within these bounds.
    pub digest: Digest<G>,
}
impl<G: DigestGroup> TreeHead<G> {
    /// Encode a versioned head for a peer, independent of a local snapshot.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&(1u8, self.prefix.to_bytes(), &self.bounds.start, &self.bounds.end, self.digest.to_row_bytes())).unwrap()
    }
    /// Decode a head and validate its prefix, bounds, count and group encoding.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TreeError> {
        use bincode::Options;
        type EncodedHead = (u8, Vec<u8>, std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>, Vec<u8>);
        let (version, prefix, start, end, digest): EncodedHead = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes()
            .deserialize(bytes)
            .map_err(|e| TreeError::unreadable("tree head", e))?;
        if version != 1 {
            return Err(TreeError::InvalidKey("unknown tree head version"));
        }
        let prefix = NodePrefix::from_bytes(&prefix).map_err(|e| TreeError::unreadable("head prefix", e))?;
        let bounds = AddressRange::new(start, end);
        let digest = Digest::from_row_bytes(&digest).map_err(|e| TreeError::unreadable("head digest", e))?;
        if crate::bounds::empty(&bounds) || !subset(&bounds, &AddressRange::under(&prefix)) || digest.count() < 0 {
            return Err(TreeError::InvalidKey("invalid tree head bounds or count"));
        }
        Ok(Self { prefix, bounds, digest })
    }
}

/// An opaque continuation within a scope's order and snapshot.
#[derive(Debug, Clone)]
pub struct LeafCursor {
    remaining: Vec<AddressRange>,
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
    /// `AddressRange::under`. Unlike Selection scopes these count leaves,
    /// including multiple keys for one entity, and walk in address order.
    pub fn scope_ranges(&self, ranges: impl IntoIterator<Item = AddressRange>) -> TreeScope {
        TreeScope {
            ranges: normalize(ranges.into_iter().collect()),
            directions: vec![IndexDirection::Asc; self.spec.as_ref().map_or(0, |spec| spec.key_spec().keyparts.len())],
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
    pub async fn scope(&mut self, request: &TreeRequest, codec: &impl KeyCodec) -> Result<TreeScope, TreeError> {
        let spec = self.spec.as_ref().ok_or(TreeError::UnsupportedSelection)?.clone();
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
                let rows = self.walk(&ranges, &directions, id_direction, codec, limit).await?;
                if rows.len() == limit {
                    let last = rows.last().expect("a positive limit was filled");
                    let cut = order_cut(spec.key_spec(), &directions, id_direction, codec, last, true)?;
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

    /// Summaries that tile the scope without overlap. Fully covered cached
    /// nodes contribute their stored digest; boundary buckets are clipped by
    /// reading only their leaves inside the requested address bounds. Empty
    /// regions remain in the cover: a peer may have leaves there.
    pub async fn heads(&mut self, scope: &TreeScope) -> Result<Vec<TreeHead<G>>, TreeError> {
        self.check_scope(scope)?;
        let mut heads = Vec::new();
        for range in &scope.ranges {
            heads.extend(self.heads_in(&NodePrefix::root(), range).await?);
        }
        Ok(heads)
    }

    /// Exact leaf count; Selection scopes require one leaf per entity.
    pub async fn count(&mut self, scope: &TreeScope) -> Result<u64, TreeError> {
        let digest = self.digest(scope).await?;
        u64::try_from(digest.count()).map_err(|_| TreeError::Audit("negative scoped count"))
    }

    /// Sum the exact summaries of a scope.
    pub async fn digest(&mut self, scope: &TreeScope) -> Result<Digest<G>, TreeError> {
        Digest::checked_sum(self.heads(scope).await?.iter().map(|head| head.digest)).map_err(Into::into)
    }

    /// Refine a head into the nearest cached descendants, keeping its address
    /// clip. At a terminal bucket, returns its exact clipped head again.
    pub async fn refine(&mut self, head: &TreeHead<G>) -> Result<Vec<TreeHead<G>>, TreeError> {
        let bounds = intersect(&AddressRange::under(&head.prefix), &head.bounds).ok_or(TreeError::InvalidKey("empty head"))?;
        let children = self.child_prefixes(&head.prefix).await?;
        if children.is_empty() {
            return self.heads_in(&head.prefix, &bounds).await;
        }
        let mut heads = empty_gaps(&head.prefix, &bounds, &children);
        for prefix in children {
            heads.extend(self.heads_in(&prefix, &bounds).await?);
        }
        heads.sort_by(|a, b| lower_cmp(&a.bounds.start, &b.bounds.start));
        Ok(heads)
    }

    async fn heads_in(&mut self, prefix: &NodePrefix, range: &AddressRange) -> Result<Vec<TreeHead<G>>, TreeError> {
        let mut pending = vec![prefix.clone()];
        let mut heads = Vec::new();
        while let Some(prefix) = pending.pop() {
            let entire = AddressRange::under(&prefix);
            let Some(bounds) = intersect(&entire, range) else {
                continue;
            };
            if subset(&entire, range) {
                if let Some(node) = self.node(&prefix).await? {
                    if node.digest.count() < 0 {
                        return Err(TreeError::Audit("negative node count"));
                    }
                    heads.push(TreeHead { prefix, bounds, digest: node.digest });
                    continue;
                }
            }
            let children = self.child_prefixes(&prefix).await?;
            if children.is_empty() {
                let rows = self.all_leaves(&bounds).await?;
                heads.push(TreeHead { prefix, bounds, digest: rows.iter().map(|row| Digest::from(row.element)).sum() });
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
    pub async fn page(
        &mut self,
        scope: &TreeScope,
        codec: &impl KeyCodec,
        cursor: Option<&LeafCursor>,
        limit: usize,
    ) -> Result<LeafPage<G>, TreeError> {
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
        let mut rows = self.walk(ranges, &scope.directions, scope.id_direction, codec, limit.saturating_add(1)).await?;
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next = if more {
            let spec = self.spec.as_ref().ok_or(TreeError::UnsupportedSelection)?;
            let cut = order_cut(spec.key_spec(), &scope.directions, scope.id_direction, codec, rows.last().unwrap(), false)?;
            Some(LeafCursor { remaining: intersections(ranges, &cut), scope: scope.clone() })
        } else {
            None
        };
        Ok(LeafPage { leaves: rows, next })
    }

    async fn walk(
        &mut self,
        ranges: &[AddressRange],
        directions: &[IndexDirection],
        id_direction: IndexDirection,
        codec: &impl KeyCodec,
        limit: usize,
    ) -> Result<Vec<LeafRow<G>>, TreeError> {
        let spec = self.spec.as_ref().ok_or(TreeError::UnsupportedSelection)?.key_spec().clone();
        let mut walk = LeafWalk { spec, directions, id_direction, codec, limit, leaves: Vec::new() };
        walk.read(self, ranges.to_vec(), 0).await?;
        Ok(walk.leaves)
    }
}

/// Children cover the locally stored leaves, but compressed prefixes need
/// not cover their parent's addresses. Represent the rest explicitly.
fn empty_gaps<G: DigestGroup>(prefix: &NodePrefix, bounds: &AddressRange, children: &[NodePrefix]) -> Vec<TreeHead<G>> {
    let covered: Vec<_> = children.iter().map(AddressRange::under).collect();
    gaps(bounds, &covered).into_iter().map(|bounds| TreeHead { prefix: prefix.clone(), bounds, digest: Digest::empty() }).collect()
}

/// A walk's fixed order and accumulated page, shared by all tuple tiers.
struct LeafWalk<'a, G: DigestGroup, C: KeyCodec> {
    spec: KeySpec<PropertyId>,
    directions: &'a [IndexDirection],
    id_direction: IndexDirection,
    codec: &'a C,
    limit: usize,
    leaves: Vec<LeafRow<G>>,
}

impl<G: DigestGroup, C: KeyCodec> LeafWalk<'_, G, C> {
    async fn read<T: MapAccess>(
        &mut self,
        transaction: &mut TreeTransaction<G, T>,
        mut ranges: Vec<AddressRange>,
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
            let parts = self.codec.split(&self.spec, &seed.key)?;
            let prefix = parts[..=depth].concat();
            let inside = intersections(&ranges, &[prefix_range(&prefix)]);
            Box::pin(self.read(transaction, inside, depth + 1)).await?;
            let rest = match direction {
                IndexDirection::Asc => match prefix_end(&prefix) {
                    Some(end) => AddressRange::new(Included(end), Unbounded),
                    None => break,
                },
                IndexDirection::Desc => AddressRange::new(Unbounded, Excluded(prefix)),
            };
            ranges = intersections(&ranges, &[rest]);
        }
        Ok(())
    }
}

fn intersections(a: &[AddressRange], b: &[AddressRange]) -> Vec<AddressRange> {
    normalize(a.iter().flat_map(|a| b.iter().filter_map(move |b| intersect(a, b))).collect())
}

// A lexicographic cut has one interval for each earlier differing tuple part
// and one for the entity-id tie. All intervals use ascending physical bytes.
fn order_cut<G: DigestGroup>(
    spec: &KeySpec<PropertyId>,
    directions: &[IndexDirection],
    id_direction: IndexDirection,
    codec: &impl KeyCodec,
    pivot: &LeafRow<G>,
    before: bool,
) -> Result<Vec<AddressRange>, TreeError> {
    let parts = codec.split(spec, &pivot.key)?;
    let mut prefix = Vec::new();
    let mut ranges = Vec::new();
    for (part, direction) in parts.iter().zip(directions) {
        let value = [prefix.as_slice(), part].concat();
        let tier = prefix_range(&prefix);
        let smaller = before == (*direction == IndexDirection::Asc);
        let range = if smaller {
            Some(AddressRange::new(tier.start, Excluded(value.clone())))
        } else {
            prefix_end(&value).map(|start| AddressRange::new(Included(start), tier.end))
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
        AddressRange::new(tier.start, if before { Included(address) } else { Excluded(address) })
    } else {
        AddressRange::new(if before { Included(address) } else { Excluded(address) }, tier.end)
    };
    ranges.push(edge);
    Ok(normalize(ranges))
}
