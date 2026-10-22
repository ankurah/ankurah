//! Snapshot-bound query scopes, tier-ordered traversal and span digests.

use crate::bounds::{gaps, intersect, lower_cmp, normalize, subset};
use crate::key::prefix_end;
use crate::layout;
use crate::map::{MapAccess, MapName};
use crate::request::{prefix_range, ReusePlan};
use crate::{
    CanonicalCodec, Digest, DigestGroup, LeafRow, NodePrefix, Ristretto255, TreeError, TreeRequest, TreeSpan, TreeSpec, TreeTransaction,
};
use ankurah_core_types::{EntityId, IndexDirection, KeySpec, PropertyId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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

impl TreeSpec {
    /// Resolve a request's membership and limit using an existing ordered address
    /// map, without building or reading digests. Underfilled limits retain the
    /// membership bounds, just as [`TreeTransaction::scope`] does.
    ///
    /// The caller must supply this spec's admitted members, with exactly one
    /// canonical address (complete key followed by entity ID) per entity. Only
    /// addresses encountered while selecting the limit are validated; checking
    /// admission and uniqueness requires the caller's maintained membership index.
    pub fn scope_addresses<V>(&self, request: &TreeRequest, addresses: &BTreeMap<Vec<u8>, V>) -> Result<Vec<TreeSpan>, TreeError> {
        let ReusePlan { mut ranges, directions, id_direction, .. } = self.plan(request).ok_or(TreeError::UnsupportedSelection)?;
        let Some(limit) = request.limit else { return Ok(ranges) };
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut walk = TierWalk::new(self.key_spec(), &directions, id_direction, limit, &ranges);
        let mut last = None;
        while let Some(scan) = walk.next_scan() {
            let mut range = addresses.range::<[u8], _>(scan.bounds.bounds());
            let mut first = None;
            let mut count = 0;
            for _ in 0..scan.limit {
                let next = match scan.direction {
                    IndexDirection::Asc => range.next(),
                    IndexDirection::Desc => range.next_back(),
                };
                let Some((address, _)) = next else { break };
                address_parts(self.key_spec(), address)?;
                first.get_or_insert(address.as_slice());
                if scan.leaves {
                    last = Some(address.as_slice());
                }
                count += 1;
            }
            walk.advance(first, count)?;
        }
        if walk.seen == limit {
            let cut = order_cut(self.key_spec(), &directions, id_direction, last.expect("a positive limit was filled"), true)?;
            ranges = intersections(&ranges, &cut);
        }
        Ok(ranges)
    }
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
                    let cut = order_cut(spec.key_spec(), &directions, id_direction, &last.address(), true)?;
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
            let cut = order_cut(spec.key_spec(), &scope.directions, scope.id_direction, &rows.last().unwrap().address(), false)?;
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
        let mut walk = TierWalk::new(&spec, directions, id_direction, limit, ranges);
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
    gaps(bounds, &covered).into_iter().map(|bounds| TreeSpanDigest { prefix: prefix.clone(), bounds, digest: Digest::empty() }).collect()
}

/// Traversal decisions shared by stored leaves and an already ordered address
/// map. Each tuple tier has its own direction; entity IDs break final ties.
struct TierWalk<'a> {
    spec: &'a KeySpec<PropertyId>,
    directions: &'a [IndexDirection],
    id_direction: IndexDirection,
    limit: usize,
    seen: usize,
    tiers: Vec<Tier>,
}
struct Tier {
    ranges: Vec<TreeSpan>,
    depth: usize,
    next: usize,
}
struct TierScan {
    bounds: TreeSpan,
    direction: IndexDirection,
    limit: usize,
    leaves: bool,
}

impl<'a> TierWalk<'a> {
    fn new(
        spec: &'a KeySpec<PropertyId>,
        directions: &'a [IndexDirection],
        id_direction: IndexDirection,
        limit: usize,
        ranges: &[TreeSpan],
    ) -> Self {
        Self { spec, directions, id_direction, limit, seen: 0, tiers: vec![Tier { ranges: ranges.to_vec(), depth: 0, next: 0 }] }
    }

    fn next_scan(&mut self) -> Option<TierScan> {
        if self.seen == self.limit {
            return None;
        }
        loop {
            let tier = self.tiers.last()?;
            if tier.next == tier.ranges.len() {
                self.tiers.pop();
                continue;
            }
            let direction = self.directions.get(tier.depth).copied().unwrap_or(self.id_direction);
            let index = match direction {
                IndexDirection::Asc => tier.next,
                IndexDirection::Desc => tier.ranges.len() - tier.next - 1,
            };
            let leaves = tier.depth == self.spec.keyparts.len();
            return Some(TierScan {
                bounds: tier.ranges[index].clone(),
                direction,
                limit: if leaves { self.limit - self.seen } else { 1 },
                leaves,
            });
        }
    }

    fn advance(&mut self, first: Option<&[u8]>, count: usize) -> Result<(), TreeError> {
        let tier = self.tiers.last_mut().expect("advance follows a scan");
        if tier.depth == self.spec.keyparts.len() {
            self.seen += count;
            tier.next += 1;
            return Ok(());
        }
        let Some(address) = first else {
            tier.next += 1;
            return Ok(());
        };
        let parts = address_parts(self.spec, address)?;
        let prefix = parts[..=tier.depth].concat();
        let inside = intersections(&tier.ranges, &[prefix_range(&prefix)]);
        let direction = self.directions[tier.depth];
        let rest = match direction {
            IndexDirection::Asc => prefix_end(&prefix).map(|end| TreeSpan::new(Included(end), Unbounded)),
            IndexDirection::Desc => Some(TreeSpan::new(Unbounded, Excluded(prefix))),
        };
        tier.ranges = rest.map(|rest| intersections(&tier.ranges, &[rest])).unwrap_or_default();
        tier.next = 0;
        let depth = tier.depth + 1;
        self.tiers.push(Tier { ranges: inside, depth, next: 0 });
        Ok(())
    }
}

fn address_parts<'a>(spec: &KeySpec<PropertyId>, address: &'a [u8]) -> Result<Vec<&'a [u8]>, TreeError> {
    let key_len = address.len().checked_sub(EntityId::BYTE_LEN).ok_or(TreeError::InvalidKey("truncated leaf address"))?;
    CanonicalCodec.split(spec, &address[..key_len])
}

fn intersections(a: &[TreeSpan], b: &[TreeSpan]) -> Vec<TreeSpan> {
    normalize(a.iter().flat_map(|a| b.iter().filter_map(move |b| intersect(a, b))).collect())
}

// A lexicographic cut has one interval for each earlier differing tuple part
// and one for the entity-id tie. All intervals use ascending physical bytes.
fn order_cut(
    spec: &KeySpec<PropertyId>,
    directions: &[IndexDirection],
    id_direction: IndexDirection,
    address: &[u8],
    before: bool,
) -> Result<Vec<TreeSpan>, TreeError> {
    let parts = address_parts(spec, address)?;
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
    let address = address.to_vec();
    let smaller = before == (id_direction == IndexDirection::Asc);
    let edge = if smaller {
        TreeSpan::new(tier.start, if before { Included(address) } else { Excluded(address) })
    } else {
        TreeSpan::new(if before { Included(address) } else { Excluded(address) }, tier.end)
    };
    ranges.push(edge);
    Ok(normalize(ranges))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{leaf_address, DigestTree, HeadHash, LeafData};
    use ankql::ast::{Resolved, Selection};
    use ankurah_core_types::{IndexKeyPart, ModelId, PropertyPath, Value, ValueType};
    use std::collections::HashMap;

    fn property(name: &str) -> PropertyId {
        let byte = match name {
            "project" => 1,
            "foo" => 2,
            "bar" => 3,
            _ => panic!("unknown fixture property"),
        };
        PropertyId::EntityId(EntityId::from_bytes([byte; 32]))
    }

    fn selection(text: &str) -> Selection<Resolved> {
        ankql::selection::map_references(
            &ankql::parser::parse_selection(text).unwrap(),
            &|path| PropertyPath::from(if path.steps[0] == "id" { PropertyId::Id } else { property(&path.steps[0]) }),
            &|_| ModelId::EntityId(EntityId::from_bytes([99; 32])),
        )
    }

    fn types() -> HashMap<PropertyId, ValueType> {
        HashMap::from([(property("project"), ValueType::String), (property("foo"), ValueType::I64), (property("bar"), ValueType::I64)])
    }

    #[tokio::test]
    async fn address_scopes_match_tree_scopes_and_sorted_members() {
        let admission = selection("project = 'acme'");
        let spec = TreeSpec::new(
            KeySpec::new(vec![
                IndexKeyPart::asc(property("project"), ValueType::String),
                IndexKeyPart::asc(property("foo"), ValueType::I64),
                IndexKeyPart::asc(property("bar"), ValueType::I64),
            ]),
            admission.predicate,
        );
        let model: Vec<_> = (0..90u8)
            .map(|n| {
                let (foo, bar) = (i64::from(n % 5), i64::from(n / 5 % 3));
                let key = CanonicalCodec.encode_prefix(spec.key_spec(), &[Value::from("acme"), Value::I64(foo), Value::I64(bar)]).unwrap();
                (foo, bar, LeafData { key, entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[n; 32]]) })
            })
            .collect();
        let tree: DigestTree = DigestTree::new(spec);
        let mut tx = tree.begin();
        tx.build(model.iter().map(|(_, _, row)| row.clone())).await.unwrap();
        let addresses: BTreeMap<_, _> = model.iter().map(|(_, _, row)| (leaf_address(&row.key, row.entity_id), ())).collect();
        for disjoint in [false, true] {
            for foo_desc in [false, true] {
                for bar_desc in [false, true] {
                    for id_desc in [false, true] {
                        for limit in [0, 1, 9, 37, 200] {
                            let dir = |desc| if desc { "DESC" } else { "ASC" };
                            let predicate = if disjoint { "project = 'acme' AND (foo < 1 OR foo > 3)" } else { "project = 'acme'" };
                            let query = selection(&format!(
                                "{predicate} ORDER BY foo {}, bar {}, id {} LIMIT {limit}",
                                dir(foo_desc),
                                dir(bar_desc),
                                dir(id_desc),
                            ));
                            let request = TreeRequest::for_selection(&query, &types()).unwrap();
                            let actual = tree.spec().scope_addresses(&request, &addresses).unwrap();
                            let scope = tx.scope(&request).await.unwrap();
                            assert_eq!(actual, scope.ranges(), "{query}");

                            let mut expected: Vec<_> = model.iter().filter(|(foo, _, _)| !disjoint || *foo < 1 || *foo > 3).collect();
                            let compare = |a: std::cmp::Ordering, desc| if desc { a.reverse() } else { a };
                            expected.sort_by(|a, b| {
                                compare(a.0.cmp(&b.0), foo_desc)
                                    .then(compare(a.1.cmp(&b.1), bar_desc))
                                    .then(compare(a.2.entity_id.cmp(&b.2.entity_id), id_desc))
                            });
                            expected.truncate(limit);
                            let mut expected_ids: Vec<_> = expected.iter().map(|(_, _, row)| row.entity_id).collect();
                            expected_ids.sort();
                            let mut actual_ids: Vec<_> = model
                                .iter()
                                .filter(|(_, _, row)| actual.iter().any(|span| span.contains(&leaf_address(&row.key, row.entity_id))))
                                .map(|(_, _, row)| row.entity_id)
                                .collect();
                            actual_ids.sort();
                            assert_eq!(actual_ids, expected_ids, "{query}");
                        }
                    }
                }
            }
        }
        for query in ["project = 'acme'", "project = 'acme' AND foo > 2", "project = 'acme' AND foo > 10 ORDER BY foo, bar LIMIT 10"] {
            let request = TreeRequest::for_selection(&selection(query), &types()).unwrap();
            assert_eq!(tree.spec().scope_addresses(&request, &addresses).unwrap(), tx.scope(&request).await.unwrap().ranges());
        }
        let request = TreeRequest::for_selection(&selection("project = 'acme' ORDER BY foo, bar LIMIT 10"), &types()).unwrap();
        let mut empty: crate::TreeTransaction = DigestTree::new(tree.spec().clone()).begin();
        let empty_ranges = tree.spec().scope_addresses::<()>(&request, &BTreeMap::new()).unwrap();
        assert_eq!(empty_ranges, empty.scope(&request).await.unwrap().ranges());
        assert_eq!(empty_ranges, vec![TreeSpan::all()], "an empty local capture must allow missing peer members");
        let incompatible = TreeRequest::for_selection(&selection("project = 'acme' ORDER BY bar LIMIT 1"), &types()).unwrap();
        assert!(matches!(tree.spec().scope_addresses(&incompatible, &addresses), Err(TreeError::UnsupportedSelection)));
    }

    #[tokio::test]
    async fn entity_id_scopes_match_without_tuple_parts() {
        let spec = TreeSpec::new(KeySpec::new(vec![]), selection("true").predicate);
        let rows: Vec<_> = (0..20u8)
            .map(|n| LeafData { key: Vec::new(), entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[n; 32]]) })
            .collect();
        let addresses: BTreeMap<_, _> = rows.iter().map(|row| (leaf_address(&row.key, row.entity_id), ())).collect();
        let tree: DigestTree = DigestTree::new(spec);
        let mut tx = tree.begin();
        tx.build(rows).await.unwrap();
        for query in ["true LIMIT 0", "true LIMIT 3", "true ORDER BY id DESC LIMIT 3", "true ORDER BY id DESC LIMIT 30"] {
            let request = TreeRequest::for_selection(&selection(query), &types()).unwrap();
            assert_eq!(tree.spec().scope_addresses(&request, &addresses).unwrap(), tx.scope(&request).await.unwrap().ranges(), "{query}");
        }
    }

    #[test]
    fn address_scope_validates_only_encountered_addresses() {
        let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property("foo"), ValueType::I64)]), selection("true").predicate);
        let request = TreeRequest::for_selection(&selection("true ORDER BY foo LIMIT 1"), &types()).unwrap();
        for malformed in [vec![0], vec![1; 33], vec![0xff; 64]] {
            assert!(matches!(spec.scope_addresses(&request, &BTreeMap::from([(malformed, ())])), Err(TreeError::InvalidKey(_))));
        }
        let key = CanonicalCodec.encode_prefix(spec.key_spec(), &[Value::I64(0)]).unwrap();
        let address = leaf_address(&key, EntityId::from_bytes([0; 32]));
        let mut addresses = BTreeMap::from([(address.clone(), ()), (vec![0xff; 64], ())]);
        assert!(spec.scope_addresses(&request, &addresses).unwrap().iter().any(|span| span.contains(&address)));
        addresses.remove(&address);
        let zero = TreeRequest::for_selection(&selection("true ORDER BY foo LIMIT 0"), &types()).unwrap();
        assert!(spec.scope_addresses(&zero, &addresses).unwrap().is_empty());
        let scalar = TreeSpec::new(KeySpec::new(vec![]), selection("true").predicate);
        let scalar_request = TreeRequest::for_selection(&selection("true LIMIT 1"), &types()).unwrap();
        assert!(matches!(scalar.scope_addresses(&scalar_request, &BTreeMap::from([(vec![0], ())])), Err(TreeError::InvalidKey(_))));
    }
}
