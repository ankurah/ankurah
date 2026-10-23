//! The spans a request's limit selects: a walk over leaves in the request's
//! order, tier by tier, through a tree or through any ordered map of leaf
//! addresses, and the cut after the last leaf the limit admits.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

use ankurah_core_types::{EntityId, IndexDirection, KeySpec, PropertyId};

use super::reuse::ReusePlan;
use super::{TreeRequest, TreeSpec};
use crate::address::span::prefix_end;
use crate::address::{CanonicalCodec, TreeSpan};
use crate::error::TreeError;

impl TreeSpec {
    /// The spans of leaf addresses that `request` selects from this spec's
    /// members, its membership and its limit resolved over `addresses`, the
    /// members' addresses in a map the caller keeps, without building or
    /// reading digests. An underfilled limit keeps the membership's spans, just
    /// as [`TreeTransaction::scope`](crate::TreeTransaction::scope) does.
    ///
    /// The caller must supply this spec's admitted members, with exactly one
    /// canonical address (complete key followed by entity ID) per entity. Only
    /// addresses encountered while selecting the limit are validated; checking
    /// admission and uniqueness requires the caller's maintained membership index.
    pub fn selected_spans<K: Borrow<[u8]> + Ord, V>(
        &self,
        request: &TreeRequest,
        addresses: &BTreeMap<K, V>,
    ) -> Result<Vec<TreeSpan>, TreeError> {
        let ReusePlan { mut spans, directions, id_direction, .. } = self.plan(request).ok_or(TreeError::UnsupportedSelection)?;
        let Some(limit) = request.limit else { return Ok(spans) };
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut walk = TierWalk::new(self.key_spec(), &directions, id_direction, limit, &spans);
        let mut last = None;
        while let Some(scan) = walk.next_scan() {
            let mut entries = addresses.range::<[u8], _>(scan.bounds.bounds());
            let mut first = None;
            let mut count = 0;
            for _ in 0..scan.limit {
                let next = match scan.direction {
                    IndexDirection::Asc => entries.next(),
                    IndexDirection::Desc => entries.next_back(),
                };
                let Some((address, _)) = next else { break };
                let address: &[u8] = address.borrow();
                address_parts(self.key_spec(), address)?;
                first.get_or_insert(address);
                if scan.leaves {
                    last = Some(address);
                }
                count += 1;
            }
            walk.advance(first, count)?;
        }
        if walk.seen == limit {
            let cut = order_cut(self.key_spec(), &directions, id_direction, last.expect("a positive limit was filled"), true)?;
            spans = TreeSpan::intersect_all(&spans, &cut);
        }
        Ok(spans)
    }
}

/// Traversal decisions shared by stored leaves and an already ordered address
/// map. Each tuple tier has its own direction; entity IDs break final ties.
pub(crate) struct TierWalk<'a> {
    spec: &'a KeySpec<PropertyId>,
    directions: &'a [IndexDirection],
    id_direction: IndexDirection,
    limit: usize,
    seen: usize,
    tiers: Vec<Tier>,
}
struct Tier {
    spans: Vec<TreeSpan>,
    depth: usize,
    next: usize,
}
pub(crate) struct TierScan {
    pub(crate) bounds: TreeSpan,
    pub(crate) direction: IndexDirection,
    pub(crate) limit: usize,
    pub(crate) leaves: bool,
}

impl<'a> TierWalk<'a> {
    pub(crate) fn new(
        spec: &'a KeySpec<PropertyId>,
        directions: &'a [IndexDirection],
        id_direction: IndexDirection,
        limit: usize,
        spans: &[TreeSpan],
    ) -> Self {
        Self { spec, directions, id_direction, limit, seen: 0, tiers: vec![Tier { spans: spans.to_vec(), depth: 0, next: 0 }] }
    }

    pub(crate) fn next_scan(&mut self) -> Option<TierScan> {
        if self.seen == self.limit {
            return None;
        }
        loop {
            let tier = self.tiers.last()?;
            if tier.next == tier.spans.len() {
                self.tiers.pop();
                continue;
            }
            let direction = self.directions.get(tier.depth).copied().unwrap_or(self.id_direction);
            let index = match direction {
                IndexDirection::Asc => tier.next,
                IndexDirection::Desc => tier.spans.len() - tier.next - 1,
            };
            let leaves = tier.depth == self.spec.keyparts.len();
            return Some(TierScan {
                bounds: tier.spans[index].clone(),
                direction,
                limit: if leaves { self.limit - self.seen } else { 1 },
                leaves,
            });
        }
    }

    pub(crate) fn advance(&mut self, first: Option<&[u8]>, count: usize) -> Result<(), TreeError> {
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
        let inside = TreeSpan::intersect_all(&tier.spans, &[TreeSpan::starting_with(&prefix)]);
        let direction = self.directions[tier.depth];
        let rest = match direction {
            IndexDirection::Asc => prefix_end(&prefix).map(|end| TreeSpan::new(Included(end), Unbounded)),
            IndexDirection::Desc => Some(TreeSpan::new(Unbounded, Excluded(prefix))),
        };
        tier.spans = rest.map(|rest| TreeSpan::intersect_all(&tier.spans, &[rest])).unwrap_or_default();
        tier.next = 0;
        let depth = tier.depth + 1;
        self.tiers.push(Tier { spans: inside, depth, next: 0 });
        Ok(())
    }
}

fn address_parts<'a>(spec: &KeySpec<PropertyId>, address: &'a [u8]) -> Result<Vec<&'a [u8]>, TreeError> {
    let key_len = address.len().checked_sub(EntityId::BYTE_LEN).ok_or(TreeError::InvalidKey("truncated leaf address"))?;
    CanonicalCodec.split(spec, &address[..key_len])
}

// A lexicographic cut has one interval for each earlier differing tuple part
// and one for the entity-id tie. All intervals use ascending physical bytes.
pub(crate) fn order_cut(
    spec: &KeySpec<PropertyId>,
    directions: &[IndexDirection],
    id_direction: IndexDirection,
    address: &[u8],
    before: bool,
) -> Result<Vec<TreeSpan>, TreeError> {
    let parts = address_parts(spec, address)?;
    let mut prefix = Vec::new();
    let mut spans = Vec::new();
    for (part, direction) in parts.iter().zip(directions) {
        let value = [prefix.as_slice(), part].concat();
        let tier = TreeSpan::starting_with(&prefix);
        let smaller = before == (*direction == IndexDirection::Asc);
        let span = if smaller {
            Some(TreeSpan::new(tier.start, Excluded(value.clone())))
        } else {
            prefix_end(&value).map(|start| TreeSpan::new(Included(start), tier.end))
        };
        if let Some(span) = span {
            spans.push(span);
        }
        prefix = value;
    }
    let tier = TreeSpan::starting_with(&prefix);
    let address = address.to_vec();
    let smaller = before == (id_direction == IndexDirection::Asc);
    let edge = if smaller {
        TreeSpan::new(tier.start, if before { Included(address) } else { Excluded(address) })
    } else {
        TreeSpan::new(if before { Included(address) } else { Excluded(address) }, tier.end)
    };
    spans.push(edge);
    Ok(TreeSpan::normalize(spans))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{leaf_address, DigestTree, HeadHash, LeafInput};
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
                (foo, bar, LeafInput { key, entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[n; 32]]) })
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
                            let actual = tree.spec().selected_spans(&request, &addresses).unwrap();
                            let scope = tx.scope(&request).await.unwrap();
                            assert_eq!(actual, scope.spans(), "{query}");

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
            assert_eq!(tree.spec().selected_spans(&request, &addresses).unwrap(), tx.scope(&request).await.unwrap().spans());
        }
        let request = TreeRequest::for_selection(&selection("project = 'acme' ORDER BY foo, bar LIMIT 10"), &types()).unwrap();
        let mut empty: crate::TreeTransaction = DigestTree::new(tree.spec().clone()).begin();
        let empty_spans = tree.spec().selected_spans(&request, &BTreeMap::<Vec<u8>, ()>::new()).unwrap();
        assert_eq!(empty_spans, empty.scope(&request).await.unwrap().spans());
        assert_eq!(empty_spans, vec![TreeSpan::all()], "an empty local capture must allow missing peer members");
        let incompatible = TreeRequest::for_selection(&selection("project = 'acme' ORDER BY bar LIMIT 1"), &types()).unwrap();
        assert!(matches!(tree.spec().selected_spans(&incompatible, &addresses), Err(TreeError::UnsupportedSelection)));
    }

    #[tokio::test]
    async fn entity_id_scopes_match_without_tuple_parts() {
        let spec = TreeSpec::new(KeySpec::new(vec![]), selection("true").predicate);
        let rows: Vec<_> = (0..20u8)
            .map(|n| LeafInput { key: Vec::new(), entity_id: EntityId::from_bytes([n; 32]), head: HeadHash::of([[n; 32]]) })
            .collect();
        let addresses: BTreeMap<_, _> = rows.iter().map(|row| (leaf_address(&row.key, row.entity_id), ())).collect();
        let tree: DigestTree = DigestTree::new(spec);
        let mut tx = tree.begin();
        tx.build(rows).await.unwrap();
        for query in ["true LIMIT 0", "true LIMIT 3", "true ORDER BY id DESC LIMIT 3", "true ORDER BY id DESC LIMIT 30"] {
            let request = TreeRequest::for_selection(&selection(query), &types()).unwrap();
            assert_eq!(tree.spec().selected_spans(&request, &addresses).unwrap(), tx.scope(&request).await.unwrap().spans(), "{query}");
        }
    }

    #[test]
    fn address_scope_validates_only_encountered_addresses() {
        let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(property("foo"), ValueType::I64)]), selection("true").predicate);
        let request = TreeRequest::for_selection(&selection("true ORDER BY foo LIMIT 1"), &types()).unwrap();
        for malformed in [vec![0], vec![1; 33], vec![0xff; 64]] {
            assert!(matches!(spec.selected_spans(&request, &BTreeMap::from([(malformed, ())])), Err(TreeError::InvalidKey(_))));
        }
        let key = CanonicalCodec.encode_prefix(spec.key_spec(), &[Value::I64(0)]).unwrap();
        let address = leaf_address(&key, EntityId::from_bytes([0; 32]));
        let mut addresses = BTreeMap::from([(address.clone(), ()), (vec![0xff; 64], ())]);
        assert!(spec.selected_spans(&request, &addresses).unwrap().iter().any(|span| span.contains(&address)));
        addresses.remove(&address);
        let zero = TreeRequest::for_selection(&selection("true ORDER BY foo LIMIT 0"), &types()).unwrap();
        assert!(spec.selected_spans(&zero, &addresses).unwrap().is_empty());
        let scalar = TreeSpec::new(KeySpec::new(vec![]), selection("true").predicate);
        let scalar_request = TreeRequest::for_selection(&selection("true LIMIT 1"), &types()).unwrap();
        assert!(matches!(scalar.selected_spans(&scalar_request, &BTreeMap::from([(vec![0], ())])), Err(TreeError::InvalidKey(_))));
    }
}
