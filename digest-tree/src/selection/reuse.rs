//! Whether a tree serves a request: the proof that the tree admits every
//! member the request asks for, and the exact spans of its leaves that select
//! them, in the request's order.

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included, Unbounded};

use ankurah_core_types::{IndexDirection, IndexKeyPart, KeySpec, PropertyId, PropertyPath, ValueType};

use super::request::{clauses, path, Clause, TreeRequest};
use super::TreeSpec;
use crate::address::span::{intersect, normalize, prefix_end, prefix_range, singleton, subset};
use crate::address::{validate_part, TreeSpan};

/// How a tree serves a request before applying its optional limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeMatch {
    /// The request has the tree's whole admitted membership.
    Whole,
    /// Exact bounded spans select a subset of the tree's membership.
    Span,
}

pub(crate) struct ReusePlan {
    pub kind: TreeMatch,
    pub ranges: Vec<TreeSpan>,
    pub directions: Vec<IndexDirection>,
    pub id_direction: IndexDirection,
}

impl TreeSpec {
    /// Prove that this tree contains every requested member and that exact
    /// address bounds and, under a limit, the requested order can select them.
    /// `None` includes cases the conservative implication proof cannot establish.
    /// Selection scopes additionally check that each entity has only one leaf.
    pub fn serves(&self, request: &TreeRequest) -> Option<TreeMatch> { self.plan(request).map(|plan| plan.kind) }

    pub(crate) fn plan(&self, request: &TreeRequest) -> Option<ReusePlan> {
        if request.clauses.is_empty() || request.limit == Some(0) {
            return Some(ReusePlan {
                kind: TreeMatch::Span,
                ranges: vec![],
                directions: vec![IndexDirection::Asc; self.key_spec().keyparts.len()],
                id_direction: IndexDirection::Asc,
            });
        }
        let mut types = request.types.clone();
        for key in &self.key_spec().keyparts {
            validate_part(key).ok()?;
            if types.get(&path(key)).is_some_and(|t| *t != key.value_type) {
                return None;
            }
            types.insert(path(key), key.value_type);
        }
        let admission = clauses(self.predicate(), &types).ok()?;
        if !implies(&request.clauses, &admission) {
            return None;
        }
        let whole = implies(&admission, &request.clauses);
        let mut ranges = Vec::new();
        if whole {
            ranges.push(TreeSpan::all());
        } else {
            for clause in &request.clauses {
                ranges.push(span(self.key_spec(), clause, &admission)?);
            }
        }
        let mut directions = vec![IndexDirection::Asc; self.key_spec().keyparts.len()];
        let mut id_direction = IndexDirection::Asc;
        if request.limit.is_some() {
            let mut order = request.order.keyparts.iter().filter(|p| request.pinned(&path(p)).is_none());
            for (i, key) in self.key_spec().keyparts.iter().enumerate() {
                if request.pinned(&path(key)).is_some() {
                    continue;
                }
                let requested = order.next()?;
                if path(key) != path(requested) || key.value_type != requested.value_type {
                    return None;
                }
                directions[i] = requested.direction;
            }
            if let Some(last) = order.next() {
                if path(last) != PropertyPath::id() {
                    return None;
                }
                id_direction = last.direction;
                // Once entity id is compared, later terms cannot break a tie.
            }
        }
        Some(ReusePlan {
            kind: if whole { TreeMatch::Whole } else { TreeMatch::Span },
            ranges: normalize(ranges),
            directions,
            id_direction,
        })
    }
}

fn implies(left: &[Clause], right: &[Clause]) -> bool {
    left.iter().all(|a| {
        right.iter().any(|b| {
            b.opaque.is_subset(&a.opaque)
                && b.fields.iter().all(|(path, domain)| a.fields.get(path).is_some_and(|value| subset(value, domain)))
        })
    })
}

fn span(key: &KeySpec<PropertyId>, query: &Clause, admission: &[Clause]) -> Option<TreeSpan> {
    if query.opaque.iter().any(|term| !admission.iter().all(|c| c.opaque.contains(term))) {
        return None;
    }
    let mut required: BTreeSet<_> = query
        .fields
        .iter()
        .filter(|(path, domain)| !admission.iter().all(|c| c.fields.get(*path).is_some_and(|stored| subset(stored, domain))))
        .map(|(path, _)| path.clone())
        .collect();
    let mut prefix = Vec::new();
    let mut range = TreeSpan::all();
    let mut parts = key.keyparts.clone();
    parts.push(IndexKeyPart::asc(PropertyId::Id, ValueType::EntityId));
    for (index, keypart) in parts.into_iter().enumerate() {
        let path = path(&keypart);
        let implicit_id = index == key.keyparts.len();
        let Some(domain) = query.fields.get(&path) else { break };
        required.remove(&path);
        if let Some(value) = singleton(domain) {
            // The implicit entity-id suffix has no nullable part marker.
            prefix.extend(if implicit_id { value.get(1..)? } else { value });
            range = prefix_range(&prefix);
        } else {
            range = part_range(&prefix, domain, implicit_id)?;
            break;
        }
    }
    required.is_empty().then_some(range)
}
// Convert a value interval into the addresses of all tuples with this
// leading prefix. An inclusive upper value ends after every suffix of it;
// an exclusive lower value starts there instead.
fn part_range(prefix: &[u8], domain: &TreeSpan, implicit_id: bool) -> Option<TreeSpan> {
    let address = |value: &[u8]| -> Option<Vec<u8>> {
        let value = if implicit_id { value.get(1..)? } else { value };
        Some([prefix, value].concat())
    };
    let whole = prefix_range(prefix);
    let empty = || TreeSpan::new(Included(vec![]), Excluded(vec![]));
    let start = match &domain.start {
        Included(value) => Included(address(value)?),
        Excluded(value) => match prefix_end(&address(value)?) {
            Some(after) => Included(after),
            None => return Some(empty()),
        },
        Unbounded => whole.start.clone(),
    };
    let end = match &domain.end {
        Included(value) => prefix_end(&address(value)?).map_or(Unbounded, Excluded),
        Excluded(value) => Excluded(address(value)?),
        Unbounded => whole.end.clone(),
    };
    Some(intersect(&whole, &TreeSpan::new(start, end)).unwrap_or_else(empty))
}
