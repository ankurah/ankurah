//! Whether a tree serves a request: the proof that the tree admits every
//! member the request asks for, and the exact spans of its leaves that select
//! them, in the request's order.

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included, Unbounded};

use ankurah_core_types::{IndexDirection, IndexKeyPart, KeySpec, PropertyId, PropertyPath, ValueType};

use super::request::{clauses, Clause, TreeRequest};
use super::TreeSpec;
use crate::address::span::prefix_end;
use crate::address::{validate_part, TreeSpan};

/// How a tree serves a request before applying its optional limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeMatch {
    /// The request has the tree's whole admitted membership.
    Whole,
    /// Exact bounded spans select a subset of the tree's membership.
    Span,
}

/// How a tree serves a request: whole or as spans, the spans that select the
/// request's members, and the direction the request walks each key part and
/// the entity id in.
pub(crate) struct ReusePlan {
    pub kind: TreeMatch,
    pub spans: Vec<TreeSpan>,
    pub directions: Vec<IndexDirection>,
    pub id_direction: IndexDirection,
}

impl TreeSpec {
    /// Prove that this tree contains every requested member and that exact
    /// address bounds and, under a limit, the requested order can select
    /// them: `None` where the proof, which is conservative, cannot establish
    /// it. A Selection's scope also requires each entity to have one leaf.
    ///
    /// A tree serves a narrower request when it admits every entity the
    /// request asks for and the rest of the request is exactly a set of bounds
    /// in the tree's key order: the tree admitting `project = 'acme' AND foo >
    /// 7`, keyed by (project, foo), serves `project = 'acme' AND foo > 10` as
    /// a span. Both read the same leaves at the same addresses, so serving the
    /// narrower request makes no second tree. The tree's nodes need not end at
    /// the bounds: a tree node a span cuts is descended into, or its leaves
    /// within the span read, never counted whole. The proof reads the
    /// predicates alone, never entity state.
    pub fn serves(&self, request: &TreeRequest) -> Option<TreeMatch> { self.plan(request).map(|plan| plan.kind) }

    pub(crate) fn plan(&self, request: &TreeRequest) -> Option<ReusePlan> {
        if request.clauses.is_empty() || request.limit == Some(0) {
            return Some(ReusePlan {
                kind: TreeMatch::Span,
                spans: vec![],
                directions: vec![IndexDirection::Asc; self.key_spec().keyparts.len()],
                id_direction: IndexDirection::Asc,
            });
        }
        let mut types = request.types.clone();
        for key in &self.key_spec().keyparts {
            validate_part(key).ok()?;
            if types.get(&key.path()).is_some_and(|t| *t != key.value_type) {
                return None;
            }
            types.insert(key.path(), key.value_type);
        }
        let admission = clauses(self.predicate(), &types).ok()?;
        if !implies(&request.clauses, &admission) {
            return None;
        }
        let whole = implies(&admission, &request.clauses);
        let mut spans = Vec::new();
        if whole {
            spans.push(TreeSpan::all());
        } else {
            for clause in &request.clauses {
                spans.push(clause_span(self.key_spec(), clause, &admission)?);
            }
        }
        let mut directions = vec![IndexDirection::Asc; self.key_spec().keyparts.len()];
        let mut id_direction = IndexDirection::Asc;
        if request.limit.is_some() {
            let mut order = request.order.keyparts.iter().filter(|p| request.pinned(&p.path()).is_none());
            for (i, key) in self.key_spec().keyparts.iter().enumerate() {
                if request.pinned(&key.path()).is_some() {
                    continue;
                }
                let requested = order.next()?;
                if key.path() != requested.path() || key.value_type != requested.value_type {
                    return None;
                }
                directions[i] = requested.direction;
            }
            if let Some(last) = order.next() {
                if last.path() != PropertyPath::id() {
                    return None;
                }
                id_direction = last.direction;
                // Once entity id is compared, later terms cannot break a tie.
            }
        }
        Some(ReusePlan {
            kind: if whole { TreeMatch::Whole } else { TreeMatch::Span },
            spans: TreeSpan::normalize(spans),
            directions,
            id_direction,
        })
    }
}

fn implies(left: &[Clause], right: &[Clause]) -> bool {
    left.iter().all(|a| {
        right.iter().any(|b| {
            b.opaque.is_subset(&a.opaque)
                && b.fields.iter().all(|(path, domain)| a.fields.get(path).is_some_and(|value| value.is_subset_of(domain)))
        })
    })
}

fn clause_span(key: &KeySpec<PropertyId>, query: &Clause, admission: &[Clause]) -> Option<TreeSpan> {
    if query.opaque.iter().any(|term| !admission.iter().all(|c| c.opaque.contains(term))) {
        return None;
    }
    let mut required: BTreeSet<_> = query
        .fields
        .iter()
        .filter(|(path, domain)| !admission.iter().all(|c| c.fields.get(*path).is_some_and(|stored| stored.is_subset_of(domain))))
        .map(|(path, _)| path.clone())
        .collect();
    let mut prefix = Vec::new();
    let mut span = TreeSpan::all();
    let mut parts = key.keyparts.clone();
    parts.push(IndexKeyPart::asc(PropertyId::Id, ValueType::EntityId));
    for (index, keypart) in parts.into_iter().enumerate() {
        let path = keypart.path();
        let implicit_id = index == key.keyparts.len();
        let Some(domain) = query.fields.get(&path) else { break };
        required.remove(&path);
        if let Some(value) = domain.single() {
            // The implicit entity-id suffix has no nullable part marker.
            prefix.extend(if implicit_id { value.get(1..)? } else { value });
            span = TreeSpan::starting_with(&prefix);
        } else {
            span = value_span(&prefix, domain, implicit_id)?;
            break;
        }
    }
    required.is_empty().then_some(span)
}

/// The addresses of every key that begins with `prefix` and continues with a
/// value in `domain`: an included upper value ends the span after every key
/// that continues past it, and an excluded lower value starts it there.
fn value_span(prefix: &[u8], domain: &TreeSpan, implicit_id: bool) -> Option<TreeSpan> {
    let address = |value: &[u8]| -> Option<Vec<u8>> {
        let value = if implicit_id { value.get(1..)? } else { value };
        Some([prefix, value].concat())
    };
    let whole = TreeSpan::starting_with(prefix);
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
    Some(whole.intersect(&TreeSpan::new(start, end)).unwrap_or_else(empty))
}
