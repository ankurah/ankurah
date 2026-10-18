//! Selection planning belongs to the standalone tree. Reuse proves membership
//! coverage first, then derives exact intervals in the existing key layout.

use crate::bounds::{intersect, normalize, singleton, subset};
use crate::key::{encode_part, prefix_end, validate_part};
use crate::{PropertyTypes, TreeError, TreeSpan, TreeSpec};
use ankql::ast::{ComparisonOperator as Op, Expr, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core_types::{IndexDirection, IndexKeyPart, KeySpec, PropertyId, PropertyPath, ValueType};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{self, Excluded, Included, Unbounded};

/// One selection's membership, ordering, and optional limit. It does not
/// prescribe the layout of a tree that might already serve it.
#[derive(Debug, Clone)]
pub struct TreeRequest {
    pub(crate) clauses: Vec<Clause>,
    pub(crate) types: BTreeMap<PropertyPath, ValueType>,
    pub(crate) order: KeySpec<PropertyId>,
    pub(crate) limit: Option<u64>,
}

/// How a tree serves a request before applying its optional limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeMatch {
    /// The request has the tree's whole admitted membership.
    Whole,
    /// Exact bounded spans select a subset of the tree's membership.
    Span,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Clause {
    fields: BTreeMap<PropertyPath, TreeSpan>,
    opaque: BTreeSet<Vec<u8>>,
}

pub(crate) struct ReusePlan {
    pub kind: TreeMatch,
    pub ranges: Vec<TreeSpan>,
    pub directions: Vec<IndexDirection>,
    pub id_direction: IndexDirection,
}

impl TreeRequest {
    /// Resolve types and retain the request's order separately from membership.
    /// Equality/ranges, AND, OR, IN and BETWEEN are understood. Other terms
    /// require the same term in the serving tree's admission predicate.
    pub fn for_selection(selection: &Selection<Resolved>, types: &impl PropertyTypes) -> Result<Self, TreeError> {
        let mut paths = BTreeSet::new();
        collect_paths(&selection.predicate, &mut paths);
        for item in selection.order_by.iter().flatten() {
            paths.insert(item.path.clone());
        }
        let mut resolved = BTreeMap::new();
        for path in paths {
            let value_type = if path == PropertyPath::id() { Some(ValueType::EntityId) } else { types.value_type(&path) };
            resolved.insert(path, value_type.ok_or(TreeError::InvalidKey("unknown property type"))?);
        }
        let clauses = clauses(&selection.predicate, &resolved)?;
        let mut order = Vec::new();
        let mut seen = BTreeSet::new();
        for item in selection.order_by.iter().flatten() {
            if seen.insert(item.path.clone()) {
                let mut part = part(&item.path, resolved[&item.path]);
                part.direction = match item.direction {
                    OrderDirection::Asc => IndexDirection::Asc,
                    OrderDirection::Desc => IndexDirection::Desc,
                };
                order.push(part);
            }
        }
        Ok(Self { clauses, types: resolved, order: KeySpec::new(order), limit: selection.limit })
    }

    pub(crate) fn pinned(&self, path: &PropertyPath) -> Option<&[u8]> {
        let value = singleton(self.clauses.first()?.fields.get(path)?)?;
        self.clauses.iter().all(|c| c.fields.get(path).and_then(singleton) == Some(value)).then_some(value)
    }
}

impl TreeSpec {
    /// Choose a new layout: equality-pinned paths first; under a limit, the
    /// nonconstant ordering paths; otherwise one constrained range path, with
    /// ORDER BY breaking ties. Entity identity is always the final address part.
    pub fn for_selection(selection: &Selection<Resolved>, types: &impl PropertyTypes) -> Result<Self, TreeError> {
        let request = TreeRequest::for_selection(selection, types)?;
        let mut paths: Vec<_> =
            request.types.keys().filter(|p| p.property_id() != PropertyId::Id && request.pinned(p).is_some()).cloned().collect();
        if request.limit.is_some() {
            for key in &request.order.keyparts {
                let path = path(key);
                if path == PropertyPath::id() {
                    break;
                }
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        } else {
            let ranges: BTreeSet<_> = request
                .clauses
                .iter()
                .flat_map(|c| c.fields.keys())
                .filter(|p| p.property_id() != PropertyId::Id && !paths.contains(p))
                .cloned()
                .collect();
            let ordered = request.order.keyparts.iter().map(path).find(|p| ranges.contains(p));
            if let Some(range) = ordered.or_else(|| ranges.into_iter().next()) {
                paths.push(range);
            }
        }
        let parts = paths.iter().map(|p| part(p, request.types[p])).collect();
        Ok(Self::new(KeySpec::new(parts), selection.predicate.clone()))
    }

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

pub(crate) fn prefix_range(prefix: &[u8]) -> TreeSpan {
    TreeSpan::new(Included(prefix.to_vec()), prefix_end(prefix).map_or(Unbounded, Excluded))
}
pub(crate) fn path(part: &IndexKeyPart<PropertyId>) -> PropertyPath {
    let mut path = PropertyPath::from(part.key);
    path.subpath = part.sub_path.clone().unwrap_or_default();
    path
}
fn part(path: &PropertyPath, value_type: ValueType) -> IndexKeyPart<PropertyId> {
    let mut part = IndexKeyPart::asc(path.property_id(), value_type);
    part.sub_path = (!path.subpath.is_empty()).then(|| path.subpath.clone());
    part
}

fn clauses(predicate: &Predicate<Resolved>, types: &BTreeMap<PropertyPath, ValueType>) -> Result<Vec<Clause>, TreeError> {
    match predicate {
        Predicate::True => Ok(vec![Clause::default()]),
        Predicate::False => Ok(vec![]),
        Predicate::And(a, b) => {
            let (a, b) = (clauses(a, types)?, clauses(b, types)?);
            if a.len().saturating_mul(b.len()) > 256 {
                return Err(TreeError::UnsupportedSelection);
            }
            let mut out = Vec::new();
            for a in a {
                for b in &b {
                    let mut merged = a.clone();
                    merged.opaque.extend(b.opaque.iter().cloned());
                    let mut possible = true;
                    for (path, domain) in &b.fields {
                        let domain = match merged.fields.get(path) {
                            Some(previous) => intersect(previous, domain),
                            None => Some(domain.clone()),
                        };
                        match domain {
                            Some(domain) => {
                                merged.fields.insert(path.clone(), domain);
                            }
                            None => {
                                possible = false;
                                break;
                            }
                        }
                    }
                    if possible {
                        out.push(merged);
                    }
                }
            }
            Ok(out)
        }
        Predicate::Or(a, b) => {
            let mut out = clauses(a, types)?;
            out.extend(clauses(b, types)?);
            if out.len() > 256 {
                return Err(TreeError::UnsupportedSelection);
            }
            Ok(out)
        }
        Predicate::Comparison { left, operator, right } => {
            let (path, value, op) = match (left.as_ref(), right.as_ref()) {
                (Expr::Path(path), value) => (path, value, operator.clone()),
                (value, Expr::Path(path)) => (
                    path,
                    value,
                    match operator {
                        Op::GreaterThan => Op::LessThan,
                        Op::GreaterThanOrEqual => Op::LessThanOrEqual,
                        Op::LessThan => Op::GreaterThan,
                        Op::LessThanOrEqual => Op::GreaterThanOrEqual,
                        Op::Equal => Op::Equal,
                        Op::NotEqual => Op::NotEqual,
                        _ => return Ok(opaque(predicate)),
                    },
                ),
                _ => return Ok(opaque(predicate)),
            };
            let part = part(path, *types.get(path).ok_or(TreeError::InvalidKey("unknown predicate type"))?);
            let encode = |expr: &Expr<Resolved>| match expr {
                Expr::Literal(v) => encode_part(&part, Some(v)),
                _ => Err(TreeError::UnsupportedSelection),
            };
            let nonnull = Included(vec![1]);
            let domain = match (&op, value) {
                (Op::In, Expr::ExprList(values)) => {
                    let mut out = Vec::new();
                    for value in values {
                        let bytes = encode(value)?;
                        out.push(field(path, Included(bytes.clone()), Included(bytes)));
                    }
                    if out.len() > 256 {
                        return Err(TreeError::UnsupportedSelection);
                    }
                    return Ok(out);
                }
                (Op::Between, Expr::ExprList(values)) if values.len() == 2 => {
                    TreeSpan::new(Included(encode(&values[0])?), Included(encode(&values[1])?))
                }
                (_, Expr::Literal(_)) => {
                    let bytes = encode(value)?;
                    match op {
                        Op::Equal => TreeSpan::new(Included(bytes.clone()), Included(bytes)),
                        Op::NotEqual => {
                            return Ok(vec![field(path, nonnull, Excluded(bytes.clone())), field(path, Excluded(bytes), Unbounded)])
                        }
                        Op::GreaterThan => TreeSpan::new(Excluded(bytes), Unbounded),
                        Op::GreaterThanOrEqual => TreeSpan::new(Included(bytes), Unbounded),
                        Op::LessThan => TreeSpan::new(nonnull, Excluded(bytes)),
                        Op::LessThanOrEqual => TreeSpan::new(nonnull, Included(bytes)),
                        _ => return Ok(opaque(predicate)),
                    }
                }
                _ => return Ok(opaque(predicate)),
            };
            if crate::bounds::empty(&domain) {
                Ok(vec![])
            } else {
                Ok(vec![field(path, domain.start, domain.end)])
            }
        }
        Predicate::IsNull(expr) => match expr.as_ref() {
            Expr::Path(path) if *path == PropertyPath::id() => Ok(vec![]),
            Expr::Path(path) => Ok(vec![field(path, Included(vec![0]), Included(vec![0]))]),
            _ => Ok(opaque(predicate)),
        },
        _ => Ok(opaque(predicate)),
    }
}
fn field(path: &PropertyPath, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Clause {
    Clause { fields: BTreeMap::from([(path.clone(), TreeSpan::new(start, end))]), opaque: BTreeSet::new() }
}
fn opaque(predicate: &Predicate<Resolved>) -> Vec<Clause> {
    let normalized = ankql::selection::map_references::<Resolved, Resolved>(
        &Selection::from(predicate.clone()),
        &|p| {
            let mut path = PropertyPath::from(p.property_id());
            path.subpath = p.subpath.clone();
            path
        },
        &|id| *id,
    );
    vec![Clause { fields: BTreeMap::new(), opaque: BTreeSet::from([bincode::serialize(&normalized.predicate).unwrap()]) }]
}
fn collect_paths(predicate: &Predicate<Resolved>, out: &mut BTreeSet<PropertyPath>) {
    match predicate {
        Predicate::And(a, b) | Predicate::Or(a, b) => {
            collect_paths(a, out);
            collect_paths(b, out);
        }
        Predicate::Not(p) => collect_paths(p, out),
        Predicate::Comparison { left, right, .. } => {
            collect_expr(left, out);
            collect_expr(right, out);
        }
        Predicate::IsNull(value) => collect_expr(value, out),
        _ => {}
    }
}

fn collect_expr(expr: &Expr<Resolved>, out: &mut BTreeSet<PropertyPath>) {
    match expr {
        Expr::Path(path) => {
            out.insert(path.clone());
        }
        Expr::Predicate(predicate) => collect_paths(predicate, out),
        Expr::InfixExpr { left, right, .. } => {
            collect_expr(left, out);
            collect_expr(right, out);
        }
        Expr::ExprList(values) => {
            for value in values {
                collect_expr(value, out);
            }
        }
        _ => {}
    }
}
