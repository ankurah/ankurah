//! The request a Selection makes of a tree: its membership as clauses over
//! encoded property values, its order and its limit, and the property types
//! that resolve them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use ankql::ast::{ComparisonOperator as Op, Expr, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core_types::{IndexDirection, IndexKeyPart, KeySpec, PropertyId, PropertyPath, ValueType};

use super::spec::unlabeled;
use crate::address::encode_part;
use crate::address::TreeSpan;
use crate::error::TreeError;

/// Resolve the type of an exact property path without a node or a catalog.
pub trait PropertyTypes {
    /// The type at this path, or `None` when it is unknown.
    fn value_type(&self, path: &PropertyPath) -> Option<ValueType>;
}
impl PropertyTypes for HashMap<PropertyId, ValueType> {
    fn value_type(&self, path: &PropertyPath) -> Option<ValueType> {
        if !path.subpath.is_empty() {
            return None;
        }
        if path.property_id() == PropertyId::Id {
            return Some(ValueType::EntityId);
        }
        self.get(&path.property_id()).copied()
    }
}

/// One selection's membership, ordering, and optional limit. It does not
/// prescribe the layout of a tree that might already serve it.
#[derive(Debug, Clone)]
pub struct TreeRequest {
    pub(crate) clauses: Vec<Clause>,
    pub(crate) types: BTreeMap<PropertyPath, ValueType>,
    pub(crate) order: KeySpec<PropertyId>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Clause {
    pub(super) fields: BTreeMap<PropertyPath, TreeSpan>,
    pub(super) opaque: BTreeSet<Vec<u8>>,
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
                let mut part = IndexKeyPart::asc_on(&item.path, resolved[&item.path]);
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
        let value = self.clauses.first()?.fields.get(path)?.single()?;
        self.clauses.iter().all(|c| c.fields.get(path).and_then(TreeSpan::single) == Some(value)).then_some(value)
    }
}

pub(super) fn clauses(predicate: &Predicate<Resolved>, types: &BTreeMap<PropertyPath, ValueType>) -> Result<Vec<Clause>, TreeError> {
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
                            Some(previous) => previous.intersect(domain),
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
                        _ => return Ok(opaque_clause(predicate)),
                    },
                ),
                _ => return Ok(opaque_clause(predicate)),
            };
            let part = IndexKeyPart::asc_on(path, *types.get(path).ok_or(TreeError::InvalidKey("unknown predicate type"))?);
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
                        out.push(field_clause(path, Included(bytes.clone()), Included(bytes)));
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
                            return Ok(vec![
                                field_clause(path, nonnull, Excluded(bytes.clone())),
                                field_clause(path, Excluded(bytes), Unbounded),
                            ])
                        }
                        Op::GreaterThan => TreeSpan::new(Excluded(bytes), Unbounded),
                        Op::GreaterThanOrEqual => TreeSpan::new(Included(bytes), Unbounded),
                        Op::LessThan => TreeSpan::new(nonnull, Excluded(bytes)),
                        Op::LessThanOrEqual => TreeSpan::new(nonnull, Included(bytes)),
                        _ => return Ok(opaque_clause(predicate)),
                    }
                }
                _ => return Ok(opaque_clause(predicate)),
            };
            if domain.is_empty() {
                Ok(vec![])
            } else {
                Ok(vec![field_clause(path, domain.start, domain.end)])
            }
        }
        Predicate::IsNull(expr) => match expr.as_ref() {
            Expr::Path(path) if *path == PropertyPath::id() => Ok(vec![]),
            Expr::Path(path) => Ok(vec![field_clause(path, Included(vec![0]), Included(vec![0]))]),
            _ => Ok(opaque_clause(predicate)),
        },
        _ => Ok(opaque_clause(predicate)),
    }
}
fn field_clause(path: &PropertyPath, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Clause {
    Clause { fields: BTreeMap::from([(path.clone(), TreeSpan::new(start, end))]), opaque: BTreeSet::new() }
}
fn opaque_clause(predicate: &Predicate<Resolved>) -> Vec<Clause> {
    let opaque = bincode::serialize(&unlabeled(predicate)).expect("a predicate serializes");
    vec![Clause { fields: BTreeMap::new(), opaque: BTreeSet::from([opaque]) }]
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
