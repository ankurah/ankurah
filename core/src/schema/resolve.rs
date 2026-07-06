//! The resolution pass: `PathExpr` -> `Identifier` (RFC 5.3, AC4/AC5).
//!
//! Resolution binds a property reference's first step against the catalog
//! map, producing the resolved `Expr::Identifier` form and failing CLOSED
//! (`PropertyError::UnknownProperty`) for references nothing defines. This
//! replaces the three inconsistent missing-property behaviors with one
//! rule: an unresolvable reference fails at build time; a resolvable
//! reference whose property is absent on a given entity evaluates under
//! the RFC 5.4 read rules.
//!
//! Wired (since the protocol v2 epoch) at the four query ORIGIN sites --
//! context fetch, `EntityLiveQuery`, and the node-level local-fetch and
//! remote-subscribe paths -- via [`CatalogManager::resolve_selection_deferred`];
//! receivers pass already-resolved selections through (the pass is
//! idempotent, and `Identifier` expressions are left untouched).
//!
//! Special cases:
//! - `id` is the entity-id pseudo-property, not a catalog property: it
//!   stays a `PathExpr` untouched.
//! - The legacy collection-qualified form (`album.name` queried against
//!   the `album` collection) is NORMALIZED away: the qualifier is dropped
//!   and the remainder resolves as the property (+ subpath). A bare
//!   single-step path equal to the collection name resolves as a property
//!   first (a property may legitimately share its collection's name).

use ankql::ast::{Expr, Identifier, OrderByItem, PathExpr, Predicate, Selection};
use ankurah_proto::CollectionId;

use crate::policy::PolicyAgent;
use crate::property::PropertyError;
use crate::schema::catalog::CatalogManager;
use crate::storage::StorageEngine;

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Resolve a selection with the Phase A CATALOG-LAG DEFERRAL rule (RFC
    /// 5.5): attempt [`resolve_selection`]; if it fails `UnknownProperty`
    /// only because the catalog is not ready yet, await readiness and retry
    /// ONCE, then propagate. A compiled model resolves immediately via the
    /// `cache_compiled` overlay, so this awaits only when a reference is
    /// genuinely unknown on a not-yet-warm catalog. Any non-`UnknownProperty`
    /// error, or an `UnknownProperty` when the catalog IS ready (a real
    /// unknown reference), propagates without waiting (fail closed).
    pub async fn resolve_selection_deferred(&self, collection: &CollectionId, selection: &Selection) -> Result<Selection, PropertyError> {
        match self.resolve_selection(collection, selection) {
            Ok(resolved) => Ok(resolved),
            Err(PropertyError::UnknownProperty { collection: c, name }) => {
                if self.is_catalog_ready() {
                    // Catalog is warm and still cannot resolve it: a real
                    // unknown reference. Fail closed.
                    return Err(PropertyError::UnknownProperty { collection: c, name });
                }
                // Data may have outrun metadata: wait for the catalog to warm,
                // then retry exactly once.
                self.wait_catalog_ready().await;
                self.resolve_selection(collection, selection)
            }
            Err(other) => Err(other),
        }
    }

    /// Resolve every property reference in `selection` against the catalog
    /// for `collection`. Errors with `UnknownProperty` on the first
    /// reference nothing defines (fail closed, AC5). Already-resolved
    /// `Identifier` expressions pass through untouched, so the pass is
    /// idempotent.
    pub fn resolve_selection(&self, collection: &CollectionId, selection: &Selection) -> Result<Selection, PropertyError> {
        let order_by = match &selection.order_by {
            Some(items) => Some(items.iter().map(|item| self.resolve_order_by(collection, item)).collect::<Result<Vec<_>, _>>()?),
            None => None,
        };
        Ok(Selection { predicate: self.resolve_predicate(collection, &selection.predicate)?, order_by, limit: selection.limit })
    }

    /// Sort keys resolve under the SAME rules as predicate references
    /// (fail-closed UnknownProperty, `id` pseudo-property passthrough,
    /// collection-qualifier normalization) but KEEP the PathExpr
    /// representation: sorting addresses the resolved display name (the SQL
    /// column / materialized key); id-keyed sort addressing rides the
    /// catalog-bound engine work (#312).
    fn resolve_order_by(&self, collection: &CollectionId, item: &OrderByItem) -> Result<OrderByItem, PropertyError> {
        let steps = &item.path.steps;
        if steps.is_empty() || steps[0] == "id" || self.resolve(collection.as_str(), &steps[0]).is_some() {
            return Ok(item.clone());
        }
        // Legacy collection-qualified form: strip the qualifier.
        if steps.len() > 1 && steps[0] == collection.as_str() {
            if steps[1] == "id" || self.resolve(collection.as_str(), &steps[1]).is_some() {
                return Ok(OrderByItem { path: PathExpr { steps: steps[1..].to_vec() }, direction: item.direction.clone() });
            }
            return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[1].clone() });
        }
        Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[0].clone() })
    }

    fn resolve_predicate(&self, collection: &CollectionId, predicate: &Predicate) -> Result<Predicate, PropertyError> {
        Ok(match predicate {
            Predicate::Comparison { left, operator, right } => Predicate::Comparison {
                left: Box::new(self.resolve_expr(collection, left)?),
                operator: operator.clone(),
                right: Box::new(self.resolve_expr(collection, right)?),
            },
            Predicate::And(a, b) => {
                Predicate::And(Box::new(self.resolve_predicate(collection, a)?), Box::new(self.resolve_predicate(collection, b)?))
            }
            Predicate::Or(a, b) => {
                Predicate::Or(Box::new(self.resolve_predicate(collection, a)?), Box::new(self.resolve_predicate(collection, b)?))
            }
            Predicate::Not(inner) => Predicate::Not(Box::new(self.resolve_predicate(collection, inner)?)),
            Predicate::IsNull(expr) => Predicate::IsNull(Box::new(self.resolve_expr(collection, expr)?)),
            Predicate::True | Predicate::False | Predicate::Placeholder => predicate.clone(),
        })
    }

    fn resolve_expr(&self, collection: &CollectionId, expr: &Expr) -> Result<Expr, PropertyError> {
        Ok(match expr {
            Expr::Path(path) => {
                let steps = &path.steps;
                if steps.is_empty() {
                    return Ok(expr.clone());
                }
                // The entity-id pseudo-property is not a catalog property.
                if steps[0] == "id" {
                    return Ok(expr.clone());
                }
                // Try the first step as a property of this collection.
                if let Some(property) = self.resolve(collection.as_str(), &steps[0]) {
                    return Ok(Expr::Identifier(Identifier {
                        property: property.to_ulid(),
                        name: steps[0].clone(),
                        subpath: steps[1..].to_vec(),
                    }));
                }
                // Legacy collection-qualified form: strip the qualifier and
                // resolve the remainder.
                if steps.len() > 1 && steps[0] == collection.as_str() {
                    if steps[1] == "id" {
                        return Ok(expr.clone());
                    }
                    if let Some(property) = self.resolve(collection.as_str(), &steps[1]) {
                        return Ok(Expr::Identifier(Identifier {
                            property: property.to_ulid(),
                            name: steps[1].clone(),
                            subpath: steps[2..].to_vec(),
                        }));
                    }
                    return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[1].clone() });
                }
                return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[0].clone() });
            }
            // Already resolved: idempotent pass-through.
            Expr::Identifier(_) | Expr::Literal(_) | Expr::Placeholder => expr.clone(),
            Expr::ExprList(exprs) => Expr::ExprList(exprs.iter().map(|e| self.resolve_expr(collection, e)).collect::<Result<Vec<_>, _>>()?),
            Expr::Predicate(p) => Expr::Predicate(self.resolve_predicate(collection, p)?),
            Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
                left: Box::new(self.resolve_expr(collection, left)?),
                operator: operator.clone(),
                right: Box::new(self.resolve_expr(collection, right)?),
            },
        })
    }
}
