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
//! NOT yet wired into the query paths: fail-closed resolution becomes the
//! default together with the client-side registration lifecycle and the
//! protocol v2 epoch (there is no interim name-on-the-wire state, rev 3).
//! Until then this pass is invoked explicitly (and by tests).
//!
//! Special cases:
//! - `id` is the entity-id pseudo-property, not a catalog property: it
//!   stays a `PathExpr` untouched.
//! - The legacy collection-qualified form (`album.name` queried against
//!   the `album` collection) is NORMALIZED away: the qualifier is dropped
//!   and the remainder resolves as the property (+ subpath). A bare
//!   single-step path equal to the collection name resolves as a property
//!   first (a property may legitimately share its collection's name).

use ankql::ast::{Expr, Identifier, Predicate, Selection};
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
    /// Resolve every property reference in `selection` against the catalog
    /// for `collection`. Errors with `UnknownProperty` on the first
    /// reference nothing defines (fail closed, AC5). Already-resolved
    /// `Identifier` expressions pass through untouched, so the pass is
    /// idempotent.
    pub fn resolve_selection(&self, collection: &CollectionId, selection: &Selection) -> Result<Selection, PropertyError> {
        Ok(Selection {
            predicate: self.resolve_predicate(collection, &selection.predicate)?,
            order_by: selection.order_by.clone(),
            limit: selection.limit,
        })
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
