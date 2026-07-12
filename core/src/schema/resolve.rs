//! The resolution pass: `PathExpr` -> `Identifier` (RFC 5.3 in
//! specs/model-property-metadata/rfc.md).
//!
//! Resolution binds a property reference's first step against the catalog
//! map, producing the resolved `Expr::Identifier` form and failing CLOSED
//! (`PropertyError::UnknownProperty`) for references nothing defines. This
//! replaces the three inconsistent missing-property behaviors with one
//! rule: an unresolvable reference fails at build time; a resolvable
//! reference whose property is absent on a given entity evaluates as NULL in
//! predicates and policy.
//!
//! Wired in the protocol v3 epoch at the four query ORIGIN sites --
//! context fetch, `EntityLiveQuery`, and the node-level local-fetch and
//! remote-subscribe paths -- via [`CatalogManager::resolve_selection_deferred`];
//! receivers pass already-resolved selections through (the pass is
//! idempotent: `Identifier` expressions retain their ids, and resolved
//! `OrderByItem`s retain their ids while refreshing renamed display paths).
//!
//! Special cases:
//! - The frozen system/catalog bootstrap schemas remain name-keyed, so their
//!   selections retain `PathExpr` references rather than consulting the
//!   catalog for property identities it deliberately does not define.
//! - `id` is the entity-id pseudo-property, not a catalog property: it
//!   stays a `PathExpr` untouched.
//! - The legacy collection-qualified form (`album.name` queried against
//!   the `album` collection) is NORMALIZED away: the qualifier is dropped
//!   and the remainder resolves as the property (+ subpath). A bare
//!   single-step path equal to the collection name resolves as a property
//!   first (a property may legitimately share its collection's name).

use ankql::ast::{Expr, Identifier, OrderByItem, PathExpr, Predicate, Selection};
use ankurah_proto::{CollectionId, EntityId};

use crate::policy::PolicyAgent;
use crate::property::PropertyError;
use crate::schema::catalog::CatalogManager;
use crate::storage::StorageEngine;
use crate::value::{Value, ValueType};

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Resolve a selection after admitting its exact compiled model shape.
    /// Typed query origins pass `schema`; internal or already-resolved fetches
    /// pass `None`. Registration happens before name resolution so a catalog
    /// property that merely shares the requested spelling cannot bypass the
    /// declaring model's compatibility check.
    ///
    /// - DURABLE node: the storage warm always completes; await readiness.
    /// - EPHEMERAL node with `cdata`: the subscription may never have been
    ///   kicked (the sync `Node::context` cannot await it), so KICK it here,
    ///   awaited inline -- [`CatalogManager::ensure_subscribed`] is
    ///   idempotent and returns with readiness set on success. If the
    ///   catalog is still not ready afterwards (no durable peer, subscribe
    ///   failed), FAIL CLOSED rather than wait for a warm that cannot
    ///   arrive.
    /// - Node without `cdata` (the update path of an already-admitted typed
    ///   query): the retained exact binding is the admission proof. Resolve
    ///   from the state already held locally; an unknown property fails
    ///   immediately because this path cannot initiate a policy-aware warm.
    ///
    /// A registration response feeds the catalog map directly, so an
    /// ephemeral query can still resolve even if its background catalog
    /// subscription is unavailable. A reference absent after the one warm
    /// attempt is an ordinary `UnknownProperty` error.
    pub async fn resolve_selection_deferred(
        &self,
        node: &crate::node::Node<SE, PA>,
        cdata: Option<&PA::ContextData>,
        collection: &CollectionId,
        schema: Option<&'static crate::schema::ModelSchema>,
        selection: &Selection,
    ) -> Result<Selection, PropertyError> {
        if let Some(schema) = schema {
            if let Some(cdata) = cdata {
                self.ensure_schema_for_use(node, cdata, schema).await.map_err(|error| {
                    if self.model_by_collection(schema.collection).is_none() {
                        PropertyError::UnregisteredCollection { collection: schema.collection.to_string() }
                    } else {
                        PropertyError::RetrievalError(crate::error::RetrievalError::Other(error.to_string()))
                    }
                })?;
            } else if !self.has_schema_binding(schema) {
                return Err(PropertyError::UnregisteredCollection { collection: schema.collection.to_string() });
            }
        }

        match self.resolve_selection(collection, selection) {
            Ok(resolved) => return Ok(resolved),
            Err(error @ PropertyError::UnknownProperty { .. }) if schema.is_some() && cdata.is_none() => return Err(error),
            Err(PropertyError::UnknownProperty { .. }) if !self.is_catalog_ready() => {}
            Err(other) => return Err(other),
        }

        if self.is_durable() {
            self.wait_catalog_ready().await;
        } else if let Some(cdata) = cdata {
            self.ensure_subscribed(cdata.clone(), node).await;
        } else {
            self.wait_catalog_ready().await;
        }

        self.resolve_selection(collection, selection)
    }

    /// Resolve every property reference in `selection` against the catalog
    /// for `collection`. Errors with `UnknownProperty` on the first
    /// reference nothing defines (fail closed). Already-resolved
    /// `Identifier` expressions pass through untouched, so the pass is
    /// idempotent.
    pub fn resolve_selection(&self, collection: &CollectionId, selection: &Selection) -> Result<Selection, PropertyError> {
        // The system collection and the metadata catalog are the bootstrap
        // base case: their frozen schemas stay name-keyed and deliberately
        // have no property-definition entities of their own (RFC section 4).
        // Leaving their paths untouched also lets a PolicyAgent add a
        // property-based catalog filter without recursively trying to warm the
        // catalog that filter is meant to read.
        if collection.as_str() == crate::system::SYSTEM_COLLECTION_ID || crate::schema::is_catalog_collection(collection) {
            return Ok(selection.clone());
        }

        let order_by = match &selection.order_by {
            Some(items) => Some(items.iter().map(|item| self.resolve_order_by(collection, item)).collect::<Result<Vec<_>, _>>()?),
            None => None,
        };
        Ok(Selection { predicate: self.resolve_predicate(collection, &selection.predicate)?, order_by, limit: selection.limit })
    }

    /// Sort keys resolve under the SAME rules as predicate references
    /// (fail-closed UnknownProperty, `id` pseudo-property passthrough,
    /// collection-qualifier normalization). They keep the display path for
    /// SQL/physical-column addressing and also carry the stable property id.
    /// Re-resolving an already-resolved selection refreshes its display name
    /// when the catalog has renamed that property without losing identity.
    fn resolve_order_by(&self, collection: &CollectionId, item: &OrderByItem) -> Result<OrderByItem, PropertyError> {
        if let Some(property) = item.property {
            let mut resolved = item.clone();
            if let Some(definition) = self.property_by_id(&EntityId::from_bytes(property)) {
                if let Some(first) = resolved.path.steps.first_mut() {
                    *first = definition.name;
                }
            }
            return Ok(resolved);
        }

        let steps = &item.path.steps;
        if steps.is_empty() || steps[0] == "id" {
            return Ok(item.clone());
        }

        if let Some(property) = self.resolve(collection.as_str(), &steps[0]) {
            let mut resolved = item.clone();
            resolved.property = Some(property.to_bytes());
            return Ok(resolved);
        }

        // Legacy collection-qualified form: strip the qualifier.
        if steps.len() > 1 && steps[0] == collection.as_str() {
            if steps[1] == "id" {
                return Ok(OrderByItem {
                    path: PathExpr { steps: steps[1..].to_vec() },
                    direction: item.direction.clone(),
                    property: None,
                });
            }
            if let Some(property) = self.resolve(collection.as_str(), &steps[1]) {
                return Ok(OrderByItem {
                    path: PathExpr { steps: steps[1..].to_vec() },
                    direction: item.direction.clone(),
                    property: Some(property.to_bytes()),
                });
            }
            return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[1].clone() });
        }
        Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[0].clone() })
    }

    fn resolve_predicate(&self, collection: &CollectionId, predicate: &Predicate) -> Result<Predicate, PropertyError> {
        Ok(match predicate {
            Predicate::Comparison { left, operator, right } => {
                let left = self.resolve_expr(collection, left)?;
                let right = self.resolve_expr(collection, right)?;
                // Canonicalize literal operands to the compared property's
                // canonical value_type (rfc.md 5.6 as amended 2026-07-10):
                // stored values are canonically typed, and the reactor's
                // watcher index matches on collated bytes, so the literal
                // side must collate in the same type. Whole-property
                // comparisons only: a subpath addresses a JSON subfield,
                // whose inner type the catalog does not describe (the
                // TypeResolver Json heuristic keeps covering those). A
                // literal the canonical type cannot represent fails the
                // query loud at its origin, consistent with fail-closed
                // resolution.
                let (left, right) = self.canonicalize_comparison(left, right)?;
                Predicate::Comparison { left: Box::new(left), operator: operator.clone(), right: Box::new(right) }
            }
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

    /// The canonical cast target of a comparison side: a resolved
    /// whole-property `Identifier` (an empty subpath) whose canonical
    /// value_type the catalog knows and this build can parse. Subfield
    /// references and unresolved forms have no target.
    fn canonical_target(&self, expr: &Expr) -> Option<ValueType> {
        if let Expr::Identifier(ident) = expr {
            if ident.subpath.is_empty() {
                return self.canonical_value_type_of(&EntityId::from_bytes(ident.property)).and_then(|s| ValueType::from_property_str(&s));
            }
        }
        None
    }

    /// Cast the literal operands opposite a resolved whole-property reference
    /// to that property's canonical value_type (see [`Self::resolve_predicate`]).
    /// Identifier-vs-identifier and literal-vs-literal comparisons pass
    /// through unchanged.
    fn canonicalize_comparison(&self, left: Expr, right: Expr) -> Result<(Expr, Expr), PropertyError> {
        fn cast_literals(expr: Expr, target: ValueType) -> Result<Expr, PropertyError> {
            Ok(match expr {
                Expr::Literal(lit) => {
                    let value: Value = (&lit).into();
                    if ValueType::of(&value) == target {
                        Expr::Literal(lit)
                    } else {
                        Expr::Literal(value.cast_to(target)?.into())
                    }
                }
                Expr::ExprList(items) => {
                    Expr::ExprList(items.into_iter().map(|e| cast_literals(e, target)).collect::<Result<Vec<_>, _>>()?)
                }
                other => other,
            })
        }
        Ok(match (self.canonical_target(&left), self.canonical_target(&right)) {
            (Some(target), None) => {
                let right = cast_literals(right, target)?;
                (left, right)
            }
            (None, Some(target)) => {
                let left = cast_literals(left, target)?;
                (left, right)
            }
            _ => (left, right),
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
                        property: property.to_bytes(),
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
                            property: property.to_bytes(),
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
