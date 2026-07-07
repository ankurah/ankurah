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
    /// only because the catalog is not warm yet, warm it and retry ONCE,
    /// then propagate. Rev 4: ids exist only in the catalog and its
    /// registration responses (the compiled-schema overlay is gone), so
    /// resolution is the point where a cold catalog must actually get
    /// warmed:
    ///
    /// - DURABLE node: the storage warm always completes; await readiness.
    /// - EPHEMERAL node with `cdata`: the subscription may never have been
    ///   kicked (the sync `Node::context` cannot await it), so KICK it here,
    ///   awaited inline -- [`CatalogManager::ensure_subscribed`] is
    ///   idempotent and returns with readiness set on success. If the
    ///   catalog is still not ready afterwards (no durable peer, subscribe
    ///   failed), FAIL CLOSED rather than wait for a warm that cannot
    ///   arrive.
    /// - EPHEMERAL node without `cdata` (the update path of an
    ///   already-registered query): the original subscribe already kicked
    ///   the catalog, so awaiting readiness is bounded.
    ///
    /// If the WARM catalog still cannot resolve the reference and the
    /// binary carries a compiled schema for the collection, the model
    /// REGISTERS AT FIRST USE ([`Self::register_first_use`], REN 2 revised)
    /// and resolution retries against the authoritative rows fed by the
    /// response. Only when that is impossible or refused does the
    /// anticipated-collection rule (RFC 5.3) classify the miss for the
    /// caller to defer or answer empty.
    ///
    /// Any non-`UnknownProperty` error, or a remaining `UnknownProperty`
    /// that classification does not soften (a real unknown reference),
    /// propagates without waiting (fail closed, AC5).
    pub async fn resolve_selection_deferred(
        &self,
        node: &crate::node::Node<SE, PA>,
        cdata: Option<&PA::ContextData>,
        collection: &CollectionId,
        selection: &Selection,
    ) -> Result<Selection, PropertyError> {
        match self.resolve_selection(collection, selection) {
            Ok(resolved) => Ok(resolved),
            Err(PropertyError::UnknownProperty { collection: c, name }) => {
                if !self.is_catalog_ready() {
                    if self.is_durable() {
                        // The storage warm is in flight and always completes.
                        self.wait_catalog_ready().await;
                    } else if let Some(cdata) = cdata {
                        self.ensure_subscribed(cdata.clone(), node).await;
                        if !self.is_catalog_ready() {
                            // The kick could not warm the catalog (offline or
                            // subscribe failure). First-use registration may
                            // still succeed -- it needs a durable peer, not a
                            // working subscription -- and its response feeds
                            // the map directly. Otherwise fail closed: with a
                            // cold catalog nothing can be proven about the
                            // collection.
                            if self.register_first_use(node, Some(cdata), collection).await {
                                return match self.resolve_selection(collection, selection) {
                                    Err(PropertyError::UnknownProperty { collection: c, name }) => {
                                        Err(self.classify_unknown(collection, c, name))
                                    }
                                    other => other,
                                };
                            }
                            return Err(PropertyError::UnknownProperty { collection: c, name });
                        }
                    } else {
                        self.wait_catalog_ready().await;
                    }
                }
                match self.resolve_selection(collection, selection) {
                    Err(PropertyError::UnknownProperty { collection: c, name }) => {
                        // Warm catalog, still unresolvable. First-use
                        // registration (REN 2 revised, plan decision 25b): a
                        // compiled model registers at first use -- an
                        // idempotent upsert that no-ops (zero events, policy
                        // verb skipped) when the catalog already carries the
                        // schema -- so a replica lagging the authority learns
                        // the rows synchronously from the response instead of
                        // misclassifying the collection as anticipated. If no
                        // registration is possible (no cdata, no compiled
                        // schema, already ensured, denied, offline), classify:
                        // a real unknown reference fails closed; an
                        // anticipated unregistered collection is the caller's
                        // to defer or answer empty.
                        if self.register_first_use(node, cdata, collection).await {
                            return match self.resolve_selection(collection, selection) {
                                Err(PropertyError::UnknownProperty { collection: c, name }) => {
                                    Err(self.classify_unknown(collection, c, name))
                                }
                                other => other,
                            };
                        }
                        Err(self.classify_unknown(collection, c, name))
                    }
                    other => other,
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Attempt first-use registration of `collection`'s compiled schema
    /// (rev 4 + REN 2 revision: reads may trigger the idempotent
    /// registration upsert; an existing schema is a no-op plan that emits
    /// nothing and skips the policy verb). Returns true only when a
    /// registration ran and the map was fed, so the caller should
    /// re-resolve. Returns false when no attempt applies (no cdata on this
    /// path, no compiled schema, or the collection is already ensured this
    /// process) or the attempt failed (policy denial, no durable peer) --
    /// those fall back to the anticipated-collection deferral semantics.
    async fn register_first_use(
        &self,
        node: &crate::node::Node<SE, PA>,
        cdata: Option<&PA::ContextData>,
        collection: &CollectionId,
    ) -> bool {
        let Some(cdata) = cdata else { return false };
        let Some(schema) = self.unensured_schema_for(collection.as_str()) else { return false };
        match self.ensure_registered(node, cdata, schema).await {
            Ok(()) => true,
            Err(e) => {
                tracing::debug!("first-use registration of '{}' failed; deferral semantics apply: {}", collection, e);
                false
            }
        }
    }

    /// Classify an `UnknownProperty` from a WARM catalog (rev 4, RFC 5.3):
    /// if the queried collection has no model in the catalog but this
    /// binary carries a compiled schema for it, the schema is ANTICIPATED
    /// and merely unregistered -- and an unregistered collection provably
    /// holds no entities (creation requires registration first), so callers
    /// can answer a fetch with an empty result or defer a live query until
    /// the collection registers. Anything else is a genuinely unresolvable
    /// reference: fail closed (AC5).
    fn classify_unknown(&self, collection: &CollectionId, c: String, name: String) -> PropertyError {
        if self.model_by_collection(collection.as_str()).is_none() && self.has_compiled(collection.as_str()) {
            PropertyError::UnregisteredCollection { collection: c }
        } else {
            PropertyError::UnknownProperty { collection: c, name }
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
