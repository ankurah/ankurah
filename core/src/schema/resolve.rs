//! The resolution pass: `PathExpr` -> `Identifier` (RFC 5.3 in specs/model-property-metadata/rfc.md, AC4/AC5).
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
//! - `id` is the entity-id pseudo-property, not a catalog property: it
//!   stays a `PathExpr` untouched.
//! - The legacy collection-qualified form (`album.name` queried against
//!   the `album` collection) is NORMALIZED away: the qualifier is dropped
//!   and the remainder resolves as the property (+ subpath). A bare
//!   single-step path equal to the collection name resolves as a property
//!   first (a property may legitimately share its collection's name).

use ankql::ast::{Expr, Identifier, OrderByItem, PathExpr, Predicate, Selection};
use ankurah_proto::{CollectionId, EntityId};
use std::sync::{atomic::AtomicBool, Arc};

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
    /// response. The catalog map is a CACHE; registration is the source of
    /// truth and the doubt-resolver. When registration cannot run either
    /// (policy denial, no durable peer), the miss surfaces LOUD as
    /// [`PropertyError::UnregisteredCollection`] -- callers error rather
    /// than idle.
    ///
    /// Any other error propagates without waiting (fail closed, AC5).
    pub async fn resolve_selection_deferred(
        &self,
        node: &crate::node::Node<SE, PA>,
        cdata: Option<&PA::ContextData>,
        collection: &CollectionId,
        selection: &Selection,
    ) -> Result<Selection, PropertyError> {
        let generation = node.entities.system_generation();
        self.resolve_selection_deferred_in_generation(node, cdata, collection, selection, &generation).await
    }

    /// Generation-pinned form used when an outer operation (such as
    /// `Context::fetch_entities`) already captured its exact token before
    /// entering predicate resolution.
    pub(crate) async fn resolve_selection_deferred_in_generation(
        &self,
        node: &crate::node::Node<SE, PA>,
        cdata: Option<&PA::ContextData>,
        collection: &CollectionId,
        selection: &Selection,
        generation: &Arc<AtomicBool>,
    ) -> Result<Selection, PropertyError> {
        // Predicate first-use is an operation in the same sense as a
        // transaction: every retry belongs to the system generation in
        // which resolution began, even if catalog warm-up or registration
        // awaits a peer and a reset completes meanwhile.
        match self.resolve_selection_in_generation(node, generation, collection, selection).await {
            Ok(resolved) => Ok(resolved),
            Err(PropertyError::UnknownProperty { collection: c, name }) => {
                if !self.is_catalog_ready() {
                    if self.is_durable() {
                        // The storage warm is in flight and always completes.
                        self.wait_catalog_ready_in_generation(node, generation).await.map_err(|_| PropertyError::SystemReset)?;
                    } else if let Some(cdata) = cdata {
                        self.ensure_subscribed(cdata.clone(), node).await;
                        if !self.is_catalog_ready() {
                            // The kick could not warm the catalog (offline or
                            // subscribe failure). First-use registration may
                            // still succeed -- it needs a durable peer, not a
                            // working subscription -- and its response feeds
                            // the map directly. Otherwise classify exactly as
                            // the warm path does: a compiled-but-unregisterable
                            // collection surfaces the loud UnregisteredCollection
                            // error (RFC 5.3), anything else fails closed.
                            if self.register_first_use(node, Some(cdata), collection, generation).await? {
                                return match self.resolve_selection_in_generation(node, generation, collection, selection).await {
                                    Err(PropertyError::UnknownProperty { collection: c, name }) => {
                                        Err(self.classify_unknown(collection, c, name))
                                    }
                                    other => other,
                                };
                            }
                            return Err(self.classify_unknown(collection, c, name));
                        }
                    } else {
                        self.wait_catalog_ready_in_generation(node, generation).await.map_err(|_| PropertyError::SystemReset)?;
                    }
                }
                match self.resolve_selection_in_generation(node, generation, collection, selection).await {
                    Err(PropertyError::UnknownProperty { collection: c, name }) => {
                        // Warm catalog, still unresolvable. First-use
                        // registration (REN 2 revised, plan decision 25b): a
                        // compiled model registers at first use -- an
                        // idempotent upsert that no-ops (zero events, policy
                        // verb skipped) when the catalog already carries the
                        // schema -- so a replica lagging the authority learns
                        // the rows synchronously from the response. If no
                        // registration is possible (no cdata, no compiled
                        // schema, already ensured, denied, offline), classify:
                        // a real unknown reference fails closed, and a
                        // compiled-but-unregisterable collection surfaces as
                        // the loud UnregisteredCollection error.
                        if self.register_first_use(node, cdata, collection, generation).await? {
                            return match self.resolve_selection_in_generation(node, generation, collection, selection).await {
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

    /// Resolve while holding a short exact-generation lease. The lease never
    /// spans catalog warm-up or peer I/O; each retry reacquires it, turning a
    /// reset during an await into `SystemReset` rather than an ABA read from
    /// a newly populated catalog map.
    async fn resolve_selection_in_generation(
        &self,
        node: &crate::node::Node<SE, PA>,
        generation: &Arc<AtomicBool>,
        collection: &CollectionId,
        selection: &Selection,
    ) -> Result<Selection, PropertyError> {
        let _generation_guard = node.system.guard_generation(generation).await.map_err(|_| PropertyError::SystemReset)?;
        self.resolve_selection(collection, selection)
    }

    /// Attempt first-use registration of `collection`'s compiled schema
    /// (rev 4 + REN 2 revision: reads may trigger the idempotent
    /// registration upsert; an existing schema is a no-op plan that emits
    /// nothing and skips the policy verb). Returns true only when a
    /// registration ran and the map was fed, so the caller should
    /// re-resolve. Returns false when no attempt applies (no cdata on this
    /// path, no compiled schema, or that compiled shape is already ensured this
    /// process) or the attempt failed (policy denial, no durable peer) --
    /// those surface as `UnregisteredCollection` via `classify_unknown`.
    async fn register_first_use(
        &self,
        node: &crate::node::Node<SE, PA>,
        cdata: Option<&PA::ContextData>,
        collection: &CollectionId,
        generation: &Arc<AtomicBool>,
    ) -> Result<bool, PropertyError> {
        let Some(cdata) = cdata else { return Ok(false) };
        let schemas = {
            let _generation_guard = node.system.guard_generation(generation).await.map_err(|_| PropertyError::SystemReset)?;
            self.unensured_schemas_for(collection.as_str())
        };
        if schemas.is_empty() {
            let _generation_guard = node.system.guard_generation(generation).await.map_err(|_| PropertyError::SystemReset)?;
            return Ok(false);
        }
        let mut registered_any = false;
        for schema in schemas {
            match self.ensure_registered_in_generation(node, cdata, schema, generation).await {
                Ok(()) => registered_any = true,
                Err(crate::schema::registration::RegistrationError::Mutation(crate::error::MutationError::SystemReset)) => {
                    return Err(PropertyError::SystemReset);
                }
                Err(e) => {
                    tracing::debug!("first-use registration of '{}' failed; unresolved references still surface loud: {}", collection, e);
                }
            }
        }
        let _generation_guard = node.system.guard_generation(generation).await.map_err(|_| PropertyError::SystemReset)?;
        Ok(registered_any)
    }

    /// Classify an `UnknownProperty` from a WARM catalog (rev 4, RFC 5.3):
    /// if the queried collection has no model in the catalog but this
    /// binary carries a compiled schema for it, the reference failed
    /// DESPITE first-use registration being the resolution path --
    /// registration was denied or unreachable -- and the caller surfaces
    /// `UnregisteredCollection` as a loud, actionable error. Anything else
    /// is a genuinely unresolvable reference: fail closed (AC5).
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
