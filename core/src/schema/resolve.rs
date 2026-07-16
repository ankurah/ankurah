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

use ankql::ast::{Expr, OrderByItem, OrderKey, PathExpr, Predicate, PropertyPath, Selection};
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
            // These collections have no property-definition ids to resolve
            // against, but resolution must still leave no raw property Path for a
            // storage engine to read as a name: bind each reference to a `System`
            // identifier by name, and the `id` pseudo-property to its own identity.
            let order_by = match &selection.order_by {
                Some(items) => Some(items.iter().map(|item| systemize_order_by(collection, item)).collect::<Result<Vec<_>, _>>()?),
                None => None,
            };
            return Ok(Selection { predicate: systemize_predicate(collection, &selection.predicate)?, order_by, limit: selection.limit });
        }

        let order_by = match &selection.order_by {
            Some(items) => Some(items.iter().map(|item| self.resolve_order_by(collection, item)).collect::<Result<Vec<_>, _>>()?),
            None => None,
        };
        Ok(Selection { predicate: self.resolve_predicate(collection, &selection.predicate)?, order_by, limit: selection.limit })
    }

    /// Sort keys resolve under the SAME rules as predicate references
    /// (fail-closed UnknownProperty, `id` pseudo-property passthrough,
    /// collection-qualifier normalization), with one extra restriction: an
    /// ORDER BY key must name a WHOLE property. A JSON subpath is refused at
    /// resolution (see `subpath_order_error`) instead of producing a key
    /// nothing downstream orders by correctly. Resolved keys keep the display
    /// label for SQL/physical addressing and also carry the stable property id.
    /// Re-resolving an already-resolved key is an idempotent pass-through:
    /// identity is stable, and the Display label is a resolved-at snapshot that
    /// is deliberately NOT refreshed on a later rename (sorting addresses by id,
    /// not by label -- see the inline note below).
    fn resolve_order_by(&self, collection: &CollectionId, item: &OrderByItem) -> Result<OrderByItem, PropertyError> {
        // Already resolved: idempotent pass-through. Identity is stable; the
        // Display label is a resolved-at snapshot and is deliberately not
        // refreshed on re-resolution (sorting addresses by id, not by label).
        let path = match &item.key {
            OrderKey::Property(_) => return Ok(item.clone()),
            OrderKey::Path(path) => path,
        };
        let resolved = |key: PropertyPath| OrderByItem { key: OrderKey::Property(key), direction: item.direction.clone() };

        let steps = &path.steps;
        if steps.is_empty() {
            // Names no property: fails closed, as in `resolve_expr`.
            return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: String::new() });
        }
        // The `id` pseudo-property resolves to its own identity ([`PropertyId::Id`]).
        if steps[0] == "id" {
            whole_property_order(collection, path, &steps[1..])?;
            return Ok(resolved(PropertyPath::id(vec![])));
        }

        if let Some(property) = self.resolve(collection.as_str(), &steps[0]) {
            whole_property_order(collection, path, &steps[1..])?;
            return Ok(resolved(PropertyPath::registered(property.to_ulid(), steps[0].clone(), vec![])));
        }

        // Legacy collection-qualified form: strip the qualifier.
        if steps.len() > 1 && steps[0] == collection.as_str() {
            if steps[1] == "id" {
                whole_property_order(collection, path, &steps[2..])?;
                return Ok(resolved(PropertyPath::id(vec![])));
            }
            if let Some(property) = self.resolve(collection.as_str(), &steps[1]) {
                whole_property_order(collection, path, &steps[2..])?;
                return Ok(resolved(PropertyPath::registered(property.to_ulid(), steps[1].clone(), vec![])));
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
        if let Expr::PropertyIdentifier(ident) = expr {
            if ident.subpath.is_empty() {
                // Only registered properties have a catalog canonical type; a
                // system property has none, so it has no cast target.
                if let ankql::ast::PropertyId::EntityId(id) = ident.id_or_systemname() {
                    return self.canonical_value_type_of(&EntityId::from_ulid(id)).and_then(|s| ValueType::from_property_str(&s));
                }
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
                    // Names no property, so it resolves to nothing. Fails closed
                    // rather than passing through as a raw Path an engine would
                    // reject (`ankql::ast::Selection::check`).
                    return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: String::new() });
                }
                // The entity-id pseudo-property is not a catalog property; it
                // resolves to its own identity ([`PropertyId::Id`]), and only
                // whole: the entity id has no subfields to traverse.
                if steps[0] == "id" {
                    if steps.len() > 1 {
                        return Err(id_subpath_error(collection, path));
                    }
                    return Ok(Expr::PropertyIdentifier(PropertyPath::id(vec![])));
                }
                // Try the first step as a property of this collection.
                if let Some(property) = self.resolve(collection.as_str(), &steps[0]) {
                    return Ok(Expr::PropertyIdentifier(PropertyPath::registered(
                        property.to_ulid(),
                        steps[0].clone(),
                        steps[1..].to_vec(),
                    )));
                }
                // Legacy collection-qualified form: strip the qualifier and
                // resolve the remainder.
                if steps.len() > 1 && steps[0] == collection.as_str() {
                    if steps[1] == "id" {
                        if steps.len() > 2 {
                            return Err(id_subpath_error(collection, path));
                        }
                        return Ok(Expr::PropertyIdentifier(PropertyPath::id(vec![])));
                    }
                    if let Some(property) = self.resolve(collection.as_str(), &steps[1]) {
                        return Ok(Expr::PropertyIdentifier(PropertyPath::registered(
                            property.to_ulid(),
                            steps[1].clone(),
                            steps[2..].to_vec(),
                        )));
                    }
                    return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[1].clone() });
                }
                return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: steps[0].clone() });
            }
            // Already resolved: idempotent pass-through.
            Expr::PropertyIdentifier(_) | Expr::Literal(_) | Expr::Placeholder => expr.clone(),
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

// -- Subpath refusals --
//
// Two reference shapes are refused at resolution because nothing can ever
// serve them:
//  - an ORDER BY key with a JSON subpath. Downstream support is partial at
//    best (no planner index key addresses a subpath, and a comparator
//    falling back to the whole property would sort the entire JSON value),
//    and partially supported ordering returns wrong rows under LIMIT.
//    Refusing at the origin keeps every consumer honest.
//  - any subpath on the `id` pseudo-property: the entity id is an opaque
//    scalar with no subfields, in predicates and order keys alike.

/// Whether an order key names a whole property: `rest` is what remains of
/// the path after the property step, and anything there is a refused subpath.
fn whole_property_order(collection: &CollectionId, path: &PathExpr, rest: &[String]) -> Result<(), PropertyError> {
    if rest.is_empty() {
        return Ok(());
    }
    Err(subpath_order_error(collection, path))
}

fn subpath_order_error(collection: &CollectionId, path: &PathExpr) -> PropertyError {
    PropertyError::UnsupportedSubpath {
        collection: collection.to_string(),
        path: path.steps.join("."),
        reason: "ORDER BY keys name whole properties; JSON subpaths are not sortable".to_string(),
    }
}

fn id_subpath_error(collection: &CollectionId, path: &PathExpr) -> PropertyError {
    PropertyError::UnsupportedSubpath {
        collection: collection.to_string(),
        path: path.steps.join("."),
        reason: "the id pseudo-property is the entity id and has no subfields".to_string(),
    }
}

// -- System/catalog selections --
//
// These frozen collections have no property-definition ids, so their
// references cannot resolve to a `Registered` id. Resolution still binds them
// to `System` identifiers by name so that nothing downstream sees a raw
// property Path (the sole exception is the `id` pseudo-property). No catalog
// lookup and no literal canonicalization happen here; a `System` property has
// no catalog canonical type.

/// The resolved form of a name-addressed reference on a frozen collection:
/// the `id` pseudo-property resolves to its own identity, everything else to a
/// `System` property. This is the single decision for what a catalog field
/// resolves to, shared by the systemize pass below and by the allocator, which
/// hand-builds its own catalog predicates (`registration.rs`) rather than
/// going through [`Resolver::resolve_selection`].
pub(crate) fn system_property(name: &str, subpath: Vec<String>) -> PropertyPath {
    if name == "id" {
        return PropertyPath::id(subpath);
    }
    PropertyPath::system(name.to_string(), subpath)
}

fn systemize_predicate(collection: &CollectionId, predicate: &Predicate) -> Result<Predicate, PropertyError> {
    Ok(match predicate {
        Predicate::Comparison { left, operator, right } => Predicate::Comparison {
            left: Box::new(systemize_expr(collection, left)?),
            operator: operator.clone(),
            right: Box::new(systemize_expr(collection, right)?),
        },
        Predicate::And(a, b) => {
            Predicate::And(Box::new(systemize_predicate(collection, a)?), Box::new(systemize_predicate(collection, b)?))
        }
        Predicate::Or(a, b) => Predicate::Or(Box::new(systemize_predicate(collection, a)?), Box::new(systemize_predicate(collection, b)?)),
        Predicate::Not(inner) => Predicate::Not(Box::new(systemize_predicate(collection, inner)?)),
        Predicate::IsNull(expr) => Predicate::IsNull(Box::new(systemize_expr(collection, expr)?)),
        Predicate::True | Predicate::False | Predicate::Placeholder => predicate.clone(),
    })
}

fn systemize_expr(collection: &CollectionId, expr: &Expr) -> Result<Expr, PropertyError> {
    Ok(match expr {
        Expr::Path(path) => systemize_path(collection, path)?,
        Expr::ExprList(exprs) => Expr::ExprList(exprs.iter().map(|e| systemize_expr(collection, e)).collect::<Result<Vec<_>, _>>()?),
        Expr::Predicate(p) => Expr::Predicate(systemize_predicate(collection, p)?),
        Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
            left: Box::new(systemize_expr(collection, left)?),
            operator: operator.clone(),
            right: Box::new(systemize_expr(collection, right)?),
        },
        // Already an identifier, a literal, or a placeholder: unchanged.
        Expr::PropertyIdentifier(_) | Expr::Literal(_) | Expr::Placeholder => expr.clone(),
    })
}

/// Strip the legacy collection-qualified form (`_ankurah_model.name` queried
/// against `_ankurah_model`), the same qualifier the registered resolver
/// strips in `resolve_expr`. The registered resolver tries the first step as
/// a property before falling back to the qualifier reading; the frozen
/// collections define a fixed field set that never includes a name equal to
/// the collection itself, so the qualifier reading is unambiguous here.
fn strip_collection_qualifier<'a>(collection: &CollectionId, steps: &'a [String]) -> &'a [String] {
    if steps.len() > 1 && steps[0] == collection.as_str() {
        &steps[1..]
    } else {
        steps
    }
}

/// Map a raw path to its resolved form (first step is the property, the rest is
/// its JSON sub-path). A path naming no property at all resolves to nothing and
/// fails closed, so that a successful resolution leaves no raw Path behind for
/// an engine to reject (`ankql::ast::Selection::check`). A subpath on the `id`
/// pseudo-property is refused, as in `resolve_expr`.
fn systemize_path(collection: &CollectionId, path: &PathExpr) -> Result<Expr, PropertyError> {
    let steps = strip_collection_qualifier(collection, &path.steps);
    let Some(name) = steps.first() else {
        return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: String::new() });
    };
    if name == "id" && steps.len() > 1 {
        return Err(id_subpath_error(collection, path));
    }
    Ok(Expr::PropertyIdentifier(system_property(name, steps[1..].to_vec())))
}

/// Sort keys systemize under the same rules as predicate references
/// (collection-qualifier normalization included), plus the whole-property
/// ORDER BY restriction (see `subpath_order_error`).
fn systemize_order_by(collection: &CollectionId, item: &OrderByItem) -> Result<OrderByItem, PropertyError> {
    let key = match &item.key {
        OrderKey::Path(path) => {
            let steps = strip_collection_qualifier(collection, &path.steps);
            let Some(name) = steps.first() else {
                return Err(PropertyError::UnknownProperty { collection: collection.to_string(), name: String::new() });
            };
            whole_property_order(collection, path, &steps[1..])?;
            OrderKey::Property(system_property(name, vec![]))
        }
        // Already resolved.
        OrderKey::Property(_) => item.key.clone(),
    };
    Ok(OrderByItem { key, direction: item.direction.clone() })
}
