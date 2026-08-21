//! Selection resolution: the model-scoped name lookup, and the one walk that
//! carries a selection from names to durable identities.
//!
//! A query is written in names and enters the system exactly once through
//! [`resolve_selection`], the ONLY door from [`Parsed`] to [`Resolved`].
//! Downstream of that door -- filtering, the reactor, storage, the wire --
//! everything addresses properties by [`PropertyId`] alone. Each name's
//! lookup answers with the property's registered type as well as its
//! identity, and the walk casts that comparison's literals to the type on the
//! spot, so no consumer downstream has to guess what a literal was compared
//! against.
//!
//! Two [`ModelResolver`] implementations serve the two ways a query names a
//! model. A query made with a compiled declaration in hand (`fetch::<R>`,
//! `query::<R>`) binds field names through the descriptor's cells, so the
//! binding is fixed per epoch and a display-name change cannot re-aim a
//! running query. A query that names a collection by string alone (relay and
//! policy paths) binds through the catalog map's current display names. Both
//! resolutions fail closed on unknown names
//! ([`ModelResolutionError::UnknownProperty`]), never treating a typo as
//! NULL.

use ankql::ast::{Expr, OrderByItem, Parsed, PathExpr, Predicate, PropertyIdExt, PropertyPath, Resolved, Selection};
use ankurah_proto::{CollectionId, ModelId, PropertyId, SystemProperty};
use thiserror::Error;

use super::CatalogManager;
use crate::error::RetrievalError;
use crate::schema::{ModelStructDescriptor, SchemaEpoch};
use crate::value::ValueType;

/// What one source-level property name binds to: the durable identity every
/// consumer past the query boundary addresses it by, plus the registered type
/// its comparison literals canonicalize to. One lookup answers both, because
/// a property whose canonical type cannot be supplied is not a resolved
/// property -- the walk would have nothing to canonicalize against, and a
/// comparison would reach execution untyped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedProperty {
    /// The durable property identity.
    pub id: PropertyId,
    /// The property's registered logical type.
    pub value_type: ValueType,
}

/// A failure to bind a source-level name, or to canonicalize a comparison
/// literal against the type that name bound to.
#[derive(Debug, Error)]
pub enum ModelResolutionError {
    /// A model-scoped property lookup failed.
    #[error("property lookup for '{name}' in model '{model}' failed: {message}")]
    Lookup {
        /// The model being searched.
        model: ModelId,
        /// The source-level property name.
        name: String,
        /// The resolver's failure message.
        message: String,
    },
    /// A qualified model-name lookup failed.
    #[error("model lookup for qualifier '{name}' failed: {message}")]
    ModelLookup {
        /// The source-level model qualifier.
        name: String,
        /// The resolver's failure message.
        message: String,
    },
    /// No property with the requested name exists in an authoritative model.
    #[error("unknown property '{name}' in model '{model}'")]
    UnknownProperty {
        /// The model being searched.
        model: ModelId,
        /// The unresolved source-level name.
        name: String,
    },
    /// A resolved property cannot support the requested nested path.
    #[error("unsupported subpath '{path}' in model '{model}': {reason}")]
    UnsupportedSubpath {
        /// The model containing the property.
        model: ModelId,
        /// The complete source-level path.
        path: String,
        /// Why this consumer rejects the subpath.
        reason: String,
    },
    /// A comparison literal could not be cast to the registered type of the
    /// property it is compared against.
    #[error("comparison canonicalization in model '{model}' failed: {message}")]
    Canonicalization {
        /// The model containing the comparison.
        model: ModelId,
        /// The cast failure message.
        message: String,
    },
    /// A property resolved by name, but its registered type could not be
    /// supplied.
    #[error("value-type lookup for property '{property}' in model '{model}' failed: {message}")]
    ValueTypeLookup {
        /// The model containing the property.
        model: ModelId,
        /// The durable property identity.
        property: PropertyId,
        /// The resolver's failure message.
        message: String,
    },
}

/// The model-scoped name lookup [`resolve_selection`] runs on.
///
/// Exactly two lookups, both fail-closed: a miss is authoritative, so
/// `Ok(None)` means there is no such model or property -- never "not known
/// yet, ask again later". Implementors answer for ordinary registered models
/// only; the frozen system models resolve inside the walk from the closed
/// [`SystemProperty`] vocabulary and are never shown to an implementor.
pub trait ModelResolver {
    /// Resolve a source-level model qualifier, where qualified property paths
    /// are supported. Returning `None` leaves the first path step to be read
    /// as a property name, which is what a resolver with no qualifier
    /// vocabulary wants.
    fn resolve_model(&self, _name: &str) -> Result<Option<ModelId>, ModelResolutionError> { Ok(None) }

    /// Resolve `name` within an already-known `model`, answering with the
    /// durable identity AND the registered type this property's comparison
    /// literals canonicalize to.
    ///
    /// `Ok(None)` means the property is absent: a miss is authoritative. A
    /// property that exists but whose type cannot be supplied is an error,
    /// never a silent skip of canonicalization.
    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError>;
}

/// The canonical value type of each closed system property. System
/// properties have no catalog rows to record their types (they are the
/// frozen bootstrap base case), so the vocabulary is closed here instead.
pub(crate) fn system_property_value_type(property: SystemProperty) -> ValueType {
    match property {
        SystemProperty::Item => ValueType::String,
        SystemProperty::Label => ValueType::String,
        SystemProperty::Name => ValueType::String,
        SystemProperty::MintedFor => ValueType::EntityId,
        SystemProperty::Backend => ValueType::String,
        SystemProperty::ValueType => ValueType::String,
        SystemProperty::TargetModel => ValueType::EntityId,
        SystemProperty::Model => ValueType::EntityId,
        SystemProperty::Property => ValueType::EntityId,
        SystemProperty::Optional => ValueType::Bool,
    }
}

/// Raw resolution: the catalog's current display names, scoped to the
/// model being queried.
impl ModelResolver for CatalogManager {
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> {
        // Answers the legacy collection-qualified reference form
        // (`album.name`): resolution strips the qualifier when this returns
        // the model being queried.
        Ok(self.model_id_for(name))
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        let found = self.try_resolve(model, name).map_err(|error| ModelResolutionError::Lookup {
            model: *model,
            name: name.to_owned(),
            message: error.to_string(),
        })?;
        let Some(id) = found else { return Ok(None) };
        Ok(Some(ResolvedProperty { id, value_type: self.registered_value_type(model, &id)? }))
    }
}

impl CatalogManager {
    /// The registered type of a property the catalog has already
    /// resolved: the `id` pseudo-property and the frozen system vocabulary
    /// carry their own, and an allocated property takes the one its catalog
    /// definition records.
    fn registered_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, ModelResolutionError> {
        let lookup_failed =
            |message: &str| ModelResolutionError::ValueTypeLookup { model: *model, property: *property, message: message.into() };
        match property {
            PropertyId::Id => Ok(ValueType::EntityId),
            PropertyId::System(system) => Ok(system_property_value_type(*system)),
            PropertyId::EntityId(id) => {
                let property = self.property_by_id(id).ok_or_else(|| lookup_failed("no catalog definition"))?;
                ValueType::from_property_str(&property.value_type)
                    .ok_or_else(|| lookup_failed(&format!("unparseable registered type '{}'", property.value_type)))
            }
        }
    }
}

/// Typed resolution: the compiled declaration's fields bind through their
/// descriptor cells under the operation's epoch; names the struct does not
/// carry fall back to raw catalog resolution (an older binary may query a
/// field it does not compile).
pub(crate) struct DescriptorResolver<'a> {
    pub schema: &'static ModelStructDescriptor,
    pub epoch: SchemaEpoch,
    pub catalog: &'a CatalogManager,
}

impl ModelResolver for DescriptorResolver<'_> {
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> {
        // The declaration's own label answers from its cells (the admitted
        // identity, put there by the bind every typed entry runs first);
        // anything else falls back to the catalog, like a raw reference.
        if name == self.schema.label {
            if let Some(model) = self.schema.resolved.get(self.epoch) {
                return Ok(Some(model));
            }
        }
        self.catalog.resolve_model(name)
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        if let Some(field) = self.schema.field_by_name(name) {
            // A populated cell is the admitted binding, which a later
            // display-name change cannot re-aim, and the compiled
            // declaration states the type. A miss means the catalog could not
            // prove this shape and there is no admitted binding to protect,
            // so the name resolves like a raw one, against the catalog's
            // current names.
            if let Some(id) = field.resolved.get(self.epoch) {
                let value_type = ValueType::from_property_str(field.value_type).ok_or_else(|| ModelResolutionError::ValueTypeLookup {
                    model: *model,
                    property: id,
                    message: format!("unparseable compiled type '{}'", field.value_type),
                })?;
                return Ok(Some(ResolvedProperty { id, value_type }));
            }
        }
        self.catalog.resolve_property(model, name)
    }
}

/// Bind every source-level property path in `selection` to a durable
/// [`PropertyId`] under `model`, casting each comparison's literals to the
/// registered type of the property they are compared against.
///
/// This is the one door between the two stages: nothing else produces a
/// [`Selection<Resolved>`] from a [`Selection<Parsed>`], so a resolved
/// selection anywhere in the system was bound in one model scope, by one
/// resolver, and carries literals already canonicalized to the types that
/// resolver reported.
///
/// A literal that cannot take its property's registered type is a query-time
/// type error surfaced by the operation that ran the comparison, never a
/// silent false predicate and never a lenient pass-through -- policy-injected
/// comparisons included: a rule whose literal cannot type against the
/// credential fails the request loudly instead of silently filtering rows.
pub fn resolve_selection<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    selection: Selection<Parsed>,
) -> Result<Selection<Resolved>, ModelResolutionError> {
    let order_by = selection
        .order_by
        .as_ref()
        .map(|items| items.iter().map(|item| resolve_order_by_item(model, resolver, item)).collect())
        .transpose()?;
    Ok(Selection { predicate: resolve_predicate(model, resolver, &selection.predicate)?, order_by, limit: selection.limit })
}

/// The resolved form of a selection that names no property at all -- `true`,
/// `false`, a comparison of two literals -- and `None` for one that does.
///
/// Such a selection has nothing to bind, so it resolves without a model scope
/// and without a catalog. That matters where a query names a collection that
/// is not a registered model: a live query over `true` (an ephemeral node
/// bootstrapping its policy, say) must still run, and only a selection that
/// actually names a property needs the model whose catalog would bind it.
pub fn resolve_without_model(selection: &Selection<Parsed>) -> Option<Selection<Resolved>> {
    if selection.order_by.is_some() {
        return None;
    }
    Some(Selection { predicate: predicate_without_model(&selection.predicate)?, order_by: None, limit: selection.limit })
}

/// See [`resolve_without_model`].
fn predicate_without_model(predicate: &Predicate<Parsed>) -> Option<Predicate<Resolved>> {
    Some(match predicate {
        Predicate::Comparison { left, operator, right } => Predicate::Comparison {
            left: Box::new(expr_without_names(left)?),
            operator: operator.clone(),
            right: Box::new(expr_without_names(right)?),
        },
        Predicate::IsNull(expr) => Predicate::IsNull(Box::new(expr_without_names(expr)?)),
        Predicate::And(left, right) => Predicate::And(Box::new(predicate_without_model(left)?), Box::new(predicate_without_model(right)?)),
        Predicate::Or(left, right) => Predicate::Or(Box::new(predicate_without_model(left)?), Box::new(predicate_without_model(right)?)),
        Predicate::Not(inner) => Predicate::Not(Box::new(predicate_without_model(inner)?)),
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,
        Predicate::Placeholder => Predicate::Placeholder,
    })
}

/// See [`resolve_without_model`]: `None` as soon as a property name appears,
/// since binding that name is what needs a model.
fn expr_without_names(expr: &Expr<Parsed>) -> Option<Expr<Resolved>> {
    Some(match expr {
        Expr::Path(_) => return None,
        Expr::Literal(value) => Expr::Literal(value.clone()),
        Expr::Placeholder => Expr::Placeholder,
        Expr::ExprList(items) => Expr::ExprList(items.iter().map(expr_without_names).collect::<Option<Vec<_>>>()?),
        Expr::Predicate(predicate) => Expr::Predicate(predicate_without_model(predicate)?),
        Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
            left: Box::new(expr_without_names(left)?),
            operator: operator.clone(),
            right: Box::new(expr_without_names(right)?),
        },
    })
}

/// Resolve an ORDER BY key under the same rules as predicate paths. Sort keys
/// must address a whole property; JSON subpaths are rejected.
fn resolve_order_by_item<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    item: &OrderByItem<Parsed>,
) -> Result<OrderByItem<Resolved>, ModelResolutionError> {
    let path = &item.path;
    let head = property_head_index(model, resolver, path)?;
    let Some(name) = path.steps.get(head) else {
        return Err(unknown_property(model, ""));
    };
    whole_property_order(model, path, &path.steps[head + 1..])?;
    let (property, _sorts_by_identity) = resolve_path_head(model, resolver, name, vec![])?;
    Ok(OrderByItem { path: property, direction: item.direction.clone() })
}

/// Bind every source-level property path in this predicate and cast each
/// comparison's literals against the type the compared property resolved
/// with.
fn resolve_predicate<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    predicate: &Predicate<Parsed>,
) -> Result<Predicate<Resolved>, ModelResolutionError> {
    Ok(match predicate {
        Predicate::Comparison { left, operator, right } => {
            let (left, left_type) = resolve_expr(model, resolver, left)?;
            let (right, right_type) = resolve_expr(model, resolver, right)?;
            // One side names a property and the other does not: the named
            // property's registered type is the one the other side's literals
            // must take. Two properties (or two literals) leave both as they
            // came.
            let (left, right) = match (left_type, right_type) {
                (Some(target), None) => (left, cast_comparison_value(model, right, target)?),
                (None, Some(target)) => (cast_comparison_value(model, left, target)?, right),
                _ => (left, right),
            };
            Predicate::Comparison { left: Box::new(left), operator: operator.clone(), right: Box::new(right) }
        }
        Predicate::And(left, right) => {
            Predicate::And(Box::new(resolve_predicate(model, resolver, left)?), Box::new(resolve_predicate(model, resolver, right)?))
        }
        Predicate::Or(left, right) => {
            Predicate::Or(Box::new(resolve_predicate(model, resolver, left)?), Box::new(resolve_predicate(model, resolver, right)?))
        }
        Predicate::Not(inner) => Predicate::Not(Box::new(resolve_predicate(model, resolver, inner)?)),
        Predicate::IsNull(expr) => Predicate::IsNull(Box::new(resolve_expr(model, resolver, expr)?.0)),
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,
        Predicate::Placeholder => Predicate::Placeholder,
    })
}

/// Bind one expression, reporting alongside it the type a comparison against
/// it canonicalizes to -- `None` when the expression names no property, which
/// is what makes it the side that gets cast.
fn resolve_expr<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    expr: &Expr<Parsed>,
) -> Result<(Expr<Resolved>, Option<ValueType>), ModelResolutionError> {
    Ok(match expr {
        Expr::Path(path) => {
            let head = property_head_index(model, resolver, path)?;
            let Some(name) = path.steps.get(head) else {
                return Err(unknown_property(model, ""));
            };
            if name == "id" && path.steps.len() > head + 1 {
                return Err(id_subpath_error(model, path));
            }
            let (resolved, value_type) = resolve_path_head(model, resolver, name, path.steps[head + 1..].to_vec())?;
            // A JSON sub-path compares as JSON whatever the whole property's
            // registered type is: the sub-path addresses a value inside the
            // document, not the document.
            let value_type = if resolved.subpath.is_empty() { value_type } else { ValueType::Json };
            (Expr::Path(resolved), Some(value_type))
        }
        Expr::Literal(value) => (Expr::Literal(value.clone()), None),
        Expr::Placeholder => (Expr::Placeholder, None),
        Expr::ExprList(items) => (
            Expr::ExprList(
                items.iter().map(|item| resolve_expr(model, resolver, item).map(|(expr, _)| expr)).collect::<Result<Vec<_>, _>>()?,
            ),
            None,
        ),
        Expr::Predicate(predicate) => (Expr::Predicate(resolve_predicate(model, resolver, predicate)?), None),
        Expr::InfixExpr { left, operator, right } => (
            Expr::InfixExpr {
                left: Box::new(resolve_expr(model, resolver, left)?.0),
                operator: operator.clone(),
                right: Box::new(resolve_expr(model, resolver, right)?.0),
            },
            None,
        ),
    })
}

/// Cast one side of a comparison to the type the other side's property
/// resolved with.
fn cast_comparison_value(model: &ModelId, expr: Expr<Resolved>, target: ValueType) -> Result<Expr<Resolved>, ModelResolutionError> {
    Ok(match expr {
        Expr::Literal(value) => Expr::Literal(
            value.cast_to(target).map_err(|error| ModelResolutionError::Canonicalization { model: *model, message: error.to_string() })?,
        ),
        Expr::ExprList(values) => {
            Expr::ExprList(values.into_iter().map(|value| cast_comparison_value(model, value, target)).collect::<Result<Vec<_>, _>>()?)
        }
        other => other,
    })
}

/// Which step of a path is the property: the second one when the first is
/// this model's own name (the legacy collection-qualified form), the first
/// otherwise.
fn property_head_index<R: ModelResolver + ?Sized>(model: &ModelId, resolver: &R, path: &PathExpr) -> Result<usize, ModelResolutionError> {
    let Some(first) = path.steps.first() else { return Ok(0) };
    if path.steps.len() > 1 && resolver.resolve_model(first)?.as_ref() == Some(model) {
        Ok(1)
    } else {
        Ok(0)
    }
}

/// Bind the property step of a path: the `id` pseudo-property and the frozen
/// system vocabulary answer here, everything else asks the resolver.
fn resolve_path_head<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    name: &str,
    subpath: Vec<String>,
) -> Result<(PropertyPath, ValueType), ModelResolutionError> {
    if name == "id" {
        return Ok((PropertyPath::id(), ValueType::EntityId));
    }
    let resolved = match model {
        // A system model's properties are the frozen bootstrap vocabulary:
        // no catalog rows describe them, so the walk answers them from the
        // closed vocabulary and never asks a resolver about them.
        ModelId::System(_) => SystemProperty::from_name(name)
            .map(|system| ResolvedProperty { id: PropertyId::System(system), value_type: system_property_value_type(system) }),
        ModelId::EntityId(_) => resolver.resolve_property(model, name)?,
    }
    .ok_or_else(|| unknown_property(model, name))?;
    Ok((resolved_property_path(resolved.id, name, subpath), resolved.value_type))
}

fn resolved_property_path(property: PropertyId, label: &str, subpath: Vec<String>) -> PropertyPath {
    match property {
        PropertyId::Id => PropertyId::Id.path(&subpath),
        PropertyId::EntityId(id) => PropertyPath::registered(id, label, subpath),
        PropertyId::System(system) => PropertyPath::system(system, subpath),
    }
}

fn whole_property_order(model: &ModelId, path: &PathExpr, rest: &[String]) -> Result<(), ModelResolutionError> {
    if rest.is_empty() {
        return Ok(());
    }
    Err(ModelResolutionError::UnsupportedSubpath {
        model: *model,
        path: path.steps.join("."),
        reason: "ORDER BY keys name whole properties; JSON subpaths are not sortable".to_owned(),
    })
}

fn id_subpath_error(model: &ModelId, path: &PathExpr) -> ModelResolutionError {
    ModelResolutionError::UnsupportedSubpath {
        model: *model,
        path: path.steps.join("."),
        reason: "the id pseudo-property is the entity id and has no subfields".to_owned(),
    }
}

fn unknown_property(model: &ModelId, name: impl Into<String>) -> ModelResolutionError {
    ModelResolutionError::UnknownProperty { model: *model, name: name.into() }
}

impl CatalogManager {
    /// Resolve a RAW selection against `collection`: bind its names through
    /// the catalog's current display names and canonicalize its comparison
    /// values. A selection with no property references never needs the model
    /// scope at all.
    pub fn resolve_selection(
        &self,
        collection: &CollectionId,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        let Some(model) = self.model_id_for(collection.as_str()) else {
            return resolve_without_model(&selection).ok_or_else(|| {
                RetrievalError::Other(format!("collection '{collection}' is not a registered model; its property names cannot resolve"))
            });
        };
        resolve_selection(&model, self, selection).map_err(|error| RetrievalError::Other(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use ankql::ast::{ComparisonOperator, PathExpr, Value};
    use ankurah_proto::{EntityId, SystemModel};

    use super::*;

    fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([0x11; 32])) }
    fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([0x22; 32])) }

    /// A resolver whose catalog holds one typed property, and one name it
    /// knows but cannot type.
    struct WarmResolver;

    /// A resolver whose catalog holds nothing.
    struct ColdResolver;

    impl ModelResolver for WarmResolver {
        fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            Ok(match name {
                "value" => Some(ResolvedProperty { id: property(), value_type: ValueType::I64 }),
                // A name whose type the catalog cannot supply: half a
                // resolution is not a resolution, so the lookup fails
                // instead of answering an untyped identity.
                "typeless" => {
                    return Err(ModelResolutionError::ValueTypeLookup {
                        model: *model,
                        property: PropertyId::System(SystemProperty::Name),
                        message: "test resolver has no type for this property".to_owned(),
                    })
                }
                _ => None,
            })
        }
    }

    impl ModelResolver for ColdResolver {
        fn resolve_property(&self, _model: &ModelId, _name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            // Answers nothing; a miss is authoritative.
            Ok(None)
        }
    }

    fn comparison(path: PathExpr, value: Value) -> Selection<Parsed> {
        Predicate::Comparison {
            left: Box::new(Expr::Path(path)),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(value)),
        }
        .into()
    }

    #[test]
    fn the_lookup_is_object_safe() {
        // A resolver reaches the walk through type-erased boundaries (the
        // context behind `Arc<dyn TContext>` hands one over), so `dyn
        // ModelResolver` has to be a legal type: a generic method or a
        // `Self`-returning one here would foreclose that, silently, at the
        // next edit.
        let erased: &dyn ModelResolver = &ColdResolver;
        assert!(resolve_selection(&model(), erased, Selection::from(Predicate::True)).is_ok());
    }

    #[test]
    fn property_free_selection_needs_no_resolver() {
        let resolved = resolve_selection(&model(), &ColdResolver, Selection::from(Predicate::True)).unwrap();
        assert_eq!(resolved.predicate, Predicate::True);
    }

    #[test]
    fn unresolved_property_is_unknown() {
        let error = resolve_selection(&model(), &ColdResolver, comparison(PathExpr::simple("value"), Value::I64(42))).unwrap_err();
        assert!(matches!(error, ModelResolutionError::UnknownProperty { .. }));
    }

    #[test]
    fn resolution_casts_registered_property_literal() {
        let resolved = resolve_selection(&model(), &WarmResolver, comparison(PathExpr::simple("value"), Value::I32(42))).unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::I64(42)));
    }

    #[test]
    fn resolved_property_without_a_type_is_rejected() {
        let error = resolve_selection(&model(), &WarmResolver, comparison(PathExpr::simple("typeless"), Value::String("value".to_owned())))
            .unwrap_err();
        assert!(matches!(error, ModelResolutionError::ValueTypeLookup { property: PropertyId::System(SystemProperty::Name), .. }));
    }

    #[test]
    fn resolution_casts_json_subpath_literal() {
        let resolved = resolve_selection(
            &model(),
            &WarmResolver,
            comparison(PathExpr { steps: vec!["value".to_owned(), "nested".to_owned()] }, Value::String("hello".to_owned())),
        )
        .unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::Json(serde_json::json!("hello"))));
    }

    #[test]
    fn system_model_properties_type_from_the_frozen_vocabulary() {
        // A system model's properties never reach the resolver: the closed
        // vocabulary answers both identity and type, which is why a resolver
        // that knows nothing still resolves them.
        let model = ModelId::System(SystemModel::Model);
        let resolved =
            resolve_selection(&model, &ColdResolver, comparison(PathExpr::simple("optional"), Value::String("true".to_owned()))).unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::Bool(true)));
    }
}
