//! Resolution of source-level property paths into durable property identities.
//!
//! The AST owns this traversal because it knows every place a property path
//! can occur. Callers supply model-scoped identity and registered-type lookup.

#![deny(missing_docs)]

use ankurah_core_types::{CastError, ModelId, PropertyId, SystemProperty, ValueType};
use thiserror::Error;

use crate::ast::{Expr, OrderByItem, OrderKey, PathExpr, Predicate, PropertyPath, Selection};

/// A failure to resolve source-level names or validate comparison values.
#[derive(Debug, Error)]
pub enum NameResolutionError {
    /// Metadata for a registered model has not finished loading.
    #[error("name resolver is not ready for model '{model}'")]
    ResolverNotReady {
        /// The model whose metadata is not yet authoritative.
        model: ModelId,
    },
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
    /// An origin-side comparison literal could not be cast to its registered type.
    #[error("comparison canonicalization in model '{model}' failed: {message}")]
    Canonicalization {
        /// The model containing the comparison.
        model: ModelId,
        /// The cast failure message.
        message: String,
    },
    /// Registered type lookup failed for a resolved property.
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

/// A value failed the point-of-use cast requested by an AST consumer.
///
/// Name resolution performs the same kind of cast as early validation, but
/// consumers use this separate operation at their own trust boundary rather
/// than treating a previously normalized AST as proof.
#[derive(Debug, Error, Clone, PartialEq)]
#[error("comparison value for property {property:?} cannot be cast to {target:?}: {source}")]
pub struct ComparisonValueCastError {
    /// The resolved property whose value establishes the comparison type.
    pub property: PropertyId,
    /// The consumer's authoritative target type.
    pub target: ValueType,
    #[source]
    /// The underlying value conversion failure.
    pub source: CastError,
}

/// Model-scoped services needed while resolving an AnkQL AST.
///
/// Identity lookup and registered value-type lookup are both required. A
/// resolved property without a canonical type is not a valid property. AnkQL
/// uses that type to validate and normalize comparison values at query origin.
/// Every execution consumer must still cast/check values again at its trust
/// boundary; a normalized wire AST is never treated as proof.
pub trait NameResolver {
    /// Whether absence from this resolver is authoritative for `model`.
    ///
    /// Resolvers backed by asynchronously populated metadata return `false`
    /// until they can distinguish an unknown name from metadata that has not
    /// arrived yet. In-memory/static resolvers can use the default.
    fn is_ready(&self, _model: &ModelId) -> bool { true }

    /// Resolve a source-level model qualifier, when qualified property paths
    /// are supported. Returning `None` leaves the first path step as a
    /// property name.
    fn resolve_model_name(&self, _name: &str) -> Result<Option<ModelId>, NameResolutionError> { Ok(None) }

    /// Resolve `name` within an already-known `model`.
    ///
    /// `Ok(None)` means the property is absent only when [`Self::is_ready`]
    /// reports that the model's metadata is authoritative.
    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<PropertyId>, NameResolutionError>;

    /// Return a resolved property's registered logical type.
    ///
    /// This is used for early validation and canonicalization at query origin;
    /// execution consumers must still cast at their own trust boundaries. A
    /// property whose type cannot be supplied must return an error rather than
    /// silently disabling canonicalization.
    fn property_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, NameResolutionError>;
}

impl Selection {
    /// Bind every source-level property path in this selection to a durable
    /// [`PropertyId`]. Already-resolved paths pass through unchanged.
    pub fn resolve_names<R: NameResolver + ?Sized>(&self, model: &ModelId, resolver: &R) -> Result<Self, NameResolutionError> {
        let order_by =
            self.order_by.as_ref().map(|items| items.iter().map(|item| item.resolve_names(model, resolver)).collect()).transpose()?;
        Ok(Self { predicate: self.predicate.resolve_names(model, resolver)?, order_by, limit: self.limit })
    }

    /// Re-cast comparison literals immediately before a consumer uses them.
    ///
    /// `type_of` supplies that consumer's authoritative type for a whole
    /// resolved property. JSON subpaths and the `id` pseudo-property are typed
    /// here as `Json` and `EntityId` respectively. This deliberately remains a
    /// point-of-use API: callers should invoke it at each execution boundary,
    /// even when [`Self::resolve_names`] already normalized the AST at origin.
    pub fn cast_comparison_values<F>(&self, type_of: &F) -> Result<Self, ComparisonValueCastError>
    where F: Fn(&PropertyPath) -> Option<ValueType> {
        Ok(Self { predicate: cast_predicate_values(&self.predicate, type_of)?, order_by: self.order_by.clone(), limit: self.limit })
    }
}

fn cast_predicate_values<F>(predicate: &Predicate, type_of: &F) -> Result<Predicate, ComparisonValueCastError>
where F: Fn(&PropertyPath) -> Option<ValueType> {
    Ok(match predicate {
        Predicate::Comparison { left, operator, right } => {
            let left_target = comparison_target(left, type_of);
            let right_target = comparison_target(right, type_of);
            let left = match right_target {
                Some((path, target)) => cast_execution_expr(left, path, target)?,
                None => left.as_ref().clone(),
            };
            let right = match left_target {
                Some((path, target)) => cast_execution_expr(right, path, target)?,
                None => right.as_ref().clone(),
            };
            Predicate::Comparison { left: Box::new(left), operator: operator.clone(), right: Box::new(right) }
        }
        Predicate::And(left, right) => {
            Predicate::And(Box::new(cast_predicate_values(left, type_of)?), Box::new(cast_predicate_values(right, type_of)?))
        }
        Predicate::Or(left, right) => {
            Predicate::Or(Box::new(cast_predicate_values(left, type_of)?), Box::new(cast_predicate_values(right, type_of)?))
        }
        Predicate::Not(inner) => Predicate::Not(Box::new(cast_predicate_values(inner, type_of)?)),
        Predicate::IsNull(_) | Predicate::True | Predicate::False | Predicate::Placeholder => predicate.clone(),
    })
}

fn comparison_target<'a, F>(expr: &'a Expr, type_of: &F) -> Option<(&'a PropertyPath, ValueType)>
where F: Fn(&PropertyPath) -> Option<ValueType> {
    let Expr::PropertyPath(path) = expr else { return None };
    let target = if !path.subpath.is_empty() {
        ValueType::Json
    } else if path.id() == PropertyId::Id {
        ValueType::EntityId
    } else {
        type_of(path)?
    };
    Some((path, target))
}

fn cast_execution_expr(expr: &Expr, property: &PropertyPath, target: ValueType) -> Result<Expr, ComparisonValueCastError> {
    Ok(match expr {
        Expr::Literal(value) => {
            Expr::Literal(value.cast_to(target).map_err(|source| ComparisonValueCastError { property: property.id(), target, source })?)
        }
        Expr::ExprList(values) => {
            Expr::ExprList(values.iter().map(|value| cast_execution_expr(value, property, target)).collect::<Result<Vec<_>, _>>()?)
        }
        other => other.clone(),
    })
}

impl OrderByItem {
    /// Resolve an ORDER BY key under the same rules as predicate paths. Sort
    /// keys must address a whole property; JSON subpaths are rejected.
    pub fn resolve_names<R: NameResolver + ?Sized>(&self, model: &ModelId, resolver: &R) -> Result<Self, NameResolutionError> {
        let path = match &self.key {
            OrderKey::Property(_) => return Ok(self.clone()),
            OrderKey::Path(path) => path,
        };
        let head = property_head_index(model, resolver, path)?;
        let Some(name) = path.steps.get(head) else {
            return Err(unknown_property(model, ""));
        };
        whole_property_order(model, path, &path.steps[head + 1..])?;
        let property = resolve_path_head(model, resolver, name, vec![])?;
        Ok(Self { key: OrderKey::Property(property), direction: self.direction.clone() })
    }
}

impl Predicate {
    /// Bind every source-level property path in this predicate and perform
    /// origin-side comparison-value validation against registered types.
    pub fn resolve_names<R: NameResolver + ?Sized>(&self, model: &ModelId, resolver: &R) -> Result<Self, NameResolutionError> {
        Ok(match self {
            Predicate::Comparison { left, operator, right } => {
                let left = resolve_expr(left, model, resolver)?;
                let right = resolve_expr(right, model, resolver)?;
                let (left, right) = normalize_comparison(model, resolver, left, right)?;
                Predicate::Comparison { left: Box::new(left), operator: operator.clone(), right: Box::new(right) }
            }
            Predicate::And(left, right) => {
                Predicate::And(Box::new(left.resolve_names(model, resolver)?), Box::new(right.resolve_names(model, resolver)?))
            }
            Predicate::Or(left, right) => {
                Predicate::Or(Box::new(left.resolve_names(model, resolver)?), Box::new(right.resolve_names(model, resolver)?))
            }
            Predicate::Not(inner) => Predicate::Not(Box::new(inner.resolve_names(model, resolver)?)),
            Predicate::IsNull(expr) => Predicate::IsNull(Box::new(resolve_expr(expr, model, resolver)?)),
            Predicate::True | Predicate::False | Predicate::Placeholder => self.clone(),
        })
    }
}

fn normalize_comparison<R: NameResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    left: Expr,
    right: Expr,
) -> Result<(Expr, Expr), NameResolutionError> {
    let left_type = comparison_value_type(model, resolver, &left)?;
    let right_type = comparison_value_type(model, resolver, &right)?;
    match (left_type, right_type) {
        (Some(target), None) => cast_comparison_value(model, right, target).map(|right| (left, right)),
        (None, Some(target)) => cast_comparison_value(model, left, target).map(|left| (left, right)),
        _ => Ok((left, right)),
    }
}

fn comparison_value_type<R: NameResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    expr: &Expr,
) -> Result<Option<ValueType>, NameResolutionError> {
    let Expr::PropertyPath(path) = expr else { return Ok(None) };
    if !path.subpath.is_empty() {
        return Ok(Some(ValueType::Json));
    }
    match path.id() {
        PropertyId::Id => Ok(Some(ValueType::EntityId)),
        property @ (PropertyId::EntityId(_) | PropertyId::System(_)) => resolver.property_value_type(model, &property).map(Some),
    }
}

fn cast_comparison_value(model: &ModelId, expr: Expr, target: ValueType) -> Result<Expr, NameResolutionError> {
    Ok(match expr {
        Expr::Literal(value) => Expr::Literal(
            value.cast_to(target).map_err(|error| NameResolutionError::Canonicalization { model: *model, message: error.to_string() })?,
        ),
        Expr::ExprList(values) => {
            Expr::ExprList(values.into_iter().map(|value| cast_comparison_value(model, value, target)).collect::<Result<Vec<_>, _>>()?)
        }
        other => other,
    })
}

fn resolve_expr<R: NameResolver + ?Sized>(expr: &Expr, model: &ModelId, resolver: &R) -> Result<Expr, NameResolutionError> {
    Ok(match expr {
        Expr::Path(path) => {
            let head = property_head_index(model, resolver, path)?;
            let Some(name) = path.steps.get(head) else {
                return Err(unknown_property(model, ""));
            };
            if name == "id" && path.steps.len() > head + 1 {
                return Err(id_subpath_error(model, path));
            }
            Expr::PropertyPath(resolve_path_head(model, resolver, name, path.steps[head + 1..].to_vec())?)
        }
        Expr::PropertyPath(_) | Expr::Literal(_) | Expr::Placeholder => expr.clone(),
        Expr::ExprList(items) => {
            Expr::ExprList(items.iter().map(|item| resolve_expr(item, model, resolver)).collect::<Result<Vec<_>, _>>()?)
        }
        Expr::Predicate(predicate) => Expr::Predicate(predicate.resolve_names(model, resolver)?),
        Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
            left: Box::new(resolve_expr(left, model, resolver)?),
            operator: operator.clone(),
            right: Box::new(resolve_expr(right, model, resolver)?),
        },
    })
}

fn property_head_index<R: NameResolver + ?Sized>(model: &ModelId, resolver: &R, path: &PathExpr) -> Result<usize, NameResolutionError> {
    let Some(first) = path.steps.first() else { return Ok(0) };
    if matches!(model, ModelId::EntityId(_)) && path.steps.len() > 1 && !resolver.is_ready(model) {
        return Err(NameResolutionError::ResolverNotReady { model: *model });
    }
    if path.steps.len() > 1 && resolver.resolve_model_name(first)?.as_ref() == Some(model) {
        Ok(1)
    } else {
        Ok(0)
    }
}

fn resolve_path_head<R: NameResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    name: &str,
    subpath: Vec<String>,
) -> Result<PropertyPath, NameResolutionError> {
    if name == "id" {
        return Ok(PropertyPath::primary_key(subpath));
    }
    let property = match model {
        ModelId::System(_) => SystemProperty::from_name(name).map(PropertyId::System),
        ModelId::EntityId(_) => {
            if !resolver.is_ready(model) {
                return Err(NameResolutionError::ResolverNotReady { model: *model });
            }
            resolver.resolve_property(model, name)?
        }
    }
    .ok_or_else(|| unknown_property(model, name))?;
    Ok(resolved_property_path(property, name, subpath))
}

/// Resolve one frozen system-model property without consulting a catalog.
///
/// Returns `None` when `name` is not a built-in property. The `id`
/// pseudo-property is accepted independently of [`SystemProperty`].
pub fn system_property(name: &str, subpath: Vec<String>) -> Option<PropertyPath> {
    if name == "id" {
        return Some(PropertyPath::primary_key(subpath));
    }
    SystemProperty::from_name(name).map(|property| PropertyPath::system(property, subpath))
}

fn resolved_property_path(property: PropertyId, label: &str, subpath: Vec<String>) -> PropertyPath {
    match property {
        PropertyId::Id => PropertyPath::primary_key(subpath),
        PropertyId::EntityId(id) => PropertyPath::registered(id, label, subpath),
        PropertyId::System(system) => PropertyPath::system(system, subpath),
    }
}

fn whole_property_order(model: &ModelId, path: &PathExpr, rest: &[String]) -> Result<(), NameResolutionError> {
    if rest.is_empty() {
        return Ok(());
    }
    Err(NameResolutionError::UnsupportedSubpath {
        model: *model,
        path: path.steps.join("."),
        reason: "ORDER BY keys name whole properties; JSON subpaths are not sortable".to_owned(),
    })
}

fn id_subpath_error(model: &ModelId, path: &PathExpr) -> NameResolutionError {
    NameResolutionError::UnsupportedSubpath {
        model: *model,
        path: path.steps.join("."),
        reason: "the id pseudo-property is the entity id and has no subfields".to_owned(),
    }
}

fn unknown_property(model: &ModelId, name: impl Into<String>) -> NameResolutionError {
    NameResolutionError::UnknownProperty { model: *model, name: name.into() }
}

#[cfg(test)]
mod tests {
    use ankurah_core_types::{EntityId, SystemModel, Value};

    use super::*;
    use crate::ast::{ComparisonOperator, PathExpr};

    fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([0x11; 16])) }
    fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([0x22; 16])) }

    struct Resolver;

    struct ColdResolver;

    impl NameResolver for Resolver {
        fn resolve_property(&self, _model: &ModelId, name: &str) -> Result<Option<PropertyId>, NameResolutionError> {
            Ok(match name {
                "value" => Some(property()),
                "typeless" => Some(PropertyId::System(SystemProperty::Name)),
                _ => None,
            })
        }

        fn property_value_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, NameResolutionError> {
            match property {
                PropertyId::EntityId(_) => Ok(ValueType::I64),
                PropertyId::System(SystemProperty::Optional) => Ok(ValueType::Bool),
                _ => Err(NameResolutionError::ValueTypeLookup {
                    model: *model,
                    property: *property,
                    message: "test resolver has no type for this property".to_owned(),
                }),
            }
        }
    }

    impl NameResolver for ColdResolver {
        fn is_ready(&self, _model: &ModelId) -> bool { false }

        fn resolve_property(&self, _model: &ModelId, _name: &str) -> Result<Option<PropertyId>, NameResolutionError> {
            panic!("a cold resolver must not be queried")
        }

        fn property_value_type(&self, model: &ModelId, _property: &PropertyId) -> Result<ValueType, NameResolutionError> {
            Err(NameResolutionError::ResolverNotReady { model: *model })
        }
    }

    fn comparison(path: PathExpr, value: Value) -> Selection {
        Predicate::Comparison {
            left: Box::new(Expr::Path(path)),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(value)),
        }
        .into()
    }

    #[test]
    fn property_free_selection_does_not_require_ready_resolver() {
        let resolved = Selection::from(Predicate::True).resolve_names(&model(), &ColdResolver).unwrap();
        assert_eq!(resolved.predicate, Predicate::True);
    }

    #[test]
    fn unresolved_property_reports_cold_resolver() {
        let error = comparison(PathExpr::simple("value"), Value::I64(42)).resolve_names(&model(), &ColdResolver).unwrap_err();
        assert!(matches!(error, NameResolutionError::ResolverNotReady { .. }));
    }

    #[test]
    fn resolve_names_casts_registered_property_literal() {
        let resolved = comparison(PathExpr::simple("value"), Value::I32(42)).resolve_names(&model(), &Resolver).unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::I64(42)));
    }

    #[test]
    fn resolved_property_without_a_type_is_rejected() {
        let error =
            comparison(PathExpr::simple("typeless"), Value::String("value".to_owned())).resolve_names(&model(), &Resolver).unwrap_err();
        assert!(matches!(error, NameResolutionError::ValueTypeLookup { property: PropertyId::System(SystemProperty::Name), .. }));
    }

    #[test]
    fn resolve_names_casts_json_subpath_literal() {
        let resolved = comparison(PathExpr { steps: vec!["value".to_owned(), "nested".to_owned()] }, Value::String("hello".to_owned()))
            .resolve_names(&model(), &Resolver)
            .unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::Json(serde_json::json!("hello"))));
    }

    #[test]
    fn resolve_names_uses_registered_type_for_system_property() {
        let model = ModelId::System(SystemModel::Model);
        let resolved = comparison(PathExpr::simple("optional"), Value::String("true".to_owned())).resolve_names(&model, &Resolver).unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::Bool(true)));
    }

    #[test]
    fn execution_cast_does_not_trust_origin_normalization() {
        let selection: Selection = Predicate::Comparison {
            left: Box::new(Expr::PropertyPath(PropertyPath::registered(
                match property() {
                    PropertyId::EntityId(id) => id,
                    _ => unreachable!(),
                },
                "value",
                vec![],
            ))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::I32(42))),
        }
        .into();

        let cast = selection.cast_comparison_values(&|_| Some(ValueType::I64)).unwrap();
        let Predicate::Comparison { right, .. } = cast.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::I64(42)));
    }
}
