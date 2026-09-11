//! Resolves property names to durable identities and canonical values.

use ankql::ast::{Expr, OrderByItem, Parsed, PathExpr, Predicate, PropertyPath, Resolved, Selection};
use ankurah_proto::{ModelId, PropertyId, SystemModel, SystemProperty};
use thiserror::Error;

use crate::internal::prelude::*;
use crate::value::ValueType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedProperty {
    pub id: PropertyId,
    pub value_type: ValueType,
}

#[derive(Debug, Error)]
pub enum ModelResolutionError {
    #[error(transparent)]
    Catalog(#[from] RetrievalError),
    #[error("property lookup for '{name}' in model '{model}' failed: {message}")]
    Lookup { model: ModelId, name: String, message: String },
    #[error("unknown property '{name}' in model '{model}'")]
    UnknownProperty { model: ModelId, name: String },
    #[error("unsupported subpath '{path}' in model '{model}': {reason}")]
    UnsupportedSubpath { model: ModelId, path: String, reason: String },
    #[error("comparison canonicalization in model '{model}' failed: {message}")]
    Canonicalization { model: ModelId, message: String },
    #[error("value-type lookup for property '{property}' in model '{model}' failed: {message}")]
    ValueTypeLookup { model: ModelId, property: PropertyId, message: String },
}

impl From<ModelResolutionError> for RetrievalError {
    fn from(error: ModelResolutionError) -> Self {
        match error {
            ModelResolutionError::Catalog(error) => error,
            other => Self::Other(other.to_string()),
        }
    }
}

pub trait ModelResolver {
    /// Look up a model qualifier in a path such as `album.name`.
    fn resolve_model(&self, _name: &str) -> Result<Option<ModelId>, ModelResolutionError> { Ok(None) }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError>;
}

/// Canonical types for the bootstrap system properties, which have no catalog rows.
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

pub(crate) fn resolve_system_property(model: SystemModel, name: &str) -> Option<SystemProperty> {
    match (model, name) {
        (SystemModel::System, "item") => Some(SystemProperty::Item),
        (SystemModel::Model, "label") => Some(SystemProperty::Label),
        (SystemModel::Model, "name") => Some(SystemProperty::Name),
        (SystemModel::Property, "name") => Some(SystemProperty::Name),
        (SystemModel::Property, "minted_for") => Some(SystemProperty::MintedFor),
        (SystemModel::Property, "backend") => Some(SystemProperty::Backend),
        (SystemModel::Property, "value_type") => Some(SystemProperty::ValueType),
        (SystemModel::Property, "target_model") => Some(SystemProperty::TargetModel),
        (SystemModel::ModelProperty, "model") => Some(SystemProperty::Model),
        (SystemModel::ModelProperty, "property") => Some(SystemProperty::Property),
        (SystemModel::ModelProperty, "optional") => Some(SystemProperty::Optional),
        _ => None,
    }
}

impl ModelResolver for CatalogManager {
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> { Ok(self.model_id_for(name)?) }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        let found = self.try_resolve(model, name)?;
        let Some(id) = found else { return Ok(None) };
        Ok(Some(ResolvedProperty { id, value_type: self.registered_value_type(model, &id)? }))
    }
}

/// Resolves names through epoch-bound descriptor cells, falling back to the
/// catalog while those bindings have not yet been populated.
pub(crate) struct DescriptorResolver<'a> {
    pub schema: &'static ModelStructDescriptor,
    pub epoch: SystemEpoch,
    pub catalog: &'a CatalogManager,
}

impl ModelResolver for DescriptorResolver<'_> {
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> {
        if name == self.schema.label {
            if let Some(model) = self.schema.resolved.get(self.epoch) {
                return Ok(Some(model));
            }
        }
        self.catalog.resolve_model(name)
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        let Some(field) = self.schema.field_by_name(name) else { return Ok(None) };
        if let Some(id) = field.resolved.get(self.epoch) {
            let value_type = ValueType::from_property_str(field.value_type).ok_or_else(|| ModelResolutionError::ValueTypeLookup {
                model: *model,
                property: id,
                message: format!("unparseable compiled type '{}'", field.value_type),
            })?;
            return Ok(Some(ResolvedProperty { id, value_type }));
        }
        self.catalog.resolve_property(model, name)
    }
}

/// Check declared names without requiring their durable identities.
pub(crate) fn validate_selection_names(schema: &ModelStructDescriptor, selection: &Selection<Parsed>) -> Result<(), RetrievalError> {
    let check = |path: &PathExpr| {
        let head = usize::from(path.steps.len() > 1 && path.steps[0] == schema.label);
        let name = path.steps.get(head).map(String::as_str).unwrap_or("");
        if name == "id" || schema.field_by_name(name).is_some() {
            Ok(())
        } else {
            Err(RetrievalError::Other(format!("unknown property '{name}' in model '{}'", schema.label)))
        }
    };
    check_predicate_names(&selection.predicate, &check)?;
    for item in selection.order_by.iter().flatten() {
        check(&item.path)?;
    }
    Ok(())
}

fn check_predicate_names(
    predicate: &Predicate<Parsed>,
    check: &impl Fn(&PathExpr) -> Result<(), RetrievalError>,
) -> Result<(), RetrievalError> {
    predicate.walk(Ok(()), &mut |result, predicate| {
        result?;
        match predicate {
            Predicate::Comparison { left, right, .. } => {
                check_expr_names(left, check)?;
                check_expr_names(right, check)
            }
            Predicate::IsNull(expr) => check_expr_names(expr, check),
            Predicate::And(..) | Predicate::Or(..) | Predicate::Not(_) | Predicate::True | Predicate::False | Predicate::Placeholder => {
                Ok(())
            }
        }
    })
}

fn check_expr_names(expr: &Expr<Parsed>, check: &impl Fn(&PathExpr) -> Result<(), RetrievalError>) -> Result<(), RetrievalError> {
    match expr {
        Expr::Path(path) => check(path),
        Expr::Predicate(predicate) => check_predicate_names(predicate, check),
        Expr::InfixExpr { left, right, .. } => {
            check_expr_names(left, check)?;
            check_expr_names(right, check)
        }
        Expr::ExprList(items) => items.iter().try_for_each(|item| check_expr_names(item, check)),
        Expr::Literal(_) | Expr::Placeholder => Ok(()),
    }
}

/// Resolve every property path and canonicalize its comparison literals.
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

fn resolve_order_by_item<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    item: &OrderByItem<Parsed>,
) -> Result<OrderByItem<Resolved>, ModelResolutionError> {
    let path = &item.path;
    let head = property_name_index(model, resolver, path)?;
    let Some(name) = path.steps.get(head) else {
        return Err(unknown_property(model, ""));
    };
    check_order_by_path(model, path, &path.steps[head + 1..])?;
    let (property, _value_type) = resolve_path_head(model, resolver, name, vec![])?;
    Ok(OrderByItem { path: property, direction: item.direction.clone() })
}

fn resolve_predicate<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    predicate: &Predicate<Parsed>,
) -> Result<Predicate<Resolved>, ModelResolutionError> {
    Ok(match predicate {
        Predicate::Comparison { left, operator, right } => {
            let (left, left_type) = resolve_expr(model, resolver, left)?;
            let (right, right_type) = resolve_expr(model, resolver, right)?;
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

fn resolve_expr<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    expr: &Expr<Parsed>,
) -> Result<(Expr<Resolved>, Option<ValueType>), ModelResolutionError> {
    Ok(match expr {
        Expr::Path(path) => {
            let head = property_name_index(model, resolver, path)?;
            let Some(name) = path.steps.get(head) else {
                return Err(unknown_property(model, ""));
            };
            if name == "id" && path.steps.len() > head + 1 {
                return Err(id_subpath_error(model, path));
            }
            let (resolved, value_type) = resolve_path_head(model, resolver, name, path.steps[head + 1..].to_vec())?;
            if !resolved.subpath.is_empty() {
                if value_type != ValueType::Json {
                    return Err(ModelResolutionError::UnsupportedSubpath {
                        model: *model,
                        path: path.steps.join("."),
                        reason: "only JSON properties support subpaths".to_owned(),
                    });
                }
            }
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

/// Locate the property name, skipping a leading qualifier when it resolves to
/// the model already being queried.
fn property_name_index<R: ModelResolver + ?Sized>(model: &ModelId, resolver: &R, path: &PathExpr) -> Result<usize, ModelResolutionError> {
    let Some(first) = path.steps.first() else { return Ok(0) };
    if path.steps.len() > 1 && resolver.resolve_model(first)?.as_ref() == Some(model) {
        Ok(1)
    } else {
        Ok(0)
    }
}

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
        ModelId::System(model) => resolve_system_property(*model, name)
            .map(|system| ResolvedProperty { id: PropertyId::System(system), value_type: system_property_value_type(system) }),
        ModelId::EntityId(_) => resolver.resolve_property(model, name)?,
    }
    .ok_or_else(|| unknown_property(model, name))?;
    if resolved.id == PropertyId::Id && !subpath.is_empty() {
        return Err(ModelResolutionError::UnsupportedSubpath {
            model: *model,
            path: std::iter::once(name).chain(subpath.iter().map(String::as_str)).collect::<Vec<_>>().join("."),
            reason: "the id pseudo-property is the entity id and has no subfields".to_owned(),
        });
    }
    Ok((resolved_property_path(resolved.id, name, subpath), resolved.value_type))
}

fn resolved_property_path(property: PropertyId, label: &str, subpath: Vec<String>) -> PropertyPath {
    match property {
        PropertyId::Id => PropertyPath::id(),
        PropertyId::EntityId(id) => PropertyPath::registered(id, label, subpath),
        PropertyId::System(system) => PropertyPath::system(system, subpath),
    }
}

fn check_order_by_path(model: &ModelId, path: &PathExpr, rest: &[String]) -> Result<(), ModelResolutionError> {
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

#[cfg(test)]
mod tests {
    use ankql::ast::{ComparisonOperator, PathExpr, Value};
    use ankurah_proto::{EntityId, SystemModel};

    use super::*;

    fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([0x11; 32])) }
    fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([0x22; 32])) }

    struct WarmResolver;

    struct ColdResolver;

    impl ModelResolver for WarmResolver {
        fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            Ok(match name {
                "value" => Some(ResolvedProperty { id: property(), value_type: ValueType::I64 }),
                "document" => Some(ResolvedProperty { id: property(), value_type: ValueType::Json }),
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
        fn resolve_property(&self, _model: &ModelId, _name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> { Ok(None) }
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
    fn unregistered_collections_are_rejected() {
        let catalog = CatalogManager::default();
        let collection = CollectionId::fixed_name("unregistered");
        for source in ["TRUE LIMIT 2", "1 = 1", "name = 'Alice'", "id = ?", "TRUE ORDER BY id"] {
            let selection = ankql::parser::parse_selection(source).unwrap();
            assert!(catalog.resolve_selection(&collection, selection).is_err(), "{source}");
        }
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
            comparison(PathExpr { steps: vec!["document".to_owned(), "nested".to_owned()] }, Value::String("hello".to_owned())),
        )
        .unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::Json(serde_json::json!("hello"))));
    }

    #[test]
    fn scalar_subpaths_are_rejected() {
        let path = PathExpr { steps: vec!["value".to_owned(), "nested".to_owned()] };
        let error = resolve_selection(&model(), &WarmResolver, comparison(path, Value::I64(1))).unwrap_err();
        assert!(matches!(error, ModelResolutionError::UnsupportedSubpath { .. }));
    }

    #[test]
    fn system_properties_are_model_scoped() {
        let cases = [
            (SystemModel::System, "item", Some(SystemProperty::Item)),
            (SystemModel::Model, "label", Some(SystemProperty::Label)),
            (SystemModel::Model, "name", Some(SystemProperty::Name)),
            (SystemModel::Property, "backend", Some(SystemProperty::Backend)),
            (SystemModel::ModelProperty, "optional", Some(SystemProperty::Optional)),
            (SystemModel::Model, "optional", None),
            (SystemModel::Property, "label", None),
            (SystemModel::System, "name", None),
        ];
        for (model, name, expected) in cases {
            assert_eq!(resolve_system_property(model, name), expected, "{model}.{name}");
        }
    }
}
