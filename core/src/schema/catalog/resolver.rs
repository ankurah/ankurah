//! Resolves property names to durable identities and canonical values.

use ankql::ast::{Expr, ModelRef, OrderByItem, Parsed, PathExpr, Predicate, PropertyPath, Resolved, Selection};
use ankurah_proto::{ModelId, PropertyId, SystemModel, SystemProperty};
use thiserror::Error;

use crate::internal::prelude::*;
use crate::value::ValueType;

mod component_scope;
pub(crate) use component_scope::validate_selection_names;
pub use component_scope::ComponentScopeError;
use component_scope::{locate_property, locate_sort_key, ComponentNames, QueryComponent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedProperty {
    pub id: PropertyId,
    pub value_type: ValueType,
}

#[derive(Debug, Error)]
pub enum ModelResolutionError {
    #[error(transparent)]
    Catalog(#[from] RetrievalError),
    #[error("unknown model '{0}'")]
    UnknownModel(String),
    #[error("property lookup for '{name}' in model '{model}' failed: {message}")]
    Lookup { model: ModelId, name: String, message: String },
    #[error("unknown property '{name}' in model '{model}'")]
    UnknownProperty { model: ModelId, name: String },
    #[error(transparent)]
    ComponentScope(#[from] ComponentScopeError),
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
            ModelResolutionError::ComponentScope(error) => Self::ComponentScope(error),
            other => Self::Other(other.to_string()),
        }
    }
}

pub trait ModelResolver {
    /// Look up a model label for membership or a path qualifier such as `album.name`.
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

/// Resolves names through the epoch-bound descriptor cells of a query's components, falling back to
/// the catalog for any other component and while those bindings have not yet been populated.
pub(crate) struct DescriptorResolver<'a> {
    pub schemas: &'a [&'static ModelStructDescriptor],
    pub epoch: SystemEpoch,
    pub catalog: &'a CatalogManager,
}

impl ModelResolver for DescriptorResolver<'_> {
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> {
        if let Some(schema) = self.schemas.iter().find(|schema| schema.label == name) {
            if let Ok(model) = schema.resolved.get(self.epoch) {
                return Ok(Some(model));
            }
        }
        self.catalog.resolve_model(name)
    }

    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        let Some(schema) = self.schemas.iter().find(|schema| schema.resolved.get(self.epoch).ok().as_ref() == Some(model)) else {
            return self.catalog.resolve_property(model, name);
        };
        let Some(field) = schema.field_by_name(name) else { return Ok(None) };
        if let Ok(id) = field.resolved.get(self.epoch) {
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

/// Resolve names and literal types against a query's components, restricting the selection to entities
/// that are members of every one of them.
pub(crate) fn resolve_component_selection(
    schemas: &[&'static ModelStructDescriptor],
    catalog: &CatalogManager,
    epoch: Option<SystemEpoch>,
    selection: Selection<Parsed>,
) -> Result<Selection<Resolved>, RetrievalError> {
    // Before the node has a system epoch, only system models resolve, under the bootstrap epoch.
    let epoch = match (epoch, schemas.iter().find(|schema| schema.system.is_none())) {
        (Some(epoch), _) => epoch,
        (None, None) => SystemEpoch::BOOTSTRAP,
        (None, Some(schema)) => return Err(RetrievalError::UnboundDeclaration { label: schema.label.to_string() }),
    };
    let components = schemas
        .iter()
        .map(|schema| Ok(QueryComponent { model: schema.bind_local(catalog, epoch)?, names: Some(ComponentNames::from(*schema)) }))
        .collect::<Result<Vec<_>, RetrievalError>>()?;
    let resolver = DescriptorResolver { schemas, epoch, catalog };
    let selection = resolve_selection_across(&components, &resolver, selection)?;
    Ok(components.iter().rev().fold(selection, |selection, component| selection.and_member_of(component.model)))
}

/// Resolve model labels and property paths against one model, and canonicalize comparison literals.
pub fn resolve_selection<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    selection: Selection<Parsed>,
) -> Result<Selection<Resolved>, ModelResolutionError> {
    resolve_selection_across(&[QueryComponent { model: *model, names: None }], resolver, selection)
}

/// Resolve model labels and property paths across `components`, and canonicalize comparison literals.
fn resolve_selection_across<R: ModelResolver + ?Sized>(
    components: &[QueryComponent<'_>],
    resolver: &R,
    selection: Selection<Parsed>,
) -> Result<Selection<Resolved>, ModelResolutionError> {
    let order_by = selection
        .order_by
        .as_ref()
        .map(|items| items.iter().map(|item| resolve_order_by_item(components, resolver, item)).collect())
        .transpose()?;
    Ok(Selection { predicate: resolve_predicate(components, resolver, &selection.predicate)?, order_by, limit: selection.limit })
}

fn resolve_order_by_item<R: ModelResolver + ?Sized>(
    components: &[QueryComponent<'_>],
    resolver: &R,
    item: &OrderByItem<Parsed>,
) -> Result<OrderByItem<Resolved>, ModelResolutionError> {
    let path = &item.path;
    let found = locate_sort_key(components, resolver, path)?;
    check_order_by_path(&found.model, path, found.subpath)?;
    Ok(OrderByItem { path: resolved_property_path(found.property.id, found.name, vec![]), direction: item.direction.clone() })
}

fn resolve_predicate<R: ModelResolver + ?Sized>(
    components: &[QueryComponent<'_>],
    resolver: &R,
    predicate: &Predicate<Parsed>,
) -> Result<Predicate<Resolved>, ModelResolutionError> {
    Ok(match predicate {
        Predicate::Comparison { left, operator, right } => {
            let (left, left_type) = resolve_expr(components, resolver, left)?;
            let (right, right_type) = resolve_expr(components, resolver, right)?;
            let (left, right) = match (left_type, right_type) {
                (Some((model, target)), None) => (left, cast_comparison_value(&model, right, target)?),
                (None, Some((model, target))) => (cast_comparison_value(&model, left, target)?, right),
                _ => (left, right),
            };
            Predicate::Comparison { left: Box::new(left), operator: operator.clone(), right: Box::new(right) }
        }
        Predicate::And(left, right) => Predicate::And(
            Box::new(resolve_predicate(components, resolver, left)?),
            Box::new(resolve_predicate(components, resolver, right)?),
        ),
        Predicate::Or(left, right) => Predicate::Or(
            Box::new(resolve_predicate(components, resolver, left)?),
            Box::new(resolve_predicate(components, resolver, right)?),
        ),
        Predicate::Not(inner) => Predicate::Not(Box::new(resolve_predicate(components, resolver, inner)?)),
        Predicate::IsNull(expr) => Predicate::IsNull(Box::new(resolve_expr(components, resolver, expr)?.0)),
        Predicate::MemberOf(reference) => Predicate::MemberOf(match reference {
            ModelRef::Id(id) => *id,
            ModelRef::Label(label) => resolver.resolve_model(label)?.ok_or_else(|| ModelResolutionError::UnknownModel(label.clone()))?,
        }),
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,
        Predicate::Placeholder => Predicate::Placeholder,
    })
}

/// A resolved expression, and for a property path, the model it was found in and its value type.
type ResolvedExpr = (Expr<Resolved>, Option<(ModelId, ValueType)>);

fn resolve_expr<R: ModelResolver + ?Sized>(
    components: &[QueryComponent<'_>],
    resolver: &R,
    expr: &Expr<Parsed>,
) -> Result<ResolvedExpr, ModelResolutionError> {
    Ok(match expr {
        Expr::Path(path) => {
            let found = locate_property(components, resolver, path)?;
            let value_type = found.property.value_type;
            if !found.subpath.is_empty() {
                if found.property.id == PropertyId::Id {
                    return Err(id_subpath_error(&found.model, path));
                }
                if value_type != ValueType::Json {
                    return Err(ModelResolutionError::UnsupportedSubpath {
                        model: found.model,
                        path: path.to_string(),
                        reason: "only JSON properties support subpaths".to_owned(),
                    });
                }
            }
            (Expr::Path(resolved_property_path(found.property.id, found.name, found.subpath.to_vec())), Some((found.model, value_type)))
        }
        Expr::Literal(value) => (Expr::Literal(value.clone()), None),
        Expr::Placeholder => (Expr::Placeholder, None),
        Expr::ExprList(items) => (
            Expr::ExprList(
                items.iter().map(|item| resolve_expr(components, resolver, item).map(|(expr, _)| expr)).collect::<Result<Vec<_>, _>>()?,
            ),
            None,
        ),
        Expr::Predicate(predicate) => (Expr::Predicate(resolve_predicate(components, resolver, predicate)?), None),
        Expr::InfixExpr { left, operator, right } => (
            Expr::InfixExpr {
                left: Box::new(resolve_expr(components, resolver, left)?.0),
                operator: operator.clone(),
                right: Box::new(resolve_expr(components, resolver, right)?.0),
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
        path: path.to_string(),
        reason: "ORDER BY keys name whole properties; JSON subpaths are not sortable".to_owned(),
    })
}

fn id_subpath_error(model: &ModelId, path: &PathExpr) -> ModelResolutionError {
    ModelResolutionError::UnsupportedSubpath {
        model: *model,
        path: path.to_string(),
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
        fn resolve_model(&self, label: &str) -> Result<Option<ModelId>, ModelResolutionError> { Ok((label == "album").then(model)) }

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
    fn unregistered_models_are_rejected() {
        let catalog = CatalogManager::default();
        let model = ModelId::EntityId(proto::EntityId::random());
        for source in ["TRUE LIMIT 2", "1 = 1", "name = 'Alice'", "id = ?", "TRUE ORDER BY id"] {
            let selection = ankql::parser::parse_selection(source).unwrap();
            assert!(catalog.resolve_selection(&model, selection).is_err(), "{source}");
        }
    }

    #[test]
    fn unresolved_property_is_unknown() {
        let error = resolve_selection(&model(), &ColdResolver, comparison(PathExpr::simple("value"), Value::I64(42))).unwrap_err();
        assert!(matches!(error, ModelResolutionError::UnknownProperty { .. }));
    }

    #[test]
    fn membership_labels_and_ids_resolve() {
        let selection = ankurah_derive::selection!("MEMBEROF('album') OR (NOT (MEMBEROF('system:model')))");
        let resolved = resolve_selection(&model(), &WarmResolver, selection).unwrap();
        assert_eq!(
            resolved.predicate,
            Predicate::Or(
                Box::new(Predicate::MemberOf(model())),
                Box::new(Predicate::Not(Box::new(Predicate::MemberOf(ModelId::System(SystemModel::Model))))),
            )
        );
        let by_id = ankql::parser::parse_selection(&format!("MEMBEROF('{}')", model())).unwrap();
        assert_eq!(resolve_selection(&model(), &ColdResolver, by_id).unwrap().predicate, Predicate::MemberOf(model()));
        let unknown = ankql::parser::parse_selection("MEMBEROF('unknown')").unwrap();
        assert!(matches!(resolve_selection(&model(), &WarmResolver, unknown), Err(ModelResolutionError::UnknownModel(_))));
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
            comparison(
                PathExpr { component: None, steps: vec!["document".to_owned(), "nested".to_owned()] },
                Value::String("hello".to_owned()),
            ),
        )
        .unwrap();
        let Predicate::Comparison { right, .. } = resolved.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::Json(serde_json::json!("hello"))));
    }

    #[test]
    fn scalar_subpaths_are_rejected() {
        let path = PathExpr { component: None, steps: vec!["value".to_owned(), "nested".to_owned()] };
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
