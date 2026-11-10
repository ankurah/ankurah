//! Which of a query's components a property name belongs to.

use ankql::ast::PathExpr;
use ankurah_proto::{ModelId, PropertyId};
use thiserror::Error;

use super::{resolve_system_property, system_property_value_type, unknown_property, ModelResolutionError, ModelResolver, ResolvedProperty};
use crate::value::ValueType;

/// A component a query returns: every match is a member of it, and its property names need no qualifier.
#[derive(Debug, Clone, Copy)]
pub(super) struct QueryComponent<'a> {
    pub model: ModelId,
    /// The display name a `Component::` qualifier may use for it, such as `Assignment`. A component known
    /// only by its model id has none; a qualifier names it by label.
    pub name: Option<&'a str>,
}

impl QueryComponent<'_> {
    /// How an error names this component.
    fn describe(&self) -> String { self.name.map_or_else(|| self.model.to_string(), str::to_owned) }
}

/// A property name that a query's components cannot place on exactly one property.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ComponentScopeError {
    /// A bare name two of the query's components declare as different properties.
    #[error(
        "'{name}' names different properties in components '{first}' and '{second}'; qualify it as {first}::{name} or {second}::{name}"
    )]
    Ambiguous { name: String, first: String, second: String },
    /// A qualifier naming neither a component of the query by display name nor any component by label.
    #[error("unknown component '{0}': a qualifier names one of the query's components by name, or any component by its label")]
    UnknownComponent(String),
    /// A name that none of these components declares.
    #[error("no property '{name}' in {}", describe_components(.components))]
    Undeclared { name: String, components: Vec<String> },
}

fn describe_components(components: &[String]) -> String {
    let quoted: Vec<_> = components.iter().map(|component| format!("'{component}'")).collect();
    match quoted.as_slice() {
        [one] => format!("component {one}"),
        _ => format!("components {}", quoted.join(", ")),
    }
}

/// A path's property, the model it was found in, and the JSON subpath after its name.
pub(super) struct LocatedProperty<'p> {
    pub model: ModelId,
    pub name: &'p str,
    pub property: ResolvedProperty,
    pub subpath: &'p [String],
}

/// Find the property a path names. `Component::name` reads it from the component the qualifier names: one of
/// `components` by display name, or any component by label, including one a match may lack. A leading dotted
/// label of one of `components` (`album.name`) also qualifies the path. A bare name belongs to whichever of
/// `components` declares it, and is refused when two of them declare it as different properties.
pub(super) fn locate_property<'p, R: ModelResolver + ?Sized>(
    components: &[QueryComponent<'_>],
    resolver: &R,
    path: &'p PathExpr,
) -> Result<LocatedProperty<'p>, ModelResolutionError> {
    let Some((first, rest)) = path.steps.split_first() else {
        return Err(undeclared(components, ""));
    };
    if let Some(qualifier) = &path.component {
        let model = match components.iter().find(|component| component.name == Some(qualifier.as_str())) {
            Some(component) => component.model,
            None => resolver.resolve_model(qualifier)?.ok_or_else(|| ComponentScopeError::UnknownComponent(qualifier.clone()))?,
        };
        let property = lookup_property(&model, resolver, first)?
            .ok_or_else(|| ComponentScopeError::Undeclared { name: first.clone(), components: vec![qualifier.clone()] })?;
        return Ok(LocatedProperty { model, name: first, property, subpath: rest });
    }
    if let [name, subpath @ ..] = rest {
        if let Some(model) = resolver.resolve_model(first)?.filter(|model| components.iter().any(|component| component.model == *model)) {
            let property = lookup_property(&model, resolver, name)?.ok_or_else(|| unknown_property(&model, name.as_str()))?;
            return Ok(LocatedProperty { model, name, property, subpath });
        }
    }
    let mut found: Option<(&QueryComponent<'_>, ResolvedProperty)> = None;
    for component in components {
        let Some(property) = lookup_property(&component.model, resolver, first)? else { continue };
        match found {
            None => found = Some((component, property)),
            Some((earlier, earlier_property)) if earlier_property.id != property.id => {
                return Err(ComponentScopeError::Ambiguous {
                    name: first.clone(),
                    first: earlier.describe(),
                    second: component.describe(),
                }
                .into());
            }
            Some(_) => {}
        }
    }
    let (component, property) = found.ok_or_else(|| undeclared(components, first))?;
    Ok(LocatedProperty { model: component.model, name: first, property, subpath: rest })
}

/// The property `name` in `model`: the entity id, a system property, or a registered one.
fn lookup_property<R: ModelResolver + ?Sized>(
    model: &ModelId,
    resolver: &R,
    name: &str,
) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
    if name == "id" {
        return Ok(Some(ResolvedProperty { id: PropertyId::Id, value_type: ValueType::EntityId }));
    }
    Ok(match model {
        ModelId::System(model) => resolve_system_property(*model, name)
            .map(|system| ResolvedProperty { id: PropertyId::System(system), value_type: system_property_value_type(system) }),
        ModelId::EntityId(_) => resolver.resolve_property(model, name)?,
    })
}

/// A bare name no component declares. A one-component query keeps its one-model error.
fn undeclared(components: &[QueryComponent<'_>], name: &str) -> ModelResolutionError {
    match components {
        [only] => unknown_property(&only.model, name),
        _ => {
            ComponentScopeError::Undeclared { name: name.to_owned(), components: components.iter().map(QueryComponent::describe).collect() }
                .into()
        }
    }
}
