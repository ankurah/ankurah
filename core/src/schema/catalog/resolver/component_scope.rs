//! Which of a query's components a property name belongs to.

use ankql::ast::{Parsed, PathExpr, Selection};
use ankurah_proto::{ModelId, PropertyId};
use thiserror::Error;

use super::{resolve_system_property, system_property_value_type, unknown_property, ModelResolutionError, ModelResolver, ResolvedProperty};
use crate::internal::prelude::*;
use crate::value::ValueType;

/// A component whose property names a selection may write without a qualifier, and the names a `Component::`
/// qualifier may use for it. A typed query also requires every match to be a member of each of its components;
/// a one-model selection, such as a policy predicate, only reads its names.
#[derive(Debug, Clone, Copy)]
pub(super) struct QueryComponent<'a> {
    pub model: ModelId,
    /// The names a `Component::` qualifier may use for it. A component known only by its model id has none,
    /// and a qualifier finds it by label through the resolver.
    pub names: Option<ComponentNames<'a>>,
}

impl QueryComponent<'_> {
    /// How an error names this component.
    fn describe(&self) -> String { self.names.map_or_else(|| self.model.to_string(), |names| names.name.to_owned()) }
}

/// The names a selection's text may give a component: its display name (`Assignment`) and its label (`assignment`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ComponentNames<'a> {
    pub name: &'a str,
    pub label: &'a str,
}

impl<'a> From<&'a ModelStructDescriptor> for ComponentNames<'a> {
    fn from(schema: &'a ModelStructDescriptor) -> Self { Self { name: schema.name, label: schema.label } }
}

/// Which of `members` a `Component::` qualifier names, by display name or by label. A qualifier that names two
/// different components, by either kind of name, is refused as `AmbiguousQualifier`.
fn member_named<'a>(
    members: impl IntoIterator<Item = Option<ComponentNames<'a>>>,
    qualifier: &str,
) -> Result<Option<usize>, ComponentScopeError> {
    let mut found: Option<(usize, ComponentNames<'a>)> = None;
    for (index, names) in members.into_iter().enumerate() {
        let Some(names) = names.filter(|names| names.name == qualifier || names.label == qualifier) else { continue };
        match found {
            Some((_, first)) if first.label != names.label => {
                return Err(ComponentScopeError::AmbiguousQualifier {
                    qualifier: qualifier.to_owned(),
                    first: first.label.to_owned(),
                    second: names.label.to_owned(),
                });
            }
            Some(_) => {}
            None => found = Some((index, names)),
        }
    }
    Ok(found.map(|(index, _)| index))
}

/// A name in a selection that the query's components refuse: an ambiguous or unknown name or qualifier, or a sort
/// key outside the query's tuple.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ComponentScopeError {
    /// A bare name two of the query's components declare as different properties.
    #[error(
        "'{name}' names different properties in components '{first}' and '{second}'; qualify it with its component, as Component::property"
    )]
    Ambiguous { name: String, first: String, second: String },
    /// A qualifier that names two different components of the query, as a display name or as a label.
    #[error("'{qualifier}' names two components of the query, labelled '{first}' and '{second}'; qualify by a label that names only one")]
    AmbiguousQualifier { qualifier: String, first: String, second: String },
    /// A qualifier naming neither a component of the query by display name nor any component by label.
    #[error("unknown component '{0}': a qualifier names one of the query's components by name, or any component by its label")]
    UnknownComponent(String),
    /// An ORDER BY key whose qualifier names no component in the query's tuple, whether a component outside it
    /// or none at all. An entity without that component has no value to sort by, and some engines take sort
    /// candidates only from entities that have one.
    #[error("ORDER BY names '{name}' of component '{component}', which is not in the query's tuple; sort by a property of a component the query returns, or add that component's view to the tuple")]
    SortKeyOutsideQuery { component: String, name: String },
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

/// Find the property a path names:
/// - `Q::name`: `Q` is one of `components`, by display name or label. Only if none has that name is it a
///   component the resolver knows by label, which a match may lack. `Q` naming two of `components` is refused.
/// - A bare name is asked of every one of `components`, and is refused when two declare it as different
///   properties.
/// - Over one component, a leading dotted label of that component is its qualifier (`album.name`), also when
///   a property has that name. Over several, a dot only starts a subpath.
/// - `id` is the entity's id under any qualifier or none, before any declared property.
///
/// A qualifier says which property is meant and adds no membership test: on an entity that lacks the
/// component, its properties read as missing, except `id` and a property shared with a component it has.
pub(super) fn locate_property<'p, R: ModelResolver + ?Sized>(
    components: &[QueryComponent<'_>],
    resolver: &R,
    path: &'p PathExpr,
) -> Result<LocatedProperty<'p>, ModelResolutionError> {
    let Some((first, rest)) = path.steps.split_first() else {
        return Err(undeclared(components, ""));
    };
    if let Some(qualifier) = &path.component {
        let model = match member_named(components.iter().map(|component| component.names), qualifier)? {
            Some(index) => components[index].model,
            None => resolver.resolve_model(qualifier)?.ok_or_else(|| ComponentScopeError::UnknownComponent(qualifier.clone()))?,
        };
        let property = lookup_property(&model, resolver, first)?
            .ok_or_else(|| ComponentScopeError::Undeclared { name: first.clone(), components: vec![qualifier.clone()] })?;
        return Ok(LocatedProperty { model, name: first, property, subpath: rest });
    }
    if let ([only], [name, subpath @ ..]) = (components, rest) {
        if resolver.resolve_model(first)?.as_ref() == Some(&only.model) {
            let property = lookup_property(&only.model, resolver, name)?.ok_or_else(|| unknown_property(&only.model, name.as_str()))?;
            return Ok(LocatedProperty { model: only.model, name, property, subpath });
        }
    }
    let mut found: Option<(&QueryComponent<'_>, ResolvedProperty)> = None;
    for component in components {
        let Some(property) = lookup_property(&component.model, resolver, first)? else { continue };
        match found {
            None => found = Some((component, property)),
            Some((earlier, earlier_property)) if earlier_property.id != property.id => {
                // Components that share a display name are told apart by label.
                let (first_component, second_component) = match (earlier.names, component.names) {
                    (Some(earlier), Some(later)) if earlier.name == later.name => (earlier.label.to_owned(), later.label.to_owned()),
                    _ => (earlier.describe(), component.describe()),
                };
                return Err(ComponentScopeError::Ambiguous { name: first.clone(), first: first_component, second: second_component }.into());
            }
            Some(_) => {}
        }
    }
    let (component, property) = found.ok_or_else(|| undeclared(components, first))?;
    Ok(LocatedProperty { model: component.model, name: first, property, subpath: rest })
}

/// Find the property an ORDER BY key names, which must belong to one of `components`.
pub(super) fn locate_sort_key<'p, R: ModelResolver + ?Sized>(
    components: &[QueryComponent<'_>],
    resolver: &R,
    path: &'p PathExpr,
) -> Result<LocatedProperty<'p>, ModelResolutionError> {
    if let Some(qualifier) = &path.component {
        let Some(name) = path.steps.first() else { return Err(undeclared(components, "")) };
        let in_query = member_named(components.iter().map(|component| component.names), qualifier)?.is_some()
            || resolver.resolve_model(qualifier)?.is_some_and(|model| components.iter().any(|component| component.model == model));
        if !in_query {
            return Err(ComponentScopeError::SortKeyOutsideQuery { component: qualifier.clone(), name: name.clone() }.into());
        }
    }
    locate_property(components, resolver, path)
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

/// A bare name no component declares: `UnknownProperty` for a one-component selection, `Undeclared` across several.
fn undeclared(components: &[QueryComponent<'_>], name: &str) -> ModelResolutionError {
    match components {
        [only] => unknown_property(&only.model, name),
        _ => {
            ComponentScopeError::Undeclared { name: name.to_owned(), components: components.iter().map(QueryComponent::describe).collect() }
                .into()
        }
    }
}

/// Check a live query's names against its components' declarations before their durable identities are
/// bound, refusing what resolution would refuse by name alone. A predicate's qualifier naming no component of
/// the query is left to resolution, which finds that component through the catalog; whether two components
/// declare a bare name as one property or two is known only once their identities are bound.
pub(crate) fn validate_selection_names(schemas: &[&ModelStructDescriptor], selection: &Selection<Parsed>) -> Result<(), RetrievalError> {
    let members: Vec<ComponentNames> = schemas.iter().map(|schema| ComponentNames::from(*schema)).collect();
    let declares = |schema: &ModelStructDescriptor, name: &str| name == "id" || schema.field_by_name(name).is_some();
    let check = |path: &PathExpr, clause: Clause| -> Result<(), RetrievalError> {
        // A path with no steps names the property `` and is refused as undeclared.
        let (first, rest) = path.steps.split_first().map_or(("", &[][..]), |(first, rest)| (first.as_str(), rest));
        if let Some(qualifier) = &path.component {
            return match member_named(members.iter().copied().map(Some), qualifier)? {
                // Several of the query's descriptors may declare one component, and any of them may declare the property.
                Some(index) if !schemas.iter().any(|schema| schema.label == schemas[index].label && declares(schema, first)) => {
                    Err(ComponentScopeError::Undeclared { name: first.to_owned(), components: vec![qualifier.clone()] }.into())
                }
                Some(_) => Ok(()),
                None if clause == Clause::OrderBy => {
                    Err(ComponentScopeError::SortKeyOutsideQuery { component: qualifier.clone(), name: first.to_owned() }.into())
                }
                None => Ok(()),
            };
        }
        // A selection over one component reads a leading dotted label of that component as its qualifier.
        if let ([only], [name, ..]) = (schemas, rest) {
            if first == only.label {
                return if declares(only, name) {
                    Ok(())
                } else {
                    Err(RetrievalError::Other(format!("unknown property '{name}' in model '{}'", only.label)))
                };
            }
        }
        if schemas.iter().any(|schema| declares(schema, first)) {
            return Ok(());
        }
        Err(match schemas {
            [only] => RetrievalError::Other(format!("unknown property '{first}' in model '{}'", only.label)),
            _ => ComponentScopeError::Undeclared {
                name: first.to_owned(),
                components: members.iter().map(|names| names.name.to_owned()).collect(),
            }
            .into(),
        })
    };
    super::check_predicate_names(&selection.predicate, &|path| check(path, Clause::Predicate))?;
    for item in selection.order_by.iter().flatten() {
        check(&item.path, Clause::OrderBy)?;
    }
    Ok(())
}

/// The part of a selection a name stands in; an ORDER BY key must belong to a component of the query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Clause {
    Predicate,
    OrderBy,
}

#[cfg(test)]
mod tests {
    use ankurah_proto::EntityId;

    use super::*;

    fn model(id: u8) -> ModelId { ModelId::EntityId(EntityId::from_bytes([id; 32])) }
    fn property(id: u8) -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([id; 32])) }

    /// Task and Assignment share `project` and each declare their own `note`; Closure stays outside the tuples below.
    struct Catalog;

    impl ModelResolver for Catalog {
        fn resolve_model(&self, label: &str) -> Result<Option<ModelId>, ModelResolutionError> {
            Ok(match label {
                "task" => Some(model(1)),
                "assignment" => Some(model(2)),
                "records.closure" => Some(model(3)),
                _ => None,
            })
        }

        fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            let ModelId::EntityId(model) = model else { return Ok(None) };
            let id = match (model.to_bytes()[0], name) {
                (1, "status") => 10,
                (1, "note") => 11,
                (1 | 2, "project") => 12,
                (2, "assignee") => 20,
                (2, "note") => 21,
                (3, "closed_at") => 30,
                _ => return Ok(None),
            };
            Ok(Some(ResolvedProperty { id: property(id), value_type: ValueType::String }))
        }
    }

    fn component(id: u8, name: &'static str, label: &'static str) -> QueryComponent<'static> {
        QueryComponent { model: model(id), names: Some(ComponentNames { name, label }) }
    }

    fn task() -> QueryComponent<'static> { component(1, "Task", "task") }
    fn assignment() -> QueryComponent<'static> { component(2, "Assignment", "assignment") }

    fn path(component: Option<&str>, steps: &[&str]) -> PathExpr {
        PathExpr { component: component.map(str::to_owned), steps: steps.iter().map(|step| step.to_string()).collect() }
    }

    /// The model and property a path resolves to across `components`.
    fn locate(components: &[QueryComponent<'_>], path: &PathExpr) -> Result<(ModelId, PropertyId), ModelResolutionError> {
        locate_property(components, &Catalog, path).map(|found| (found.model, found.property.id))
    }

    #[test]
    fn a_qualifier_that_is_one_component_s_display_name_and_another_s_label_is_refused() {
        // The second component's display name is the first one's label.
        let components = [component(1, "Task", "assignment"), component(2, "assignment", "task")];
        let refused = locate(&components, &path(Some("assignment"), &["note"])).unwrap_err();
        let expected =
            ComponentScopeError::AmbiguousQualifier { qualifier: "assignment".into(), first: "assignment".into(), second: "task".into() };
        assert!(matches!(refused, ModelResolutionError::ComponentScope(ref found) if *found == expected), "{refused:?}");
        assert_eq!(locate(&components, &path(Some("Task"), &["note"])).unwrap(), (model(1), property(11)));
    }

    #[test]
    fn a_bare_name_two_components_declare_as_one_property_resolves() {
        assert_eq!(locate(&[task(), assignment()], &path(None, &["project"])).unwrap(), (model(1), property(12)));
        assert_eq!(locate(&[task(), assignment()], &path(None, &["assignee"])).unwrap(), (model(2), property(20)));
    }

    #[test]
    fn a_bare_name_two_components_declare_as_different_properties_is_ambiguous() {
        let error = locate(&[task(), assignment()], &path(None, &["note"])).unwrap_err();
        let expected = ComponentScopeError::Ambiguous { name: "note".into(), first: "Task".into(), second: "Assignment".into() };
        assert!(matches!(error, ModelResolutionError::ComponentScope(found) if found == expected));
    }

    #[test]
    fn id_is_the_entity_id_in_every_component() {
        assert_eq!(locate(&[task(), assignment()], &path(None, &["id"])).unwrap(), (model(1), PropertyId::Id));
        assert_eq!(locate(&[task(), assignment()], &path(Some("Assignment"), &["id"])).unwrap(), (model(2), PropertyId::Id));
    }

    #[test]
    fn a_qualifier_reads_a_component_outside_the_tuple_by_label_but_never_sorts_by_it() {
        let outside = path(Some("records.closure"), &["closed_at"]);
        assert_eq!(locate(&[task(), assignment()], &outside).unwrap(), (model(3), property(30)));
        let error = locate_sort_key(&[task(), assignment()], &Catalog, &outside).map(|_| ()).unwrap_err();
        assert!(matches!(error, ModelResolutionError::ComponentScope(ComponentScopeError::SortKeyOutsideQuery { .. })));
        let unknown = locate(&[task(), assignment()], &path(Some("Closure"), &["closed_at"])).unwrap_err();
        assert!(
            matches!(unknown, ModelResolutionError::ComponentScope(ComponentScopeError::UnknownComponent(found)) if found == "Closure")
        );
    }

    #[test]
    fn two_components_with_one_display_name_are_told_apart_by_label() {
        let components = [task(), component(4, "Task", "other.task")];
        let qualified = locate(&components, &path(Some("Task"), &["note"])).unwrap_err();
        let expected =
            ComponentScopeError::AmbiguousQualifier { qualifier: "Task".into(), first: "task".into(), second: "other.task".into() };
        assert!(matches!(qualified, ModelResolutionError::ComponentScope(ref found) if *found == expected));
        assert_eq!(locate(&components, &path(Some("task"), &["note"])).unwrap(), (model(1), property(11)));
    }

    #[test]
    fn a_bare_name_ambiguous_between_components_with_one_display_name_names_their_labels() {
        struct Shadowed;
        impl ModelResolver for Shadowed {
            fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
                let ModelId::EntityId(model) = model else { return Ok(None) };
                Ok((name == "note").then(|| ResolvedProperty { id: property(model.to_bytes()[0]), value_type: ValueType::String }))
            }
        }
        let components = [task(), component(4, "Task", "other.task")];
        let error = locate_property(&components, &Shadowed, &path(None, &["note"])).map(|_| ()).unwrap_err();
        let expected = ComponentScopeError::Ambiguous { name: "note".into(), first: "task".into(), second: "other.task".into() };
        assert!(matches!(error, ModelResolutionError::ComponentScope(ref found) if *found == expected), "{error:?}");
    }

    #[test]
    fn a_component_named_twice_is_one_component() {
        assert_eq!(locate(&[task(), task()], &path(None, &["note"])).unwrap(), (model(1), property(11)));
        assert_eq!(locate(&[task(), task()], &path(Some("Task"), &["note"])).unwrap(), (model(1), property(11)));
    }
}
