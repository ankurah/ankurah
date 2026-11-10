//! Which of a query's components a property name belongs to.

use ankql::ast::{Expr, Parsed, PathExpr, Predicate, Selection};
use ankurah_proto::{ModelId, PropertyId};
use thiserror::Error;

use super::{resolve_system_property, system_property_value_type, unknown_property, ModelResolutionError, ModelResolver, ResolvedProperty};
use crate::internal::prelude::*;
use crate::value::ValueType;

/// A component a query returns: every match is a member of it, and its property names need no qualifier.
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

/// Which of `members` a `Component::` qualifier names: the one with that display name, else the one with that
/// label. A display name that two different components share names neither.
fn member_named<'a>(
    members: impl IntoIterator<Item = Option<ComponentNames<'a>>>,
    qualifier: &str,
) -> Result<Option<usize>, ComponentScopeError> {
    let members: Vec<_> = members.into_iter().collect();
    let mut by_name = members.iter().enumerate().filter_map(|(index, names)| Some((index, names.filter(|names| names.name == qualifier)?)));
    if let Some((index, first)) = by_name.next() {
        if let Some((_, second)) = by_name.find(|(_, other)| other.label != first.label) {
            return Err(ComponentScopeError::AmbiguousQualifier {
                qualifier: qualifier.to_owned(),
                first: first.label.to_owned(),
                second: second.label.to_owned(),
            });
        }
        return Ok(Some(index));
    }
    Ok(members.iter().position(|names| names.is_some_and(|names| names.label == qualifier)))
}

/// A property name that a query's components cannot place on exactly one property.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ComponentScopeError {
    /// A bare name two of the query's components declare as different properties.
    #[error(
        "'{name}' names different properties in components '{first}' and '{second}'; qualify it as {}::{name} or {}::{name}",
        qualifier_spelling(.first),
        qualifier_spelling(.second)
    )]
    Ambiguous { name: String, first: String, second: String },
    /// A display name two different components of the query share; only their labels tell them apart.
    #[error(
        "'{qualifier}' is the display name of components '{first}' and '{second}'; qualify by label, as {}:: or {}::",
        qualifier_spelling(.first),
        qualifier_spelling(.second)
    )]
    AmbiguousQualifier { qualifier: String, first: String, second: String },
    /// A qualifier naming neither a component of the query by display name nor any component by label.
    #[error("unknown component '{0}': a qualifier names one of the query's components by name, or any component by its label")]
    UnknownComponent(String),
    /// An ORDER BY key on a component outside the query. An entity without that component has no value
    /// to sort by, and some engines take sort candidates only from entities that have one.
    #[error("ORDER BY {component}::{name} sorts by a component outside the query; add {component} to the query's tuple")]
    SortKeyOutsideQuery { component: String, name: String },
    /// A name that none of these components declares.
    #[error("no property '{name}' in {}", describe_components(.components))]
    Undeclared { name: String, components: Vec<String> },
}

/// How a qualifier writes `name`: bare when the grammar reads it as one identifier, double-quoted otherwise.
fn qualifier_spelling(name: &str) -> String {
    let plain = name.chars().next().is_some_and(|first| !first.is_ascii_digit())
        && name.chars().all(|char| char.is_ascii_alphanumeric() || char == '_' || char == '-');
    if plain {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
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
/// `components` by display name, or any component by label, including one a match may lack. A selection over
/// one component also reads a leading dotted label of that component as its qualifier (`album.name`); over
/// several, a dot only ever starts a subpath. A bare name belongs to whichever of `components` declares it,
/// and is refused when two of them declare it as different properties.
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
        let in_query = member_named(components.iter().map(|component| component.names), qualifier)?.is_some()
            || resolver.resolve_model(qualifier)?.is_some_and(|model| components.iter().any(|component| component.model == model));
        if !in_query {
            let name = path.steps.first().cloned().unwrap_or_default();
            return Err(ComponentScopeError::SortKeyOutsideQuery { component: qualifier.clone(), name }.into());
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

/// Check a live query's names against its components' declarations before their durable identities are
/// bound, refusing what resolution would refuse by name alone. A qualifier naming no component of the
/// query is left to resolution, which finds that component through the catalog; whether two components
/// declare a bare name as one property or two is known only once their identities are bound.
pub(crate) fn validate_selection_names(schemas: &[&ModelStructDescriptor], selection: &Selection<Parsed>) -> Result<(), RetrievalError> {
    let members: Vec<ComponentNames> = schemas.iter().map(|schema| ComponentNames::from(*schema)).collect();
    let declares = |schema: &ModelStructDescriptor, name: &str| name == "id" || schema.field_by_name(name).is_some();
    let check = |path: &PathExpr, sort_key: bool| -> Result<(), RetrievalError> {
        let Some((first, rest)) = path.steps.split_first() else { return Ok(()) };
        if let Some(qualifier) = &path.component {
            return match member_named(members.iter().copied().map(Some), qualifier)? {
                Some(index) if !declares(schemas[index], first) => {
                    Err(ComponentScopeError::Undeclared { name: first.clone(), components: vec![qualifier.clone()] }.into())
                }
                Some(_) => Ok(()),
                None if sort_key => {
                    Err(ComponentScopeError::SortKeyOutsideQuery { component: qualifier.clone(), name: first.clone() }.into())
                }
                None => Ok(()),
            };
        }
        // A selection over one component reads a leading dotted label of that component as its qualifier.
        if let ([only], [name, ..]) = (schemas, rest) {
            if *first == only.label {
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
                name: first.clone(),
                components: members.iter().map(|names| names.name.to_owned()).collect(),
            }
            .into(),
        })
    };
    check_predicate_names(&selection.predicate, &|path| check(path, false))?;
    for item in selection.order_by.iter().flatten() {
        check(&item.path, true)?;
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
            Predicate::And(..)
            | Predicate::Or(..)
            | Predicate::Not(_)
            | Predicate::MemberOf(_)
            | Predicate::True
            | Predicate::False
            | Predicate::Placeholder => Ok(()),
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
    fn a_qualifier_matches_a_display_name_before_a_label() {
        // The second component's display name is the first one's label.
        let components = [component(1, "Task", "assignment"), component(2, "assignment", "task")];
        assert_eq!(locate(&components, &path(Some("assignment"), &["note"])).unwrap(), (model(2), property(21)));
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
        assert_eq!(
            qualified.to_string(),
            "'Task' is the display name of components 'task' and 'other.task'; qualify by label, as task:: or \"other.task\"::"
        );
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
        assert_eq!(
            error.to_string(),
            "'note' names different properties in components 'task' and 'other.task'; qualify it as task::note or \"other.task\"::note"
        );
    }

    #[test]
    fn a_component_named_twice_is_one_component() {
        assert_eq!(locate(&[task(), task()], &path(None, &["note"])).unwrap(), (model(1), property(11)));
        assert_eq!(locate(&[task(), task()], &path(Some("Task"), &["note"])).unwrap(), (model(1), property(11)));
    }
}
