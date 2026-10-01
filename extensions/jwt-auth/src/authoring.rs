use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{Expr, Parsed, Predicate, Resolved};
use ankurah::{CachePolicy, Context, Ref};
use ankurah::transaction::Transaction;
use ankurah::proto::{ModelId, PropertyId};
use ankurah_core::schema::catalog::CatalogManager;
use ankurah_core::schema::registration::RegistrationPlan;
use ankurah_core::schema::resolver::{ModelResolutionError, ModelResolver, ResolvedProperty, resolve_selection};
use ankurah_core_types::ValueType;

use crate::{PolicyCatalog, PolicyConfig, bound_predicate::BoundPredicate, graph::PolicyGraph, model::*};

/// Replace rule entities in one transaction, retaining role/privilege identities and retiring old rules.
pub(crate) async fn set_policy(context: &Context, catalog: &CatalogManager, config: &PolicyConfig, pem: String) -> anyhow::Result<()> {
    let mut graph = PolicyGraph::fetch(context, CachePolicy::Durable).await?;
    let transaction = context.begin();
    for (id, rule) in &mut graph.grants {
        if rule.status == RuleStatus::Active {
            transaction.get::<RolePrivilege>(id).await?.status()?.set(&RuleStatus::Retired)?;
            rule.status = RuleStatus::Retired;
        }
    }
    for (id, rule) in &mut graph.models {
        if rule.status == RuleStatus::Active {
            transaction.get::<ModelPolicy>(id).await?.status()?.set(&RuleStatus::Retired)?;
            rule.status = RuleStatus::Retired;
        }
    }
    for (id, rule) in &mut graph.scopes {
        if rule.status == RuleStatus::Active {
            transaction.get::<PolicyScope>(id).await?.status()?.set(&RuleStatus::Retired)?;
            rule.status = RuleStatus::Retired;
        }
    }

    let mut roles: BTreeMap<_, _> = graph.roles.iter().map(|(id, role)| (role.name.clone(), *id)).collect();
    let mut privileges: BTreeMap<_, _> = graph.privileges.iter().map(|(id, privilege)| (privilege.name.clone(), *id)).collect();
    let needed_privileges: BTreeSet<_> = config.roles.values().flatten().chain(config.collections.values().flat_map(|rule| {
        rule.read.iter().chain(&rule.retrieve).chain(&rule.write).chain(rule.scope.iter().filter_map(|scope| scope.unless_privilege.as_ref()))
    })).collect();
    for name in needed_privileges {
        if !privileges.contains_key(name) {
            let id = transaction.create(&Privilege { name: name.clone() }).await?.id();
            privileges.insert(name.clone(), id);
        }
    }
    for (name, granted) in &config.roles {
        let role = match roles.get(name) {
            Some(id) => *id,
            None => {
                let id = transaction.create(&Role { name: name.clone() }).await?.id();
                roles.insert(name.clone(), id);
                id
            }
        };
        for name in granted.iter().collect::<BTreeSet<_>>() {
            transaction.create(&RolePrivilege { role: role.into(), privilege: privileges[name].into(), status: RuleStatus::Active }).await?;
        }
    }
    for (label, rule) in &config.collections {
        let model = unique_model(catalog, label)?.map_or(Binding::Pending, Binding::AtPolicySet);
        let privilege = |name: &Option<String>| name.as_ref().map(|name| Ref::new(privileges[name]));
        let row = ModelPolicy {
            label: label.clone(), model,
            read: privilege(&rule.read), retrieve: privilege(&rule.retrieve), write: privilege(&rule.write),
            scope_count: rule.scope.len().try_into()?, status: RuleStatus::Active,
        };
        let policy = transaction.create(&row).await?.id();
        graph.models.insert(policy, row);
        let mut property_labels = BTreeSet::new();
        for scope in &rule.scope {
            let (parsed, _) = crate::variables::parse_template(&scope.filter)?;
            collect_property_labels(&parsed, label, &mut property_labels);
            let row = PolicyScope {
                policy: policy.into(), filter: scope.filter.clone(), unless_privilege: privilege(&scope.unless_privilege),
                applies_to: scope.applies_to, resolved: None, status: RuleStatus::Active,
            };
            let id = transaction.create(&row).await?.id();
            graph.scopes.insert(id, row);
        }
        for label in property_labels {
            let row = PolicyProperty { policy: policy.into(), label, property: Binding::Pending, value_type: None };
            let id = transaction.create(&row).await?.id();
            graph.properties.insert(id, row);
        }
    }
    bind_properties_and_scopes(&transaction, &mut graph, catalog, false).await?;
    anyhow::ensure!(graph.keys.len() <= 1, "multiple JWT verification keys are installed");
    match graph.keys.keys().next() {
        Some(id) => { transaction.get::<JwtVerificationKey>(id).await?.public_key_pem()?.set(&pem)?; }
        None => { transaction.create(&JwtVerificationKey { public_key_pem: pem }).await?; }
    }
    transaction.commit().await?;
    Ok(())
}

/// Fill only pending bindings, in the same transaction that creates their schema identities.
pub(crate) async fn schema_registered(
    context: &Context, catalog: &CatalogManager, transaction: &Transaction, plan: &RegistrationPlan,
) -> anyhow::Result<()> {
    let mut graph = PolicyGraph::fetch(context, CachePolicy::Durable).await?;
    for (id, policy) in &mut graph.models {
        if policy.status != RuleStatus::Active || policy.model.id().is_some() { continue; }
        if let Some((model, _)) = plan.creates_models.iter().find(|(_, model)| model.label == policy.label) {
            policy.model = Binding::AtRegistration(ModelId::EntityId(*model));
            transaction.get::<ModelPolicy>(id).await?.model()?.set(&policy.model)?;
            tracing::warn!(policy = %id, model = %model, "JWT policy bound by first matching schema registration");
        }
    }
    bind_properties_and_scopes(transaction, &mut graph, &RegistrationCatalog { catalog, plan }, true).await
}

async fn bind_properties_and_scopes(
    transaction: &Transaction, graph: &mut PolicyGraph, catalog: &dyn PolicyCatalog, at_registration: bool,
) -> anyhow::Result<()> {
    for (id, property) in &mut graph.properties {
        if property.property.id().is_some() { continue; }
        let Some(policy) = graph.models.get(&property.policy.id()).filter(|policy| policy.status == RuleStatus::Active) else { continue };
        let Some(model) = policy.model.id() else { continue };
        let Some(resolved) = catalog.property(&model, &property.label).map_err(anyhow::Error::msg)? else { continue };
        property.property = if at_registration { Binding::AtRegistration(resolved.id) } else { Binding::AtPolicySet(resolved.id) };
        property.value_type = Some(type_label(resolved.value_type).into());
        let edit = transaction.get::<PolicyProperty>(id).await?;
        edit.property()?.set(&property.property)?;
        edit.value_type()?.set(&property.value_type)?;
    }
    for (id, scope) in &mut graph.scopes {
        if scope.status != RuleStatus::Active || scope.resolved.is_some() { continue; }
        let Some(policy) = graph.models.get(&scope.policy.id()).filter(|policy| policy.status == RuleStatus::Active) else { continue };
        let Some(model) = policy.model.id() else { continue };
        let properties: Vec<_> = graph.properties.values().filter(|property| property.policy.id() == scope.policy.id()).collect();
        if properties.iter().any(|property| property.property.id().is_none()) { continue; }
        let bound = BoundPredicate::bind(&scope.filter, &model, &StoredBindings { model, label: &policy.label, properties })?;
        let resolved = transaction.create(&ResolvedPolicyScope { predicate: ScopePredicate(bound.predicate) }).await?.id();
        for (position, (variable, value_type)) in bound.parameters.into_iter().enumerate() {
            transaction.create(&ClaimParameter {
                scope: resolved.into(), position: position.try_into()?, variable, value_type: value_type.map(|ty| type_label(ty).into()),
            }).await?;
        }
        scope.resolved = Some(resolved.into());
        transaction.get::<PolicyScope>(id).await?.resolved()?.set(&scope.resolved)?;
    }
    Ok(())
}

fn unique_model(catalog: &dyn PolicyCatalog, label: &str) -> anyhow::Result<Option<ModelId>> {
    if let Some(model) = ankurah_core::schema::system_model_id(label) { return Ok(Some(model)); }
    let matches: Vec<_> = catalog.model_labels().into_iter().filter(|(_, found)| found == label).collect();
    anyhow::ensure!(matches.len() <= 1, "model label '{label}' is ambiguous");
    Ok(matches.first().map(|(id, _)| *id))
}

/// Resolve a scope using only the property identities already persisted for its policy.
struct StoredBindings<'a> {
    model: ModelId,
    label: &'a str,
    properties: Vec<&'a PolicyProperty>,
}

impl ModelResolver for StoredBindings<'_> {
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> { Ok((name == self.label).then_some(self.model)) }
    fn resolve_property(&self, _: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        Ok(self.properties.iter().find(|property| property.label == name).and_then(|property| Some(ResolvedProperty {
            id: property.property.id()?, value_type: ValueType::from_property_str(property.value_type.as_deref()?)?,
        })))
    }
}

impl PolicyCatalog for StoredBindings<'_> {
    fn model_labels(&self) -> Vec<(ModelId, String)> { vec![(self.model, self.label.into())] }
    fn property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, String> {
        self.resolve_property(model, name).map_err(|error| error.to_string())
    }
    fn property_type(&self, _: &ModelId, property: &PropertyId) -> Result<ValueType, String> {
        if *property == PropertyId::Id { return Ok(ValueType::EntityId); }
        self.properties.iter().find(|row| row.property.id() == Some(*property))
            .and_then(|row| row.value_type.as_deref()).and_then(ValueType::from_property_str)
            .ok_or_else(|| "bound property has no value type".into())
    }
    fn resolve_predicate(&self, model: &ModelId, predicate: Predicate<Parsed>) -> Result<Predicate<Resolved>, String> {
        resolve_selection(model, self, predicate.into()).map(|selection| selection.predicate).map_err(|error| error.to_string())
    }
}

/// The registration transaction's new rows overlay the committed catalog until commit.
struct RegistrationCatalog<'a> { catalog: &'a CatalogManager, plan: &'a RegistrationPlan }

impl ModelResolver for RegistrationCatalog<'_> {
    fn resolve_model(&self, name: &str) -> Result<Option<ModelId>, ModelResolutionError> {
        Ok(self.plan.creates_models.iter().find(|(_, row)| row.label == name).map(|(id, _)| ModelId::EntityId(*id))
            .or(self.catalog.model_id_for(name)?))
    }
    fn resolve_property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
        if let ModelId::EntityId(model_id) = model {
            for update in &self.plan.updates {
                if update.collection == ModelId::System(ankurah::proto::SystemModel::Property) && update.field == "name"
                    && update.to.as_ref() == Some(&ankurah::value::Value::String(name.into()))
                    && self.catalog.membership(model_id, &update.entity)?.is_some() {
                    let id = PropertyId::EntityId(update.entity);
                    return Ok(Some(ResolvedProperty { id, value_type: self.catalog.registered_value_type(model, &id)? }));
                }
            }
        }
        for membership in &self.plan.creates_memberships {
            if *model != ModelId::EntityId(membership.model) { continue; }
            let row = self.plan.creates_properties.iter().find(|(id, _)| *id == membership.property).map(|(_, row)| row.clone())
                .or(self.catalog.property_by_id(&membership.property)?);
            if let Some(row) = row.filter(|row| row.name == name) {
                return Ok(ValueType::from_property_str(&row.value_type).map(|value_type| ResolvedProperty {
                    id: PropertyId::EntityId(membership.property), value_type,
                }));
            }
        }
        self.catalog.resolve_property(model, name)
    }
}

impl PolicyCatalog for RegistrationCatalog<'_> {
    fn model_labels(&self) -> Vec<(ModelId, String)> { self.catalog.model_labels() }
    fn property(&self, model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, String> {
        self.resolve_property(model, name).map_err(|error| error.to_string())
    }
    fn property_type(&self, model: &ModelId, property: &PropertyId) -> Result<ValueType, String> {
        if let Some((_, row)) = self.plan.creates_properties.iter().find(|(id, _)| PropertyId::EntityId(*id) == *property) {
            return ValueType::from_property_str(&row.value_type).ok_or_else(|| "unknown registered value type".into());
        }
        self.catalog.property_type(model, property)
    }
    fn resolve_predicate(&self, model: &ModelId, predicate: Predicate<Parsed>) -> Result<Predicate<Resolved>, String> {
        resolve_selection(model, self, predicate.into()).map(|selection| selection.predicate).map_err(|error| error.to_string())
    }
}

fn collect_property_labels(predicate: &Predicate<Parsed>, model: &str, labels: &mut BTreeSet<String>) {
    predicate.walk((), &mut |(), predicate| match predicate {
        Predicate::Comparison { left, right, .. } => { collect_expr_labels(left, model, labels); collect_expr_labels(right, model, labels); }
        Predicate::IsNull(expr) => collect_expr_labels(expr, model, labels),
        _ => {}
    });
}

fn collect_expr_labels(expr: &Expr<Parsed>, model: &str, labels: &mut BTreeSet<String>) {
    match expr {
        Expr::Path(path) => {
            let index = usize::from(path.steps.len() > 1 && path.steps[0] == model);
            if let Some(name) = path.steps.get(index).filter(|name| *name != "id") { labels.insert(name.clone()); }
        }
        Expr::ExprList(items) => for item in items { collect_expr_labels(item, model, labels); },
        Expr::InfixExpr { left, right, .. } => { collect_expr_labels(left, model, labels); collect_expr_labels(right, model, labels); }
        Expr::Predicate(predicate) => collect_property_labels(predicate, model, labels),
        _ => {}
    }
}

pub(crate) fn type_label(ty: ValueType) -> &'static str {
    match ty {
        ValueType::I16 => "i16", ValueType::I32 => "i32", ValueType::I64 => "i64", ValueType::F64 => "f64", ValueType::Bool => "bool",
        ValueType::String => "string", ValueType::EntityId => "entityid", ValueType::Object => "object", ValueType::Binary => "binary", ValueType::Json => "json",
    }
}
