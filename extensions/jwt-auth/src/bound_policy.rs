use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use ankql::ast::{Predicate, Resolved};
use ankurah_core::{
    entity::Entity,
    policy::AccessDenied,
    selection::filter::{evaluate_predicate, Filterable},
    util::Iterable,
};
use ankurah_proto::ModelId;

use crate::{bound_predicate::BoundPredicate, JwtContext, PolicyConfig, ScopeRuleOp};
#[cfg(feature = "test-helpers")]
use crate::PolicyCatalog;
use crate::{graph::PolicyGraph, model::RuleStatus, config::{CollectionRules, ScopeRule}};

/// Stored policy bindings. Unresolved scopes remain errors, never unrestricted grants.
pub(crate) struct BoundPolicy {
    config: Arc<PolicyConfig>,
    models: BTreeMap<ModelId, ModelRules>,
    policy_models: BTreeSet<ModelId>,
}

struct ModelRules {
    read: Option<String>,
    retrieve: Option<String>,
    write: Option<String>,
    scopes: Vec<Scope>,
}

struct Scope {
    predicate: Result<BoundPredicate, AccessDenied>,
    unless_privilege: Option<String>,
    applies_to: ScopeRuleOp,
}

#[derive(Clone, Copy)]
pub(crate) enum ReadOperation { Query, Retrieve }

impl BoundPolicy {
    /// Build the synchronous evaluator from stored bindings; missing restrictions remain denials.
    pub(crate) fn from_graph(graph: &PolicyGraph, policy_models: BTreeSet<ModelId>) -> anyhow::Result<Self> {
        let mut config = PolicyConfig::default();
        for grant in graph.grants.values().filter(|grant| grant.status == RuleStatus::Active) {
            let (Some(role), Some(privilege)) = (graph.roles.get(&grant.role.id()), graph.privileges.get(&grant.privilege.id())) else { continue };
            config.roles.entry(role.name.clone()).or_default().push(privilege.name.clone());
        }
        let privilege = |reference: &Option<ankurah::Ref<crate::Privilege>>| -> anyhow::Result<Option<String>> {
            reference.as_ref().map(|reference| graph.privileges.get(&reference.id()).map(|row| row.name.clone())
                .ok_or_else(|| anyhow::anyhow!("policy privilege {} has not arrived", reference.id()))).transpose()
        };
        let mut models = BTreeMap::new();
        for (id, policy) in graph.models.iter().filter(|(_, policy)| policy.status == RuleStatus::Active) {
            let scopes: Vec<_> = graph.scopes.values().filter(|scope| scope.policy.id() == *id && scope.status == RuleStatus::Active).collect();
            let read = privilege(&policy.read)?;
            let retrieve = privilege(&policy.retrieve)?;
            let write = privilege(&policy.write)?;
            config.collections.insert(policy.label.clone(), CollectionRules {
                read: read.clone(), retrieve: retrieve.clone(), write: write.clone(),
                scope: scopes.iter().map(|scope| Ok(ScopeRule {
                    filter: scope.filter.clone(), unless_privilege: privilege(&scope.unless_privilege)?, applies_to: scope.applies_to,
                })).collect::<anyhow::Result<_>>()?,
            });
            let Some(model) = policy.model.id() else { continue };
            let mut rules = ModelRules { read, retrieve, write, scopes: Vec::new() };
            if usize::try_from(policy.scope_count).ok() != Some(scopes.len()) {
                rules.scopes.push(Scope {
                    predicate: Err(AccessDenied::ByPolicy("policy scopes have not all arrived")),
                    unless_privilege: None, applies_to: ScopeRuleOp::ReadWrite,
                });
            } else {
                for scope in scopes {
                    rules.scopes.push(Scope {
                        predicate: stored_predicate(graph, scope),
                        unless_privilege: privilege(&scope.unless_privilege)?, applies_to: scope.applies_to,
                    });
                }
            }
            anyhow::ensure!(models.insert(model, rules).is_none(), "multiple active policy rules for model {model}");
        }
        Ok(Self { config: Arc::new(config), models, policy_models })
    }

    pub(crate) fn config(&self) -> Arc<PolicyConfig> { self.config.clone() }

    pub(crate) fn is_bound(&self, model: &ModelId) -> bool {
        self.models.get(model).is_some_and(|rules| rules.scopes.iter().all(|scope| scope.predicate.is_ok()))
    }

    #[cfg(feature = "test-helpers")]
    pub(crate) fn bind(config: Arc<PolicyConfig>, catalog: &dyn PolicyCatalog) -> Self {
        let mut models = BTreeMap::new();
        let mut policy_models = BTreeSet::new();
        for (model, label) in catalog.model_labels() {
            if label.starts_with("jwtagent_") { policy_models.insert(model); }
            let rules = config.collections.get(&label);
            models.insert(model, ModelRules {
                read: rules.and_then(|rules| rules.read.clone()),
                retrieve: rules.and_then(|rules| rules.retrieve.clone()),
                write: rules.and_then(|rules| rules.write.clone()),
                scopes: rules.into_iter().flat_map(|rules| &rules.scope).map(|rule| Scope {
                    predicate: BoundPredicate::bind(&rule.filter, &model, catalog),
                    unless_privilege: rule.unless_privilege.clone(),
                    applies_to: rule.applies_to,
                }).collect(),
            });
        }
        Self { config, models, policy_models }
    }

    pub(crate) fn can_access_model<C: Iterable<JwtContext>>(&self, data: &C, model: &ModelId) -> Result<(), AccessDenied> {
        if data.iterable().any(JwtContext::is_privileged) || self.policy_models.contains(model) { return Ok(()); }
        let rules = self.model(model);
        if data.iterable().any(|context| rules.can_retrieve(&self.config, context)) { return Ok(()); }
        Err(AccessDenied::ModelDenied(*model))
    }

    pub(crate) fn read_predicate<C: Iterable<JwtContext>>(
        &self, data: &C, operation: ReadOperation,
    ) -> Predicate<Resolved> {
        if data.iterable().any(JwtContext::is_privileged) { return Predicate::True; }
        let mut slices: Vec<_> = self.policy_models.iter().copied().map(Predicate::MemberOf).collect();
        for (model, rules) in &self.models {
            if self.policy_models.contains(model) { continue; }
            let scope = rules.read_filter(&self.config, data, operation);
            if scope == Predicate::False { continue; }
            slices.push(if scope == Predicate::True { Predicate::MemberOf(*model) }
                else { Predicate::And(Box::new(Predicate::MemberOf(*model)), Box::new(scope)) });
        }
        slices.into_iter().reduce(|left, right| Predicate::Or(Box::new(left), Box::new(right))).unwrap_or(Predicate::False)
    }

    /// An existing membership must permit both states; a proposed membership cannot authorize itself.
    pub(crate) fn check_write(&self, context: &JwtContext, before: Option<&Entity>, after: &Entity) -> Result<(), AccessDenied> {
        if context.is_privileged() { return Ok(()); }
        if matches!(context, JwtContext::NoUser) {
            return Err(AccessDenied::ByPolicy("NoUser context cannot write events"));
        }
        let memberships = after.memberships();
        if memberships.iter().any(|model| self.policy_models.contains(model)) {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may write JWT policy entities"));
        }
        let mut denial = AccessDenied::ByPolicy("No existing membership grants edit access to this entity");
        for model in memberships {
            if before.is_some_and(|entity| !entity.has_membership(&model)) { continue; }
            let rules = self.model(&model);
            if !rules.can_write(&self.config, context) { denial = AccessDenied::ModelDenied(model); continue; }
            let check = || {
                if let Some(before) = before { rules.check_write_scope(&self.config, context, before)?; }
                rules.check_write_scope(&self.config, context, after)
            };
            match check() {
                Ok(()) => return Ok(()),
                Err(error) => denial = error,
            }
        }
        Err(denial)
    }

    fn model(&self, model: &ModelId) -> &ModelRules {
        static UNCONFIGURED: ModelRules = ModelRules { read: None, retrieve: None, write: None, scopes: Vec::new() };
        self.models.get(model).unwrap_or(&UNCONFIGURED)
    }
}

fn stored_predicate(graph: &PolicyGraph, scope: &crate::PolicyScope) -> Result<BoundPredicate, AccessDenied> {
    let missing = || AccessDenied::ByPolicy("policy scope binding has not arrived");
    let resolved = scope.resolved.as_ref().ok_or_else(missing)?.id();
    let predicate = graph.resolved_scopes.get(&resolved).ok_or_else(missing)?.predicate.0.clone();
    let mut parameters: Vec<_> = graph.parameters.values().filter(|parameter| parameter.scope.id() == resolved).collect();
    parameters.sort_by_key(|parameter| parameter.position);
    let parameters = parameters.into_iter().enumerate().map(|(index, parameter)| {
        if usize::try_from(parameter.position).ok() != Some(index) { return Err(missing()); }
        let value_type = parameter.value_type.as_deref().map(|label| ankurah_core_types::ValueType::from_property_str(label)
            .ok_or(AccessDenied::ByPolicy("policy parameter has an invalid value type"))).transpose()?;
        Ok((parameter.variable.clone(), value_type))
    }).collect::<Result<_, AccessDenied>>()?;
    BoundPredicate::from_stored(predicate, parameters)
}

impl ModelRules {
    fn has_privilege(config: &PolicyConfig, context: &JwtContext, privilege: &Option<String>) -> bool {
        privilege.as_ref().is_some_and(|privilege| config.roles_have_privilege(context.roles(), privilege))
    }

    fn can_scan(&self, config: &PolicyConfig, context: &JwtContext) -> bool {
        Self::has_privilege(config, context, &self.read) || self.can_write(config, context)
    }

    fn can_write(&self, config: &PolicyConfig, context: &JwtContext) -> bool {
        Self::has_privilege(config, context, &self.write)
    }

    fn can_retrieve(&self, config: &PolicyConfig, context: &JwtContext) -> bool {
        self.can_scan(config, context) || Self::has_privilege(config, context, &self.retrieve)
    }

    fn predicates(&self, config: &PolicyConfig, context: &JwtContext, write: bool) -> Result<Vec<Predicate<Resolved>>, AccessDenied> {
        let mut predicates = Vec::new();
        for scope in &self.scopes {
            let applies = if write { scope.applies_to.applies_to_writes() } else { scope.applies_to.applies_to_reads() };
            if !applies || scope.unless_privilege.as_ref().is_some_and(|privilege| config.roles_have_privilege(context.roles(), privilege)) {
                continue;
            }
            let JwtContext::User { claims, .. } = context else {
                return Err(AccessDenied::ByPolicy("No authenticated context for scope enforcement"));
            };
            predicates.push(scope.predicate.as_ref().map_err(Clone::clone)?.populate(claims)?);
        }
        Ok(predicates)
    }

    fn read_filter<C: Iterable<JwtContext>>(
        &self, config: &PolicyConfig, data: &C, operation: ReadOperation,
    ) -> Predicate<Resolved> {
        let mut slices = Vec::new();
        for context in data.iterable().filter(|context| match operation {
            ReadOperation::Query => self.can_scan(config, context),
            ReadOperation::Retrieve => self.can_retrieve(config, context),
        }) {
            let Ok(predicates) = self.predicates(config, context, false) else { continue };
            let Some(slice) = predicates.into_iter().reduce(|left, right| Predicate::And(Box::new(left), Box::new(right))) else {
                return Predicate::True;
            };
            if !slices.contains(&slice) { slices.push(slice); }
        }
        slices.into_iter().reduce(|left, right| Predicate::Or(Box::new(left), Box::new(right)))
            .unwrap_or(Predicate::False)
    }

    fn check_write_scope<E: Filterable>(&self, config: &PolicyConfig, context: &JwtContext, entity: &E) -> Result<(), AccessDenied> {
        for predicate in self.predicates(config, context, true)? {
            match evaluate_predicate(entity, &predicate) {
                Ok(true) => {}
                Ok(false) => return Err(AccessDenied::ByPolicy("Write outside permitted scope")),
                Err(_) => return Err(AccessDenied::ByPolicy("Write scope predicate could not be evaluated")),
            }
        }
        Ok(())
    }
}
