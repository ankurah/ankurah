use std::collections::{BTreeMap, BTreeSet};

use ankurah::{CachePolicy, Context, LiveQuery, MatchArgs};
use ankql::ast::{Parsed, Predicate};
use ankurah::core::model::{Model, View};
use ankurah::core::schema::{catalog::CatalogManager, SystemEpoch};
use ankurah::proto::{EntityId, ModelId};
use ankurah::signals::{Get, Subscribe, SubscriptionGuard};
use futures::FutureExt;

use crate::model::*;

/// Every stored policy record, keyed by entity ID.
#[derive(Default)]
pub(crate) struct PolicyGraph {
    pub roles: BTreeMap<EntityId, Role>,
    pub privileges: BTreeMap<EntityId, Privilege>,
    pub grants: BTreeMap<EntityId, RolePrivilege>,
    pub models: BTreeMap<EntityId, ModelPolicy>,
    pub properties: BTreeMap<EntityId, PolicyProperty>,
    pub scopes: BTreeMap<EntityId, PolicyScope>,
    pub resolved_scopes: BTreeMap<EntityId, ResolvedPolicyScope>,
    pub parameters: BTreeMap<EntityId, ClaimParameter>,
    pub keys: BTreeMap<EntityId, JwtVerificationKey>,
}

impl PolicyGraph {
    /// Fetch every policy record.
    pub async fn fetch(context: &Context, cache_policy: CachePolicy) -> anyhow::Result<Self> {
        let args = || MatchArgs { selection: Predicate::<Parsed>::True.into(), cache_policy };
        Ok(Self {
            roles: decode(context.fetch::<RoleView>(args()).await?)?,
            privileges: decode(context.fetch::<PrivilegeView>(args()).await?)?,
            grants: decode(context.fetch::<RolePrivilegeView>(args()).await?)?,
            models: decode(context.fetch::<ModelPolicyView>(args()).await?)?,
            properties: decode(context.fetch::<PolicyPropertyView>(args()).await?)?,
            scopes: decode(context.fetch::<PolicyScopeView>(args()).await?)?,
            resolved_scopes: decode(context.fetch::<ResolvedPolicyScopeView>(args()).await?)?,
            parameters: decode(context.fetch::<ClaimParameterView>(args()).await?)?,
            keys: decode(context.fetch::<JwtVerificationKeyView>(args()).await?)?,
        })
    }
}

/// One livequery per policy model; `snapshot` combines their current results into a `PolicyGraph`.
pub(crate) struct PolicyQueries {
    roles: LiveQuery<RoleView>,
    privileges: LiveQuery<PrivilegeView>,
    grants: LiveQuery<RolePrivilegeView>,
    models: LiveQuery<ModelPolicyView>,
    properties: LiveQuery<PolicyPropertyView>,
    scopes: LiveQuery<PolicyScopeView>,
    resolved_scopes: LiveQuery<ResolvedPolicyScopeView>,
    parameters: LiveQuery<ClaimParameterView>,
    keys: LiveQuery<JwtVerificationKeyView>,
}

impl PolicyQueries {
    pub fn new(context: &Context) -> anyhow::Result<Self> {
        Ok(Self {
            roles: context.query("true")?,
            privileges: context.query("true")?,
            grants: context.query("true")?,
            models: context.query("true")?,
            properties: context.query("true")?,
            scopes: context.query("true")?,
            resolved_scopes: context.query("true")?,
            parameters: context.query("true")?,
            keys: context.query("true")?,
        })
    }

    pub async fn wait_durable_answered(&self) -> anyhow::Result<()> {
        futures::future::try_join_all([
            self.roles.wait_durable_answered().boxed(),
            self.privileges.wait_durable_answered().boxed(),
            self.grants.wait_durable_answered().boxed(),
            self.models.wait_durable_answered().boxed(),
            self.properties.wait_durable_answered().boxed(),
            self.scopes.wait_durable_answered().boxed(),
            self.resolved_scopes.wait_durable_answered().boxed(),
            self.parameters.wait_durable_answered().boxed(),
            self.keys.wait_durable_answered().boxed(),
        ]).await?;
        Ok(())
    }

    pub fn snapshot(&self) -> anyhow::Result<PolicyGraph> {
        Ok(PolicyGraph {
            roles: decode(self.roles.get())?,
            privileges: decode(self.privileges.get())?,
            grants: decode(self.grants.get())?,
            models: decode(self.models.get())?,
            properties: decode(self.properties.get())?,
            scopes: decode(self.scopes.get())?,
            resolved_scopes: decode(self.resolved_scopes.get())?,
            parameters: decode(self.parameters.get())?,
            keys: decode(self.keys.get())?,
        })
    }

    pub fn subscribe(&self, changed: impl Fn() + Clone + Send + Sync + 'static) -> Vec<SubscriptionGuard> {
        vec![
            self.roles.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.privileges.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.grants.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.models.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.properties.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.scopes.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.resolved_scopes.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.parameters.subscribe({ let changed = changed.clone(); move |_| changed() }),
            self.keys.subscribe(move |_| changed()),
        ]
    }
}

fn decode<V: View>(views: Vec<V>) -> anyhow::Result<BTreeMap<EntityId, V::Model>> {
    views.into_iter().map(|view| Ok((view.id(), view.to_model()?))).collect()
}

/// Bind every policy model from the local catalog without registering; fails while any cannot bind yet.
pub(crate) fn bind_models(catalog: &CatalogManager, epoch: SystemEpoch) -> anyhow::Result<BTreeSet<ModelId>> {
    Ok([
        Role::descriptor().bind_local(catalog, epoch)?,
        Privilege::descriptor().bind_local(catalog, epoch)?,
        RolePrivilege::descriptor().bind_local(catalog, epoch)?,
        ModelPolicy::descriptor().bind_local(catalog, epoch)?,
        PolicyProperty::descriptor().bind_local(catalog, epoch)?,
        PolicyScope::descriptor().bind_local(catalog, epoch)?,
        ResolvedPolicyScope::descriptor().bind_local(catalog, epoch)?,
        ClaimParameter::descriptor().bind_local(catalog, epoch)?,
        JwtVerificationKey::descriptor().bind_local(catalog, epoch)?,
    ].into_iter().collect())
}

/// Resolve every policy model's ID, registering any that are missing; only policy installation registers them.
pub(crate) async fn register_models(context: &Context) -> anyhow::Result<BTreeSet<ModelId>> {
    Ok([
        context.resolve_model_id::<Role>().await?,
        context.resolve_model_id::<Privilege>().await?,
        context.resolve_model_id::<RolePrivilege>().await?,
        context.resolve_model_id::<ModelPolicy>().await?,
        context.resolve_model_id::<PolicyProperty>().await?,
        context.resolve_model_id::<ResolvedPolicyScope>().await?,
        context.resolve_model_id::<PolicyScope>().await?,
        context.resolve_model_id::<ClaimParameter>().await?,
        context.resolve_model_id::<JwtVerificationKey>().await?,
    ].into_iter().collect())
}
