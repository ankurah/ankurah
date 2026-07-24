use crate::agent_state::start_ephemeral_policy_sync;
pub use crate::agent_state::{AgentState, AgentStateReadGuard};
use crate::{JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankql::ast::Predicate;
use ankurah_core::{
    entity::{Entity, TemporaryEntity},
    error::ValidationError,
    livequery::EntityLiveQuery,
    node::{Node, NodeInner, WeakNode},
    policy::{AccessDenied, PolicyAgent, RegistrationPlan},
    selection::filter::evaluate_predicate,
    storage::StorageEngine,
    util::Iterable,
};
use ankurah_proto::{self as proto, Attested};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use tracing::debug;

/// JWT-based PolicyAgent for ankurah.
///
/// Validates incoming requests using RS256 JWTs, and enforces access control
/// based on a configurable policy (roles -> privileges -> entity rules).
#[derive(Clone)]
pub struct JwtAgent {
    state: Arc<RwLock<AgentState>>,
    policy_path: Option<PathBuf>,
    /// Weak-node LiveQuery (EntityLiveQuery::new_weak_node) so the agent never keeps its own node alive
    policy_livequery: Arc<Mutex<Option<EntityLiveQuery>>>,
    catalog_resolvers: Arc<RwLock<Vec<std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>>>>,
}

impl JwtAgent {
    /// Create a new durable JwtAgent with signing keys and a policy file path.
    pub fn new_durable(keys: SigningKeys, policy_path: impl AsRef<Path>) -> Result<Self, anyhow::Error> {
        let path = policy_path.as_ref();
        let json_str =
            std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("Failed to read policy file {}: {}", path.display(), e))?;
        let config: PolicyConfig =
            serde_json::from_str(&json_str).map_err(|e| anyhow::anyhow!("Failed to parse policy config from {}: {}", path.display(), e))?;
        Ok(Self {
            state: Arc::new(RwLock::new(AgentState { config, keys: Some(JwtKeys::Signing(keys)) })),
            policy_path: Some(path.to_path_buf()),
            policy_livequery: Arc::new(Mutex::new(None)),
            catalog_resolvers: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Create a new ephemeral JwtAgent with no keys and deny-all config.
    pub fn new_ephemeral() -> Self {
        Self {
            state: Arc::new(RwLock::new(AgentState { config: PolicyConfig::default(), keys: None })),
            policy_path: None,
            policy_livequery: Arc::new(Mutex::new(None)),
            catalog_resolvers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Returns a clone of the signing keys if the agent has a full keypair.
    pub fn signing_keys(&self) -> Option<SigningKeys> {
        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        match guard.keys.as_ref() {
            Some(JwtKeys::Signing(keys)) => Some(keys.clone()),
            _ => None,
        }
    }

    /// Replace the keys at runtime.
    pub fn set_keys(&self, keys: JwtKeys) { self.state.write().unwrap_or_else(|e| e.into_inner()).keys = Some(keys); }

    /// Returns a shared handle to the combined state.
    pub fn state_handle(&self) -> Arc<RwLock<AgentState>> { Arc::clone(&self.state) }

    pub fn config(&self) -> AgentStateReadGuard<'_> { AgentStateReadGuard::new(self.state.read().unwrap_or_else(|e| e.into_inner())) }

    /// Returns true once the agent has policy and key material.
    pub fn policy_ready(&self) -> bool {
        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        !guard.config.roles.is_empty() && guard.keys.is_some()
    }

    /// Replaces the in-memory config with a new one.
    pub fn update_config(&self, new_config: PolicyConfig) { self.state.write().unwrap_or_else(|e| e.into_inner()).config = new_config; }

    /// Attach a catalog used to translate exact runtime model identities to
    /// the human names in policy configuration. Nodes install their catalog
    /// automatically from [`PolicyAgent::on_node_ready`]. The agent retains a
    /// weak set rather than one replaceable slot because cloned agents share
    /// policy state and may be attached to multiple nodes; a cold peer's
    /// catalog must not hide a ready durable node's catalog.
    pub fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>) {
        let mut resolvers = self.catalog_resolvers.write().unwrap_or_else(|error| error.into_inner());
        resolvers.retain(|known| known.strong_count() != 0);
        if !resolvers.iter().any(|known| std::sync::Weak::ptr_eq(known, &resolver)) {
            resolvers.push(resolver);
        }
    }

    /// Policy configuration is intentionally human-name keyed. Resolve the
    /// durable model identity through catalog metadata, then normalize the
    /// registered model name to the extension's historical lowercase key.
    fn policy_model_name(&self, model: &ankurah_core::ModelId) -> Result<String, AccessDenied> {
        let resolvers = self.catalog_resolvers.read().unwrap_or_else(|error| error.into_inner());
        if resolvers.is_empty() {
            return Err(AccessDenied::ByPolicy("catalog resolver is unavailable for model policy lookup"));
        }
        resolvers
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .find_map(|resolver| resolver.model_name(model).ok())
            .map(|name| name.to_ascii_lowercase())
            .ok_or(AccessDenied::ByPolicy("model is unknown to the policy catalog"))
    }
}

#[async_trait]
impl PolicyAgent for JwtAgent {
    type ContextData = JwtContext;

    fn on_node_ready<SE: StorageEngine + Send + Sync + 'static>(&self, node: WeakNode<SE, Self>) {
        let Some(ready_node) = node.upgrade() else {
            tracing::warn!("on_node_ready: node already dropped");
            return;
        };
        self.set_catalog_resolver(ready_node.catalog.resolver_weak());
        drop(ready_node);

        #[cfg(feature = "watcher")]
        if let Some(ref policy_path) = self.policy_path {
            crate::agent_state::start_durable_policy_watcher(node, policy_path.clone(), self.state_handle());
            return;
        }

        if self.policy_path.is_none() {
            start_ephemeral_policy_sync(node, self.state_handle(), self.policy_livequery.clone());
        }
    }

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        debug!("JwtAgent sign_request");
        let mut auth_data = Vec::new();
        for ctx in cdata.iterable() {
            auth_data.push(ctx.auth_data()?);
        }
        Ok(auth_data)
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &Node<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        let state_guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        let keys =
            state_guard.keys.as_ref().ok_or_else(|| ValidationError::ValidationFailed("No keys configured for JWT verification".into()))?;

        let mut contexts = Vec::new();
        for auth_data in auth.iterable() {
            if auth_data.0.is_empty() {
                contexts.push(JwtContext::NoUser);
                continue;
            }
            let token =
                std::str::from_utf8(&auth_data.0).map_err(|e| ValidationError::ValidationFailed(format!("Invalid UTF-8 in token: {e}")))?;
            let claims = keys.verify(token).map_err(|e| ValidationError::ValidationFailed(format!("JWT verification failed: {e}")))?;
            contexts.push(JwtContext::from_claims(claims, token.to_string()));
        }
        Ok(contexts)
    }

    fn check_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        entity_before: &Entity,
        entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        if cdata.is_privileged() {
            return Ok(None);
        }
        // Core admits writes to these protected models only through the
        // schema-registration executor. Its resolved-plan policy check runs
        // before this per-event check, so an authenticated caller that passed
        // that gate may persist the catalog effects. NoUser is still denied.
        if ankurah_core::schema::is_catalog_collection(entity_after.collection()) {
            return if matches!(cdata, JwtContext::NoUser) {
                Err(AccessDenied::ByPolicy("NoUser context cannot write schema metadata"))
            } else {
                Ok(None)
            };
        }
        let model_name = self.policy_model_name(entity_after.collection())?;
        if model_name == "jwtpolicy" {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may write to jwtpolicy"));
        }
        if matches!(cdata, JwtContext::NoUser) {
            return Err(AccessDenied::ByPolicy("NoUser context cannot write events"));
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        if !state.config.can_write_collection(cdata.roles(), &model_name) {
            return Err(AccessDenied::CollectionDenied(entity_after.collection().clone()));
        }
        if !entity_before.head().is_empty() {
            enforce_write_scope(&state.config, cdata, entity_before, &model_name)?;
        }
        enforce_write_scope(&state.config, cdata, entity_after, &model_name)?;
        Ok(None)
    }

    fn check_schema_registration<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        plan: &RegistrationPlan,
    ) -> Result<(), AccessDenied> {
        if cdata.is_privileged() {
            return Ok(());
        }
        if matches!(cdata, JwtContext::NoUser) {
            return Err(AccessDenied::ByPolicy("NoUser context cannot define schema"));
        }

        let state = self.state.read().unwrap_or_else(|error| error.into_inner());
        let may_write = |collection: &str| state.config.can_write_collection(cdata.roles(), collection);

        if plan.creates_models.iter().any(|(_, model)| !may_write(&model.collection))
            || plan.creates_properties.iter().any(|(_, property)| !may_write(&property.minting_collection))
        {
            return Err(AccessDenied::ByPolicy("schema creation requires write access to its model"));
        }

        let created_models: std::collections::BTreeMap<_, _> =
            plan.creates_models.iter().map(|(id, model)| (*id, model.collection.as_str())).collect();
        for membership in &plan.creates_memberships {
            let collection = match created_models.get(&membership.model) {
                Some(collection) => (*collection).to_owned(),
                None => self.policy_model_name(&proto::ModelId::EntityId(membership.model))?,
            };
            if !may_write(&collection) {
                return Err(AccessDenied::ByPolicy("schema membership creation requires write access to its model"));
            }
        }

        // Renames, retargeting, and optionality changes alter existing
        // contracts. Keep those administrative until the policy format grows
        // a dedicated schema-migration privilege.
        if !plan.updates.is_empty() && !state.config.roles_have_privilege(cdata.roles(), "*") {
            return Err(AccessDenied::ByPolicy("schema metadata updates require wildcard privilege"));
        }

        Ok(())
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _model: &proto::ModelId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &Node<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _model: &proto::ModelId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, data: &C, collection: &ankurah_core::ModelId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        // Ephemeral agents must read the public metadata catalog before they
        // can resolve the JwtPolicy model that supplies their policy and
        // verification key. This grants no catalog mutation capability:
        // registration events still pass through check_event, where NoUser
        // is denied.
        if matches!(
            collection,
            proto::ModelId::System(proto::SystemModel::Model | proto::SystemModel::Property | proto::SystemModel::ModelProperty)
        ) {
            return Ok(());
        }
        let model_name = self.policy_model_name(collection)?;
        if model_name == "jwtpolicy" {
            return Ok(());
        }
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(());
            }
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        for ctx in data.iterable() {
            if state.config.can_access_collection(ctx.roles(), &model_name) {
                return Ok(());
            }
        }
        Err(AccessDenied::CollectionDenied(collection.clone()))
    }

    fn filter_predicate<C>(&self, data: &C, collection: &ankurah_core::ModelId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C: Iterable<Self::ContextData> {
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(predicate);
            }
        }

        let model_name = self.policy_model_name(collection)?;
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        if state.config.scope_rules_for_collection(&model_name).is_empty() {
            return Ok(predicate);
        }

        let claims = data
            .iterable()
            .find_map(|ctx| match ctx {
                JwtContext::User { claims, .. } => Some(claims),
                _ => None,
            })
            .ok_or(AccessDenied::ByPolicy("No authenticated context for row filtering"))?;

        let mut result = predicate;
        for filter in scoped_predicates(&state.config, &model_name, claims, ScopeAccess::Read)? {
            result = Predicate::And(Box::new(result), Box::new(filter));
        }

        Ok(result)
    }

    fn check_read<C>(
        &self,
        data: &C,
        id: &proto::EntityId,
        collection: &ankurah_core::ModelId,
        state: &proto::State,
        resolver: Option<std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        self.can_access_collection(data, collection)?;

        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(());
            }
        }

        let model_name = self.policy_model_name(collection)?;
        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        if guard.config.scope_rules_for_collection(&model_name).is_empty() {
            return Ok(());
        }

        // Scope filters are NAME-addressed (config strings) and post-epoch
        // state is id-keyed: bind the inspection view through the node's
        // catalog resolver or every filter reads nothing and denies.
        let entity = TemporaryEntity::new_bound(*id, collection.clone(), state, resolver)
            .map_err(|_| AccessDenied::ByPolicy("Read scope entity state could not be evaluated"))?;
        enforce_read_scope(&guard.config, data, &entity, &model_name)
    }

    fn check_read_event<C>(
        &self,
        data: &C,
        collection: &ankurah_core::ModelId,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        let _ = event;
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(());
            }
        }
        self.can_access_collection(data, collection)
    }

    fn check_write(&self, cdata: &Self::ContextData, entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        let model_name = self.policy_model_name(entity.collection())?;
        if model_name == "jwtpolicy" && !cdata.is_privileged() {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may write to jwtpolicy"));
        }
        if cdata.is_privileged() {
            return Ok(());
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        if !state.config.can_write_collection(cdata.roles(), &model_name) {
            Err(AccessDenied::CollectionDenied(entity.collection().clone()))
        } else {
            enforce_write_scope(&state.config, cdata, entity, &model_name)
        }
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer_id: &proto::EntityId,
        _head_relation: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// Which access path scope predicates are being collected for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeAccess {
    Read,
    Write,
}

fn scoped_predicates(
    config: &PolicyConfig,
    collection: &str,
    claims: &crate::JwtClaims,
    access: ScopeAccess,
) -> Result<Vec<Predicate>, AccessDenied> {
    let mut predicates = Vec::new();

    for rule in config.scope_rules_for_collection(collection) {
        let applies = match access {
            ScopeAccess::Read => rule.applies_to.applies_to_reads(),
            ScopeAccess::Write => rule.applies_to.applies_to_writes(),
        };
        if !applies {
            continue;
        }

        let should_apply = match &rule.unless_privilege {
            Some(priv_name) => !config.roles_have_privilege(claims.roles.as_slice(), priv_name),
            None => true,
        };

        if should_apply {
            predicates.push(crate::variables::parse_and_substitute(&rule.filter, claims)?);
        }
    }

    Ok(predicates)
}

fn enforce_write_scope(config: &PolicyConfig, cdata: &JwtContext, entity: &Entity, model_name: &str) -> Result<(), AccessDenied> {
    let JwtContext::User { claims, .. } = cdata else {
        return Err(AccessDenied::ByPolicy("No authenticated context for write scope enforcement"));
    };

    for predicate in scoped_predicates(config, model_name, claims, ScopeAccess::Write)? {
        match evaluate_predicate(entity, &predicate) {
            Ok(true) => {}
            Ok(false) => return Err(AccessDenied::ByPolicy("Write outside permitted scope")),
            Err(_) => return Err(AccessDenied::ByPolicy("Write scope predicate could not be evaluated")),
        }
    }

    Ok(())
}

fn enforce_read_scope<C>(config: &PolicyConfig, data: &C, entity: &TemporaryEntity, model_name: &str) -> Result<(), AccessDenied>
where C: Iterable<JwtContext> {
    for ctx in data.iterable() {
        let JwtContext::User { claims, .. } = ctx else {
            continue;
        };
        if !config.can_access_collection(ctx.roles(), model_name) {
            continue;
        }

        let mut allowed = true;
        for predicate in scoped_predicates(config, model_name, claims, ScopeAccess::Read)? {
            match evaluate_predicate(entity, &predicate) {
                Ok(true) => {}
                Ok(false) => {
                    allowed = false;
                    break;
                }
                Err(_) => return Err(AccessDenied::ByPolicy("Read scope predicate could not be evaluated")),
            }
        }
        if allowed {
            return Ok(());
        }
    }

    Err(AccessDenied::ByPolicy("Read outside permitted scope"))
}
