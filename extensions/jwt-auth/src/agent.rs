use crate::agent_state::start_policy_sync;
pub use crate::agent_state::AgentState;
use crate::{JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankql::ast::{Predicate, Resolved};
use ankurah::signals::{Mut, Read};
use futures::FutureExt;
use crate::bound_policy::{BoundPolicy, ReadOperation};
use ankurah_core::{
    entity::Entity,
    error::ValidationError,
    node::{Node, NodeInner, WeakNode},
    policy::{AccessDenied, PolicyAgent},
    schema::registration::RegistrationPlan,
    storage::StorageEngine,
    util::Iterable,
};
use ankurah_proto::{self as proto, Attested};
use async_trait::async_trait;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::debug;

/// JWT-based PolicyAgent for ankurah.
///
/// Validates incoming requests using RS256 JWTs, and enforces access control
/// based on a configurable policy (roles -> privileges -> entity rules).
#[derive(Clone)]
pub struct JwtAgent {
    state: Mut<AgentState>,
    /// Serialize policy replacement with binding pending rules during schema registration.
    /// Registration holds this through commit so replacement cannot miss the new schema
    /// and leave its rules pending after the registration hook has already run.
    authoring: Arc<tokio::sync::Mutex<()>>,
    /// Shared by agent clones; dropping the last owner cancels background policy synchronization.
    policy_sync: Arc<Mutex<Option<futures::future::RemoteHandle<()>>>>,
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
            state: Mut::new(AgentState::new(config, Some(JwtKeys::Signing(keys)), true)),
            authoring: Arc::new(tokio::sync::Mutex::new(())),
            policy_sync: Arc::new(Mutex::new(None)),
        })
    }

    /// Create a new ephemeral JwtAgent with no keys and deny-all config.
    pub fn new_ephemeral() -> Self {
        Self {
            state: Mut::new(AgentState::new(PolicyConfig::default(), None, false)),
            authoring: Arc::new(tokio::sync::Mutex::new(())),
            policy_sync: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a clone of the signing keys if the agent has a full keypair.
    pub fn signing_keys(&self) -> Option<SigningKeys> {
        self.state.with(|state| match state.keys.as_ref() {
            Some(JwtKeys::Signing(keys)) => Some(keys.clone()),
            _ => None,
        })
    }

    /// Replace the keys at runtime.
    pub fn set_keys(&self, keys: JwtKeys) { self.state.update(|state| state.keys = Some(keys)); }

    /// Observe policy configuration and keys.
    pub fn state_handle(&self) -> Read<AgentState> { self.state.read() }

    pub fn config(&self) -> Arc<PolicyConfig> { self.state.with(|state| state.config.clone()) }

    /// Returns true after policy configuration and key material have been supplied.
    pub fn policy_ready(&self) -> bool { self.state.with(AgentState::ready) }

    /// Set configuration for later installation; active permissions are unchanged.
    /// Use [`Self::set_policy`] to change the system's policy.
    pub fn update_config(&self, config: PolicyConfig) {
        self.state.update(|state| { state.config = Arc::new(config); state.config_loaded = true; });
    }

    /// Create or replace the system's stored policy, using this agent's public verification key.
    pub async fn set_policy<SE: StorageEngine + Send + Sync + 'static>(
        &self,
        node: &Node<SE, Self>,
        config: &PolicyConfig,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(node.durable, "setting policy requires a durable node");
        let context = ankurah_core::context::Context::new_weak(node, JwtContext::system());
        let public_key_pem = self.state.with(|state| -> anyhow::Result<_> {
            Ok(state.keys.as_ref().ok_or_else(|| anyhow::anyhow!("policy has no verification keys"))?.public_key_pem()?)
        })?;
        let models = crate::graph::register_models(&context).await?;
        let _authoring = self.authoring.lock().await;
        self.state.update(|state| state.policy_models = models);
        crate::authoring::set_policy(&context, &node.catalog, config, public_key_pem).await
    }

    /// Read a JSON policy file and persist it once; this does not watch the file.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn set_policy_from_file<SE: StorageEngine + Send + Sync + 'static>(
        &self,
        node: &Node<SE, Self>,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        use anyhow::Context;

        let path = path.as_ref();
        let json = tokio::fs::read_to_string(path).await.with_context(|| format!("Failed to read policy file {}", path.display()))?;
        let config = serde_json::from_str(&json).with_context(|| format!("Failed to parse policy config from {}", path.display()))?;
        self.set_policy(node, &config).await
    }

    /// Install a local evaluator fixture without persistence or reactive rebinding.
    #[cfg(feature = "test-helpers")]
    pub fn set_catalog(&self, catalog: Arc<dyn crate::PolicyCatalog>) {
        let policy = Arc::new(BoundPolicy::bind(self.config(), catalog.as_ref()));
        self.state.update(|state| state.policy = Some(policy));
    }

    fn policy(&self) -> Result<Arc<BoundPolicy>, AccessDenied> {
        self.state.with(|state| state.policy.clone()).ok_or(AccessDenied::ByPolicy("policy has not loaded"))
    }

    pub fn can_access_model<C: Iterable<JwtContext>>(&self, data: &C, model: &proto::ModelId) -> Result<(), AccessDenied> {
        if data.iterable().any(JwtContext::is_privileged) { return Ok(()); }
        if self.state.with(|state| state.policy_models.contains(model)) { return Ok(()); }
        self.policy()?.can_access_model(data, model)
    }

    fn read_predicate<C: Iterable<JwtContext>>(&self, data: &C, operation: ReadOperation) -> Result<Predicate<Resolved>, AccessDenied> {
        if data.iterable().any(JwtContext::is_privileged) { return Ok(Predicate::True); }
        self.state.with(|state| match &state.policy {
            Some(policy) => Ok(policy.read_predicate(data, operation)),
            // Policy records must be readable before their contents have loaded.
            None => state.policy_models.iter().copied().map(Predicate::MemberOf)
                .reduce(|left, right| Predicate::Or(Box::new(left), Box::new(right)))
                .ok_or(AccessDenied::ByPolicy("policy has not loaded")),
        })
    }

}

#[async_trait]
impl PolicyAgent for JwtAgent {
    type ContextData = JwtContext;

    async fn start<SE: StorageEngine + Send + Sync + 'static>(&self, node: WeakNode<SE, Self>) -> anyhow::Result<()> {
        let (loaded, result) = futures::channel::oneshot::channel();
        let (sync, handle) = start_policy_sync(node, self.state.clone(), loaded).remote_handle();
        *self.policy_sync.lock().unwrap_or_else(|error| error.into_inner()) = Some(handle);
        ankurah_core::task::spawn(sync);
        result.await??;
        Ok(())
    }

    async fn preflight<SE: StorageEngine + Send + Sync + 'static>(
        &self,
        node: &Node<SE, Self>,
        model: proto::ModelId,
    ) -> Result<(), ankurah_core::error::RetrievalError> {
        let needs_bindings = self.state.with(|state| {
            !state.policy_models.is_empty()
                && !state.policy_models.contains(&model)
                && !state.policy.as_ref().is_some_and(|policy| policy.is_bound(&model))
        });
        if needs_bindings {
            let context = ankurah::Context::new_weak(node, JwtContext::NoUser);
            // Applying these answers refreshes the policy livequeries before access is checked.
            crate::graph::PolicyGraph::fetch(&context, ankurah::CachePolicy::Tracked).await?;
        }
        Ok(())
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
        // All-or-nothing: one unsignable member (Root's auth_data errors
        // by design) fails the whole request even when other members
        // could serve. Skip-vs-fail is an open decision:
        // https://github.com/ankurah/ankurah/issues/432
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
        self.state.with(|state| {
            let keys =
                state.keys.as_ref().ok_or_else(|| ValidationError::ValidationFailed("No keys configured for JWT verification".into()))?;

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
        })
    }

    fn check_schema_registration<SE: StorageEngine>(
        &self,
        node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        plan: &RegistrationPlan,
    ) -> Result<(), AccessDenied> {
        if cdata.is_privileged() { return Ok(()); }
        if matches!(cdata, JwtContext::NoUser) && !plan.is_noop() {
            return Err(AccessDenied::ByPolicy("Anonymous contexts cannot change the catalog"));
        }
        let protected = self.state.with(|state| state.policy_models.clone());
        let protects = |id| protected.contains(&proto::ModelId::EntityId(id));
        let modifies_policy = plan.creates_models.iter().any(|(_, model)| model.label.starts_with("jwtagent_"))
            || plan.creates_properties.iter().any(|(_, property)| property.minted_for.is_some_and(protects))
            || plan.creates_memberships.iter().any(|membership| protects(membership.model))
            || plan.updates.iter().any(|update| match update.collection {
                proto::ModelId::System(proto::SystemModel::ModelProperty) => match node.catalog.membership_by_id(&update.entity) {
                    Ok(Some(membership)) => protects(membership.model),
                    _ => true,
                },
                _ => protects(update.entity)
                    || node.catalog.property_by_id(&update.entity).ok().flatten().and_then(|row| row.minted_for).is_some_and(protects),
            });
        if modifies_policy {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may change JWT policy schema"));
        }
        Ok(())
    }

    async fn schema_registered<SE: StorageEngine + Send + Sync + 'static>(
        &self, node: &Node<SE, Self>, transaction: &ankurah::transaction::Transaction, plan: &RegistrationPlan,
    ) -> anyhow::Result<Option<tokio::sync::OwnedMutexGuard<()>>> {
        let context = ankurah::Context::new_weak(node, JwtContext::system());
        let Some(epoch) = node.system.system_epoch() else { return Ok(None) };
        if crate::graph::bind_models(&node.catalog, epoch).is_err() { return Ok(None); }
        let authoring = self.authoring.clone().lock_owned().await;
        crate::authoring::schema_registered(&context, &node.catalog, transaction, plan).await?;
        Ok(Some(authoring))
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        cdata: &Self::ContextData,
        entity_before: &Entity,
        entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        if !cdata.is_privileged() {
            self.policy()?.check_write(cdata, (!entity_before.head().is_empty()).then_some(entity_before), entity_after)?;
        }
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &Node<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C>(&self, data: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.read_predicate(data, ReadOperation::Query)
    }

    fn retrieval_predicate<C>(&self, data: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.read_predicate(data, ReadOperation::Retrieve)
    }

    fn check_write(&self, cdata: &Self::ContextData, entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        if cdata.is_privileged() { return Ok(()); }
        self.policy()?.check_write(cdata, None, entity)
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
