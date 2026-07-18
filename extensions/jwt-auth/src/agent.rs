use crate::agent_state::start_ephemeral_policy_sync;
pub use crate::agent_state::{AgentState, AgentStateReadGuard};
use crate::{JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankql::ast::Predicate;
use ankurah_core::{
    entity::{Entity, TemporaryEntity},
    error::ValidationError,
    livequery::EntityLiveQuery,
    node::{Node, NodeInner, WeakNode},
    policy::{AccessDenied, PolicyAgent},
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
}

impl JwtAgent {
    /// Create a new durable JwtAgent with signing keys and a policy file path.
    pub fn new_durable(keys: SigningKeys, policy_path: impl AsRef<Path>) -> Result<Self, anyhow::Error> {
        let path = policy_path.as_ref();
        let json_str =
            std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("Failed to read policy file {}: {}", path.display(), e))?;
        let config = PolicyConfig::from_json(&json_str)
            .map_err(|e| anyhow::anyhow!("Failed to load policy config from {}: {}", path.display(), e))?;
        Ok(Self {
            state: Arc::new(RwLock::new(AgentState { config, keys: Some(JwtKeys::Signing(keys)) })),
            policy_path: Some(path.to_path_buf()),
            policy_livequery: Arc::new(Mutex::new(None)),
        })
    }

    /// Create a new ephemeral JwtAgent with no keys and deny-all config.
    pub fn new_ephemeral() -> Self {
        Self {
            state: Arc::new(RwLock::new(AgentState { config: PolicyConfig::default(), keys: None })),
            policy_path: None,
            policy_livequery: Arc::new(Mutex::new(None)),
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
}

#[async_trait]
impl PolicyAgent for JwtAgent {
    type ContextData = JwtContext;

    fn on_node_ready<SE: StorageEngine + Send + Sync + 'static>(&self, node: WeakNode<SE, Self>) {
        #[cfg(feature = "watcher")]
        if let Some(ref policy_path) = self.policy_path {
            crate::agent_state::start_durable_policy_watcher(node, policy_path.clone(), self.state_handle());
            return;
        }

        if self.policy_path.is_none() {
            let Some(node) = node.upgrade() else {
                tracing::warn!("on_node_ready: node already dropped");
                return;
            };
            start_ephemeral_policy_sync(&node, self.state_handle(), &self.policy_livequery);
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
        if entity_after.collection().as_str() == "jwtpolicy" {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may write to jwtpolicy"));
        }
        if matches!(cdata, JwtContext::NoUser) {
            return Err(AccessDenied::ByPolicy("NoUser context cannot write events"));
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        if !state.config.can_write_collection(cdata.roles(), entity_after.collection()) {
            return Err(AccessDenied::CollectionDenied(entity_after.collection().clone()));
        }
        if !entity_before.head().is_empty() {
            enforce_write_scope(&state.config, cdata, entity_before)?;
        }
        enforce_write_scope(&state.config, cdata, entity_after)?;
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

    fn can_access_collection<C>(&self, data: &C, collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        if collection.as_str() == "jwtpolicy" {
            return Ok(());
        }
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(());
            }
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        for ctx in data.iterable() {
            if state.config.can_access_collection(ctx.roles(), collection) {
                return Ok(());
            }
        }
        Err(AccessDenied::CollectionDenied(collection.clone()))
    }

    fn filter_predicate<C>(&self, data: &C, collection: &proto::CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C: Iterable<Self::ContextData> {
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(predicate);
            }
        }

        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        if state.config.scope_rules_for_collection(collection.as_str()).is_empty() {
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
        for filter in scoped_predicates(&state.config, collection.as_str(), claims, ScopeAccess::Read)? {
            result = Predicate::And(Box::new(result), Box::new(filter));
        }

        Ok(result)
    }

    fn check_read<C>(
        &self,
        data: &C,
        id: &proto::EntityId,
        collection: &proto::CollectionId,
        state: &proto::State,
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

        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        if guard.config.scope_rules_for_collection(collection.as_str()).is_empty() {
            return Ok(());
        }

        let entity = TemporaryEntity::new(*id, collection.clone(), state)
            .map_err(|_| AccessDenied::ByPolicy("Read scope entity state could not be evaluated"))?;
        enforce_read_scope(&guard.config, data, &entity)
    }

    fn check_read_event<C>(&self, data: &C, event: &Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(());
            }
        }
        self.can_access_collection(data, &event.payload.collection)
    }

    fn check_write(&self, cdata: &Self::ContextData, entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        if entity.collection().as_str() == "jwtpolicy" && !cdata.is_privileged() {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may write to jwtpolicy"));
        }
        if cdata.is_privileged() {
            return Ok(());
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        if !state.config.can_write_collection(cdata.roles(), entity.collection()) {
            Err(AccessDenied::CollectionDenied(entity.collection().clone()))
        } else {
            enforce_write_scope(&state.config, cdata, entity)
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

fn enforce_write_scope(config: &PolicyConfig, cdata: &JwtContext, entity: &Entity) -> Result<(), AccessDenied> {
    let JwtContext::User { claims, .. } = cdata else {
        return Err(AccessDenied::ByPolicy("No authenticated context for write scope enforcement"));
    };

    for predicate in scoped_predicates(config, entity.collection().as_str(), claims, ScopeAccess::Write)? {
        match evaluate_predicate(entity, &predicate) {
            Ok(true) => {}
            Ok(false) => return Err(AccessDenied::ByPolicy("Write outside permitted scope")),
            Err(_) => return Err(AccessDenied::ByPolicy("Write scope predicate could not be evaluated")),
        }
    }

    Ok(())
}

fn enforce_read_scope<C>(config: &PolicyConfig, data: &C, entity: &TemporaryEntity) -> Result<(), AccessDenied>
where C: Iterable<JwtContext> {
    for ctx in data.iterable() {
        let JwtContext::User { claims, .. } = ctx else {
            continue;
        };
        if !config.can_access_collection(ctx.roles(), &entity.collection) {
            continue;
        }

        let mut allowed = true;
        for predicate in scoped_predicates(config, entity.collection.as_str(), claims, ScopeAccess::Read)? {
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
