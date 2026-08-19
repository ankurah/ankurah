use crate::agent_state::start_ephemeral_policy_sync;
pub use crate::agent_state::{AgentState, AgentStateReadGuard};
use crate::{JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankql::ast::{Parsed, Predicate, Resolved};
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

/// Binds a scope rule's property names to durable identities, scoped to the
/// collection being checked. Installed from the node's catalog at attach. A
/// rule is written in names, and everything that consumes one -- a row check
/// evaluating it, a query it narrows -- addresses properties by id, so this
/// is what carries a rule across that boundary.
pub type SelectionResolver = Arc<dyn Fn(&proto::ModelId, Predicate<Parsed>) -> Result<Predicate<Resolved>, String> + Send + Sync>;

/// Resolves an authored collection label to the model it names. A policy
/// config is keyed by label, because a person wrote it and a person writes
/// names; everything that reaches this agent addresses its collection by
/// [`proto::ModelId`]. This is the door between the two, installed from the
/// node's catalog at attach. A rule set whose label this cannot resolve
/// binds to nothing, which is a deny.
pub type ModelLookup = Arc<dyn Fn(&str) -> Option<proto::ModelId> + Send + Sync>;

/// The label of this crate's own policy collection, the one an ephemeral node
/// reads its configuration out of. Reads of it are granted before any
/// credential is consulted. Taken from the declaration itself, so the label
/// this agent grants is the one the model registers under.
pub(crate) fn policy_collection_label() -> &'static str {
    use ankurah_core::model::Model;
    crate::JwtPolicy::descriptor().label
}

/// JWT-based PolicyAgent for ankurah.
///
/// Validates incoming requests using RS256 JWTs, and enforces access control
/// based on a configurable policy (roles -> privileges -> entity rules).
#[derive(Clone)]
pub struct JwtAgent {
    state: Arc<RwLock<AgentState>>,
    policy_path: Option<PathBuf>,
    /// The name binding for rule predicates, installed at node attach
    /// (tests may install a fixture binding via
    /// [`JwtAgent::set_selection_resolver`]). Absent until then; row scope
    /// checks fail closed without it.
    resolver: Arc<RwLock<Option<SelectionResolver>>>,
    /// The label binding for policy config keys, installed at node attach
    /// beside [`Self::resolver`] (tests may install a fixture binding via
    /// [`JwtAgent::set_model_lookup`]). Absent until then; a collection whose
    /// authored label cannot be resolved has no rules, so an unprivileged
    /// caller is denied.
    models: Arc<RwLock<Option<ModelLookup>>>,
    /// Weak-node LiveQuery (EntityLiveQuery::new_weak_node) so the agent never keeps its own node alive
    policy_livequery: Arc<Mutex<Option<EntityLiveQuery>>>,
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
            resolver: Arc::new(RwLock::new(None)),
            models: Arc::new(RwLock::new(None)),
        })
    }

    /// Create a new ephemeral JwtAgent with no keys and deny-all config.
    pub fn new_ephemeral() -> Self {
        Self {
            state: Arc::new(RwLock::new(AgentState { config: PolicyConfig::default(), keys: None })),
            policy_path: None,
            policy_livequery: Arc::new(Mutex::new(None)),
            resolver: Arc::new(RwLock::new(None)),
            models: Arc::new(RwLock::new(None)),
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

    /// Install the rule-predicate name binding. Node attach does this with
    /// the node's catalog; tests exercising row scope checks without a node
    /// install a fixture binding.
    pub fn set_selection_resolver(&self, resolver: SelectionResolver) {
        *self.resolver.write().unwrap_or_else(|e| e.into_inner()) = Some(resolver);
    }

    fn selection_resolver(&self) -> Option<SelectionResolver> { self.resolver.read().unwrap_or_else(|e| e.into_inner()).clone() }

    /// Install the config-key label binding. Node attach does this with the
    /// node's catalog; tests exercising policy without a node install a
    /// fixture binding.
    pub fn set_model_lookup(&self, lookup: ModelLookup) { *self.models.write().unwrap_or_else(|e| e.into_inner()) = Some(lookup); }

    fn model_lookup(&self) -> Option<ModelLookup> { self.models.read().unwrap_or_else(|e| e.into_inner()).clone() }

    /// The authored config key naming `model`, when this policy has one. The
    /// keys are labels a person wrote, so this is where a label meets a
    /// durable identity; nothing past it carries a label.
    fn collection_key(&self, config: &PolicyConfig, model: &proto::ModelId) -> Option<String> {
        let lookup = self.model_lookup()?;
        config.collections.keys().find(|label| lookup(label).as_ref() == Some(model)).cloned()
    }

    /// Whether `model` is this crate's own policy collection
    /// ([`policy_collection_label`]).
    fn is_policy_collection(&self, model: &proto::ModelId) -> bool {
        self.model_lookup().and_then(|lookup| lookup(policy_collection_label())).as_ref() == Some(model)
    }
}

#[async_trait]
impl PolicyAgent for JwtAgent {
    type ContextData = JwtContext;

    fn on_node_ready<SE: StorageEngine + Send + Sync + 'static>(&self, node: WeakNode<SE, Self>) {
        if let Some(strong) = node.upgrade() {
            let catalog = strong.catalog.clone();
            self.set_selection_resolver(Arc::new(move |collection, predicate| {
                catalog
                    .resolve_selection(collection, ankql::ast::Selection { predicate, order_by: None, limit: None })
                    .map(|selection| selection.predicate)
                    .map_err(|error| error.to_string())
            }));
            // One agent may serve several nodes of the same system -- a
            // durable and an ephemeral in one process, say -- so each
            // attachment ADDS its catalog to the lookup rather than replacing
            // what is already there. A label's identity belongs to the
            // system, not to the node that heard about it first, so whichever
            // catalog knows the label answers for it and the two cannot
            // disagree; without this, a node's own writes would be judged
            // against a catalog that has not heard of the model yet.
            let catalog = strong.catalog.clone();
            let installed = self.model_lookup();
            self.set_model_lookup(Arc::new(move |label| {
                catalog.model_id_for(label).or_else(|| installed.as_ref().and_then(|installed| installed(label)))
            }));
        }

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
        if self.is_policy_collection(entity_after.collection()) {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may write to jwtpolicy"));
        }
        // Core admits writes to the catalog collections only through the
        // schema-registration executor, whose resolved-plan policy check runs
        // before this per-event check. An authenticated caller that passed
        // that gate may persist the catalog effects of its own registration;
        // NoUser is still denied below.
        if ankurah_core::schema::is_catalog_collection(entity_after.collection()) {
            return if matches!(cdata, JwtContext::NoUser) {
                Err(AccessDenied::ByPolicy("NoUser context cannot write schema metadata"))
            } else {
                Ok(None)
            };
        }
        if matches!(cdata, JwtContext::NoUser) {
            return Err(AccessDenied::ByPolicy("NoUser context cannot write events"));
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        let key = self.collection_key(&state.config, entity_after.collection());
        if !state.config.can_write_collection(cdata.roles(), key.as_deref()) {
            return Err(AccessDenied::CollectionDenied(*entity_after.collection()));
        }
        if !entity_before.head().is_empty() {
            enforce_write_scope(&state.config, self.selection_resolver().as_ref(), cdata, entity_before, key.as_deref())?;
        }
        enforce_write_scope(&state.config, self.selection_resolver().as_ref(), cdata, entity_after, key.as_deref())?;
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

    fn can_access_collection<C>(&self, data: &C, collection: &proto::ModelId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        if self.is_policy_collection(collection) {
            return Ok(());
        }
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(());
            }
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        let key = self.collection_key(&state.config, collection);
        for ctx in data.iterable() {
            if state.config.can_access_collection(ctx.roles(), key.as_deref()) {
                return Ok(());
            }
        }
        Err(AccessDenied::CollectionDenied(*collection))
    }

    fn filter_predicate<C>(
        &self,
        data: &C,
        collection: &proto::ModelId,
        predicate: Predicate<Resolved>,
    ) -> Result<Predicate<Resolved>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        // The policy collection is granted before any credential is
        // consulted, mirroring can_access_collection's carve-out and for the
        // same reason: an ephemeral node bootstraps its policy through a
        // livequery on this collection while its config is still deny-all,
        // and a scan-privilege check here (added with the `retrieve` tier)
        // would compose that bootstrap query to False.
        if self.is_policy_collection(collection) {
            return Ok(predicate);
        }
        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(predicate);
            }
        }

        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        let key = self.collection_key(&state.config, collection);

        if state.config.scope_rules_for_collection(key.as_deref()).is_empty() {
            // An unscoped collection passes a scan-privileged caller's
            // predicate through untouched. Any other caller the entry gate
            // admitted — which now includes retrieval-only credentials —
            // scans nothing: False, an empty answer rather than an error,
            // because arriving here at the retrieval tier is a tier limit
            // and not a fault. Every predicate is a scan here, id-shaped or
            // not: predicate-shaped retrieval (an id-bounded pass at this
            // tier, for fetch_one("id = …") and named-row live queries) is
            // a deliberate follow-up, not part of this change. (Before
            // `retrieve` existed this arm passed unconditionally, and
            // could: the entry gate had already refused every caller
            // without read or write.)
            for ctx in data.iterable() {
                if state.config.can_scan_collection(ctx.roles(), key.as_deref()) {
                    return Ok(predicate);
                }
            }
            return Ok(Predicate::False);
        }

        // A caller holding several credentials may read what any one of them
        // may read, so the query is narrowed to the union of the per-context
        // slices — the same any-of admission enforce_read_scope applies row by
        // row. Narrowing by one context alone would drop rows the caller is
        // entitled to, and a single-credential caller still gets exactly its
        // own slice. The caller's predicate stays factored in front of the
        // union — P AND (s1 OR s2), never (P AND s1) OR (P AND s2) — because a
        // storage planner reads indexable terms off the top-level conjunction
        // and treats an Or as one opaque term it can only scan. A credential
        // whose scope cannot be constructed — its filter names a claim the
        // token does not carry — contributes nothing, warned about rather than
        // fatal: the union means whatever any resolvable, authorized credential
        // admits. enforce_read_scope skips such a credential the same way, on
        // purpose, so neither half's answer turns on the order the caller
        // happens to present its credentials in. A caller whose every
        // credential is skipped leaves no slice and is refused below, exactly
        // as a caller holding nothing authorized is.
        let resolver = self.selection_resolver();
        let mut slices = Vec::new();
        for ctx in data.iterable() {
            let JwtContext::User { claims, .. } = ctx else {
                continue;
            };
            // Only scan-authorized contexts contribute: a credential that
            // cannot run this query must not widen it on its own account. A
            // retrieval-only credential reads rows it names, never rows a
            // scan surfaces, so it contributes no slice here.
            // (enforce_read_scope keeps the wide check for the per-row
            // half: a retrieval credential may satisfy a row's scope for a
            // row it named.)
            if !state.config.can_scan_collection(ctx.roles(), key.as_deref()) {
                continue;
            }

            let scope = match scoped_predicates(&state.config, key.as_deref(), claims, ScopeAccess::Read) {
                Ok(scope) => scope,
                Err(err) => {
                    tracing::warn!("skipping credential with unresolvable read scope for {collection}: {err}");
                    continue;
                }
            };

            let mut filters = scope.into_iter();
            let Some(first) = filters.next() else {
                // No scope constrains this credential, so it may read every row
                // the caller asked for and the union can be nothing narrower
                // than the caller's own predicate.
                return Ok(predicate);
            };
            let slice = filters.fold(first, |conjunction, filter| Predicate::And(Box::new(conjunction), Box::new(filter)));
            // A rule is authored in names and the query it narrows is
            // addressed by property id, so the slice binds here, against the
            // collection being read. A slice whose names do not bind admits
            // nothing and is skipped, the same as one that could not be
            // constructed; a caller left with no slice at all is refused
            // below.
            // TODO: perform this resolution at rule load time, not here
            let Some(slice) = resolve_rule_predicate(resolver.as_ref(), collection, slice) else {
                tracing::warn!("skipping credential whose read scope did not resolve for {collection}");
                continue;
            };
            // Equal-valued credentials are legal and yield equal slices;
            // repeating one costs evaluation and index extraction without
            // admitting a single extra row.
            if !slices.contains(&slice) {
                slices.push(slice);
            }
        }

        let Some(union) = slices.into_iter().reduce(|union, next| Predicate::Or(Box::new(union), Box::new(next))) else {
            return Err(AccessDenied::ByPolicy("No authorized context for row filtering"));
        };
        // Slice order is load-bearing: evaluate tries an Or's left branch first, in the order enforce_read_scope walks the same contexts.
        Ok(Predicate::And(Box::new(predicate), Box::new(union)))
    }

    fn check_read<C>(&self, data: &C, id: &proto::EntityId, collection: &proto::ModelId, state: &proto::State) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.can_access_collection(data, collection)?;

        for ctx in data.iterable() {
            if ctx.is_privileged() {
                return Ok(());
            }
        }

        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        let key = self.collection_key(&guard.config, collection);
        if guard.config.scope_rules_for_collection(key.as_deref()).is_empty() {
            return Ok(());
        }

        let entity = TemporaryEntity::new(*id, *collection, state)
            .map_err(|_| AccessDenied::ByPolicy("Read scope entity state could not be evaluated"))?;
        enforce_read_scope(&guard.config, self.selection_resolver().as_ref(), data, &entity, key.as_deref())
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
        if self.is_policy_collection(entity.collection()) && !cdata.is_privileged() {
            return Err(AccessDenied::ByPolicy("Only privileged contexts may write to jwtpolicy"));
        }
        if cdata.is_privileged() {
            return Ok(());
        }
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        let key = self.collection_key(&state.config, entity.collection());
        if !state.config.can_write_collection(cdata.roles(), key.as_deref()) {
            Err(AccessDenied::CollectionDenied(*entity.collection()))
        } else {
            enforce_write_scope(&state.config, self.selection_resolver().as_ref(), cdata, entity, key.as_deref())
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
    collection: Option<&str>,
    claims: &crate::JwtClaims,
    access: ScopeAccess,
) -> Result<Vec<Predicate<Parsed>>, AccessDenied> {
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

fn enforce_write_scope(
    config: &PolicyConfig,
    resolver: Option<&SelectionResolver>,
    cdata: &JwtContext,
    entity: &Entity,
    collection: Option<&str>,
) -> Result<(), AccessDenied> {
    let JwtContext::User { claims, .. } = cdata else {
        return Err(AccessDenied::ByPolicy("No authenticated context for write scope enforcement"));
    };

    for predicate in scoped_predicates(config, collection, claims, ScopeAccess::Write)? {
        let predicate = resolve_rule_predicate(resolver, entity.collection(), predicate)
            .ok_or(AccessDenied::ByPolicy("Write scope predicate could not be resolved"))?;
        match evaluate_predicate(entity, &predicate) {
            Ok(true) => {}
            Ok(false) => return Err(AccessDenied::ByPolicy("Write outside permitted scope")),
            Err(_) => return Err(AccessDenied::ByPolicy("Write scope predicate could not be evaluated")),
        }
    }

    Ok(())
}

/// Bind one rule predicate's names to durable identities for `collection`.
/// `None` when no binding is installed or the rule's names cannot resolve;
/// row scope checks fail closed on it.
fn resolve_rule_predicate(
    resolver: Option<&SelectionResolver>,
    collection: &proto::ModelId,
    predicate: Predicate<Parsed>,
) -> Option<Predicate<Resolved>> {
    let resolver = resolver?;
    match resolver(collection, predicate) {
        Ok(resolved) => Some(resolved),
        Err(error) => {
            tracing::warn!("rule predicate for {collection} did not resolve (row denied): {error}");
            None
        }
    }
}

fn enforce_read_scope<C>(
    config: &PolicyConfig,
    resolver: Option<&SelectionResolver>,
    data: &C,
    entity: &TemporaryEntity,
    collection: Option<&str>,
) -> Result<(), AccessDenied>
where
    C: Iterable<JwtContext>,
{
    for ctx in data.iterable() {
        let JwtContext::User { claims, .. } = ctx else {
            continue;
        };
        if !config.can_access_collection(ctx.roles(), collection) {
            continue;
        }

        // Matching filter_predicate deliberately: a credential whose scope
        // cannot be constructed admits no row and denies none either, so which
        // credential the caller lists first cannot change the answer. Debug,
        // not warn: this runs once per row, and the query-time half already
        // warns once per query about the same credential.
        let scope = match scoped_predicates(config, collection, claims, ScopeAccess::Read) {
            Ok(scope) => scope,
            Err(err) => {
                tracing::debug!("skipping credential with unresolvable read scope for {}: {err}", entity.collection);
                continue;
            }
        };

        let mut allowed = true;
        for predicate in scope {
            let Some(predicate) = resolve_rule_predicate(resolver, &entity.collection, predicate) else {
                return Err(AccessDenied::ByPolicy("Read scope predicate could not be resolved"));
            };
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
