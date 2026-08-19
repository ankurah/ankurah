use crate::{JwtContext, JwtKeys, PolicyConfig};
use ankurah_core::{livequery::EntityLiveQuery, resultset::EntityResultSet, storage::StorageEngine, Node};
use std::sync::{Arc, Mutex, RwLock};

/// Combined policy config and verification keys, always updated atomically.
#[derive(Clone)]
pub struct AgentState {
    pub config: PolicyConfig,
    pub keys: Option<JwtKeys>,
}

/// A read guard that exposes config fields from the combined AgentState.
pub struct AgentStateReadGuard<'a> {
    guard: std::sync::RwLockReadGuard<'a, AgentState>,
}

impl<'a> AgentStateReadGuard<'a> {
    pub(crate) fn new(guard: std::sync::RwLockReadGuard<'a, AgentState>) -> Self { Self { guard } }
}

impl<'a> std::ops::Deref for AgentStateReadGuard<'a> {
    type Target = PolicyConfig;
    fn deref(&self) -> &PolicyConfig { &self.guard.config }
}

/// Start durable policy watcher: spawns a background task that watches the policy file
/// and syncs it to the node.
#[cfg(feature = "watcher")]
pub(crate) fn start_durable_policy_watcher<SE, PA>(
    node: ankurah_core::node::WeakNode<SE, PA>,
    policy_path: std::path::PathBuf,
    state_handle: Arc<RwLock<AgentState>>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: ankurah_core::policy::PolicyAgent<ContextData = JwtContext> + Send + Sync + 'static,
{
    ankurah_core::task::spawn(async move {
        let Some(node) = node.upgrade() else {
            tracing::warn!("on_node_ready: node already dropped before watcher start");
            return;
        };
        let ctx = node.context_async(JwtContext::system()).await;
        match crate::PolicyWatcher::start(policy_path, ctx, state_handle).await {
            Ok(_watcher) => {
                std::future::pending::<()>().await;
            }
            Err(e) => {
                tracing::error!("on_node_ready: failed to start policy watcher: {}", e);
            }
        }
    });
}

/// Start ephemeral policy sync: spawns a background task that opens a
/// weak-node LiveQuery over the policy collection (so the agent does not keep
/// its own node alive) and applies policy updates from the durable node.
pub(crate) fn start_ephemeral_policy_sync<SE, PA>(
    node: &Node<SE, PA>,
    state_handle: Arc<RwLock<AgentState>>,
    policy_livequery: &Arc<Mutex<Option<EntityLiveQuery>>>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: ankurah_core::policy::PolicyAgent<ContextData = JwtContext> + Send + Sync + 'static,
{
    let args: ankurah_core::node::MatchArgs<ankql::ast::Parsed> = match "true".try_into() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("on_node_ready: failed to parse selection: {}", e);
            return;
        }
    };

    let weak_node = node.weak();
    let policy_livequery = policy_livequery.clone();
    ankurah_core::task::spawn(async move {
        // The query is addressed to the policy MODEL, and an ephemeral node
        // learns that identity from the system it joins: the catalog it
        // projects from its durable peer is what turns the declaration into
        // an identity. So this waits for readiness first -- the catalog is
        // loaded by then, and it loads without consulting any policy, which
        // is what keeps this from waiting on itself.
        //
        // Registration is asserted in the same breath, for the reads below:
        // they go through JwtPolicy's typed accessors, which resolve fields
        // via the descriptor's cells at this node's epoch, and nothing else
        // on an ephemeral node runs the registration gate for this model. The
        // durable side already holds the schema, so the forwarded request is
        // a no-op plan that skips the policy verb. The readiness wait runs on
        // a cloned SystemManager with the strong node handle dropped: this
        // task must never keep its own node alive.
        let system = match weak_node.upgrade() {
            Some(node) => node.system.clone(),
            None => return,
        };
        system.wait_system_ready().await;
        let Some(node) = weak_node.upgrade() else { return };
        use ankurah_core::model::Model;
        if let Err(error) = node.catalog.ensure_registered(&JwtContext::NoUser, crate::JwtPolicy::descriptor()).await {
            tracing::warn!("ephemeral policy sync: JwtPolicy registration did not confirm: {error}");
        }
        let Some(model) = node.catalog.model_id_for(crate::agent::policy_collection_label()) else {
            tracing::error!("ephemeral policy sync: this system has no policy model registered, so no policy will be applied");
            return;
        };

        // Raw rather than typed on purpose: the query names the model
        // outright and its selection is `true`, so it asks the catalog for
        // nothing that the binding above has not already settled.
        let lq = match EntityLiveQuery::new_weak_node(&node, model, args, JwtContext::NoUser) {
            Ok(lq) => lq,
            Err(e) => {
                tracing::error!("on_node_ready: failed to create policy livequery: {}", e);
                return;
            }
        };
        let lq_clone = lq.clone();
        *policy_livequery.lock().unwrap_or_else(|e| e.into_inner()) = Some(lq);
        drop(node);

        // A policy query that failed to initialize will never carry a key or
        // a config: reading its resultset applies nothing and the
        // subscription below would never fire, leaving an agent that behaves
        // like a system with no policy at all. Say so once, loudly, instead.
        if let Err(error) = lq_clone.wait_initialized().await {
            tracing::error!("ephemeral policy sync: the policy livequery never initialized, so no policy will be applied: {error}");
            return;
        }

        apply_policy_from_resultset(&lq_clone.resultset(), &state_handle);

        let sh = state_handle.clone();
        use ankurah::signals::Subscribe;
        let _guard = lq_clone.resultset().wrap::<crate::JwtPolicyView>().subscribe(move |policies: Vec<crate::JwtPolicyView>| {
            for policy in &policies {
                apply_policy_view(policy, &sh);
            }
        });

        std::future::pending::<()>().await;
    });
}

/// Process all JwtPolicy entities in the resultset, updating the agent's config and keys.
fn apply_policy_from_resultset(resultset: &EntityResultSet, state: &Arc<RwLock<AgentState>>) {
    use ankurah_core::model::View;
    let read = resultset.read();
    for (_, entity) in read.iter_entities() {
        let view = crate::JwtPolicyView::from_entity(entity.clone());
        apply_policy_view(&view, state);
    }
}

/// Process a single JwtPolicy view, updating config and keys atomically.
fn apply_policy_view(view: &crate::JwtPolicyView, state: &Arc<RwLock<AgentState>>) {
    let new_config = match view.config_json() {
        Ok(json) => match serde_json::from_str::<PolicyConfig>(&json) {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!("Ephemeral: failed to parse policy config: {e}");
                None
            }
        },
        Err(e) => {
            tracing::warn!("Ephemeral: failed to read config_json: {e}");
            None
        }
    };

    let new_keys = match view.public_key_pem() {
        Ok(pem) if !pem.is_empty() => match JwtKeys::from_public_pem(&pem) {
            Ok(k) => Some(k),
            Err(e) => {
                tracing::warn!("Ephemeral: failed to parse public key: {e}");
                None
            }
        },
        _ => None,
    };

    // Update config and keys atomically under a single write lock
    if new_config.is_some() || new_keys.is_some() {
        let mut guard = state.write().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = new_config {
            guard.config = c;
            tracing::info!("Ephemeral: policy config updated from LiveQuery");
        }
        if let Some(k) = new_keys {
            guard.keys = Some(k);
            tracing::info!("Ephemeral: verification keys set from LiveQuery");
        }
    }
}
