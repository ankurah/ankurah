use crate::{JwtContext, JwtKeys, PolicyConfig};
use ankurah_core::{livequery::EntityLiveQuery, resultset::EntityResultSet, storage::StorageEngine, Node};
use ankurah_proto as proto;
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
///
/// Exported so an application can **wrap** [`JwtAgent`](crate::JwtAgent) — a
/// newtype delegating [`PolicyAgent`](ankurah_core::policy::PolicyAgent) while
/// adding its own checks — and reuse the policy-sync machinery instead of
/// duplicating it. Pass the inner agent's state handle
/// ([`JwtAgent::state_handle`](crate::JwtAgent::state_handle)). Generic over
/// any `PolicyAgent<ContextData = JwtContext>`, so it accepts the wrapper type.
#[cfg(feature = "watcher")]
pub fn start_durable_policy_watcher<SE, PA>(
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

/// Start ephemeral policy sync: creates a weak-node LiveQuery on "jwtpolicy" (so the
/// agent does not keep its own node alive) and spawns a background task that applies
/// policy updates from the durable node.
///
/// Exported for the same wrapping use case as
/// [`start_durable_policy_watcher`]: a wrapper passes the inner agent's state
/// handle ([`JwtAgent::state_handle`](crate::JwtAgent::state_handle)) plus its
/// own `Arc<Mutex<Option<EntityLiveQuery>>>` cell (which owns the livequery's
/// lifetime), and gets the standard ephemeral sync without reimplementing it.
pub fn start_ephemeral_policy_sync<SE, PA>(
    node: &Node<SE, PA>,
    state_handle: Arc<RwLock<AgentState>>,
    policy_livequery: &Arc<Mutex<Option<EntityLiveQuery>>>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: ankurah_core::policy::PolicyAgent<ContextData = JwtContext> + Send + Sync + 'static,
{
    let args: ankurah_core::node::MatchArgs = match "true".try_into() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("on_node_ready: failed to parse selection: {}", e);
            return;
        }
    };
    let lq = match EntityLiveQuery::new_weak_node(node, proto::CollectionId::from("jwtpolicy"), args, JwtContext::NoUser) {
        Ok(lq) => lq,
        Err(e) => {
            tracing::error!("on_node_ready: failed to create policy livequery: {}", e);
            return;
        }
    };

    let lq_clone = lq.clone();
    *policy_livequery.lock().unwrap_or_else(|e| e.into_inner()) = Some(lq);

    ankurah_core::task::spawn(async move {
        lq_clone.wait_initialized().await;

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
