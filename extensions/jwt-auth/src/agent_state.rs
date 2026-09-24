use std::{collections::BTreeSet, sync::Arc};

use ankurah::{Context, proto::ModelId, signals::{Mut, Wait}};
use ankurah_core::{node::WeakNode, storage::StorageEngine};

use crate::{JwtAgent, JwtContext, JwtKeys, PolicyConfig, bound_policy::BoundPolicy, graph::{self, PolicyQueries}};

/// The locally loaded policy and verification keys. Private signing material stays local.
#[derive(Clone)]
pub struct AgentState {
    pub config: Arc<PolicyConfig>,
    pub keys: Option<JwtKeys>,
    pub(crate) config_loaded: bool,
    pub(crate) policy: Option<Arc<BoundPolicy>>,
    pub(crate) policy_models: BTreeSet<ModelId>,
}

impl AgentState {
    pub(crate) fn new(config: PolicyConfig, keys: Option<JwtKeys>, config_loaded: bool) -> Self {
        Self { config: Arc::new(config), keys, config_loaded, policy: None, policy_models: BTreeSet::new() }
    }

    pub(crate) fn ready(&self) -> bool { self.config_loaded && self.keys.is_some() }
}

/// Load each policy model through its own livequery; observe stored IDs without resolving policy labels.
pub(crate) async fn start_policy_sync<SE: StorageEngine + Send + Sync + 'static>(
    node: WeakNode<SE, JwtAgent>, state: Mut<AgentState>, loaded: futures::channel::oneshot::Sender<anyhow::Result<()>>,
) {
    let initialize = async {
        let owner = node.upgrade().ok_or(ankurah_core::error::NodeDropped)?;
        let catalog = owner.catalog.clone();
        let epoch = owner.system.system_epoch().ok_or(ankurah_core::error::RetrievalError::NodeNotReady)?;
        let changed = Mut::new(());
        let subscription = owner.catalog.subscribe_changes({ let changed = changed.clone(); move || changed.set(()) });
        drop(owner);
        let models = changed.wait_for(move |_| graph::bind_models(&catalog, epoch).ok()).await;
        drop(subscription);
        state.update(|state| state.policy_models = models);

        let owner = node.upgrade().ok_or(ankurah_core::error::NodeDropped)?;
        let queries = Arc::new(PolicyQueries::new(&Context::new_weak(&owner, JwtContext::NoUser))?);
        drop(owner);
        queries.wait_durable_answered().await?;
        let refresh = {
            let queries = Arc::downgrade(&queries);
            let state = state.clone();
            // Serialize snapshot through publication so a slower refresh cannot replace newer policy.
            // Release before notifying listeners, which may trigger another refresh.
            let refresh_lock = std::sync::Mutex::new(());
            Arc::new(move || {
                let Some(queries) = queries.upgrade() else { return };
                let guard = refresh_lock.lock().unwrap_or_else(|error| error.into_inner());
                let mut next = state.value();
                let loaded = queries.snapshot().and_then(|graph| {
                    let policy = BoundPolicy::from_graph(&graph, next.policy_models.clone())?;
                    anyhow::ensure!(graph.keys.len() == 1, "waiting for one JWT verification key");
                    let pem = &graph.keys.values().next().unwrap().public_key_pem;
                    let keys = JwtKeys::from_public_pem(pem)?;
                    let signing_matches = match &next.keys {
                        Some(JwtKeys::Signing(signing)) => signing.public_key_pem().is_ok_and(|local| &local == pem),
                        _ => false,
                    };
                    if !signing_matches { next.keys = Some(keys); }
                    next.config = policy.config();
                    next.policy = Some(Arc::new(policy));
                    next.config_loaded = true;
                    Ok(())
                });
                if let Err(error) = loaded {
                    tracing::debug!("JWT policy graph is not ready: {error}");
                    next.policy = None;
                    next.config_loaded = false;
                }
                state.set_before_notify(next, || drop(guard));
            })
        };
        let subscriptions = queries.subscribe({ let refresh = refresh.clone(); move || refresh() });
        refresh();
        state.wait_for(|state| (state.ready() && state.policy.is_some()).then_some(())).await;
        Ok::<_, anyhow::Error>((queries, subscriptions))
    };
    match initialize.await {
        Ok((_queries, _subscriptions)) => {
            let _ = loaded.send(Ok(()));
            std::future::pending::<()>().await;
        }
        Err(error) => { let _ = loaded.send(Err(error)); }
    }
}
