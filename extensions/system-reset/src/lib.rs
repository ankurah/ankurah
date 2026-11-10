//! A node plugin that resets a client node after a development reseed.
//!
//! An application designates one of its models, its System model, to stand for one seeding of its
//! data, and its server holds one entity of that model per seeding. [`SystemReset`] announces the
//! ids of the System entities its node holds. When a peer trusted as durable announces some, the
//! node holds some, and the two share none, the peer holds another seeding: the plugin refuses the
//! peer, after resetting the node if the application chose [`OnReseed::Reset`], and the host
//! builds a fresh node on the wiped store. Either side holding none is no verdict, so a first
//! connection, a peer that announces nothing and a peer of the same seeding all register. An
//! announcement the plugin cannot read is no verdict either: the peer registers, and the plugin
//! logs a warning. An ephemeral node that registers a durable peer comes to hold the System
//! entities the peer announced, so its first connection leaves it holding the seeding later ones
//! are compared with.

mod announcement;

use std::{collections::BTreeSet, marker::PhantomData, sync::Mutex};

use ankql::ast::{ComparisonOperator, Expr, PathExpr, Predicate, Selection};
use ankurah_core::{
    context::Context,
    error::RetrievalError,
    livequery::EntityLiveQuery,
    model::Model,
    node::{AnnouncementFailure, Node, NodePlugin, PeerRefusal, PeerRegistration},
    policy::PolicyAgent,
    session::ContextData,
    storage::StorageEngine,
    EntityId,
};
use async_trait::async_trait;
use tracing::warn;

/// The name a [`SystemReset`] announces under, which a peer's plugin of the same name reads.
pub const PLUGIN_NAME: &str = "ankurah-system-reset";

/// What a node does on meeting a durable peer that holds another seeding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnReseed {
    /// Reset the node ([`Node::reset`]), then refuse the peer.
    Reset,
    /// Refuse the peer and keep the node's data.
    Refuse,
}

/// Resets a node, or only refuses the peer, by its [`OnReseed`], when a durable peer holds
/// another seeding of the application's System model `M`.
pub struct SystemReset<M, CD> {
    on_reseed: OnReseed,
    /// What the node reads the peer's System entities under.
    context_data: CD,
    /// Brings the node the System entities its durable peer announced and it lacked. A query
    /// rather than a single read, because the peer registers only after the hook returns and a
    /// query waits for a durable peer. It stands until this plugin decides the next durable
    /// peer's registration, whatever that peer announces, or until the node halts and closes its
    /// queries. Nothing tells a plugin that a later plugin refused a peer it accepted, so the
    /// query then waits for the next durable peer.
    fetch: Mutex<Option<EntityLiveQuery>>,
    model: PhantomData<fn() -> M>,
}

impl<M: Model, CD: ContextData> SystemReset<M, CD> {
    /// A plugin over the System model `M` that reads the peer's System entities under `context_data`.
    pub fn new(on_reseed: OnReseed, context_data: CD) -> Self {
        Self { on_reseed, context_data, fetch: Mutex::new(None), model: PhantomData }
    }

    /// Make `node` come to hold the System entities `lacking` once its durable peer registers. A
    /// durable node reads only its own storage, so it fetches nothing.
    fn fetch<SE, PA>(&self, node: &Node<SE, PA>, lacking: Vec<EntityId>) -> Result<(), RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        if lacking.is_empty() || node.durable {
            return Ok(());
        }
        let ids = Expr::ExprList(lacking.iter().map(Expr::from).collect());
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("id"))),
            operator: ComparisonOperator::In,
            right: Box::new(ids),
        };
        let query = Context::new_weak(node, self.context_data.clone()).query::<M::View>(Selection::from(predicate))?;
        *self.fetch.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(query.into());
        Ok(())
    }
}

#[async_trait]
impl<SE, PA, M> NodePlugin<SE, PA> for SystemReset<M, PA::ContextData>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    M: Model + 'static,
{
    fn name(&self) -> &'static str { PLUGIN_NAME }

    async fn announce(&self, node: &Node<SE, PA>) -> Result<Option<Vec<u8>>, AnnouncementFailure> {
        let held = held::<M, _, _>(node).await.map_err(|error| AnnouncementFailure {
            plugin: PLUGIN_NAME,
            reason: format!("reading this node's System entities failed: {error}"),
        })?;
        Ok((!held.is_empty()).then(|| announcement::encode(&held)))
    }

    async fn register_peer(&self, node: &Node<SE, PA>, peer: &PeerRegistration<'_>) -> Result<(), PeerRefusal> {
        // Only a peer trusted as durable speaks for a seeding.
        if !peer.trust.durable {
            return Ok(());
        }
        // This durable peer's registration decides afresh what the node fetches.
        drop(self.fetch.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take());
        let Some(announcement) = peer.announcement else { return Ok(()) };
        let announced = match announcement::decode(announcement) {
            Ok(announced) => announced,
            Err(unreadable) => {
                warn!("Durable peer {} announced no seeding this plugin can read, so it gives no verdict: {unreadable}", peer.node_id);
                return Ok(());
            }
        };
        let held = held::<M, _, _>(node).await.map_err(|error| refusal(format!("reading this node's System entities failed: {error}")))?;
        if holds_another_seeding(&held, &announced) {
            let reason = "the peer holds another seeding of the System model";
            return Err(match self.on_reseed {
                OnReseed::Refuse => refusal(reason),
                OnReseed::Reset => match node.reset().await {
                    Ok(()) => refusal(format!("{reason}; this node reset")),
                    Err(error) => refusal(format!("{reason}; resetting this node failed: {error}")),
                },
            });
        }
        self.fetch(node, announced.difference(&held).copied().collect())
            .map_err(|error| refusal(format!("fetching the peer's System entities failed: {error}")))
    }
}

/// The rule: a node holding the System entities `held` meets another seeding in a durable peer
/// that announced `announced` when both hold some and they share none.
fn holds_another_seeding(held: &BTreeSet<EntityId>, announced: &BTreeSet<EntityId>) -> bool {
    !held.is_empty() && !announced.is_empty() && held.is_disjoint(announced)
}

/// The ids of the `M` entities `node` stores, read once its catalog holds the definitions it stores.
async fn held<M, SE, PA>(node: &Node<SE, PA>) -> Result<BTreeSet<EntityId>, RetrievalError>
where
    M: Model,
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.wait_catalog_loaded().await?;
    let Some(model) = node.catalog.model_id_for(M::descriptor().label)? else { return Ok(BTreeSet::new()) };
    let stored = node.storage.fetch_states(&Selection::from(Predicate::True).and_member_of(model)).await?;
    Ok(stored.into_iter().map(|state| state.payload.entity_id).collect())
}

fn refusal(reason: impl ToString) -> PeerRefusal { PeerRefusal { plugin: PLUGIN_NAME, reason: reason.to_string() } }

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah::{
        core::connector::PeerProvenance,
        core::node::NodeKey,
        policy::{PeerTrust, DEFAULT_CONTEXT},
        PermissiveAgent,
    };
    use ankurah_storage_sled::SledStorageEngine;
    use std::sync::Arc;

    /// The application's System model.
    #[derive(ankurah::Model, Debug, serde::Serialize, serde::Deserialize)]
    pub struct System {
        pub seeding: String,
    }

    fn ids(bytes: &[u8]) -> BTreeSet<EntityId> { bytes.iter().map(|byte| EntityId::from_bytes([*byte; 32])).collect() }

    #[tokio::test]
    async fn the_next_durable_peers_registration_drops_the_standing_fetch_whatever_it_announced() {
        let node = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
        let plugin = SystemReset::<System, _>::new(OnReseed::Reset, DEFAULT_CONTEXT);
        let durable = |announcement| PeerRegistration {
            node_id: NodeKey::generate().node_id(),
            provenance: PeerProvenance::Configured,
            trust: PeerTrust { durable: true },
            announcement,
        };
        let announced = announcement::encode(&ids(&[1]));
        plugin.register_peer(&node, &durable(Some(&announced))).await.unwrap();
        assert!(plugin.fetch.lock().unwrap().is_some(), "the node fetches the System entity it lacks");
        plugin.register_peer(&node, &durable(None)).await.unwrap();
        assert!(plugin.fetch.lock().unwrap().is_none(), "a durable peer announcing nothing drops the fetch");
    }

    #[test]
    fn only_two_nonempty_sets_that_share_no_entity_are_another_seeding() {
        assert!(holds_another_seeding(&ids(&[1]), &ids(&[2])));
        assert!(holds_another_seeding(&ids(&[1, 2]), &ids(&[3, 4])));
        assert!(!holds_another_seeding(&ids(&[1, 2]), &ids(&[2, 3])), "one shared entity is the same seeding");
        assert!(!holds_another_seeding(&ids(&[]), &ids(&[2])), "a node holding none has no verdict");
        assert!(!holds_another_seeding(&ids(&[1]), &ids(&[])), "a peer announcing none has no verdict");
    }
}
