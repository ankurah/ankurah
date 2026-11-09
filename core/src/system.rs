use crate::error::{NodeHaltReason, NodeReadinessError};
use crate::internal::prelude::*;
use ankurah_signals::{Mut, Read, Wait};
use anyhow::Result;
use std::sync::{Arc, RwLock};

use crate::entity::WeakEntitySet;
use crate::property::{Property, PropertyError};
use crate::retrieval::{LocalEventGetter, LocalStateGetter};
use crate::value::Value;
pub const SYSTEM_COLLECTION_ID: &str = "_ankurah_system";
pub const PROTECTED_COLLECTIONS: &[&str] = &[SYSTEM_COLLECTION_ID];

/// A node's local system state: the `_ankurah_system` items in its storage,
/// readiness once they load, and the node's run state up to terminal halt.
pub struct SystemManager<SE>(Arc<Inner<SE>>);
impl<SE> Clone for SystemManager<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct Inner<SE> {
    storage: Arc<SE>,
    entities: WeakEntitySet,
    durable: bool,
    items: RwLock<Vec<Entity>>,
    /// Set once the items load; cleared again only when local storage is reset.
    system_ready: RwLock<bool>,
    node_state: Mut<NodeState>,
}

impl<SE> SystemManager<SE>
where SE: StorageEngine + Send + Sync + 'static
{
    pub(crate) fn new(storage: Arc<SE>, entities: WeakEntitySet, durable: bool) -> Self {
        let me = Self(Arc::new(Inner {
            storage,
            entities,
            durable,
            items: RwLock::new(Vec::new()),
            system_ready: RwLock::new(false),
            node_state: Mut::new(NodeState::Uninitialized),
        }));
        {
            let me = me.clone();
            crate::task::spawn(async move {
                match me.load_system_items().await {
                    Ok(()) => me.mark_system_ready(),
                    Err(e) => {
                        me.halt(NodeHaltReason::SystemLoad(e.to_string()));
                    }
                }
            });
        }
        me
    }

    pub fn items(&self) -> Vec<Entity> { self.0.items.read().unwrap().clone() }

    /// Whether the local system has loaded and the node has not halted.
    pub fn is_system_ready(&self) -> bool { self.check_not_halted().is_ok() && *self.0.system_ready.read().unwrap() }

    /// This node's immutable system epoch, absent until the local system loads or after a reset.
    pub fn system_epoch(&self) -> Option<SystemEpoch> { self.0.system_ready.read().unwrap().then_some(self.0.entities.system_epoch()) }

    pub(crate) fn require_system_ready(&self) -> Result<SystemEpoch, NodeReadinessError> {
        self.check_not_halted()?;
        self.system_epoch().ok_or(NodeReadinessError::NotReady)
    }

    pub(crate) fn node_state(&self) -> Read<NodeState> { self.0.node_state.read() }

    /// Publish successful initialization without reviving a halted node.
    pub(crate) fn mark_running(&self) {
        self.0.node_state.update(|state| {
            if matches!(state, NodeState::Startup) {
                *state = NodeState::Running;
            }
        });
    }

    /// Reject terminal halt without blocking the work that initializes this node.
    pub(crate) fn check_not_halted(&self) -> Result<(), NodeHaltReason> {
        self.0.node_state.with(|state| state.halt_reason().cloned().map_or(Ok(()), Err))
    }

    /// Wait for terminal halt and return its reason.
    pub(crate) async fn wait_halted(&self) -> NodeHaltReason { self.node_state().wait_for(|state| state.halt_reason().cloned()).await }

    /// Publish terminal halt; services observe the state and stop their own work.
    /// In-flight storage work may still finish.
    pub(crate) fn halt(&self, halt_reason: NodeHaltReason) -> NodeHaltReason {
        self.0.node_state.update(|state| match state {
            NodeState::Halted(reason) => reason.clone(),
            NodeState::Uninitialized | NodeState::Startup | NodeState::Running => {
                *state = NodeState::Halted(halt_reason.clone());
                halt_reason
            }
        })
    }

    /// Publish readiness once the local system items have loaded.
    fn mark_system_ready(&self) {
        self.0.node_state.update(|state| {
            *self.0.system_ready.write().unwrap() = true;
            if matches!(state, NodeState::Uninitialized) {
                *state = NodeState::Startup;
            }
        });
    }

    /// Wait for the local system to load, or return the reason the node halted.
    pub async fn wait_system_ready(&self) -> Result<(), NodeHaltReason> {
        self.node_state()
            .wait_for(|state| match state {
                NodeState::Uninitialized => None,
                NodeState::Startup | NodeState::Running => Some(Ok(())),
                NodeState::Halted(reason) => Some(Err(reason.clone())),
            })
            .await
    }

    /// Halt with [`NodeHaltReason::Reset`], then delete everything in storage.
    /// The wipe runs in its own task, so it finishes even if the caller stops waiting.
    pub(crate) async fn reset(&self) -> Result<()> {
        self.halt(NodeHaltReason::Reset);
        let system = self.clone();
        let (finished, completion) = tokio::sync::oneshot::channel();
        crate::task::spawn(async move {
            let _ = finished.send(system.wipe_storage().await);
        });
        // Cannot fail in practice: the task always sends before it ends, and it does not panic.
        completion.await.expect("storage wipe task ended without reporting")
    }

    /// Delete persisted entities, events and materializations, and clear the loaded items.
    async fn wipe_storage(&self) -> Result<()> {
        self.0.storage.delete_all().await?;
        self.0.items.write().unwrap().clear();
        *self.0.system_ready.write().unwrap() = false;
        Ok(())
    }

    async fn load_system_items(&self) -> Result<()> {
        let storage = self.0.storage.clone();
        let state_getter = LocalStateGetter::new(storage.clone());
        let event_getter = LocalEventGetter::new(storage.clone(), self.0.durable);

        let mut entities = Vec::new();
        for state in storage
            .fetch_states(&ankql::ast::Selection {
                predicate: ankql::ast::Predicate::MemberOf(ModelId::System(proto::SystemModel::System)),
                order_by: None,
                limit: None,
            })
            .await?
        {
            let (_entity_changed, entity) =
                self.0.entities.with_state(&state_getter, &event_getter, state.payload.entity_id, state.payload.state.clone()).await?;
            entities.push(entity);
        }
        self.0.items.write().unwrap().extend(entities);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{node::Node, policy::PermissiveAgent, test_utils::TestStorage};

    #[tokio::test]
    async fn an_ephemeral_node_loads_its_system_without_any_peer() {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), node.system.wait_system_ready()).await.unwrap().unwrap();
        assert!(node.system.is_system_ready());
        assert!(node.system.system_epoch().is_some());
    }

    #[tokio::test]
    async fn an_unreadable_persisted_system_item_fails_readiness_instead_of_hanging() {
        let storage = Arc::new(TestStorage::default());
        let state = proto::State {
            state_buffers: proto::StateBuffers([("lww".to_owned(), vec![0xff])].into()),
            memberships: [ModelId::System(proto::SystemModel::System)].into(),
            head: proto::Clock::genesis(proto::EventId::from_bytes([1; 32])),
        };
        storage.set_state(proto::EntityState { entity_id: proto::EntityId::from_bytes([2; 32]), state }.into());

        let node = Node::new_durable(storage, PermissiveAgent::new());
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), node.system.wait_system_ready()).await.unwrap().unwrap_err();
        assert!(matches!(error, NodeHaltReason::SystemLoad(_)));
        assert_eq!(node.context_async(crate::policy::DEFAULT_CONTEXT).await.err(), Some(error.into()));
    }
}

impl Property for proto::sys::Item {
    const VALUE_TYPE: &'static str = "string";

    fn into_value(&self) -> std::result::Result<Option<Value>, crate::property::PropertyError> {
        Ok(Some(Value::String(
            serde_json::to_string(self).map_err(|_| PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })?,
        )))
    }

    fn from_value(value: Option<Value>) -> std::result::Result<Self, crate::property::PropertyError> {
        if let Some(Value::String(string)) = value {
            let item: proto::sys::Item = serde_json::from_str(&string)
                .map_err(|_| PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })?;
            Ok(item)
        } else {
            Err(PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })
        }
    }
}
