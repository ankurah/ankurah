//! The shared persist funnel (D2-6, plan REV 5 section E): the ONE
//! implementation of "write this resident's state buffer to storage",
//! shared by every lane that persists — the subscription-update apply, the
//! Get response path, the remote commit lane, and the local commit lane
//! (context.rs keeps its own macro-order and routes only this step here).
//! One home is what makes the persist-currency marker and the
//! stale-instance refusal see every write.

use ankurah_proto::{self as proto};
use proto::Attested;

use crate::error::MutationError;
use crate::ingest;
use crate::node::Node;
use crate::policy::PolicyAgent;
use crate::storage::StorageEngine;

/// Persistence adapter for ingest. It supplies the node's PolicyAgent for
/// state attestation and centralizes marker-safe resident persistence for
/// apply, Get response, remote commit, and local commit lanes.
pub(crate) struct NodePersist<'a, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) node: &'a Node<SE, PA>,
    pub(crate) collection: &'a crate::storage::StorageCollectionWrapper,
}

#[async_trait::async_trait]
impl<'a, SE, PA> ingest::PersistState for NodePersist<'a, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Persist the canonical resident's current state. The completed-persist
    /// marker elides the write only when it names the resident's exact current
    /// head in the current reset epoch.
    ///
    /// The reset fence orders the whole operation before or after a hard
    /// reset. Inside that fence, the per-entity persist span serializes the
    /// canonical check, marker check, snapshot, storage write, and marker
    /// stamp. This prevents sibling lanes from landing snapshots out of order
    /// and prevents a stale duplicate resident from overwriting the canonical
    /// state. The marker records the head returned by `save_state`, never a
    /// later re-read.
    ///
    /// Lock order is reset fence first, then the per-entity persist span.
    /// Callers that already hold the reset fence use `persist_fenced` to avoid
    /// recursively acquiring the fair read lock.
    async fn persist(&self, entity: &crate::entity::Entity) -> Result<(), MutationError> {
        let _fence = self.node.entities.reset_fence_read().await;
        self.persist_fenced(entity).await
    }

    async fn persist_fenced(&self, entity: &crate::entity::Entity) -> Result<(), MutationError> {
        let span = self.node.entities.persist_span(entity.id());
        let _guard = span.lock().await;
        // Markers and snapshots belong to one Entity instance. Refuse a
        // non-canonical live instance before consulting its marker so it
        // cannot overwrite or claim durability for the canonical resident.
        // An absent map entry is allowed because this is then the only live
        // instance known to the node. A refused delivery is retryable; its
        // redelivery resolves to the canonical resident.
        if let Some(canonical) = self.node.entities.get(&entity.id()) {
            if canonical != *entity {
                return Err(MutationError::StaleInstancePersistRefused(entity.id()));
            }
        }
        let epoch = self.node.entities.reset_epoch();
        if entity.persist_marker_current(epoch) {
            return Ok(());
        }
        let persisted_head = save_state(self.node, entity, self.collection).await?;
        entity.stamp_persist_marker(epoch, persisted_head);
        Ok(())
    }
}

/// Serialize, attest, and persist the resident's current state.
/// Returns the HEAD the completed set_state wrote (the snapshot
/// to_state read under the entity lock, which may lag a concurrent
/// advance): the persist-currency marker must stamp exactly what was
/// persisted, never a re-read.
async fn save_state<SE, PA>(
    node: &Node<SE, PA>,
    entity: &crate::entity::Entity,
    collection_wrapper: &crate::storage::StorageCollectionWrapper,
) -> Result<proto::Clock, MutationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let state = entity.to_state()?;
    let persisted_head = state.head.clone();
    let entity_state = proto::EntityState { entity_id: entity.id(), model: entity.model_id()?, state };
    let attestation = node.policy_agent.attest_state(node, &entity_state);
    let attested = Attested::opt(entity_state, attestation);
    collection_wrapper.set_state(attested).await?;
    Ok(persisted_head)
}
