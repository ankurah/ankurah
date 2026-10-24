//! The storage write that ends one application, and the resident entity's advance after it.
use super::Delivery;
use crate::{
    changes::EntityChange,
    entity::RemoteTrxEntity,
    error::MutationError,
    node::Node,
    policy::PolicyAgent,
    retrieval::{LocalEventGetter, LocalStateGetter},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
};
use ankurah_proto::{self as proto, Attested};

/// The write one application decided on: the stored head it replaces, the state and events it
/// stores, and how the resident entity follows once storage commits them.
pub(super) struct EntityWrite {
    /// The stored head this write replaces; storage refuses the write if it has changed.
    pub expected_head: proto::Clock,
    pub state: Attested<proto::EntityState>,
    pub events: Vec<Attested<proto::Event>>,
    pub resident: ResidentAdvance,
}

/// How the resident entity follows a committed write.
pub(super) enum ResidentAdvance {
    /// Register the resident entity for a newly stored entity, unless a reader registered it first.
    Create,
    /// Commit the fork that adopted the stored snapshot; the change carries the events stored beside it.
    Adopt(RemoteTrxEntity),
    /// Commit the fork whose events produced the stored state; the change carries the events it applied.
    Apply(RemoteTrxEntity),
}

impl EntityWrite {
    /// Persist this write, then advance the resident entity and publish the change to resident
    /// maintenance, in a task of its own that the caller awaits. An engine may finish a dispatched
    /// commit after its caller is gone (sled commits on a blocking thread), so once the task starts,
    /// a dropped caller can no longer leave storage holding a write the resident entity lacks.
    /// Returns `None` when storage refused the write because the stored head was no longer
    /// `expected_head`.
    pub(super) async fn commit<SE, PA>(self, node: &Node<SE, PA>, delivery: Delivery) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node = node.clone();
        let (finished, completion) = tokio::sync::oneshot::channel();
        crate::task::spawn(async move {
            let _ = finished.send(self.persist_and_advance(&node, delivery).await);
        });
        completion.await.expect("entity write task panicked")
    }

    async fn persist_and_advance<SE, PA>(self, node: &Node<SE, PA>, delivery: Delivery) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let EntityWrite { expected_head, state, events, resident } = self;
        let mut storage_trx = node.storage.transaction();
        storage_trx.set_state(&expected_head, &state).await?;
        storage_trx.add_events(&events).await?;
        let mut source = node.reactor.begin_commit().await;
        if let StorageCommitOutcome::Conflict { .. } = source.record_storage_outcome(storage_trx.commit().await)? {
            return Ok(None);
        }
        // The advance needs none of the caller's history. `source` holds the commit gate, so no
        // other commit moves the resident entity before this one publishes, and a reader can only
        // load the state this write committed. So the resident entity is absent or at
        // `expected_head` and takes the committed state, or already holds it; the advance reads at
        // most this write's events, which local storage now holds.
        let history = LocalEventGetter::new(node.storage.clone(), node.durable);
        let change = match resident {
            ResidentAdvance::Create => {
                let states = LocalStateGetter::new(node.storage.clone());
                let (_, entity) = node.entities.with_state(&states, &history, state.payload.entity_id, state.payload.state).await?;
                EntityChange::new(entity, events)
            }
            ResidentAdvance::Adopt(fork) => EntityChange::new(fork.commit(&node.entities, &history).await?.into_parts().0, events),
            ResidentAdvance::Apply(fork) => fork.commit(&node.entities, &history).await?,
        };
        source.publish(std::slice::from_ref(&change), delivery == Delivery::Live);
        Ok(Some(change))
    }
}
