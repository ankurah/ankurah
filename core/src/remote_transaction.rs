use crate::internal::prelude::*;
use crate::entity::RemoteTrxEntity;
use crate::node::event_admissibility::check_genesis_membership;
use crate::policy::ContextPolicy;
use crate::reactor::ChangeNotification;
use crate::retrieval::{LocalEventGetter, LocalStateGetter};
use crate::storage::StorageTransaction;
use ankurah_proto::{Attested, EntityId, EntityState, Event};
use ankurah_signals::{Peek, Signal};
use indexmap::{map::Entry, IndexMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Apply received events through transaction-local forks and one atomic storage transaction.
pub(crate) struct RemoteTransaction<'a, SE: StorageEngine + 'static, PA: PolicyAgent, C: Signal> {
    node: &'a Node<SE, PA>,
    policy: &'a ContextPolicy<'a, PA, C>,
    storage: SE::Transaction<'a>,
    entities: TransactionEntities,
    state_getter: LocalStateGetter<SE>,
    event_getter: LocalEventGetter<SE>,
    failed: bool,
}

/// Keep each entity's original state and working fork alive until the transaction finishes or is dropped.
struct TransactionEntities {
    entries: IndexMap<EntityId, TransactionEntity>,
    alive: Arc<AtomicBool>,
}

struct TransactionEntity {
    before: Entity,
    fork: RemoteTrxEntity,
}

impl<'a, SE: StorageEngine + 'static, PA: PolicyAgent, C: Signal + Peek<Vec<PA::ContextData>>> RemoteTransaction<'a, SE, PA, C> {
    pub(crate) fn new(node: &'a Node<SE, PA>, policy: &'a ContextPolicy<'a, PA, C>) -> Self {
        Self {
            node,
            policy,
            storage: node.storage.transaction(),
            entities: TransactionEntities { entries: IndexMap::new(), alive: Arc::new(AtomicBool::new(true)) },
            state_getter: LocalStateGetter::new(node.storage.clone()),
            // Received events must resolve their lineage from local storage only.
            event_getter: LocalEventGetter::new(node.storage.clone(), node.durable),
            failed: false,
        }
    }

    /// Reuse the working fork, capturing its original state on first access.
    async fn get_entity(&mut self, event: &Event) -> Result<RemoteTrxEntity, MutationError> {
        let entity = match self.entities.entries.entry(event.entity_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let fork = RemoteTrxEntity::for_event(
                    &self.node.entities, &self.state_getter, &self.event_getter, event, self.entities.alive.clone(),
                ).await?;
                let before = fork.snapshot();
                entry.insert(TransactionEntity { before, fork })
            }
        };
        Ok(entity.fork.clone())
    }

    /// Apply and authorize one borrowed event and its resulting state, writing both through storage.
    /// Failure or cancellation poisons the transaction, preventing further additions or commit.
    pub(crate) async fn add_event(&mut self, event: &Attested<Event>) -> Result<(), MutationError> {
        if self.failed { return Err(MutationError::TransactionFailed); }
        // Clear only on success, so errors and dropped futures leave the transaction poisoned.
        self.failed = true;
        event.payload.validate_structure()?;
        check_genesis_membership(&event.payload)?;
        let entity = self.get_entity(&event.payload).await?;
        let expected_head = entity.head();

        let mut event = event.clone();
        let before = &self.entities.entries[&entity.id()].before;
        let after = entity.read();
        entity.apply_event(&self.event_getter, &mut event, |event| {
            self.policy.check_write_event(self.node, before, &after, event)
        }).await?;
        self.storage.add_events(std::slice::from_ref(&event)).await?;
        let state = EntityState { entity_id: entity.id(), state: entity.to_state()? };
        let attestation = self.policy.attest_state(self.node, &state);
        self.storage.set_state(&expected_head, &Attested::opt(state, attestation)).await?;
        self.failed = false;
        Ok(())
    }

    /// Commit storage, then publish the events and notify the reactor.
    /// Any failure before storage commits drops the transaction without publishing its writes.
    pub(crate) async fn commit(mut self) -> Result<(), MutationError> {
        if self.failed { return Err(MutationError::TransactionFailed); }
        let publication = self.node.commit_publication_lock.lock().await;
        self.storage.commit().await?.committed()?;
        let mut changes = Vec::new();
        for (_, entity) in self.entities.entries.drain(..) {
            let change = entity.fork.commit(&self.node.entities, &self.event_getter).await?;
            if !change.events().is_empty() { changes.push(change); }
        }
        drop(publication);
        self.node.reactor.notify_change(changes).await;
        Ok(())
    }
}

impl Drop for TransactionEntities {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        for entity in self.entries.values() { entity.fork.rollback(); }
    }
}
