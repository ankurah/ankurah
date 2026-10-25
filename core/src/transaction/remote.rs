use crate::entity::RemoteTrxEntity;
use crate::internal::prelude::*;
use crate::node::event_admissibility::check_genesis_membership;
use crate::policy::ContextPolicy;
use crate::reactor::ChangeNotification;
use crate::retrieval::{LocalEventGetter, LocalStateGetter};
use crate::transaction::gated_commit::{EntityWrite, GatedCommit, ResidentAdvance};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event};
use ankurah_signals::{Peek, Signal};
use indexmap::{map::Entry, IndexMap};

/// Apply received events through transaction-local forks, then commit their writes in one gated
/// storage commit.
pub(crate) struct RemoteTransaction<'a, SE: StorageEngine + 'static, PA: PolicyAgent, C: Signal> {
    node: &'a Node<SE, PA>,
    policy: &'a ContextPolicy<'a, PA, C>,
    entities: TransactionEntities,
    state_getter: LocalStateGetter<SE>,
    event_getter: LocalEventGetter<SE>,
    failed: bool,
}

/// Keep each entity's original state and working fork alive until the transaction finishes or is dropped.
struct TransactionEntities {
    entries: IndexMap<EntityId, TransactionEntity>,
}

struct TransactionEntity {
    before: Entity,
    fork: RemoteTrxEntity,
    /// The head before the transaction's first event, which its write replaces.
    expected_head: Clock,
    /// The events applied and authorized, with their attestations, which the write stores.
    events: Vec<Attested<Event>>,
}

impl<'a, SE: StorageEngine + 'static, PA: PolicyAgent, C: Signal + Peek<Vec<PA::ContextData>>> RemoteTransaction<'a, SE, PA, C> {
    pub(crate) fn new(node: &'a Node<SE, PA>, policy: &'a ContextPolicy<'a, PA, C>) -> Self {
        Self {
            node,
            policy,
            entities: TransactionEntities { entries: IndexMap::new() },
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
                let fork = RemoteTrxEntity::for_event(&self.node.entities, &self.state_getter, &self.event_getter, event).await?;
                let before = fork.snapshot();
                let expected_head = fork.head();
                entry.insert(TransactionEntity { before, fork, expected_head, events: Vec::new() })
            }
        };
        Ok(entity.fork.clone())
    }

    /// Apply and authorize one borrowed event, keeping it for the entity's write.
    /// Failure or cancellation poisons the transaction, preventing further additions or commit.
    pub(crate) async fn add_event(&mut self, event: &Attested<Event>) -> Result<(), MutationError> {
        if self.failed {
            return Err(MutationError::TransactionFailed);
        }
        // Clear only on success, so errors and dropped futures leave the transaction poisoned.
        self.failed = true;
        event.payload.validate_structure()?;
        check_genesis_membership(&event.payload)?;
        let entity = self.get_entity(&event.payload).await?;

        let mut event = event.clone();
        let transaction_entity = &self.entities.entries[&entity.id()];
        let after = entity.read();
        entity
            .apply_events(&self.event_getter, std::slice::from_mut(&mut event), |event| {
                self.policy.check_write_event(self.node, &transaction_entity.before, &after, event)
            })
            .await?;
        self.entities.entries[&entity.id()].events.push(event);
        self.failed = false;
        Ok(())
    }

    /// Commit each entity's events and resulting state in one gated storage commit, which advances
    /// the resident entities and publishes them to maintenance, then notify the reactor. The commit
    /// finishes once it takes the commit gate even if this future is dropped; a failure before then
    /// commits nothing.
    pub(crate) async fn commit(mut self) -> Result<(), MutationError> {
        if self.failed {
            return Err(MutationError::TransactionFailed);
        }
        let mut writes = Vec::with_capacity(self.entities.entries.len());
        for entity in self.entities.entries.values_mut() {
            let state = EntityState { entity_id: entity.fork.id(), state: entity.fork.to_state()? };
            let attestation = self.policy.attest_state(self.node, &state);
            writes.push(EntityWrite {
                expected_head: entity.expected_head.clone(),
                state: Attested::opt(state, attestation),
                events: std::mem::take(&mut entity.events),
                resident: ResidentAdvance::Apply(entity.fork.clone()),
            });
        }
        let mut changes = GatedCommit::stage(self.node, writes).await?.commit().await?.ok_or(MutationError::WriteConflict)?;
        // Reactor subscribers hear only of new events.
        changes.retain(|change| !change.events().is_empty());
        self.node.reactor.notify_change(changes).await;
        Ok(())
    }
}

impl Drop for TransactionEntities {
    fn drop(&mut self) {
        for entity in self.entries.values() {
            entity.fork.rollback();
        }
    }
}
