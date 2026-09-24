use crate::context::ContextAuth;
use crate::entity::RemoteTrxEntity;
use crate::internal::prelude::*;
use crate::node::event_admissibility::check_genesis_membership;
use crate::policy::ContextPolicy;
use crate::reactor::ChangeNotification;
use crate::retrieval::{LocalEventGetter, LocalStateGetter};
use crate::storage::StorageTransaction;
use crate::util::retry::retry_on;
use ankurah_proto::{Attested, EntityState, Event};
use std::sync::atomic::Ordering;
/// Validate and commit the transaction, then publish its entity changes.
/// Privileged contexts bypass policy, not epoch or event-validity checks.
pub(crate) async fn commit<SE, PA>(node: &Node<SE, PA>, auth: &ContextAuth<SessionSet<PA::ContextData>>, trx: &Transaction) -> Result<Vec<Event>, MutationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let epoch = node.system.require_system_ready()?;

    if trx.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return Err(MutationError::General("Transaction already committed or rolled back".into()));
    }

    let policy = ContextPolicy::new(&node.policy_agent, auth.clone());

    let mut entity_events = Vec::new();
    for entity in trx.entities.iter() {
        if entity.system_epoch() != epoch { return Err(MutationError::ForeignEntity); }
        let events = entity.prepare_events()?;
        for event in &events {
            event.payload.validate_structure()?;
            check_genesis_membership(&event.payload)?;
        }
        if !events.is_empty() {
            entity_events.push((entity, events));
        } else {
            // An unchanged edit still returns its views to the resident entity.
            entity.rollback();
        }
    }

    let event_getter = LocalEventGetter::new(node.storage.clone(), node.durable);
    let state_getter = LocalStateGetter::new(node.storage.clone());
    let mut relayed = false;
    retry_on!(MutationError::WriteConflict, {
        let mut storage_trx = node.storage.transaction();
        let mut attested_events = Vec::new();
        let mut forks = Vec::new();
        for (entity, events) in &entity_events {
            // TODO(#509): Finalize the existing transaction fork; rebase it only on write conflicts.
            // A subscription echo may already have committed this creation locally.
            let entity_after = RemoteTrxEntity::for_event(
                &node.entities, &state_getter, &event_getter, &events[0].payload, trx.alive.clone(),
            ).await?;
            let entity_before = entity_after.snapshot();
            let after = entity_after.read();
            for event in events {
                let expected_head = entity_after.head();
                let mut attested = event.clone();
                entity_after.apply_event(&event_getter, &mut attested, |event| {
                    policy.check_write_event(node, &entity_before, &after, event)
                }).await?;
                storage_trx.add_events(std::slice::from_ref(&attested)).await?;
                let state = EntityState { entity_id: entity.id(), state: entity_after.to_state()? };
                let attestation = policy.attest_state(node, &state);
                storage_trx.set_state(&expected_head, &Attested::opt(state, attestation)).await?;

                attested_events.push(attested);
            }
            forks.push((entity, entity_after));
        }

        if !relayed {
            if let ContextAuth::Sessions(sessions) = auth {
                node.relay_to_required_peers(&sessions.write_credential()?, trx.id.clone(), &attested_events).await?;
            }
            relayed = true;
        }
        let publication = node.commit_publication_lock.lock().await;
        storage_trx.commit().await?.committed()?;
        let mut changes = Vec::new();
        for (entity, fork) in forks {
            let change = fork.commit(&node.entities, &event_getter).await?;
            entity.committed(change.entity())?;
            if !change.events().is_empty() { changes.push(change); }
        }
        drop(publication);
        node.reactor.notify_change(changes).await;
        Ok(attested_events.into_iter().map(|event| event.payload).collect())
    })
}
