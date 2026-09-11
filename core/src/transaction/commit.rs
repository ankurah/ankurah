use crate::context::ContextAuth;
use crate::internal::prelude::*;
use crate::node::event_admissibility::{check_membership, check_unprivileged_write};
use crate::retrieval::SuspenseEvents;
use ankurah_proto::{Attested, Clock, EntityState, Event};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use super::PendingGenesis;

/// Validate and commit the transaction, then publish its entity changes.
/// Privileged contexts bypass policy, not epoch or event-validity checks.
pub(crate) async fn commit<SE, PA>(node: &Node<SE, PA>, auth: &ContextAuth<PA>, trx: &Transaction) -> Result<Vec<Event>, MutationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let epoch = node.system.require_system_ready()?;

    if trx.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return Err(MutationError::General("Transaction already committed or rolled back".into()));
    }

    let cdata = match auth {
        ContextAuth::Sessions(sessions) => Some(sessions.write_credential()?),
        ContextAuth::Privileged => None,
    };

    let trx_id = trx.id.clone();
    let genesis_events = trx.genesis_events.read().unwrap().clone();

    let mut entity_events = Vec::new();
    let mut seen_created = std::collections::HashSet::new();
    for entity in trx.entities.iter() {
        entity.check_epoch(epoch)?;
        let mut events = Vec::with_capacity(2);
        if let Some(PendingGenesis { event: genesis, schema }) = genesis_events.get(&entity.id) {
            if !seen_created.insert(entity.id) {
                return Err(MutationError::CommitInvariant("two transaction entities claim the same frozen genesis"));
            }
            if genesis.entity_id != entity.id {
                return Err(MutationError::CommitInvariant("the frozen genesis names an entity other than the one holding it"));
            }
            if !genesis.is_entity_create() {
                return Err(MutationError::CommitInvariant("the event frozen by create() is not a genesis"));
            }
            genesis.validate_structure()?;
            if entity.head() != Clock::new([genesis.id()]) {
                return Err(MutationError::CommitInvariant("the created entity's head is not exactly its frozen genesis"));
            }
            check_membership(node, Some(schema), genesis)?;
            events.push(genesis.clone());
        }

        if let Some(event) = entity.generate_commit_event(proto::AuthorId::Unknown)? {
            check_membership(node, None, &event)?;
            events.push(event);
        }

        if !events.is_empty() {
            entity_events.push((entity.clone(), events));
        }
    }
    if seen_created.len() != genesis_events.len() {
        return Err(MutationError::CommitInvariant("an entity create() recorded is absent from the transaction's entities"));
    }

    let mut attested_events = Vec::new();
    let mut entity_attested_events = Vec::new();

    for (entity, events) in entity_events {
        if matches!(auth, ContextAuth::Sessions(_)) {
            check_unprivileged_write(entity.collection())?;
        }
        let validation_alive = Arc::new(AtomicBool::new(true));

        let mut entity_before = match &entity.kind {
            crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
            crate::entity::EntityKind::Primary => entity.clone(),
        };
        let collection = node.collections.get(entity.collection()).await?;
        let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
        let mut entity_attested = Vec::with_capacity(events.len());

        for event in events {
            event_getter.stage_event(event.clone());
            let entity_after = entity_before.snapshot(validation_alive.clone());
            entity_after.apply_event(&event_getter, &event).await?;

            let attestation = match &cdata {
                Some(cdata) => node.policy_agent.check_event(node, cdata, &entity_before, &entity_after, &event)?,
                None => None,
            };
            let attested = Attested::opt(event, attestation);

            attested_events.push(attested.clone());
            entity_attested.push(attested);
            entity_before = entity_after;
        }
        entity_attested_events.push((entity, entity_attested));
    }

    for (entity, events) in &entity_attested_events {
        let collection = node.collections.get(entity.collection()).await?;
        let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
        for attested in events {
            event_getter.commit_event(attested).await?;
        }
    }

    for (entity, events) in &entity_attested_events {
        if let Some(last) = events.last() {
            entity.commit_head(Clock::new([last.payload.id()]));
        }
    }
    if let Some(cdata) = &cdata {
        node.relay_to_required_peers(cdata, trx_id, &attested_events).await?;
    }

    let mut changes: Vec<EntityChange> = Vec::new();
    for (entity, events) in entity_attested_events {
        let collection = node.collections.get(entity.collection()).await?;

        let canonical_entity = match &entity.kind {
            crate::entity::EntityKind::Transacted { upstream, .. } => {
                let event_getter = crate::retrieval::LocalEventGetter::new(collection.clone(), node.durable);
                for attested in &events {
                    upstream.apply_event(&event_getter, &attested.payload).await?;
                }
                upstream.clone()
            }
            crate::entity::EntityKind::Primary => entity,
        };

        let state = canonical_entity.to_state()?;

        let entity_state = EntityState { entity_id: canonical_entity.id(), collection: canonical_entity.collection().clone(), state };
        let attestation = match auth {
            ContextAuth::Sessions(_) => node.policy_agent.attest_state(node, &entity_state),
            ContextAuth::Privileged => None,
        };
        let attested = Attested::opt(entity_state, attestation);
        collection.set_state(attested).await?;

        changes.push(EntityChange::new(canonical_entity, events)?);
    }

    node.reactor.notify_change(changes).await;

    Ok(attested_events.into_iter().map(|a| a.payload).collect())
}
