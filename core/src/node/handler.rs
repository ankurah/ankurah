use super::Node;
use crate::{
    error::{MutationError, RetrievalError},
    policy::{AccessDenied, ContextPolicy, PolicyAgent},
    storage::{GetStateResult, StorageEngine},
    transaction::remote::RemoteTransaction,
    util::{retry::retry_on, Iterable},
};
use ankql::ast::Predicate;
use ankurah_proto::{Attested, EntityId, Event, EventId, NodeResponseBody, State, TransactionId};
use anyhow::anyhow;
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;

/// Commit received events under one authenticated principal, retrying storage conflicts.
pub async fn commit_transaction<SE, PA, C>(
    node: &Node<SE, PA>,
    cdata: &C,
    id: TransactionId,
    events: Vec<Attested<Event>>,
) -> anyhow::Result<NodeResponseBody>
where
    SE: StorageEngine + 'static,
    PA: PolicyAgent,
    C: Iterable<PA::ContextData>,
{
    let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for CommitTransaction"))?;
    node.system.require_system_ready()?;

    debug!("{node} commiting transaction {id} with {} events", events.len());
    let policy = ContextPolicy::from_credentials(&node.policy_agent, cdata);
    retry_on!(MutationError::WriteConflict, {
        let mut trx = RemoteTransaction::new(node, &policy);
        for event in &events {
            trx.add_event(event).await?;
        }
        trx.commit().await
    })?;
    Ok(NodeResponseBody::CommitComplete { id })
}

/// Return the requested events these credentials may read, checking each against its entity's state.
pub(super) async fn get_events<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent, C: Iterable<PA::ContextData>>(
    node: &Node<SE, PA>,
    credentials: &C,
    event_ids: Vec<EventId>,
) -> Result<NodeResponseBody, RetrievalError> {
    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
    let events = node.storage.get_events(event_ids).await?;
    let states = entity_states(node, events.iter().map(|event| event.payload.entity_id)).await?;

    let mut accepted = Vec::new();
    for event in events {
        // Without its entity's state there is nothing to check an event against, so it is withheld.
        let Some(state) = states.get(&event.payload.entity_id) else { continue };
        match policy.check_read_event(&event, state) {
            Ok(()) => accepted.push(event),
            Err(AccessDenied::ByPolicy(_) | AccessDenied::ModelDenied(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(NodeResponseBody::GetEvents(accepted))
}

/// Each entity's current state, read once: from its resident instance when loaded, else from storage.
async fn entity_states<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent>(
    node: &Node<SE, PA>,
    ids: impl IntoIterator<Item = EntityId>,
) -> Result<BTreeMap<EntityId, State>, RetrievalError> {
    let mut states = BTreeMap::new();
    let mut unloaded = Vec::new();
    for id in ids.into_iter().collect::<BTreeSet<_>>() {
        match node.entities.get(&id) {
            Some(entity) => { states.insert(id, entity.to_state()?); }
            None => unloaded.push(id),
        }
    }
    for result in node.storage.get_states(unloaded, &Predicate::True).await? {
        if let GetStateResult::Found(state) = result {
            states.insert(state.payload.entity_id, state.payload.state);
        }
    }
    Ok(states)
}
