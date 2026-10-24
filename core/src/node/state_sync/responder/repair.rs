//! The data a responder sends toward one entity's current state: the events
//! connecting the requester's copy to it, or the state itself, under the
//! requester's read policy.

#[cfg(test)]
mod tests;

use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    storage::StorageEngine,
};
use ankurah_proto as proto;
use ankurah_signals::{Peek, Signal};
use std::collections::HashMap;

/// What a responder sends toward one entity's current state.
#[derive(Debug)]
pub(crate) enum CurrentStateRepair {
    /// The node holds no state of the entity that the principal may read.
    Denied,
    /// The requester's known head is already the current one.
    Current,
    /// The data that brings the requester's copy to the current state, at `head`.
    Delta { head: proto::Clock, delta: proto::EntityDelta },
}

/// The repairs that bring a requester holding each listed head, or nothing, to
/// each entity's current state: one per entity, in order. The states no resident
/// entity holds are read from storage in one read. `catalog` confines a request
/// that bypassed the read policy to catalog entities.
pub(crate) async fn current_state_repairs<SE, PA, C>(
    node: &Node<SE, PA>,
    policy: &ContextPolicy<'_, PA, C>,
    entities: &[(proto::EntityId, Option<proto::Clock>)],
    credentials: &Vec<PA::ContextData>,
    catalog: bool,
) -> anyhow::Result<Vec<CurrentStateRepair>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
    let mut states = node_states(node, entities.iter().map(|(id, _)| *id), policy).await?;
    let mut repairs = Vec::with_capacity(entities.len());
    for (id, known) in entities {
        repairs.push(match states.remove(id) {
            Some(state) => current_state_repair(node, policy, *id, known.clone(), state, credentials, catalog).await?,
            None => CurrentStateRepair::Denied,
        });
    }
    Ok(repairs)
}

/// The repair that brings a requester holding `known`, or nothing, to entity
/// `id`'s current `state`: connecting events when they are smaller than a
/// snapshot, else the snapshot.
async fn current_state_repair<SE, PA, C>(
    node: &Node<SE, PA>,
    policy: &ContextPolicy<'_, PA, C>,
    id: proto::EntityId,
    known: Option<proto::Clock>,
    state: proto::Attested<proto::EntityState>,
    credentials: &Vec<PA::ContextData>,
    catalog: bool,
) -> anyhow::Result<CurrentStateRepair>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
    if authorize(policy, catalog, id, &state.payload.state).is_err() {
        return Ok(CurrentStateRepair::Denied);
    }
    let head = state.payload.state.head.clone();
    if known.as_ref() == Some(&head) {
        return Ok(CurrentStateRepair::Current);
    }
    let snapshot = known.as_ref().map(|_| proto::DeltaContent::StateSnapshot { state: state.clone().into() });
    let known = known.map(|head| HashMap::from([(id, head)])).unwrap_or_default();
    let Some(mut delta) = node.generate_entity_delta(&known, state, credentials).await? else { return Ok(CurrentStateRepair::Current) };
    if matches!(delta.content, proto::DeltaContent::EventBridge { .. }) {
        if let Some(snapshot) = snapshot {
            if bincode::serialized_size(&snapshot)? < bincode::serialized_size(&delta.content)? {
                delta.content = snapshot;
            }
        }
    }
    Ok(CurrentStateRepair::Delta { head, delta })
}

/// Refuse to send entity `id`'s data unless `policy` reads `state` and, for a
/// request that bypassed the read policy, the entity belongs to the catalog.
fn authorize<PA, C>(policy: &ContextPolicy<'_, PA, C>, catalog: bool, id: proto::EntityId, state: &proto::State) -> anyhow::Result<()>
where
    PA: PolicyAgent,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
    policy.check_read(&id, state)?;
    anyhow::ensure!(!catalog || state.memberships.iter().any(crate::schema::reads_bypass_policy), "repair is outside catalog scope");
    Ok(())
}

/// The current state of each of `ids` that `policy` retrieves: a resident
/// entity's, attested afresh, or else the stored state with its attestations.
async fn node_states<SE, PA, C>(
    node: &Node<SE, PA>,
    ids: impl Iterator<Item = proto::EntityId>,
    policy: &ContextPolicy<'_, PA, C>,
) -> anyhow::Result<HashMap<proto::EntityId, proto::Attested<proto::EntityState>>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
    let mut states = HashMap::new();
    let mut stored = Vec::new();
    for id in ids {
        // Resident handles contain the node's current committed state. Re-attest
        // that state instead of reloading it; callers still authorize this payload.
        match node.entities.get(&id) {
            Some(entity) => {
                let state = entity.to_entity_state()?;
                let attestation = node.policy_agent.attest_state(node, &state);
                states.insert(id, proto::Attested::opt(state, attestation));
            }
            None => stored.push(id),
        }
    }
    if !stored.is_empty() {
        for result in node.storage.get_states(stored, &policy.retrieval_predicate()).await? {
            if let Ok(state) = proto::Attested::<proto::EntityState>::try_from(proto::GetResult::from(result)) {
                states.insert(state.payload.entity_id, state);
            }
        }
    }
    Ok(states)
}
