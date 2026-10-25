//! The update content a responder sends toward one entity's current state: the
//! events connecting the requester's copy to it, or the state itself, under the
//! requester's read policy.

mod bridge;
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
    Unavailable,
    /// The requester's known head is the current one, `head`.
    Current { head: proto::Clock },
    /// The requester lacks the current state at `head`; `content` brings its copy there.
    Lacking { head: proto::Clock, content: proto::UpdateContent },
}

/// The repairs that bring a requester holding each listed head, or nothing, to
/// each entity's current state: one per entity, in order. The states no resident
/// entity holds are read from storage in one read. `catalog` confines a request
/// that bypassed the read policy to catalog entities.
pub(crate) async fn current_state_repairs<SE, PA, C>(
    node: &Node<SE, PA>,
    policy: &ContextPolicy<'_, PA, C>,
    entities: &[(proto::EntityId, Option<proto::Clock>)],
    catalog: bool,
) -> anyhow::Result<Vec<CurrentStateRepair>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
    let mut states = current_states(node, entities.iter().map(|(id, _)| *id), policy).await?;
    let mut repairs = Vec::with_capacity(entities.len());
    for (id, known) in entities {
        repairs.push(match states.remove(id) {
            Some(state) => current_state_repair(node, policy, *id, known.as_ref(), state, catalog).await,
            None => CurrentStateRepair::Unavailable,
        });
    }
    Ok(repairs)
}

/// The repair that brings a requester holding `known`, or nothing, to entity
/// `id`'s current `state`: the events from its head when they connect it to the
/// current one, else the state. The events go even when the state is smaller: a
/// requester adopts a state over an older copy only after proving its lineage,
/// which fetches those same events one request at a time.
async fn current_state_repair<SE, PA, C>(
    node: &Node<SE, PA>,
    policy: &ContextPolicy<'_, PA, C>,
    id: proto::EntityId,
    known: Option<&proto::Clock>,
    state: proto::Attested<proto::EntityState>,
    catalog: bool,
) -> CurrentStateRepair
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
    if !may_send(policy, catalog, id, &state.payload.state) {
        return CurrentStateRepair::Unavailable;
    }
    let head = state.payload.state.head.clone();
    if known == Some(&head) {
        return CurrentStateRepair::Current { head };
    }
    if let Some(known) = known {
        // Any failure to build the bridge sends the state instead, which is always
        // a valid repair; a known head this node never saw fails this way.
        match bridge::event_bridge(node, known, &head, &state.payload.state, policy).await {
            Ok(events) if !events.is_empty() => {
                let content = proto::UpdateContent::EventOnly(events.into_iter().map(Into::into).collect());
                return CurrentStateRepair::Lacking { head, content };
            }
            Ok(_) => {}
            Err(error) => tracing::debug!(%id, %error, "no event bridge from the requester's head; sending the state"),
        }
    }
    CurrentStateRepair::Lacking { head, content: proto::UpdateContent::StateAndEvent(state.into(), vec![]) }
}

/// Whether this node may send entity `id`'s data: `policy` reads `state` and, for
/// a request that bypassed the read policy, the entity belongs to the catalog.
fn may_send<PA, C>(policy: &ContextPolicy<'_, PA, C>, catalog: bool, id: proto::EntityId, state: &proto::State) -> bool
where
    PA: PolicyAgent,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
    policy.check_read(&id, state).is_ok() && (!catalog || state.memberships.iter().any(crate::schema::reads_bypass_policy))
}

/// The current state of each of `ids` that `policy` retrieves: a resident
/// entity's, attested afresh, or else the stored state with its attestations.
async fn current_states<SE, PA, C>(
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
