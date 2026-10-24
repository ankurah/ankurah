//! One subscribed peer's live view of a resident and its membership changes.

use super::{
    bootstrap::{ReadPolicy, TreeUpdate},
    identity::Coverage,
};
use crate::{
    node::{state_sync::QueryTree, Node},
    policy::PolicyAgent,
    storage::StorageEngine,
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{self as proto, Attested};
use std::{collections::BTreeMap, sync::Weak};

pub(super) struct Route {
    pub(super) peer: proto::EntityId,
    pub(super) stream: proto::StateStreamId,
    pub(super) owner: Weak<()>,
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) members: BTreeMap<proto::EntityId, proto::QueryMember>,
}

pub(super) async fn current_state<SE, PA>(node: &Node<SE, PA>, id: proto::EntityId) -> anyhow::Result<Option<proto::EntityState>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    if let Some(entity) = node.entities.get(&id) {
        return Ok(Some(entity.to_entity_state()?));
    }
    Ok(node.storage.get_states(vec![id], &ankql::ast::Predicate::True).await?.into_iter().next().and_then(|result| match result {
        crate::storage::GetStateResult::Found(state) => Some(state.payload),
        _ => None,
    }))
}

impl Route {
    pub(super) async fn changes<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        coverage: &Coverage,
        tree: &QueryTree,
        policy: &ReadPolicy<'_, PA>,
        sources: &[TreeUpdate],
    ) -> anyhow::Result<Vec<proto::QueryChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let selected = coverage.select(tree.snapshot(), &self.selection, &node.catalog).await?;
        let ordered = match selected.ordered_ids(&self.selection, &node.catalog) {
            Ok(ordered) => ordered,
            Err(error) if self.selection.limit.is_none() && error.is::<crate::node::state_sync::tree::UnsupportedTreeSelection>() => {
                selected.members().into_values().collect()
            }
            Err(error) => return Err(error),
        };
        let next = selected.query_members();
        self.changes_for_members(node, policy, sources, next, ordered).await
    }
    pub(super) async fn changes_for_members<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        policy: &ReadPolicy<'_, PA>,
        sources: &[TreeUpdate],
        next: BTreeMap<proto::EntityId, proto::QueryMember>,
        ordered: Vec<proto::EntityId>,
    ) -> anyhow::Result<Vec<proto::QueryChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut events: BTreeMap<proto::EntityId, Vec<proto::EventId>> = BTreeMap::new();
        let mut ids = Vec::new();
        for source in sources {
            if (self.members.contains_key(&source.entity_id) || next.contains_key(&source.entity_id))
                && !events.contains_key(&source.entity_id)
            {
                ids.push(source.entity_id);
            }
            events.entry(source.entity_id).or_default().extend(source.events.iter().cloned());
        }
        // A LIMIT refill can admit unchanged entities. They need data as well.
        for id in ordered {
            if self.members.get(&id) != next.get(&id) && !ids.contains(&id) {
                ids.push(id);
            }
        }
        for id in self.members.keys() {
            if !next.contains_key(id) && !ids.contains(id) {
                ids.push(*id);
            }
        }
        let mut changes = Vec::new();
        for id in ids {
            let member = next.get(&id).cloned();
            let delta = if let Some(state) = current_state(node, id).await? {
                if policy.check_read(&id, &state.state).is_ok() {
                    let event_ids = events.remove(&id).unwrap_or_default();
                    let found = node.storage.get_events(event_ids.clone()).await?;
                    let by_id = found.into_iter().map(|event| (event.payload.id(), event)).collect::<BTreeMap<_, _>>();
                    let mut carried = Vec::new();
                    for event_id in event_ids {
                        let event = by_id.get(&event_id).ok_or_else(|| anyhow::anyhow!("live event unavailable"))?;
                        anyhow::ensure!(event.payload.entity_id == id, "live event entity mismatch");
                        policy.check_read_event(event, &state.state)?;
                        if !carried.iter().any(|existing: &proto::Attested<proto::Event>| existing.payload.id() == event_id) {
                            carried.push(event.clone());
                        }
                    }
                    let attestation = node.policy_agent.attest_state(node, &state);
                    Some(proto::DeltaContent::StateAndEvents {
                        state: Attested::opt(state, attestation).into(),
                        events: carried.into_iter().map(Into::into).collect(),
                    })
                } else {
                    None
                }
            } else {
                None
            };
            changes.push(proto::QueryChange { entity_id: id, member, delta });
        }
        self.members = next;
        Ok(changes)
    }
}
