//! What a node serves a peer that knows an older head of an entity.

use super::{identity::attestation, Node};
use crate::internal::prelude::*;
use crate::policy::ContextPolicy;
use anyhow::anyhow;

/// Generate EntityDelta for an entity state, using known_matches to decide between StateSnapshot and EventBridge
/// Returns None if the entity is in known_matches with equal heads (client already has current state)
pub(crate) async fn generate_entity_delta<SE, PA, C>(
    node: &Node<SE, PA>,
    known_map: &std::collections::HashMap<proto::EntityId, proto::Clock>,
    entity_state: proto::EntityState,
    cdata: &C,
) -> anyhow::Result<Option<proto::EntityDelta>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: crate::util::Iterable<PA::ContextData>,
{
    // Destructure to take ownership and avoid clones
    let proto::EntityState { entity_id, state } = entity_state;
    let current_head = &state.head;

    // Entity is in known_matches - try to optimize the response
    if let Some(known_head) = known_map.get(&entity_id) {
        // Case 1: Heads equal → return None (omit entity, client already has current state) ✓
        if known_head == current_head {
            return Ok(None);
        }

        // Case 2: Heads differ → try to build EventBridge (cheaper than full state) ✓
        let policy = ContextPolicy::from_credentials(&node.policy_agent, cdata);
        match collect_event_bridge(node, known_head, current_head, &state, &policy).await {
            Ok(attested_events) if !attested_events.is_empty() => {
                // Convert Attested<Event> to EventFragments (strips entity_id)
                let event_fragments: Vec<proto::EventFragment> = attested_events.into_iter().map(|e| e.into()).collect();

                return Ok(Some(proto::EntityDelta { entity_id, content: proto::DeltaContent::EventBridge { events: event_fragments } }));
            }
            _ => {
                // Fall through to StateSnapshot if bridge building failed or returned empty
            }
        }
    }

    // Case 3: Entity not in known_matches OR bridge building failed → send full StateSnapshot ✓
    let state_fragment = attestation::served_state(node, proto::EntityState { entity_id, state }).into();
    Ok(Some(proto::EntityDelta { entity_id, content: proto::DeltaContent::StateSnapshot { state: state_fragment } }))
}

/// Collect events between known_head and current_head using event_dag comparison.
/// Returns events needed to advance from known_head to current_head.
pub(crate) async fn collect_event_bridge<SE, PA, C>(
    node: &Node<SE, PA>,
    known_head: &proto::Clock,
    current_head: &proto::Clock,
    state: &proto::State,
    policy: &ContextPolicy<'_, PA, C>,
) -> anyhow::Result<Vec<proto::Attested<proto::Event>>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: ankurah_signals::Signal + ankurah_signals::Peek<Vec<PA::ContextData>>,
{
    use crate::event_dag::{compare, AbstractCausalRelation};
    use crate::retrieval::LocalEventGetter;
    use std::collections::HashSet;

    let event_getter = LocalEventGetter::new(node.storage.clone(), node.durable);

    // First check the causal relationship
    let comparison_result = compare(&event_getter, current_head, known_head, 100000).await?;

    match comparison_result.relation {
        AbstractCausalRelation::Equal => {
            // Heads are equal - no events needed
            Ok(vec![])
        }
        AbstractCausalRelation::StrictDescends { chain: _ } => {
            // Current descends from known - collect events by walking backward
            // TODO: Optimize with forward chain from relation (GitHub #200)
            let known_set: HashSet<_> = known_head.ids().collect();
            let mut events = Vec::new();
            let mut frontier: Vec<proto::EventId> = current_head.ids().cloned().collect();
            let mut visited: HashSet<proto::EventId> = HashSet::new();

            while !frontier.is_empty() {
                let batch = std::mem::take(&mut frontier);
                let fetched = node.storage.get_events(batch).await?;

                for event in fetched {
                    let id = event.payload.id();
                    if visited.insert(id.clone()) && !known_set.contains(&id) {
                        // Add parents to frontier (walking backward)
                        for parent in event.payload.parent.ids() {
                            if !visited.contains(parent) && !known_set.contains(parent) {
                                frontier.push(parent.clone());
                            }
                        }
                        events.push(event);
                    }
                }
            }

            // Receivers must not learn events the read policy hides.
            // One unreadable event makes the whole bridge unusable (a
            // chain with a hole loses operations downstream), so give up
            // entirely and let the caller fall back to a state snapshot,
            // which passes its own read check.
            for event in &events {
                // FIXME: should we really pass state here? Ideally we'd pass the Entity
                // but we don't necessarily have it here. It's possible the PolicyAgent
                // could fetch the Entity itself for a given clock (though this might require)
                // checkpointing. I think we're not even using this anyway
                // in favor of PolicyAgent-generated predicates anyway. If that's the case
                // maybe we omit state here and handwave about future specific-clock (snapshot) retrievals
                // as a way to discharge this theoretical requirement in the future?
                match policy.check_read_event(event, state) {
                    Ok(()) => {}
                    Err(AccessDenied::ByPolicy(_)) => return Ok(vec![]),
                    Err(e) => return Err(anyhow!("check_read_event failed while building event bridge: {}", e)),
                }
            }

            // Backward BFS discovery order is not a causal order (uneven
            // branch lengths interleave); sort parents-first so the wire
            // carries a sane order. Receivers sort again and must not
            // trust this.
            let events = crate::event_dag::ordering::topo_sort_events(events)?;
            Ok(events)
        }
        _ => {
            // Other relationships (NotDescends, Incomparable, etc.) - can't build simple bridge
            Ok(vec![])
        }
    }
}
