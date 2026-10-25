//! The events that bring a requester's copy of an entity from the head it holds
//! to this node's current one.

use crate::{
    event_dag::{compare, AbstractCausalRelation},
    node::Node,
    policy::{AccessDenied, ContextPolicy, PolicyAgent},
    retrieval::LocalEventGetter,
    storage::StorageEngine,
};
use ankurah_proto as proto;
use ankurah_signals::{Peek, Signal};
use anyhow::anyhow;
use std::collections::HashSet;

/// The events between `known_head` and `current_head`, parents first, when the
/// current head strictly descends from the known one and `policy` admits every
/// event between them; otherwise none.
pub(super) async fn event_bridge<SE, PA, C>(
    node: &Node<SE, PA>,
    known_head: &proto::Clock,
    current_head: &proto::Clock,
    state: &proto::State,
    policy: &ContextPolicy<'_, PA, C>,
) -> anyhow::Result<Vec<proto::Attested<proto::Event>>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Signal + Peek<Vec<PA::ContextData>>,
{
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
