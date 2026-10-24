//! The events a query still has to deliver for an entity. Each query keeps the
//! frontier of the events it has delivered apart from the entity's current
//! state, which an initial snapshot advances without delivering any events.
use crate::{
    error::RetrievalError,
    event_dag::{compare, AbstractCausalRelation, DEFAULT_BUDGET},
    retrieval::GetEvents,
};
use ankurah_proto as proto;

/// An event the reactor delivers. `causal_event` is the protocol event whose parents place it in
/// its entity's history; an event without one is always delivered.
pub trait ReactorEvent: Clone + Send + 'static {
    fn causal_event(&self) -> Option<&proto::Event> { None }
}
impl ReactorEvent for proto::Attested<proto::Event> {
    fn causal_event(&self) -> Option<&proto::Event> { Some(&self.payload) }
}

/// The events among `events` that `delivered` does not yet include, in order,
/// and the frontier advanced past them. An event whose parent is not the
/// frontier is traced to it through `history`: an ancestor of the frontier is
/// dropped, a descendant replaces it, and a concurrent event joins the tips it
/// does not descend from.
pub(super) async fn undelivered_events<Ev: ReactorEvent>(
    history: Option<&(dyn GetEvents + Send + Sync)>,
    mut delivered: proto::Clock,
    events: &[Ev],
) -> Result<(proto::Clock, Vec<Ev>), RetrievalError> {
    let mut accepted = Vec::new();
    for item in events {
        let Some(event) = item.causal_event() else {
            accepted.push(item.clone());
            continue;
        };
        if delivered.contains(&event.id()) {
            continue;
        }
        let head = proto::Clock::singleton(event);
        if delivered.is_empty() || delivered == event.parent {
            delivered = head;
        } else {
            let history = history.ok_or_else(|| RetrievalError::Other("event delivery requires local causal history".into()))?;
            // Trace the incoming event back to the delivered frontier. An
            // initial snapshot can establish that frontier without its payloads.
            match compare(history, &head, &delivered, DEFAULT_BUDGET).await?.relation {
                AbstractCausalRelation::Equal | AbstractCausalRelation::StrictAscends => continue,
                AbstractCausalRelation::StrictDescends { .. } => delivered = head,
                AbstractCausalRelation::DivergedSince { .. } => {
                    // Retain concurrent tips while removing only proven ancestors.
                    let mut concurrent = Vec::new();
                    for (generation, id) in delivered.entries() {
                        let tip =
                            proto::Clock::new(vec![(*generation, id.clone())]).map_err(|error| RetrievalError::Other(error.to_string()))?;
                        match compare(history, &head, &tip, DEFAULT_BUDGET).await?.relation {
                            AbstractCausalRelation::Equal | AbstractCausalRelation::StrictDescends { .. } => {}
                            AbstractCausalRelation::DivergedSince { .. } => concurrent.push((*generation, id.clone())),
                            _ => return Err(RetrievalError::Other("cannot establish event delivery frontier".into())),
                        }
                    }
                    concurrent.extend_from_slice(head.entries());
                    delivered = proto::Clock::new(concurrent).map_err(|error| RetrievalError::Other(error.to_string()))?;
                }
                _ => return Err(RetrievalError::Other("cannot establish event delivery frontier".into())),
            }
        }
        accepted.push(item.clone());
    }
    Ok((delivered, accepted))
}

#[cfg(test)]
mod tests;
