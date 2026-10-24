//! The events a query still has to deliver for an entity. Each query keeps the
//! frontier of the events it has delivered apart from the entity's current
//! state, which an initial snapshot advances without delivering any events.
use super::{AbstractEntity, NotificationFailure, ReactorUpdateItem};
use crate::{
    error::RetrievalError,
    event_dag::{compare, AbstractCausalRelation, DEFAULT_BUDGET},
    retrieval::GetEvents,
};
use ankurah_proto as proto;
use std::collections::HashMap;

/// An event the reactor delivers. `causal_event` is the protocol event whose parents place it in
/// its entity's history; an event without one is always delivered.
pub trait ReactorEvent: Clone + Send + 'static {
    fn causal_event(&self) -> Option<&proto::Event> { None }
}
impl ReactorEvent for proto::Attested<proto::Event> {
    fn causal_event(&self) -> Option<&proto::Event> { Some(&self.payload) }
}

/// The head of the events delivered for each entity, kept by a query, or by a subscription for
/// its explicit entity subscriptions.
#[derive(Clone, Debug, Default)]
pub(super) struct DeliveredHeads(HashMap<proto::EntityId, proto::Clock>);

impl DeliveredHeads {
    /// The heads of `ids`, as a copy to plan a delivery against.
    pub(super) fn of(&self, ids: impl IntoIterator<Item = proto::EntityId>) -> Self {
        Self(ids.into_iter().filter_map(|id| self.0.get(&id).map(|head| (id, head.clone()))).collect())
    }

    /// Record `head` as `id`'s delivered head; an empty head, which delivered nothing, changes nothing.
    pub(super) fn advance(&mut self, id: proto::EntityId, head: proto::Clock) {
        if !head.is_empty() {
            self.0.insert(id, head);
        }
    }

    /// Start `id`'s delivered head at `head`, unless events were already delivered for it.
    pub(super) fn start(&mut self, id: proto::EntityId, head: proto::Clock) { self.0.entry(id).or_insert(head); }

    pub(super) fn forget(&mut self, id: &proto::EntityId) { self.0.remove(id); }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&proto::EntityId) -> bool) { self.0.retain(|id, _| keep(id)); }

    pub(super) fn clear(&mut self) { self.0.clear(); }

    /// Record the heads `staged` advanced, for the entities `keep` accepts.
    pub(super) fn extend_from(&mut self, staged: DeliveredHeads, keep: impl Fn(&proto::EntityId) -> bool) {
        self.0.extend(staged.0.into_iter().filter(|(id, _)| keep(id)));
    }

    /// The events among `events` not yet delivered for `id`, in order, and the head past them,
    /// which also becomes `id`'s delivered head here.
    pub(super) async fn take_undelivered<Ev: ReactorEvent>(
        &mut self,
        history: Option<&(dyn GetEvents + Send + Sync)>,
        id: proto::EntityId,
        events: &[Ev],
    ) -> Result<(proto::Clock, Vec<Ev>), RetrievalError> {
        let (head, events) = undelivered_events(history, self.0.get(&id).cloned().unwrap_or_default(), events).await?;
        self.advance(id, head.clone());
        Ok((head, events))
    }
}

/// The changes one target will publish, planned under its subscription's lock: a query, or for
/// `query: None` the subscription's explicit entity subscriptions.
pub(super) struct DeliveryPlan<Ev> {
    pub(super) query: Option<proto::QueryId>,
    pub(super) version: u32,
    /// The target's delivered heads for the planned entities.
    pub(super) delivered: DeliveredHeads,
    /// Each planned change's offset among the target's candidates, its entity, and its events.
    pub(super) rows: Vec<(usize, proto::EntityId, Vec<Ev>)>,
}

/// Each planned change's undelivered events and the head past them, by target and offset.
pub(super) struct PreparedEvents<Ev> {
    rows: HashMap<(Option<proto::QueryId>, usize), (proto::Clock, Vec<Ev>)>,
}

impl<Ev> PreparedEvents<Ev> {
    /// Take the events prepared for `query`'s change at `offset`, if that change was planned.
    pub(super) fn take(&mut self, query: Option<proto::QueryId>, offset: usize) -> Option<(proto::Clock, Vec<Ev>)> {
        self.rows.remove(&(query, offset))
    }
}

/// Resolve every plan's undelivered events through `history`. A plan whose history cannot be
/// proven prepares nothing and returns its failure instead.
pub(super) async fn prepare<Ev: ReactorEvent>(
    history: Option<&(dyn GetEvents + Send + Sync)>,
    plans: Vec<DeliveryPlan<Ev>>,
) -> (PreparedEvents<Ev>, Vec<NotificationFailure>) {
    let mut prepared = PreparedEvents { rows: HashMap::new() };
    let mut failures = Vec::new();
    for DeliveryPlan { query, version, mut delivered, rows } in plans {
        let mut staged = Vec::new();
        let mut failure = None;
        for (offset, id, events) in rows {
            match delivered.take_undelivered(history, id, &events).await {
                Ok(row) => staged.push(((query, offset), row)),
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = failure {
            failures.push(NotificationFailure { query, version, error });
        } else {
            prepared.rows.extend(staged);
        }
    }
    (prepared, failures)
}

/// Reduce each change's events to those not yet delivered for its entity, in order, advancing
/// `delivered` past them.
pub(super) async fn filter_changes<E: AbstractEntity, Ev: ReactorEvent>(
    history: Option<&(dyn GetEvents + Send + Sync)>,
    delivered: &mut DeliveredHeads,
    changes: &mut [ReactorUpdateItem<E, Ev>],
) -> Result<(), RetrievalError> {
    for change in changes {
        change.events = delivered.take_undelivered(history, change.entity.id(), &change.events).await?.1;
    }
    Ok(())
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
