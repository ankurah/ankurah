//! The subscription side of queries whose membership a remote peer's state stream decides:
//! reconciling a query's rows with a stream's answer, and evaluating a stream's publication for
//! the query that owns it.

use super::{membership_item, QueryState, Subscription};
use crate::{
    reactor::{
        delivery::PreparedEvents, AbstractEntity, CandidateChanges, ChangeNotification, MembershipChange, ReactorUpdateItem,
        RemoteMembership, WatcherChange,
    },
    resultset::ResultSetWrite,
    selection::filter::{evaluate_predicate, Filterable},
};
use ankql::ast::{Predicate, Resolved};
use ankurah_proto as proto;
use indexmap::IndexMap;
use std::collections::HashSet;

impl<E: AbstractEntity + Filterable + Send + 'static> QueryState<E> {
    /// Replace this query's rows with a remote answer's as one set, and push the membership
    /// changes that makes. Returns the entities that entered and the ids of the rows that left.
    pub(super) fn reconcile_remote_rows<Ev>(
        &self,
        query_id: proto::QueryId,
        rows: &mut ResultSetWrite<'_, E>,
        answer: Vec<E>,
        membership: &RemoteMembership,
        predicate: &Predicate<Resolved>,
        initial_baseline: bool,
        reactor_updates: &mut Vec<ReactorUpdateItem<E, Ev>>,
    ) -> (Vec<E>, Vec<proto::EntityId>) {
        let before: IndexMap<_, _> = rows.iter_entities().map(|(id, entity)| (id, entity.clone())).collect();
        // Start from the rows visible now, under the notification lock: they include every local
        // or other-query change notified since the requester took the snapshot it offered.
        let mut included: IndexMap<_, _> = before
            .iter()
            .filter(|(id, _)| !membership.offered.contains(id) && !membership.absent.contains(id))
            .map(|(id, entity)| (*id, entity.clone()))
            .collect();
        for entity in answer {
            if !membership.absent.contains(&entity.id()) && self.source.admits_query_entity(&entity) {
                included.insert(entity.id(), entity);
            }
        }
        let included = included
            .into_values()
            .filter(|entity| {
                self.source.can_read(entity)
                    && ((membership.held.contains(&entity.id()) && before.contains_key(&entity.id()))
                        || evaluate_predicate(entity, predicate).unwrap_or(false))
            })
            .collect();
        // Replace as a set before applying its order/limit. Adding to the old limited set first
        // could evict a new member the answer admits.
        rows.replace_all_preserving_keys(included, &membership.held);
        let entered_as = if initial_baseline { MembershipChange::Initial } else { MembershipChange::Add };
        let mut entered = Vec::new();
        for (id, entity) in rows.iter_entities() {
            if !before.contains_key(&id) {
                reactor_updates.push(membership_item(entity, query_id, entered_as.clone()));
                entered.push(entity.clone());
            }
        }
        let mut left = Vec::new();
        for (id, entity) in before {
            if !rows.contains(&id) {
                left.push(id);
                if self.source.can_read(&entity) {
                    reactor_updates.push(membership_item(&entity, query_id, MembershipChange::Remove));
                }
            }
        }
        (entered, left)
    }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: crate::reactor::ReactorEvent> Subscription<E, Ev> {
    /// Evaluate a remote stream's publication for the query that owns the stream, returning the
    /// watcher changes to apply and the ids it notified.
    pub(in crate::reactor) fn evaluate_publication<C: ChangeNotification<Entity = E, Event = Ev>>(
        self,
        candidates: CandidateChanges<C>,
        prepared: PreparedEvents<Ev>,
        held: &HashSet<proto::EntityId>,
    ) -> (Vec<WatcherChange>, HashSet<proto::EntityId>) {
        let mut notified = HashSet::new();
        let watchers = self.evaluate_candidates(candidates, prepared, Some(held), Some(&mut notified));
        (watchers, notified)
    }
}
