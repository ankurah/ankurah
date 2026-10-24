//! The subscription side of queries whose membership a remote peer's state stream decides:
//! reconciling a query's rows with a stream's answer, and evaluating a stream's publication for
//! the query that owns it.

use super::{membership_item, QueryState, Subscription};
use crate::{
    reactor::{
        delivery::PreparedEvents, AbstractEntity, CandidateChanges, ChangeNotification, MembershipChange, PeerQueryMembership,
        ReactorUpdateItem, WatcherChange,
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
        membership: Option<&PeerQueryMembership>,
        predicate: &Predicate<Resolved>,
        initial_baseline: bool,
        reactor_updates: &mut Vec<ReactorUpdateItem<E, Ev>>,
    ) -> (Vec<E>, Vec<proto::EntityId>) {
        let mut entered = Vec::new();
        let mut left = Vec::new();
        let before: IndexMap<_, _> = rows.iter_entities().map(|(id, entity)| (id, entity.clone())).collect();
        // Read the current public set while holding the notification lock,
        // after any local/other-query notifications that raced capture.
        let mut included: IndexMap<_, _> = before
            .iter()
            .filter(|(id, _)| membership.is_some_and(|membership| !membership.replace.contains(id) && !membership.remove.contains(id)))
            .map(|(id, entity)| (*id, entity.clone()))
            .collect();
        for entity in answer {
            if membership.is_none_or(|membership| !membership.remove.contains(&entity.id())) && self.source.admits_query_entity(&entity) {
                included.insert(entity.id(), entity);
            }
        }
        let included = included
            .into_values()
            .filter(|entity| {
                self.source.can_read(entity)
                    && (membership
                        .is_some_and(|membership| membership.preserve.contains(&entity.id()) && before.contains_key(&entity.id()))
                        || evaluate_predicate(entity, predicate).unwrap_or(false))
            })
            .collect();
        // Replace as a set before applying its order/limit. Adding to the
        // old limited set first could evict an authoritative new member.
        if let Some(membership) = membership {
            rows.replace_all_preserving_keys(included, &membership.preserve);
        } else {
            rows.replace_all(included);
        }
        for (id, entity) in rows.iter_entities() {
            if !before.contains_key(&id) {
                let change = if membership.is_some() && !initial_baseline { MembershipChange::Add } else { MembershipChange::Initial };
                reactor_updates.push(membership_item(entity, query_id, change));
                entered.push(entity.clone());
            }
        }
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
    pub(in crate::reactor) fn evaluate_publication<C: ChangeNotification<Entity = E, Event = Ev>>(
        self,
        candidates: CandidateChanges<C>,
        prepared: PreparedEvents<Ev>,
        preserve: &HashSet<proto::EntityId>,
    ) -> (Vec<WatcherChange>, HashSet<proto::EntityId>) {
        let mut notified = HashSet::new();
        let watchers = self.evaluate_candidates(candidates, prepared, Some(preserve), Some(&mut notified));
        (watchers, notified)
    }
}
