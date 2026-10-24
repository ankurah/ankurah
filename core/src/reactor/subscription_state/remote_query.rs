//! The subscription side of queries whose membership a remote peer's state stream decides: what
//! one change, local or from a stream, does to a query's membership; reconciling a query's rows
//! with a stream's answer; and evaluating a stream's publication for the query that owns it.

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

/// What one change does to one query's membership. Delivery planning and evaluation both take it
/// from [`QueryState::effect_of`], so every change evaluation publishes was planned the same way.
pub(super) struct ChangeEffect {
    /// Whether the query's owner may read the entity.
    pub(super) readable: bool,
    /// Whether the entity was a member before the change.
    pub(super) did_match: bool,
    /// Whether the entity is a member after the change.
    pub(super) matches: bool,
}

impl ChangeEffect {
    /// Whether the change reaches the query's owner: a readable row that is or was a member, or
    /// one the subscription explicitly subscribes to.
    pub(super) fn publishes(&self, subscribed: bool) -> bool { self.readable && (self.matches || self.did_match || subscribed) }
}

/// What a committed publication from a query's remote stream decides beyond its changes' states.
pub(in crate::reactor) struct RemotePublication<'a> {
    /// Rows whose own publication has not committed: they keep their visible membership and sort
    /// key until it does.
    pub(in crate::reactor) held: &'a HashSet<proto::EntityId>,
    /// Rows the publication removes, even local additions.
    pub(in crate::reactor) absent: &'a HashSet<proto::EntityId>,
}

impl<E: AbstractEntity + Filterable + Send + 'static> QueryState<E> {
    /// What `change` does to this query's membership, or `None` when the change comes from the
    /// query's own remote stream but may not move its membership: the stream was replaced, the
    /// change waits for its publication to commit, or the stream did not admit the entity. Such a
    /// change must neither restore a row the stream removed nor undo a later local or Fetch
    /// admission. Within the stream's own `publication`, an absent row leaves and a held row keeps
    /// its membership.
    pub(super) fn effect_of<C: ChangeNotification<Entity = E>>(
        &self,
        query_id: proto::QueryId,
        predicate: &Predicate<Resolved>,
        change: &C,
        publication: Option<&RemotePublication<'_>>,
    ) -> Option<ChangeEffect> {
        let entity = change.entity();
        let readable = self.source.can_read(entity);
        let did_match = self.resultset.contains_key(&entity.id());
        if let Some(origin) = change.query_origin().filter(|origin| origin.query == query_id) {
            let current = self.source.query_origin() == Some(origin) && !change.defers_query_membership();
            if current && publication.is_some_and(|publication| publication.absent.contains(&entity.id())) {
                return Some(ChangeEffect { readable, did_match, matches: false });
            }
            if readable && (!current || !self.source.admits_query_entity(entity)) {
                return None;
            }
        }
        let matches = readable
            && if publication.is_some_and(|publication| publication.held.contains(&entity.id())) {
                did_match
            } else {
                evaluate_predicate(entity, predicate).unwrap_or(false)
            };
        Some(ChangeEffect { readable, did_match, matches })
    }

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
        publication: &RemotePublication<'_>,
    ) -> (Vec<WatcherChange>, HashSet<proto::EntityId>) {
        let mut notified = HashSet::new();
        let watchers = self.evaluate_candidates(candidates, prepared, Some(publication), Some(&mut notified));
        (watchers, notified)
    }
}
