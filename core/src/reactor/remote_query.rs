//! Queries whose membership a remote peer's state stream decides. Each answer the stream sends is
//! installed into the query as the rows the peer selected, and each later live publication reaches
//! only the query that owns the stream.

use super::{
    AbstractEntity, CandidateChanges, ChangeNotification, GapFetcher, LocalEntitySource, PreNotifyHook, Reactor, ReactorEvent,
    ReactorSubscriptionId, ReactorUpdateItem,
};
use crate::{resultset::EntityResultSet, selection::filter::Filterable};
use ankql::ast::Resolved;
use ankurah_proto as proto;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// A query stream's notification authority, including same-version retries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOrigin {
    pub query: proto::QueryId,
    pub version: u32,
    pub request: proto::RequestId,
}

/// Reconcile the source's captured identities while retaining independent local
/// additions. Explicit live absences also remove identities outside that capture.
#[derive(Default)]
pub(crate) struct PeerQueryMembership {
    pub replace: HashSet<proto::EntityId>,
    pub remove: HashSet<proto::EntityId>,
    pub preserve: HashSet<proto::EntityId>,
    /// Captured membership heads, before newer initial payloads were applied.
    pub baseline: HashMap<proto::EntityId, proto::Clock>,
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: ReactorEvent> Reactor<E, Ev> {
    pub(crate) async fn upsert_query_from_peer<H: PreNotifyHook>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        node: Arc<dyn LocalEntitySource<E>>,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn GapFetcher<E>>,
        version: u32,
        pre_notify_hook: H,
        entities: Vec<E>,
        membership: PeerQueryMembership,
        changes: Vec<ReactorUpdateItem<E, Ev>>,
    ) -> anyhow::Result<HashSet<proto::EntityId>> {
        self.install_query(
            subscription_id,
            query_id,
            selection,
            node,
            resultset,
            gap_fetcher,
            version,
            pre_notify_hook,
            Some(entities),
            Some(membership),
            changes,
        )
        .await
    }

    /// Publish an already applied source publication to its owning query only.
    /// Other queries and direct entity subscribers received the application once.
    pub(crate) async fn notify_query_changes<C: ChangeNotification<Entity = E, Event = Ev>>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query: proto::QueryId,
        changes: Vec<C>,
        preserve: HashSet<proto::EntityId>,
    ) -> anyhow::Result<HashSet<proto::EntityId>> {
        let _notify = self.0.notify_lock.lock().await;
        let Some(subscription) = self.subscription(subscription_id) else { return Ok(HashSet::new()) };
        let changes = Arc::new(changes);
        let mut candidates = CandidateChanges::new(changes.clone());
        for offset in 0..changes.len() {
            candidates.add_query(query, offset);
        }
        let (prepared, failures) = subscription.prepare_events(&candidates, Some(&preserve), self.0.history.as_deref()).await;
        if let Some(failure) = failures.into_iter().next() {
            return Err(failure.error.into());
        }
        let (changes, notified) = subscription.evaluate_publication(candidates, prepared, &preserve);
        let mut watchers = self.0.watcher_set.lock().unwrap();
        for change in changes {
            watchers.apply_watcher_change(change);
        }
        Ok(notified)
    }
}

/// Merge source events with the same membership transition, rather than
/// removing first and later misreporting its causing events as an Update.
pub(super) fn merge_carried_changes<E: AbstractEntity + Filterable + Send + 'static, Ev>(
    membership: Vec<ReactorUpdateItem<E, Ev>>,
    changes: Vec<ReactorUpdateItem<E, Ev>>,
    carried_events: &HashSet<proto::EntityId>,
    source: &dyn LocalEntitySource<E>,
    resultset: &EntityResultSet<E>,
    complete: bool,
) -> Vec<ReactorUpdateItem<E, Ev>> {
    let positions: HashMap<_, _> = membership.iter().enumerate().map(|(offset, item)| (item.entity.id(), offset)).collect();
    let mut membership_items: Vec<_> = membership.into_iter().map(Some).collect();
    let mut ordered = Vec::with_capacity(membership_items.len() + changes.len());
    for mut change in changes {
        if !source.can_read(&change.entity) {
            continue;
        }
        if let Some(offset) = positions.get(&change.entity.id()) {
            // Finite state-only advances need no second notification for
            // rows already gaining/losing membership. Keep those generated
            // transitions in query order; live publications retain change order.
            if complete && change.events.is_empty() {
                continue;
            }
            if let Some(mut item) = membership_items[*offset].take() {
                item.entity = change.entity;
                item.events.append(&mut change.events);
                ordered.push(item);
            }
        } else if resultset.contains_key(&change.entity.id())
            && (!carried_events.contains(&change.entity.id()) || !change.events.is_empty())
        {
            ordered.push(change);
        }
    }
    ordered.extend(membership_items.into_iter().flatten());
    ordered
}

#[cfg(test)]
mod tests;
