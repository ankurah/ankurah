//! Queries whose membership a remote peer's state stream decides. Each answer the stream sends is
//! installed into the query as the rows the peer selected, and each later live publication reaches
//! only the query that owns the stream.

use super::{
    delivery::DeliveredHeads,
    subscription_state::{RemotePublication, Subscription},
    AbstractEntity, CandidateChanges, ChangeNotification, GapFetcher, LocalEntitySource, PreNotifyHook, Reactor, ReactorEvent,
    ReactorSubscriptionId, ReactorUpdateItem,
};
use crate::{resultset::EntityResultSet, selection::filter::Filterable};
use ankql::ast::{Resolved, Selection};
use ankurah_proto as proto;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Identifies the remote state stream that applied a change: the query, its selection version,
/// and the request that opened the stream. Only the query's current stream may change that
/// query's membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOrigin {
    pub query: proto::QueryId,
    pub version: u32,
    pub request: proto::RequestId,
}

/// How a remote stream's answer changes a query's rows. The peer decides the membership of the
/// rows the requester offered; other rows stay as local additions unless the stream removes them.
#[derive(Default)]
pub(crate) struct RemoteMembership {
    /// The ids the requester offered, whose membership the answer decides: an offered row the
    /// answer omits leaves the results.
    pub offered: HashSet<proto::EntityId>,
    /// The ids the stream removed, including rows it never admitted: they leave the results even
    /// as local additions.
    pub absent: HashSet<proto::EntityId>,
    /// Rows whose publication has not committed yet: they keep their visible membership and sort
    /// key until it does.
    pub held: HashSet<proto::EntityId>,
    /// The heads the stream's comparison holds for its rows. Delivery for a row without delivered
    /// events starts there, so the events between that head and a newer applied state are still
    /// delivered when they arrive.
    pub known_heads: HashMap<proto::EntityId, proto::Clock>,
}

/// One answer from a remote stream, to install into its query.
pub(crate) struct RemoteInstall<E, Ev> {
    /// The local entities for the ids the stream admitted.
    pub entities: Vec<E>,
    pub membership: RemoteMembership,
    /// The changes the answer applied, with their events, in the order the stream applied them.
    pub changes: Vec<ReactorUpdateItem<E, Ev>>,
    /// Whether this answer completes the query's initialization from the stream. A partial
    /// answer publishes membership without marking the results loaded.
    pub complete: bool,
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: ReactorEvent> Reactor<E, Ev> {
    /// Install a remote stream's answer into a query, notifying its owner before listeners, and
    /// return the ids whose notifications this installation published.
    pub(crate) async fn install_remote_query<H: PreNotifyHook>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        node: Arc<dyn LocalEntitySource<E>>,
        resultset: EntityResultSet<E>,
        gap_fetcher: std::sync::Arc<dyn GapFetcher<E>>,
        version: u32,
        pre_notify_hook: H,
        install: RemoteInstall<E, Ev>,
    ) -> anyhow::Result<HashSet<proto::EntityId>> {
        self.install_query(subscription_id, query_id, selection, node, resultset, gap_fetcher, version, pre_notify_hook, Some(install))
            .await
    }

    /// Apply a remote answer to its query under the notification lock: reduce the answer's
    /// changes to the events the query has not delivered, reconcile the query's rows with the
    /// answer, and merge the changes into the membership notifications. Returns the
    /// notifications and the delivered heads to record once they are published.
    pub(super) async fn apply_remote_answer(
        &self,
        subscription: &Subscription<E, Ev>,
        query_id: proto::QueryId,
        selection: Selection<Resolved>,
        source: &dyn LocalEntitySource<E>,
        resultset: &EntityResultSet<E>,
        version: u32,
        install: RemoteInstall<E, Ev>,
    ) -> anyhow::Result<(Vec<ReactorUpdateItem<E, Ev>>, DeliveredHeads)> {
        let RemoteInstall { entities, membership, mut changes, complete } = install;
        changes.retain(|change| source.can_read(&change.entity));
        let carried_events: HashSet<_> =
            changes.iter().filter(|change| !change.events.is_empty()).map(|change| change.entity.id()).collect();
        let delivered =
            subscription.filter_query_events(query_id, &mut changes, &membership.known_heads, self.0.history.as_deref()).await?;
        let mut items = Vec::new();
        subscription.update_query(query_id, selection, entities, version, &mut items, Some(&membership), complete)?;
        Ok((merge_carried_changes(items, changes, &carried_events, source, resultset, complete), delivered))
    }

    /// Publish a remote stream's committed publication, whose changes are already applied, to the
    /// query that owns the stream, and return the ids it notified. Other queries and entity
    /// subscriptions were notified when the changes were applied. `held` rows keep their visible
    /// membership and sort key.
    pub(crate) async fn notify_remote_publication<C: ChangeNotification<Entity = E, Event = Ev>>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query: proto::QueryId,
        changes: Vec<C>,
        held: HashSet<proto::EntityId>,
    ) -> anyhow::Result<HashSet<proto::EntityId>> {
        self.notify_remote_membership(subscription_id, query, changes, held, HashSet::new()).await
    }

    /// Publish a remote stream's committed publication that also removes rows: each `absent` row
    /// leaves the query, even a local addition, with the events of its change. `changes` must
    /// hold a change for every absent row in the results, and the stream must already admit the
    /// rows the publication adds. Returns the ids notified.
    pub(crate) async fn notify_remote_membership<C: ChangeNotification<Entity = E, Event = Ev>>(
        &self,
        subscription_id: ReactorSubscriptionId,
        query: proto::QueryId,
        changes: Vec<C>,
        held: HashSet<proto::EntityId>,
        absent: HashSet<proto::EntityId>,
    ) -> anyhow::Result<HashSet<proto::EntityId>> {
        let _notify = self.0.notify_lock.lock().await;
        let Some(subscription) = self.subscription(subscription_id) else { return Ok(HashSet::new()) };
        let changes = Arc::new(changes);
        let mut candidates = CandidateChanges::new(changes.clone());
        for offset in 0..changes.len() {
            candidates.add_query(query, offset);
        }
        let publication = RemotePublication { held: &held, absent: &absent };
        let (prepared, failures) = subscription.prepare_events(&candidates, Some(&publication), self.0.history.as_deref()).await;
        if let Some(failure) = failures.into_iter().next() {
            return Err(failure.error.into());
        }
        let (changes, notified) = subscription.evaluate_publication(candidates, prepared, &publication);
        let mut watchers = self.0.watcher_set.lock().unwrap();
        for change in changes {
            watchers.apply_watcher_change(change);
        }
        Ok(notified)
    }
}

/// Merge each carried change into its row's membership notification, so that the change's
/// causing events travel with that transition rather than following a removal as an update. A
/// change for a row without a transition is an update, unless delivery already published every
/// event it carried.
fn merge_carried_changes<E: AbstractEntity + Filterable + Send + 'static, Ev>(
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
