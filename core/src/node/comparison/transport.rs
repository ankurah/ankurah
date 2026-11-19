//! Admit only requested repair deliveries and release comparisons on cancellation.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, Weak},
};

use ankql::ast::{Resolved, Selection};
use ankurah_proto::{self as proto, ComparisonFailure, ComparisonId, EntityId};
use tokio::sync::oneshot;
use web_time::Instant;

use super::{ComparisonBudget, RepairReceipt};
use crate::{
    error::{RequestError, RetrievalError},
    node::{Node, PeerState},
    policy::{ComparisonLimits, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
    util::safemap::SafeMap,
};

pub(in crate::node) struct IncomingComparisons<CD: ContextData>(Mutex<HashMap<ComparisonId, Arc<Incoming<CD>>>>);

impl<CD: ContextData> Default for IncomingComparisons<CD> {
    fn default() -> Self { Self(Mutex::new(HashMap::new())) }
}

impl<CD: ContextData> IncomingComparisons<CD> {
    fn admit(&self, id: ComparisonId, credentials: Vec<CD>, limits: ComparisonLimits) -> Result<(), RetrievalError> {
        let expires = Instant::now().checked_add(limits.lifetime).ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
        let mut comparisons = self.0.lock().unwrap();
        comparisons.retain(|_, incoming| incoming.expires > Instant::now());
        if comparisons.len() >= limits.max_captures || limits.lifetime.is_zero() {
            return Err(failure(ComparisonFailure::LimitExceeded));
        }
        comparisons.insert(id, Arc::new(Incoming { credentials, limits, expires, deliveries: SafeMap::new() }));
        Ok(())
    }
    fn get(&self, id: &ComparisonId) -> Option<Arc<Incoming<CD>>> { self.0.lock().unwrap().get(id).cloned() }
    fn remove(&self, id: &ComparisonId) { self.0.lock().unwrap().remove(id); }
    pub(in crate::node) fn clear(&self) { self.0.lock().unwrap().clear(); }
}

pub(in crate::node) struct Incoming<CD: ContextData> {
    credentials: Vec<CD>,
    limits: ComparisonLimits,
    expires: Instant,
    deliveries: SafeMap<proto::UpdateId, Delivery>,
}

struct Delivery {
    entities: HashSet<EntityId>,
    done: oneshot::Sender<Result<(usize, usize), RetrievalError>>,
}

struct Active<CD: ContextData> {
    connection: Weak<PeerState<CD>>,
    from: EntityId,
    id: ComparisonId,
}

impl<CD: ContextData> Drop for Active<CD> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.upgrade() {
            connection.comparisons.remove(&self.id);
            let _ = connection.send_message(proto::NodeMessage::ReleaseComparison { from: self.from, comparison: self.id });
        }
    }
}

struct PendingDelivery<CD: ContextData> {
    incoming: Arc<Incoming<CD>>,
    id: proto::UpdateId,
}

impl<CD: ContextData> Drop for PendingDelivery<CD> {
    fn drop(&mut self) { self.incoming.deliveries.remove(&self.id); }
}

impl<SE: StorageEngine + 'static, PA: PolicyAgent> Node<SE, PA> {
    /// Reconciliation retries retain applied progress but always capture fresh
    /// versions. One budget bounds all attempts, including initialization.
    pub(crate) async fn reconcile_query(
        &self,
        peer: EntityId,
        selection: Selection<Resolved>,
        credentials: &Vec<PA::ContextData>,
        subscription: Option<(proto::QueryId, u32)>,
    ) -> Result<(), RetrievalError> {
        self.reconcile_selection(peer, selection, credentials, subscription).await.map(|_| ())
    }

    /// Keep the managed tree alive with the completed source coverage. Durable
    /// interests retain this owner between sweeps without retaining entity views.
    pub(in crate::node) async fn reconcile_selection(
        &self,
        peer: EntityId,
        selection: Selection<Resolved>,
        credentials: &Vec<PA::ContextData>,
        subscription: Option<(proto::QueryId, u32)>,
    ) -> Result<(proto::KnownState, super::super::trees::ComparisonTree<SE>), RetrievalError> {
        let limits = self.policy_agent.comparison_limits(credentials);
        let mut budget = ComparisonBudget::new(limits);
        for attempt in 0..3 {
            let result = async {
                let id = ComparisonId::new();
                let active = {
                    let connection = self.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
                    connection.comparisons.admit(id, credentials.clone(), limits)?;
                    Active { connection: Arc::downgrade(&connection), from: self.id, id }
                };
                let (owner, local, discovery) =
                    budget.wait(self.capture_comparison(selection.clone(), credentials)).await.map_err(super::serve::restart)?;
                let known_state = proto::KnownState {
                    digest: budget.wait(async { local.view().span_digest(..).await.map_err(super::client::view_error) }).await?.digest,
                };
                budget
                    .wait(
                        self.validate_comparison_authorization(&selection, &local.view(), credentials, &discovery, &|work| {
                            budget.charge(work)
                        }),
                    )
                    .await?;
                let key_spec = super::wire::key_spec_to_wire(local.key_spec());
                let current = self.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
                if !Weak::ptr_eq(&active.connection, &Arc::downgrade(&current)) {
                    return Err(failure(ComparisonFailure::Expired));
                }
                drop(current);
                let request = match subscription {
                    Some((query_id, version)) => proto::NodeRequestBody::SubscribeQuery {
                        query_id,
                        version,
                        selection: selection.clone(),
                        comparison: id,
                        key_spec,
                        known_state,
                    },
                    None => proto::NodeRequestBody::Fetch { selection: selection.clone(), comparison: id, key_spec, known_state },
                };
                let response = budget.wait(async { self.request(peer, credentials, request).await.map_err(RetrievalError::from) }).await?;
                let target = match (subscription, response) {
                    (None, proto::NodeResponseBody::Fetch(target)) => target,
                    (Some((expected, _)), proto::NodeResponseBody::QuerySubscribed { query_id, target }) if query_id == expected => target,
                    (_, proto::NodeResponseBody::Compared(Err(error))) => return Err(failure(error)),
                    (_, proto::NodeResponseBody::Error(error)) => return Err(RequestError::ServerError(error).into()),
                    (_, other) => return Err(RequestError::UnexpectedResponse(other).into()),
                };
                if target.id != id {
                    return Err(failure(ComparisonFailure::InvalidRequest("unexpected comparison identity".into())));
                }
                let coverage = target.known_state.clone();
                self.compare_query(peer, &selection, &local, target, credentials, &mut budget).await?;
                Ok((coverage, owner))
            }
            .await
            .map_err(super::serve::restart);
            match result {
                Err(RetrievalError::StorageError(ref error))
                    if attempt < 2
                        && matches!(
                            error.downcast_ref::<ComparisonFailure>(),
                            Some(ComparisonFailure::Expired | ComparisonFailure::Restart)
                        ) =>
                {
                    budget
                        .wait(async {
                            futures_timer::Delay::new(std::time::Duration::from_millis(1 << attempt)).await;
                            Ok(())
                        })
                        .await?;
                }
                result => return result,
            }
        }
        unreachable!("the final attempt returns")
    }

    pub(in crate::node) async fn request_comparison_repair(
        &self,
        peer: EntityId,
        comparison: ComparisonId,
        selection: &Selection<Resolved>,
        members: Vec<proto::RepairMember>,
        events_only: bool,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<RepairReceipt, RetrievalError> {
        let incoming = self
            .peer_connections
            .get(&peer)
            .and_then(|connection| connection.comparisons.get(&comparison))
            .ok_or_else(|| failure(ComparisonFailure::Expired))?;
        let delivery = proto::UpdateId::new();
        let entities: HashSet<_> = members.iter().map(|member| member.known.entity_id).collect();
        if entities.len() != members.len() || entities.len() > incoming.limits.max_members {
            return Err(failure(ComparisonFailure::InvalidRequest("duplicate or excessive repair identities".into())));
        }
        let (done, receipt) = oneshot::channel();
        incoming.deliveries.insert(delivery.clone(), Delivery { entities: entities.clone(), done });
        let _pending = PendingDelivery { incoming, id: delivery.clone() };
        let response = self
            .request(
                peer,
                credentials,
                proto::NodeRequestBody::Compare {
                    comparison,
                    selection: selection.clone(),
                    request: proto::ComparisonRequest::Repair { delivery: delivery.clone(), members, events_only },
                },
            )
            .await?;
        let unavailable = match response {
            proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Repaired { delivery: received, unavailable }))
                if received == delivery =>
            {
                unavailable
            }
            proto::NodeResponseBody::Compared(Err(error)) => return Err(failure(error)),
            proto::NodeResponseBody::Error(error) => return Err(RequestError::ServerError(error).into()),
            other => return Err(RequestError::UnexpectedResponse(other).into()),
        };
        let mut seen = HashSet::new();
        if unavailable.iter().any(|id| !entities.contains(id) || !seen.insert(*id)) {
            return Err(failure(ComparisonFailure::InvalidRequest("unexpected unavailable repair identity".into())));
        }
        let (bytes, events) = receipt.await.map_err(|_| RequestError::ConnectionLost)??;
        Ok(RepairReceipt { unavailable, bytes, events })
    }

    pub(in crate::node) async fn handle_comparison(
        &self,
        peer: EntityId,
        comparison: ComparisonId,
        selection: &Selection<Resolved>,
        request: proto::ComparisonRequest,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<proto::ComparisonReply, RetrievalError> {
        match request {
            proto::ComparisonRequest::Pages { spans } => {
                Ok(proto::ComparisonReply::Pages(self.comparison_pages(peer, comparison, selection, spans, credentials).await?))
            }
            proto::ComparisonRequest::Repair { delivery, members, events_only } => {
                let prepared = self.prepare_comparison_repair(peer, comparison, selection, members, events_only, credentials).await?;
                let connection = prepared.connection()?;
                prepared.revalidate(self, credentials).await?;
                connection
                    .send_message(proto::NodeMessage::Update(proto::NodeUpdate {
                        id: delivery.clone(),
                        from: self.id,
                        to: peer,
                        body: proto::NodeUpdateBody::Comparison { comparison, deltas: prepared.deltas },
                    }))
                    .map_err(RequestError::from)?;
                Ok(proto::ComparisonReply::Repaired { delivery, unavailable: prepared.unavailable })
            }
        }
    }

    pub(in crate::node) async fn receive_comparison_repair(
        &self,
        peer: EntityId,
        comparison: ComparisonId,
        delivery: proto::UpdateId,
        deltas: Vec<proto::EntityDelta>,
    ) -> Result<(), RetrievalError> {
        let incoming = self
            .peer_connections
            .get(&peer)
            .and_then(|connection| connection.comparisons.get(&comparison))
            .ok_or_else(|| failure(ComparisonFailure::Expired))?;
        let requested = incoming
            .deliveries
            .remove(&delivery)
            .ok_or_else(|| failure(ComparisonFailure::InvalidRequest("unrequested repair delivery".into())))?;
        let apply = async {
            if Instant::now() >= incoming.expires {
                return Err(failure(ComparisonFailure::Expired));
            }
            let limits = self.policy_agent.comparison_limits(&incoming.credentials);
            let bytes: usize = bincode::serialized_size(&deltas)?.try_into().map_err(|_| failure(ComparisonFailure::LimitExceeded))?;
            let events = deltas.iter().try_fold(0usize, |total, delta| {
                let count = match &delta.content {
                    proto::DeltaContent::StateSnapshot { .. } => 0,
                    proto::DeltaContent::EventBridge { events } => events.len(),
                    proto::DeltaContent::StateAndRelation { .. } => {
                        return Err(failure(ComparisonFailure::InvalidRequest("unsupported repair relation".into())))
                    }
                };
                total.checked_add(count).ok_or_else(|| failure(ComparisonFailure::LimitExceeded))
            })?;
            if bytes > limits.max_bytes.min(incoming.limits.max_bytes)
                || events > limits.max_events.min(incoming.limits.max_events)
                || deltas.len() > limits.max_members.min(incoming.limits.max_members)
            {
                return Err(failure(ComparisonFailure::LimitExceeded));
            }
            let mut seen = HashSet::new();
            if deltas.iter().any(|delta| !requested.entities.contains(&delta.entity_id) || !seen.insert(delta.entity_id)) {
                return Err(failure(ComparisonFailure::InvalidRequest("unexpected repair entity".into())));
            }
            let event_getter = crate::retrieval::CachedEventGetter::new(self, &incoming.credentials);
            let state_getter = crate::retrieval::LocalStateGetter::new(self.storage.clone());
            super::super::applier::NodeApplier::apply_deltas(self, &peer, deltas, &event_getter, &state_getter).await?;
            Ok((bytes, events))
        };
        let result = tokio::select! {
            result = apply => result,
            _ = futures_timer::Delay::new(incoming.expires.saturating_duration_since(Instant::now())) => Err(failure(ComparisonFailure::Expired)),
        };
        let _ = requested.done.send(result.clone());
        result.map(|_| ())
    }
}

fn failure(error: ComparisonFailure) -> RetrievalError { RetrievalError::storage(error) }

#[cfg(test)]
mod tests;
