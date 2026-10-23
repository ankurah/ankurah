use std::{collections::BTreeMap, sync::Arc};

use ankurah_proto::{self as proto, ComparisonProgress, RepairItem, StateAnswer, StateFrameBody, StateItem};
use anyhow::{bail, ensure};
use bincode::Options;
use futures::FutureExt;

use super::{feedback::Feedback, IncomingStream, Snapshot};
use crate::{
    error::{RequestError, RetrievalError},
    node::{applier::NodeApplier, Node},
    policy::PolicyAgent,
    retrieval::{CachedEventGetter, LocalEventGetter, LocalStateGetter},
    storage::StorageEngine,
};

mod driver;
mod projection;
pub(crate) use driver::RefreshHandle;

pub(crate) struct RemoteState {
    pub stream_id: proto::StateStreamId,
    pub ids: Vec<proto::EntityId>,
    driver: driver::Driver,
}

/// Install the origin before sending: a pushed continuation may beat metadata.
pub(crate) async fn request_state<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    credentials: &Vec<PA::ContextData>,
    body: proto::NodeRequestBody,
) -> Result<(IncomingStream, proto::NodeResponseBody), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let query = match &body {
        proto::NodeRequestBody::SubscribeQuery { query_id, version, .. } => Some((*query_id, *version)),
        _ => None,
    };
    let connection = node.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
    let weak = node.weak();
    let original_connection = Arc::downgrade(&connection);
    let abandon = Arc::new(move |stream_id| {
        let weak = weak.clone();
        let original_connection = original_connection.clone();
        crate::task::spawn(async move {
            if let Some(node) = weak.upgrade() {
                let Some(connection) = original_connection.upgrade() else { return };
                if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection)) {
                    return;
                }
                let _ = send_abandonment(&connection, node.id, peer, stream_id);
            }
        });
    });
    let stream = connection.incoming_states.register(proto::RequestId::new(), query, Some(abandon))?;
    drop(connection);
    let response = state_deadline(node.request_with_id(peer, credentials, stream.request.clone(), body)).await??;
    Ok((stream, response))
}

pub(crate) async fn receive_state<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    credentials: &Vec<PA::ContextData>,
    snapshot: Snapshot,
    initial: proto::StateReply,
    catalog: bool,
    query: Option<(proto::QueryId, u32)>,
    mut stream: IncomingStream,
) -> Result<RemoteState, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let stream_id = initial.frame.stream;
    let connection = node.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
    stream.bind(stream_id, catalog, connection.outgoing_state_streams.clone());
    drop(connection);
    if let Err(error) = initial.validate() {
        let _ = abandon_current_state(node, peer, stream_id, &stream);
        return Err(RetrievalError::Other(error.to_string()));
    }
    if let Err(error) = stream.inbox.insert(initial.frame) {
        let _ = abandon_current_state(node, peer, stream_id, &stream);
        return Err(error.into());
    }
    let mut driver = driver::Driver::new(snapshot, initial.round, stream, stream_id);
    let result = async {
        driver.prepare(node, query).await?;
        driver.drive(node.weak(), peer, credentials, query, true).await
    }
    .await;
    if let Err(error) = result {
        let _ = abandon_current_state(node, peer, stream_id, &driver.stream);
        return Err(stream_error(error));
    }
    Ok(RemoteState { stream_id, ids: driver.ids(), driver })
}

fn stream_error(error: anyhow::Error) -> RetrievalError {
    if let Some(error) = error.downcast_ref::<RetrievalError>() {
        return error.clone();
    }
    if let Some(error) = error.downcast_ref::<RequestError>() {
        return error.clone().into();
    }
    error.into()
}

async fn covers_target<SE, PA>(node: &Node<SE, PA>, actual: &proto::Clock, target: &proto::Clock) -> anyhow::Result<bool>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    if actual == target || target.entries().iter().all(|entry| actual.entries().contains(entry)) {
        return Ok(true);
    }
    let relation = match crate::event_dag::compare(LocalEventGetter::new(node.storage.clone(), node.durable), actual, target, 256).await {
        Ok(relation) => relation,
        // A later target can be registered before its data/history applies.
        // Missing local history proves neither coverage nor an invalid stream.
        Err(RetrievalError::EventNotFound(_)) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(matches!(
        relation.relation,
        crate::event_dag::AbstractCausalRelation::Equal | crate::event_dag::AbstractCausalRelation::StrictDescends { .. }
    ))
}

/// The same owner continues receiving while the round acknowledgment is in flight.
pub(crate) fn finish_state<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    credentials: Vec<PA::ContextData>,
    remote: RemoteState,
    query: Option<(proto::QueryId, u32)>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let weak = node.weak();
    crate::task::spawn(async move {
        let mut driver = remote.driver;
        let Some(node) = weak.upgrade() else { return };
        driver.acknowledge_round(&node, peer, &credentials);
        if let Some((id, version)) = query {
            if let Some(query) = node.live_queries.get(id) {
                query.set_refresh(version, &driver.stream.request, driver.refresh_handle());
            }
        }
        drop(node);
        let result =
            if query.is_some() { driver.drive(weak.clone(), peer, &credentials, query, false).await } else { driver.finish_fetch().await };
        if let Err(error) = result {
            tracing::warn!(%error, "state stream failed");
            let Some(node) = weak.upgrade() else { return };
            if !owns_state_stream(&node, peer, &driver.stream) {
                return;
            }
            if let Some((id, version)) = query {
                if let Some(relay) = &node.subscription_relay {
                    if driver.terminal {
                        relay.stream_terminal(peer, id, version, &driver.stream.request, stream_error(error));
                    } else {
                        relay.stream_failed(peer, id, version, &driver.stream.request);
                    }
                }
            }
            let _ = abandon_current_state(&node, peer, remote.stream_id, &driver.stream);
        }
    });
}

fn owns_state_stream<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, stream: &IncomingStream) -> bool
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.peer_connections.get(&peer).is_some_and(|connection| connection.incoming_states.is_current(&stream.request))
        && stream.query.is_none_or(|(query, version)| node.live_queries.get(query).is_some_and(|query| query.is_current(version)))
}

/// Superseded bound streams belong to replacement/unsubscribe cleanup;
/// abandoning them here can erase the tree before replacement is prepared.
pub(crate) fn abandon_current_state<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    stream_id: proto::StateStreamId,
    stream: &IncomingStream,
) -> Result<(), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let Some(connection) = node.peer_connections.get(&peer) else { return Ok(()) };
    if connection.incoming_states.is_current(&stream.request)
        && stream.query.is_none_or(|(query, version)| node.live_queries.get(query).is_some_and(|query| query.is_current(version)))
    {
        send_abandonment(&connection, node.id, peer, stream_id)?;
        stream.disarm_abandonment();
    }
    Ok(())
}

async fn finish_state_round<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    stream: proto::StateStreamId,
    round: u64,
    credentials: &Vec<PA::ContextData>,
    outcome: proto::StateRoundOutcome,
) -> Result<(), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // Live feedback still needs the catalog bypass after snapshot release.
    match state_deadline(node.request(peer, credentials, proto::NodeRequestBody::FinishStateRound { stream, round, outcome })).await?? {
        proto::NodeResponseBody::Success => Ok(()),
        proto::NodeResponseBody::StateStreamUnavailable => Err(RequestError::ConnectionLost.into()),
        proto::NodeResponseBody::Error(error) => Err(RequestError::ServerError(error).into()),
        other => Err(RequestError::UnexpectedResponse(other).into()),
    }
}

fn send_abandonment<CD: crate::session::ContextData>(
    connection: &crate::node::peer_state::PeerState<CD>,
    from: proto::EntityId,
    to: proto::EntityId,
    stream: proto::StateStreamId,
) -> Result<(), RequestError> {
    let result = connection.send_message(proto::NodeMessage::AbandonStateStream { from, to, stream });
    connection.outgoing_state_streams.remove(&stream);
    result.map_err(RequestError::from)
}

async fn state_deadline<T>(future: impl std::future::Future<Output = T>) -> Result<T, RetrievalError> {
    let future = future.fuse();
    let timeout = futures_timer::Delay::new(std::time::Duration::from_secs(120)).fuse();
    futures::pin_mut!(future, timeout);
    futures::select! {
        result = future => Ok(result),
        _ = timeout => Err(RequestError::ConnectionLost.into()),
    }
}

fn decode_delta(bytes: &[u8]) -> anyhow::Result<proto::DeltaContent> {
    Ok(bincode::DefaultOptions::new().with_fixint_encoding().with_limit(bytes.len() as u64).reject_trailing_bytes().deserialize(bytes)?)
}

fn check_target(target: &proto::Clock, content: &proto::DeltaContent) -> anyhow::Result<()> {
    match content {
        proto::DeltaContent::StateSnapshot { state } | proto::DeltaContent::StateAndEvents { state, .. } => {
            ensure!(state.state.head == *target, "snapshot does not match repair target")
        }
        proto::DeltaContent::EventBridge { events } => ensure!(!events.is_empty(), "empty event bridge"),
        proto::DeltaContent::StateAndRelation { .. } => bail!("StateAndRelation is not supported"),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
