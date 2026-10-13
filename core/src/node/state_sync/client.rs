use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use ankurah_digest_tree::{leaf_address, CanonicalCodec, KeyCodec};
use ankurah_proto::{self as proto, ComparisonProgress, StateAnswer, StateFrameBody, StateItem};
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

pub(crate) struct RemoteState {
    pub comparison: proto::ComparisonId,
    pub ids: Vec<proto::EntityId>,
    pub processed_through: u64,
    pub stream: IncomingStream,
    feedback: Arc<Feedback>,
}

struct Assembly {
    target: proto::Clock,
    bytes: Vec<u8>,
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Install the origin before sending: a pushed continuation may beat metadata.
    pub(crate) async fn request_state(
        &self,
        peer: proto::EntityId,
        credentials: &Vec<PA::ContextData>,
        body: proto::NodeRequestBody,
    ) -> Result<(IncomingStream, proto::NodeResponseBody), RetrievalError> {
        let query = match &body {
            proto::NodeRequestBody::SubscribeQuery { query_id, version, .. } => Some((*query_id, *version)),
            _ => None,
        };
        let connection = self.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
        let catalog = match &body {
            proto::NodeRequestBody::SubscribeQuery { selection, .. } | proto::NodeRequestBody::Fetch { selection, .. } => {
                crate::schema::is_catalog_read(&selection.predicate)
            }
            _ => false,
        };
        let weak = self.weak();
        let cleanup_credentials = credentials.clone();
        let late_reply = Arc::new(move |comparison| {
            let weak = weak.clone();
            let credentials = cleanup_credentials.clone();
            crate::task::spawn(async move {
                if let Some(node) = weak.upgrade() {
                    if let Some(connection) = node.peer_connections.get(&peer) {
                        connection.outgoing_comparisons.insert(comparison, catalog);
                    }
                    let _ = node.close_comparison(peer, comparison, &credentials, proto::ComparisonOutcome::Abandoned).await;
                }
            });
        });
        let stream = connection.incoming_states.register(proto::RequestId::new(), query, Some(late_reply))?;
        drop(connection);
        let response = state_deadline(self.request_with_id(peer, credentials, stream.request.clone(), body)).await??;
        Ok((stream, response))
    }

    pub(crate) async fn receive_state(
        &self,
        peer: proto::EntityId,
        credentials: &Vec<PA::ContextData>,
        mut snapshot: Snapshot,
        initial: proto::StateReply,
        catalog: bool,
        query: Option<(proto::QueryId, u32)>,
        mut stream: IncomingStream,
    ) -> Result<RemoteState, RetrievalError> {
        let id = initial.frame.comparison;
        let connection = self.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
        stream.bind(id, catalog, connection.outgoing_comparisons.clone());
        drop(connection);
        let result = async {
            ensure!(initial.spec == snapshot.spec, "peer changed the comparison spec");
            ensure!(initial.scope == [super::whole()], "unsupported materialized result scope");
            ensure!(initial.frame.sequence == 0, "initial response did not carry frame zero");
            stream.inbox.insert(initial.frame)?;
            let feedback = Arc::new(Feedback::default());
            let mut members = snapshot.members();
            let mut assemblies: HashMap<proto::EntityId, Assembly> = HashMap::new();
            let mut required: HashMap<proto::EntityId, proto::Clock> = HashMap::new();
            let mut repair_attempts: HashMap<proto::EntityId, u8> = HashMap::new();
            let mut assembly_bytes = 0usize;
            let event_getter = CachedEventGetter::new(self, credentials);
            let state_getter = LocalStateGetter::new(self.storage.clone());
            let mut complete = false;
            loop {
                if let Some((query, version)) = query {
                    ensure!(self.live_queries.get(query).is_some_and(|query| query.is_current(version)), "query comparison superseded");
                }
                let frame = stream.inbox.next(false).await?;
                let (items, progress) = match frame.body {
                    StateFrameBody::State { items, progress } => (items, progress),
                    StateFrameBody::RestartRequired => return Err(RequestError::ConnectionLost.into()),
                    _ => bail!("initial stream sent live data before activation"),
                };
                ensure!(frame.comparison == id && items.len() <= 128, "invalid comparison reply");
                let mut answers = Vec::new();
                let mut deltas = Vec::new();
                let mut targets = Vec::new();
                for item in items {
                    match item {
                        StateItem::Equal(_) => {}
                        StateItem::Summary(head) => {
                            let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                            answers.push(StateAnswer::Summary(snapshot.head(head.bounds).await?));
                        }
                        StateItem::NeedLeaves(bounds) => answers.push(StateAnswer::Leaves(snapshot.page(bounds).await?)),
                        StateItem::Leaves(page) => replace_members(&mut members, &snapshot.spec, page, super::LEAF_PAGE)?,
                        StateItem::Snapshots(page) => {
                            // Membership comes from the captured sender state,
                            // before the applier can merge a newer local state.
                            let leaves = snapshot_leaves(&snapshot.spec, &page)?;
                            replace_members(&mut members, &snapshot.spec, leaves, super::MAX_SNAPSHOT_ROWS)?;
                            for state in page.states {
                                let entity_id = state.payload.entity_id;
                                ensure!(!assemblies.contains_key(&entity_id), "snapshot interrupted repair chunks");
                                targets.push((entity_id, state.payload.state.head.clone()));
                                deltas.push(proto::EntityDelta {
                                    entity_id,
                                    content: proto::DeltaContent::StateSnapshot { state: state.into() },
                                });
                            }
                        }
                        StateItem::NeedHeads(ids) => {
                            ensure!(ids.len() <= 128, "too many requested entity clocks");
                            let mut known = Vec::new();
                            let mut unknown = Vec::new();
                            for entity_id in ids {
                                let head = if let Some(state) = snapshot.states.get(&entity_id) {
                                    Some(state.payload.state.head.clone())
                                } else {
                                    match self.storage.get_state(entity_id).await {
                                        Ok(state)
                                            if !catalog
                                                || state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy) =>
                                        {
                                            Some(state.payload.state.head)
                                        }
                                        Ok(_) => None,
                                        Err(RetrievalError::EntityNotFound(_)) => None,
                                        Err(error) => return Err(error.into()),
                                    }
                                };
                                match head {
                                    Some(head) => known.push(proto::KnownEntity { entity_id, head }),
                                    None => unknown.push(entity_id),
                                }
                            }
                            answers.push(StateAnswer::Heads { known, unknown });
                        }
                        StateItem::Repair { target, delta } => {
                            ensure!(!assemblies.contains_key(&delta.entity_id), "repair interrupted its chunks");
                            check_target(&target, &delta.content)?;
                            targets.push((delta.entity_id, target));
                            deltas.push(delta);
                        }
                        StateItem::RepairChunk { target, chunk } => {
                            ensure!(!chunk.bytes.is_empty(), "empty repair chunk");
                            ensure!(
                                assemblies.contains_key(&chunk.entity_id) || assemblies.len() < super::MAX_INFLIGHT_FRAMES,
                                "too many partial repairs"
                            );
                            let assembly =
                                assemblies.entry(chunk.entity_id).or_insert_with(|| Assembly { target: target.clone(), bytes: Vec::new() });
                            ensure!(
                                assembly.target == target && chunk.offset == assembly.bytes.len() as u64,
                                "invalid repair chunk sequence"
                            );
                            assembly_bytes =
                                assembly_bytes.checked_add(chunk.bytes.len()).ok_or_else(|| anyhow::anyhow!("repair size overflow"))?;
                            ensure!(assembly_bytes <= super::MAX_REPAIR_BYTES, "repair assembly exceeds limit");
                            assembly.bytes.extend_from_slice(&chunk.bytes);
                            if chunk.last {
                                let assembly = assemblies.remove(&chunk.entity_id).unwrap();
                                let content = decode_delta(&assembly.bytes)?;
                                check_target(&assembly.target, &content)?;
                                assembly_bytes -= assembly.bytes.len();
                                targets.push((chunk.entity_id, assembly.target));
                                deltas.push(proto::EntityDelta { entity_id: chunk.entity_id, content });
                            }
                        }
                        StateItem::RepairUnavailable { entity_id, .. } => bail!("peer cannot repair required state for {entity_id}"),
                    }
                }
                for (entity_id, target) in &targets {
                    ensure!(required.get(entity_id).is_none_or(|pending| pending == target), "repair changed its pending target");
                }
                let divergences = NodeApplier::apply_deltas(self, &peer, deltas, &event_getter, &state_getter).await?;
                for (entity_id, target) in targets {
                    ensure!(required.get(&entity_id).is_none_or(|pending| *pending == target), "repair changed its pending target");
                    let actual = match self.entities.get(&entity_id) {
                        Some(entity) => entity.head(),
                        None => self.storage.get_state(entity_id).await?.payload.state.head,
                    };
                    let divergent = divergences.iter().any(|gap| gap.entity_id == entity_id);
                    let covered = !divergent && self.covers_target(&actual, &target).await?;
                    if covered {
                        required.remove(&entity_id);
                        repair_attempts.remove(&entity_id);
                    } else {
                        ensure!(
                            required.contains_key(&entity_id) || required.len() < super::MAX_INFLIGHT_FRAMES * 128,
                            "too many pending repair targets"
                        );
                        let attempts = repair_attempts.entry(entity_id).or_default();
                        ensure!(*attempts < 2, "connecting history failed to cover target");
                        *attempts += 1;
                        required.insert(entity_id, target.clone());
                        answers.push(StateAnswer::NeedRepair { entity_id, target, actual });
                    }
                }
                complete |= progress == ComparisonProgress::Complete;
                if complete && answers.is_empty() && required.is_empty() && assemblies.is_empty() {
                    return Ok((members.into_values().collect(), frame.sequence, feedback));
                }
                feedback.queue(self, peer, credentials, id, frame.sequence, answers, &stream.inbox)?;
            }
        }
        .await;
        if result.is_err() {
            let _ = self.abandon_current_state(peer, id, credentials, &stream).await;
        }
        let (ids, processed_through, feedback) = result.map_err(|error: anyhow::Error| {
            if let Some(error) = error.downcast_ref::<RetrievalError>() {
                return error.clone();
            }
            if let Some(error) = error.downcast_ref::<RequestError>() {
                return error.clone().into();
            }
            error.into()
        })?;
        Ok(RemoteState { comparison: id, ids, processed_through, stream, feedback })
    }

    async fn covers_target(&self, actual: &proto::Clock, target: &proto::Clock) -> anyhow::Result<bool> {
        if actual == target || target.entries().iter().all(|entry| actual.entries().contains(entry)) {
            return Ok(true);
        }
        let relation = crate::event_dag::compare(LocalEventGetter::new(self.storage.clone(), self.durable), actual, target, 256).await?;
        Ok(matches!(
            relation.relation,
            crate::event_dag::AbstractCausalRelation::Equal | crate::event_dag::AbstractCausalRelation::StrictDescends { .. }
        ))
    }

    /// Readiness follows local activation, not the acknowledgment's round trip.
    pub(crate) fn finish_state(
        &self,
        peer: proto::EntityId,
        credentials: Vec<PA::ContextData>,
        remote: RemoteState,
        query: Option<(proto::QueryId, u32)>,
    ) {
        let weak = self.weak();
        crate::task::spawn(async move {
            let comparison = remote.comparison;
            let result: anyhow::Result<()> = async {
                let applied = async {
                    let Some(node) = weak.upgrade() else { return Ok::<_, anyhow::Error>(()) };
                    node.close_comparison(
                        peer,
                        comparison,
                        &credentials,
                        proto::ComparisonOutcome::Applied { processed_through: remote.processed_through },
                    )
                    .await?;
                    Ok(())
                }
                .fuse();
                if query.is_none() {
                    return applied.await;
                }
                let live = async {
                    let Some((query_id, version)) = query else { return Ok(()) };
                    let mut assembly: Option<(proto::EntityId, Vec<(proto::QueryId, proto::MembershipChange)>, Vec<u8>)> = None;
                    loop {
                        let frame = remote.stream.inbox.next(assembly.is_none()).await?;
                        let Some(node) = weak.upgrade() else { return Ok(()) };
                        ensure!(
                            node.live_queries.get(query_id).is_some_and(|query| query.is_current(version)),
                            "live state stream superseded"
                        );
                        let items = match frame.body {
                            StateFrameBody::Live(items) => {
                                ensure!(assembly.is_none(), "live update interrupted chunks");
                                ensure!(items.len() <= 128, "live frame item limit exceeded");
                                items
                            }
                            StateFrameBody::LiveChunk { chunk, predicate_relevance } => {
                                ensure!(!chunk.bytes.is_empty(), "empty live chunk");
                                ensure!(
                                    predicate_relevance.len() <= 128 && predicate_relevance.iter().all(|(id, _)| *id == query_id),
                                    "invalid live chunk relevance"
                                );
                                let (id, relevance, bytes) =
                                    assembly.get_or_insert_with(|| (chunk.entity_id, predicate_relevance.clone(), Vec::new()));
                                ensure!(
                                    *id == chunk.entity_id && *relevance == predicate_relevance && chunk.offset == bytes.len() as u64,
                                    "invalid live chunk sequence"
                                );
                                ensure!(bytes.len() + chunk.bytes.len() <= super::MAX_REPAIR_BYTES, "live assembly exceeds limit");
                                bytes.extend_from_slice(&chunk.bytes);
                                if chunk.last {
                                    let (entity_id, predicate_relevance, bytes) = assembly.take().unwrap();
                                    vec![proto::SubscriptionUpdateItem {
                                        delta: proto::EntityDelta { entity_id, content: decode_delta(&bytes)? },
                                        predicate_relevance,
                                    }]
                                } else {
                                    Vec::new()
                                }
                            }
                            _ => bail!("live state stream requires restart"),
                        };
                        ensure!(
                            items.iter().all(|item| item.predicate_relevance.len() <= 128
                                && item.predicate_relevance.iter().all(|(id, _)| *id == query_id)),
                            "live frame names another query"
                        );
                        if !items.is_empty() {
                            NodeApplier::apply_stream_updates(&node, &peer, &credentials, items).await?;
                        }
                        remote.feedback.queue(&node, peer, &credentials, comparison, frame.sequence, Vec::new(), &remote.stream.inbox)?;
                    }
                }
                .fuse();
                futures::pin_mut!(applied, live);
                futures::select! {
                    result = applied => { result?; live.await },
                    result = live => result,
                }
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "state stream failed");
                if let Some(node) = weak.upgrade() {
                    if !node.owns_state_stream(peer, &remote.stream) {
                        return;
                    }
                    if let Some((query, version)) = query {
                        if let Some(relay) = &node.subscription_relay {
                            relay.stream_failed(peer, query, version, &remote.stream.request);
                        }
                    }
                    let _ = node.abandon_current_state(peer, comparison, &credentials, &remote.stream).await;
                }
            }
        });
    }

    fn owns_state_stream(&self, peer: proto::EntityId, stream: &IncomingStream) -> bool {
        self.peer_connections.get(&peer).is_some_and(|connection| connection.incoming_states.is_current(&stream.request))
            && stream.query.is_none_or(|(query, version)| self.live_queries.get(query).is_some_and(|query| query.is_current(version)))
    }

    /// Superseded bound streams belong to replacement/unsubscribe cleanup;
    /// abandoning them here can erase the tree before replacement is prepared.
    pub(crate) async fn abandon_current_state(
        &self,
        peer: proto::EntityId,
        comparison: proto::ComparisonId,
        credentials: &Vec<PA::ContextData>,
        stream: &IncomingStream,
    ) -> Result<(), RetrievalError> {
        if self.owns_state_stream(peer, stream) {
            self.close_comparison(peer, comparison, credentials, proto::ComparisonOutcome::Abandoned).await?;
        }
        Ok(())
    }

    pub(crate) async fn close_comparison(
        &self,
        peer: proto::EntityId,
        comparison: proto::ComparisonId,
        credentials: &Vec<PA::ContextData>,
        outcome: proto::ComparisonOutcome,
    ) -> Result<(), RetrievalError> {
        let result = state_deadline(self.request(peer, credentials, proto::NodeRequestBody::CloseComparison { comparison, outcome })).await;
        // Live feedback still needs the catalog bypass after snapshot release.
        if matches!(outcome, proto::ComparisonOutcome::Abandoned) {
            if let Some(connection) = self.peer_connections.get(&peer) {
                connection.outgoing_comparisons.remove(&comparison);
            }
        }
        match result?? {
            proto::NodeResponseBody::Success => Ok(()),
            proto::NodeResponseBody::ComparisonUnavailable { .. } => Err(RequestError::ConnectionLost.into()),
            proto::NodeResponseBody::Error(error) => Err(RequestError::ServerError(error).into()),
            other => Err(RequestError::UnexpectedResponse(other).into()),
        }
    }
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

fn snapshot_leaves(spec: &proto::TreeSpec, page: &proto::SnapshotPage) -> anyhow::Result<proto::LeafPage> {
    ensure!(page.states.len() <= super::MAX_SNAPSHOT_ROWS, "peer sent too many snapshots");
    let mut ids = std::collections::HashSet::new();
    let leaves = page
        .states
        .iter()
        .map(|state| {
            let entity_id = state.payload.entity_id;
            ensure!(ids.insert(entity_id), "duplicate entity in snapshot page");
            Ok(proto::KnownLeaf {
                key: super::tree::key(spec, state)?,
                entity: proto::KnownEntity { entity_id, head: state.payload.state.head.clone() },
            })
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(proto::LeafPage { bounds: page.bounds.clone(), leaves })
}

fn replace_members(
    members: &mut BTreeMap<Vec<u8>, proto::EntityId>,
    spec: &proto::TreeSpec,
    page: proto::LeafPage,
    max_rows: usize,
) -> anyhow::Result<()> {
    use std::ops::Bound::*;
    ensure!(page.leaves.len() <= max_rows, "peer sent too many leaves");
    let nonempty = match (&page.bounds.start, &page.bounds.end) {
        (Unbounded, _) | (_, Unbounded) => true,
        (Included(a), Included(b)) => a <= b,
        (Included(a) | Excluded(a), Included(b) | Excluded(b)) => a < b,
    };
    ensure!(nonempty, "invalid peer leaf bounds");
    let mut previous = None;
    for leaf in &page.leaves {
        CanonicalCodec.split(spec.key_spec(), &leaf.key)?;
        let address = leaf_address(&leaf.key, leaf.entity.entity_id);
        ensure!(page.bounds.contains(&address) && previous.as_ref().is_none_or(|p| p < &address), "invalid peer leaf page");
        previous = Some(address);
    }
    let removed: Vec<_> = members.range((page.bounds.start.clone(), page.bounds.end.clone())).map(|(key, _)| key.clone()).collect();
    for key in removed {
        members.remove(&key);
    }
    for leaf in page.leaves {
        members.insert(leaf_address(&leaf.key, leaf.entity.entity_id), leaf.entity.entity_id);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
