use std::collections::{BTreeMap, HashMap};

use ankurah_digest_tree::{leaf_address, CanonicalCodec, KeyCodec};
use ankurah_proto::{self as proto, ComparisonProgress, StateAnswer, StateItem};
use anyhow::{bail, ensure};
use bincode::Options;

use super::Snapshot;
use crate::{
    error::{RequestError, RetrievalError},
    node::{applier::NodeApplier, Node},
    policy::PolicyAgent,
    retrieval::{CachedEventGetter, LocalStateGetter},
    storage::StorageEngine,
};

pub(crate) struct RemoteState {
    pub comparison: proto::ComparisonId,
    pub ids: Vec<proto::EntityId>,
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
    pub(crate) async fn receive_state(
        &self,
        peer: proto::EntityId,
        credentials: &Vec<PA::ContextData>,
        mut snapshot: Snapshot,
        initial: proto::StateReply,
        catalog: bool,
        query: Option<(proto::QueryId, u32)>,
    ) -> Result<RemoteState, RetrievalError> {
        let id = initial.page.comparison;
        let connection = self.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
        connection.outgoing_comparisons.insert(id, catalog);
        let result = async {
            ensure!(initial.spec == snapshot.spec, "peer changed the comparison spec");
            ensure!(initial.scope == [super::whole()], "unsupported materialized result scope");
            let mut members: BTreeMap<_, _> =
                snapshot.tree.leaves(&super::whole(), usize::MAX).await?.into_iter().map(|leaf| (leaf.address(), leaf.entity_id)).collect();
            let mut assemblies: HashMap<proto::EntityId, Assembly> = HashMap::new();
            let mut assembly_bytes = 0usize;
            let event_getter = CachedEventGetter::new(self, credentials);
            let state_getter = LocalStateGetter::new(self.storage.clone());
            let mut page = initial.page;
            for _ in 0..100_000 {
                if let Some((query, version)) = query {
                    ensure!(self.live_queries.get(query).is_some_and(|query| query.is_current(version)), "query comparison superseded");
                }
                ensure!(page.comparison == id && page.items.len() <= 128, "invalid comparison reply");
                let mut answers = Vec::new();
                let mut deltas = Vec::new();
                for item in page.items {
                    match item {
                        StateItem::Equal(_) => {}
                        StateItem::Summary(head) => {
                            let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                            answers.push(StateAnswer::Summary(snapshot.head(head.bounds).await?));
                        }
                        StateItem::NeedLeaves(bounds) => answers.push(StateAnswer::Leaves(snapshot.page(bounds).await?)),
                        StateItem::Leaves(page) => {
                            ensure!(page.leaves.len() <= super::LEAF_PAGE, "peer sent too many leaves");
                            let mut previous = None;
                            for leaf in &page.leaves {
                                CanonicalCodec.split(snapshot.spec.key_spec(), &leaf.key)?;
                                let address = leaf_address(&leaf.key, leaf.entity.entity_id);
                                ensure!(
                                    page.bounds.contains(&address) && previous.as_ref().is_none_or(|p| p < &address),
                                    "invalid peer leaf page"
                                );
                                previous = Some(address);
                            }
                            members.retain(|address, _| !page.bounds.contains(address));
                            for leaf in page.leaves {
                                members.insert(leaf_address(&leaf.key, leaf.entity.entity_id), leaf.entity.entity_id);
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
                            deltas.push(delta);
                        }
                        StateItem::RepairChunk(chunk) => {
                            let assembly = assemblies
                                .entry(chunk.entity_id)
                                .or_insert_with(|| Assembly { target: chunk.target.clone(), bytes: Vec::new() });
                            ensure!(
                                assembly.target == chunk.target && chunk.offset == assembly.bytes.len() as u64,
                                "invalid repair chunk sequence"
                            );
                            assembly_bytes =
                                assembly_bytes.checked_add(chunk.bytes.len()).ok_or_else(|| anyhow::anyhow!("repair size overflow"))?;
                            ensure!(assembly_bytes <= super::MAX_REPAIR_BYTES, "repair assembly exceeds limit");
                            assembly.bytes.extend_from_slice(&chunk.bytes);
                            if chunk.last {
                                let assembly = assemblies.remove(&chunk.entity_id).unwrap();
                                let content: proto::DeltaContent = bincode::DefaultOptions::new()
                                    .with_fixint_encoding()
                                    .with_limit(assembly.bytes.len() as u64)
                                    .reject_trailing_bytes()
                                    .deserialize(&assembly.bytes)?;
                                check_target(&assembly.target, &content)?;
                                assembly_bytes -= assembly.bytes.len();
                                deltas.push(proto::EntityDelta { entity_id: chunk.entity_id, content });
                            }
                        }
                    }
                }
                NodeApplier::apply_deltas(self, &peer, deltas, &event_getter, &state_getter).await?;
                if page.progress == ComparisonProgress::Complete {
                    ensure!(answers.is_empty() && assemblies.is_empty(), "peer completed an unfinished comparison");
                    return Ok(RemoteState { comparison: id, ids: members.into_values().collect() });
                }
                page = match self.request(peer, credentials, proto::NodeRequestBody::CompareTree { comparison: id, answers }).await? {
                    proto::NodeResponseBody::TreeCompared(page) => page,
                    proto::NodeResponseBody::ComparisonUnavailable { .. } => return Err(RequestError::ConnectionLost.into()),
                    proto::NodeResponseBody::Error(error) => return Err(RequestError::ServerError(error).into()),
                    other => return Err(RequestError::UnexpectedResponse(other).into()),
                };
            }
            bail!("comparison exceeded its round limit")
        }
        .await;
        if result.is_err() {
            let _ = self.close_comparison(peer, id, credentials, proto::ComparisonOutcome::Abandoned).await;
        }
        result.map_err(|error: anyhow::Error| {
            if let Some(error) = error.downcast_ref::<RetrievalError>() {
                return error.clone();
            }
            if let Some(error) = error.downcast_ref::<RequestError>() {
                return error.clone().into();
            }
            error.into()
        })
    }

    pub(crate) async fn close_comparison(
        &self,
        peer: proto::EntityId,
        comparison: proto::ComparisonId,
        credentials: &Vec<PA::ContextData>,
        outcome: proto::ComparisonOutcome,
    ) -> Result<(), RetrievalError> {
        let result = self.request(peer, credentials, proto::NodeRequestBody::CloseComparison { comparison, outcome }).await;
        if let Some(connection) = self.peer_connections.get(&peer) {
            connection.outgoing_comparisons.remove(&comparison);
        }
        match result? {
            proto::NodeResponseBody::Success => Ok(()),
            proto::NodeResponseBody::ComparisonUnavailable { .. } => Err(RequestError::ConnectionLost.into()),
            proto::NodeResponseBody::Error(error) => Err(RequestError::ServerError(error).into()),
            other => Err(RequestError::UnexpectedResponse(other).into()),
        }
    }
}

fn check_target(target: &proto::Clock, content: &proto::DeltaContent) -> anyhow::Result<()> {
    match content {
        proto::DeltaContent::StateSnapshot { state } | proto::DeltaContent::StateAndRelation { state, .. } => {
            ensure!(state.state.head == *target, "snapshot does not match repair target");
        }
        proto::DeltaContent::EventBridge { events } => ensure!(!events.is_empty(), "empty event bridge"),
    }
    Ok(())
}
