//! Authorized entity repair and bounded, entity-scoped causal proofs.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Bound,
    sync::Mutex,
};

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::{leaf_address, Span};
use ankurah_proto::{self as proto, Attested, Clock, ComparisonFailure, EntityId, Event, EventId, State};
use async_trait::async_trait;
use futures::TryStreamExt;

use super::serve::CheckedCapture;
use crate::{
    error::RetrievalError,
    event_dag::{compare, AbstractCausalRelation},
    node::Node,
    policy::{ComparisonLimits, ContextPolicy, PolicyAgent},
    retrieval::GetEvents,
    storage::{StateScan, StorageEngine},
    sync::EntityMemberId,
};

pub(in crate::node) struct PreparedRepair<SE, CD: crate::session::ContextData> {
    pub deltas: Vec<proto::EntityDelta>,
    pub unavailable: Vec<EntityId>,
    pub bytes: usize,
    checked: CheckedCapture<SE, CD>,
    states: Vec<Attested<proto::EntityState>>,
    members: usize,
    inspected_events: usize,
}

impl<SE: StorageEngine + 'static, CD: crate::session::ContextData> PreparedRepair<SE, CD> {
    pub(in crate::node) fn connection(&self) -> Result<std::sync::Arc<crate::node::PeerState<CD>>, RetrievalError> {
        self.checked.connection()
    }

    /// Call after all other asynchronous preparation, immediately before pushing
    /// the update. The retained capture keeps its version alive through delivery.
    pub(in crate::node) async fn revalidate<PA: PolicyAgent<ContextData = CD>>(
        &self,
        node: &Node<SE, PA>,
        credentials: &Vec<CD>,
    ) -> Result<(), RetrievalError> {
        let revalidate = async {
            node.revalidate_comparison(&self.checked, credentials).await?;
            // Policies may change during history reads. Check both the state being
            // disclosed and a fresh canonical state, including known-ID reads outside
            // the capture. No partial bridge is disclosed when one event is denied.
            let ids: Vec<_> = self.states.iter().map(|state| state.payload.entity_id).collect();
            let mut snapshot = node.storage.state_snapshot(StateScan::Changes(None)).await?;
            let current = snapshot.get_states(&ids).await?;
            if current.len() != self.states.len() {
                return Err(crate::sync::Error::AnswerMisfit.into());
            }
            snapshot.finish().await?;
            let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
            let deltas: HashMap<_, _> = self.deltas.iter().map(|delta| (delta.entity_id, delta)).collect();
            let mut state_bytes = 0usize;
            for (sent, current) in self.states.iter().zip(current) {
                let current = current.ok_or_else(|| failure(ComparisonFailure::AuthorizationChanged))?;
                state_bytes = state_bytes.checked_add(size(&current)?).filter(|bytes| *bytes <= self.checked.limits().max_capture_bytes)
                    .ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
                if current.payload.entity_id != sent.payload.entity_id {
                    return Err(crate::sync::Error::AnswerMisfit.into());
                }
                policy
                    .check_read(&sent.payload.entity_id, &sent.payload.state)
                    .and_then(|()| policy.check_read(&current.payload.entity_id, &current.payload.state))
                    .map_err(|_| failure(ComparisonFailure::AuthorizationChanged))?;
                if let Some(proto::EntityDelta { content: proto::DeltaContent::EventBridge { events }, .. }) =
                    deltas.get(&sent.payload.entity_id)
                {
                    for event in events {
                        let event: Attested<Event> = (sent.payload.entity_id, event.clone()).into();
                        policy
                            .check_read_event(&event, &current.payload.state)
                            .map_err(|_| failure(ComparisonFailure::AuthorizationChanged))?;
                    }
                }
            }
            // Revalidation can tighten the policy limits while the capture is live.
            node.check_comparison_disclosure(&self.checked, credentials)?;
            if self.inspected_events > self.checked.limits().max_events || self.members > self.checked.limits().max_members {
                return Err(failure(ComparisonFailure::LimitExceeded));
            }
            self.checked.charge(0, self.bytes)
        };
        tokio::select! {
            result = revalidate => result,
            _ = self.checked.cancelled() => Err(failure(ComparisonFailure::Expired)),
            _ = futures_timer::Delay::new(self.checked.remaining()) => Err(failure(ComparisonFailure::Expired)),
        }
    }
}

impl<SE: StorageEngine + 'static, PA: PolicyAgent> Node<SE, PA> {
    /// Resolve only the identities requested. A key proves membership in the
    /// captured tree; an omitted key is a known-ID read, including predicate exits.
    pub(in crate::node) async fn prepare_comparison_repair(
        &self,
        peer: EntityId,
        id: proto::ComparisonId,
        selection: &Selection<Resolved>,
        members: Vec<proto::RepairMember>,
        events_only: bool,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<PreparedRepair<SE, PA::ContextData>, RetrievalError> {
        let checked = self.checked_comparison(peer, id, selection, credentials, members.len()).await?;
        let remaining = checked.remaining();
        let prepare = async {
            let limits = checked.limits();
            if members.is_empty() || members.len() > limits.max_members {
                return Err(failure(ComparisonFailure::LimitExceeded));
            }
            checked.charge(0, size(&members)?)?;
            let mut unique = HashSet::new();
            let mut targets = Vec::with_capacity(members.len());
            for member in &members {
                if !unique.insert(member.known.entity_id) {
                    return Err(invalid("an entity is requested more than once"));
                }
                let captured = if let Some(key) = &member.key {
                    let address = leaf_address(key, &EntityMemberId(member.known.entity_id));
                    let span = Span::new(Bound::Included(address.clone()), Bound::Included(address));
                    let mut found = checked.view().range(span).map_err(super::serve::tree_failure)?;
                    let leaf = found
                        .try_next()
                        .await
                        .map_err(super::serve::tree_failure)?
                        .ok_or_else(|| invalid("repair key does not name a captured member"))?;
                    if leaf.id.0 != member.known.entity_id || leaf.key != *key {
                        return Err(invalid("repair key names a different captured member"));
                    }
                    Some(leaf.payload.0)
                } else {
                    None
                };
                targets.push(captured);
            }

            let ids: Vec<_> = members.iter().map(|member| member.known.entity_id).collect();
            let mut snapshot = self.storage.state_snapshot(StateScan::Changes(None)).await?;
            let states = snapshot.get_states(&ids).await?;
            if states.len() != ids.len() {
                return Err(crate::sync::Error::AnswerMisfit.into());
            }
            snapshot.finish().await?;

            let budget = HistoryBudget::new(limits, Some(&checked), None);
            let policy = ContextPolicy::from_credentials(&self.policy_agent, credentials);
            let mut deltas = Vec::new();
            let mut unavailable = Vec::new();
            let mut disclosed = Vec::new();
            for ((member, captured), state) in members.iter().zip(targets).zip(states) {
                let entity_id = member.known.entity_id;
                let Some(state) = state else {
                    unavailable.push(entity_id);
                    continue;
                };
                if state.payload.entity_id != entity_id {
                    return Err(crate::sync::Error::AnswerMisfit.into());
                }
                if policy.check_read(&entity_id, &state.payload.state).is_err() {
                    unavailable.push(entity_id);
                    continue;
                }
                budget.inspect_state_bytes(size(&state)?)?;
                let current = &state.payload.state.head;
                // An equal current head does not discharge an explicit request for
                // events: the requester may have adopted that state without its history.
                if !events_only && member.known.head == *current {
                    disclosed.push(state);
                    continue;
                }
                let content = if !events_only && member.known.head.is_empty() {
                    proto::DeltaContent::StateSnapshot {
                        state: proto::StateFragment { state: state.payload.state.clone(), attestations: state.attestations.clone() },
                    }
                } else {
                    let history = RepairHistory::new(self, entity_id, &state.payload.state, credentials, &budget);
                    let events = history.bridge(&member.known.head, current, captured.as_ref(), events_only).await;
                    match events {
                        Ok(events) => proto::DeltaContent::EventBridge { events: events.into_iter().map(Into::into).collect() },
                        Err(RetrievalError::EventNotFound(_) | RetrievalError::AccessDenied(_)) => {
                            unavailable.push(entity_id);
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                };
                disclosed.push(state);
                deltas.push(proto::EntityDelta { entity_id, content });
            }

            let bytes = size(&(&deltas, &unavailable))?;
            checked.check_active()?;
            if bytes > limits.max_bytes {
                return Err(failure(ComparisonFailure::LimitExceeded));
            }
            let inspected_events = budget.used.lock().unwrap().events;
            drop(budget);
            Ok((deltas, unavailable, bytes, disclosed, inspected_events))
        };
        let (deltas, unavailable, bytes, states, inspected_events) = tokio::select! {
            result = prepare => result,
            _ = checked.cancelled() => Err(failure(ComparisonFailure::Expired)),
            _ = futures_timer::Delay::new(remaining) => Err(failure(ComparisonFailure::Expired)),
        }?;
        Ok(PreparedRepair { deltas, unavailable, bytes, checked, states, members: members.len(), inspected_events })
    }

    /// Prove coverage from local canonical state and locally available history.
    /// Missing history is inconclusive; a remote causal assertion is not proof.
    pub(in crate::node) async fn comparison_covers(
        &self,
        entity_id: EntityId,
        local_head: &Clock,
        captured_head: &Clock,
        credentials: &Vec<PA::ContextData>,
        charge: &(dyn Fn(usize) -> Result<(), RetrievalError> + Send + Sync),
    ) -> Result<bool, RetrievalError> {
        charge(1)?;
        let state = match self.storage.get_state(entity_id).await {
            Ok(state) => state,
            Err(RetrievalError::EntityNotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        charge(size(&state)?.div_ceil(1024))?;
        if state.payload.entity_id != entity_id {
            return Err(crate::sync::Error::AnswerMisfit.into());
        }
        // This proof discloses no state or event. A locally denied entity can
        // still cover a remote member; outgoing known heads have a separate gate.
        if state.payload.state.head != *local_head {
            return Ok(false);
        }
        if local_head == captured_head || captured_head.is_empty() {
            return Ok(true);
        }
        if local_head.is_empty() {
            return Ok(false);
        }
        let budget = HistoryBudget::new(self.policy_agent.comparison_limits(credentials), None, Some(charge));
        let history = RepairHistory::new(self, entity_id, &state.payload.state, credentials, &budget);
        match compare(&history, local_head, captured_head, budget.comparison_budget()).await {
            Ok(result) => match result.relation {
                AbstractCausalRelation::Equal | AbstractCausalRelation::StrictDescends { .. } => Ok(true),
                AbstractCausalRelation::BudgetExceeded { .. } => Err(failure(ComparisonFailure::LimitExceeded)),
                _ => Ok(false),
            },
            Err(RetrievalError::EventNotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

struct HistoryBudget<'a, SE, CD: crate::session::ContextData> {
    limits: ComparisonLimits,
    capture: Option<&'a CheckedCapture<SE, CD>>,
    proof_charge: Option<&'a (dyn Fn(usize) -> Result<(), RetrievalError> + Send + Sync)>,
    used: Mutex<HistoryUsage>,
}

#[derive(Default)]
struct HistoryUsage {
    events: usize,
    history_bytes: usize,
    state_bytes: usize,
}

impl<'a, SE, CD: crate::session::ContextData> HistoryBudget<'a, SE, CD> {
    fn new(
        limits: ComparisonLimits,
        capture: Option<&'a CheckedCapture<SE, CD>>,
        proof_charge: Option<&'a (dyn Fn(usize) -> Result<(), RetrievalError> + Send + Sync)>,
    ) -> Self {
        Self { limits, capture, proof_charge, used: Mutex::new(HistoryUsage::default()) }
    }

    fn read_event(&self) -> Result<(), RetrievalError> {
        let mut used = self.used.lock().unwrap();
        used.events = used
            .events
            .checked_add(1)
            .filter(|count| *count <= self.limits.max_events && *count <= self.limits.max_steps)
            .ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
        if let Some(capture) = self.capture {
            capture.charge(1, 0)?;
        }
        if let Some(charge) = self.proof_charge {
            charge(1)?;
        }
        Ok(())
    }

    fn inspect_event_bytes(&self, bytes: usize) -> Result<(), RetrievalError> {
        let mut used = self.used.lock().unwrap();
        used.history_bytes = used
            .history_bytes
            .checked_add(bytes)
            .filter(|total| *total <= self.limits.max_bytes)
            .ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
        if let Some(charge) = self.proof_charge {
            charge(bytes.div_ceil(1024))?;
        }
        Ok(())
    }

    fn inspect_state_bytes(&self, bytes: usize) -> Result<(), RetrievalError> {
        let mut used = self.used.lock().unwrap();
        used.state_bytes = used.state_bytes.checked_add(bytes).filter(|bytes| *bytes <= self.limits.max_capture_bytes)
            .ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
        Ok(())
    }

    fn comparison_budget(&self) -> usize { self.limits.max_events.min(self.limits.max_steps).min(usize::MAX / 4) }
}

struct RepairHistory<'a, SE: StorageEngine, PA: PolicyAgent> {
    node: &'a Node<SE, PA>,
    entity_id: EntityId,
    state: &'a State,
    credentials: &'a Vec<PA::ContextData>,
    budget: &'a HistoryBudget<'a, SE, PA::ContextData>,
    events: Mutex<HashMap<EventId, Attested<Event>>>,
}

impl<'a, SE: StorageEngine + 'static, PA: PolicyAgent> RepairHistory<'a, SE, PA> {
    fn new(
        node: &'a Node<SE, PA>,
        entity_id: EntityId,
        state: &'a State,
        credentials: &'a Vec<PA::ContextData>,
        budget: &'a HistoryBudget<'a, SE, PA::ContextData>,
    ) -> Self {
        Self { node, entity_id, state, credentials, budget, events: Mutex::new(HashMap::new()) }
    }

    async fn verify_clock(&self, clock: &Clock) -> Result<(), RetrievalError> {
        for (claimed, id) in clock {
            let event = self.get_event(id).await?;
            check_generation(id, *claimed, event.generation())?;
        }
        Ok(())
    }

    async fn bridge(
        &self,
        known: &Clock,
        current: &Clock,
        captured: Option<&Clock>,
        force_history: bool,
    ) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let target = captured.unwrap_or(current);
        // A sender can use the requester's frontier as a floor only after its
        // events establish the entity and generations the requester claims.
        let known_verified = match self.verify_clock(known).await {
            Ok(()) => true,
            Err(RetrievalError::EventNotFound(_)) => false,
            Err(error) => return Err(error),
        };
        if !force_history && known_verified && !known.is_empty() && target == current {
            match compare(self, current, known, self.budget.comparison_budget()).await {
                Ok(result) => match result.relation {
                    AbstractCausalRelation::Equal | AbstractCausalRelation::StrictAscends => return Ok(Vec::new()),
                    AbstractCausalRelation::StrictDescends { chain } => {
                        let mut events = Vec::with_capacity(chain.len());
                        for id in chain {
                            self.get_event(&id).await?;
                            events.push(self.events.lock().unwrap()[&id].clone());
                        }
                        return self.authorize_events(events);
                    }
                    AbstractCausalRelation::BudgetExceeded { .. } => return Err(failure(ComparisonFailure::LimitExceeded)),
                    _ => {}
                },
                Err(RetrievalError::EventNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }

        // Concurrent or unknown frontiers need a complete sender history. This
        // also restores history omitted by an earlier state-snapshot transfer.
        let mut pending = BTreeMap::new();
        for clock in [current, target] {
            for (generation, id) in clock {
                insert_generation(&mut pending, id.clone(), *generation)?;
            }
        }
        let mut visited = BTreeMap::new();
        let mut events = Vec::new();
        while let Some((id, claimed)) = pending.pop_first() {
            if let Some(known) = visited.get(&id) {
                check_generation(&id, claimed, *known)?;
                continue;
            }
            let event = self.get_event(&id).await?;
            check_generation(&id, claimed, event.generation())?;
            visited.insert(id.clone(), event.generation());
            for (generation, parent) in &event.parent {
                insert_generation(&mut pending, parent.clone(), *generation)?;
            }
            events.push(self.events.lock().unwrap()[&id].clone());
        }
        self.authorize_events(events)
    }

    fn authorize_events(&self, events: Vec<Attested<Event>>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let policy = ContextPolicy::from_credentials(&self.node.policy_agent, self.credentials);
        for event in &events { policy.check_read_event(event, self.state)?; }
        Ok(crate::event_dag::ordering::topo_sort_events(events)?)
    }
}

#[async_trait]
impl<SE: StorageEngine + 'static, PA: PolicyAgent> GetEvents for RepairHistory<'_, SE, PA> {
    async fn get_event(&self, id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(event) = self.events.lock().unwrap().get(id) {
            return Ok(event.payload.clone());
        }
        self.budget.read_event()?;
        let events = self.node.storage.get_events(vec![id.clone()]).await?;
        let event = events.into_iter().find(|event| event.payload.id() == *id).ok_or_else(|| RetrievalError::EventNotFound(id.clone()))?;
        if event.payload.entity_id != self.entity_id {
            return Err(invalid("a repair head belongs to another entity"));
        }
        event.payload.validate_structure()?;
        self.budget.inspect_event_bytes(size(&event)?)?;
        self.events.lock().unwrap().insert(id.clone(), event.clone());
        Ok(event.payload)
    }

    async fn event_stored(&self, id: &EventId) -> Result<bool, RetrievalError> {
        match self.get_event(id).await {
            Ok(_) => Ok(true),
            Err(RetrievalError::EventNotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn storage_is_definitive(&self) -> bool { self.node.durable }
}

fn insert_generation(pending: &mut BTreeMap<EventId, u32>, id: EventId, generation: u32) -> Result<(), RetrievalError> {
    if let Some(known) = pending.insert(id.clone(), generation) {
        check_generation(&id, generation, known)?;
    }
    Ok(())
}

fn check_generation(id: &EventId, claimed: u32, known: u32) -> Result<(), RetrievalError> {
    if claimed != known {
        return Err(proto::EventStructureError::GenerationMismatch { event: id.clone(), claimed, known }.into());
    }
    Ok(())
}

fn failure(error: ComparisonFailure) -> RetrievalError { RetrievalError::storage(error) }
fn invalid(message: &str) -> RetrievalError { failure(ComparisonFailure::InvalidRequest(message.into())) }
fn size(value: &impl serde::Serialize) -> Result<usize, RetrievalError> {
    bincode::serialized_size(value)?.try_into().map_err(|_| failure(ComparisonFailure::LimitExceeded))
}

#[cfg(test)]
mod tests;
