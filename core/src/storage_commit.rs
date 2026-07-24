use std::collections::{BTreeMap, BTreeSet};

use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelContext, ModelId, State};

use crate::{
    entity::{Entity, StateApplyResult},
    error::MutationError,
    node::Node,
    policy::PolicyAgent,
    retrieval::GetEvents,
    storage::{CommitBatchOutcome, PreparedEntityWrite, StorageCommitResult, StorageEngine, StorageWriteBatch},
};

/// Replayable intent for one canonical entity in a storage batch.
///
/// Events have already passed ingress policy and been durably appended before
/// this intent is committed. `associate_with` records every model context used
/// by the logical transaction.
pub(crate) struct ResidentWriteIntent {
    /// Canonical in-memory entity to reconcile after durable commit.
    pub(crate) entity: Entity,
    /// Model usages which must be durably associated with the entity.
    pub(crate) associate_with: BTreeSet<ModelId>,
    /// Validated events to replay over the latest durable state.
    pub(crate) events: Vec<Attested<Event>>,
    /// Model usage under which each event was authorized.
    ///
    /// Retry policy checks must use the same model context as the original
    /// ingress check even though event payloads are model-independent.
    pub(crate) event_models: BTreeMap<EventId, ModelId>,
    /// Validated state seed for replication shapes without replayable events.
    pub(crate) seed_state: Option<State>,
}

impl ResidentWriteIntent {
    /// Build an event-backed write intent while retaining authorization and
    /// materialization model contexts outside the model-independent events.
    pub(crate) fn from_events(entity: Entity, events: Vec<ModelContext<Attested<Event>>>) -> Result<Self, MutationError> {
        let associate_with = events.iter().map(|event| event.model).collect();
        let mut event_models = BTreeMap::new();
        for event in &events {
            if let Some(existing) = event_models.insert(event.value.payload.id(), event.model) {
                if existing != event.model {
                    return Err(MutationError::InvalidUpdate("the same event cannot carry conflicting model contexts"));
                }
            }
        }
        let events = events.into_iter().map(|event| event.value).collect();
        Ok(Self { entity, associate_with, events, event_models, seed_state: None })
    }

    /// Build an intent from a validated resident snapshot.
    ///
    /// This is used by replication shapes which may carry state without a
    /// replayable event list. On conflict, durable concurrent tips are merged
    /// into this seed before the next CAS attempt.
    pub(crate) fn from_resident_state(entity: Entity, accessed_as: ModelId, events: Vec<Attested<Event>>) -> Result<Self, MutationError> {
        let seed_state = Some(entity.to_state()?);
        let event_models = events.iter().map(|event| (event.payload.id(), accessed_as)).collect();
        Ok(Self { entity, associate_with: [accessed_as].into(), events, event_models, seed_state })
    }
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Commit canonical resident entities with exact-head optimistic retries.
    ///
    /// A failed engine CAS returns the complete canonical states it observed.
    /// Each event-backed retry rebuilds on a detached copy of that exact state
    /// and replays the transaction's already-durable events, re-running each
    /// event's original policy check against its refreshed before/after states.
    /// A state-backed retry instead merges the observed durable state into its
    /// validated snapshot seed. The next compare value is the observed storage
    /// head; the swap value is the rebuilt state. Only a successful durable
    /// result is reconciled into the canonical resident. `event_getter` is the
    /// ingress path's causal source: local commits resolve their appended
    /// events from storage, while replicated snapshots may fetch missing
    /// lineage from the serving peer.
    pub(crate) async fn commit_resident_writes<E>(
        &self,
        mut intents: Vec<ResidentWriteIntent>,
        policy_context: Option<&PA::ContextData>,
        event_getter: &E,
    ) -> Result<StorageCommitResult, MutationError>
    where
        E: GetEvents + Send + Sync,
    {
        if intents.is_empty() {
            return Ok(StorageCommitResult::default());
        }
        intents.sort_by_key(|intent| intent.entity.id());
        for pair in intents.windows(2) {
            if pair[0].entity.id() == pair[1].entity.id() {
                return Err(MutationError::General(
                    format!("resident write batch contains duplicate entity {}", pair[0].entity.id()).into(),
                ));
            }
        }
        for intent in &mut intents {
            intent.events = crate::event_dag::ordering::topo_sort_events(std::mem::take(&mut intent.events))?;
        }

        let ids: Vec<EntityId> = intents.iter().map(|intent| intent.entity.id()).collect();
        let mut observed: BTreeMap<EntityId, Option<Attested<EntityState>>> = ids.iter().copied().map(|id| (id, None)).collect();
        for state in self.storage.get_states(ids.clone()).await? {
            observed.insert(state.payload.entity_id, Some(state));
        }

        let residents: BTreeMap<EntityId, Entity> = intents.iter().map(|intent| (intent.entity.id(), intent.entity.clone())).collect();
        const MAX_ATTEMPTS: usize = 32;
        for _attempt in 0..MAX_ATTEMPTS {
            let mut writes = Vec::with_capacity(intents.len());
            let mut prepared_states = BTreeMap::new();
            for intent in &intents {
                let entity_id = intent.entity.id();
                let storage_state = observed
                    .get(&entity_id)
                    .ok_or_else(|| MutationError::General(format!("storage omitted conflict state for entity {entity_id}").into()))?;
                let expected_head = storage_state.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_else(Clock::default);

                let candidate = if let Some(seed_state) = &intent.seed_state {
                    let candidate = intent.entity.detached_at_state(Some(seed_state))?;
                    if let Some(storage_state) = storage_state {
                        match candidate.apply_state(event_getter, &storage_state.payload.state).await? {
                            StateApplyResult::DivergedRequiresEvents => {
                                for event_id in storage_state.payload.state.head.iter() {
                                    let event = event_getter.get_event(event_id).await?;
                                    candidate.apply_event(event_getter, &event).await?;
                                }
                            }
                            StateApplyResult::Applied | StateApplyResult::AlreadyApplied | StateApplyResult::Older => {}
                        }
                    }
                    candidate
                } else {
                    intent.entity.detached_at_state(storage_state.as_ref().map(|state| &state.payload.state))?
                };

                for event in &intent.events {
                    let model = intent.event_models.get(&event.payload.id()).copied().ok_or_else(|| {
                        MutationError::General(format!("write intent omitted model context for event {}", event.payload.id()).into())
                    })?;
                    let before =
                        candidate.snapshot(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true))).with_model_context(model);
                    let applied = candidate.apply_event(event_getter, &event.payload).await?;
                    if applied {
                        if let Some(context) = policy_context {
                            // The event passed this same check before append.
                            // Re-run it against the refreshed before/after
                            // states so a concurrent durable branch cannot
                            // invalidate state-dependent authorization.
                            self.policy_agent.check_event(self, context, &before, &candidate.with_model_context(model), &event.payload)?;
                        }
                    }
                }

                let candidate_state = candidate.to_state()?;
                let entity_state = EntityState { entity_id, state: candidate_state.clone() };
                let attestation = self.policy_agent.attest_state(self, &entity_state);
                prepared_states.insert(entity_id, candidate_state);
                writes.push(PreparedEntityWrite::new(
                    expected_head,
                    Attested::opt(entity_state, attestation),
                    intent.associate_with.iter().copied(),
                ));
            }

            match self.storage.commit_batch(StorageWriteBatch::new(writes)).await? {
                CommitBatchOutcome::Committed(result) => {
                    for (entity_id, state) in prepared_states {
                        residents
                            .get(&entity_id)
                            .ok_or_else(|| MutationError::General(format!("missing resident entity {entity_id}").into()))?
                            .reconcile_committed_state(event_getter, &state)
                            .await?;
                    }
                    return Ok(result);
                }
                CommitBatchOutcome::Conflict { observed: conflict_states } => {
                    for entity_id in &ids {
                        let state = conflict_states.get(entity_id).ok_or_else(|| {
                            MutationError::General(format!("storage omitted conflict state for entity {entity_id}").into())
                        })?;
                        observed.insert(*entity_id, state.clone());
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
        Err(MutationError::TOCTOUAttemptsExhausted)
    }
}
