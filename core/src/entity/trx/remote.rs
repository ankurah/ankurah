use super::TrxEntityData;
use crate::{
    changes::EntityChange,
    entity::{entity::EntityInner, proxy::{ProxyTarget, RolledBackCreation}, state::EntityState, Entity, WeakEntitySet},
    error::{MutationError, RetrievalError, StateError},
    property::PropertyError,
    retrieval::{GetEvents, GetState},
    schema::SystemEpoch,
};
use ankurah_proto::{Attestation, Attested, Clock, EntityId, Event, EventId, State};
use std::sync::{Arc, Mutex};

/// Transaction-local application of existing events and state snapshots, isolated from the resident entity until
/// storage commits. "Remote" names this input mode, not where the events come from: local commits use it too.
#[derive(Debug, Clone)]
pub struct RemoteTrxEntity(Arc<RemoteTrxEntityInner>);

#[derive(Debug)]
pub(in crate::entity) enum RemoteTrxEntityInner {
    New {
        id: EntityId,
        data: TrxEntityData,
    },
    Mut {
        upstream: Arc<EntityInner>,
        data: TrxEntityData,
    },
}

impl RemoteTrxEntity {
    /// Fork the entity an event targets, loading it from storage if it isn't resident.
    /// If this node has no such entity, the event must create it.
    pub(crate) async fn for_event<S, G>(
        entities: &WeakEntitySet, state_getter: &S, event_getter: &G, event: &Event,
    ) -> Result<Self, MutationError>
    where
        S: GetState + Send + Sync,
        G: GetEvents + Send + Sync,
    {
        match entities.get_or_retrieve(state_getter, event_getter, &event.entity_id).await? {
            Some(entity) => Ok(Self::edit(&entity)?),
            None => Self::new(event, entities.system_epoch()),
        }
    }

    /// Check that an event is a valid genesis, then start an empty creation for its entity. The caller applies the
    /// genesis next; no resident entity is registered until the creation commits.
    pub(crate) fn new(genesis: &Event, epoch: SystemEpoch) -> Result<Self, MutationError> {
        genesis.validate_structure()?;
        if !genesis.is_entity_create() { return Err(MutationError::InvalidEvent); }

        Ok(Self(Arc::new(RemoteTrxEntityInner::New {
            id: genesis.entity_id,
            data: TrxEntityData::new(EntityState::empty(), epoch),
        })))
    }

    /// Fork a resident entity; committing the fork applies its events to that entity.
    pub(crate) fn edit(entity: &Entity) -> Result<Self, PropertyError> {
        let upstream = entity.resident()?;
        let data = TrxEntityData::new(upstream.state.fork(), upstream.system_epoch());
        Ok(Self(Arc::new(RemoteTrxEntityInner::Mut { upstream, data })))
    }

    pub fn id(&self) -> EntityId { self.0.id() }

    pub fn head(&self) -> Clock { self.0.data().state.head() }

    pub fn to_state(&self) -> Result<State, StateError> { self.0.data().state.to_state() }

    pub fn read(&self) -> Entity {
        self.0.data().read(ProxyTarget::Remote(self.0.clone()), || RolledBackCreation::Remote(self.id()))
    }

    pub(crate) async fn apply_state<G>(&self, getter: &G, state: &State) -> Result<crate::entity::StateApplyResult, MutationError>
    where G: GetEvents + Send + Sync {
        self.0.data().state.apply_state(getter, state).await
    }

    /// Apply and admit an event before retaining it for publication. Discard this fork on failure.
    pub(crate) async fn apply_event<G>(
        &self,
        getter: &G,
        event: &mut Attested<Event>,
        check: impl FnOnce(&Event) -> Result<Option<Attestation>, MutationError>,
    ) -> Result<bool, MutationError>
    where G: GetEvents + Send + Sync {
        if event.payload.entity_id != self.id() { return Err(MutationError::InvalidEvent); }

        let data = self.0.data();
        let getter = TransactionEventGetter::new(&data.events, &event.payload, getter);
        let applied = data.state.apply_event(&getter, &event.payload).await?;

        if let Some(attestation) = check(&event.payload)? {
            event.attestations.push(attestation);
        }

        if applied { data.events.lock().unwrap().push(event.clone()); }
        Ok(applied)
    }

    /// A frozen policy input; it shares neither mutable backends nor pending events with this fork.
    pub(crate) fn snapshot(&self) -> Entity {
        let data = self.0.data();
        let snapshot = TrxEntityData::new(data.state.fork(), data.system_epoch);

        let inner = match &*self.0 {
            RemoteTrxEntityInner::New { id, .. } => RemoteTrxEntityInner::New { id: *id, data: snapshot },
            RemoteTrxEntityInner::Mut { upstream, .. } => RemoteTrxEntityInner::Mut { upstream: upstream.clone(), data: snapshot },
        };
        Self(Arc::new(inner)).read()
    }

    /// Publish admitted events after storage commits, then redirect any views to the resident entity.
    pub(crate) async fn commit<G>(self, entities: &WeakEntitySet, getter: &G) -> Result<EntityChange, MutationError>
    where G: GetEvents + Send + Sync {
        let data = self.0.data();
        let events = std::mem::take(&mut *data.events.lock().unwrap());

        let resident = match &*self.0 {
            RemoteTrxEntityInner::Mut { upstream, .. } => upstream.clone(),
            RemoteTrxEntityInner::New { id, .. } => {
                let (existed, resident) = entities.publish_new(*id, &data.state)?;
                if !existed {
                    data.committed(resident.clone());
                    return EntityChange::new(Entity::from_resident(resident), events);
                }
                resident
            }
        };

        let mut change = EntityChange::new(Entity::from_resident(resident.clone()), Vec::new())?;
        for event in events {
            // Storage now contains the complete transaction's causal history.
            if resident.state.apply_event(getter, &event.payload).await? {
                change.push_event(event)?;
            }
        }

        data.committed(resident);
        Ok(change)
    }

    pub(crate) fn rollback(&self) {
        let upstream = match &*self.0 {
            RemoteTrxEntityInner::New { .. } => None,
            RemoteTrxEntityInner::Mut { upstream, .. } => Some(upstream),
        };
        self.0.data().rollback(upstream, RolledBackCreation::Remote(self.id()));
    }
}

impl RemoteTrxEntityInner {
    pub(in crate::entity) fn data(&self) -> &TrxEntityData {
        match self {
            Self::New { data, .. } | Self::Mut { data, .. } => data,
        }
    }

    pub(in crate::entity) fn id(&self) -> EntityId {
        match self {
            Self::New { id, .. } => *id,
            Self::Mut { upstream, .. } => upstream.id,
        }
    }
}

/// Search transaction events before falling back to stored or remote history.
/// Later events can depend on earlier events in the same uncommitted transaction.
struct TransactionEventGetter<'a, G> {
    events: &'a Mutex<Vec<Attested<Event>>>,
    current: &'a Event,
    fallback: &'a G,
}

impl<'a, G> TransactionEventGetter<'a, G> {
    fn new(events: &'a Mutex<Vec<Attested<Event>>>, current: &'a Event, fallback: &'a G) -> Self { Self { events, current, fallback } }
}

#[async_trait::async_trait]
impl<G: GetEvents + Send + Sync> GetEvents for TransactionEventGetter<'_, G> {
    async fn get_event(&self, id: &EventId) -> Result<Event, RetrievalError> {
        if self.current.id() == *id {
            return Ok(self.current.clone());
        }
        if let Some(event) = self.events.lock().unwrap().iter().find(|event| event.payload.id() == *id) {
            return Ok(event.payload.clone());
        }
        self.fallback.get_event(id).await
    }

    // Pending events are discoverable, but are not evidence of permanent storage.
    async fn event_stored(&self, id: &EventId) -> Result<bool, RetrievalError> { self.fallback.event_stored(id).await }

    fn storage_is_definitive(&self) -> bool { self.fallback.storage_is_definitive() }
}
