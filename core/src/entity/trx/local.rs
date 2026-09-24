use super::TrxEntityData;
use crate::{
    entity::{set::EntityInner, proxy::ProxyTarget, state::EntityState, Entity},
    error::{MutationError, RetrievalError, StateError},
    property::{backend::PropertyBackend, PropertyError},
    schema::SystemEpoch,
};
use ankurah_proto::{Attested, AuthorId, Clock, EntityId, Event, Membership, ModelId, Operation, OperationSet, State};
use std::{
    collections::BTreeSet,
    sync::{atomic::AtomicBool, Arc, Mutex, OnceLock},
};

/// Transaction-local property mutations, frozen into events before commit.
#[derive(Debug, Clone)]
pub struct LocalTrxEntity(Arc<LocalTrxEntityInner>);

#[derive(Debug)]
pub(in crate::entity) struct LocalTrxEntityInner {
    origin: Origin,
    author: AuthorId,
    data: TrxEntityData,
    /// Memberships added to the working state but not yet written into an event.
    staged_memberships: Mutex<BTreeSet<ModelId>>,
}

#[derive(Debug)]
enum Origin {
    Pending {
        /// First id demand or commit preparation freezes the genesis that determines identity.
        genesis: OnceLock<Event>,
        system: Option<EntityId>,
    },
    Mut {
        upstream: Arc<EntityInner>,
    },
}

impl LocalTrxEntity {
    pub(crate) fn new(system: Option<EntityId>, author: AuthorId, epoch: SystemEpoch, alive: Arc<AtomicBool>) -> Self {
        Self(Arc::new(LocalTrxEntityInner {
            origin: Origin::Pending { genesis: OnceLock::new(), system },
            author,
            data: TrxEntityData::new(EntityState::empty(), alive, epoch),
            staged_memberships: Mutex::default(),
        }))
    }

    pub(crate) fn edit(entity: &Entity, author: AuthorId, alive: Arc<AtomicBool>) -> Result<Self, PropertyError> {
        let upstream = entity.resident()?;
        let data = TrxEntityData::new(upstream.state.fork(), alive, upstream.system_epoch());
        Ok(Self(Arc::new(LocalTrxEntityInner {
            origin: Origin::Mut { upstream },
            author,
            data,
            staged_memberships: Mutex::default(),
        })))
    }

    /// Freeze the genesis on first demand, including all mutations made so far.
    pub fn id(&self) -> EntityId { self.0.id() }

    /// Inspect identity without forcing a pending creation to generate its genesis.
    pub(crate) fn assigned_id(&self) -> Option<EntityId> { self.0.assigned_id() }

    pub fn system_epoch(&self) -> SystemEpoch { self.0.data.system_epoch }

    pub fn head(&self) -> Clock { self.0.data.state.head() }

    pub fn memberships(&self) -> BTreeSet<ModelId> { self.0.data.state.memberships() }

    pub fn has_membership(&self, model: &ModelId) -> bool { self.memberships().contains(model) }

    /// Add a membership now; the next generated event records it.
    pub fn add_membership(&self, model: ModelId) -> Result<(), PropertyError> {
        self.check_open()?;
        // Holding the staged set keeps event generation from seeing the addition in only one place.
        let mut staged = self.0.staged_memberships.lock().unwrap();
        if self.0.data.state.add_membership(model) { staged.insert(model); }
        Ok(())
    }

    pub(crate) fn check_open(&self) -> Result<(), PropertyError> { self.0.data.check_open() }

    pub(crate) fn notify_changed(&self) { self.0.data.state.broadcast.send(()); }

    pub fn get_backend<P: PropertyBackend>(&self) -> Result<Arc<P>, RetrievalError> {
        self.check_open()?;
        self.0.data.state.get_backend::<P>()
    }

    pub fn to_state(&self) -> Result<State, StateError> { self.0.data.state.to_state() }

    /// Read local mutations now, then follow the committed resident entity or rollback outcome.
    pub fn read(&self) -> Entity { self.0.data.read(ProxyTarget::Local(self.0.clone())) }

    /// Freeze pending mutations for admission and storage; retries reuse these exact events.
    pub(crate) fn prepare_events(&self) -> Result<Vec<Attested<Event>>, MutationError> {
        let id = self.id();
        let data = &self.0.data;
        let mut events = data.events.lock().unwrap();
        // Collect mutations made since the previous event; a genesis frozen just now leaves none.
        let operations = self.0.take_operations()?;
        if !operations.is_empty() {
            let event = Event::update(id, data.state.head(), self.0.author.clone(), operations);
            data.state.set_head(event.id().into());
            events.push(event.into());
        }
        Ok(events.clone())
    }

    pub(crate) fn rollback(&self) {
        self.0.data.rollback(self.0.upstream(), self.assigned_id());
    }

    /// Redirect views after the admitted events have been persisted and published.
    pub(crate) fn committed(&self, entity: &Entity) -> Result<(), PropertyError> {
        self.0.data.committed(entity.resident()?);
        Ok(())
    }
}

impl PartialEq for LocalTrxEntity {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}

impl LocalTrxEntityInner {
    pub(in crate::entity) fn data(&self) -> &TrxEntityData { &self.data }

    pub(in crate::entity) fn id(&self) -> EntityId {
        match &self.origin {
            Origin::Mut { upstream } => upstream.id,
            Origin::Pending { genesis, system } => {
                genesis.get_or_init(|| {
                    let event = Event::genesis(*system, self.author.clone(), self.take_operations().unwrap());
                    self.data.state.set_head(event.id().into());
                    self.data.events.lock().unwrap().push(event.clone().into());
                    event
                }).entity_id
            }
        }
    }

    pub(in crate::entity) fn assigned_id(&self) -> Option<EntityId> {
        match &self.origin {
            Origin::Pending { genesis, .. } => genesis.get().map(|event| event.entity_id),
            Origin::Mut { upstream } => Some(upstream.id),
        }
    }

    fn upstream(&self) -> Option<&Arc<EntityInner>> {
        match &self.origin {
            Origin::Pending { .. } => None,
            Origin::Mut { upstream } => Some(upstream),
        }
    }

    /// Drain pending property operations and staged memberships into one event's operations.
    fn take_operations(&self) -> Result<OperationSet, MutationError> {
        let mut operations = self.data.state.extract_backend_operations()?;
        for model in std::mem::take(&mut *self.staged_memberships.lock().unwrap()) {
            operations.push(Operation::Membership(Membership::Add(model)));
        }
        Ok(operations)
    }
}
