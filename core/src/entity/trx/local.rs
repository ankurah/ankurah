use super::TrxEntityData;
use crate::{
    entity::{
        entity::EntityInner,
        proxy::{ProxyTarget, RolledBackCreation},
        state::EntityState,
        Entity,
    },
    error::{MutationError, RetrievalError, StateError},
    property::{backend::PropertyBackend, PropertyError},
    schema::SystemEpoch,
};
use ankurah_proto::{Attested, Clock, EntityId, Event, Identity, Membership, ModelId, Operation, OperationSet, State};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
};

/// Transaction-local property mutations, frozen into events before commit.
#[derive(Debug, Clone)]
pub struct LocalTrxEntity(Arc<LocalTrxEntityInner>);

#[derive(Debug)]
pub(in crate::entity) struct LocalTrxEntityInner {
    origin: Origin,
    data: TrxEntityData,
    /// Cleared when the transaction commits or rolls back; local writes check it.
    alive: Arc<AtomicBool>,
    /// The memberships this entity's events record so far: the forked entity's, or none for a
    /// creation. The next generated event adds and removes whatever the working state differs by.
    recorded_memberships: Mutex<BTreeSet<ModelId>>,
}

#[derive(Debug)]
enum Origin {
    Pending {
        /// First id demand or commit preparation freezes the genesis that determines identity.
        genesis: OnceLock<Event>,
        /// The identity the genesis is minted under.
        minter: Identity,
    },
    Mut {
        upstream: Arc<EntityInner>,
    },
}

impl LocalTrxEntity {
    pub(crate) fn new(minter: Identity, epoch: SystemEpoch, alive: Arc<AtomicBool>) -> Self {
        Self(Arc::new(LocalTrxEntityInner {
            origin: Origin::Pending { genesis: OnceLock::new(), minter },
            data: TrxEntityData::new(EntityState::empty(), epoch),
            alive,
            recorded_memberships: Mutex::new(BTreeSet::new()),
        }))
    }

    pub(crate) fn edit(entity: &Entity, alive: Arc<AtomicBool>) -> Result<Self, PropertyError> {
        let upstream = entity.resident()?;
        let data = TrxEntityData::new(upstream.state.fork(), upstream.system_epoch());
        let recorded_memberships = Mutex::new(data.state.memberships());
        Ok(Self(Arc::new(LocalTrxEntityInner { origin: Origin::Mut { upstream }, data, alive, recorded_memberships })))
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
        // Holding the recorded set keeps event generation from reading the working state mid-change.
        let _recorded = self.0.recorded_memberships.lock().unwrap();
        self.0.data.state.add_membership(model);
        Ok(())
    }

    /// Remove a membership now; the next generated event records it. The entity stays
    /// addressable by id even when this removes its last membership.
    pub fn remove_membership(&self, model: &ModelId) -> Result<(), PropertyError> {
        self.check_open()?;
        let _recorded = self.0.recorded_memberships.lock().unwrap();
        self.0.data.state.remove_membership(model);
        Ok(())
    }

    pub(crate) fn check_open(&self) -> Result<(), PropertyError> {
        if self.0.alive.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(PropertyError::TransactionClosed)
        }
    }

    pub(crate) fn notify_changed(&self) { self.0.data.state.broadcast.send(()); }

    pub fn get_backend<P: PropertyBackend>(&self) -> Result<Arc<P>, RetrievalError> {
        self.check_open()?;
        self.0.data.state.get_backend::<P>()
    }

    pub fn to_state(&self) -> Result<State, StateError> { self.0.data.state.to_state() }

    /// Read local mutations now, then follow the committed resident entity or rollback outcome.
    pub fn read(&self) -> Entity { self.0.data.read(ProxyTarget::Local(self.0.clone()), || RolledBackCreation::Local(self.0.clone())) }

    /// Freeze pending mutations for admission and storage; retries reuse these exact events.
    pub(crate) fn prepare_events(&self) -> Result<Vec<Attested<Event>>, MutationError> {
        let creating = self.assigned_id().is_none();
        let id = self.id();
        let data = &self.0.data;
        let mut events = data.events.lock().unwrap();
        // A genesis frozen just now already took every pending mutation; extracting again is not a no-op,
        // because yrs re-encodes its whole delete set in every diff.
        if !creating {
            let operations = self.0.take_operations()?;
            if !operations.is_empty() {
                let event = Event::update(id, data.state.head(), operations);
                data.state.set_head(Clock::singleton(&event));
                events.push(event.into());
            }
        }
        Ok(events.clone())
    }

    pub(crate) fn rollback(&self) { self.0.data.rollback(self.0.upstream(), RolledBackCreation::Local(self.0.clone())); }

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
            Origin::Pending { genesis, minter } => {
                genesis
                    .get_or_init(|| {
                        // Cannot fail: Yrs encoding has no error path, and LWW's bincode encoding of `Value`s cannot fail.
                        let event = Event::genesis(*minter, self.take_operations().unwrap());
                        self.data.state.set_head(Clock::singleton(&event));
                        self.data.events.lock().unwrap().push(event.clone().into());
                        event
                    })
                    .entity_id
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

    /// Drain pending property operations, and the membership changes since the last event, into one event's operations.
    fn take_operations(&self) -> Result<OperationSet, MutationError> {
        let mut operations = self.data.state.extract_backend_operations()?;
        let mut recorded = self.recorded_memberships.lock().unwrap();
        let current = self.data.state.memberships();
        for model in current.difference(&recorded) {
            operations.push(Operation::Membership(Membership::Add(*model)));
        }
        for model in recorded.difference(&current) {
            operations.push(Operation::Membership(Membership::Remove(*model)));
        }
        *recorded = current;
        Ok(operations)
    }
}
