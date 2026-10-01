use super::{proxy::EntityProxy, set::Registration, state::EntityState};
use crate::{error::{MutationError, StateError}, property::PropertyError, schema::SystemEpoch, value::Value};
use ankurah_proto::{Clock, EntityId, EntityState as ProtoEntityState, ModelId, PropertyId, State};
use ankurah_signals::broadcast::Broadcast;
use std::{collections::BTreeSet, sync::Arc};

/// A replicated object's identity, properties, and model memberships.
/// Reads either the node's resident instance or a transaction proxy that follows commit or rollback.
#[derive(Debug, Clone)]
pub struct Entity(Handle);

#[derive(Debug, Clone)]
enum Handle {
    Resident(Arc<EntityInner>),
    Proxy(Arc<EntityProxy>),
}

impl Entity {
    pub(crate) fn from_resident(entity: Arc<EntityInner>) -> Self { Self(Handle::Resident(entity)) }

    pub(super) fn from_proxy(proxy: Arc<EntityProxy>) -> Self { Self(Handle::Proxy(proxy)) }

    /// Freezes a pending creation's genesis on first demand.
    pub fn id(&self) -> EntityId {
        // FIXME: A rolled-back creation still reports an id, though no entity with that id exists.
        match &self.0 {
            Handle::Resident(entity) => entity.id,
            Handle::Proxy(proxy) => proxy.id(),
        }
    }

    pub fn system_epoch(&self) -> SystemEpoch {
        match &self.0 {
            Handle::Resident(entity) => entity.system_epoch(),
            Handle::Proxy(proxy) => proxy.system_epoch,
        }
    }

    pub(crate) fn check_epoch(&self, expected: SystemEpoch) -> Result<(), MutationError> {
        if self.system_epoch() != expected { return Err(MutationError::ForeignEntity); }
        Ok(())
    }

    /// A rolled-back creation has no history, so its head is empty.
    pub fn head(&self) -> Clock { self.with_state(EntityState::head).unwrap_or_default() }

    /// Model memberships visible through this handle, including transaction-local additions.
    /// A rolled-back creation has none.
    pub fn memberships(&self) -> BTreeSet<ModelId> { self.with_state(EntityState::memberships).unwrap_or_default() }

    pub fn has_membership(&self, model: &ModelId) -> bool { self.memberships().contains(model) }

    /// Whether this handle still has an entity to read; false only for a creation whose transaction rolled back.
    pub fn is_alive(&self) -> bool { self.with_state(|_| ()).is_ok() }

    pub fn to_state(&self) -> Result<State, StateError> {
        self.with_state(EntityState::to_state).map_err(|_| StateError::TransactionClosed)?
    }

    pub fn to_entity_state(&self) -> Result<ProtoEntityState, StateError> {
        Ok(ProtoEntityState { entity_id: self.id(), state: self.to_state()? })
    }

    pub fn values(&self) -> Result<Vec<(PropertyId, Option<Value>)>, PropertyError> { self.with_state(EntityState::values) }

    pub fn property_value(&self, property: &PropertyId) -> Result<Option<Value>, PropertyError> {
        if *property == PropertyId::Id { return Ok(Some(Value::EntityId(self.id()))); }
        self.with_state(|state| state.value(property))
    }

    /// Read a backend's property value through the resident entity or this view's transaction proxy.
    pub fn read_property(&self, backend: &str, property: &PropertyId) -> Result<Option<Value>, PropertyError> {
        self.with_state(|state| state.read_property(backend, property))
    }

    pub fn broadcast(&self) -> &Broadcast {
        match &self.0 {
            Handle::Resident(entity) => &entity.state.broadcast,
            Handle::Proxy(proxy) => &proxy.broadcast,
        }
    }

    pub(super) fn with_state<T>(&self, read: impl FnOnce(&EntityState) -> T) -> Result<T, PropertyError> {
        match &self.0 {
            Handle::Resident(entity) => Ok(read(&entity.state)),
            Handle::Proxy(proxy) => proxy.with_state(read),
        }
    }

    /// The committed entity this handle reads; fails while it reads uncommitted state or after its creation rolled back.
    pub(crate) fn resident(&self) -> Result<Arc<EntityInner>, PropertyError> {
        match &self.0 {
            Handle::Resident(entity) => Ok(entity.clone()),
            Handle::Proxy(proxy) => proxy.resident(),
        }
    }
}

impl PartialEq for Entity {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Handle::Resident(a), Handle::Resident(b)) => Arc::ptr_eq(a, b),
            (Handle::Proxy(a), Handle::Proxy(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Formatting must neither freeze a pending genesis nor panic for a creation rolled back before it had an ID.
        let id = match &self.0 {
            Handle::Resident(entity) => Some(entity.id),
            Handle::Proxy(proxy) => proxy.assigned_id(),
        };
        match id {
            Some(id) => write!(f, "Entity({} {:#})", id.to_base64_short(), self.head()),
            None => write!(f, "Entity(unassigned {:#})", self.head()),
        }
    }
}

impl crate::reactor::AbstractEntity for Entity {
    fn id(&self) -> EntityId { self.id() }
    fn memberships(&self) -> BTreeSet<ModelId> { self.memberships() }
    fn value(&self, property: &PropertyId) -> Option<Value> { self.property_value(property).ok().flatten() }
}

impl crate::selection::filter::Filterable for Entity {
    fn value(&self, property: &PropertyId) -> Option<Value> { self.property_value(property).ok().flatten() }

    fn is_member_of(&self, model: &ModelId) -> Result<bool, crate::selection::filter::Error> {
        self.with_state(|state| state.memberships().contains(model))
            .map_err(|_| crate::selection::filter::Error::UnsupportedExpression("entity was rolled back"))
    }
}

/// The node's committed instance of an entity. Only `WeakEntitySet` constructs one, since only it can create the
/// `Registration` it holds.
pub(crate) struct EntityInner {
    pub(super) id: EntityId,
    pub(super) state: EntityState,
    pub(super) registry: Registration,
}

impl EntityInner {
    pub(super) fn system_epoch(&self) -> SystemEpoch { self.registry.system_epoch() }
}

impl std::fmt::Debug for EntityInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityInner")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("system_epoch", &self.system_epoch())
            .finish()
    }
}
