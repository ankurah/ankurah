use super::{
    set::EntityInner,
    state::EntityState,
    trx::{LocalTrxEntityInner, RemoteTrxEntityInner},
};
use crate::{property::PropertyError, schema::SystemEpoch};
use ankurah_proto::EntityId;
use ankurah_signals::broadcast::{Broadcast, ListenerGuard};
use std::sync::{Arc, Mutex, RwLock};

/// Reads a transaction entity's working state until commit or rollback redirects it:
/// to the resident entity, or to nothing for a rolled-back creation.
pub struct EntityProxy {
    target: RwLock<ProxyTarget>,
    /// Notifies readers when the target changes, so they can follow its signals.
    pub(super) broadcast: Broadcast,
    pub(super) system_epoch: SystemEpoch,
    subscription: Mutex<Option<ListenerGuard>>,
}

#[derive(Debug, Clone)]
pub(super) enum ProxyTarget {
    Local(Arc<LocalTrxEntityInner>),
    Remote(Arc<RemoteTrxEntityInner>),
    Resident(Arc<EntityInner>),
    /// A rolled-back creation has no committed entity to read.
    RolledBack(Option<EntityId>),
}

impl EntityProxy {
    pub(super) fn new(target: ProxyTarget, system_epoch: SystemEpoch) -> Self {
        let broadcast = Broadcast::new();
        let subscription = target.listen(&broadcast);
        Self { target: RwLock::new(target), broadcast, system_epoch, subscription: Mutex::new(subscription) }
    }

    pub(super) fn id(&self) -> EntityId {
        let target = self.target.read().unwrap().clone();
        match target {
            ProxyTarget::Local(entity) => entity.id(),
            ProxyTarget::Remote(entity) => entity.id(),
            ProxyTarget::Resident(entity) => entity.id,
            ProxyTarget::RolledBack(id) => id.expect("rolled-back creation has no entity id"),
        }
    }

    /// The identity, if already assigned; unlike `id`, this never freezes a pending genesis.
    pub(super) fn assigned_id(&self) -> Option<EntityId> {
        match &*self.target.read().unwrap() {
            ProxyTarget::Local(entity) => entity.assigned_id(),
            ProxyTarget::Remote(entity) => Some(entity.id()),
            ProxyTarget::Resident(entity) => Some(entity.id),
            ProxyTarget::RolledBack(id) => *id,
        }
    }

    pub(super) fn with_state<T>(&self, read: impl FnOnce(&EntityState) -> T) -> Result<T, PropertyError> {
        // Do not hold the target lock across callbacks or lazy genesis generation.
        let target = self.target.read().unwrap().clone();
        match target {
            ProxyTarget::Local(entity) => Ok(read(&entity.data().state)),
            ProxyTarget::Remote(entity) => Ok(read(&entity.data().state)),
            ProxyTarget::Resident(entity) => Ok(read(&entity.state)),
            ProxyTarget::RolledBack(_) => Err(PropertyError::TransactionClosed),
        }
    }

    pub(super) fn resident(&self) -> Result<Arc<EntityInner>, PropertyError> {
        match &*self.target.read().unwrap() {
            ProxyTarget::Resident(entity) => Ok(entity.clone()),
            ProxyTarget::RolledBack(_) => Err(PropertyError::TransactionClosed),
            _ => Err(crate::error::RetrievalError::Other("entity is still in a transaction".into()).into()),
        }
    }

    /// Commit or rollback redirects a proxy once; transaction drop cannot undo publication.
    pub(super) fn finish(&self, target: ProxyTarget) {
        let mut current = self.target.write().unwrap();
        if matches!(*current, ProxyTarget::Resident(_) | ProxyTarget::RolledBack(_)) { return; }
        let subscription = target.listen(&self.broadcast);
        *current = target;
        *self.subscription.lock().unwrap() = subscription;
        drop(current);
        self.broadcast.send(());
    }
}

impl ProxyTarget {
    fn listen(&self, broadcast: &Broadcast) -> Option<ListenerGuard> {
        let source = match self {
            Self::Local(entity) => &entity.data().state.broadcast,
            Self::Remote(entity) => &entity.data().state.broadcast,
            Self::Resident(entity) => &entity.state.broadcast,
            Self::RolledBack(_) => return None,
        };
        let broadcast = broadcast.clone();
        Some(source.reference().listen(move |_| broadcast.send(())))
    }
}

impl std::fmt::Debug for EntityProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityProxy").field("target", &*self.target.read().unwrap()).finish()
    }
}
