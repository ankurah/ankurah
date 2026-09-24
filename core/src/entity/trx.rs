mod local;
mod remote;

pub use local::LocalTrxEntity;
pub use remote::RemoteTrxEntity;
pub(super) use local::LocalTrxEntityInner;
pub(super) use remote::RemoteTrxEntityInner;

use super::{set::EntityInner, proxy::{EntityProxy, ProxyTarget}, state::EntityState, Entity};
use crate::{property::PropertyError, schema::SystemEpoch};
use ankurah_proto::{Attested, EntityId, Event};
use std::sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, Weak};

#[derive(Debug)]
pub(super) struct TrxEntityData {
    pub(super) state: EntityState,
    /// Events awaiting commit: a local entity's frozen events, or a remote fork's admitted ones.
    events: Mutex<Vec<Attested<Event>>>,
    trx_alive: Arc<AtomicBool>,
    system_epoch: SystemEpoch,
    /// Coordinate proxy creation with the final outcome; otherwise a racing read could
    /// create a view that never receives the commit/rollback redirection.
    view: Mutex<TrxView>,
}

#[derive(Debug)]
enum TrxView {
    /// The proxy owns its transaction target, so the back-reference must be weak.
    Open(Weak<EntityProxy>),
    Resident(Arc<EntityInner>),
    RolledBack(Option<EntityId>),
}

impl TrxEntityData {
    fn new(state: EntityState, trx_alive: Arc<AtomicBool>, system_epoch: SystemEpoch) -> Self {
        Self { state, events: Mutex::new(Vec::new()), trx_alive, system_epoch, view: Mutex::new(TrxView::Open(Weak::new())) }
    }

    fn check_open(&self) -> Result<(), PropertyError> {
        if !self.trx_alive.load(Ordering::Acquire) { return Err(PropertyError::TransactionClosed); }
        Ok(())
    }

    fn read(&self, target: ProxyTarget) -> Entity {
        match &mut *self.view.lock().unwrap() {
            TrxView::Open(proxy) => Entity::Proxy(proxy.upgrade().unwrap_or_else(|| {
                let entity = Arc::new(EntityProxy::new(target, self.system_epoch));
                *proxy = Arc::downgrade(&entity);
                entity
            })),
            TrxView::Resident(entity) => Entity::Resident(entity.clone()),
            TrxView::RolledBack(id) => Entity::Proxy(Arc::new(EntityProxy::new(ProxyTarget::RolledBack(*id), self.system_epoch))),
        }
    }

    fn committed(&self, resident: Arc<EntityInner>) {
        self.finish(TrxView::Resident(resident));
    }

    fn rollback(&self, upstream: Option<&Arc<EntityInner>>, id: Option<EntityId>) {
        self.finish(match upstream {
            Some(entity) => TrxView::Resident(entity.clone()),
            None => TrxView::RolledBack(id),
        });
    }

    fn finish(&self, outcome: TrxView) {
        let mut view = self.view.lock().unwrap();
        let TrxView::Open(proxy) = &*view else { return };
        let proxy = proxy.upgrade();
        let target = match &outcome {
            TrxView::Resident(entity) => ProxyTarget::Resident(entity.clone()),
            TrxView::RolledBack(id) => ProxyTarget::RolledBack(*id),
            TrxView::Open(_) => unreachable!(),
        };
        *view = outcome;
        drop(view);
        if let Some(proxy) = proxy { proxy.finish(target); }
    }
}
