mod local;
mod remote;

pub use local::LocalTrxEntity;
pub(super) use local::LocalTrxEntityInner;
pub use remote::RemoteTrxEntity;
pub(super) use remote::RemoteTrxEntityInner;

use super::{
    entity::EntityInner,
    proxy::{EntityProxy, ProxyTarget, RolledBackCreation},
    state::EntityState,
    Entity,
};
use crate::schema::SystemEpoch;
use ankurah_proto::{Attested, Event};
use std::sync::{Arc, Mutex, Weak};

#[derive(Debug)]
pub(super) struct TrxEntityData {
    pub(super) state: EntityState,
    /// Events awaiting commit: a local entity's frozen events, or a remote fork's admitted ones.
    events: Mutex<Vec<Attested<Event>>>,
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
    RolledBack,
}

impl TrxEntityData {
    fn new(state: EntityState, system_epoch: SystemEpoch) -> Self {
        Self { state, events: Mutex::new(Vec::new()), system_epoch, view: Mutex::new(TrxView::Open(Weak::new())) }
    }

    fn read(&self, target: ProxyTarget, rolled_back: impl FnOnce() -> RolledBackCreation) -> Entity {
        match &mut *self.view.lock().unwrap() {
            TrxView::Open(proxy) => Entity::from_proxy(proxy.upgrade().unwrap_or_else(|| {
                let entity = Arc::new(EntityProxy::new(target, self.system_epoch));
                *proxy = Arc::downgrade(&entity);
                entity
            })),
            TrxView::Resident(entity) => Entity::from_resident(entity.clone()),
            TrxView::RolledBack => {
                Entity::from_proxy(Arc::new(EntityProxy::new(ProxyTarget::RolledBack(rolled_back()), self.system_epoch)))
            }
        }
    }

    /// Redirect this transaction entity's existing and future reads to the committed resident.
    /// Call after persistence and publication; existing proxies switch their signal subscriptions
    /// and notify their listeners.
    fn committed(&self, resident: Arc<EntityInner>) { self.finish(TrxView::Resident(resident.clone()), ProxyTarget::Resident(resident)); }

    fn rollback(&self, upstream: Option<&Arc<EntityInner>>, creation: RolledBackCreation) {
        match upstream {
            Some(entity) => self.finish(TrxView::Resident(entity.clone()), ProxyTarget::Resident(entity.clone())),
            None => self.finish(TrxView::RolledBack, ProxyTarget::RolledBack(creation)),
        }
    }

    fn finish(&self, outcome: TrxView, target: ProxyTarget) {
        let mut view = self.view.lock().unwrap();
        let TrxView::Open(proxy) = &*view else { return };
        let proxy = proxy.upgrade();
        *view = outcome;
        drop(view);
        if let Some(proxy) = proxy {
            proxy.finish(target);
        }
    }
}
