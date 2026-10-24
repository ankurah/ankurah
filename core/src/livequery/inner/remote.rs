//! Binding a live query to a remote peer's state stream: which stream currently decides the
//! query's membership, the ids that stream admitted, and installing what it sends.

use super::LiveQueryInner;
use crate::context::{Context, DynContextInner};
use crate::internal::prelude::*;
use crate::reactor::fetch_gap::{GapFetcher, QueryGapFetcher};
use ankql::ast::Resolved;
use std::sync::{atomic::Ordering, Arc};

/// The remote state stream bound to a live query's selection version.
pub(super) struct RemoteStream {
    version: u32,
    /// The request that opened the stream.
    origin: proto::RequestId,
    /// A complete install has initialized the query from this stream.
    activated: bool,
    /// An install, complete or partial, has published this stream's membership.
    installed: bool,
    /// Cleared when the stream is lost, fails or is replaced: from then on it no longer decides
    /// the query's membership, and local data may enter the query again.
    decides_membership: Arc<std::sync::atomic::AtomicBool>,
    /// The ids the stream admitted into the query.
    admitted: Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>,
    refresh: Option<crate::node::state_sync::RefreshHandle>,
}

impl LiveQueryInner {
    pub(in crate::livequery) fn bind_remote_stream(
        &self,
        version: u32,
        origin: proto::RequestId,
        canceled: &(dyn Fn() -> bool + Send + Sync),
    ) {
        let mut results = self.resultset.write();
        let state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut stream = self.remote_stream.lock().unwrap();
        if self.is_current(version) && !canceled() {
            // A replacement can bind before the old loss callback runs.
            // Its pending state must already invalidate that earlier answer.
            if stream.as_ref().is_some_and(|stream| stream.version == version && stream.activated) {
                self.initialized_version.store(version.saturating_sub(1), Ordering::Release);
                self.remote_activated_version.store(0, Ordering::Release);
                results.set_loaded(false);
            }
            self.durable_version.store(version.saturating_sub(1), Ordering::Release);
            if let Some(previous) = stream.as_ref() {
                previous.decides_membership.store(false, Ordering::Release);
            }
            *stream = Some(RemoteStream {
                version,
                origin,
                activated: false,
                installed: false,
                decides_membership: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                admitted: Arc::new(std::sync::RwLock::new(results.iter_entities().map(|(id, _)| id).collect())),
                refresh: None,
            });
        }
        drop(stream);
        drop(state);
        drop(results);
    }

    pub(in crate::livequery) fn set_admitted(
        &self,
        version: u32,
        origin: &proto::RequestId,
        ids: impl IntoIterator<Item = proto::EntityId>,
    ) -> bool {
        let stream = self.remote_stream.lock().unwrap();
        let Some(stream) = stream.as_ref().filter(|stream| stream.version == version && &stream.origin == origin) else { return false };
        *stream.admitted.write().unwrap() = ids.into_iter().collect();
        true
    }

    pub(in crate::livequery) fn set_refresh(
        &self,
        version: u32,
        origin: &proto::RequestId,
        refresh: crate::node::state_sync::RefreshHandle,
    ) {
        let mut stream = self.remote_stream.lock().unwrap();
        if let Some(stream) = stream.as_mut().filter(|stream| stream.version == version && &stream.origin == origin) {
            stream.refresh = Some(refresh);
        }
    }

    pub(in crate::livequery) fn retain_tree(&self, version: u32, pin: Option<crate::node::state_sync::ResidentPin>) {
        let _state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if self.is_current(version) {
            *self.resident_tree.lock().unwrap() = pin;
        }
    }

    pub(in crate::livequery) fn refresh_handle(&self) -> Option<crate::node::state_sync::RefreshHandle> {
        self.remote_stream.lock().unwrap().as_ref().filter(|stream| stream.activated).and_then(|stream| stream.refresh.clone())
    }

    pub(in crate::livequery) fn remote_stream_pending(&self, version: u32) -> bool {
        self.remote_stream.lock().unwrap().as_ref().is_some_and(|stream| stream.version == version && !stream.activated)
    }

    fn remote_stream_current(&self, version: u32, origin: &proto::RequestId) -> bool {
        self.is_current(version)
            && self.remote_stream.lock().unwrap().as_ref().is_some_and(|stream| {
                stream.version == version && &stream.origin == origin && stream.decides_membership.load(Ordering::Acquire)
            })
    }

    pub(in crate::livequery) fn lose_remote_stream(&self, version: u32, origin: &proto::RequestId) {
        let mut results = self.resultset.write();
        let state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut bound = self.remote_stream.lock().unwrap();
        let Some(stream) = bound.as_mut().filter(|stream| stream.version == version && &stream.origin == origin) else { return };
        if !self.is_current(version) {
            return;
        }
        stream.activated = false;
        stream.installed = false;
        stream.refresh = None;
        stream.decides_membership.store(false, Ordering::Release);
        self.remote_activated_version.store(0, Ordering::Release);
        self.initialized_version.store(version.saturating_sub(1), Ordering::Release);
        self.durable_version.store(version.saturating_sub(1), Ordering::Release);
        results.set_loaded(false);
        drop(bound);
        drop(state);
        drop(results);
        self.initialized_notify.notify_waiters();
        self.durable_notify.notify_waiters();
    }

    pub(in crate::livequery) async fn install_remote(
        &self,
        version: u32,
        entities: Vec<Entity>,
        membership: crate::reactor::RemoteMembership,
        changes: Vec<crate::changes::EntityChange>,
        origin: &proto::RequestId,
        canceled: &(dyn Fn() -> bool + Send + Sync),
        complete: bool,
    ) -> Result<Option<std::collections::HashSet<proto::EntityId>>, RetrievalError> {
        let Some((selection, stored_version)) = self.selection.value() else { return Ok(None) };
        if version != stored_version || canceled() || !self.remote_stream_current(version, origin) {
            return Ok(None);
        }
        let source = {
            let stream = self.remote_stream.lock().unwrap();
            let Some(stream) = stream.as_ref().filter(|stream| stream.version == version && &stream.origin == origin) else {
                return Ok(None);
            };
            Arc::new(RemoteQuerySource {
                origin: crate::reactor::QueryOrigin { query: self.query_id, version, request: origin.clone() },
                context: self.context.clone(),
                admitted: stream.admitted.clone(),
                decides_membership: stream.decides_membership.clone(),
            })
        };
        let notified = self
            .context
            .node()?
            .reactor()
            .install_remote_query(
                self.subscription.id(),
                self.query_id,
                selection,
                source.clone(),
                self.resultset.clone(),
                source,
                version,
                RemoteActivation { query: self, origin, canceled, complete },
                entities,
                membership,
                changes
                    .into_iter()
                    .map(|change| {
                        let (entity, events) = change.into_parts();
                        crate::reactor::ReactorUpdateItem { entity, events, predicate_relevance: vec![] }
                    })
                    .collect(),
            )
            .await?;
        if canceled() || !self.remote_stream_current(version, origin) {
            return Ok(None);
        }
        Ok(Some(notified))
    }

    /// Stop the stream bound for `version` deciding membership, because that version failed.
    pub(super) fn release_failed_stream(&self, version: u32) {
        if let Some(stream) = self.remote_stream.lock().unwrap().as_ref().filter(|stream| stream.version == version) {
            stream.decides_membership.store(false, Ordering::Release);
        }
    }

    /// Stop the bound stream deciding membership, because a newer selection version replaces it.
    pub(super) fn release_replaced_stream(&self) {
        if let Some(stream) = self.remote_stream.lock().unwrap().as_ref() {
            stream.decides_membership.store(false, Ordering::Release);
        }
    }

    /// Whether `version`'s stream has installed its membership, which a cached local activation
    /// must not replace.
    pub(super) fn remote_installed(&self, version: u32) -> bool {
        self.remote_stream.lock().unwrap().as_ref().is_some_and(|stream| stream.version == version && stream.installed)
    }
}

/// The local source of a query whose membership a remote stream decides. While the stream
/// decides, local data enters the query only for the ids the stream admitted, so a local scan
/// can drop a row the peer selected but cannot add one it did not.
struct RemoteQuerySource {
    origin: crate::reactor::QueryOrigin,
    context: Arc<dyn DynContextInner>,
    admitted: Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>,
    decides_membership: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait::async_trait]
impl crate::reactor::LocalEntitySource for RemoteQuerySource {
    fn query_origin(&self) -> Option<&crate::reactor::QueryOrigin> {
        self.decides_membership.load(Ordering::Acquire).then_some(&self.origin)
    }
    fn can_read(&self, entity: &Entity) -> bool { self.context.can_read(entity) }
    fn admits_query_entity(&self, entity: &Entity) -> bool {
        !self.decides_membership.load(Ordering::Acquire) || self.admitted.read().unwrap().contains(&entity.id())
    }
    async fn fetch_entities_from_local(&self, selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        Ok(self.context.fetch_entities_from_local(selection).await?.into_iter().filter(|entity| self.admits_query_entity(entity)).collect())
    }
}
#[async_trait::async_trait]
impl GapFetcher<Entity> for RemoteQuerySource {
    async fn fetch_gap(
        &self,
        selection: &ankql::ast::Selection<Resolved>,
        last: Option<&Entity>,
        size: usize,
    ) -> Result<Vec<Entity>, RetrievalError> {
        if !self.decides_membership.load(Ordering::Acquire) {
            return QueryGapFetcher::new(Context(self.context.clone())).fetch_gap(selection, last, size).await;
        }
        // The stream's publications refill a remote LIMIT window; local storage can hold
        // predicate matches outside it.
        Ok(Vec::new())
    }
}

/// Publishes a remote install's readiness to its live query, unless the stream was superseded.
struct RemoteActivation<'a> {
    query: &'a LiveQueryInner,
    origin: &'a proto::RequestId,
    canceled: &'a (dyn Fn() -> bool + Send + Sync),
    complete: bool,
}

impl crate::reactor::PreNotifyHook for RemoteActivation<'_> {
    fn is_current(&self, version: u32) -> bool { !(self.canceled)() && self.query.remote_stream_current(version, self.origin) }

    fn marks_loaded(&self) -> bool { self.complete }

    fn pre_notify(&self, version: u32) {
        let state = self.query.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut stream = self.query.remote_stream.lock().unwrap();
        let Some(current) = stream.as_mut().filter(|stream| stream.version == version && &stream.origin == self.origin) else { return };
        if (self.canceled)() || !self.query.is_current(version) || !current.decides_membership.load(Ordering::Acquire) {
            return;
        }
        current.installed = true;
        if !self.complete {
            return;
        }
        current.activated = true;
        self.query.remote_activated_version.store(version, Ordering::Release);
        self.query.initialized_version.fetch_max(version, Ordering::AcqRel);
        drop(stream);
        if self.query.error.with(Option::is_some) {
            self.query.error.set_before_notify(None, || drop(state));
        } else {
            drop(state);
        }
        self.query.initialized_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests;
