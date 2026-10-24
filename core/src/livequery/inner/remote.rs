//! Binding a live query to a remote peer's state stream: which stream currently decides the
//! query's membership, the ids that stream admitted, and installing what it sends.

use super::LiveQueryInner;
use crate::context::{Context, DynContextInner};
use crate::internal::prelude::*;
use crate::reactor::fetch_gap::{GapFetcher, QueryGapFetcher};
use ankql::ast::Resolved;
use std::sync::{atomic::Ordering, Arc};

pub(super) struct RemoteStream {
    version: u32,
    origin: proto::RequestId,
    activated: bool,
    installed: bool,
    admission: Arc<std::sync::atomic::AtomicBool>,
    candidates: Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>,
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
                previous.admission.store(false, Ordering::Release);
            }
            *stream = Some(RemoteStream {
                version,
                origin,
                activated: false,
                installed: false,
                admission: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                candidates: Arc::new(std::sync::RwLock::new(results.iter_entities().map(|(id, _)| id).collect())),
                refresh: None,
            });
        }
        drop(stream);
        drop(state);
        drop(results);
    }

    pub(in crate::livequery) fn set_candidates(
        &self,
        version: u32,
        origin: &proto::RequestId,
        ids: impl IntoIterator<Item = proto::EntityId>,
    ) -> bool {
        let stream = self.remote_stream.lock().unwrap();
        let Some(stream) = stream.as_ref().filter(|stream| stream.version == version && &stream.origin == origin) else { return false };
        *stream.candidates.write().unwrap() = ids.into_iter().collect();
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
            && self
                .remote_stream
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|stream| stream.version == version && &stream.origin == origin && stream.admission.load(Ordering::Acquire))
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
        stream.admission.store(false, Ordering::Release);
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
        membership: crate::reactor::PeerQueryMembership,
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
            Arc::new(PeerQuerySource {
                origin: crate::reactor::QueryOrigin { query: self.query_id, version, request: origin.clone() },
                context: self.context.clone(),
                candidates: stream.candidates.clone(),
                admission: stream.admission.clone(),
            })
        };
        let notified = self
            .context
            .node()?
            .reactor()
            .upsert_query_from_peer(
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
            stream.admission.store(false, Ordering::Release);
        }
    }

    /// Stop the bound stream deciding membership, because a newer selection version replaces it.
    pub(super) fn release_replaced_stream(&self) {
        if let Some(stream) = self.remote_stream.lock().unwrap().as_ref() {
            stream.admission.store(false, Ordering::Release);
        }
    }

    /// Whether `version`'s stream has installed its membership, which a cached local activation
    /// must not replace.
    pub(super) fn remote_installed(&self, version: u32) -> bool {
        self.remote_stream.lock().unwrap().as_ref().is_some_and(|stream| stream.version == version && stream.installed)
    }
}

/// The source owns the remote candidate window. Local merge/policy evaluation
/// may remove candidates, but a local cache scan cannot invent a source winner.
struct PeerQuerySource {
    origin: crate::reactor::QueryOrigin,
    context: Arc<dyn DynContextInner>,
    candidates: Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>,
    admission: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait::async_trait]
impl crate::reactor::LocalEntitySource for PeerQuerySource {
    fn query_origin(&self) -> Option<&crate::reactor::QueryOrigin> { self.admission.load(Ordering::Acquire).then_some(&self.origin) }
    fn can_read(&self, entity: &Entity) -> bool { self.context.can_read(entity) }
    fn admits_query_entity(&self, entity: &Entity) -> bool {
        !self.admission.load(Ordering::Acquire) || self.candidates.read().unwrap().contains(&entity.id())
    }
    async fn fetch_entities_from_local(&self, selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        Ok(self.context.fetch_entities_from_local(selection).await?.into_iter().filter(|entity| self.admits_query_entity(entity)).collect())
    }
}
#[async_trait::async_trait]
impl GapFetcher<Entity> for PeerQuerySource {
    async fn fetch_gap(
        &self,
        selection: &ankql::ast::Selection<Resolved>,
        last: Option<&Entity>,
        size: usize,
    ) -> Result<Vec<Entity>, RetrievalError> {
        if !self.admission.load(Ordering::Acquire) {
            return QueryGapFetcher::new(Context(self.context.clone())).fetch_gap(selection, last, size).await;
        }
        // A remote LIMIT window is refilled by coherent source publications.
        // Local storage can contain predicate matches outside that window.
        Ok(Vec::new())
    }
}

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
        if (self.canceled)() || !self.query.is_current(version) || !current.admission.load(Ordering::Acquire) {
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
