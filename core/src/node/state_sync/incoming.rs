//! Route pushed frames without making the connector wait for entity application.
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use ankurah_proto::{self as proto, RequestId, StateFrame};
use anyhow::ensure;
use futures::FutureExt;

use super::{CONTROL_BYTES, FRAME_BYTES, MAX_INFLIGHT_FRAMES, WINDOW_BYTES};

type AbandonStream = Arc<dyn Fn(proto::StateStreamId) + Send + Sync>;

#[derive(Default)]
pub(crate) struct IncomingStates(Mutex<Registry>);
#[derive(Default)]
struct Registry {
    streams: HashMap<RequestId, Registered>,
    retired: VecDeque<(RequestId, AbandonStream)>,
}
struct Registered {
    query: Option<(proto::QueryId, u32)>,
    inbox: Arc<Inbox>,
    abandon: Option<AbandonStream>,
    comparison: Option<proto::StateStreamId>,
    deferred_comparison: Option<proto::StateStreamId>,
}
impl Registry {
    fn remove(&mut self, request: &RequestId) -> Option<(AbandonStream, proto::StateStreamId)> {
        if let Some(stream) = self.streams.remove(request) {
            stream.inbox.close();
            // Bound queries belong to replacement/unsubscribe. A finite Fetch
            // has no such owner: dropping it must abandon its remote stream too.
            if let Some(handler) = stream.abandon.filter(|_| stream.query.is_none() || stream.comparison.is_none()) {
                if let Some(comparison) = stream.comparison.or(stream.deferred_comparison) {
                    return Some((handler, comparison));
                }
                self.retired.push_back((request.clone(), handler));
                if self.retired.len() > 128 {
                    self.retired.pop_front();
                }
            }
        }
        None
    }
    fn cancel_query(&mut self, query: proto::QueryId) -> Vec<(AbandonStream, proto::StateStreamId)> {
        let origins: Vec<_> =
            self.streams.iter().filter(|(_, stream)| stream.query.is_some_and(|(id, _)| id == query)).map(|(id, _)| id.clone()).collect();
        origins.into_iter().filter_map(|origin| self.remove(&origin)).collect()
    }
}

pub(crate) struct IncomingStream {
    pub request: RequestId,
    pub query: Option<(proto::QueryId, u32)>,
    pub inbox: Arc<Inbox>,
    owner: Weak<IncomingStates>,
    comparison: Option<(proto::StateStreamId, Arc<crate::util::safemap::SafeMap<proto::StateStreamId, bool>>)>,
}

impl IncomingStream {
    /// Cleanup was sent or completion confirmed; dropping this owner needs no further abandonment.
    pub fn disarm_abandonment(&self) {
        if let Some(owner) = self.owner.upgrade() {
            if let Some(stream) = owner.0.lock().unwrap().streams.get_mut(&self.request) {
                stream.abandon = None;
            }
        }
    }

    pub fn bind(
        &mut self,
        id: proto::StateStreamId,
        catalog: bool,
        comparisons: Arc<crate::util::safemap::SafeMap<proto::StateStreamId, bool>>,
    ) {
        if let Some(owner) = self.owner.upgrade() {
            let mut registry = owner.0.lock().unwrap();
            if let Some(stream) = registry.streams.get_mut(&self.request) {
                if stream.deferred_comparison.is_some_and(|previous| previous != id) {
                    stream.inbox.close();
                }
                stream.comparison = Some(id);
                stream.deferred_comparison = None;
            }
            // The caller consumed this metadata even if cancellation removed
            // the origin first. Its subscription owner now handles cleanup.
            registry.retired.retain(|(request, _)| request != &self.request);
        }
        comparisons.insert(id, catalog);
        self.comparison = Some((id, comparisons));
    }
}

impl Drop for IncomingStream {
    fn drop(&mut self) {
        if let Some((id, comparisons)) = &self.comparison {
            comparisons.remove(id);
        }
        if let Some(owner) = self.owner.upgrade() {
            owner.remove(&self.request);
        }
    }
}

#[derive(Default)]
pub(crate) struct Inbox {
    state: Mutex<Pending>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct Pending {
    comparison: Option<proto::StateStreamId>,
    next: u64,
    frames: BTreeMap<u64, (StateFrame, Vec<u8>)>,
    bytes: usize,
    closed: bool,
}

impl IncomingStates {
    pub fn register(
        self: &Arc<Self>,
        request: RequestId,
        query: Option<(proto::QueryId, u32)>,
        abandon: Option<AbandonStream>,
    ) -> anyhow::Result<IncomingStream> {
        let mut registry = self.0.lock().unwrap();
        ensure!(!registry.streams.contains_key(&request), "state request already registered");
        let cleanup = query.map_or_else(Vec::new, |(query, _)| registry.cancel_query(query));
        ensure!(registry.streams.len() < 128, "too many incoming state streams");
        let inbox = Arc::new(Inbox::default());
        registry
            .streams
            .insert(request.clone(), Registered { query, inbox: inbox.clone(), abandon, comparison: None, deferred_comparison: None });
        drop(registry);
        for (handler, comparison) in cleanup {
            handler(comparison);
        }
        Ok(IncomingStream { request, query, inbox, owner: Arc::downgrade(self), comparison: None })
    }

    pub fn route(&self, request: &RequestId, frame: StateFrame) -> anyhow::Result<()> {
        let inbox = self.0.lock().unwrap().streams.get(request).map(|stream| stream.inbox.clone());
        let Some(inbox) = inbox else {
            self.observe_comparison(request, frame.comparison);
            return Ok(());
        };
        if frame.sequence == 0 {
            inbox.close();
            anyhow::bail!("frame zero belongs in the initial response");
        }
        let result = inbox.insert(frame);
        if result.is_err() {
            inbox.close();
        }
        result
    }

    /// Remember the remote handle even when cancellation races with its delivery.
    pub fn observe_comparison(&self, request: &RequestId, comparison: proto::StateStreamId) {
        let handler = {
            let mut registry = self.0.lock().unwrap();
            if let Some(stream) = registry.streams.get_mut(request) {
                let closed = stream.inbox.state.lock().unwrap().closed;
                if let Some(bound) = stream.comparison {
                    if bound != comparison {
                        stream.inbox.close();
                    }
                    None
                } else if closed {
                    stream.abandon.take()
                } else {
                    // The first response may have reached its waiter without
                    // being consumed yet. Bind disarms a duplicate; drop
                    // abandons metadata whose receiver was canceled.
                    if stream.deferred_comparison.is_some_and(|previous| previous != comparison) {
                        stream.inbox.close();
                    } else {
                        stream.deferred_comparison = Some(comparison);
                    }
                    None
                }
            } else {
                registry
                    .retired
                    .iter()
                    .position(|(id, _)| id == request)
                    .and_then(|index| registry.retired.remove(index).map(|(_, handler)| handler))
            }
        };
        if let Some(handler) = handler {
            handler(comparison);
        }
    }

    pub fn remove(&self, request: &RequestId) {
        let cleanup = self.0.lock().unwrap().remove(request);
        if let Some((handler, comparison)) = cleanup {
            handler(comparison);
        }
    }
    pub fn is_current(&self, request: &RequestId) -> bool { self.0.lock().unwrap().streams.contains_key(request) }
    pub fn cancel_query(&self, query: proto::QueryId) {
        let cleanup = self.0.lock().unwrap().cancel_query(query);
        for (handler, comparison) in cleanup {
            handler(comparison);
        }
    }
    pub fn close(&self) {
        let mut registry = self.0.lock().unwrap();
        for (_, stream) in registry.streams.drain() {
            stream.inbox.close();
        }
        registry.retired.clear();
    }
}

impl Drop for IncomingStates {
    fn drop(&mut self) { self.close(); }
}

impl Inbox {
    pub fn insert(&self, frame: StateFrame) -> anyhow::Result<()> {
        let encoded = bincode::serialize(&frame)?;
        ensure!(encoded.len() <= FRAME_BYTES, "state frame exceeds limit");
        ensure!(
            encoded.len() as u64 + super::budget::frame_expansion(&frame)? <= FRAME_BYTES as u64,
            "state frame exceeds decoded-work limit"
        );
        let mut state = self.state.lock().unwrap();
        ensure!(!state.closed, "state stream closed");
        ensure!(state.comparison.is_none_or(|id| id == frame.comparison), "state stream changed its comparison");
        state.comparison = Some(frame.comparison);
        if frame.sequence < state.next {
            return Ok(());
        }
        if let Some((_, previous)) = state.frames.get(&frame.sequence) {
            ensure!(previous == &encoded, "conflicting repeated state frame");
            return Ok(());
        }
        ensure!(state.frames.len() < MAX_INFLIGHT_FRAMES, "too many out-of-order frames");
        ensure!(state.bytes + encoded.len() <= WINDOW_BYTES + CONTROL_BYTES, "incoming state window exceeded");
        state.bytes += encoded.len();
        state.frames.insert(frame.sequence, (frame, encoded));
        drop(state);
        self.changed.notify_one();
        Ok(())
    }

    pub fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.changed.notify_waiters();
    }

    pub async fn next(&self, live: bool) -> anyhow::Result<StateFrame> {
        let mut stalled_since = None;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let waiting_for_gap = {
                let mut state = self.state.lock().unwrap();
                if state.closed {
                    return Err(crate::error::RequestError::ConnectionLost.into());
                }
                let next = state.next;
                if let Some((frame, encoded)) = state.frames.remove(&next) {
                    state.bytes -= encoded.len();
                    state.next = next.checked_add(1).ok_or_else(|| anyhow::anyhow!("state sequence overflow"))?;
                    return Ok(frame);
                }
                !state.frames.is_empty()
            };
            if live && !waiting_for_gap {
                notified.await;
                continue;
            }
            let started = stalled_since.get_or_insert_with(web_time::Instant::now);
            let remaining = Duration::from_secs(120).saturating_sub(started.elapsed());
            let timeout = futures_timer::Delay::new(remaining).fuse();
            futures::pin_mut!(timeout);
            futures::select! {
                _ = notified.fuse() => {},
                _ = timeout => return Err(crate::error::RequestError::ConnectionLost.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(comparison: proto::StateStreamId, sequence: u64) -> StateFrame {
        StateFrame {
            comparison,
            sequence,
            body: proto::StateFrameBody::State { round: 0, items: Vec::new(), progress: proto::ComparisonProgress::Continue },
        }
    }

    #[tokio::test]
    async fn early_reordered_frames_wait_for_metadata_and_duplicates_do_not_reapply() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let stream = registry.register(RequestId::new(), None, None)?;
        let id = proto::StateStreamId::new();
        registry.route(&stream.request, frame(id, 2))?;
        registry.route(&stream.request, frame(id, 1))?;
        registry.route(&stream.request, frame(id, 1))?;
        assert!(tokio::time::timeout(Duration::from_millis(10), stream.inbox.next(false)).await.is_err());
        stream.inbox.insert(frame(id, 0))?;
        for sequence in 0..3 {
            assert_eq!(stream.inbox.next(false).await?.sequence, sequence);
        }
        registry.route(&stream.request, frame(id, 1))?;
        assert!(stream.inbox.state.lock().unwrap().frames.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn replacement_and_disconnect_wake_waiters_without_canceling_the_replacement() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let query = proto::QueryId::new();
        let first = registry.register(RequestId::new(), Some((query, 1)), None)?;
        let next = registry.register(RequestId::new(), Some((query, 2)), None)?;
        assert!(first.inbox.next(true).await.is_err());
        drop(first);
        assert!(registry.is_current(&next.request));
        registry.close();
        assert!(next.inbox.next(true).await.is_err());
        assert!(!registry.is_current(&next.request));
        Ok(())
    }

    #[tokio::test]
    async fn changed_comparison_conflicting_duplicates_and_overflow_close_the_stream() -> anyhow::Result<()> {
        for attack in 0..4 {
            let registry = Arc::new(IncomingStates::default());
            let stream = registry.register(RequestId::new(), None, None)?;
            let id = proto::StateStreamId::new();
            registry.route(&stream.request, frame(id, 1))?;
            let result = match attack {
                0 => registry.route(&stream.request, frame(proto::StateStreamId::new(), 2)),
                1 => {
                    let mut changed = frame(id, 1);
                    changed.body = proto::StateFrameBody::RestartRequired;
                    registry.route(&stream.request, changed)
                }
                2 => {
                    for sequence in 2..=MAX_INFLIGHT_FRAMES {
                        registry.route(&stream.request, frame(id, sequence as u64))?;
                    }
                    registry.route(&stream.request, frame(id, MAX_INFLIGHT_FRAMES as u64 + 1))
                }
                _ => registry.route(&stream.request, frame(id, 0)),
            };
            assert!(result.is_err());
            assert!(stream.inbox.next(false).await.is_err());
        }
        Ok(())
    }
    #[test]
    fn late_metadata_and_frames_cancel_only_bounded_known_origins() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let handler: AbandonStream = {
            let cancelled = cancelled.clone();
            Arc::new(move |id| cancelled.lock().unwrap().push(id))
        };
        let stream = registry.register(RequestId::new(), None, Some(handler.clone()))?;
        let request = stream.request.clone();
        drop(stream);
        let comparison = proto::StateStreamId::new();
        registry.route(&request, frame(comparison, 1))?;
        registry.observe_comparison(&request, comparison);
        registry.observe_comparison(&RequestId::new(), proto::StateStreamId::new());
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        for _ in 0..256 {
            drop(registry.register(RequestId::new(), None, Some(handler.clone()))?);
        }
        assert_eq!(registry.0.lock().unwrap().retired.len(), 128);
        Ok(())
    }

    #[test]
    fn canceled_bound_query_ignores_late_frames_and_releases_its_catalog_entry() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let query = proto::QueryId::new();
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let handler: AbandonStream = {
            let cancelled = cancelled.clone();
            Arc::new(move |id| cancelled.lock().unwrap().push(id))
        };
        let mut stream = registry.register(RequestId::new(), Some((query, 1)), Some(handler))?;
        let request = stream.request.clone();
        let comparison = proto::StateStreamId::new();
        let comparisons = Arc::new(crate::util::safemap::SafeMap::new());
        stream.bind(comparison, true, comparisons.clone());
        registry.cancel_query(query);
        assert!(!registry.is_current(&request));
        assert!(stream.inbox.state.lock().unwrap().closed);
        registry.route(&request, frame(comparison, 1))?;
        registry.observe_comparison(&request, comparison);
        assert!(cancelled.lock().unwrap().is_empty(), "late traffic from a replaced bound stream cannot abandon the standing query");
        drop(stream);
        assert!(comparisons.get(&comparison).is_none());
        assert!(registry.0.lock().unwrap().retired.is_empty());
        Ok(())
    }

    #[test]
    fn consuming_canceled_metadata_disarms_its_unbound_orphan_handler() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let query = proto::QueryId::new();
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let handler: AbandonStream = {
            let cancelled = cancelled.clone();
            Arc::new(move |id| cancelled.lock().unwrap().push(id))
        };
        let mut stream = registry.register(RequestId::new(), Some((query, 1)), Some(handler))?;
        registry.cancel_query(query);
        assert_eq!(registry.0.lock().unwrap().retired.len(), 1, "unconsumed metadata still needs orphan cleanup");
        let comparison = proto::StateStreamId::new();
        stream.bind(comparison, false, Arc::new(crate::util::safemap::SafeMap::new()));
        registry.route(&stream.request, frame(comparison, 1))?;
        registry.observe_comparison(&stream.request, comparison);
        assert!(cancelled.lock().unwrap().is_empty(), "consumed metadata belongs to the superseding subscription's cleanup");
        assert!(registry.0.lock().unwrap().retired.is_empty());
        Ok(())
    }

    #[test]
    fn duplicate_active_metadata_before_or_after_bind_does_not_abandon_the_stream() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let handler: AbandonStream = {
            let cancelled = cancelled.clone();
            Arc::new(move |id| cancelled.lock().unwrap().push(id))
        };
        let mut stream = registry.register(RequestId::new(), None, Some(handler))?;
        let comparison = proto::StateStreamId::new();
        registry.observe_comparison(&stream.request, comparison);
        assert!(cancelled.lock().unwrap().is_empty());
        stream.bind(comparison, false, Arc::new(crate::util::safemap::SafeMap::new()));
        registry.observe_comparison(&stream.request, comparison);
        assert!(cancelled.lock().unwrap().is_empty());
        assert!(!stream.inbox.state.lock().unwrap().closed);
        assert!(registry.is_current(&stream.request));
        registry.observe_comparison(&stream.request, proto::StateStreamId::new());
        assert!(stream.inbox.state.lock().unwrap().closed, "changed comparison must fail the active stream");
        assert!(cancelled.lock().unwrap().is_empty(), "a conflicting duplicate cannot abandon an unrelated comparison");
        Ok(())
    }

    #[test]
    fn unconsumed_late_metadata_is_abandoned_on_drop_without_holding_the_registry_lock() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let request = RequestId::new();
        let handler: AbandonStream = {
            let registry = Arc::downgrade(&registry);
            let cancelled = cancelled.clone();
            let request = request.clone();
            Arc::new(move |id| {
                assert!(!registry.upgrade().unwrap().is_current(&request));
                cancelled.lock().unwrap().push(id);
            })
        };
        let stream = registry.register(request.clone(), None, Some(handler))?;
        let comparison = proto::StateStreamId::new();
        registry.observe_comparison(&request, comparison);
        assert!(cancelled.lock().unwrap().is_empty());
        drop(stream);
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        registry.observe_comparison(&request, comparison);
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        Ok(())
    }

    #[test]
    fn replacement_cleans_up_deferred_metadata_without_touching_its_new_origin() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let query = proto::QueryId::new();
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let replacement = RequestId::new();
        let handler: AbandonStream = {
            let registry = Arc::downgrade(&registry);
            let cancelled = cancelled.clone();
            let replacement = replacement.clone();
            Arc::new(move |id| {
                assert!(registry.upgrade().unwrap().is_current(&replacement));
                cancelled.lock().unwrap().push(id);
            })
        };
        let first = registry.register(RequestId::new(), Some((query, 1)), Some(handler))?;
        let comparison = proto::StateStreamId::new();
        registry.observe_comparison(&first.request, comparison);
        let next = registry.register(replacement, Some((query, 2)), None)?;
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        drop(first);
        assert!(registry.is_current(&next.request));
        assert!(!next.inbox.state.lock().unwrap().closed);
        Ok(())
    }
}
