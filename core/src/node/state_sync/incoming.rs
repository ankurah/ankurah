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

type LateReply = Arc<dyn Fn(proto::ComparisonId) + Send + Sync>;

#[derive(Default)]
pub(crate) struct IncomingStates(Mutex<Registry>);
#[derive(Default)]
struct Registry {
    streams: HashMap<RequestId, Registered>,
    retired: VecDeque<(RequestId, LateReply)>,
}
struct Registered {
    query: Option<(proto::QueryId, u32)>,
    inbox: Arc<Inbox>,
    late_reply: Option<LateReply>,
    comparison: Option<proto::ComparisonId>,
    deferred_comparison: Option<proto::ComparisonId>,
}
impl Registry {
    fn remove(&mut self, request: &RequestId) -> Option<(LateReply, proto::ComparisonId)> {
        if let Some(stream) = self.streams.remove(request) {
            stream.inbox.close();
            if let Some(handler) = stream.late_reply {
                if let Some(comparison) = stream.deferred_comparison {
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
    fn cancel_query(&mut self, query: proto::QueryId) -> Vec<(LateReply, proto::ComparisonId)> {
        let origins: Vec<_> =
            self.streams.iter().filter(|(_, stream)| stream.query.is_some_and(|(id, _)| id == query)).map(|(id, _)| id.clone()).collect();
        origins.into_iter().filter_map(|origin| self.remove(&origin)).collect()
    }
}

pub(crate) struct IncomingStream {
    pub request: RequestId,
    pub inbox: Arc<Inbox>,
    owner: Weak<IncomingStates>,
    comparison: Option<(proto::ComparisonId, Arc<crate::util::safemap::SafeMap<proto::ComparisonId, bool>>)>,
}

impl IncomingStream {
    pub fn bind(
        &mut self,
        id: proto::ComparisonId,
        catalog: bool,
        comparisons: Arc<crate::util::safemap::SafeMap<proto::ComparisonId, bool>>,
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
    comparison: Option<proto::ComparisonId>,
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
        late_reply: Option<LateReply>,
    ) -> anyhow::Result<IncomingStream> {
        let mut registry = self.0.lock().unwrap();
        ensure!(!registry.streams.contains_key(&request), "state request already registered");
        let cleanup = query.map_or_else(Vec::new, |(query, _)| registry.cancel_query(query));
        ensure!(registry.streams.len() < 128, "too many incoming state streams");
        let inbox = Arc::new(Inbox::default());
        registry
            .streams
            .insert(request.clone(), Registered { query, inbox: inbox.clone(), late_reply, comparison: None, deferred_comparison: None });
        drop(registry);
        for (handler, comparison) in cleanup {
            handler(comparison);
        }
        Ok(IncomingStream { request, inbox, owner: Arc::downgrade(self), comparison: None })
    }

    pub fn route(&self, request: &RequestId, frame: StateFrame) -> anyhow::Result<()> {
        let inbox = self.0.lock().unwrap().streams.get(request).map(|stream| stream.inbox.clone());
        let Some(inbox) = inbox else {
            self.late_response(request, frame.comparison);
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

    /// A canceled request can still reveal its responder-issued id later.
    pub fn late_response(&self, request: &RequestId, comparison: proto::ComparisonId) {
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
                    stream.late_reply.take()
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

    fn frame(comparison: proto::ComparisonId, sequence: u64) -> StateFrame {
        StateFrame {
            comparison,
            sequence,
            body: proto::StateFrameBody::State { items: Vec::new(), progress: proto::ComparisonProgress::Continue },
        }
    }

    #[tokio::test]
    async fn early_reordered_frames_wait_for_metadata_and_duplicates_do_not_reapply() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let stream = registry.register(RequestId::new(), None, None)?;
        let id = proto::ComparisonId::new();
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
            let id = proto::ComparisonId::new();
            registry.route(&stream.request, frame(id, 1))?;
            let result = match attack {
                0 => registry.route(&stream.request, frame(proto::ComparisonId::new(), 2)),
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
        let handler: LateReply = {
            let cancelled = cancelled.clone();
            Arc::new(move |id| cancelled.lock().unwrap().push(id))
        };
        let stream = registry.register(RequestId::new(), None, Some(handler.clone()))?;
        let request = stream.request.clone();
        drop(stream);
        let comparison = proto::ComparisonId::new();
        registry.route(&request, frame(comparison, 1))?;
        registry.late_response(&request, comparison);
        registry.late_response(&RequestId::new(), proto::ComparisonId::new());
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        for _ in 0..256 {
            drop(registry.register(RequestId::new(), None, Some(handler.clone()))?);
        }
        assert_eq!(registry.0.lock().unwrap().retired.len(), 128);
        Ok(())
    }

    #[test]
    fn duplicate_active_metadata_before_or_after_bind_does_not_abandon_the_stream() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let handler: LateReply = {
            let cancelled = cancelled.clone();
            Arc::new(move |id| cancelled.lock().unwrap().push(id))
        };
        let mut stream = registry.register(RequestId::new(), None, Some(handler))?;
        let comparison = proto::ComparisonId::new();
        registry.late_response(&stream.request, comparison);
        assert!(cancelled.lock().unwrap().is_empty());
        stream.bind(comparison, false, Arc::new(crate::util::safemap::SafeMap::new()));
        registry.late_response(&stream.request, comparison);
        assert!(cancelled.lock().unwrap().is_empty());
        assert!(!stream.inbox.state.lock().unwrap().closed);
        assert!(registry.is_current(&stream.request));
        registry.late_response(&stream.request, proto::ComparisonId::new());
        assert!(stream.inbox.state.lock().unwrap().closed, "changed comparison must fail the active stream");
        assert!(cancelled.lock().unwrap().is_empty(), "a conflicting duplicate cannot abandon an unrelated comparison");
        Ok(())
    }

    #[test]
    fn unconsumed_late_metadata_is_abandoned_on_drop_without_holding_the_registry_lock() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let request = RequestId::new();
        let handler: LateReply = {
            let registry = Arc::downgrade(&registry);
            let cancelled = cancelled.clone();
            let request = request.clone();
            Arc::new(move |id| {
                assert!(!registry.upgrade().unwrap().is_current(&request));
                cancelled.lock().unwrap().push(id);
            })
        };
        let stream = registry.register(request.clone(), None, Some(handler))?;
        let comparison = proto::ComparisonId::new();
        registry.late_response(&request, comparison);
        assert!(cancelled.lock().unwrap().is_empty());
        drop(stream);
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        registry.late_response(&request, comparison);
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        Ok(())
    }

    #[test]
    fn replacement_cleans_up_deferred_metadata_without_touching_its_new_origin() -> anyhow::Result<()> {
        let registry = Arc::new(IncomingStates::default());
        let query = proto::QueryId::new();
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let replacement = RequestId::new();
        let handler: LateReply = {
            let registry = Arc::downgrade(&registry);
            let cancelled = cancelled.clone();
            let replacement = replacement.clone();
            Arc::new(move |id| {
                assert!(registry.upgrade().unwrap().is_current(&replacement));
                cancelled.lock().unwrap().push(id);
            })
        };
        let first = registry.register(RequestId::new(), Some((query, 1)), Some(handler))?;
        let comparison = proto::ComparisonId::new();
        registry.late_response(&first.request, comparison);
        let next = registry.register(replacement, Some((query, 2)), None)?;
        assert_eq!(*cancelled.lock().unwrap(), vec![comparison]);
        drop(first);
        assert!(registry.is_current(&next.request));
        assert!(!next.inbox.state.lock().unwrap().closed);
        Ok(())
    }
}
