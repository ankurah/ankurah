use super::{
    Snapshot, CONTROL_BYTES, FRAME_BYTES, LEAF_PAGE, MAX_INFLIGHT_FRAMES, MAX_REPAIR_BYTES, PAGE_BYTES, PAGE_STEPS, PAGE_WORK, WINDOW_BYTES,
};
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_digest_tree::{leaf_address, CanonicalCodec, KeyCodec, NodePrefix};
use ankurah_proto::{self as proto, AddressRange, ComparisonId, ComparisonProgress, StateAnswer, StateFrame, StateFrameBody, StateItem};
use anyhow::{bail, ensure};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use web_time::Instant;

const COMPARISON_TTL: Duration = Duration::from_secs(120);
const MAX_COMPARISONS: usize = 128;
const MAX_FEEDBACK: usize = 256;
const MAX_OFFERED_TARGETS: usize = 100_000;
const MAX_ORIGINS: usize = 100_000;
const MAX_FEEDBACK_IDS: usize = 100_000;
const MAX_HISTORY: usize = 4096;
const MAX_LIVE_BYTES: usize = MAX_REPAIR_BYTES + WINDOW_BYTES;
const MAX_LIVE_ITEMS: usize = 4096;
#[cfg(test)]
mod tests;

pub(crate) struct Comparisons<CD: ContextData> {
    sessions: Mutex<HashMap<ComparisonId, Arc<Session<CD>>>>,
    closed: Mutex<VecDeque<ComparisonId>>,
    retired: Mutex<VecDeque<RetiredOrigin<CD>>>,
    origins: Mutex<HashMap<proto::RequestId, ComparisonId>>,
}
struct RetiredOrigin<CD> {
    request: proto::RequestId,
    id: ComparisonId,
    fingerprint: Vec<u8>,
    credentials: Vec<CD>,
    last_sent: u64,
    catalog: bool,
}
impl<CD: ContextData> Default for Comparisons<CD> {
    fn default() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            closed: Mutex::new(VecDeque::new()),
            retired: Mutex::new(VecDeque::new()),
            origins: Mutex::new(HashMap::new()),
        }
    }
}
pub(crate) struct Session<CD: ContextData> {
    pub catalog: bool,
    credentials: Vec<CD>,
    pub query: Option<(proto::QueryId, u32)>,
    initial_request: proto::RequestId,
    initial: Mutex<Option<Arc<StateFrame>>>,
    sent: AtomicU64,
    spec: proto::TreeSpec,
    scope: Vec<AddressRange>,
    fingerprint: Vec<u8>,
    state: tokio::sync::Mutex<Stream>,
    live: Mutex<LiveQueue>,
    pumping: AtomicBool,
    wake: AtomicBool,
    _cancel_expiry: futures::channel::oneshot::Sender<()>,
}
#[derive(Default)]
struct LiveQueue {
    items: VecDeque<proto::SubscriptionUpdateItem>,
    bytes: usize,
    stopped: bool,
}
struct Retained {
    frame: Arc<StateFrame>,
    bytes: usize,
}
struct LiveAssembly {
    entity_id: proto::EntityId,
    bytes: Vec<u8>,
    offset: usize,
    relevance: Vec<(proto::QueryId, proto::MembershipChange)>,
}
struct Stream {
    comparison: Option<Comparison>,
    retained: VecDeque<Retained>,
    bytes: usize,
    last_sent: u64,
    processed: Option<u64>,
    required_through: u64,
    applied: bool,
    failed: bool,
    last_activity: Instant,
    feedback: VecDeque<(proto::RequestId, Vec<u8>)>,
    feedback_bytes: usize,
    retired_feedback: HashSet<proto::RequestId>,
    live_chunk: Option<LiveAssembly>,
}
impl<CD: ContextData> Comparisons<CD> {
    pub fn get(&self, id: &ComparisonId) -> Option<Arc<Session<CD>>> { self.sessions.lock().unwrap().get(id).cloned() }
    pub fn remove(&self, id: &ComparisonId) -> Option<Arc<Session<CD>>> {
        let session = self.sessions.lock().unwrap().remove(id)?;
        session.live.lock().unwrap().stopped = true;
        session.initial.lock().unwrap().take();
        let mut retired = self.retired.lock().unwrap();
        retired.push_back(RetiredOrigin {
            request: session.initial_request.clone(),
            id: *id,
            fingerprint: session.fingerprint.clone(),
            credentials: session.credentials.clone(),
            last_sent: session.sent.load(Ordering::Acquire),
            catalog: session.catalog,
        });
        if retired.len() > MAX_COMPARISONS {
            retired.pop_front();
        }
        Some(session)
    }
    pub fn was_closed(&self, id: &ComparisonId) -> bool { self.closed.lock().unwrap().contains(id) }
    fn bypasses_policy(&self, id: &ComparisonId) -> bool {
        if let Some(session) = self.get(id) {
            return session.catalog;
        }
        self.retired.lock().unwrap().iter().find(|origin| &origin.id == id).is_some_and(|origin| origin.catalog)
    }
    pub fn finish(&self, id: ComparisonId) {
        self.remove(&id);
        let mut closed = self.closed.lock().unwrap();
        if !closed.contains(&id) {
            closed.push_back(id);
        }
        if closed.len() > MAX_COMPARISONS {
            closed.pop_front();
        }
    }
    pub fn invalidate_query(&self, query: proto::QueryId) {
        let ids = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(id, session)| session.query.is_some_and(|(q, _)| q == query).then_some(*id))
            .collect::<Vec<_>>();
        for id in ids {
            self.remove(&id);
        }
    }
    pub fn replay_initial(
        &self,
        request: &proto::RequestId,
        fingerprint: &[u8],
        credentials: &[CD],
    ) -> anyhow::Result<Option<proto::NodeResponseBody>> {
        let session = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .find_map(|(id, session)| (&session.initial_request == request).then_some((*id, session.clone())));
        if let Some((id, session)) = session {
            ensure!(session.fingerprint == fingerprint && session.permits(credentials), "initial retry changed its request");
            let initial = session.initial.lock().unwrap();
            let Some(initial) = initial.as_ref() else {
                return Ok(Some(proto::NodeResponseBody::ComparisonUnavailable { comparison: id }));
            };
            let reply = proto::StateReply { spec: session.spec.clone(), scope: session.scope.clone(), frame: (**initial).clone() };
            return Ok(Some(match session.query {
                Some((query_id, _)) => proto::NodeResponseBody::QuerySubscribed { query_id, state: reply },
                None => proto::NodeResponseBody::Fetch(reply),
            }));
        }
        if let Some(origin) = self.retired.lock().unwrap().iter().find(|origin| &origin.request == request) {
            ensure!(
                origin.fingerprint == fingerprint && same_credentials(&origin.credentials, credentials),
                "retired initial retry changed its request"
            );
            return Ok(Some(proto::NodeResponseBody::ComparisonUnavailable { comparison: origin.id }));
        }
        if let Some(id) = self.origins.lock().unwrap().get(request) {
            return Ok(Some(proto::NodeResponseBody::ComparisonUnavailable { comparison: *id }));
        }
        Ok(None)
    }
    fn permits_retired_receipt(&self, id: ComparisonId, processed: u64, credentials: &[CD]) -> bool {
        self.was_closed(&id)
            && self
                .retired
                .lock()
                .unwrap()
                .iter()
                .any(|origin| origin.id == id && processed <= origin.last_sent && same_credentials(&origin.credentials, credentials))
    }
}
fn same_credentials<CD: ContextData>(a: &[CD], b: &[CD]) -> bool { a.len() == b.len() && a.iter().all(|credential| b.contains(credential)) }
impl<CD: ContextData> Session<CD> {
    pub fn permits(&self, credentials: &[CD]) -> bool { same_credentials(&self.credentials, credentials) }
}
enum Work {
    Compare { head: proto::TreeHead, prefix: NodePrefix },
    Offer(proto::TreeHead),
    Expand { local: proto::TreeHead, known: proto::TreeHead },
    AskLeaves(AddressRange),
    SendLeaves(AddressRange),
    SendSnapshots(AddressRange),
    Item(StateItem),
    Repair(proto::EntityId, Option<proto::Clock>),
    History { entity_id: proto::EntityId, target: proto::Clock },
    Chunks { entity_id: proto::EntityId, target: proto::Clock, bytes: Vec<u8>, offset: usize },
}
struct Comparison {
    snapshot: Snapshot,
    work: VecDeque<Work>,
    summaries: HashMap<Vec<u8>, NodePrefix>,
    leaves: Vec<AddressRange>,
    heads: HashSet<proto::EntityId>,
    repaired: HashSet<proto::EntityId>,
    known_clocks: HashMap<proto::EntityId, proto::Clock>,
    offered: HashMap<proto::EntityId, proto::Clock>,
    pending_repairs: HashSet<proto::EntityId>,
    repair_attempts: HashMap<proto::EntityId, usize>,
    complete: bool,
    unavailable: bool,
}
fn range_id(range: &AddressRange) -> Vec<u8> { bincode::serialize(range).expect("address bounds serialize") }
impl Stream {
    fn acknowledge(&mut self, processed: u64) -> anyhow::Result<()> {
        ensure!(processed <= self.last_sent, "processed prefix exceeds sent frames");
        if self.processed.is_some_and(|previous| processed <= previous) {
            return Ok(());
        }
        while self.retained.front().is_some_and(|frame| frame.frame.sequence <= processed) {
            self.bytes -= self.retained.pop_front().unwrap().bytes;
        }
        self.processed = Some(processed);
        self.last_activity = Instant::now();
        Ok(())
    }
    fn room(&self, priority: bool) -> bool {
        self.retained.len() < MAX_INFLIGHT_FRAMES - if priority { 0 } else { 2 }
            && self.bytes + FRAME_BYTES <= WINDOW_BYTES + if priority { CONTROL_BYTES } else { 0 }
    }
    fn retain(&mut self, frame: StateFrame, priority: bool) -> anyhow::Result<()> {
        let bytes = bincode::serialized_size(&frame)? as usize;
        ensure!(bytes <= FRAME_BYTES, "state frame exceeds hard limit");
        ensure!(
            self.retained.len() < MAX_INFLIGHT_FRAMES && self.bytes + bytes <= WINDOW_BYTES + if priority { CONTROL_BYTES } else { 0 },
            "state replay window exhausted"
        );
        self.last_sent = frame.sequence;
        self.bytes += bytes;
        self.retained.push_back(Retained { frame: Arc::new(frame), bytes });
        Ok(())
    }
    fn next_live(&mut self, live: &Mutex<LiveQueue>) -> anyhow::Result<Option<StateFrameBody>> {
        if let Some(assembly) = self.live_chunk.as_mut() {
            let end = (assembly.offset + PAGE_BYTES / 2).min(assembly.bytes.len());
            let body = StateFrameBody::LiveChunk {
                chunk: proto::EntityDeltaChunk {
                    entity_id: assembly.entity_id,
                    offset: assembly.offset as u64,
                    bytes: assembly.bytes[assembly.offset..end].to_vec().into(),
                    last: end == assembly.bytes.len(),
                },
                predicate_relevance: assembly.relevance.clone(),
            };
            assembly.offset = end;
            if end == assembly.bytes.len() {
                self.live_chunk = None;
            }
            return Ok(Some(body));
        }
        let mut queue = live.lock().unwrap();
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut entities = HashSet::new();
        while let Some(item) = queue.items.front() {
            // Reactor publication treats one entity change as one notification.
            // Preserve connecting events by publishing each queued change before
            // another change for the same entity enters the next frame.
            if entities.contains(&item.delta.entity_id) {
                break;
            }
            let size = bincode::serialized_size(item)? as usize;
            ensure!(item.predicate_relevance.len() <= 128, "live relevance exceeds limit");
            if size > PAGE_BYTES / 2 {
                if !items.is_empty() {
                    break;
                }
                let item = queue.items.pop_front().unwrap();
                queue.bytes -= size;
                let bytes = bincode::serialize(&item.delta.content)?;
                ensure!(bytes.len() <= MAX_REPAIR_BYTES, "live delta exceeds assembly limit");
                self.live_chunk =
                    Some(LiveAssembly { entity_id: item.delta.entity_id, bytes, offset: 0, relevance: item.predicate_relevance });
                drop(queue);
                return self.next_live(live);
            }
            if bytes + size > PAGE_BYTES || items.len() == PAGE_STEPS {
                break;
            }
            bytes += size;
            entities.insert(item.delta.entity_id);
            queue.bytes -= size;
            items.push(queue.items.pop_front().unwrap());
        }
        Ok((!items.is_empty()).then_some(StateFrameBody::Live(items)))
    }
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn comparison_bypasses_policy(&self, peer: proto::EntityId, body: &proto::NodeRequestBody, outgoing: bool) -> bool {
        let id = match body {
            proto::NodeRequestBody::ContinueState { comparison, .. } | proto::NodeRequestBody::CloseComparison { comparison, .. } => {
                comparison
            }
            _ => return crate::schema::request_bypasses_policy(body),
        };
        let Some(connection) = self.peer_connections.get(&peer) else { return false };
        if outgoing {
            connection.outgoing_comparisons.get(id).is_some_and(|catalog| catalog)
        } else {
            connection.comparisons.bypasses_policy(id)
        }
    }
    pub(crate) async fn start_comparison(
        &self,
        peer: proto::EntityId,
        request: proto::RequestId,
        fingerprint: Vec<u8>,
        credentials: Vec<PA::ContextData>,
        query: Option<(proto::QueryId, u32)>,
        catalog: bool,
        snapshot: Snapshot,
        known: Option<proto::KnownState>,
    ) -> anyhow::Result<proto::StateReply> {
        let connection = self.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let mut comparison = Comparison::new(snapshot, known)?;
        let id = ComparisonId::new();
        let body = comparison.reply(self, &credentials, catalog, query.is_some()).await?;
        let frame = StateFrame { comparison: id, sequence: 0, body };
        let initial = proto::StateReply { spec: comparison.snapshot.spec.clone(), scope: vec![super::whole()], frame: frame.clone() };
        let mut state = Stream {
            comparison: Some(comparison),
            retained: VecDeque::new(),
            bytes: 0,
            last_sent: 0,
            processed: None,
            required_through: 0,
            applied: false,
            failed: false,
            last_activity: Instant::now(),
            feedback: VecDeque::new(),
            feedback_bytes: 0,
            retired_feedback: HashSet::new(),
            live_chunk: None,
        };
        state.retain(frame, false)?;
        let (cancel_expiry, mut cancelled) = futures::channel::oneshot::channel();
        let session = Arc::new(Session {
            catalog,
            credentials,
            query,
            initial_request: request,
            initial: Mutex::new(Some(state.retained.front().unwrap().frame.clone())),
            sent: AtomicU64::new(0),
            spec: initial.spec.clone(),
            scope: initial.scope.clone(),
            fingerprint,
            state: tokio::sync::Mutex::new(state),
            live: Mutex::new(LiveQueue::default()),
            pumping: AtomicBool::new(false),
            wake: AtomicBool::new(false),
            _cancel_expiry: cancel_expiry,
        });
        {
            let mut sessions = connection.comparisons.sessions.lock().unwrap();
            let mut origins = connection.comparisons.origins.lock().unwrap();
            ensure!(sessions.len() < MAX_COMPARISONS, "too many open state streams");
            ensure!(origins.len() < MAX_ORIGINS, "connection exhausted its bounded origin history");
            ensure!(!origins.contains_key(&session.initial_request), "initial request origin was already used");
            ensure!(!sessions.values().any(|s| s.initial_request == session.initial_request), "initial request already has a stream");
            origins.insert(session.initial_request.clone(), id);
            sessions.insert(id, session);
        }
        let node = self.weak();
        crate::task::spawn(async move {
            loop {
                let expiry = futures_timer::Delay::new(COMPARISON_TTL);
                if matches!(futures::future::select(expiry, &mut cancelled).await, futures::future::Either::Right(_)) {
                    break;
                }
                let Some(node) = node.upgrade() else { break };
                let Some(connection) = node.peer_connections.get(&peer) else { break };
                let Some(session) = connection.comparisons.get(&id) else { break };
                let state = session.state.lock().await;
                let idle = state.last_activity.elapsed() >= COMPARISON_TTL
                    && (!state.applied || !state.retained.is_empty() || session.live.lock().unwrap().bytes > 0);
                drop(state);
                if idle {
                    let _ = node.expire_comparison(peer, id).await;
                    break;
                }
            }
        });
        Ok(initial)
    }
    pub(crate) async fn continue_state(
        &self,
        peer: proto::EntityId,
        request: proto::RequestId,
        id: ComparisonId,
        processed_through: u64,
        answers: Vec<StateAnswer>,
        credentials: Vec<PA::ContextData>,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        let connection = self.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let Some(session) = connection.comparisons.get(&id) else {
            return Ok(if answers.is_empty() && connection.comparisons.permits_retired_receipt(id, processed_through, &credentials) {
                proto::NodeResponseBody::Success
            } else {
                proto::NodeResponseBody::ComparisonUnavailable { comparison: id }
            });
        };
        ensure!(session.permits(&credentials), "comparison credentials changed");
        ensure!(answers.len() <= 128, "too many comparison answers");
        let encoded = bincode::serialize(&(processed_through, &answers))?;
        ensure!(encoded.len() <= FRAME_BYTES, "comparison feedback exceeds limit");
        let mut state = session.state.lock().await;
        if !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
            return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
        }
        if state.failed {
            if !answers.is_empty() {
                return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
            }
            state.acknowledge(processed_through)?;
            if processed_through == state.last_sent {
                connection.comparisons.finish(id);
            }
            return Ok(proto::NodeResponseBody::Success);
        }
        if let Some((_, previous)) = state.feedback.iter().find(|(previous, _)| previous == &request) {
            ensure!(previous == &encoded, "feedback retry changed its contents");
            return Ok(proto::NodeResponseBody::Success);
        }
        if state.retired_feedback.contains(&request) {
            return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
        }
        ensure!(state.retired_feedback.len() < MAX_FEEDBACK_IDS, "stream exhausted control replay history");
        state.acknowledge(processed_through)?;
        session.initial.lock().unwrap().take();
        if let Some(comparison) = state.comparison.as_mut() {
            comparison.answer(answers).await?;
        } else {
            ensure!(answers.is_empty(), "live stream cannot answer snapshot questions");
        }
        state.feedback_bytes += encoded.len();
        state.feedback.push_back((request, encoded));
        while state.feedback.len() > MAX_FEEDBACK || state.feedback_bytes > CONTROL_BYTES {
            let (retired, bytes) = state.feedback.pop_front().unwrap();
            state.feedback_bytes -= bytes.len();
            state.retired_feedback.insert(retired);
        }
        drop(state);
        self.kick_comparison(peer, id);
        Ok(proto::NodeResponseBody::Success)
    }
    pub(crate) async fn close_state(
        &self,
        peer: proto::EntityId,
        id: ComparisonId,
        outcome: proto::ComparisonOutcome,
        credentials: Vec<PA::ContextData>,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        let connection = self.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let Some(session) = connection.comparisons.get(&id) else {
            return Ok(if connection.comparisons.was_closed(&id) {
                proto::NodeResponseBody::Success
            } else {
                proto::NodeResponseBody::ComparisonUnavailable { comparison: id }
            });
        };
        ensure!(session.permits(&credentials), "comparison credentials changed");
        let mut state = session.state.lock().await;
        if !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
            return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
        }
        if state.failed && outcome != proto::ComparisonOutcome::Abandoned {
            return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
        }
        match outcome {
            proto::ComparisonOutcome::Applied { processed_through } => {
                if state.applied {
                    ensure!(
                        processed_through >= state.required_through && processed_through <= state.last_sent,
                        "invalid repeated Applied prefix"
                    );
                    return Ok(proto::NodeResponseBody::Success);
                }
                let comparison = state.comparison.as_ref().unwrap();
                ensure!(
                    !comparison.unavailable
                        && comparison.complete
                        && comparison.work.is_empty()
                        && comparison.summaries.is_empty()
                        && comparison.leaves.is_empty()
                        && comparison.heads.is_empty()
                        && comparison.pending_repairs.is_empty(),
                    "comparison still has outstanding or unavailable work"
                );
                ensure!(processed_through >= state.required_through, "Applied does not cover all repair frames");
                state.acknowledge(processed_through)?;
                session.initial.lock().unwrap().take();
                state.applied = true;
                state.comparison = None;
                if session.query.is_none() {
                    connection.comparisons.finish(id);
                }
            }
            proto::ComparisonOutcome::Abandoned => {
                connection.comparisons.finish(id);
            }
        }
        drop(state);
        if outcome == proto::ComparisonOutcome::Abandoned {
            if let Some((query, version)) = session.query {
                connection.subscription_handler.finish_comparison(self, query, version, id, outcome).await?;
            }
        } else {
            self.kick_comparison(peer, id);
        }
        Ok(proto::NodeResponseBody::Success)
    }
    pub(crate) fn queue_live(
        &self,
        peer: proto::EntityId,
        id: ComparisonId,
        items: Vec<proto::SubscriptionUpdateItem>,
    ) -> anyhow::Result<()> {
        let connection = self.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let session = connection.comparisons.get(&id).ok_or_else(|| anyhow::anyhow!("live stream unavailable"))?;
        ensure!(session.query.is_some(), "Fetch has no live route");
        let sizes = items.iter().map(|item| bincode::serialized_size(item).map(|size| size as usize)).collect::<Result<Vec<_>, _>>()?;
        let bytes = sizes.iter().sum::<usize>();
        let mut queue = session.live.lock().unwrap();
        if queue.stopped
            || sizes.iter().any(|size| *size > MAX_REPAIR_BYTES)
            || queue.items.len() + items.len() > MAX_LIVE_ITEMS
            || queue.bytes.saturating_add(bytes) > MAX_LIVE_BYTES
        {
            queue.stopped = true;
            drop(queue);
            self.kick_comparison(peer, id);
            bail!("bounded live queue exhausted");
        }
        queue.bytes += bytes;
        queue.items.extend(items);
        drop(queue);
        self.kick_comparison(peer, id);
        Ok(())
    }
    pub(crate) fn kick_comparison(&self, peer: proto::EntityId, id: ComparisonId) {
        let Some(connection) = self.peer_connections.get(&peer) else { return };
        let Some(session) = connection.comparisons.get(&id) else { return };
        session.wake.store(true, Ordering::Release);
        if session.pumping.swap(true, Ordering::AcqRel) {
            return;
        }
        let node = self.weak();
        crate::task::spawn(async move {
            loop {
                session.wake.store(false, Ordering::Release);
                let Some(node) = node.upgrade() else { break };
                if let Err(error) = node.pump_comparison(peer, id).await {
                    tracing::warn!(%error, "state stream failed");
                    let _ = node.expire_comparison(peer, id).await;
                    break;
                }
                if session.wake.swap(false, Ordering::AcqRel) {
                    continue;
                }
                session.pumping.store(false, Ordering::Release);
                if session.wake.swap(false, Ordering::AcqRel) && !session.pumping.swap(true, Ordering::AcqRel) {
                    continue;
                }
                return;
            }
            session.pumping.store(false, Ordering::Release);
        });
    }
    pub(crate) async fn pump_comparison(&self, peer: proto::EntityId, id: ComparisonId) -> anyhow::Result<()> {
        let connection = self.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let Some(session) = connection.comparisons.get(&id) else { return Ok(()) };
        let mut state = session.state.lock().await;
        if state.failed || !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
            return Ok(());
        }
        ensure!(!session.live.lock().unwrap().stopped, "live queue stopped");
        loop {
            if !self.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
                || !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
            {
                break;
            }
            let priority = state.comparison.as_ref().is_some_and(|comparison| !comparison.pending_repairs.is_empty());
            if !state.room(priority) {
                break;
            }
            let body = if let Some(comparison) = state.comparison.as_mut() {
                if comparison.work.is_empty() || comparison.waiting_for_capacity() {
                    break;
                }
                comparison.reply(self, &session.credentials, session.catalog, session.query.is_some()).await?
            } else if state.applied {
                let Some(body) = state.next_live(&session.live)? else { break };
                body
            } else {
                break;
            };
            let sequence = state.last_sent.checked_add(1).ok_or_else(|| anyhow::anyhow!("state sequence exhausted"))?;
            let frame = StateFrame { comparison: id, sequence, body };
            if !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
                break;
            }
            if matches!(frame.body, StateFrameBody::State { .. }) {
                state.required_through = sequence;
            }
            state.retain(frame.clone(), priority)?;
            session.sent.store(sequence, Ordering::Release);
            connection.send_message(proto::NodeMessage::Update(proto::NodeUpdate {
                id: proto::UpdateId::new(),
                from: self.id,
                to: peer,
                body: proto::NodeUpdateBody::StateFrame { request_id: session.initial_request.clone(), frame },
            }))?;
        }
        Ok(())
    }
    pub(crate) async fn expire_comparison(&self, peer: proto::EntityId, id: ComparisonId) -> anyhow::Result<()> {
        let Some(connection) = self.peer_connections.get(&peer) else { return Ok(()) };
        let Some(session) = connection.comparisons.get(&id) else { return Ok(()) };
        let mut state = session.state.lock().await;
        if state.failed {
            return Ok(());
        }
        state.failed = true;
        state.comparison = None;
        state.live_chunk = None;
        session.initial.lock().unwrap().take();
        {
            let mut live = session.live.lock().unwrap();
            live.stopped = true;
            live.items.clear();
            live.bytes = 0;
        }
        let frame = StateFrame { comparison: id, sequence: state.last_sent.saturating_add(1), body: StateFrameBody::RestartRequired };
        let retained = state.room(true) && state.retain(frame.clone(), true).is_ok();
        session.sent.store(state.last_sent, Ordering::Release);
        drop(state);
        if let Some((query, version)) = session.query {
            connection.subscription_handler.finish_comparison(self, query, version, id, proto::ComparisonOutcome::Abandoned).await?;
        }
        if !self.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection)) {
            return Ok(());
        }
        if !retained
            || connection
                .send_message(proto::NodeMessage::Update(proto::NodeUpdate {
                    id: proto::UpdateId::new(),
                    from: self.id,
                    to: peer,
                    body: proto::NodeUpdateBody::StateFrame { request_id: session.initial_request.clone(), frame: frame.clone() },
                }))
                .is_err()
        {
            self.deregister_peer(peer);
            return Ok(());
        }
        let node = self.weak();
        crate::task::spawn(async move {
            for _ in 0..3 {
                futures_timer::Delay::new(Duration::from_secs(1)).await;
                let Some(node) = node.upgrade() else { return };
                if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
                    || !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
                {
                    return;
                }
                if connection
                    .send_message(proto::NodeMessage::Update(proto::NodeUpdate {
                        id: proto::UpdateId::new(),
                        from: node.id,
                        to: peer,
                        body: proto::NodeUpdateBody::StateFrame { request_id: session.initial_request.clone(), frame: frame.clone() },
                    }))
                    .is_err()
                {
                    node.deregister_peer(peer);
                    return;
                }
            }
            if let Some(node) = node.upgrade() {
                if node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
                    && connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
                {
                    node.deregister_peer(peer);
                }
            }
        });
        Ok(())
    }

    async fn target_history(
        &self,
        entity_id: proto::EntityId,
        target: &proto::Clock,
        snapshot: &Snapshot,
        credentials: &Vec<PA::ContextData>,
        catalog: bool,
    ) -> anyhow::Result<Vec<proto::Attested<proto::Event>>> {
        let policy = ContextPolicy::from_credentials(&self.policy_agent, credentials);
        let state = match snapshot.states.get(&entity_id) {
            Some(state) => (**state).clone(),
            None => node_state(self, entity_id, &policy).await?.ok_or_else(|| anyhow::anyhow!("repair state unavailable"))?,
        };
        policy.check_read(&entity_id, &state.payload.state)?;
        ensure!(
            !catalog || state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy),
            "repair is outside catalog scope"
        );
        ensure!(!target.is_empty(), "empty causal repair target");
        let mut pending = target.entries().iter().map(|(generation, id)| (id.clone(), *generation)).collect::<HashMap<_, _>>();
        let mut visited = HashSet::new();
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut genesis = None;
        while !pending.is_empty() {
            ensure!(visited.len() + pending.len() <= MAX_HISTORY, "causal repair history budget exhausted");
            let batch = std::mem::take(&mut pending);
            let fetched = self.storage.get_events(batch.keys().cloned().collect()).await?;
            ensure!(fetched.len() == batch.len(), "causal repair history is incomplete");
            for event in fetched {
                let id = event.payload.id();
                ensure!(batch.get(&id) == Some(&event.payload.generation()), "causal repair event or generation mismatch");
                ensure!(event.payload.entity_id == entity_id, "causal repair entity mismatch");
                event.payload.validate_structure()?;
                policy.check_read_event(&event, &state.payload.state)?;
                if !visited.insert(id.clone()) {
                    continue;
                }
                bytes = bytes
                    .checked_add(bincode::serialized_size(&event)? as usize)
                    .ok_or_else(|| anyhow::anyhow!("history size overflow"))?;
                ensure!(bytes <= MAX_REPAIR_BYTES, "causal repair exceeds byte limit");
                if event.payload.parent.is_empty() {
                    ensure!(
                        proto::EntityId::from(id.clone()) == entity_id && genesis.as_ref().is_none_or(|root| root == &id),
                        "causal repair has inconsistent genesis"
                    );
                    genesis = Some(id);
                }
                for (generation, parent) in &event.payload.parent {
                    if !visited.contains(parent) {
                        if let Some(previous) = pending.insert(parent.clone(), *generation) {
                            ensure!(previous == *generation, "inconsistent parent generation");
                        }
                    }
                }
                events.push(event);
            }
        }
        ensure!(genesis.is_some(), "causal repair did not prove a genesis");
        Ok(crate::event_dag::ordering::topo_sort_events(events)?)
    }
}

async fn node_state<SE, PA, C>(
    node: &Node<SE, PA>,
    id: proto::EntityId,
    policy: &ContextPolicy<'_, PA, C>,
) -> anyhow::Result<Option<proto::Attested<proto::EntityState>>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: ankurah_signals::Signal + ankurah_signals::Peek<Vec<PA::ContextData>>,
{
    Ok(node
        .storage
        .get_states(vec![id], &policy.retrieval_predicate())
        .await?
        .pop()
        .and_then(|result| proto::GetResult::from(result).try_into().ok()))
}

impl Comparison {
    fn waiting_for_capacity(&self) -> bool {
        match self.work.front() {
            Some(Work::Offer(_)) => self.summaries.len() >= 128,
            Some(Work::AskLeaves(_)) => self.leaves.len() >= 128,
            _ => false,
        }
    }
    fn new(snapshot: Snapshot, known: Option<proto::KnownState>) -> anyhow::Result<Self> {
        let mut comparison = Self {
            snapshot,
            work: VecDeque::new(),
            summaries: HashMap::new(),
            leaves: Vec::new(),
            heads: HashSet::new(),
            repaired: HashSet::new(),
            known_clocks: HashMap::new(),
            offered: HashMap::new(),
            pending_repairs: HashSet::new(),
            repair_attempts: HashMap::new(),
            complete: false,
            unavailable: false,
        };
        let mut regions = if let Some(known) = known {
            ensure!(known.spec == comparison.snapshot.spec, "unsupported comparison tree spec");
            ensure!(
                known.regions.len() <= 128 && bincode::serialized_size(&known)? as usize <= FRAME_BYTES,
                "initial inventory exceeds limit"
            );
            known.regions
        } else {
            Vec::new()
        };
        regions.sort_by(|a, b| {
            let start = |region: &proto::KnownRegion| match region {
                proto::KnownRegion::Summary(head) => head.bounds.start.clone(),
                proto::KnownRegion::Leaves(page) => page.bounds.start.clone(),
            };
            let a = start(a);
            let b = start(b);
            if a == b {
                std::cmp::Ordering::Equal
            } else if start_le(&a, &b) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });
        let mut start = super::whole().start;
        for region in regions {
            ensure!(!matches!(start, std::ops::Bound::Unbounded), "initial inventory extends past scope");
            let bounds = match &region {
                proto::KnownRegion::Summary(head) => &head.bounds,
                proto::KnownRegion::Leaves(page) => &page.bounds,
            };
            ensure!(
                valid_range(bounds) && start_le(&start, &bounds.start) && end_le(&bounds.end, &super::whole().end),
                "initial regions overlap or exceed scope"
            );
            if start != bounds.start {
                let gap = AddressRange::new(start.clone(), opposite(&bounds.start));
                if valid_range(&gap) {
                    comparison.work.push_back(Work::SendSnapshots(gap));
                }
            }
            start = opposite(&bounds.end);
            match region {
                proto::KnownRegion::Summary(head) => {
                    let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                    comparison.work.push_back(Work::Compare { prefix: NodePrefix::root(), head });
                }
                proto::KnownRegion::Leaves(page) => {
                    comparison.validate_leaves(&page)?;
                    for leaf in &page.leaves {
                        ensure!(
                            comparison.known_clocks.insert(leaf.entity.entity_id, leaf.entity.head.clone()).is_none(),
                            "duplicate entity in initial inventory"
                        );
                        if !comparison.snapshot.states.contains_key(&leaf.entity.entity_id) {
                            comparison.work.push_back(Work::Repair(leaf.entity.entity_id, Some(leaf.entity.head.clone())));
                        }
                    }
                    comparison.work.push_back(if page.leaves.is_empty() {
                        Work::SendSnapshots(page.bounds)
                    } else {
                        Work::SendLeaves(page.bounds)
                    });
                }
            }
        }
        if !matches!(start, std::ops::Bound::Unbounded) {
            let rest = AddressRange::new(start, super::whole().end);
            if valid_range(&rest) {
                comparison.work.push_back(Work::SendSnapshots(rest));
            }
        }
        Ok(comparison)
    }

    fn validate_leaves(&self, page: &proto::LeafPage) -> anyhow::Result<()> {
        ensure!(valid_range(&page.bounds) && page.leaves.len() <= LEAF_PAGE, "invalid leaf inventory size or bounds");
        let mut previous = None;
        for leaf in &page.leaves {
            CanonicalCodec.split(self.snapshot.spec.key_spec(), &leaf.key)?;
            let address = leaf_address(&leaf.key, leaf.entity.entity_id);
            ensure!(page.bounds.contains(&address) && previous.as_ref().is_none_or(|p| p < &address), "invalid leaf order or bounds");
            previous = Some(address);
        }
        Ok(())
    }

    async fn answer(&mut self, answers: Vec<StateAnswer>) -> anyhow::Result<()> {
        ensure!(answers.len() <= 128, "too many comparison answers");
        for answer in answers {
            match answer {
                StateAnswer::NeedRepair { entity_id, target, actual: _ } => {
                    ensure!(self.offered.get(&entity_id) == Some(&target), "unsolicited causal repair target");
                    if self.pending_repairs.contains(&entity_id) {
                        continue;
                    }
                    ensure!(self.pending_repairs.len() < 128, "too many pending causal repairs");
                    let attempts = self.repair_attempts.entry(entity_id).or_default();
                    *attempts += 1;
                    ensure!(*attempts <= 8, "causal repair retry budget exhausted");
                    self.pending_repairs.insert(entity_id);
                    self.work.push_front(Work::History { entity_id, target });
                }
                StateAnswer::Summary(head) => {
                    let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                    let prefix =
                        self.summaries.remove(&range_id(&head.bounds)).ok_or_else(|| anyhow::anyhow!("unsolicited summary bounds"))?;
                    self.work.push_back(Work::Compare { head, prefix });
                }
                StateAnswer::Heads { known, unknown } => {
                    ensure!(known.len() + unknown.len() <= 128, "too many clock answers");
                    for entity in known {
                        ensure!(self.heads.remove(&entity.entity_id), "unsolicited entity clock");
                        self.work.push_back(Work::Repair(entity.entity_id, Some(entity.head)));
                    }
                    for id in unknown {
                        ensure!(self.heads.remove(&id), "unsolicited absent clock");
                        self.work.push_back(Work::Repair(id, None));
                    }
                }
                StateAnswer::Leaves(page) => {
                    ensure!(page.leaves.len() <= LEAF_PAGE, "too many leaves in one page");
                    let index = self
                        .leaves
                        .iter()
                        .position(|bounds| bounds.start == page.bounds.start)
                        .ok_or_else(|| anyhow::anyhow!("unsolicited leaf page"))?;
                    let requested = self.leaves[index].clone();
                    ensure!(end_le(&page.bounds.end, &requested.end), "leaf page exceeds requested bounds");
                    let mut previous = None;
                    let mut clocks = std::collections::BTreeMap::new();
                    for leaf in &page.leaves {
                        CanonicalCodec.split(self.snapshot.spec.key_spec(), &leaf.key)?;
                        let address = leaf_address(&leaf.key, leaf.entity.entity_id);
                        ensure!(
                            page.bounds.contains(&address) && previous.as_ref().is_none_or(|p| p < &address),
                            "invalid leaf order or bounds"
                        );
                        ensure!(clocks.insert(leaf.entity.entity_id, leaf.entity.head.clone()).is_none(), "duplicate entity in leaf page");
                        previous = Some(address);
                    }
                    self.leaves.remove(index);
                    if page.bounds.end != requested.end {
                        let std::ops::Bound::Included(last) = &page.bounds.end else { bail!("partial page must end at its last leaf") };
                        ensure!(previous.as_ref() == Some(last), "partial page made no progress");
                        self.work.push_back(Work::AskLeaves(requested.after(last)));
                    }
                    // The small responder region can contain many requester-only leaves,
                    // but never needs to send its entire result inventory at once.
                    let remote = self.snapshot.page(page.bounds.clone()).await?;
                    ensure!(remote.bounds == page.bounds, "leaf region was not fully refined");
                    let mut needed = Vec::new();
                    for leaf in &remote.leaves {
                        let id = leaf.entity.entity_id;
                        if let Some(clock) = clocks.remove(&id) {
                            self.work.push_back(Work::Repair(id, Some(clock)));
                        } else if !self.repaired.contains(&id) && self.heads.insert(id) {
                            needed.push(id);
                        }
                    }
                    if !needed.is_empty() {
                        self.work.push_back(Work::Item(StateItem::NeedHeads(needed)));
                    }
                    for (id, clock) in clocks {
                        self.work.push_back(Work::Repair(id, Some(clock)));
                    }
                    self.work.push_front(Work::Item(StateItem::Leaves(remote)));
                }
            }
        }
        Ok(())
    }

    fn snapshot_page<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        credentials: &Vec<PA::ContextData>,
        catalog: bool,
        bounds: AddressRange,
    ) -> anyhow::Result<StateItem>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        use std::ops::Bound::Included;
        let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
        let rows = self.snapshot.states_in(&bounds, super::MAX_SNAPSHOT_ROWS + 1).collect::<Vec<_>>();
        let mut states = Vec::new();
        let mut bytes = 0;
        let mut last = None;
        for (address, state) in rows.iter().take(super::MAX_SNAPSHOT_ROWS) {
            let id = state.payload.entity_id;
            policy.check_read(&id, &state.payload.state)?;
            ensure!(
                !catalog || state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy),
                "snapshot page is outside catalog scope"
            );
            let clipped = AddressRange::new(bounds.start.clone(), Included(address.to_vec()));
            // Reserve the larger possible page end, plus the item tag and vector length.
            let overhead = 12 + bincode::serialized_size(&bounds)?.max(bincode::serialized_size(&clipped)?) as usize;
            let size = bincode::serialized_size(state)? as usize;
            let known = self.known_clocks.get(&id).cloned();
            if known.is_some() || overhead + bytes + size > PAGE_BYTES / 2 {
                if !states.is_empty() {
                    break;
                }
                // A moved member may have supplied its clock in another range.
                // Keep that bridge choice, and use the same fallback for a row
                // too large to fit inline. Only this one row needs Leaves/Repair.
                let end = if rows.len() == 1 { bounds.end.clone() } else { Included(address.to_vec()) };
                let page_bounds = AddressRange::new(bounds.start.clone(), end);
                if page_bounds.end != bounds.end {
                    self.work.push_front(Work::SendSnapshots(bounds.after(address)));
                }
                self.work.push_front(Work::Repair(id, known));
                return Ok(StateItem::Leaves(proto::LeafPage {
                    bounds: page_bounds,
                    leaves: vec![proto::KnownLeaf {
                        key: address[..address.len() - proto::EntityId::BYTE_LEN].to_vec(),
                        entity: proto::KnownEntity { entity_id: id, head: state.payload.state.head.clone() },
                    }],
                }));
            }
            bytes += size;
            last = Some(address.to_vec());
            states.push((*state).clone());
        }
        let end = if states.len() == rows.len() {
            bounds.end.clone()
        } else {
            let last = last.expect("a partial snapshot page contains at least one row");
            self.work.push_front(Work::SendSnapshots(bounds.after(&last)));
            Included(last)
        };
        Ok(StateItem::Snapshots(proto::SnapshotPage { bounds: AddressRange::new(bounds.start, end), states }))
    }

    async fn reply<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        credentials: &Vec<PA::ContextData>,
        catalog: bool,
        standing_query: bool,
    ) -> anyhow::Result<StateFrameBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut tree_or_history_work = 0;
        let repair_frame = !self.pending_repairs.is_empty();
        for _ in 0..PAGE_STEPS {
            if repair_frame && self.pending_repairs.is_empty() {
                break;
            }
            ensure!(self.work.len() <= 4096, "comparison work queue exhausted");
            if self.waiting_for_capacity() {
                break;
            }
            let Some(work) = self.work.pop_front() else { break };
            // Keep this in step with tree/storage/history access in the arms below.
            let expensive = match &work {
                Work::Compare { .. } | Work::Expand { .. } | Work::SendLeaves(_) | Work::SendSnapshots(_) => true,
                Work::Repair(id, known) => known.is_some() || !self.snapshot.states.contains_key(id),
                Work::History { .. } => true,
                _ => false,
            };
            if expensive {
                if tree_or_history_work == PAGE_WORK {
                    self.work.push_front(work);
                    break;
                }
                tree_or_history_work += 1;
            }
            let item = match work {
                Work::History { entity_id, target } => {
                    match node.target_history(entity_id, &target, &self.snapshot, credentials, catalog).await {
                        Ok(events) => {
                            let delta = proto::EntityDelta {
                                entity_id,
                                content: proto::DeltaContent::EventBridge { events: events.into_iter().map(Into::into).collect() },
                            };
                            let bytes = bincode::serialize(&delta.content)?;
                            if bytes.len() > PAGE_BYTES / 2 {
                                self.work.push_front(Work::Chunks { entity_id, target, bytes, offset: 0 });
                                None
                            } else {
                                Some(StateItem::Repair { target, delta })
                            }
                        }
                        Err(_) => Some(StateItem::RepairUnavailable { entity_id, target }),
                    }
                }
                Work::Compare { head: known, prefix } => {
                    if known.digest == ankurah_digest_tree::Digest::empty() {
                        if self.snapshot.states_in(&known.bounds, 1).next().is_none() {
                            Some(StateItem::Equal(known.bounds))
                        } else {
                            self.work.push_front(Work::SendSnapshots(known.bounds));
                            None
                        }
                    } else {
                        let mut local = self.snapshot.head(known.bounds.clone()).await?;
                        // The answering peer's topology may differ. Continue from
                        // the responder prefix retained with this question.
                        local.prefix = prefix;
                        if local.digest == known.digest {
                            Some(StateItem::Equal(known.bounds))
                        } else if local.digest.count() <= LEAF_PAGE as i64 {
                            self.work.push_front(Work::AskLeaves(known.bounds));
                            None
                        } else {
                            self.work.push_front(Work::Expand { local, known });
                            None
                        }
                    }
                }
                Work::Offer(head) => {
                    self.summaries.insert(range_id(&head.bounds), head.prefix.clone());
                    Some(StateItem::Summary(head))
                }
                Work::Expand { local: head, known } => {
                    // Descend locally within the work budget, preserving cached
                    // prefixes so a reply may assist several levels at once.
                    let refined = self.snapshot.refine(&head).await?;
                    if refined.len() == 1 && refined[0].bounds == head.bounds && refined[0].prefix == head.prefix {
                        let page = self.snapshot.page(head.bounds.clone()).await?;
                        if let std::ops::Bound::Included(last) = &page.bounds.end {
                            self.work.push_back(Work::Offer(self.snapshot.head(head.bounds.after(last)).await?));
                        }
                        self.work.push_back(Work::Offer(self.snapshot.head(page.bounds).await?));
                    } else {
                        for head in refined {
                            self.work.push_back(if head.bounds == known.bounds {
                                // A new local prefix does not change the exact
                                // digest already supplied for these bounds.
                                Work::Compare { head: known.clone(), prefix: head.prefix }
                            } else {
                                Work::Offer(head)
                            });
                        }
                    }
                    None
                }
                Work::AskLeaves(bounds) => {
                    self.leaves.push(bounds.clone());
                    Some(StateItem::NeedLeaves(bounds))
                }
                Work::SendLeaves(bounds) => {
                    let page = self.snapshot.page(bounds.clone()).await?;
                    if page.bounds.end != bounds.end {
                        let std::ops::Bound::Included(last) = &page.bounds.end else { unreachable!("partial leaf page") };
                        self.work.push_back(Work::SendLeaves(bounds.after(last)));
                    }
                    for leaf in &page.leaves {
                        let id = leaf.entity.entity_id;
                        self.work.push_front(Work::Repair(id, self.known_clocks.get(&id).cloned()));
                    }
                    Some(StateItem::Leaves(page))
                }
                Work::SendSnapshots(bounds) => Some(self.snapshot_page(node, credentials, catalog, bounds)?),
                Work::Item(item) => Some(item),
                Work::Repair(id, known) => {
                    if !self.repaired.insert(id) {
                        continue;
                    }
                    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
                    let state: Option<proto::Attested<proto::EntityState>> = match self.snapshot.states.get(&id) {
                        Some(state) => Some((**state).clone()),
                        None => match node.storage.get_states(vec![id], &policy.retrieval_predicate()).await?.pop() {
                            Some(result) => proto::GetResult::from(result).try_into().ok(),
                            None => None,
                        },
                    };
                    let Some(state) = state else { continue };
                    if catalog && !state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy) {
                        continue;
                    }
                    if policy.check_read(&id, &state.payload.state).is_err() {
                        continue;
                    }
                    let target = state.payload.state.head.clone();
                    let snapshot = known.as_ref().map(|_| proto::DeltaContent::StateSnapshot { state: state.clone().into() });
                    let known = known.map(|head| HashMap::from([(id, head)])).unwrap_or_default();
                    let Some(mut delta) = node.generate_entity_delta(&known, state, credentials).await? else { continue };
                    if matches!(delta.content, proto::DeltaContent::EventBridge { .. }) {
                        if let Some(snapshot) = snapshot {
                            if bincode::serialized_size(&snapshot)? < bincode::serialized_size(&delta.content)? {
                                delta.content = snapshot;
                            }
                        }
                    }
                    let item = StateItem::Repair { target: target.clone(), delta };
                    if bincode::serialized_size(&item)? as usize > PAGE_BYTES / 2 {
                        let StateItem::Repair { delta, .. } = item else { unreachable!() };
                        let bytes = bincode::serialize(&delta.content)?;
                        ensure!(bytes.len() <= super::MAX_REPAIR_BYTES, "entity repair exceeds transfer limit");
                        self.work.push_front(Work::Chunks { entity_id: id, target, bytes, offset: 0 });
                        None
                    } else {
                        Some(item)
                    }
                }
                Work::Chunks { entity_id, target, bytes, offset } => {
                    let end = (offset + PAGE_BYTES / 2).min(bytes.len());
                    let item = StateItem::RepairChunk {
                        target: target.clone(),
                        chunk: proto::EntityDeltaChunk {
                            entity_id,
                            offset: offset as u64,
                            bytes: bytes[offset..end].to_vec().into(),
                            last: end == bytes.len(),
                        },
                    };
                    if end < bytes.len() {
                        self.work.push_front(Work::Chunks { entity_id, target, bytes, offset: end });
                    }
                    Some(item)
                }
            };
            if let Some(item) = item {
                let size = bincode::serialized_size(&item)? as usize;
                if !items.is_empty() && bytes + size > PAGE_BYTES {
                    self.work.push_front(Work::Item(item));
                    break;
                }
                bytes += size;
                match &item {
                    StateItem::Snapshots(page) => {
                        for state in &page.states {
                            let id = state.payload.entity_id;
                            self.repaired.insert(id);
                            self.offered.insert(id, state.payload.state.head.clone());
                        }
                    }
                    StateItem::Repair { target, delta } => {
                        self.offered.insert(delta.entity_id, target.clone());
                        self.pending_repairs.remove(&delta.entity_id);
                    }
                    StateItem::RepairChunk { target, chunk } => {
                        self.offered.insert(chunk.entity_id, target.clone());
                        if chunk.last {
                            self.pending_repairs.remove(&chunk.entity_id);
                        }
                    }
                    StateItem::RepairUnavailable { entity_id, .. } => {
                        self.unavailable = true;
                        self.pending_repairs.remove(entity_id);
                    }
                    _ => {}
                }
                ensure!(self.offered.len() <= MAX_OFFERED_TARGETS, "too many offered causal targets");
                items.push(item);
            }
        }
        let finished = self.work.is_empty() && self.summaries.is_empty() && self.leaves.is_empty() && self.heads.is_empty();
        let first_complete = finished && !self.complete;
        if first_complete && standing_query && !self.unavailable && self.snapshot.states.len() > LEAF_PAGE {
            // Charge retained-query construction to its initial handoff. Small
            // inventories and temporary Fetch snapshots remain lazy.
            self.snapshot.head(super::whole()).await?;
        }
        self.complete |= finished;
        Ok(StateFrameBody::State {
            items,
            progress: if first_complete { ComparisonProgress::Complete } else { ComparisonProgress::Continue },
        })
    }
}

fn opposite(bound: &std::ops::Bound<Vec<u8>>) -> std::ops::Bound<Vec<u8>> {
    match bound {
        std::ops::Bound::Included(value) => std::ops::Bound::Excluded(value.clone()),
        std::ops::Bound::Excluded(value) => std::ops::Bound::Included(value.clone()),
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
    }
}
fn start_le(a: &std::ops::Bound<Vec<u8>>, b: &std::ops::Bound<Vec<u8>>) -> bool {
    use std::ops::Bound::*;
    match (a, b) {
        (Unbounded, _) => true,
        (_, Unbounded) => false,
        (Included(a), Excluded(b)) => a <= b,
        (Excluded(a), Included(b)) => a < b,
        (Included(a), Included(b)) | (Excluded(a), Excluded(b)) => a <= b,
    }
}
fn valid_range(bounds: &AddressRange) -> bool {
    use std::ops::Bound::*;
    match (&bounds.start, &bounds.end) {
        (Included(a), Included(b)) => a <= b,
        (Included(a) | Excluded(a), Included(b) | Excluded(b)) => a < b,
        _ => true,
    }
}

fn end_le(a: &std::ops::Bound<Vec<u8>>, b: &std::ops::Bound<Vec<u8>>) -> bool {
    use std::ops::Bound::*;
    match (a, b) {
        (_, Unbounded) => true,
        (Unbounded, _) => false,
        (Included(a), Excluded(b)) => a < b,
        (Included(a), Included(b)) | (Excluded(a), Included(b)) | (Excluded(a), Excluded(b)) => a <= b,
    }
}
