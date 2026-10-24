use super::{
    resident::{LiveChange, LiveRead},
    Snapshot,
};
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_digest_tree::{leaf_address, NodePrefix};
use ankurah_proto::{
    self as proto,
    state_sync::limits::{
        CONTROL_BYTES, MAX_DELTA_BYTES, MAX_FRAMES_IN_FLIGHT, MAX_FRAME_BYTES, MAX_FRAME_ITEMS, MAX_OFFER_BYTES, MAX_OPEN_STREAMS,
        MAX_PAGE_BYTES, MAX_PAGE_HEADS, STREAM_TTL, WINDOW_BYTES,
    },
    ComparisonProgress, RepairItem, StateAnswer, StateFrame, StateFrameBody, StateItem, StateStreamId, TreeSpan,
};
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

/// Work steps one comparison reply may take. A step adds at most one item, so a
/// reply stays within MAX_FRAME_ITEMS.
const MAX_FRAME_STEPS: usize = MAX_FRAME_ITEMS;
/// Tree reads and history walks one reply may take. They can load storage, so
/// they get a lower cap than ready items.
const MAX_FRAME_TREE_WORK: usize = 32;
/// Deltas larger than this travel as chunks of this size, leaving room in a
/// frame for other items.
const DELTA_CHUNK_BYTES: usize = MAX_PAGE_BYTES / 2;
const MAX_FEEDBACK: usize = 256;
const MAX_OFFERED_TARGETS: usize = 100_000;
// Initial request identities live for the connection lifetime; reaching this
// cap requires reconnecting rather than reusing a retired origin.
const MAX_ORIGINS: usize = 100_000;
const MAX_FEEDBACK_IDS: usize = 100_000;
const MAX_HISTORY: usize = 4096;
const MAX_LIVE_BYTES: usize = MAX_DELTA_BYTES + WINDOW_BYTES;
const MAX_LIVE_ITEMS: usize = 4096;
#[cfg(test)]
mod tests;

pub(crate) struct StateStreams<CD: ContextData> {
    sessions: Mutex<HashMap<StateStreamId, Arc<Session<CD>>>>,
    closed: Mutex<VecDeque<StateStreamId>>,
    retired: Mutex<VecDeque<RetiredOrigin<CD>>>,
    origins: Mutex<HashMap<proto::RequestId, StateStreamId>>,
}
struct RetiredOrigin<CD> {
    request: proto::RequestId,
    id: StateStreamId,
    fingerprint: Vec<u8>,
    credentials: Vec<CD>,
    last_sent: u64,
    catalog: bool,
}
impl<CD: ContextData> Default for StateStreams<CD> {
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
    initial_round: proto::StateRound,
    sent: AtomicU64,
    spec: proto::TreeSpec,
    fingerprint: Vec<u8>,
    state: tokio::sync::Mutex<Stream>,
    live: Mutex<LiveQueue>,
    initialized: AtomicBool,
    pumping: AtomicBool,
    wake: AtomicBool,
    _cancel_expiry: futures::channel::oneshot::Sender<()>,
}
#[derive(Default)]
struct LiveQueue {
    publications: VecDeque<LivePublication>,
    items: usize,
    bytes: usize,
    last_revision: Option<u64>,
    stopped: bool,
    round: Option<proto::StateRound>,
    round_open: bool,
    pending_round: Option<PendingRound>,
    last_refresh: Option<(u64, Vec<u8>)>,
}
struct LivePublication {
    revision: u64,
    items: VecDeque<proto::QueryChange>,
}
struct Retained {
    frame: Arc<StateFrame>,
    bytes: usize,
}
/// A live delta too large for one frame, sent as chunks across frames.
struct LiveChunks {
    revision: u64,
    chunks: proto::DeltaChunks,
    member: Option<proto::QueryMember>,
    complete: bool,
}
struct PendingRound {
    round: proto::StateRound,
    comparison: Comparison,
}
struct LiveTarget {
    last_revision: u64,
    publications: usize,
    attempts: usize,
}
struct LiveTargetRef {
    revision: u64,
    key: Arc<[u8]>,
}
struct Stream {
    comparison: Option<Comparison>,
    round: proto::StateRound,
    pending_round: Option<PendingRound>,
    retired_round: Option<(u64, proto::StateRoundOutcome)>,
    retained: VecDeque<Retained>,
    bytes: usize,
    last_sent: u64,
    processed: Option<u64>,
    required_through: u64,
    failed: bool,
    last_activity: Instant,
    feedback: VecDeque<(proto::RequestId, Vec<u8>)>,
    feedback_bytes: usize,
    retired_feedback: HashSet<proto::RequestId>,
    live_chunk: Option<LiveChunks>,
    live_targets: VecDeque<LiveTargetRef>,
    live_target_index: HashMap<Arc<[u8]>, LiveTarget>,
    target_bytes: usize,
    live_repairs: VecDeque<Work>,
    pending_live_repairs: HashSet<Vec<u8>>,
    last_live_complete: Option<u64>,
    completed_live: VecDeque<(u64, u64)>,
    applied_through: Option<u64>,
    next_lane: usize,
}
impl<CD: ContextData> StateStreams<CD> {
    pub fn get(&self, id: &StateStreamId) -> Option<Arc<Session<CD>>> { self.sessions.lock().unwrap().get(id).cloned() }
    pub fn remove(&self, id: &StateStreamId) -> Option<Arc<Session<CD>>> {
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
        if retired.len() > MAX_OPEN_STREAMS {
            retired.pop_front();
        }
        Some(session)
    }
    pub fn was_closed(&self, id: &StateStreamId) -> bool { self.closed.lock().unwrap().contains(id) }
    fn bypasses_policy(&self, id: &StateStreamId) -> bool {
        if let Some(session) = self.get(id) {
            return session.catalog;
        }
        self.retired.lock().unwrap().iter().find(|origin| &origin.id == id).is_some_and(|origin| origin.catalog)
    }
    pub fn finish(&self, id: StateStreamId) {
        self.remove(&id);
        let mut closed = self.closed.lock().unwrap();
        if !closed.contains(&id) {
            closed.push_back(id);
        }
        if closed.len() > MAX_OPEN_STREAMS {
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
        let session = self.sessions.lock().unwrap().values().find(|session| &session.initial_request == request).cloned();
        if let Some(session) = session {
            ensure!(session.fingerprint == fingerprint && session.permits(credentials), "initial retry changed its request");
            let initial = session.initial.lock().unwrap();
            let Some(initial) = initial.as_ref() else {
                return Ok(Some(proto::NodeResponseBody::StateStreamUnavailable));
            };
            let reply = proto::StateReply { round: session.initial_round, frame: (**initial).clone() };
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
            return Ok(Some(proto::NodeResponseBody::StateStreamUnavailable));
        }
        if self.origins.lock().unwrap().contains_key(request) {
            return Ok(Some(proto::NodeResponseBody::StateStreamUnavailable));
        }
        Ok(None)
    }
    fn permits_retired_receipt(&self, id: StateStreamId, processed: u64, credentials: &[CD]) -> bool {
        self.was_closed(&id)
            && self
                .retired
                .lock()
                .unwrap()
                .iter()
                .any(|origin| origin.id == id && processed <= origin.last_sent && same_credentials(&origin.credentials, credentials))
    }
}
fn same_credentials<CD: ContextData>(a: &[CD], b: &[CD]) -> bool {
    a == b
        || (a.len() == b.len()
            && a.iter().all(|credential| {
                a.iter().filter(|value| *value == credential).count() == b.iter().filter(|value| *value == credential).count()
            }))
}
impl<CD: ContextData> Session<CD> {
    pub fn permits(&self, credentials: &[CD]) -> bool { same_credentials(&self.credentials, credentials) }
}
enum Work {
    CompareFrontier(Vec<proto::TreeSpanDigest>),
    Compare { head: proto::TreeSpanDigest, prefix: NodePrefix },
    Offer(proto::TreeSpanDigest),
    Expand { local: proto::TreeSpanDigest, known: proto::TreeSpanDigest },
    AskHeads(TreeSpan),
    InitialHeads(InitialHeads),
    AnswerHeads { page: proto::TreeSpanInventory, rest: Option<TreeSpan> },
    SendHeads(TreeSpan),
    Item(StateItem),
    Repair(proto::EntityId, Option<proto::Clock>),
    History { entity_id: proto::EntityId, target: proto::Clock },
    Chunks { target: proto::Clock, chunks: proto::DeltaChunks },
}

struct InitialHeads {
    bounds: TreeSpan,
    heads: proto::HeadCursor,
}
struct Comparison {
    snapshot: Snapshot,
    work: VecDeque<Work>,
    summaries: HashMap<Vec<u8>, NodePrefix>,
    head_requests: Vec<TreeSpan>,
    repaired: HashSet<proto::EntityId>,
    known_clocks: HashMap<proto::EntityId, proto::Clock>,
    offered: HashSet<Vec<u8>>,
    pending_repairs: HashSet<Vec<u8>>,
    repair_attempts: HashMap<Vec<u8>, usize>,
    complete: bool,
    unavailable: bool,
}
/// The next chunk of a split delta, and the work that sends the rest.
fn next_repair_chunk(target: proto::Clock, mut chunks: proto::DeltaChunks) -> (RepairItem, Option<Work>) {
    let chunk = chunks.next().expect("split work keeps its unsent chunks");
    let rest = (!chunk.last).then(|| Work::Chunks { target: target.clone(), chunks });
    (RepairItem::Chunk { target, chunk }, rest)
}
fn target_id(entity: proto::EntityId, head: &proto::Clock) -> Vec<u8> {
    bincode::serialize(&(entity, head)).expect("causal target serializes")
}
fn range_id(range: &TreeSpan) -> Vec<u8> { bincode::serialize(range).expect("address bounds serialize") }
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
        self.retained.len() < MAX_FRAMES_IN_FLIGHT - if priority { 0 } else { 2 }
            && self.bytes + MAX_FRAME_BYTES <= WINDOW_BYTES + if priority { CONTROL_BYTES } else { 0 }
    }
    fn retain(&mut self, frame: StateFrame, priority: bool) -> anyhow::Result<()> {
        let bytes = bincode::serialized_size(&frame)? as usize;
        ensure!(bytes <= MAX_FRAME_BYTES, "state frame exceeds hard limit");
        ensure!(
            self.retained.len() < MAX_FRAMES_IN_FLIGHT && self.bytes + bytes <= WINDOW_BYTES + if priority { CONTROL_BYTES } else { 0 },
            "state replay window exhausted"
        );
        if let StateFrameBody::Live { revision, complete: true, .. } | StateFrameBody::LiveChunk { revision, complete: true, .. } =
            &frame.body
        {
            ensure!(self.completed_live.len() < MAX_LIVE_ITEMS, "unapplied live publication window exhausted");
            self.last_live_complete = Some(*revision);
            self.completed_live.push_back((*revision, frame.sequence));
        }
        self.last_sent = frame.sequence;
        self.last_activity = Instant::now();
        self.bytes += bytes;
        self.retained.push_back(Retained { frame: Arc::new(frame), bytes });
        Ok(())
    }
    fn offer_live_target(&mut self, revision: u64, item: &proto::QueryChange) -> anyhow::Result<()> {
        let mut heads = Vec::new();
        if let Some(member) = &item.member {
            heads.push(&member.head);
        }
        // Absent can still carry readable state and events. Its data may need
        // connecting history even though it no longer belongs to the query.
        if let Some(head) = item.delta.as_ref().and_then(proto::DeltaContent::state_head) {
            if !heads.contains(&head) {
                heads.push(head);
            }
        }
        for head in heads {
            let encoded = bincode::serialize(&(item.entity_id, head))?;
            let existing = self.live_target_index.get_key_value(encoded.as_slice());
            if existing.is_some_and(|(_, offered)| offered.last_revision == revision) {
                continue;
            }
            let key: Arc<[u8]> = existing.map(|(key, _)| key.clone()).unwrap_or_else(|| encoded.into());
            let bytes = key.len();
            ensure!(
                self.live_targets.len() < MAX_OFFERED_TARGETS && self.target_bytes + bytes <= WINDOW_BYTES,
                "unapplied live target window exhausted"
            );
            self.target_bytes += bytes;
            let target =
                self.live_target_index.entry(key.clone()).or_insert(LiveTarget { last_revision: revision, publications: 0, attempts: 0 });
            target.last_revision = revision;
            target.publications += 1;
            self.live_targets.push_back(LiveTargetRef { revision, key });
        }
        Ok(())
    }

    fn acknowledge_live(&mut self, revision: u64) -> anyhow::Result<()> {
        if self.applied_through.is_some_and(|previous| revision <= previous) {
            return Ok(());
        }
        let sequence = self
            .completed_live
            .iter()
            .find_map(|(sent, sequence)| (*sent == revision).then_some(*sequence))
            .ok_or_else(|| anyhow::anyhow!("application does not name a completed live publication"))?;
        ensure!(self.processed.is_some_and(|processed| sequence <= processed), "application exceeds processed frames");
        while self.completed_live.front().is_some_and(|(sent, _)| *sent <= revision) {
            self.completed_live.pop_front();
        }
        // Membership-only publications may have no target ledger entry. The
        // ordered complete boundary, rather than the presence of a target, is authoritative.
        while self.live_targets.front().is_some_and(|target| target.revision <= revision) {
            let target = self.live_targets.pop_front().unwrap();
            self.target_bytes -= target.key.len();
            let indexed = self.live_target_index.get_mut(&target.key).expect("retained target is indexed");
            indexed.publications -= 1;
            if indexed.publications == 0 {
                self.live_target_index.remove(&target.key);
            }
        }
        self.applied_through = Some(revision);
        Ok(())
    }

    fn next_live(&mut self, live: &Mutex<LiveQueue>) -> anyhow::Result<Option<StateFrameBody>> {
        if let Some(live) = self.live_chunk.as_mut() {
            let chunk = live.chunks.next().expect("a live delta keeps its unsent chunks");
            let last = chunk.last;
            let body =
                StateFrameBody::LiveChunk { revision: live.revision, member: live.member.clone(), chunk, complete: last && live.complete };
            if last {
                self.live_chunk = None;
            }
            return Ok(Some(body));
        }
        let mut queue = live.lock().unwrap();
        let Some(publication) = queue.publications.front() else { return Ok(None) };
        let revision = publication.revision;
        // A refresh can be enqueued after the pump checked for its marker.
        // Leave later publications queued until the pump installs that cut.
        if queue.pending_round.as_ref().is_some_and(|pending| revision > pending.round.revision) {
            return Ok(None);
        }
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut entities = HashSet::new();
        while let Some(item) = queue.publications.front().unwrap().items.front() {
            // Keep individual event batches for an entity separate, even when
            // a validated refill publishes several at one final membership cut.
            if entities.contains(&item.entity_id) {
                break;
            }
            let size = bincode::serialized_size(item)? as usize;
            if size > DELTA_CHUNK_BYTES {
                if !items.is_empty() {
                    break;
                }
                let item = queue.publications.front_mut().unwrap().items.pop_front().unwrap();
                queue.items -= 1;
                queue.bytes -= size;
                self.offer_live_target(revision, &item)?;
                let content = item.delta.ok_or_else(|| anyhow::anyhow!("oversized live membership metadata"))?;
                let payload = content.encode_payload()?;
                ensure!(payload.len() <= MAX_DELTA_BYTES, "live delta exceeds assembly limit");
                let complete = queue.publications.front().unwrap().items.is_empty();
                if complete {
                    queue.publications.pop_front();
                }
                let chunks = proto::EntityDeltaChunk::split(item.entity_id, payload, DELTA_CHUNK_BYTES);
                self.live_chunk = Some(LiveChunks { revision, chunks, member: item.member, complete });
                drop(queue);
                return self.next_live(live);
            }
            if bytes + size > MAX_PAGE_BYTES || items.len() == MAX_FRAME_ITEMS {
                break;
            }
            let item = queue.publications.front_mut().unwrap().items.pop_front().unwrap();
            self.offer_live_target(revision, &item)?;
            bytes += size;
            entities.insert(item.entity_id);
            queue.bytes -= size;
            queue.items -= 1;
            items.push(item);
        }
        let complete = queue.publications.front().unwrap().items.is_empty();
        if complete {
            queue.publications.pop_front();
        }
        Ok(Some(StateFrameBody::Live { revision, items, complete }))
    }

    fn live_before(&self, live: &Mutex<LiveQueue>, revision: u64) -> bool {
        self.live_chunk.as_ref().is_some_and(|chunk| chunk.revision <= revision)
            || live.lock().unwrap().publications.front().is_some_and(|publication| publication.revision <= revision)
    }

    fn request_live_repairs(&mut self, requests: Vec<proto::RepairRequest>) -> anyhow::Result<()> {
        for proto::RepairRequest { entity_id, target } in requests {
            let key = bincode::serialize(&(entity_id, &target))?;
            let offered =
                self.live_target_index.get_mut(key.as_slice()).ok_or_else(|| anyhow::anyhow!("unsolicited live repair target"))?;
            if self.pending_live_repairs.contains(&key) {
                continue;
            }
            ensure!(self.pending_live_repairs.len() < 128, "too many pending live repairs");
            offered.attempts += 1;
            ensure!(offered.attempts <= 8, "live causal repair retry budget exhausted");
            self.pending_live_repairs.insert(key);
            self.live_repairs.push_back(Work::History { entity_id, target });
        }
        Ok(())
    }

    async fn next_live_repair<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        credentials: &Vec<PA::ContextData>,
        catalog: bool,
    ) -> anyhow::Result<Option<StateFrameBody>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let Some(work) = self.live_repairs.pop_front() else { return Ok(None) };
        let item = match work {
            Work::History { entity_id, target } => match target_history(node, entity_id, &target, credentials, catalog).await {
                Ok(events) => {
                    let content = proto::DeltaContent::EventBridge { events: events.into_iter().map(Into::into).collect() };
                    let payload = content.encode_payload()?;
                    if payload.len() > DELTA_CHUNK_BYTES {
                        let chunks = proto::EntityDeltaChunk::split(entity_id, payload, DELTA_CHUNK_BYTES);
                        self.live_repairs.push_front(Work::Chunks { target, chunks });
                        return Box::pin(self.next_live_repair(node, credentials, catalog)).await;
                    }
                    RepairItem::Delta { target, delta: proto::EntityDelta { entity_id, content } }
                }
                Err(error) => {
                    tracing::debug!(%entity_id, %error, "live causal repair unavailable");
                    RepairItem::Unavailable { entity_id, target }
                }
            },
            Work::Chunks { target, chunks } => {
                let (item, rest) = next_repair_chunk(target, chunks);
                if let Some(rest) = rest {
                    self.live_repairs.push_front(rest);
                }
                item
            }
            _ => unreachable!("only causal repair work enters the live repair queue"),
        };
        let finished = match &item {
            RepairItem::Delta { target, delta } => Some((delta.entity_id, target)),
            RepairItem::Chunk { target, chunk } if chunk.last => Some((chunk.entity_id, target)),
            RepairItem::Unavailable { entity_id, target } => Some((*entity_id, target)),
            _ => None,
        };
        if let Some((id, target)) = finished {
            self.pending_live_repairs.remove(&bincode::serialize(&(id, target))?);
        }
        Ok(Some(StateFrameBody::LiveRepair { items: vec![item] }))
    }
}

pub(crate) fn state_stream_bypasses_policy<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    body: &proto::NodeRequestBody,
    outgoing: bool,
) -> bool
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let id = match body {
        proto::NodeRequestBody::StateFeedback { stream, .. }
        | proto::NodeRequestBody::FinishStateRound { stream, .. }
        | proto::NodeRequestBody::RefreshState { stream, .. } => stream,
        _ => return crate::schema::request_bypasses_policy(body),
    };
    let Some(connection) = node.peer_connections.get(&peer) else { return false };
    if outgoing {
        connection.outgoing_state_streams.get(id).is_some_and(|catalog| catalog)
    } else {
        connection.state_streams.bypasses_policy(id)
    }
}

/// A registered stream whose initial frame is still being prepared. Live
/// publications can queue at its captured cut without holding a shared tree.
/// Cancellation removes the registration synchronously.
pub(crate) struct PendingStateStream<CD: ContextData> {
    connection: std::sync::Weak<crate::node::PeerState<CD>>,
    session: Arc<Session<CD>>,
    peer: proto::EntityId,
    id: StateStreamId,
    armed: bool,
}
impl<CD: ContextData> PendingStateStream<CD> {
    pub(crate) fn id(&self) -> StateStreamId { self.id }

    pub(crate) async fn initialize<SE, PA>(mut self, node: &Node<SE, PA>) -> anyhow::Result<proto::StateReply>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let connection = self.connection.upgrade().ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let mut state = self.session.state.lock().await;
        let round = self.session.initial_round;
        let comparison = state.comparison.as_mut().ok_or_else(|| anyhow::anyhow!("initial comparison unavailable"))?;
        let body = comparison.reply(node, &self.session.credentials, self.session.catalog, round.number).await?;
        ensure!(
            node.peer_connections.get(&self.peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
                && connection.state_streams.get(&self.id).is_some_and(|current| Arc::ptr_eq(&current, &self.session)),
            "initial stream was closed"
        );
        ensure!(!self.session.live.lock().unwrap().stopped, "initial live queue stopped");
        let frame = StateFrame { stream: self.id, sequence: 0, body };
        state.retain(frame.clone(), false)?;
        *self.session.initial.lock().unwrap() = Some(Arc::new(frame.clone()));
        self.session.initialized.store(true, Ordering::Release);
        self.armed = false;
        Ok(proto::StateReply { round, frame })
    }
}
impl<CD: ContextData> Drop for PendingStateStream<CD> {
    fn drop(&mut self) {
        if self.armed {
            if let Some(connection) = self.connection.upgrade() {
                connection.state_streams.finish(self.id);
            }
        }
    }
}

pub(crate) async fn start_state_stream<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    request: proto::RequestId,
    fingerprint: Vec<u8>,
    credentials: Vec<PA::ContextData>,
    query: Option<(proto::QueryId, u32)>,
    catalog: bool,
    revision: u64,
    snapshot: Snapshot,
    known: proto::KnownState,
) -> anyhow::Result<proto::StateReply>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    prepare_state_stream(node, peer, request, fingerprint, credentials, query, catalog, revision, snapshot, known)?.initialize(node).await
}

pub(crate) fn prepare_state_stream<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    request: proto::RequestId,
    fingerprint: Vec<u8>,
    credentials: Vec<PA::ContextData>,
    query: Option<(proto::QueryId, u32)>,
    catalog: bool,
    revision: u64,
    snapshot: Snapshot,
    known: proto::KnownState,
) -> anyhow::Result<PendingStateStream<PA::ContextData>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let comparison = Comparison::new(snapshot, known)?;
    let spec = comparison.snapshot.spec.clone();
    let id = StateStreamId::new();
    let round = proto::StateRound { number: 0, revision };
    let state = Stream {
        comparison: Some(comparison),
        round,
        pending_round: None,
        retired_round: None,
        retained: VecDeque::new(),
        bytes: 0,
        last_sent: 0,
        processed: None,
        required_through: 0,
        failed: false,
        last_activity: Instant::now(),
        feedback: VecDeque::new(),
        feedback_bytes: 0,
        retired_feedback: HashSet::new(),
        live_chunk: None,
        live_targets: VecDeque::new(),
        live_target_index: HashMap::new(),
        target_bytes: 0,
        live_repairs: VecDeque::new(),
        pending_live_repairs: HashSet::new(),
        last_live_complete: None,
        completed_live: VecDeque::new(),
        applied_through: None,
        next_lane: 0,
    };
    let (cancel_expiry, mut cancelled) = futures::channel::oneshot::channel();
    let session = Arc::new(Session {
        catalog,
        credentials,
        query,
        initial_request: request,
        initial: Mutex::new(None),
        initial_round: round,
        sent: AtomicU64::new(0),
        spec,
        fingerprint,
        state: tokio::sync::Mutex::new(state),
        live: Mutex::new(LiveQueue { round: Some(round), round_open: true, ..Default::default() }),
        initialized: AtomicBool::new(false),
        pumping: AtomicBool::new(false),
        wake: AtomicBool::new(false),
        _cancel_expiry: cancel_expiry,
    });
    {
        let mut sessions = connection.state_streams.sessions.lock().unwrap();
        let mut origins = connection.state_streams.origins.lock().unwrap();
        ensure!(sessions.len() < MAX_OPEN_STREAMS, "too many open state streams");
        ensure!(origins.len() < MAX_ORIGINS, "connection exhausted its bounded origin history");
        ensure!(!origins.contains_key(&session.initial_request), "initial request origin was already used");
        ensure!(!sessions.values().any(|s| s.initial_request == session.initial_request), "initial request already has a stream");
        origins.insert(session.initial_request.clone(), id);
        sessions.insert(id, session.clone());
    }
    let pending = PendingStateStream { connection: Arc::downgrade(&connection), session, peer, id, armed: true };
    let node = node.weak();
    crate::task::spawn(async move {
        loop {
            let expiry = futures_timer::Delay::new(STREAM_TTL);
            if matches!(futures::future::select(expiry, &mut cancelled).await, futures::future::Either::Right(_)) {
                break;
            }
            let Some(node) = node.upgrade() else { break };
            let Some(connection) = node.peer_connections.get(&peer) else { break };
            let Some(session) = connection.state_streams.get(&id) else { break };
            let state = session.state.lock().await;
            let idle = state.last_activity.elapsed() >= STREAM_TTL
                && (state.comparison.is_some()
                    || state.pending_round.is_some()
                    || !state.retained.is_empty()
                    || !state.live_targets.is_empty()
                    || !session.live.lock().unwrap().publications.is_empty());
            drop(state);
            if idle {
                let _ = expire_state_stream(&node, peer, id).await;
                break;
            }
        }
    });
    Ok(pending)
}

pub(crate) async fn apply_state_feedback<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    request: proto::RequestId,
    id: StateStreamId,
    processed_through: u64,
    applied_through: Option<u64>,
    answers: Option<proto::FeedbackAnswers>,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let has_answers = answers.as_ref().is_some_and(|answers| !answers.is_empty());
    let Some(session) = connection.state_streams.get(&id) else {
        return Ok(if !has_answers && connection.state_streams.permits_retired_receipt(id, processed_through, &credentials) {
            proto::NodeResponseBody::Success
        } else {
            proto::NodeResponseBody::StateStreamUnavailable
        });
    };
    ensure!(session.permits(&credentials), "state stream credentials changed");
    if let Some(answers) = &answers {
        answers.validate(&session.spec)?;
    }
    let encoded = bincode::serialize(&(processed_through, applied_through, &answers))?;
    ensure!(encoded.len() <= MAX_FRAME_BYTES, "state feedback exceeds limit");
    let mut state = session.state.lock().await;
    if !connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
        return Ok(proto::NodeResponseBody::StateStreamUnavailable);
    }
    if state.failed {
        if has_answers {
            return Ok(proto::NodeResponseBody::StateStreamUnavailable);
        }
        state.acknowledge(processed_through)?;
        if processed_through == state.last_sent {
            connection.state_streams.finish(id);
        }
        return Ok(proto::NodeResponseBody::Success);
    }
    if let Some((_, previous)) = state.feedback.iter().find(|(previous, _)| previous == &request) {
        ensure!(previous == &encoded, "feedback retry changed its contents");
        return Ok(proto::NodeResponseBody::Success);
    }
    if state.retired_feedback.contains(&request) {
        return Ok(proto::NodeResponseBody::StateStreamUnavailable);
    }
    ensure!(state.retired_feedback.len() < MAX_FEEDBACK_IDS, "stream exhausted control replay history");
    state.acknowledge(processed_through)?;
    session.initial.lock().unwrap().take();
    match answers {
        Some(proto::FeedbackAnswers::Round { round, answers }) => {
            ensure!(round <= state.round.number, "feedback names a future round");
            // A delayed repair question may arrive after live application covered
            // its target and Applied retired the round. It cannot author current
            // membership, and retaining the old snapshot merely to reject it
            // would make round retirement race normal application progress.
            if round == state.round.number {
                if let Some(comparison) = state.comparison.as_mut() {
                    comparison.answer(answers)?;
                }
            }
        }
        Some(proto::FeedbackAnswers::LiveRepair { requests }) => state.request_live_repairs(requests)?,
        None => {}
    }
    if let Some(revision) = applied_through {
        state.acknowledge_live(revision)?;
    }
    state.feedback_bytes += encoded.len();
    state.feedback.push_back((request, encoded));
    while state.feedback.len() > MAX_FEEDBACK || state.feedback_bytes > CONTROL_BYTES {
        let (retired, bytes) = state.feedback.pop_front().unwrap();
        state.feedback_bytes -= bytes.len();
        state.retired_feedback.insert(retired);
    }
    drop(state);
    kick_state_stream(node, peer, id);
    Ok(proto::NodeResponseBody::Success)
}

pub(crate) async fn finish_state_round<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: StateStreamId,
    round: u64,
    outcome: proto::StateRoundOutcome,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let Some(session) = connection.state_streams.get(&id) else {
        return Ok(if connection.state_streams.was_closed(&id) {
            proto::NodeResponseBody::Success
        } else {
            proto::NodeResponseBody::StateStreamUnavailable
        });
    };
    ensure!(session.permits(&credentials), "state stream credentials changed");
    let mut state = session.state.lock().await;
    if !connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) || state.failed {
        return Ok(proto::NodeResponseBody::StateStreamUnavailable);
    }
    if state.retired_round == Some((round, outcome)) {
        return Ok(proto::NodeResponseBody::Success);
    }
    ensure!(round == state.round.number, "FinishStateRound names a retired or future round");
    let comparison = state.comparison.as_ref().ok_or_else(|| anyhow::anyhow!("round is already retired"))?;
    if let proto::StateRoundOutcome::Applied { processed_through } = outcome {
        ensure!(
            comparison.complete && comparison.summaries.is_empty() && comparison.head_requests.is_empty(),
            "comparison enumeration is not complete"
        );
        ensure!(processed_through >= state.required_through, "Applied does not cover the completed enumeration");
        state.acknowledge(processed_through)?;
    }
    session.initial.lock().unwrap().take();
    state.comparison = None;
    state.retired_round = Some((round, outcome));
    session.live.lock().unwrap().round_open = false;
    if session.query.is_none() {
        connection.state_streams.finish(id);
    }
    drop(state);
    kick_state_stream(node, peer, id);
    Ok(proto::NodeResponseBody::Success)
}

pub(crate) async fn abandon_state_stream<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: StateStreamId)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // Cleanup grants no data access. Like unsubscribe, ownership is the peer's
    // connection plus its responder-issued handle, not a fresh credential check.
    let Some(connection) = node.peer_connections.get(&peer) else { return };
    let Some(session) = connection.state_streams.get(&id) else { return };
    let state = session.state.lock().await;
    if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
        || !connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
    {
        return;
    }
    // Failed streams must remain abandonable, without asserting round coverage.
    connection.state_streams.finish(id);
    drop(state);
    if let Some((query, version)) = session.query {
        connection.subscription_handler.abandon_state_stream(query, version, id).await;
    }
}

/// Reserve a round cut in the same short queue lock as live publications.
/// Preparing or sending another frame never blocks a shared tree's updater.
pub(crate) fn queue_round<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: StateStreamId,
    number: u64,
    revision: u64,
    snapshot: Snapshot,
    known: proto::KnownState,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let session = connection.state_streams.get(&id).ok_or_else(|| anyhow::anyhow!("state stream unavailable"))?;
    ensure!(session.query.is_some(), "Fetch cannot refresh a standing query");
    ensure!(snapshot.spec == session.spec && known.spec == session.spec, "refresh changed tree specification");
    let encoded = bincode::serialize(&known)?;
    ensure!(encoded.len() <= MAX_OFFER_BYTES + MAX_FRAME_BYTES, "refresh offer exceeds limit");
    let comparison = Comparison::new(snapshot, known)?;
    let mut queue = session.live.lock().unwrap();
    ensure!(!queue.stopped, "live stream stopped");
    if let Some((previous, offer)) = &queue.last_refresh {
        if *previous == number {
            ensure!(*offer == encoded, "refresh retry changed frozen offer");
            return Ok(());
        }
    }
    ensure!(!queue.round_open && queue.pending_round.is_none(), "a comparison round is already active");
    let previous = queue.round.ok_or_else(|| anyhow::anyhow!("stream has no initial round"))?;
    ensure!(previous.number.checked_add(1) == Some(number) && revision >= previous.revision, "invalid next round");
    ensure!(queue.last_revision.is_none_or(|queued| queued <= revision), "round cut precedes queued live state");
    let round = proto::StateRound { number, revision };
    queue.pending_round = Some(PendingRound { round, comparison });
    queue.last_refresh = Some((number, encoded));
    queue.round = Some(round);
    queue.round_open = true;
    drop(queue);
    kick_state_stream(node, peer, id);
    Ok(())
}

#[cfg(test)]
pub(crate) async fn schedule_round<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: StateStreamId,
    number: u64,
    revision: u64,
    snapshot: Snapshot,
    known: proto::KnownState,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    queue_round(node, peer, id, number, revision, snapshot, known)
}

/// Queue a live publication's `changes` on stream `id`, each with the delta
/// the stream's principal may read, built from `read`; a delta it may not
/// build stops the stream.
pub(crate) fn queue_live_changes<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: StateStreamId,
    revision: u64,
    changes: Vec<LiveChange>,
    read: &LiveRead,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let session = connection.state_streams.get(&id).ok_or_else(|| anyhow::anyhow!("live stream unavailable"))?;
    match live_items(node, &session.credentials, changes, read) {
        Ok(items) => queue_live(node, peer, id, revision, items),
        Err(error) => {
            session.live.lock().unwrap().stopped = true;
            kick_state_stream(node, peer, id);
            Err(error)
        }
    }
}

pub(crate) fn queue_live<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: StateStreamId,
    revision: u64,
    items: Vec<proto::QueryChange>,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let session = connection.state_streams.get(&id).ok_or_else(|| anyhow::anyhow!("live stream unavailable"))?;
    ensure!(session.query.is_some(), "Fetch has no live route");
    ensure!(revision > session.initial_round.revision, "live publication does not follow the initial cut");
    for item in &items {
        if let Some(delta) = &item.delta {
            delta.live_target(item.member.as_ref())?;
        }
    }
    let sizes = items.iter().map(|item| bincode::serialized_size(item).map(|size| size as usize)).collect::<Result<Vec<_>, _>>()?;
    let bytes = sizes.iter().sum::<usize>();
    let mut queue = session.live.lock().unwrap();
    if queue.stopped
        || sizes.iter().any(|size| *size > MAX_DELTA_BYTES)
        || queue.items + items.len() > MAX_LIVE_ITEMS
        || queue.publications.len() >= MAX_LIVE_ITEMS
        || queue.bytes.saturating_add(bytes) > MAX_LIVE_BYTES
        || queue.last_revision.is_some_and(|previous| revision <= previous)
    {
        queue.stopped = true;
        drop(queue);
        kick_state_stream(node, peer, id);
        bail!("live queue stopped, out of order, or exhausted");
    }
    queue.bytes += bytes;
    queue.items += items.len();
    queue.last_revision = Some(revision);
    queue.publications.push_back(LivePublication { revision, items: items.into() });
    drop(queue);
    kick_state_stream(node, peer, id);
    Ok(())
}

/// The items one stream delivers for `changes`: each change's member and,
/// when the stream's principal may read the entity's current state, that
/// state attested with the events the publication committed for it.
pub(crate) fn live_items<SE, PA>(
    node: &Node<SE, PA>,
    credentials: &Vec<PA::ContextData>,
    changes: Vec<LiveChange>,
    read: &LiveRead,
) -> anyhow::Result<Vec<proto::QueryChange>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
    changes
        .into_iter()
        .map(|LiveChange { entity_id, member }| {
            let delta = match read.state(&entity_id) {
                Some(state) if policy.check_read(&entity_id, &state.state).is_ok() => {
                    let events = read.events(&entity_id)?;
                    for event in &events {
                        policy.check_read_event(event, &state.state)?;
                    }
                    let attestation = node.policy_agent.attest_state(node, state);
                    Some(proto::DeltaContent::StateAndEvents {
                        state: proto::Attested::opt(state.clone(), attestation).into(),
                        events: events.into_iter().cloned().map(Into::into).collect(),
                    })
                }
                _ => None,
            };
            Ok(proto::QueryChange { entity_id, member, delta })
        })
        .collect()
}

pub(crate) fn kick_state_stream<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: StateStreamId)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let Some(connection) = node.peer_connections.get(&peer) else { return };
    let Some(session) = connection.state_streams.get(&id) else { return };
    if !session.initialized.load(Ordering::Acquire) {
        return;
    }
    session.wake.store(true, Ordering::Release);
    if session.pumping.swap(true, Ordering::AcqRel) {
        return;
    }
    let node = node.weak();
    crate::task::spawn(async move {
        loop {
            session.wake.store(false, Ordering::Release);
            let Some(node) = node.upgrade() else { break };
            if let Err(error) = pump_state_stream(&node, peer, id).await {
                tracing::warn!(%error, "state stream failed");
                let _ = expire_state_stream(&node, peer, id).await;
                break;
            }
            if session.wake.swap(false, Ordering::AcqRel) {
                continue;
            }
            // Publish idle before rechecking wake. A racing kick either starts
            // a new pump or leaves a wake for this pump to reclaim.
            session.pumping.store(false, Ordering::Release);
            if session.wake.swap(false, Ordering::AcqRel) && !session.pumping.swap(true, Ordering::AcqRel) {
                continue;
            }
            return;
        }
        session.pumping.store(false, Ordering::Release);
    });
}

pub(crate) async fn pump_state_stream<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: StateStreamId) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let Some(session) = connection.state_streams.get(&id) else { return Ok(()) };
    if !session.initialized.load(Ordering::Acquire) {
        return Ok(());
    }
    let mut state = session.state.lock().await;
    if state.failed || !connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
        return Ok(());
    }
    ensure!(!session.live.lock().unwrap().stopped, "live queue stopped");
    loop {
        if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
            || !connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
        {
            break;
        }
        if state.pending_round.is_none() {
            state.pending_round = session.live.lock().unwrap().pending_round.take();
        }
        // Round markers cannot split a source publication. Before activating
        // the next cut, drain every queued publication included in that cut.
        let mut chosen = None;
        if let Some(pending) = &state.pending_round {
            if state.live_before(&session.live, pending.round.revision) {
                if state.room(false) {
                    chosen = state.next_live(&session.live)?.map(|body| (body, false));
                }
            } else if state.room(false) {
                let pending = state.pending_round.take().unwrap();
                state.round = pending.round;
                state.comparison = Some(pending.comparison);
                chosen = Some((StateFrameBody::BeginRound(state.round), false));
            }
        } else {
            // Rotate among baseline, live publication, and connecting repair.
            // A blocked baseline never prevents another lane from using credit.
            for _ in 0..3 {
                let lane = state.next_lane;
                state.next_lane = (lane + 1) % 3;
                match lane {
                    0 => {
                        let priority = state.comparison.as_ref().is_some_and(|c| !c.pending_repairs.is_empty());
                        if !state.room(priority) {
                            continue;
                        }
                        let round = state.round.number;
                        if let Some(comparison) = state.comparison.as_mut() {
                            if !comparison.work.is_empty() && !comparison.waiting_for_capacity() {
                                let body = comparison.reply(node, &session.credentials, session.catalog, round).await?;
                                chosen = Some((body, priority));
                            }
                        }
                    }
                    1 if state.room(false) => chosen = state.next_live(&session.live)?.map(|body| (body, false)),
                    2 if state.room(true) => {
                        chosen = state.next_live_repair(node, &session.credentials, session.catalog).await?.map(|body| (body, true))
                    }
                    _ => {}
                }
                if chosen.is_some() {
                    break;
                }
            }
        }
        if chosen.is_none() && state.room(true) {
            chosen = state.next_live_repair(node, &session.credentials, session.catalog).await?.map(|body| (body, true));
        }
        let Some((body, priority)) = chosen else { break };
        let sequence = state.last_sent.checked_add(1).ok_or_else(|| anyhow::anyhow!("state sequence exhausted"))?;
        let frame = StateFrame { stream: id, sequence, body };
        if !connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
            break;
        }
        if matches!(frame.body, StateFrameBody::BeginRound(_) | StateFrameBody::Round { progress: ComparisonProgress::Complete, .. }) {
            // Applied covers the finite enumeration. Later requested history may
            // become unnecessary because another live update covers the target;
            // those frames still need wire credit, not another readiness fence.
            state.required_through = sequence;
        }
        state.retain(frame.clone(), priority)?;
        session.sent.store(sequence, Ordering::Release);
        connection.send_message(proto::NodeMessage::StateFrame {
            from: node.id,
            to: peer,
            request_id: session.initial_request.clone(),
            frame,
        })?;
    }
    Ok(())
}

pub(crate) async fn expire_state_stream<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: StateStreamId) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fail_stream(node, peer, id, StateFrameBody::RestartRequired).await
}

pub(crate) async fn source_unavailable<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: StateStreamId) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fail_stream(node, peer, id, StateFrameBody::SourceUnavailable).await
}

async fn fail_stream<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: StateStreamId, body: StateFrameBody) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let Some(connection) = node.peer_connections.get(&peer) else { return Ok(()) };
    let Some(session) = connection.state_streams.get(&id) else { return Ok(()) };
    let mut state = session.state.lock().await;
    if state.failed {
        return Ok(());
    }
    state.failed = true;
    state.comparison = None;
    state.pending_round = None;
    state.live_chunk = None;
    state.live_targets.clear();
    state.live_target_index.clear();
    state.completed_live.clear();
    state.target_bytes = 0;
    state.live_repairs.clear();
    state.pending_live_repairs.clear();
    session.initial.lock().unwrap().take();
    {
        let mut live = session.live.lock().unwrap();
        live.stopped = true;
        live.pending_round = None;
        live.last_refresh = None;
        live.publications.clear();
        live.items = 0;
        live.bytes = 0;
    }
    let frame = StateFrame { stream: id, sequence: state.last_sent.saturating_add(1), body };
    let retained = state.room(true) && state.retain(frame.clone(), true).is_ok();
    session.sent.store(state.last_sent, Ordering::Release);
    drop(state);
    if let Some((query, version)) = session.query {
        connection.subscription_handler.abandon_state_stream(query, version, id).await;
    }
    if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection)) {
        return Ok(());
    }
    if !retained
        || connection
            .send_message(proto::NodeMessage::StateFrame {
                from: node.id,
                to: peer,
                request_id: session.initial_request.clone(),
                frame: frame.clone(),
            })
            .is_err()
    {
        node.deregister_peer(peer);
        return Ok(());
    }
    let node = node.weak();
    crate::task::spawn(async move {
        // Retry RestartRequired three times, one second apart. If it remains
        // unacknowledged, disconnect only this still-current peer and stream.
        for _ in 0..3 {
            futures_timer::Delay::new(Duration::from_secs(1)).await;
            let Some(node) = node.upgrade() else { return };
            if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
                || !connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
            {
                return;
            }
            if connection
                .send_message(proto::NodeMessage::StateFrame {
                    from: node.id,
                    to: peer,
                    request_id: session.initial_request.clone(),
                    frame: frame.clone(),
                })
                .is_err()
            {
                node.deregister_peer(peer);
                return;
            }
        }
        if let Some(node) = node.upgrade() {
            if node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
                && connection.state_streams.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
            {
                node.deregister_peer(peer);
            }
        }
    });
    Ok(())
}

async fn target_history<SE, PA>(
    node: &Node<SE, PA>,
    entity_id: proto::EntityId,
    target: &proto::Clock,
    credentials: &Vec<PA::ContextData>,
    catalog: bool,
) -> anyhow::Result<Vec<proto::Attested<proto::Event>>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
    let state = node_state(node, entity_id, &policy).await?.ok_or_else(|| anyhow::anyhow!("repair state unavailable"))?;
    policy.check_read(&entity_id, &state.payload.state)?;
    ensure!(!catalog || state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy), "repair is outside catalog scope");
    ensure!(!target.is_empty(), "empty causal repair target");
    let mut pending = target.entries().iter().map(|(generation, id)| (id.clone(), *generation)).collect::<HashMap<_, _>>();
    let mut visited = HashSet::new();
    let mut events = Vec::new();
    let mut bytes = 0usize;
    let mut genesis = None;
    while !pending.is_empty() {
        ensure!(visited.len() + pending.len() <= MAX_HISTORY, "causal repair history budget exhausted");
        let batch = std::mem::take(&mut pending);
        let fetched = node.storage.get_events(batch.keys().cloned().collect()).await?;
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
            bytes =
                bytes.checked_add(bincode::serialized_size(&event)? as usize).ok_or_else(|| anyhow::anyhow!("history size overflow"))?;
            ensure!(bytes <= MAX_DELTA_BYTES, "causal repair exceeds byte limit");
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
    // Resident handles contain the node's current committed state. Re-attest
    // that state instead of reloading it; callers still authorize this payload.
    if let Some(entity) = node.entities.get(&id) {
        let state = entity.to_entity_state()?;
        let attestation = node.policy_agent.attest_state(node, &state);
        return Ok(Some(proto::Attested::opt(state, attestation)));
    }
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
            Some(Work::AskHeads(_)) => self.head_requests.len() >= 128,
            _ => false,
        }
    }
    fn new(snapshot: Snapshot, known: proto::KnownState) -> anyhow::Result<Self> {
        let mut comparison = Self {
            snapshot,
            work: VecDeque::new(),
            summaries: HashMap::new(),
            head_requests: Vec::new(),
            repaired: HashSet::new(),
            known_clocks: HashMap::new(),
            offered: HashSet::new(),
            pending_repairs: HashSet::new(),
            repair_attempts: HashMap::new(),
            complete: false,
            unavailable: false,
        };
        ensure!(known.spec == comparison.snapshot.spec, "unsupported comparison tree spec");
        known.validate()?;
        // A valid offer's spans are disjoint and within the tree; plan them in address order.
        let mut spans = known.spans;
        spans.sort_by(|a, b| a.bounds().cmp_start(b.bounds()));
        let mut start = proto::TreeSpan::all().start;
        let mut frontier = Some(Vec::new());
        for region in spans {
            let bounds = region.bounds();
            if start != bounds.start {
                frontier = None;
                let gap = TreeSpan::new(start.clone(), opposite(&bounds.start));
                if !gap.is_empty() {
                    comparison.work.push_back(Work::SendHeads(gap));
                }
            }
            start = opposite(&bounds.end);
            match region {
                proto::SpanContent::Digest(head) => {
                    if let Some(frontier) = &mut frontier {
                        frontier.push(head.clone());
                    }
                    comparison.work.push_back(Work::Compare { prefix: NodePrefix::root(), head });
                }
                proto::SpanContent::Heads(page) => {
                    frontier = None;
                    let mut cursor = page.heads.cursor();
                    while let Some(head) = cursor.next() {
                        comparison.known_clocks.insert(head.entity.entity_id, head.entity.head.to_owned());
                    }
                    comparison.work.push_back(Work::InitialHeads(InitialHeads { bounds: page.bounds, heads: page.heads.cursor() }));
                }
            }
        }
        if !matches!(start, std::ops::Bound::Unbounded) {
            let rest = TreeSpan::new(start, proto::TreeSpan::all().end);
            if !rest.is_empty() {
                comparison.work.push_back(Work::SendHeads(rest));
            }
        } else if let Some(frontier) = frontier.filter(|heads| heads.len() > 1) {
            comparison.work.clear();
            comparison.work.push_back(Work::CompareFrontier(frontier));
        }
        Ok(comparison)
    }

    /// Takes this round's answers, which `FeedbackAnswers::validate` already checked.
    fn answer(&mut self, answers: Vec<StateAnswer>) -> anyhow::Result<()> {
        for answer in answers {
            match answer {
                StateAnswer::NeedRepair(proto::RepairRequest { entity_id, target }) => {
                    let key = target_id(entity_id, &target);
                    ensure!(self.offered.contains(&key), "unsolicited causal repair target");
                    if self.pending_repairs.contains(&key) {
                        continue;
                    }
                    ensure!(self.pending_repairs.len() < 128, "too many pending causal repairs");
                    let attempts = self.repair_attempts.entry(key.clone()).or_default();
                    *attempts += 1;
                    ensure!(*attempts <= 8, "causal repair retry budget exhausted");
                    self.pending_repairs.insert(key);
                    self.work.push_front(Work::History { entity_id, target });
                }
                StateAnswer::Digest(head) => {
                    let prefix =
                        self.summaries.remove(&range_id(&head.bounds)).ok_or_else(|| anyhow::anyhow!("unsolicited summary bounds"))?;
                    self.work.push_back(Work::Compare { head, prefix });
                }
                StateAnswer::Heads(page) => {
                    let index = self
                        .head_requests
                        .iter()
                        .position(|bounds| bounds.start == page.bounds.start)
                        .ok_or_else(|| anyhow::anyhow!("unsolicited head inventory"))?;
                    let requested = self.head_requests[index].clone();
                    ensure!(page.bounds.is_subset_of(&requested), "head inventory exceeds requested bounds");
                    let mut last_address = None;
                    let mut cursor = page.heads.cursor();
                    let mut seen = 0;
                    while let Some(leaf) = cursor.next() {
                        seen += 1;
                        if seen == page.heads.len() {
                            last_address = Some(leaf_address(leaf.key, leaf.entity.entity_id));
                        }
                    }
                    let rest = if page.bounds.end != requested.end {
                        let std::ops::Bound::Included(last) = &page.bounds.end else { bail!("partial page must end at its last leaf") };
                        ensure!(last_address.as_ref() == Some(last), "partial page made no progress");
                        Some(requested.after(last))
                    } else {
                        None
                    };
                    self.head_requests.remove(index);
                    // Accept each validated answer once, but expand only one
                    // region at a time while the producer drains its repairs.
                    self.work.push_back(Work::AnswerHeads { page, rest });
                }
            }
        }
        Ok(())
    }

    fn expand_initial_heads(&mut self, mut page: InitialHeads) -> anyhow::Result<()> {
        use std::ops::Bound::{Excluded, Included};

        // The request carries one inventory. Process it in bounded batches so
        // alternating gaps/removals cannot expand the whole work queue at once.
        let mut work = Vec::new();
        let mut former = Vec::new();
        let mut start = page.bounds.start.clone();
        let mut removal: Option<TreeSpan> = None;
        let flush_removal = |removal: &mut Option<TreeSpan>, work: &mut Vec<Work>| {
            if let Some(bounds) = removal.take() {
                work.push(Work::Item(StateItem::Heads(proto::TreeSpanInventory { bounds, heads: Default::default() })));
            }
        };
        let count = page.heads.len().min(MAX_PAGE_HEADS);
        for _ in 0..count {
            let leaf = page.heads.next().expect("bounded by remaining rows");
            if !self.snapshot.entries.contains_key(&leaf.entity.entity_id) {
                former.push(leaf.entity.entity_id);
            }
            let address = leaf_address(leaf.key, leaf.entity.entity_id);
            let gap = TreeSpan::new(start, Excluded(address.clone()));
            if !gap.is_empty() && self.snapshot.entries_in(&gap, 1).next().is_some() {
                flush_removal(&mut removal, &mut work);
                work.push(Work::SendHeads(gap));
            }
            let point = TreeSpan::new(Included(address.clone()), Included(address.clone()));
            if let Some((entity_id, entry)) = self.snapshot.entries_in(&point, 1).next() {
                flush_removal(&mut removal, &mut work);
                if !leaf.entity.head.equals(&entry.head) {
                    work.push(Work::Item(StateItem::Heads(proto::TreeSpanInventory {
                        bounds: point,
                        heads: vec![proto::KeyedEntityHead {
                            key: entry.key().to_vec(),
                            entity: proto::EntityHead { entity_id, head: entry.head.clone() },
                        }]
                        .try_into()?,
                    })));
                    work.push(Work::Repair(leaf.entity.entity_id, Some(leaf.entity.head.to_owned())));
                }
            } else {
                // Only bridge removed points across gaps proven empty above.
                // Membership patches remain independent of repair deduplication.
                if let Some(removal) = &mut removal {
                    removal.end = Included(address.clone());
                } else {
                    removal = Some(point);
                }
                work.push(Work::Repair(leaf.entity.entity_id, Some(leaf.entity.head.to_owned())));
            }
            start = Excluded(address);
        }
        flush_removal(&mut removal, &mut work);
        let processed_bounds = if page.heads.is_empty() {
            page.bounds.clone()
        } else {
            let Excluded(last) = &start else { unreachable!("nonempty batch") };
            TreeSpan::new(page.bounds.start.clone(), Included(last.clone()))
        };
        let gap = TreeSpan::new(start.clone(), processed_bounds.end.clone());
        if !gap.is_empty() && self.snapshot.entries_in(&gap, 1).next().is_some() {
            work.push(Work::SendHeads(gap));
        }
        // Splitting a valid region can repeat long keys in its bounds. Keep
        // the original regional exchange when those patches cannot fit.
        let mut fits = true;
        for item in &work {
            fits &= match item {
                Work::Item(item) => bincode::serialized_size(item)? <= MAX_PAGE_BYTES as u64,
                Work::SendHeads(bounds) => {
                    let item = StateItem::Heads(proto::TreeSpanInventory { bounds: bounds.clone(), heads: Default::default() });
                    bincode::serialized_size(&item)? <= (MAX_PAGE_BYTES / 2) as u64
                }
                _ => true,
            };
        }
        if !fits {
            work = former.into_iter().map(|id| Work::Repair(id, Some(self.known_clocks[&id].clone()))).collect();
            work.push(Work::SendHeads(processed_bounds));
        }
        if !page.heads.is_empty() {
            page.bounds.start = start;
            self.work.push_front(Work::InitialHeads(page));
        }
        for item in work.into_iter().rev() {
            self.work.push_front(item);
        }
        Ok(())
    }

    fn expand_head_answer(&mut self, page: proto::TreeSpanInventory, rest: Option<TreeSpan>) -> anyhow::Result<()> {
        let mut clocks = std::collections::BTreeMap::new();
        let mut cursor = page.heads.cursor();
        while let Some(leaf) = cursor.next() {
            clocks.insert(leaf.entity.entity_id, leaf.entity.head.to_owned());
        }
        let mut remaining = page.bounds.clone();
        let mut pages = Vec::new();
        let mut repairs = Vec::new();
        let mut count = 0;
        loop {
            let remote = self.snapshot.page(remaining.clone())?;
            count += remote.heads.len();
            ensure!(count <= MAX_PAGE_HEADS, "head span was not fully refined");
            let mut cursor = remote.heads.cursor();
            while let Some(leaf) = cursor.next() {
                let id = leaf.entity.entity_id;
                repairs.push(Work::Repair(id, clocks.remove(&id)));
            }
            let complete = remote.bounds.end == page.bounds.end;
            if !complete {
                let std::ops::Bound::Included(last) = &remote.bounds.end else { bail!("partial page must make progress") };
                remaining = page.bounds.after(last);
            }
            pages.push(Work::Item(StateItem::Heads(remote)));
            if complete {
                break;
            }
        }
        repairs.extend(clocks.into_iter().map(|(id, clock)| Work::Repair(id, Some(clock))));
        pages.extend(repairs);
        if let Some(rest) = rest {
            pages.push(Work::AskHeads(rest));
        }
        // Keep this bounded region's pages and repairs ahead of later answers,
        // rather than multiplying all pending answers into thousands of items.
        for work in pages.into_iter().rev() {
            self.work.push_front(work);
        }
        Ok(())
    }

    async fn reply<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        credentials: &Vec<PA::ContextData>,
        catalog: bool,
        round: u64,
    ) -> anyhow::Result<StateFrameBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut tree_or_history_work = 0;
        let repair_frame = !self.pending_repairs.is_empty();
        for _ in 0..MAX_FRAME_STEPS {
            if repair_frame && self.pending_repairs.is_empty() {
                break;
            }
            ensure!(self.work.len() <= 4096, "comparison work queue exhausted");
            if self.waiting_for_capacity() {
                break;
            }
            let Some(work) = self.work.pop_front() else { break };
            // An unknown-head repair reads only current state. Keep an existing
            // resident alive through that read so it cannot become storage work.
            let resident = match &work {
                Work::Repair(id, None) => node.entities.get(id),
                _ => None,
            };
            // Known-head repairs may walk history even when their entity is resident.
            let expensive = match &work {
                Work::Repair(..) => resident.is_none(),
                Work::InitialHeads(_)
                | Work::AnswerHeads { .. }
                | Work::CompareFrontier(_)
                | Work::Compare { .. }
                | Work::Expand { .. }
                | Work::SendHeads(_)
                | Work::History { .. } => true,
                _ => false,
            };
            if expensive {
                if tree_or_history_work == MAX_FRAME_TREE_WORK {
                    self.work.push_front(work);
                    break;
                }
                tree_or_history_work += 1;
            }
            let item = match work {
                Work::History { entity_id, target } => match target_history(node, entity_id, &target, credentials, catalog).await {
                    Ok(events) => {
                        let delta = proto::EntityDelta {
                            entity_id,
                            content: proto::DeltaContent::EventBridge { events: events.into_iter().map(Into::into).collect() },
                        };
                        let payload = delta.content.encode_payload()?;
                        if payload.len() > DELTA_CHUNK_BYTES {
                            let chunks = proto::EntityDeltaChunk::split(entity_id, payload, DELTA_CHUNK_BYTES);
                            self.work.push_front(Work::Chunks { target, chunks });
                            None
                        } else {
                            Some(StateItem::Repair(RepairItem::Delta { target, delta }))
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%entity_id, %error, "required causal repair unavailable");
                        Some(StateItem::Repair(RepairItem::Unavailable { entity_id, target }))
                    }
                },
                Work::CompareFrontier(frontier) => {
                    let digest = ankurah_digest_tree::Digest::checked_sum(frontier.iter().map(|head| head.digest))?;
                    if self.snapshot.span_digest(proto::TreeSpan::all()).await?.digest == digest {
                        None
                    } else {
                        for head in frontier {
                            self.work.push_back(Work::Compare { prefix: NodePrefix::root(), head });
                        }
                        None
                    }
                }
                Work::Compare { head: known, prefix } => {
                    if known.digest == ankurah_digest_tree::Digest::empty() {
                        if self.snapshot.entries_in(&known.bounds, 1).next().is_none() {
                            None
                        } else {
                            self.work.push_front(Work::SendHeads(known.bounds));
                            None
                        }
                    } else {
                        let mut local = self.snapshot.span_digest(known.bounds.clone()).await?;
                        // The answering peer's topology may differ. Continue from
                        // the responder prefix retained with this question.
                        local.prefix = prefix;
                        if local.digest == known.digest {
                            None
                        } else if local.digest.count() <= MAX_PAGE_HEADS as i64 {
                            self.work.push_front(Work::AskHeads(known.bounds));
                            None
                        } else {
                            self.work.push_front(Work::Expand { local, known });
                            None
                        }
                    }
                }
                Work::Offer(head) => {
                    self.summaries.insert(range_id(&head.bounds), head.prefix.clone());
                    Some(StateItem::Digest(head))
                }
                Work::Expand { local: head, known } => {
                    // Descend locally within the work budget, preserving cached
                    // prefixes so a reply may assist several levels at once.
                    let refined = self.snapshot.refine(&head).await?;
                    if refined.len() == 1 && refined[0].bounds == head.bounds && refined[0].prefix == head.prefix {
                        let page = self.snapshot.page(head.bounds.clone())?;
                        if let std::ops::Bound::Included(last) = &page.bounds.end {
                            self.work.push_back(Work::Offer(self.snapshot.span_digest(head.bounds.after(last)).await?));
                        }
                        self.work.push_back(Work::Offer(self.snapshot.span_digest(page.bounds).await?));
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
                Work::InitialHeads(page) => {
                    self.expand_initial_heads(page)?;
                    None
                }
                Work::AnswerHeads { page, rest } => {
                    self.expand_head_answer(page, rest)?;
                    None
                }
                Work::AskHeads(bounds) => {
                    self.head_requests.push(bounds.clone());
                    Some(StateItem::NeedHeads(bounds))
                }
                Work::SendHeads(bounds) => {
                    let page = self.snapshot.page(bounds.clone())?;
                    if page.bounds.end != bounds.end {
                        let std::ops::Bound::Included(last) = &page.bounds.end else { unreachable!("partial head inventory") };
                        self.work.push_back(Work::SendHeads(bounds.after(last)));
                    }
                    let mut cursor = page.heads.cursor();
                    while let Some(leaf) = cursor.next() {
                        let id = leaf.entity.entity_id;
                        self.work.push_front(Work::Repair(id, self.known_clocks.get(&id).cloned()));
                    }
                    Some(StateItem::Heads(page))
                }
                Work::Item(item) => Some(item),
                Work::Repair(id, known) => {
                    if !self.repaired.insert(id) {
                        continue;
                    }
                    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
                    let state = node_state(node, id, &policy).await?;
                    drop(resident);
                    let allowed = state.as_ref().is_some_and(|state| {
                        (!catalog || state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy))
                            && policy.check_read(&id, &state.payload.state).is_ok()
                    });
                    if !allowed {
                        if let Some(entry) = self.snapshot.entries.get(&id) {
                            self.work.push_front(Work::Item(StateItem::Repair(RepairItem::Unavailable {
                                entity_id: id,
                                target: entry.head.clone(),
                            })));
                        }
                        continue;
                    }
                    let state = state.expect("authorized state exists");
                    if known.as_ref() == Some(&state.payload.state.head) {
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
                    let item = StateItem::Repair(RepairItem::Delta { target: target.clone(), delta });
                    if bincode::serialized_size(&item)? as usize > DELTA_CHUNK_BYTES {
                        let StateItem::Repair(RepairItem::Delta { delta, .. }) = item else { unreachable!() };
                        let payload = delta.content.encode_payload()?;
                        ensure!(payload.len() <= MAX_DELTA_BYTES, "entity repair exceeds transfer limit");
                        let chunks = proto::EntityDeltaChunk::split(id, payload, DELTA_CHUNK_BYTES);
                        self.work.push_front(Work::Chunks { target, chunks });
                        None
                    } else {
                        Some(item)
                    }
                }
                Work::Chunks { target, chunks } => {
                    let (item, rest) = next_repair_chunk(target, chunks);
                    if let Some(rest) = rest {
                        self.work.push_front(rest);
                    }
                    Some(StateItem::Repair(item))
                }
            };
            if let Some(item) = item {
                let size = item.work_bytes()? as usize;
                if !items.is_empty() && bytes + size > MAX_PAGE_BYTES {
                    self.work.push_front(Work::Item(item));
                    break;
                }
                bytes += size;
                match &item {
                    StateItem::Heads(page) => {
                        let mut cursor = page.heads.cursor();
                        while let Some(leaf) = cursor.next() {
                            self.offered.insert(target_id(leaf.entity.entity_id, &leaf.entity.head.to_owned()));
                        }
                    }
                    StateItem::Repair(RepairItem::Delta { target, delta }) => {
                        let key = target_id(delta.entity_id, target);
                        self.offered.insert(key.clone());
                        self.pending_repairs.remove(&key);
                    }
                    StateItem::Repair(RepairItem::Chunk { target, chunk }) => {
                        let key = target_id(chunk.entity_id, target);
                        self.offered.insert(key.clone());
                        if chunk.last {
                            self.pending_repairs.remove(&key);
                        }
                    }
                    StateItem::Repair(RepairItem::Unavailable { entity_id, target }) => {
                        self.unavailable = true;
                        self.pending_repairs.remove(&target_id(*entity_id, target));
                    }
                    _ => {}
                }
                ensure!(self.offered.len() <= MAX_OFFERED_TARGETS, "too many offered causal targets");
                items.push(item);
            }
        }
        let finished = self.work.is_empty() && self.summaries.is_empty() && self.head_requests.is_empty();
        let first_complete = finished && !self.complete;
        self.complete |= finished;
        Ok(StateFrameBody::Round {
            round,
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
