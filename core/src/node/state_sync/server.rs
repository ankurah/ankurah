use super::{
    Snapshot, CONTROL_BYTES, FRAME_BYTES, INITIAL_INVENTORY_BYTES, INITIAL_INVENTORY_LEAVES, LEAF_PAGE, MAX_INFLIGHT_FRAMES,
    MAX_REPAIR_BYTES, PAGE_BYTES, PAGE_STEPS, PAGE_WORK, WINDOW_BYTES,
};
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_digest_tree::{leaf_address, CanonicalCodec, NodePrefix};
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
// Initial request identities live for the connection lifetime; reaching this
// cap requires reconnecting rather than reusing a retired origin.
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
    initial_round: proto::StateRound,
    sent: AtomicU64,
    spec: proto::TreeSpec,
    fingerprint: Vec<u8>,
    state: tokio::sync::Mutex<Stream>,
    live: Mutex<LiveQueue>,
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
}
struct LivePublication {
    revision: u64,
    items: VecDeque<proto::QueryChange>,
}
struct Retained {
    frame: Arc<StateFrame>,
    bytes: usize,
}
struct LiveAssembly {
    revision: u64,
    entity_id: proto::EntityId,
    bytes: Vec<u8>,
    offset: usize,
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
    last_refresh: Option<(u64, Vec<u8>)>,
    retired_round: Option<proto::ComparisonOutcome>,
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
    live_chunk: Option<LiveAssembly>,
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
            let reply = proto::StateReply { spec: session.spec.clone(), round: session.initial_round, frame: (**initial).clone() };
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
    CompareFrontier(Vec<proto::TreeHead>),
    Compare { head: proto::TreeHead, prefix: NodePrefix },
    Offer(proto::TreeHead),
    Expand { local: proto::TreeHead, known: proto::TreeHead },
    AskLeaves(AddressRange),
    InitialLeaves(proto::LeafPage),
    AnswerLeaves { page: proto::LeafPage, rest: Option<AddressRange> },
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
        if let Some(proto::DeltaContent::StateSnapshot { state } | proto::DeltaContent::StateAndEvents { state, .. }) = &item.delta {
            if !heads.contains(&&state.state.head) {
                heads.push(&state.state.head);
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
        if let Some(assembly) = self.live_chunk.as_mut() {
            let end = (assembly.offset + PAGE_BYTES / 2).min(assembly.bytes.len());
            let last = end == assembly.bytes.len();
            let complete = last && assembly.complete;
            let body = StateFrameBody::LiveChunk {
                revision: assembly.revision,
                member: assembly.member.clone(),
                chunk: proto::EntityDeltaChunk {
                    entity_id: assembly.entity_id,
                    offset: assembly.offset as u64,
                    bytes: assembly.bytes[assembly.offset..end].to_vec().into(),
                    last,
                },
                complete,
            };
            assembly.offset = end;
            if last {
                self.live_chunk = None;
            }
            return Ok(Some(body));
        }
        let mut queue = live.lock().unwrap();
        let Some(publication) = queue.publications.front() else { return Ok(None) };
        let revision = publication.revision;
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
            if size > PAGE_BYTES / 2 {
                if !items.is_empty() {
                    break;
                }
                let item = queue.publications.front_mut().unwrap().items.pop_front().unwrap();
                queue.items -= 1;
                queue.bytes -= size;
                self.offer_live_target(revision, &item)?;
                let content = item.delta.ok_or_else(|| anyhow::anyhow!("oversized live membership metadata"))?;
                let bytes = bincode::serialize(&content)?;
                ensure!(bytes.len() <= MAX_REPAIR_BYTES, "live delta exceeds assembly limit");
                let complete = queue.publications.front().unwrap().items.is_empty();
                if complete {
                    queue.publications.pop_front();
                }
                self.live_chunk =
                    Some(LiveAssembly { revision, entity_id: item.entity_id, bytes, offset: 0, member: item.member, complete });
                drop(queue);
                return self.next_live(live);
            }
            if bytes + size > PAGE_BYTES || items.len() == PAGE_STEPS {
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

    fn request_live_repairs(&mut self, answers: Vec<StateAnswer>) -> anyhow::Result<()> {
        for answer in answers {
            let StateAnswer::NeedRepair { entity_id, target } = answer else { bail!("live feedback cannot answer round questions") };
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
            Work::History { entity_id, target } => match target_history(node, entity_id, &target, None, credentials, catalog).await {
                Ok(events) => {
                    let content = proto::DeltaContent::EventBridge { events: events.into_iter().map(Into::into).collect() };
                    let bytes = bincode::serialize(&content)?;
                    if bytes.len() > PAGE_BYTES / 2 {
                        self.live_repairs.push_front(Work::Chunks { entity_id, target, bytes, offset: 0 });
                        return Box::pin(self.next_live_repair(node, credentials, catalog)).await;
                    }
                    StateItem::Repair { target, delta: proto::EntityDelta { entity_id, content } }
                }
                Err(error) => {
                    tracing::debug!(%entity_id, %error, "live causal repair unavailable");
                    StateItem::RepairUnavailable { entity_id, target }
                }
            },
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
                    self.live_repairs.push_front(Work::Chunks { entity_id, target, bytes, offset: end });
                }
                item
            }
            _ => unreachable!("only causal repair work enters the live repair queue"),
        };
        let finished = match &item {
            StateItem::Repair { target, delta } => Some((delta.entity_id, target)),
            StateItem::RepairChunk { target, chunk } if chunk.last => Some((chunk.entity_id, target)),
            StateItem::RepairUnavailable { entity_id, target } => Some((*entity_id, target)),
            _ => None,
        };
        if let Some((id, target)) = finished {
            self.pending_live_repairs.remove(&bincode::serialize(&(id, target))?);
        }
        Ok(Some(StateFrameBody::LiveRepair { items: vec![item] }))
    }
}

pub(crate) fn comparison_bypasses_policy<SE, PA>(
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
        proto::NodeRequestBody::ContinueState { comparison, .. }
        | proto::NodeRequestBody::CloseComparison { comparison, .. }
        | proto::NodeRequestBody::RefreshState { comparison, .. } => comparison,
        _ => return crate::schema::request_bypasses_policy(body),
    };
    let Some(connection) = node.peer_connections.get(&peer) else { return false };
    if outgoing {
        connection.outgoing_comparisons.get(id).is_some_and(|catalog| catalog)
    } else {
        connection.comparisons.bypasses_policy(id)
    }
}

pub(crate) async fn start_comparison<SE, PA>(
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
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let mut comparison = Comparison::new(snapshot, known)?;
    let id = ComparisonId::new();
    let round = proto::StateRound { number: 0, revision };
    let body = comparison.reply(node, &credentials, catalog, query.is_some(), round.number).await?;
    let frame = StateFrame { comparison: id, sequence: 0, body };
    let initial = proto::StateReply { spec: comparison.snapshot.spec.clone(), round, frame: frame.clone() };
    let mut state = Stream {
        comparison: Some(comparison),
        round,
        pending_round: None,
        last_refresh: None,
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
    state.retain(frame, false)?;
    let (cancel_expiry, mut cancelled) = futures::channel::oneshot::channel();
    let session = Arc::new(Session {
        catalog,
        credentials,
        query,
        initial_request: request,
        initial: Mutex::new(Some(state.retained.front().unwrap().frame.clone())),
        initial_round: round,
        sent: AtomicU64::new(0),
        spec: initial.spec.clone(),
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
    let node = node.weak();
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
                && (state.comparison.is_some()
                    || state.pending_round.is_some()
                    || !state.retained.is_empty()
                    || !state.live_targets.is_empty()
                    || !session.live.lock().unwrap().publications.is_empty());
            drop(state);
            if idle {
                let _ = expire_comparison(&node, peer, id).await;
                break;
            }
        }
    });
    Ok(initial)
}

pub(crate) async fn continue_state<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    request: proto::RequestId,
    id: ComparisonId,
    round: Option<u64>,
    processed_through: u64,
    applied_through: Option<u64>,
    answers: Vec<StateAnswer>,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let Some(session) = connection.comparisons.get(&id) else {
        return Ok(if answers.is_empty() && connection.comparisons.permits_retired_receipt(id, processed_through, &credentials) {
            proto::NodeResponseBody::Success
        } else {
            proto::NodeResponseBody::ComparisonUnavailable { comparison: id }
        });
    };
    ensure!(session.permits(&credentials), "comparison credentials changed");
    ensure!(answers.len() <= 128, "too many comparison answers");
    let encoded = bincode::serialize(&(round, processed_through, applied_through, &answers))?;
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
    match round {
        Some(number) => {
            ensure!(number <= state.round.number, "feedback names a future round");
            // A delayed repair question may arrive after live application covered
            // its target and Applied retired the round. It cannot author current
            // membership, and retaining the old snapshot merely to reject it
            // would make round retirement race normal application progress.
            if number == state.round.number {
                if let Some(comparison) = state.comparison.as_mut() {
                    comparison.answer(answers)?;
                }
            }
        }
        None => state.request_live_repairs(answers)?,
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
    kick_comparison(node, peer, id);
    Ok(proto::NodeResponseBody::Success)
}

pub(crate) async fn close_state<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: ComparisonId,
    outcome: proto::ComparisonOutcome,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
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
        proto::ComparisonOutcome::Applied { round, processed_through } => {
            if state.retired_round == Some(outcome) {
                return Ok(proto::NodeResponseBody::Success);
            }
            ensure!(round == state.round.number, "Applied names a retired or future round");
            let comparison = state.comparison.as_ref().ok_or_else(|| anyhow::anyhow!("round is already retired"))?;
            ensure!(
                comparison.complete && comparison.summaries.is_empty() && comparison.leaves.is_empty(),
                "comparison enumeration is not complete"
            );
            ensure!(processed_through >= state.required_through, "Applied does not cover the completed enumeration");
            state.acknowledge(processed_through)?;
            session.initial.lock().unwrap().take();
            state.comparison = None;
            state.retired_round = Some(outcome);
            if session.query.is_none() {
                connection.comparisons.finish(id);
            }
        }
        proto::ComparisonOutcome::Cancelled { round } => {
            if state.retired_round == Some(outcome) {
                return Ok(proto::NodeResponseBody::Success);
            }
            ensure!(round == state.round.number && state.comparison.is_some(), "Cancelled names an inactive round");
            state.comparison = None;
            state.retired_round = Some(outcome);
            session.initial.lock().unwrap().take();
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
            connection.subscription_handler.abandon_comparison(query, version, id).await;
        }
    } else {
        kick_comparison(node, peer, id);
    }
    Ok(proto::NodeResponseBody::Success)
}

/// The source actor calls this after queueing all publications through revision,
/// and before queueing any later publication. The marker shares the live ordering
/// boundary, while the round's pages remain independently schedulable.
pub(crate) async fn schedule_round<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: ComparisonId,
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
    let session = connection.comparisons.get(&id).ok_or_else(|| anyhow::anyhow!("state stream unavailable"))?;
    ensure!(session.query.is_some(), "Fetch cannot refresh a standing query");
    ensure!(snapshot.spec == session.spec && known.spec == session.spec, "refresh changed tree specification");
    let encoded = bincode::serialize(&known)?;
    ensure!(encoded.len() <= INITIAL_INVENTORY_BYTES + FRAME_BYTES, "refresh offer exceeds limit");
    let mut state = session.state.lock().await;
    if let Some((previous, offer)) = &state.last_refresh {
        if *previous == number {
            ensure!(*offer == encoded, "refresh retry changed frozen offer");
            return Ok(());
        }
    }
    ensure!(!state.failed && state.comparison.is_none() && state.pending_round.is_none(), "a comparison round is already active");
    ensure!(state.round.number.checked_add(1) == Some(number) && revision >= state.round.revision, "invalid next round");
    ensure!(state.last_live_complete.is_none_or(|sent| sent <= revision), "round cut precedes delivered live state");
    ensure!(session.live.lock().unwrap().last_revision.is_none_or(|queued| queued <= revision), "round cut precedes queued live state");
    let comparison = Comparison::new(snapshot, known)?;
    state.pending_round = Some(PendingRound { round: proto::StateRound { number, revision }, comparison });
    state.last_refresh = Some((number, encoded));
    state.last_activity = Instant::now();
    drop(state);
    kick_comparison(node, peer, id);
    Ok(())
}

pub(crate) fn queue_live<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    id: ComparisonId,
    revision: u64,
    items: Vec<proto::QueryChange>,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let session = connection.comparisons.get(&id).ok_or_else(|| anyhow::anyhow!("live stream unavailable"))?;
    ensure!(session.query.is_some(), "Fetch has no live route");
    ensure!(revision > session.initial_round.revision, "live publication does not follow the initial cut");
    ensure!(
        items.iter().all(|item| !matches!(item.delta, Some(proto::DeltaContent::StateAndRelation { .. }))),
        "StateAndRelation is not supported"
    );
    ensure!(
        items.iter().all(|item| item.member.is_some() || !matches!(item.delta, Some(proto::DeltaContent::EventBridge { .. }))),
        "absent live bridge has no explicit causal target"
    );
    let sizes = items.iter().map(|item| bincode::serialized_size(item).map(|size| size as usize)).collect::<Result<Vec<_>, _>>()?;
    let bytes = sizes.iter().sum::<usize>();
    let mut queue = session.live.lock().unwrap();
    if queue.stopped
        || sizes.iter().any(|size| *size > MAX_REPAIR_BYTES)
        || queue.items + items.len() > MAX_LIVE_ITEMS
        || queue.publications.len() >= MAX_LIVE_ITEMS
        || queue.bytes.saturating_add(bytes) > MAX_LIVE_BYTES
        || queue.last_revision.is_some_and(|previous| revision <= previous)
    {
        queue.stopped = true;
        drop(queue);
        kick_comparison(node, peer, id);
        bail!("live queue stopped, out of order, or exhausted");
    }
    queue.bytes += bytes;
    queue.items += items.len();
    queue.last_revision = Some(revision);
    queue.publications.push_back(LivePublication { revision, items: items.into() });
    drop(queue);
    kick_comparison(node, peer, id);
    Ok(())
}

pub(crate) fn kick_comparison<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: ComparisonId)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let Some(connection) = node.peer_connections.get(&peer) else { return };
    let Some(session) = connection.comparisons.get(&id) else { return };
    session.wake.store(true, Ordering::Release);
    if session.pumping.swap(true, Ordering::AcqRel) {
        return;
    }
    let node = node.weak();
    crate::task::spawn(async move {
        loop {
            session.wake.store(false, Ordering::Release);
            let Some(node) = node.upgrade() else { break };
            if let Err(error) = pump_comparison(&node, peer, id).await {
                tracing::warn!(%error, "state stream failed");
                let _ = expire_comparison(&node, peer, id).await;
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

pub(crate) async fn pump_comparison<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: ComparisonId) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
    let Some(session) = connection.comparisons.get(&id) else { return Ok(()) };
    let mut state = session.state.lock().await;
    if state.failed || !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
        return Ok(());
    }
    ensure!(!session.live.lock().unwrap().stopped, "live queue stopped");
    loop {
        if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection))
            || !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session))
        {
            break;
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
                                let body =
                                    comparison.reply(node, &session.credentials, session.catalog, session.query.is_some(), round).await?;
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
        let frame = StateFrame { comparison: id, sequence, body };
        if !connection.comparisons.get(&id).is_some_and(|current| Arc::ptr_eq(&current, &session)) {
            break;
        }
        if matches!(frame.body, StateFrameBody::BeginRound(_) | StateFrameBody::State { progress: ComparisonProgress::Complete, .. }) {
            // Applied covers the finite enumeration. Later requested history may
            // become unnecessary because another live update covers the target;
            // those frames still need wire credit, not another readiness fence.
            state.required_through = sequence;
        }
        state.retain(frame.clone(), priority)?;
        session.sent.store(sequence, Ordering::Release);
        connection.send_message(proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: node.id,
            to: peer,
            body: proto::NodeUpdateBody::StateFrame { request_id: session.initial_request.clone(), frame },
        }))?;
    }
    Ok(())
}

pub(crate) async fn expire_comparison<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: ComparisonId) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fail_stream(node, peer, id, StateFrameBody::RestartRequired).await
}

pub(crate) async fn source_unavailable<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: ComparisonId) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fail_stream(node, peer, id, StateFrameBody::SourceUnavailable).await
}

async fn fail_stream<SE, PA>(node: &Node<SE, PA>, peer: proto::EntityId, id: ComparisonId, body: StateFrameBody) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let Some(connection) = node.peer_connections.get(&peer) else { return Ok(()) };
    let Some(session) = connection.comparisons.get(&id) else { return Ok(()) };
    let mut state = session.state.lock().await;
    if state.failed {
        return Ok(());
    }
    state.failed = true;
    state.comparison = None;
    state.pending_round = None;
    state.last_refresh = None;
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
        live.publications.clear();
        live.items = 0;
        live.bytes = 0;
    }
    let frame = StateFrame { comparison: id, sequence: state.last_sent.saturating_add(1), body };
    let retained = state.room(true) && state.retain(frame.clone(), true).is_ok();
    session.sent.store(state.last_sent, Ordering::Release);
    drop(state);
    if let Some((query, version)) = session.query {
        connection.subscription_handler.abandon_comparison(query, version, id).await;
    }
    if !node.peer_connections.get(&peer).is_some_and(|current| Arc::ptr_eq(&current, &connection)) {
        return Ok(());
    }
    if !retained
        || connection
            .send_message(proto::NodeMessage::Update(proto::NodeUpdate {
                id: proto::UpdateId::new(),
                from: node.id,
                to: peer,
                body: proto::NodeUpdateBody::StateFrame { request_id: session.initial_request.clone(), frame: frame.clone() },
            }))
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

async fn target_history<SE, PA>(
    node: &Node<SE, PA>,
    entity_id: proto::EntityId,
    target: &proto::Clock,
    snapshot: Option<&Snapshot>,
    credentials: &Vec<PA::ContextData>,
    catalog: bool,
) -> anyhow::Result<Vec<proto::Attested<proto::Event>>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
    let state = match snapshot.and_then(|snapshot| snapshot.states.get(&entity_id)) {
        Some(state) => (**state).clone(),
        None => node_state(node, entity_id, &policy).await?.ok_or_else(|| anyhow::anyhow!("repair state unavailable"))?,
    };
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
    fn new(snapshot: Snapshot, known: proto::KnownState) -> anyhow::Result<Self> {
        let mut comparison = Self {
            snapshot,
            work: VecDeque::new(),
            summaries: HashMap::new(),
            leaves: Vec::new(),
            repaired: HashSet::new(),
            known_clocks: HashMap::new(),
            offered: HashMap::new(),
            pending_repairs: HashSet::new(),
            repair_attempts: HashMap::new(),
            complete: false,
            unavailable: false,
        };
        ensure!(known.spec == comparison.snapshot.spec, "unsupported comparison tree spec");
        ensure!(
            known.regions.len() <= 128 && bincode::serialized_size(&known)? as usize <= INITIAL_INVENTORY_BYTES,
            "initial inventory exceeds limit"
        );
        let mut regions = known.regions;
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
        let mut frontier = Some(Vec::new());
        let mut leaf_count = 0;
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
                frontier = None;
                let gap = AddressRange::new(start.clone(), opposite(&bounds.start));
                if valid_range(&gap) {
                    comparison.work.push_back(Work::SendSnapshots(gap));
                }
            }
            start = opposite(&bounds.end);
            match region {
                proto::KnownRegion::Summary(head) => {
                    let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                    if let Some(frontier) = &mut frontier {
                        frontier.push(head.clone());
                    }
                    comparison.work.push_back(Work::Compare { prefix: NodePrefix::root(), head });
                }
                proto::KnownRegion::Leaves(page) => {
                    frontier = None;
                    comparison.validate_leaves(&page)?;
                    leaf_count += page.leaves.len();
                    ensure!(leaf_count <= INITIAL_INVENTORY_LEAVES, "initial inventory exceeds leaf limit");
                    for leaf in &page.leaves {
                        ensure!(
                            comparison.known_clocks.insert(leaf.entity.entity_id, leaf.entity.head.clone()).is_none(),
                            "duplicate entity in initial inventory"
                        );
                    }
                    comparison.work.push_back(Work::InitialLeaves(page));
                }
            }
        }
        if !matches!(start, std::ops::Bound::Unbounded) {
            let rest = AddressRange::new(start, super::whole().end);
            if valid_range(&rest) {
                comparison.work.push_back(Work::SendSnapshots(rest));
            }
        } else if let Some(frontier) = frontier.filter(|heads| heads.len() > 1) {
            comparison.work.clear();
            comparison.work.push_back(Work::CompareFrontier(frontier));
        }
        Ok(comparison)
    }

    fn validate_leaves(&self, page: &proto::LeafPage) -> anyhow::Result<()> {
        ensure!(valid_range(&page.bounds) && page.leaves.len() <= LEAF_PAGE, "invalid leaf inventory size or bounds");
        ensure!(bincode::serialized_size(&StateAnswer::Leaves(page.clone()))? <= PAGE_BYTES as u64, "initial leaf page exceeds byte limit");
        let mut previous = None;
        for leaf in &page.leaves {
            CanonicalCodec.split(self.snapshot.spec.key_spec(), &leaf.key)?;
            let address = leaf_address(&leaf.key, leaf.entity.entity_id);
            ensure!(page.bounds.contains(&address) && previous.as_ref().is_none_or(|p| p < &address), "invalid leaf order or bounds");
            previous = Some(address);
        }
        Ok(())
    }

    fn answer(&mut self, answers: Vec<StateAnswer>) -> anyhow::Result<()> {
        ensure!(answers.len() <= 128, "too many comparison answers");
        for answer in answers {
            match answer {
                StateAnswer::NeedRepair { entity_id, target } => {
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
                    let mut ids = HashSet::new();
                    for leaf in &page.leaves {
                        CanonicalCodec.split(self.snapshot.spec.key_spec(), &leaf.key)?;
                        let address = leaf_address(&leaf.key, leaf.entity.entity_id);
                        ensure!(
                            page.bounds.contains(&address) && previous.as_ref().is_none_or(|p| p < &address),
                            "invalid leaf order or bounds"
                        );
                        ensure!(ids.insert(leaf.entity.entity_id), "duplicate entity in leaf page");
                        previous = Some(address);
                    }
                    let rest = if page.bounds.end != requested.end {
                        let std::ops::Bound::Included(last) = &page.bounds.end else { bail!("partial page must end at its last leaf") };
                        ensure!(previous.as_ref() == Some(last), "partial page made no progress");
                        Some(requested.after(last))
                    } else {
                        None
                    };
                    self.leaves.remove(index);
                    // Accept each validated answer once, but expand only one
                    // region at a time while the producer drains its repairs.
                    self.work.push_back(Work::AnswerLeaves { page, rest });
                }
            }
        }
        Ok(())
    }

    fn expand_initial_leaves(&mut self, page: proto::LeafPage) -> anyhow::Result<()> {
        use std::ops::Bound::{Excluded, Included};

        let mut work = Vec::new();
        let former = page
            .leaves
            .iter()
            .filter(|leaf| !self.snapshot.states.contains_key(&leaf.entity.entity_id))
            .map(|leaf| leaf.entity.entity_id)
            .collect::<Vec<_>>();
        let mut start = page.bounds.start.clone();
        let mut removal: Option<AddressRange> = None;
        let flush_removal = |removal: &mut Option<AddressRange>, work: &mut Vec<Work>| {
            if let Some(bounds) = removal.take() {
                work.push(Work::Item(StateItem::Leaves(proto::LeafPage { bounds, leaves: Vec::new() })));
            }
        };
        for leaf in page.leaves {
            let address = leaf_address(&leaf.key, leaf.entity.entity_id);
            let gap = AddressRange::new(start, Excluded(address.clone()));
            if valid_range(&gap) && self.snapshot.states_in(&gap, 1).next().is_some() {
                flush_removal(&mut removal, &mut work);
                work.push(Work::SendSnapshots(gap));
            }
            let point = AddressRange::new(Included(address.clone()), Included(address.clone()));
            if let Some((_, state)) = self.snapshot.states_in(&point, 1).next() {
                flush_removal(&mut removal, &mut work);
                if state.payload.state.head != leaf.entity.head {
                    work.push(Work::Repair(leaf.entity.entity_id, Some(leaf.entity.head)));
                }
            } else {
                // Only bridge removed points across gaps proven empty above.
                // Membership patches remain independent of repair deduplication.
                if let Some(removal) = &mut removal {
                    removal.end = Included(address.clone());
                } else {
                    removal = Some(point);
                }
                work.push(Work::Repair(leaf.entity.entity_id, Some(leaf.entity.head)));
            }
            start = Excluded(address);
        }
        flush_removal(&mut removal, &mut work);
        let gap = AddressRange::new(start, page.bounds.end.clone());
        if valid_range(&gap) && self.snapshot.states_in(&gap, 1).next().is_some() {
            work.push(Work::SendSnapshots(gap));
        }
        // Splitting a valid region can repeat long keys in its bounds. Keep
        // the original regional exchange when those patches cannot fit.
        let mut fits = true;
        for item in &work {
            fits &= match item {
                Work::Item(item) => bincode::serialized_size(item)? <= PAGE_BYTES as u64,
                Work::SendSnapshots(bounds) => {
                    let item = StateItem::Snapshots(proto::SnapshotPage { bounds: bounds.clone(), states: Vec::new() });
                    bincode::serialized_size(&item)? <= (PAGE_BYTES / 2) as u64
                }
                _ => true,
            };
        }
        if !fits {
            work = former.into_iter().map(|id| Work::Repair(id, Some(self.known_clocks[&id].clone()))).collect();
            work.push(Work::SendLeaves(page.bounds));
        }
        for item in work.into_iter().rev() {
            self.work.push_front(item);
        }
        Ok(())
    }

    fn expand_leaf_answer(&mut self, page: proto::LeafPage, rest: Option<AddressRange>) -> anyhow::Result<()> {
        let mut clocks =
            page.leaves.into_iter().map(|leaf| (leaf.entity.entity_id, leaf.entity.head)).collect::<std::collections::BTreeMap<_, _>>();
        let mut remaining = page.bounds.clone();
        let mut pages = Vec::new();
        let mut repairs = Vec::new();
        let mut count = 0;
        loop {
            let remote = self.snapshot.page(remaining.clone())?;
            count += remote.leaves.len();
            ensure!(count <= LEAF_PAGE, "leaf region was not fully refined");
            for leaf in &remote.leaves {
                let id = leaf.entity.entity_id;
                repairs.push(Work::Repair(id, clocks.remove(&id)));
            }
            let complete = remote.bounds.end == page.bounds.end;
            if !complete {
                let std::ops::Bound::Included(last) = &remote.bounds.end else { bail!("partial page must make progress") };
                remaining = page.bounds.after(last);
            }
            pages.push(Work::Item(StateItem::Leaves(remote)));
            if complete {
                break;
            }
        }
        repairs.extend(clocks.into_iter().map(|(id, clock)| Work::Repair(id, Some(clock))));
        pages.extend(repairs);
        if let Some(rest) = rest {
            pages.push(Work::AskLeaves(rest));
        }
        // Keep this bounded region's pages and repairs ahead of later answers,
        // rather than multiplying all pending answers into thousands of items.
        for work in pages.into_iter().rev() {
            self.work.push_front(work);
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
                Work::InitialLeaves(_)
                | Work::AnswerLeaves { .. }
                | Work::CompareFrontier(_)
                | Work::Compare { .. }
                | Work::Expand { .. }
                | Work::SendLeaves(_)
                | Work::SendSnapshots(_) => true,
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
                    match target_history(node, entity_id, &target, Some(&self.snapshot), credentials, catalog).await {
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
                        Err(error) => {
                            tracing::debug!(%entity_id, %error, "required causal repair unavailable");
                            Some(StateItem::RepairUnavailable { entity_id, target })
                        }
                    }
                }
                Work::CompareFrontier(frontier) => {
                    let digest = ankurah_digest_tree::Digest::checked_sum(frontier.iter().map(|head| head.digest))?;
                    if self.snapshot.head(super::whole()).await?.digest == digest {
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
                        if self.snapshot.states_in(&known.bounds, 1).next().is_none() {
                            None
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
                            None
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
                        let page = self.snapshot.page(head.bounds.clone())?;
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
                Work::InitialLeaves(page) => {
                    self.expand_initial_leaves(page)?;
                    None
                }
                Work::AnswerLeaves { page, rest } => {
                    self.expand_leaf_answer(page, rest)?;
                    None
                }
                Work::AskLeaves(bounds) => {
                    self.leaves.push(bounds.clone());
                    Some(StateItem::NeedLeaves(bounds))
                }
                Work::SendLeaves(bounds) => {
                    let page = self.snapshot.page(bounds.clone())?;
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
                        None => node_state(node, id, &policy).await?,
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
        let finished = self.work.is_empty() && self.summaries.is_empty() && self.leaves.is_empty();
        let first_complete = finished && !self.complete;
        if first_complete && standing_query && !self.unavailable && self.snapshot.states.len() > INITIAL_INVENTORY_LEAVES {
            // Build before handoff above the initial inventory count limit.
            // Smaller inventories and Fetch keep construction on demand.
            self.snapshot.head(super::whole()).await?;
        }
        self.complete |= finished;
        Ok(StateFrameBody::State {
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
