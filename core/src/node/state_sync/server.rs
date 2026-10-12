use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use ankurah_digest_tree::{leaf_address, CanonicalCodec, KeyCodec};
use ankurah_proto::{self as proto, AddressRange, ComparisonId, ComparisonProgress, StateAnswer, StateItem};
use anyhow::{bail, ensure};
use web_time::Instant;

use super::{Snapshot, LEAF_PAGE, PAGE_BYTES, PAGE_WORK};
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
};

const COMPARISON_TTL: Duration = Duration::from_secs(120);
const MAX_COMPARISONS: usize = 128;

pub(crate) struct Comparisons<CD: ContextData> {
    sessions: Mutex<HashMap<ComparisonId, Arc<Session<CD>>>>,
    closed: Mutex<VecDeque<ComparisonId>>,
}

impl<CD: ContextData> Default for Comparisons<CD> {
    fn default() -> Self { Self { sessions: Mutex::new(HashMap::new()), closed: Mutex::new(VecDeque::new()) } }
}

pub(crate) struct Session<CD: ContextData> {
    pub catalog: bool,
    credentials: Vec<CD>,
    pub query: Option<(proto::QueryId, u32)>,
    created: Instant,
    initial_request: proto::RequestId,
    initial: proto::StateReply,
    fingerprint: Vec<u8>,
    state: tokio::sync::Mutex<Comparison>,
}

impl<CD: ContextData> Comparisons<CD> {
    pub fn get(&self, id: &ComparisonId) -> Option<Arc<Session<CD>>> { self.sessions.lock().unwrap().get(id).cloned() }
    pub fn remove(&self, id: &ComparisonId) -> Option<Arc<Session<CD>>> { self.sessions.lock().unwrap().remove(id) }
    pub fn was_closed(&self, id: &ComparisonId) -> bool { self.closed.lock().unwrap().contains(id) }
    pub fn finish(&self, id: ComparisonId) {
        self.remove(&id);
        let mut closed = self.closed.lock().unwrap();
        closed.push_back(id);
        if closed.len() > MAX_COMPARISONS {
            closed.pop_front();
        }
    }
    pub fn invalidate_query(&self, query: proto::QueryId) {
        self.sessions.lock().unwrap().retain(|_, session| session.query.is_none_or(|(id, _)| id != query));
    }
    pub fn replay_initial(
        &self,
        request: &proto::RequestId,
        fingerprint: &[u8],
        credentials: &[CD],
    ) -> anyhow::Result<Option<proto::StateReply>> {
        let sessions = self.sessions.lock().unwrap();
        let Some(session) = sessions.values().find(|session| &session.initial_request == request && !session.expired()) else {
            return Ok(None);
        };
        ensure!(session.fingerprint == fingerprint && session.permits(credentials), "initial retry changed its request");
        Ok(Some(session.initial.clone()))
    }
}

enum Work {
    Compare(proto::TreeHead),
    Offer(proto::TreeHead),
    Expand(proto::TreeHead),
    AskLeaves(AddressRange),
    Item(StateItem),
    Repair(proto::EntityId, Option<proto::Clock>),
    Chunks { entity_id: proto::EntityId, target: proto::Clock, bytes: Vec<u8>, offset: usize },
}

struct Comparison {
    snapshot: Snapshot,
    work: VecDeque<Work>,
    summaries: HashMap<Vec<u8>, AddressRange>,
    leaves: Vec<AddressRange>,
    heads: HashSet<proto::EntityId>,
    repaired: HashSet<proto::EntityId>,
    hints: HashMap<Vec<u8>, proto::TreeHead>,
    complete: bool,
    // Only the latest follow-up may be replayed; older retries restart safely.
    replay: Option<(proto::RequestId, Vec<u8>, proto::ComparisonReply)>,
    seen: HashSet<proto::RequestId>,
}

fn range_id(range: &AddressRange) -> Vec<u8> { bincode::serialize(range).expect("address bounds serialize") }

impl<CD: ContextData> Session<CD> {
    fn expired(&self) -> bool { self.created.elapsed() > COMPARISON_TTL }

    pub fn permits(&self, credentials: &[CD]) -> bool {
        self.credentials.len() == credentials.len() && self.credentials.iter().all(|credential| credentials.contains(credential))
    }

    pub async fn complete(&self) -> bool { self.state.lock().await.complete }
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn comparison_bypasses_policy(&self, peer: proto::EntityId, body: &proto::NodeRequestBody, outgoing: bool) -> bool {
        let id = match body {
            proto::NodeRequestBody::CompareTree { comparison, .. } | proto::NodeRequestBody::CloseComparison { comparison, .. } => {
                comparison
            }
            _ => return crate::schema::request_bypasses_policy(body),
        };
        let Some(connection) = self.peer_connections.get(&peer) else { return false };
        if outgoing {
            connection.outgoing_comparisons.get(id).is_some_and(|catalog| catalog)
        } else {
            connection.comparisons.get(id).is_none_or(|session| session.catalog)
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
        mut snapshot: Snapshot,
        known: Option<proto::KnownState>,
    ) -> anyhow::Result<proto::StateReply> {
        let connection = self.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let mut hints = HashMap::new();
        if let Some(known) = known {
            ensure!(known.spec == snapshot.spec, "unsupported comparison tree spec");
            ensure!(known.heads.len() <= 128, "too many initial summaries");
            for head in known.heads {
                let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                hints.insert(range_id(&head.bounds), head);
            }
        }
        let first = match hints.remove(&range_id(&super::whole())) {
            Some(head) => Work::Compare(head),
            None => Work::Offer(snapshot.head(super::whole()).await?),
        };
        let mut state = Comparison {
            snapshot,
            work: VecDeque::from([first]),
            summaries: HashMap::new(),
            leaves: Vec::new(),
            heads: HashSet::new(),
            repaired: HashSet::new(),
            hints,
            complete: false,
            replay: None,
            seen: HashSet::new(),
        };
        let id = ComparisonId::new();
        let page = state.reply(self, id, &credentials, catalog).await?;
        let initial = proto::StateReply { spec: state.snapshot.spec.clone(), scope: vec![super::whole()], page };
        let session = Arc::new(Session {
            catalog,
            credentials,
            query,
            fingerprint,
            created: Instant::now(),
            initial_request: request,
            initial: initial.clone(),
            state: tokio::sync::Mutex::new(state),
        });
        let mut sessions = connection.comparisons.sessions.lock().unwrap();
        // Query sessions retain their handoff metadata until cancellation or
        // the next follow-up can release the delivery gate.
        sessions.retain(|_, session| session.query.is_some() || !session.expired());
        ensure!(sessions.len() < MAX_COMPARISONS, "too many open comparisons; close existing comparisons first");
        sessions.insert(id, session);
        Ok(initial)
    }

    pub(crate) async fn compare_tree(
        &self,
        peer: proto::EntityId,
        request: proto::RequestId,
        id: ComparisonId,
        answers: Vec<StateAnswer>,
        credentials: Vec<PA::ContextData>,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        let connection = self.peer_connections.get(&peer).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        let Some(session) = connection.comparisons.get(&id) else {
            return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
        };
        ensure!(session.permits(&credentials), "comparison credentials changed");
        if session.expired() {
            if let Some((query, version)) = session.query {
                connection.subscription_handler.finish_comparison(self, query, version, id, proto::ComparisonOutcome::Abandoned).await?;
            }
            connection.comparisons.remove(&id);
            return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
        }
        let mut state = session.state.lock().await;
        let encoded = bincode::serialize(&answers)?;
        if let Some((previous, body, reply)) = &state.replay {
            if previous == &request {
                ensure!(body == &encoded, "a retry changed its answers");
                return Ok(proto::NodeResponseBody::TreeCompared(reply.clone()));
            }
        }
        if !state.seen.insert(request.clone()) {
            return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: id });
        }
        let result = async {
            ensure!(state.seen.len() <= 100_000, "comparison exceeded its round limit");
            state.answer(answers).await?;
            let reply = state.reply(self, id, &credentials, session.catalog).await?;
            state.replay = Some((request, encoded, reply.clone()));
            Ok(proto::NodeResponseBody::TreeCompared(reply))
        }
        .await;
        drop(state);
        if result.is_err() {
            connection.comparisons.remove(&id);
            if let Some((query, version)) = session.query {
                connection.subscription_handler.finish_comparison(self, query, version, id, proto::ComparisonOutcome::Abandoned).await?;
            }
        }
        result
    }
}

impl Comparison {
    async fn answer(&mut self, answers: Vec<StateAnswer>) -> anyhow::Result<()> {
        ensure!(answers.len() <= 128, "too many comparison answers");
        for answer in answers {
            match answer {
                StateAnswer::Summary(head) => {
                    let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                    ensure!(self.summaries.remove(&range_id(&head.bounds)).is_some(), "unsolicited summary bounds");
                    self.work.push_back(Work::Compare(head));
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

    async fn reply<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        comparison: ComparisonId,
        credentials: &Vec<PA::ContextData>,
        catalog: bool,
    ) -> anyhow::Result<proto::ComparisonReply>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut items = Vec::new();
        let mut bytes = 0;
        for _ in 0..PAGE_WORK {
            let Some(work) = self.work.pop_front() else { break };
            let item = match work {
                Work::Compare(known) => {
                    let local = self.snapshot.head(known.bounds.clone()).await?;
                    if local.digest == known.digest {
                        Some(StateItem::Equal(known.bounds))
                    } else if local.digest.count() <= LEAF_PAGE as i64 {
                        self.work.push_front(Work::AskLeaves(known.bounds));
                        None
                    } else {
                        self.work.push_front(Work::Expand(local));
                        None
                    }
                }
                Work::Offer(head) => {
                    if head.digest.count() > LEAF_PAGE as i64 {
                        self.work.push_front(Work::Expand(head));
                        None
                    } else {
                        self.summaries.insert(range_id(&head.bounds), head.bounds.clone());
                        Some(StateItem::Summary(head))
                    }
                }
                Work::Expand(head) => {
                    // Descend locally within the work budget, preserving cached
                    // prefixes so a reply may assist several levels at once.
                    let refined = self.snapshot.tree.refine(&head).await?;
                    if refined.len() == 1 && refined[0].bounds == head.bounds && refined[0].prefix == head.prefix {
                        let page = self.snapshot.page(head.bounds.clone()).await?;
                        if let std::ops::Bound::Included(last) = &page.bounds.end {
                            self.work.push_back(Work::Offer(self.snapshot.head(head.bounds.after(last)).await?));
                        }
                        self.work.push_back(Work::Offer(self.snapshot.head(page.bounds).await?));
                    } else {
                        for head in refined {
                            self.work.push_back(match self.hints.remove(&range_id(&head.bounds)) {
                                Some(hint) => Work::Compare(hint),
                                None => Work::Offer(head),
                            });
                        }
                    }
                    None
                }
                Work::AskLeaves(bounds) => {
                    self.leaves.push(bounds.clone());
                    Some(StateItem::NeedLeaves(bounds))
                }
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
                    let known = known.map(|head| HashMap::from([(id, head)])).unwrap_or_default();
                    let Some(delta) = node.generate_entity_delta(&known, state, credentials).await? else { continue };
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
                    let item = StateItem::RepairChunk(proto::RepairChunk {
                        entity_id,
                        target: target.clone(),
                        offset: offset as u64,
                        bytes: bytes[offset..end].to_vec().into(),
                        last: end == bytes.len(),
                    });
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
                items.push(item);
            }
        }
        self.complete = self.work.is_empty() && self.summaries.is_empty() && self.leaves.is_empty() && self.heads.is_empty();
        Ok(proto::ComparisonReply {
            comparison,
            items,
            progress: if self.complete { ComparisonProgress::Complete } else { ComparisonProgress::Continue },
        })
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
