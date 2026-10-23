use super::*;
use crate::{changes::EntityChange, node::applier::Delivery, reactor::ChangeNotification};
use ankurah_digest_tree::CanonicalCodec;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet, VecDeque};

use indexmap::IndexMap;
use projection::{Projection, Round};

// Keep finite storage work bounded without letting its backlog consume the
// admission reserved for ongoing publications and data-only causal repairs.
const MAX_ACTIVE_ROUND_APPLICATIONS: usize = 128;
const MAX_ACTIVE_LIVE_APPLICATIONS: usize = 16;

type TargetKey = Vec<u8>;
fn target_key(id: proto::EntityId, target: &proto::Clock) -> TargetKey {
    bincode::serialize(&(id, target)).expect("causal target serializes")
}

struct RefreshRequest {
    token: proto::RequestId,
    reply: tokio::sync::oneshot::Sender<Result<(), RetrievalError>>,
}
enum Command {
    Refresh(RefreshRequest),
    Cancel(proto::RequestId),
}
#[derive(Clone)]
pub(crate) struct RefreshHandle(tokio::sync::mpsc::Sender<Command>);
struct CancelRefresh {
    handle: RefreshHandle,
    token: Option<proto::RequestId>,
}
impl Drop for CancelRefresh {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let _ = self.handle.0.try_send(Command::Cancel(token));
        }
    }
}
impl RefreshHandle {
    pub async fn refresh(&self) -> Result<(), RetrievalError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let token = proto::RequestId::new();
        self.0
            .try_send(Command::Refresh(RefreshRequest { token: token.clone(), reply: tx }))
            .map_err(|_| RetrievalError::Other("a refresh is already pending or its stream has ended".into()))?;
        let mut cancel = CancelRefresh { handle: self.clone(), token: Some(token) };
        let result = rx.await.map_err(|_| RetrievalError::Other("state stream ended during refresh".into()))?;
        cancel.token = None;
        result
    }
}

struct Goal {
    id: proto::EntityId,
    head: proto::Clock,
    covered: bool,
    attempts: u8,
    requested: bool,
}
#[derive(Default)]
struct Goals {
    rows: BTreeMap<proto::EntityId, BTreeMap<Vec<u8>, Goal>>,
    bytes: usize,
    count: usize,
}
impl Goals {
    fn add(&mut self, id: proto::EntityId, head: proto::Clock, covered: bool) -> anyhow::Result<()> {
        let key = bincode::serialize(&head)?;
        let rows = self.rows.entry(id).or_default();
        if rows.contains_key(&key) {
            return Ok(());
        }
        ensure!(self.count < 100_000, "too many causal targets");
        self.bytes += key.len() + proto::EntityId::BYTE_LEN;
        self.count += 1;
        rows.insert(key, Goal { id, head, covered, attempts: 0, requested: false });
        Ok(())
    }
    fn values(&self) -> impl Iterator<Item = &Goal> { self.rows.values().flat_map(|rows| rows.values()) }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut Goal> { self.rows.values_mut().flat_map(|rows| rows.values_mut()) }
    fn for_id_mut(&mut self, id: &proto::EntityId) -> impl Iterator<Item = &mut Goal> {
        self.rows.get_mut(id).into_iter().flat_map(|rows| rows.values_mut())
    }
    fn covered(&self) -> bool { self.values().all(|goal| goal.covered) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    Round(u64),
    Live(u64),
    Repair,
}
struct Applied {
    owner: Owner,
    id: proto::EntityId,
    head: Option<proto::Clock>,
    change: Option<EntityChange>,
    bytes: usize,
    repair: Option<TargetKey>,
}
struct Publication {
    revision: u64,
    changes: IndexMap<proto::EntityId, Option<proto::QueryMember>>,
    applied: BTreeMap<proto::EntityId, Vec<proto::Attested<proto::Event>>>,
    retained_bytes: usize,
    change_bytes: usize,
    goals: Goals,
    jobs: usize,
    complete: bool,
}
struct Assembly {
    owner: Owner,
    id: proto::EntityId,
    target: proto::Clock,
    bytes: Vec<u8>,
    member: Option<proto::QueryMember>,
    sender: tokio::sync::oneshot::Sender<proto::EntityDelta>,
}
struct PendingRound {
    number: u64,
    snapshot: Snapshot,
    request: RefreshRequest,
}

pub(super) struct Driver {
    // Own the stream's tree, including old generations during replacement.
    // LiveQueryInner separately keeps the active selection pinned across disconnects.
    resident: Option<super::super::resident::ResidentPin>,
    projection: Projection,
    spec: proto::TreeSpec,
    goals: Goals,
    round_jobs: usize,
    round_changed: HashSet<proto::EntityId>,
    // Optional residency until this finite query round installs. The budget
    // measures full encoded state at admission, not heap usage: canonical
    // entities remain mutable and may grow before their next observation.
    round_pins: HashMap<proto::EntityId, (crate::entity::Entity, usize)>,
    round_pin_bytes: usize,
    jobs: FuturesUnordered<BoxFuture<'static, anyhow::Result<Applied>>>,
    queued_jobs: BTreeMap<proto::EntityId, VecDeque<BoxFuture<'static, anyhow::Result<Applied>>>>,
    active_entities: HashSet<proto::EntityId>,
    application_jobs: usize,
    round_application_slots: Arc<tokio::sync::Semaphore>,
    live_application_slots: Arc<tokio::sync::Semaphore>,
    assemblies: BTreeMap<TargetKey, Assembly>,
    discarded_chunks: HashSet<TargetKey>,
    assembly_bytes: usize,
    observed: HashMap<proto::EntityId, proto::Clock>,
    eligible: HashSet<proto::EntityId>,
    publications: VecDeque<Publication>,
    applied_through: Option<u64>,
    pub through: u64,
    pub feedback: Arc<Feedback>,
    pub stream: IncomingStream,
    pub stream_id: proto::StateStreamId,
    round_number: u64,
    received_revision: u64,
    waiting_applied: bool,
    controls: FuturesUnordered<BoxFuture<'static, anyhow::Result<()>>>,
    refresh_rx: tokio::sync::mpsc::Receiver<Command>,
    refresh: RefreshHandle,
    pending_round: Option<PendingRound>,
    round_waiter: Option<RefreshRequest>,
    queued_refresh: Option<RefreshRequest>,
    pub terminal: bool,
    application_bytes: usize,
    applying_repairs: HashSet<TargetKey>,
    membership_dirty: bool,
    partial_removals: HashSet<proto::EntityId>,
    baseline_installed: bool,
}

impl Driver {
    pub fn new(snapshot: Snapshot, round: proto::StateRound, stream: IncomingStream, stream_id: proto::StateStreamId) -> Self {
        let spec = snapshot.spec.clone();
        let observed = snapshot.entries.iter().map(|(id, entry)| (*id, entry.head.clone())).collect();
        let (tx, refresh_rx) = tokio::sync::mpsc::channel(2);
        Self {
            resident: snapshot.resident.clone(),
            projection: Projection::new(Round::new(round, snapshot)),
            spec,
            goals: Goals::default(),
            round_jobs: 0,
            round_changed: HashSet::new(),
            round_pins: HashMap::new(),
            round_pin_bytes: 0,
            jobs: FuturesUnordered::new(),
            queued_jobs: BTreeMap::new(),
            active_entities: HashSet::new(),
            application_jobs: 0,
            round_application_slots: Arc::new(tokio::sync::Semaphore::new(MAX_ACTIVE_ROUND_APPLICATIONS)),
            live_application_slots: Arc::new(tokio::sync::Semaphore::new(MAX_ACTIVE_LIVE_APPLICATIONS)),
            assemblies: BTreeMap::new(),
            discarded_chunks: HashSet::new(),
            assembly_bytes: 0,
            observed,
            eligible: HashSet::new(),
            publications: VecDeque::new(),
            applied_through: None,
            through: 0,
            feedback: Arc::new(Feedback::default()),
            stream,
            stream_id,
            round_number: 0,
            received_revision: round.revision,
            waiting_applied: false,
            controls: FuturesUnordered::new(),
            refresh_rx,
            refresh: RefreshHandle(tx),
            pending_round: None,
            round_waiter: None,
            queued_refresh: None,
            terminal: false,
            application_bytes: 0,
            applying_repairs: HashSet::new(),
            membership_dirty: false,
            partial_removals: HashSet::new(),
            baseline_installed: false,
        }
    }

    pub async fn prepare<SE, PA>(&mut self, node: &Node<SE, PA>, query: Option<(proto::QueryId, u32)>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if let Some((id, version)) = query {
            let query = node.live_queries.get(id).ok_or_else(|| anyhow::anyhow!("query disappeared"))?;
            query.set_refresh(version, &self.stream.request, self.refresh_handle());
            self.eligible = query.resultset().read().iter_entities().map(|(id, _)| id).collect();
            self.install(node, Some((id, version)), false, HashSet::new(), vec![]).await?;
        }
        Ok(())
    }

    async fn install<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        query: Option<(proto::QueryId, u32)>,
        complete: bool,
        replace: HashSet<proto::EntityId>,
        changes: Vec<EntityChange>,
    ) -> anyhow::Result<HashSet<proto::EntityId>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let Some((id, version)) = query else { return Ok(HashSet::new()) };
        let query = node.live_queries.get(id).ok_or_else(|| anyhow::anyhow!("query disappeared"))?;
        ensure!(query.set_remote_candidates(version, &self.stream.request, self.eligible.iter().copied()), "query stream superseded");
        let state_getter = LocalStateGetter::new(node.storage.clone());
        let event_getter = LocalEventGetter::new(node.storage.clone(), node.durable);
        let mut entities = Vec::with_capacity(self.eligible.len());
        for id in &self.eligible {
            if let Some(entity) = node.entities.get_or_retrieve(&state_getter, &event_getter, id).await? {
                entities.push(entity);
            }
        }
        let origin = &self.stream.request;
        let canceled = || !query.is_current(version);
        let preserve = self
            .publications
            .iter()
            .flat_map(|publication| publication.changes.keys())
            .filter(|id| self.projection.members.contains_key(id) || !replace.contains(id))
            .copied()
            .collect();
        let baseline = self
            .projection
            .round
            .as_ref()
            .map(|round| round.members.iter().map(|(id, member)| (*id, member.head.clone())).collect())
            .unwrap_or_default();
        let membership = crate::reactor::PeerQueryMembership { replace, remove: self.partial_removals.clone(), preserve, baseline };
        let installed = if complete {
            query.activate_remote(version, entities, membership, changes, origin, &canceled).await?
        } else {
            query.install_remote_partial(version, entities, membership, changes, origin, &canceled).await?
        };
        let notified = installed.ok_or_else(|| anyhow::anyhow!("query stream superseded during installation"))?;
        self.partial_removals.clear();
        Ok(notified)
    }

    fn publication(&mut self, revision: u64) -> anyhow::Result<&mut Publication> {
        if !self.publications.back().is_some_and(|publication| publication.revision == revision) {
            ensure!(
                self.publications.back().is_none_or(|publication| publication.complete && publication.revision < revision),
                "interleaved source publications"
            );
            ensure!(revision > self.received_revision, "stale source publication");
            ensure!(self.publications.len() < super::super::MAX_INFLIGHT_FRAMES, "too many pending source publications");
            self.publications.push_back(Publication {
                revision,
                changes: IndexMap::new(),
                applied: BTreeMap::new(),
                retained_bytes: 0,
                change_bytes: 0,
                goals: Goals::default(),
                jobs: 0,
                complete: false,
            });
        }
        let publication = self.publications.back_mut().unwrap();
        ensure!(!publication.complete, "publication continued after its boundary");
        Ok(publication)
    }

    fn enqueue<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        owner: Owner,
        delta: proto::EntityDelta,
        repair: Option<proto::Clock>,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let bytes = bincode::serialized_size(&delta)? as usize;
        ensure!(
            self.application_bytes + self.assembly_bytes + bytes <= super::super::MAX_REPAIR_BYTES + super::super::WINDOW_BYTES,
            "application byte queue exceeded limit"
        );
        self.application_bytes += bytes;
        let id = delta.entity_id;
        self.reserve_application(node, peer, credentials, owner, id, repair, Box::pin(async move { Some(delta) }))
    }

    /// Reserve at the first chunk, not after assembly. Later data for this ID
    /// must not overtake an earlier payload whose bytes are still arriving.
    fn reserve_application<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        owner: Owner,
        id: proto::EntityId,
        repair: Option<proto::Clock>,
        input: BoxFuture<'static, Option<proto::EntityDelta>>,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        ensure!(self.application_jobs < super::super::MAX_INFLIGHT_FRAMES * 128, "application queue exceeded limit");
        self.application_jobs += 1;
        match owner {
            Owner::Round(_) => self.round_jobs += 1,
            Owner::Live(revision) => self.publication(revision)?.jobs += 1,
            Owner::Repair => {}
        }
        let repair = repair.map(|target| target_key(id, &target));
        if let Some(key) = &repair {
            self.applying_repairs.insert(key.clone());
        }
        let node = node.weak();
        let credentials = credentials.to_vec();
        let application_slots = match owner {
            Owner::Round(_) => self.round_application_slots.clone(),
            Owner::Live(_) | Owner::Repair => self.live_application_slots.clone(),
        };
        let job = Box::pin(async move {
            // Dropping a cancelled round's partial assembly releases its reserved
            // slot without applying or announcing membership.
            let Some(delta) = input.await else { return Ok(Applied { owner, id, head: None, change: None, bytes: 0, repair }) };
            let bytes = bincode::serialized_size(&delta)? as usize;
            // Bound active storage work separately from queued bytes. Partial
            // chunk inputs reserve entity order without occupying a slot.
            let _slot = application_slots.acquire_owned().await?;
            let node = node.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            let events = CachedEventGetter::new(&node, &credentials);
            let states = LocalStateGetter::new(node.storage.clone());
            let outcome = NodeApplier::apply_delta(
                &node,
                &peer,
                delta,
                if matches!(owner, Owner::Live(_)) { Delivery::Live } else { Delivery::Initial },
                &events,
                &states,
            )
            .await?;
            let head = outcome.entity.as_ref().map(|entity| entity.head());
            let change = if matches!(owner, Owner::Live(_)) && !outcome.live_events.is_empty() {
                let entity = outcome.entity.ok_or_else(|| anyhow::anyhow!("accepted live events have no entity"))?;
                Some(EntityChange::new(entity, outcome.live_events))
            } else {
                outcome.change
            };
            Ok(Applied { owner, id, head, change, bytes, repair })
        });
        self.push_application(id, job);
        Ok(())
    }

    fn push_application(&mut self, id: proto::EntityId, job: BoxFuture<'static, anyhow::Result<Applied>>) {
        if self.active_entities.insert(id) {
            self.jobs.push(job);
        } else {
            self.queued_jobs.entry(id).or_default().push_back(job);
        }
    }

    async fn observe<SE, PA>(&mut self, node: &Node<SE, PA>, applied: Applied) -> anyhow::Result<Option<EntityChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        self.application_bytes -= applied.bytes;
        self.application_jobs = self.application_jobs.saturating_sub(1);
        self.active_entities.remove(&applied.id);
        if let Some(queued) = self.queued_jobs.get_mut(&applied.id) {
            if let Some(next) = queued.pop_front() {
                self.active_entities.insert(applied.id);
                self.jobs.push(next);
            }
            if queued.is_empty() {
                self.queued_jobs.remove(&applied.id);
            }
        }
        if let Some(key) = &applied.repair {
            self.applying_repairs.remove(key);
        }
        match applied.owner {
            Owner::Round(number) if number == self.round_number && self.projection.round.is_some() => self.round_jobs -= 1,
            Owner::Live(revision) => {
                if let Some(publication) = self.publications.iter_mut().find(|publication| publication.revision == revision) {
                    publication.jobs -= 1;
                    if let Some(change) = &applied.change {
                        // Retain only accepted events, not a resident entity whose
                        // state may be far larger than this publication's delta.
                        let bytes = bincode::serialized_size(change.events())? as usize;
                        publication.retained_bytes += bytes + proto::EntityId::BYTE_LEN;
                        publication.applied.entry(applied.id).or_default().extend_from_slice(change.events());
                    }
                }
            }
            _ => {}
        }
        if let Some(head) = applied.head {
            self.observe_head(node, applied.id, head).await?;
        }
        if applied.change.is_some() {
            let round_changed = match applied.owner {
                Owner::Round(number) => number == self.round_number && self.projection.round.is_some(),
                Owner::Repair => self.projection.round.is_some() && self.goals.rows.contains_key(&applied.id),
                Owner::Live(_) => false,
            };
            if self.stream.query.is_some() && round_changed {
                ensure!(self.round_changed.contains(&applied.id) || self.round_changed.len() < 100_000, "too many changed round entities");
                self.round_changed.insert(applied.id);
            }
            if self.round_changed.contains(&applied.id) {
                self.pin_round_entity(node, applied.change.as_ref().unwrap().entity());
            }
            if applied.owner == Owner::Repair {
                // Repair is Initial delivery: its history is not a new source
                // event batch. Still publish an actual state advance at the
                // waiting publication's own coherent boundary.
                for publication in &mut self.publications {
                    if publication.goals.rows.contains_key(&applied.id) && !publication.applied.contains_key(&applied.id) {
                        publication.applied.insert(applied.id, vec![]);
                        publication.retained_bytes += proto::EntityId::BYTE_LEN;
                    }
                }
            }
        }
        Ok(applied.change)
    }

    fn remove_round_pin(&mut self, id: &proto::EntityId) {
        if let Some((_, bytes)) = self.round_pins.remove(id) {
            self.round_pin_bytes -= bytes;
        }
    }

    fn clear_round_pins(&mut self) {
        self.round_pins.clear();
        self.round_pin_bytes = 0;
    }

    fn pin_round_entity<SE, PA>(&mut self, node: &Node<SE, PA>, entity: &crate::entity::Entity)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let id = entity.id();
        // Refresh the charge after any observed actual change, including live
        // data advancing a row already retained for this round.
        self.remove_round_pin(&id);
        let Some((query, _)) = self.stream.query else { return };
        let Some(query) = node.live_queries.get(query) else { return };
        if query.resultset().contains_key(&id) || self.round_pins.len() >= 100_000 || self.round_pin_bytes >= super::super::WINDOW_BYTES {
            return;
        }
        let Some(bytes) = entity
            .to_state()
            .ok()
            .and_then(|state| bincode::serialized_size(&state).ok())
            .and_then(|bytes| usize::try_from(bytes).ok())
            .and_then(|bytes| bytes.checked_add(proto::EntityId::BYTE_LEN))
        else {
            return;
        };
        // Pinning is an optimization. A large state or exhausted admission
        // budget falls back to the ordinary storage reload at installation.
        if bytes <= super::super::WINDOW_BYTES - self.round_pin_bytes {
            self.round_pin_bytes += bytes;
            self.round_pins.insert(id, (entity.clone(), bytes));
        }
    }

    async fn observe_head<SE, PA>(&mut self, node: &Node<SE, PA>, id: proto::EntityId, head: proto::Clock) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        for goal in self.goals.for_id_mut(&id).chain(self.publications.iter_mut().flat_map(|publication| publication.goals.for_id_mut(&id)))
        {
            if goal.id == id && !goal.covered && covers_target(node, &head, &goal.head).await? {
                goal.covered = true;
            }
        }
        if let Some(member) = self.projection.members.get(&id) {
            if (self.baseline_installed || self.projection.has_live_decision(&id)) && covers_target(node, &head, &member.head).await? {
                self.membership_dirty |= self.eligible.insert(id);
            }
        }
        if self.projection.members.contains_key(&id)
            || self.goals.rows.contains_key(&id)
            || self.publications.iter().any(|publication| publication.goals.rows.contains_key(&id))
        {
            self.observed.insert(id, head);
        }
        Ok(())
    }

    fn add_goal(&mut self, owner: Owner, id: proto::EntityId, target: proto::Clock) -> anyhow::Result<()> {
        let covered = self.observed.get(&id) == Some(&target);
        match owner {
            Owner::Round(_) => self.goals.add(id, target, covered),
            Owner::Live(revision) => self.publication(revision)?.goals.add(id, target, covered),
            Owner::Repair => Ok(()),
        }
    }

    fn chunk<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        owner: Owner,
        target: proto::Clock,
        member: Option<proto::QueryMember>,
        chunk: proto::EntityDeltaChunk,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        ensure!(!chunk.bytes.is_empty(), "empty delta chunk");
        let key = bincode::serialize(&(
            match owner {
                Owner::Round(n) => (0, n),
                Owner::Live(n) => (1, n),
                Owner::Repair => (2, 0),
            },
            chunk.entity_id,
            &target,
        ))?;
        if self.discarded_chunks.contains(&key) {
            return Ok(());
        }
        if !self.assemblies.contains_key(&key) {
            ensure!(
                self.assemblies.len() < super::super::MAX_INFLIGHT_FRAMES && chunk.offset == 0,
                "too many or out-of-order partial deltas"
            );
            let (sender, receive) = tokio::sync::oneshot::channel();
            self.reserve_application(
                node,
                peer,
                credentials,
                owner,
                chunk.entity_id,
                (!matches!(owner, Owner::Live(_))).then(|| target.clone()),
                Box::pin(async move { receive.await.ok() }),
            )?;
            self.assemblies.insert(
                key.clone(),
                Assembly { owner, id: chunk.entity_id, target: target.clone(), bytes: Vec::new(), member: member.clone(), sender },
            );
        }
        let assembly = self.assemblies.get_mut(&key).unwrap();
        ensure!(
            assembly.owner == owner
                && assembly.target == target
                && assembly.member == member
                && chunk.offset == assembly.bytes.len() as u64,
            "invalid delta chunk sequence"
        );
        ensure!(
            self.assembly_bytes + chunk.bytes.len() <= super::super::MAX_REPAIR_BYTES
                && self.application_bytes + self.assembly_bytes + chunk.bytes.len()
                    <= super::super::MAX_REPAIR_BYTES + super::super::WINDOW_BYTES,
            "delta assembly exceeds limit"
        );
        self.assembly_bytes += chunk.bytes.len();
        assembly.bytes.extend_from_slice(&chunk.bytes);
        if chunk.last {
            let assembly = self.assemblies.remove(&key).unwrap();
            let content = decode_delta(&assembly.bytes)?;
            if !matches!(owner, Owner::Live(_)) {
                check_target(&assembly.target, &content)?;
            }
            let delta = proto::EntityDelta { entity_id: chunk.entity_id, content };
            self.assembly_bytes -= assembly.bytes.len();
            self.application_bytes += bincode::serialized_size(&delta)? as usize;
            ensure!(
                self.application_bytes + self.assembly_bytes <= super::super::MAX_REPAIR_BYTES + super::super::WINDOW_BYTES,
                "application byte queue exceeded limit"
            );
            if matches!(owner, Owner::Live(_)) {
                self.add_live_delta_targets(
                    match owner {
                        Owner::Live(revision) => revision,
                        _ => unreachable!(),
                    },
                    delta.entity_id,
                    &assembly.member,
                    Some(&delta.content),
                )?;
                self.record_change(
                    match owner {
                        Owner::Live(revision) => revision,
                        _ => unreachable!(),
                    },
                    delta.entity_id,
                    assembly.member,
                )?;
            } else {
                self.repair_arrived(delta.entity_id, &assembly.target);
            }
            let _ = assembly.sender.send(delta);
        }
        Ok(())
    }

    async fn items<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        owner: Owner,
        items: Vec<StateItem>,
    ) -> anyhow::Result<Vec<StateAnswer>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        ensure!(items.len() <= 128, "state item limit exceeded");
        let mut answers = Vec::new();
        for item in items {
            match item {
                StateItem::Digest(head) => {
                    ensure!(matches!(owner, Owner::Round(_)), "live repair contains comparison question");
                    head.validate()?;
                    answers.push(StateAnswer::Digest(self.projection.round.as_mut().unwrap().snapshot.span_digest(head.bounds).await?));
                }
                StateItem::NeedHeads(bounds) => {
                    ensure!(matches!(owner, Owner::Round(_)), "live repair contains comparison question");
                    answers.push(StateAnswer::Heads(self.projection.round.as_ref().unwrap().snapshot.page(bounds)?));
                }
                StateItem::Heads(page) => {
                    ensure!(matches!(owner, Owner::Round(_)), "live repair contains membership");
                    ensure!(4 + page.expanded_size() <= super::super::PAGE_BYTES as u64, "peer head inventory exceeds decoded-work budget");
                    ensure!(page.heads.len() <= super::super::HEAD_PAGE, "peer sent too many heads");
                    page.validate(&self.spec)?;
                    let mut cursor = page.heads.cursor();
                    while let Some(leaf) = cursor.next() {
                        self.add_goal(owner, leaf.entity.entity_id, leaf.entity.head.to_owned())?;
                    }
                    self.projection.round.as_mut().unwrap().replace(page);
                }
                StateItem::Repair { target, delta } => {
                    check_target(&target, &delta.content)?;
                    self.repair_arrived(delta.entity_id, &target);
                    self.add_goal(owner, delta.entity_id, target.clone())?;
                    self.enqueue(node, peer, credentials, owner, delta, Some(target))?;
                }
                StateItem::RepairChunk { target, chunk } => {
                    self.add_goal(owner, chunk.entity_id, target.clone())?;
                    self.chunk(node, peer, credentials, owner, target, None, chunk)?;
                }
                StateItem::RepairUnavailable { entity_id, target } => {
                    let required = |goals: &Goals| {
                        goals.rows.get(&entity_id).is_some_and(|rows| rows.values().any(|goal| goal.head == target && !goal.covered))
                    };
                    if !required(&self.goals) && !self.publications.iter().any(|publication| required(&publication.goals)) {
                        continue;
                    }
                    let actual =
                        self.observed.get(&entity_id).cloned().or_else(|| node.entities.get(&entity_id).map(|entity| entity.head()));
                    if let Some(head) = actual {
                        if covers_target(node, &head, &target).await? {
                            self.observe_head(node, entity_id, head).await?;
                            continue;
                        }
                    }
                    bail!("peer cannot repair required state for {entity_id}");
                }
            }
        }
        Ok(answers)
    }

    fn add_live_delta_targets(
        &mut self,
        revision: u64,
        id: proto::EntityId,
        member: &Option<proto::QueryMember>,
        content: Option<&proto::DeltaContent>,
    ) -> anyhow::Result<()> {
        if let Some(member) = member {
            CanonicalCodec.split(self.spec.key_spec(), &member.key)?;
            self.add_goal(Owner::Live(revision), id, member.head.clone())?;
        }
        if let Some(content) = content {
            let target = match content {
                proto::DeltaContent::StateSnapshot { state } | proto::DeltaContent::StateAndEvents { state, .. } => {
                    state.state.head.clone()
                }
                proto::DeltaContent::EventBridge { .. } => {
                    member.as_ref().ok_or_else(|| anyhow::anyhow!("absent event bridge has no target"))?.head.clone()
                }
                _ => bail!("unsupported live delta"),
            };
            self.add_goal(Owner::Live(revision), id, target)?;
        }
        Ok(())
    }
    fn record_change(&mut self, revision: u64, id: proto::EntityId, member: Option<proto::QueryMember>) -> anyhow::Result<()> {
        let publication = self.publication(revision)?;
        if let Some(previous) = publication.changes.insert(id, member.clone()) {
            publication.change_bytes -= bincode::serialized_size(&(id, previous))? as usize;
        }
        publication.change_bytes += bincode::serialized_size(&(id, member))? as usize;
        Ok(())
    }

    async fn live<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        revision: u64,
        items: Vec<proto::QueryChange>,
        complete: bool,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        ensure!(items.len() <= 128, "live item limit exceeded");
        for item in items {
            self.add_live_delta_targets(revision, item.entity_id, &item.member, item.delta.as_ref())?;
            if let Some(content) = item.delta {
                self.enqueue(
                    node,
                    peer,
                    credentials,
                    Owner::Live(revision),
                    proto::EntityDelta { entity_id: item.entity_id, content },
                    None,
                )?;
            }
            self.record_change(revision, item.entity_id, item.member)?;
        }
        if complete {
            ensure!(
                !self.assemblies.values().any(|assembly| assembly.owner == Owner::Live(revision)),
                "publication completed inside a chunk"
            );
            self.publication(revision)?.complete = true;
            self.received_revision = revision;
        }
        Ok(())
    }

    async fn frame<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        frame: proto::StateFrame,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        ensure!(frame.stream == self.stream_id, "state stream identity changed");
        self.through = frame.sequence;
        let mut answers = Vec::new();
        let mut answer_round = None;
        match frame.body {
            StateFrameBody::BeginRound(descriptor) => {
                ensure!(!self.publications.back().is_some_and(|publication| !publication.complete), "round split a publication");
                let pending = self.pending_round.take().ok_or_else(|| anyhow::anyhow!("unsolicited round"))?;
                ensure!(pending.number == descriptor.number && descriptor.number == self.round_number + 1, "wrong round number");
                ensure!(descriptor.revision >= self.received_revision, "round cut precedes received source publications");
                self.received_revision = descriptor.revision;
                self.resident = pending.snapshot.resident.clone();
                self.projection.begin(Round::new(descriptor, pending.snapshot))?;
                self.round_number = descriptor.number;
                self.round_waiter = Some(pending.request);
                self.goals = Goals::default();
                self.round_jobs = 0;
            }
            StateFrameBody::Round { round, items, progress } => {
                if round == self.round_number && self.projection.round.is_some() {
                    answers = self.items(node, peer, credentials, Owner::Round(round), items).await?;
                    answer_round = Some(round);
                    if progress == ComparisonProgress::Complete {
                        self.projection.round.as_mut().unwrap().complete = true;
                    }
                } else {
                    ensure!(round <= self.round_number, "state frame precedes round begin");
                }
            }
            StateFrameBody::Live { revision, items, complete } => self.live(node, peer, credentials, revision, items, complete).await?,
            StateFrameBody::LiveChunk { revision, member, chunk, complete } => {
                self.publication(revision)?;
                let target = None::<proto::Clock>;
                // Absent chunk data must be snapshot-bearing: decode before learning its data target.
                let target = target.unwrap_or_default();
                let last = chunk.last;
                self.chunk(node, peer, credentials, Owner::Live(revision), target, member, chunk)?;
                ensure!(last || !complete, "publication completed inside a chunk");
                if complete {
                    self.live(node, peer, credentials, revision, vec![], true).await?;
                }
            }
            StateFrameBody::LiveRepair { items } => {
                self.items(node, peer, credentials, Owner::Repair, items).await?;
            }
            StateFrameBody::RestartRequired => return Err(RequestError::ConnectionLost.into()),
            StateFrameBody::SourceUnavailable => {
                self.terminal = true;
                bail!("source unavailable; its node must be reconstructed");
            }
        }
        self.check_retained()?;
        self.feedback.queue(
            node,
            peer,
            credentials,
            self.stream_id,
            self.through,
            answer_round,
            self.applied_through,
            answers,
            &self.stream.inbox,
        )?;
        Ok(())
    }

    fn repair_arrived(&mut self, id: proto::EntityId, target: &proto::Clock) {
        for goal in self.goals.for_id_mut(&id).chain(self.publications.iter_mut().flat_map(|publication| publication.goals.for_id_mut(&id)))
        {
            if &goal.head == target {
                goal.requested = false;
            }
        }
    }

    fn check_retained(&self) -> anyhow::Result<()> {
        let targets: usize = self.publications.iter().map(|publication| publication.goals.bytes).sum();
        let ids: usize = self.publications.iter().map(|publication| publication.goals.count).sum();
        let changes: usize = self.publications.iter().map(|publication| publication.change_bytes).sum();
        ensure!(targets <= super::super::WINDOW_BYTES && ids <= 100_000, "unapplied live target window exhausted");
        ensure!(
            changes + self.projection.retained_bytes() + self.round_changed.len() * proto::EntityId::BYTE_LEN <= super::super::WINDOW_BYTES,
            "membership notification window exhausted"
        );
        let retained: usize = self.publications.iter().map(|publication| publication.retained_bytes).sum();
        ensure!(
            retained + self.application_bytes + self.assembly_bytes <= super::super::MAX_REPAIR_BYTES + super::super::WINDOW_BYTES,
            "pending publication data exceeded byte limit"
        );
        Ok(())
    }

    fn prune_observed(&mut self) {
        self.observed.retain(|id, _| {
            self.projection.members.contains_key(id)
                || self.goals.rows.contains_key(id)
                || self.publications.iter().any(|publication| publication.goals.rows.contains_key(id))
        });
    }

    async fn repairs<SE, PA>(&mut self, node: &Node<SE, PA>, peer: proto::EntityId, credentials: &[PA::ContextData]) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // A target is checked only after its currently supplied data finishes.
        // Repeated IDs may carry several batches leading to one final member head.
        let mut groups = Vec::new();
        if self.round_jobs == 0
            && self.projection.round.as_ref().is_some_and(|round| round.complete)
            && !self.assemblies.values().any(|assembly| matches!(assembly.owner, Owner::Round(_)))
        {
            groups.push((Some(self.round_number), &mut self.goals));
        }
        for publication in &mut self.publications {
            if publication.complete && publication.jobs == 0 {
                groups.push((None, &mut publication.goals));
            }
        }
        for (round, goals) in groups {
            let mut answers = Vec::new();
            for goal in goals.values_mut().filter(|goal| !goal.covered) {
                if !self.observed.contains_key(&goal.id) {
                    let actual = match node.entities.get(&goal.id) {
                        Some(entity) => Some(entity.head()),
                        None => match node.storage.get_state(goal.id).await {
                            Ok(state) => Some(state.payload.state.head),
                            Err(RetrievalError::EntityNotFound(_)) => None,
                            Err(error) => return Err(error.into()),
                        },
                    };
                    if let Some(actual) = actual {
                        self.observed.insert(goal.id, actual);
                    }
                }
                if let Some(head) = self.observed.get(&goal.id) {
                    if covers_target(node, head, &goal.head).await? {
                        goal.covered = true;
                        if (self.baseline_installed || self.projection.has_live_decision(&goal.id))
                            && self.projection.members.get(&goal.id).is_some_and(|member| member.head == goal.head)
                        {
                            self.membership_dirty |= self.eligible.insert(goal.id);
                        }
                        continue;
                    }
                }
                // One retry may be pending; its completion clears this marker below.
                if goal.requested || self.applying_repairs.contains(&target_key(goal.id, &goal.head)) {
                    continue;
                }
                ensure!(goal.attempts < 2, "connecting history failed to cover target");
                goal.attempts += 1;
                goal.requested = true;
                answers.push(StateAnswer::NeedRepair { entity_id: goal.id, target: goal.head.clone() });
            }
            if !answers.is_empty() {
                self.feedback.queue(
                    node,
                    peer,
                    credentials,
                    self.stream_id,
                    self.through,
                    round,
                    self.applied_through,
                    answers,
                    &self.stream.inbox,
                )?;
            }
        }
        Ok(())
    }

    fn discard_covered_round_chunks(&mut self) {
        if !self.projection.round.as_ref().is_some_and(|round| round.complete) {
            return;
        }
        let covered = self
            .assemblies
            .iter()
            .filter_map(|(key, assembly)| {
                (assembly.owner == Owner::Round(self.round_number)
                    && self
                        .goals
                        .rows
                        .get(&assembly.id)
                        .is_some_and(|rows| rows.values().any(|goal| goal.head == assembly.target && goal.covered)))
                .then(|| key.clone())
            })
            .collect::<Vec<_>>();
        for key in covered {
            let assembly = self.assemblies.remove(&key).unwrap();
            self.assembly_bytes -= assembly.bytes.len();
            // Dropping sender resolves the reserved job. Ignore the remaining
            // bytes until this round retires; they no longer prove anything needed.
            self.discarded_chunks.insert(key);
        }
    }

    fn deferred_notification(&self, owner: Owner, id: proto::EntityId) -> bool {
        !matches!(owner, Owner::Repair)
            || self.goals.rows.contains_key(&id)
            || self.publications.iter().any(|publication| publication.changes.contains_key(&id) || publication.goals.rows.contains_key(&id))
    }

    fn can_receive(&self) -> bool { self.application_bytes < super::super::WINDOW_BYTES || !self.assemblies.is_empty() }

    async fn commit_publications<SE, PA>(&mut self, node: &Node<SE, PA>, query: Option<(proto::QueryId, u32)>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut advanced = false;
        while self
            .publications
            .front()
            .is_some_and(|publication| publication.complete && publication.jobs == 0 && publication.goals.covered())
        {
            let mut publication = self.publications.pop_front().unwrap();
            let mut changes = Vec::new();
            for (id, member) in &publication.changes {
                let previous = self.projection.members.get(id);
                self.membership_dirty |= match (previous, member.as_ref()) {
                    (Some(previous), Some(member)) => previous.key != member.key,
                    (None, None) => false,
                    _ => true,
                };
                if member.is_some() {
                    self.membership_dirty |= self.eligible.insert(*id);
                    self.partial_removals.remove(id);
                } else {
                    self.eligible.remove(id);
                    self.partial_removals.insert(*id);
                    // An independently admitted ID may not be source-eligible.
                    self.membership_dirty = true;
                }
                if let Some(events) = publication.applied.remove(id) {
                    let states = LocalStateGetter::new(node.storage.clone());
                    let history = LocalEventGetter::new(node.storage.clone(), node.durable);
                    if let Some(entity) = node.entities.get_or_retrieve(&states, &history, id).await? {
                        changes.push(EntityChange::new(entity, events));
                    } else {
                        bail!("applied publication entity disappeared");
                    }
                }
            }
            self.projection.publish(publication.revision, publication.changes.into_iter().collect())?;
            let actual_changes: HashSet<_> =
                if self.round_changed.is_empty() { HashSet::new() } else { changes.iter().map(|change| change.entity().id()).collect() };
            let notified = if self.membership_dirty {
                let notified = self.install(node, query, false, HashSet::new(), changes).await?;
                self.membership_dirty = false;
                notified
            } else if let Some((id, version)) = query {
                if let Some(query) = node.live_queries.get(id) {
                    let origin = crate::reactor::QueryOrigin { query: id, version, request: self.stream.request.clone() };
                    let changes =
                        changes.into_iter().map(|change| StreamChange { change, origin: Some(origin.clone()), deferred: false }).collect();
                    let preserve = self.publications.iter().flat_map(|publication| publication.changes.keys()).copied().collect();
                    node.reactor.notify_query_changes(query.subscription_id(), id, changes, preserve).await?
                } else {
                    HashSet::new()
                }
            } else {
                HashSet::new()
            };
            for id in actual_changes.intersection(&notified) {
                self.round_changed.remove(id);
                self.remove_round_pin(id);
            }
            self.applied_through = Some(publication.revision);
            advanced = true;
        }
        if advanced {
            self.prune_observed();
        }
        self.check_retained()?;
        Ok(())
    }

    async fn complete_round<SE, PA>(&mut self, node: &Node<SE, PA>, query: Option<(proto::QueryId, u32)>) -> anyhow::Result<bool>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if self.round_jobs != 0
            || !self.goals.covered()
            || !self.projection.round.as_ref().is_some_and(|round| round.complete)
            || self.assemblies.values().any(|assembly| matches!(assembly.owner, Owner::Round(_)))
            || self
                .projection
                .round
                .as_ref()
                .is_some_and(|round| self.publications.iter().any(|publication| publication.revision <= round.descriptor.revision))
        {
            return Ok(false);
        }
        self.feedback.cancel_round(self.round_number);
        self.discarded_chunks.clear();
        let round = self.projection.round.as_ref().unwrap();
        let replace = round.snapshot.entries.keys().copied().collect();
        self.partial_removals.extend(round.absent_ids());
        let baseline = self.projection.finish(self.round_number)?;
        for id in baseline.keys() {
            if self.projection.members.contains_key(id) {
                self.eligible.insert(*id);
            }
        }
        self.eligible.retain(|id| self.projection.members.contains_key(id));
        let mut changes = Vec::with_capacity(self.round_changed.len());
        let states = LocalStateGetter::new(node.storage.clone());
        let history = LocalEventGetter::new(node.storage.clone(), node.durable);
        for id in &self.round_changed {
            let entity = match self.round_pins.get(id) {
                Some((entity, _)) => Some(entity.clone()),
                None => node.entities.get_or_retrieve(&states, &history, id).await?,
            };
            if let Some(entity) = entity {
                changes.push(EntityChange::new(entity, vec![]));
            }
        }
        self.install(node, query, true, replace, changes).await?;
        self.round_changed.clear();
        self.clear_round_pins();
        self.prune_observed();
        self.baseline_installed = true;
        self.membership_dirty = false;
        self.goals = Goals::default();
        if let Some(reply) = self.round_waiter.take() {
            let _ = reply.reply.send(Ok(()));
        }
        Ok(true)
    }

    fn cancel_round<SE, PA>(&mut self, node: &Node<SE, PA>, peer: proto::EntityId, credentials: &[PA::ContextData]) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let number = self.round_number;
        self.projection.cancel(number)?;
        self.round_waiter = None;
        self.goals = Goals::default();
        self.round_jobs = 0;
        self.round_changed.clear();
        self.clear_round_pins();
        self.assemblies.retain(|_, assembly| {
            if assembly.owner == Owner::Round(number) {
                self.assembly_bytes -= assembly.bytes.len();
                false
            } else {
                true
            }
        });
        self.feedback.cancel_round(number);
        self.waiting_applied = true;
        let node = node.weak();
        let credentials = credentials.to_vec();
        let stream_id = self.stream_id;
        self.controls.push(Box::pin(async move {
            if let Some(node) = node.upgrade() {
                finish_state_round(&node, peer, stream_id, number, &credentials, proto::StateRoundOutcome::Cancelled).await?;
            }
            Ok(())
        }));
        self.prune_observed();
        Ok(())
    }

    pub fn acknowledge_round<SE, PA>(&mut self, node: &Node<SE, PA>, peer: proto::EntityId, credentials: &[PA::ContextData])
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        self.waiting_applied = true;
        let node = node.weak();
        let credentials = credentials.to_vec();
        let stream_id = self.stream_id;
        let round = self.round_number;
        let outcome = proto::StateRoundOutcome::Applied { processed_through: self.through };
        self.controls.push(Box::pin(async move {
            if let Some(node) = node.upgrade() {
                finish_state_round(&node, peer, stream_id, round, &credentials, outcome).await?;
            }
            Ok(())
        }));
    }

    async fn start_refresh<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        query: Option<(proto::QueryId, u32)>,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if self.waiting_applied || self.projection.round.is_some() || self.pending_round.is_some() {
            return Ok(());
        }
        let Some(reply) = self.queued_refresh.take() else { return Ok(()) };
        if reply.reply.is_closed() {
            return Ok(());
        }
        let Some((id, version)) = query else {
            let _ = reply.reply.send(Err(RetrievalError::Other("Fetch cannot refresh".into())));
            return Ok(());
        };
        let query = node.live_queries.get(id).ok_or_else(|| anyhow::anyhow!("query disappeared"))?;
        let (selection, stored) = query.resolved_selection().ok_or_else(|| anyhow::anyhow!("query selection disappeared"))?;
        ensure!(stored == version, "refresh selection changed");
        let mut snapshot = node.resident_trees.capture(node, selection, Some(self.spec.clone()), credentials.to_vec()).await?.snapshot;
        query.retain_tree(version, snapshot.resident.clone());
        let known_state = snapshot.offer().await?;
        let number = self.round_number.checked_add(1).ok_or_else(|| anyhow::anyhow!("round number exhausted"))?;
        self.pending_round = Some(PendingRound { number, snapshot, request: reply });
        let node = node.weak();
        let credentials = credentials.to_vec();
        let stream_id = self.stream_id;
        self.controls.push(Box::pin(async move {
            let node = node.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            let response = state_deadline(node.request(
                peer,
                &credentials,
                proto::NodeRequestBody::RefreshState { stream: stream_id, round: number, known_state },
            ))
            .await??;
            ensure!(matches!(response, proto::NodeResponseBody::Success), "refresh rejected: {response:?}");
            Ok(())
        }));
        Ok(())
    }

    pub async fn drive<SE, PA>(
        &mut self,
        weak: crate::node::WeakNode<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        query: Option<(proto::QueryId, u32)>,
        initial: bool,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        loop {
            let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            ensure!(owns_state_stream(&node, peer, &self.stream), "state stream superseded");
            if self.round_waiter.as_ref().is_some_and(|request| request.reply.is_closed()) {
                self.cancel_round(&node, peer, credentials)?;
            }
            self.repairs(&node, peer, credentials).await?;
            self.discard_covered_round_chunks();
            let previous_applied = self.applied_through;
            self.commit_publications(&node, query).await?;
            if self.applied_through != previous_applied {
                self.feedback.queue(
                    &node,
                    peer,
                    credentials,
                    self.stream_id,
                    self.through,
                    None,
                    self.applied_through,
                    vec![],
                    &self.stream.inbox,
                )?;
            }
            if self.complete_round(&node, query).await? {
                if initial {
                    return Ok(());
                }
                self.acknowledge_round(&node, peer, credentials);
            }
            self.start_refresh(&node, peer, credentials, query).await?;
            drop(node);
            enum Event {
                Frame(anyhow::Result<proto::StateFrame>),
                Job(anyhow::Result<Applied>),
                Control(anyhow::Result<()>),
                Command(Command),
            }
            let event = {
                let inbox = self.stream.inbox.clone();
                let can_receive = self.can_receive();
                let idle = self.projection.round.is_none() && self.jobs.is_empty();
                let frame = async {
                    if can_receive {
                        inbox.next(idle).await
                    } else {
                        futures::future::pending().await
                    }
                }
                .fuse();
                let job = async {
                    if self.jobs.is_empty() {
                        futures::future::pending().await
                    } else {
                        self.jobs.next().await.unwrap()
                    }
                }
                .fuse();
                let control = async {
                    if self.controls.is_empty() {
                        futures::future::pending().await
                    } else {
                        self.controls.next().await.unwrap()
                    }
                }
                .fuse();
                let refresh = self.refresh_rx.recv().fuse();
                futures::pin_mut!(frame, job, control, refresh);
                futures::select! {
                    frame = frame => Event::Frame(frame), job = job => Event::Job(job), control = control => Event::Control(control),
                    refresh = refresh => match refresh { Some(refresh) => Event::Command(refresh), None => bail!("refresh control closed") },
                }
            };
            let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            match event {
                Event::Frame(frame) => {
                    self.frame(&node, peer, credentials, frame?).await?;
                }
                Event::Job(job) => {
                    let mut changes = Vec::new();
                    let job = job?;
                    let deferred = self.deferred_notification(job.owner, job.id);
                    if let Some(change) = self.observe(&node, job).await? {
                        changes.push((change, deferred));
                    }
                    for _ in 1..128 {
                        let Some(Some(job)) = self.jobs.next().now_or_never() else { break };
                        let job = job?;
                        let deferred = self.deferred_notification(job.owner, job.id);
                        if let Some(change) = self.observe(&node, job).await? {
                            changes.push((change, deferred));
                        }
                    }
                    notify_changes(
                        &node,
                        query.map(|(query, version)| crate::reactor::QueryOrigin { query, version, request: self.stream.request.clone() }),
                        changes,
                    )
                    .await?;
                }
                Event::Control(control) => {
                    control?;
                    if self.controls.is_empty() {
                        self.waiting_applied = false;
                    }
                }
                Event::Command(Command::Refresh(reply)) => {
                    if self.queued_refresh.is_some() || self.pending_round.is_some() || self.projection.round.is_some() {
                        let _ = reply.reply.send(Err(RetrievalError::Other("another round is active".into())));
                    } else if !reply.reply.is_closed() {
                        self.queued_refresh = Some(reply);
                    }
                }
                Event::Command(Command::Cancel(token)) => {
                    if self.queued_refresh.as_ref().is_some_and(|request| request.token == token) {
                        self.queued_refresh = None;
                    }
                    // A pending request remains registered until BeginRound. The
                    // origin token prevents a dropped old waiter canceling a new round.
                    if self.round_waiter.as_ref().is_some_and(|request| request.token == token) {
                        self.cancel_round(&node, peer, credentials)?;
                    }
                }
            }
        }
    }

    pub async fn finish_fetch(&mut self) -> anyhow::Result<()> {
        while let Some(result) = self.controls.next().await {
            result?;
        }
        self.stream.disarm_abandonment();
        Ok(())
    }
    pub fn ids(&self) -> Vec<proto::EntityId> { self.projection.members.keys().copied().collect() }
    pub fn refresh_handle(&self) -> RefreshHandle { self.refresh.clone() }
}

#[derive(Debug)]
struct StreamChange {
    change: EntityChange,
    origin: Option<crate::reactor::QueryOrigin>,
    deferred: bool,
}
impl std::fmt::Display for StreamChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.change.fmt(f) }
}
impl ChangeNotification for StreamChange {
    type Entity = crate::entity::Entity;
    type Event = proto::Attested<proto::Event>;
    fn into_parts(self) -> (Self::Entity, Vec<Self::Event>) { self.change.into_parts() }
    fn entity(&self) -> &Self::Entity { self.change.entity() }
    fn events(&self) -> &[Self::Event] { self.change.events() }
    fn query_origin(&self) -> Option<&crate::reactor::QueryOrigin> { self.origin.as_ref() }
    fn defers_query_membership(&self) -> bool { self.deferred }
}

/// Reactor batches must contain each identity at most once; it coalesces
/// repeated identities and therefore cannot represent two event batches for one ID.
async fn notify_changes<SE, PA>(
    node: &Node<SE, PA>,
    origin: Option<crate::reactor::QueryOrigin>,
    changes: Vec<(EntityChange, bool)>,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut batch = Vec::new();
    let mut ids = HashSet::new();
    for (change, deferred) in changes {
        if !ids.insert(change.entity().id()) {
            let failures = node.reactor.notify_change(std::mem::take(&mut batch)).await;
            node.live_queries.fail_notifications(&failures);
            if let Some(failure) = failures.into_iter().next() {
                return Err(failure.error.into());
            }
            ids.clear();
            ids.insert(change.entity().id());
        }
        batch.push(StreamChange { change, origin: origin.clone(), deferred });
    }
    if !batch.is_empty() {
        let failures = node.reactor.notify_change(batch).await;
        node.live_queries.fail_notifications(&failures);
        if let Some(failure) = failures.into_iter().next() {
            return Err(failure.error.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
