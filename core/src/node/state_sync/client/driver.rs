use super::*;
use crate::{changes::EntityChange, node::applier::Delivery};
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet, VecDeque};

use projection::{Projection, Round};

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
    changes: BTreeMap<proto::EntityId, Option<proto::QueryMember>>,
    change_bytes: usize,
    goals: Goals,
    jobs: usize,
    complete: bool,
}
struct Assembly {
    owner: Owner,
    target: proto::Clock,
    bytes: Vec<u8>,
    member: Option<proto::QueryMember>,
}
struct PendingRound {
    number: u64,
    snapshot: Snapshot,
    request: RefreshRequest,
}

pub(super) struct Driver {
    projection: Projection,
    spec: proto::TreeSpec,
    goals: Goals,
    round_jobs: usize,
    jobs: FuturesUnordered<BoxFuture<'static, anyhow::Result<Applied>>>,
    assemblies: BTreeMap<TargetKey, Assembly>,
    assembly_bytes: usize,
    observed: HashMap<proto::EntityId, proto::Clock>,
    eligible: HashSet<proto::EntityId>,
    publications: VecDeque<Publication>,
    applied_through: Option<u64>,
    pub through: u64,
    pub feedback: Arc<Feedback>,
    pub stream: IncomingStream,
    pub comparison: proto::ComparisonId,
    round_number: u64,
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
    baseline_installed: bool,
}

impl Driver {
    pub fn new(
        snapshot: Snapshot,
        round: proto::StateRound,
        stream: IncomingStream,
        comparison: proto::ComparisonId,
    ) -> anyhow::Result<Self> {
        ensure!(round.number == 0, "initial round number is not zero");
        let spec = snapshot.spec.clone();
        let observed = snapshot.states.iter().map(|(id, state)| (*id, state.payload.state.head.clone())).collect();
        let (tx, refresh_rx) = tokio::sync::mpsc::channel(2);
        Ok(Self {
            projection: Projection::new(Round::new(round, snapshot)),
            spec,
            goals: Goals::default(),
            round_jobs: 0,
            jobs: FuturesUnordered::new(),
            assemblies: BTreeMap::new(),
            assembly_bytes: 0,
            observed,
            eligible: HashSet::new(),
            publications: VecDeque::new(),
            applied_through: None,
            through: 0,
            feedback: Arc::new(Feedback::default()),
            stream,
            comparison,
            round_number: 0,
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
            baseline_installed: false,
        })
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
            self.install(node, Some((id, version)), false).await?;
        }
        Ok(())
    }

    async fn install<SE, PA>(&mut self, node: &Node<SE, PA>, query: Option<(proto::QueryId, u32)>, complete: bool) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let Some((id, version)) = query else { return Ok(()) };
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
        let installed = if complete {
            query.activate_remote(version, entities, origin, &canceled).await?
        } else {
            query.install_remote_partial(version, entities, origin, &canceled).await?
        };
        ensure!(installed, "query stream superseded during installation");
        Ok(())
    }

    fn publication(&mut self, revision: u64) -> anyhow::Result<&mut Publication> {
        if !self.publications.back().is_some_and(|publication| publication.revision == revision) {
            ensure!(
                self.publications.back().is_none_or(|publication| publication.complete && publication.revision < revision),
                "interleaved source publications"
            );
            ensure!(revision > self.projection.revision, "stale source publication");
            ensure!(self.publications.len() < super::super::MAX_INFLIGHT_FRAMES, "too many pending source publications");
            self.publications.push_back(Publication {
                revision,
                changes: BTreeMap::new(),
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
        ensure!(self.jobs.len() < super::super::MAX_INFLIGHT_FRAMES * 128, "application queue exceeded limit");
        let bytes = bincode::serialized_size(&delta)? as usize;
        ensure!(
            self.application_bytes + self.assembly_bytes + bytes <= super::super::MAX_REPAIR_BYTES + super::super::WINDOW_BYTES,
            "application byte queue exceeded limit"
        );
        self.application_bytes += bytes;
        match owner {
            Owner::Round(_) => self.round_jobs += 1,
            Owner::Live(revision) => self.publication(revision)?.jobs += 1,
            Owner::Repair => {}
        }
        let repair = repair.map(|target| target_key(delta.entity_id, &target));
        if let Some(key) = &repair {
            self.applying_repairs.insert(key.clone());
        }
        let node = node.weak();
        let credentials = credentials.to_vec();
        self.jobs.push(Box::pin(async move {
            let node = node.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            let id = delta.entity_id;
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
            Ok(Applied { owner, id, head, change: outcome.change, bytes, repair })
        }));
        Ok(())
    }

    async fn observe<SE, PA>(&mut self, node: &Node<SE, PA>, applied: Applied) -> anyhow::Result<Option<EntityChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        self.application_bytes -= applied.bytes;
        if let Some(key) = &applied.repair {
            self.applying_repairs.remove(key);
        }
        match applied.owner {
            Owner::Round(number) if number == self.round_number && self.projection.round.is_some() => self.round_jobs -= 1,
            Owner::Live(revision) => {
                if let Some(publication) = self.publications.iter_mut().find(|publication| publication.revision == revision) {
                    publication.jobs -= 1;
                }
            }
            _ => {}
        }
        if let Some(head) = applied.head {
            for goal in self
                .goals
                .for_id_mut(&applied.id)
                .chain(self.publications.iter_mut().flat_map(|publication| publication.goals.for_id_mut(&applied.id)))
            {
                if goal.id == applied.id && !goal.covered && covers_target(node, &head, &goal.head).await? {
                    goal.covered = true;
                }
            }
            if let Some(member) = self.projection.members.get(&applied.id) {
                if (self.baseline_installed || self.projection.has_live_decision(&applied.id))
                    && covers_target(node, &head, &member.head).await?
                {
                    self.membership_dirty |= self.eligible.insert(applied.id);
                }
            }
            if self.projection.members.contains_key(&applied.id)
                || self.goals.rows.contains_key(&applied.id)
                || self.publications.iter().any(|publication| publication.goals.rows.contains_key(&applied.id))
            {
                self.observed.insert(applied.id, head);
            }
        }
        Ok(applied.change)
    }

    fn add_goal(&mut self, owner: Owner, id: proto::EntityId, target: proto::Clock) -> anyhow::Result<()> {
        let covered = self.observed.get(&id) == Some(&target);
        match owner {
            Owner::Round(_) => self.goals.add(id, target, covered),
            Owner::Live(revision) => self.publication(revision)?.goals.add(id, target, covered),
            Owner::Repair => Ok(()),
        }
    }

    fn chunk(
        &mut self,
        owner: Owner,
        target: proto::Clock,
        member: Option<proto::QueryMember>,
        chunk: proto::EntityDeltaChunk,
    ) -> anyhow::Result<Option<proto::EntityDelta>> {
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
        ensure!(self.assemblies.contains_key(&key) || self.assemblies.len() < super::super::MAX_INFLIGHT_FRAMES, "too many partial deltas");
        let assembly = self.assemblies.entry(key.clone()).or_insert_with(|| Assembly {
            owner,
            target: target.clone(),
            bytes: Vec::new(),
            member: member.clone(),
        });
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
        if !chunk.last {
            return Ok(None);
        }
        let assembly = self.assemblies.remove(&key).unwrap();
        self.assembly_bytes -= assembly.bytes.len();
        let content = decode_delta(&assembly.bytes)?;
        if !matches!(owner, Owner::Live(_)) {
            check_target(&assembly.target, &content)?;
        }
        Ok(Some(proto::EntityDelta { entity_id: chunk.entity_id, content }))
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
                StateItem::Summary(head) => {
                    ensure!(matches!(owner, Owner::Round(_)), "live repair contains comparison question");
                    let _: proto::TreeHead = proto::TreeHead::from_bytes(&head.to_bytes())?;
                    answers.push(StateAnswer::Summary(self.projection.round.as_mut().unwrap().snapshot.head(head.bounds).await?));
                }
                StateItem::NeedLeaves(bounds) => {
                    ensure!(matches!(owner, Owner::Round(_)), "live repair contains comparison question");
                    answers.push(StateAnswer::Leaves(self.projection.round.as_ref().unwrap().snapshot.page(bounds)?));
                }
                StateItem::Leaves(page) => {
                    ensure!(matches!(owner, Owner::Round(_)), "live repair contains membership");
                    let mut validation = BTreeMap::new();
                    replace_members(&mut validation, &self.spec, page.clone(), super::super::LEAF_PAGE)?;
                    for leaf in &page.leaves {
                        self.add_goal(owner, leaf.entity.entity_id, leaf.entity.head.clone())?;
                    }
                    self.projection.round.as_mut().unwrap().replace(page);
                }
                StateItem::Snapshots(page) => {
                    ensure!(matches!(owner, Owner::Round(_)), "live repair contains membership");
                    let leaves = snapshot_leaves(&self.spec, &page)?;
                    let mut validation = BTreeMap::new();
                    replace_members(&mut validation, &self.spec, leaves.clone(), super::super::MAX_SNAPSHOT_ROWS)?;
                    self.projection.round.as_mut().unwrap().replace(leaves);
                    for state in page.states {
                        let id = state.payload.entity_id;
                        let target = state.payload.state.head.clone();
                        self.add_goal(owner, id, target)?;
                        self.enqueue(
                            node,
                            peer,
                            credentials,
                            owner,
                            proto::EntityDelta { entity_id: id, content: proto::DeltaContent::StateSnapshot { state: state.into() } },
                            None,
                        )?;
                    }
                }
                StateItem::Repair { target, delta } => {
                    check_target(&target, &delta.content)?;
                    self.repair_arrived(delta.entity_id, &target);
                    self.add_goal(owner, delta.entity_id, target.clone())?;
                    self.enqueue(node, peer, credentials, owner, delta, Some(target))?;
                }
                StateItem::RepairChunk { target, chunk } => {
                    self.add_goal(owner, chunk.entity_id, target.clone())?;
                    let repair_target = target.clone();
                    if let Some(delta) = self.chunk(owner, target, None, chunk)? {
                        self.repair_arrived(delta.entity_id, &repair_target);
                        self.enqueue(node, peer, credentials, owner, delta, Some(repair_target))?;
                    }
                }
                StateItem::RepairUnavailable { entity_id, .. } => bail!("peer cannot repair required state for {entity_id}"),
            }
        }
        Ok(answers)
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
            let id = item.entity_id;
            if let Some(member) = &item.member {
                CanonicalCodec.split(self.spec.key_spec(), &member.key)?;
                self.add_goal(Owner::Live(revision), id, member.head.clone())?;
            }
            if let Some(content) = item.delta {
                let target = match &content {
                    proto::DeltaContent::StateSnapshot { state } | proto::DeltaContent::StateAndEvents { state, .. } => {
                        state.state.head.clone()
                    }
                    proto::DeltaContent::EventBridge { .. } => {
                        item.member.as_ref().ok_or_else(|| anyhow::anyhow!("absent event bridge has no target"))?.head.clone()
                    }
                    _ => bail!("unsupported live delta"),
                };
                self.add_goal(Owner::Live(revision), id, target)?;
                self.enqueue(node, peer, credentials, Owner::Live(revision), proto::EntityDelta { entity_id: id, content }, None)?;
            }
            let publication = self.publication(revision)?;
            if let Some(previous) = publication.changes.insert(id, item.member.clone()) {
                publication.change_bytes -= bincode::serialized_size(&(id, previous))? as usize;
            }
            publication.change_bytes += bincode::serialized_size(&(id, item.member))? as usize;
        }
        if complete {
            ensure!(
                !self.assemblies.values().any(|assembly| assembly.owner == Owner::Live(revision)),
                "publication completed inside a chunk"
            );
            let publication = self.publication(revision)?;
            publication.complete = true;
            let changes: Vec<_> = std::mem::take(&mut publication.changes).into_iter().collect();
            publication.change_bytes = 0;
            self.projection.publish(revision, changes.clone())?;
            for (id, member) in changes {
                if let Some(member) = member {
                    if self.observed.get(&id) == Some(&member.head) {
                        self.membership_dirty |= self.eligible.insert(id);
                    }
                } else {
                    self.membership_dirty |= self.eligible.remove(&id);
                }
            }
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
        ensure!(frame.comparison == self.comparison, "state stream identity changed");
        self.through = frame.sequence;
        let mut answers = Vec::new();
        let mut answer_round = None;
        match frame.body {
            StateFrameBody::BeginRound(descriptor) => {
                ensure!(!self.publications.back().is_some_and(|publication| !publication.complete), "round split a publication");
                let pending = self.pending_round.take().ok_or_else(|| anyhow::anyhow!("unsolicited round"))?;
                ensure!(pending.number == descriptor.number && descriptor.number == self.round_number + 1, "wrong round number");
                self.projection.begin(Round::new(descriptor, pending.snapshot))?;
                self.round_number = descriptor.number;
                self.round_waiter = Some(pending.request);
                self.goals = Goals::default();
                self.round_jobs = 0;
            }
            StateFrameBody::State { round, items, progress } => {
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
                if let Some(delta) = self.chunk(Owner::Live(revision), target, member.clone(), chunk)? {
                    self.live(
                        node,
                        peer,
                        credentials,
                        revision,
                        vec![proto::QueryChange { entity_id: delta.entity_id, member, delta: Some(delta.content) }],
                        complete,
                    )
                    .await?;
                } else {
                    ensure!(!complete, "publication completed inside a chunk");
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
            self.comparison,
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
        ensure!(changes + self.projection.retained_bytes() <= super::super::WINDOW_BYTES, "live membership window exhausted");
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
                    self.comparison,
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

    async fn complete_round<SE, PA>(&mut self, node: &Node<SE, PA>, query: Option<(proto::QueryId, u32)>) -> anyhow::Result<bool>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if self.round_jobs != 0
            || !self.goals.covered()
            || !self.projection.round.as_ref().is_some_and(|round| round.complete)
            || self.assemblies.values().any(|assembly| matches!(assembly.owner, Owner::Round(_)))
        {
            return Ok(false);
        }
        self.feedback.cancel_round(self.round_number);
        let baseline = self.projection.finish(self.round_number)?;
        for id in baseline.keys() {
            if self.projection.members.contains_key(id) {
                self.eligible.insert(*id);
            }
        }
        self.eligible.retain(|id| self.projection.members.contains_key(id));
        self.install(node, query, true).await?;
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
        let comparison = self.comparison;
        self.controls.push(Box::pin(async move {
            if let Some(node) = node.upgrade() {
                close_comparison(&node, peer, comparison, &credentials, proto::ComparisonOutcome::Cancelled { round: number }).await?;
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
        let comparison = self.comparison;
        let outcome = proto::ComparisonOutcome::Applied { round: self.round_number, processed_through: self.through };
        self.controls.push(Box::pin(async move {
            if let Some(node) = node.upgrade() {
                close_comparison(&node, peer, comparison, &credentials, outcome).await?;
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
        let entities = node.fetch_entities_from_local(&selection).await?;
        let mut snapshot = query.digest_tree().snapshot(&selection, &node.catalog, entities).await?;
        let known_state = snapshot.offer().await?;
        let number = self.round_number.checked_add(1).ok_or_else(|| anyhow::anyhow!("round number exhausted"))?;
        self.pending_round = Some(PendingRound { number, snapshot, request: reply });
        let node = node.weak();
        let credentials = credentials.to_vec();
        let comparison = self.comparison;
        self.controls.push(Box::pin(async move {
            let node = node.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            let response = state_deadline(node.request(
                peer,
                &credentials,
                proto::NodeRequestBody::RefreshState { comparison, round: number, known_state },
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
            if self.membership_dirty {
                self.install(&node, query, false).await?;
                self.membership_dirty = false;
            }
            if self.complete_round(&node, query).await? {
                if initial {
                    return Ok(());
                }
                self.acknowledge_round(&node, peer, credentials);
            }
            let mut advanced = false;
            while self
                .publications
                .front()
                .is_some_and(|publication| publication.complete && publication.jobs == 0 && publication.goals.covered())
            {
                self.applied_through = Some(self.publications.pop_front().unwrap().revision);
                advanced = true;
            }
            if advanced {
                self.prune_observed();
                self.feedback.queue(
                    &node,
                    peer,
                    credentials,
                    self.comparison,
                    self.through,
                    None,
                    self.applied_through,
                    vec![],
                    &self.stream.inbox,
                )?;
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
                let can_receive = self.application_bytes < super::super::WINDOW_BYTES;
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
                    if self.membership_dirty {
                        self.install(&node, query, false).await?;
                        self.membership_dirty = false;
                    }
                }
                Event::Job(job) => {
                    let mut changes = Vec::new();
                    if let Some(change) = self.observe(&node, job?).await? {
                        changes.push(change);
                    }
                    for _ in 1..128 {
                        let Some(Some(job)) = self.jobs.next().now_or_never() else { break };
                        if let Some(change) = self.observe(&node, job?).await? {
                            changes.push(change);
                        }
                    }
                    // Only live admission changes need a partial installation. Initial
                    // rows join as one finite baseline when the round completes.
                    if self.membership_dirty {
                        self.install(&node, query, false).await?;
                        self.membership_dirty = false;
                    }
                    if !changes.is_empty() {
                        node.reactor.notify_change(changes).await;
                    }
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
        Ok(())
    }
    pub fn ids(&self) -> Vec<proto::EntityId> { self.projection.members.keys().copied().collect() }
    pub fn refresh_handle(&self) -> RefreshHandle { self.refresh.clone() }
}
