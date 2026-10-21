//! One node-local LRU for every caller and responder using a digest tree.
use super::{
    source::{self, QuerySource, Route},
    QueryTreeCache, Snapshot,
};
use crate::{
    node::{source_publications::SourceError, Node, WeakNode},
    policy::{ContextPolicy, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
};
use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_proto as proto;
use ankurah_signals::Wait;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};
use tokio::sync::{mpsc, oneshot, Notify};

const MAX_ENTRIES: usize = 32;
const MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, PartialEq)]
struct Identity<CD> {
    selection: Selection<Resolved>,
    filtered: Selection<Resolved>,
    retrieval: Arc<Predicate<Resolved>>,
    spec: proto::TreeSpec,
    credentials: Vec<CD>,
}

impl<CD: ContextData> Identity<CD> {
    fn check<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        anyhow::ensure!(
            policy.filter_predicate(self.selection.predicate.clone())? == self.filtered.predicate
                && policy.retrieval_predicate() == self.retrieval,
            SourceError::Changed
        );
        Ok(())
    }
}

/// A single cache holds active and idle residents. Active leases pin their
/// entries under the existing operation/stream bounds; the LRU count and byte
/// allowances bound additional idle retention. Frozen rounds remain independent.
pub(crate) struct ResidentTrees<CD: ContextData> {
    entries: Arc<Mutex<Entries<CD>>>,
    // Only misses serialize. Hits never wait for an unrelated storage scan.
    builds: tokio::sync::Mutex<()>,
}
struct Entries<CD: ContextData> {
    entries: VecDeque<Entry<CD>>,
    next_id: u64,
    max_entries: usize,
    max_bytes: usize,
}
struct Entry<CD: ContextData> {
    id: u64,
    identity: Identity<CD>,
    resident: Arc<Resident<CD>>,
    bytes: usize,
}
struct Resident<CD: ContextData> {
    commands: mpsc::Sender<Command<CD>>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    cleanup: Arc<Notify>,
    alive: AtomicBool,
    pins: AtomicUsize,
    #[cfg(test)]
    tree: Arc<QueryTreeCache>,
    cache: Weak<Mutex<Entries<CD>>>,
}
impl<CD: ContextData> Drop for Resident<CD> {
    fn drop(&mut self) { self.stop.lock().unwrap().take(); }
}

pub(crate) struct ResidentLease<CD: ContextData>(Arc<Resident<CD>>);
impl<CD: ContextData> ResidentLease<CD> {
    fn new(resident: Arc<Resident<CD>>) -> Self {
        resident.pins.fetch_add(1, Ordering::AcqRel);
        Self(resident)
    }
    pub(crate) async fn snapshot(&self) -> anyhow::Result<Capture> {
        let (response, result) = oneshot::channel();
        self.0.commands.send(Command::Snapshot(response)).await.map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))?
    }
    pub(crate) async fn subscribe<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        request: proto::RequestId,
        fingerprint: Vec<u8>,
        query: proto::QueryId,
        version: u32,
        catalog: bool,
        known: proto::KnownState,
    ) -> anyhow::Result<(proto::StateReply, ResidentSubscription<CD>)>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let owner = Arc::new(());
        let (response, result) = oneshot::channel();
        self.0
            .commands
            .send(Command::Subscribe {
                peer,
                request,
                fingerprint,
                query,
                version,
                catalog,
                known,
                owner: Arc::downgrade(&owner),
                response,
            })
            .await
            .map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        let pending = result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))??;
        let reply = pending.initialize(node).await?;
        let subscription = ResidentSubscription { lease: self.clone(), owner: Some(owner), peer, stream: reply.frame.stream };
        Ok((reply, subscription))
    }
    #[cfg(test)]
    pub(crate) fn tree(&self) -> Arc<QueryTreeCache> { self.0.tree.clone() }
}
impl<CD: ContextData> Clone for ResidentLease<CD> {
    fn clone(&self) -> Self { Self::new(self.0.clone()) }
}
impl<CD: ContextData> Drop for ResidentLease<CD> {
    fn drop(&mut self) {
        if self.0.pins.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Some(cache) = self.0.cache.upgrade() {
                cache.lock().unwrap().trim_idle();
            }
        }
    }
}

/// Transport lifetime is separate from resident tree identity. Dropping this
/// route stops its live delivery, while the idle tree stays maintained in the LRU.
pub(crate) struct ResidentSubscription<CD: ContextData> {
    lease: ResidentLease<CD>,
    owner: Option<Arc<()>>,
    peer: proto::EntityId,
    stream: proto::StateStreamId,
}
impl<CD: ContextData> ResidentSubscription<CD> {
    pub(crate) async fn refresh(&self, number: u64, known: proto::KnownState) -> anyhow::Result<()> {
        let (response, result) = oneshot::channel();
        self.lease
            .0
            .commands
            .send(Command::Refresh { peer: self.peer, stream: self.stream, number, known, response })
            .await
            .map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))?
    }
    #[cfg(test)]
    pub(crate) fn tree(&self) -> Arc<QueryTreeCache> { self.lease.tree() }
}
impl<CD: ContextData> Drop for ResidentSubscription<CD> {
    fn drop(&mut self) {
        // Wake cleanup after releasing the route's liveness token.
        self.owner.take();
        self.lease.0.cleanup.notify_one();
    }
}

pub(crate) struct Capture {
    pub(crate) revision: u64,
    pub(crate) snapshot: Snapshot,
}
enum Command<CD: ContextData> {
    Snapshot(oneshot::Sender<anyhow::Result<Capture>>),
    Subscribe {
        peer: proto::EntityId,
        request: proto::RequestId,
        fingerprint: Vec<u8>,
        query: proto::QueryId,
        version: u32,
        catalog: bool,
        known: proto::KnownState,
        owner: Weak<()>,
        response: oneshot::Sender<anyhow::Result<super::PendingStateStream<CD>>>,
    },
    Refresh {
        peer: proto::EntityId,
        stream: proto::StateStreamId,
        number: u64,
        known: proto::KnownState,
        response: oneshot::Sender<anyhow::Result<()>>,
    },
}

impl<CD: ContextData> Command<CD> {
    fn reject(self, error: &anyhow::Error) {
        let error =
            error.downcast_ref::<SourceError>().map_or_else(|| anyhow::anyhow!(error.to_string()), |error| anyhow::anyhow!(error.clone()));
        match self {
            Self::Snapshot(response) => {
                let _ = response.send(Err(error));
            }
            Self::Subscribe { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Refresh { response, .. } => {
                let _ = response.send(Err(error));
            }
        }
    }
}

impl<CD: ContextData> Default for ResidentTrees<CD> {
    fn default() -> Self { Self::with_limits(MAX_ENTRIES, MAX_BYTES) }
}
impl<CD: ContextData> ResidentTrees<CD> {
    pub(super) fn with_limits(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(Entries { entries: VecDeque::new(), next_id: 0, max_entries, max_bytes })),
            builds: tokio::sync::Mutex::new(()),
        }
    }
    #[cfg(test)]
    pub(crate) fn evict_idle(&self) {
        self.entries.lock().unwrap().entries.retain(|entry| entry.resident.pins.load(Ordering::Acquire) != 0);
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize { self.entries.lock().unwrap().entries.len() }

    pub(crate) fn discard_failed(&self, lease: &ResidentLease<CD>) {
        self.entries.lock().unwrap().entries.retain(|entry| !Arc::ptr_eq(&entry.resident, &lease.0));
    }
    pub(crate) fn can_retry(error: &anyhow::Error) -> bool {
        matches!(error.downcast_ref::<SourceError>(), Some(SourceError::Changed | SourceError::Lagged))
    }
    pub(crate) async fn capture<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: proto::TreeSpec,
        credentials: Vec<CD>,
    ) -> anyhow::Result<Capture>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        for attempt in 0..2 {
            let resident = self.acquire(node, selection.clone(), spec.clone(), credentials.clone()).await?;
            match resident.snapshot().await {
                Err(error) if attempt == 0 && Self::can_retry(&error) => self.discard_failed(&resident),
                result => return result,
            }
        }
        unreachable!()
    }

    pub(crate) async fn acquire<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: proto::TreeSpec,
        credentials: Vec<CD>,
    ) -> anyhow::Result<ResidentLease<CD>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        node.system.require_system_ready()?;
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        let mut filtered = selection.clone();
        filtered.predicate = policy.filter_predicate(filtered.predicate)?;
        let identity = Identity { selection, filtered, retrieval: policy.retrieval_predicate(), spec, credentials };
        if let Some(lease) = self.entries.lock().unwrap().get(&identity) {
            return Ok(lease);
        }
        let _build = self.builds.lock().await;
        if let Some(lease) = self.entries.lock().unwrap().get(&identity) {
            return Ok(lease);
        }
        let tree = Arc::new(QueryTreeCache::default());
        let captured = source::capture_filtered_query(
            node,
            identity.filtered.clone(),
            identity.spec.clone(),
            identity.credentials.clone(),
            tree.clone(),
        )
        .await?;
        node.system.check_not_halted()?;
        let actor = QuerySource::from_capture(identity.credentials.clone(), captured);
        let bytes = actor.accounted_bytes();
        let (commands, receiver) = mpsc::channel(8);
        let (stop, canceled) = oneshot::channel();
        let cleanup = Arc::new(Notify::new());
        let resident = Arc::new(Resident {
            commands,
            stop: Mutex::new(Some(stop)),
            cleanup: cleanup.clone(),
            alive: AtomicBool::new(true),
            pins: AtomicUsize::new(0),
            #[cfg(test)]
            tree,
            cache: Arc::downgrade(&self.entries),
        });
        let lease = ResidentLease::new(resident.clone());
        let scope = identity.clone();
        let id = {
            let mut entries = self.entries.lock().unwrap();
            let id = entries.next_id;
            entries.next_id = id.checked_add(1).ok_or_else(|| anyhow::anyhow!("resident tree identity exhausted"))?;
            entries.entries.push_back(Entry { id, identity, resident: resident.clone(), bytes });
            id
        };
        let weak = node.weak();
        let run = node.run.clone();
        let entries = Arc::downgrade(&self.entries);
        let resident = Arc::downgrade(&resident);
        crate::task::spawn(async move {
            let mut actor = actor;
            let result = tokio::select! {
                _ = canceled => Ok(()),
                _ = run.wait_value(false) => Ok(()),
                result = run_resident(&mut actor, weak.clone(), receiver, cleanup, &entries, id, &scope) => result,
            };
            if let Some(resident) = resident.upgrade() {
                resident.alive.store(false, Ordering::Release);
            }
            if let Some(entries) = entries.upgrade() {
                entries.lock().unwrap().entries.retain(|entry| entry.id != id);
            }
            // Removal is immediate; transport cleanup may wait for its handler.
            if let Err(error) = result {
                tracing::debug!(%error, "resident tree stopped");
                let unavailable = matches!(error.downcast_ref::<SourceError>(), Some(SourceError::Unavailable));
                for route in actor.routes {
                    let weak = weak.clone();
                    // A peer waiting on history or its own handler must not
                    // delay invalidation of the tree's other subscribers.
                    crate::task::spawn(async move {
                        if let Some(node) = weak.upgrade() {
                            if unavailable {
                                let _ = super::source_unavailable(&node, route.peer, route.stream).await;
                            } else {
                                let _ = super::expire_state_stream(&node, route.peer, route.stream).await;
                            }
                        }
                    });
                }
            }
        });
        Ok(lease)
    }
}

impl<CD: ContextData> Entries<CD> {
    fn get(&mut self, identity: &Identity<CD>) -> Option<ResidentLease<CD>> {
        self.entries.retain(|entry| entry.resident.alive.load(Ordering::Acquire));
        let position = self.entries.iter().position(|entry| &entry.identity == identity)?;
        let entry = self.entries.remove(position)?;
        let lease = ResidentLease::new(entry.resident.clone());
        self.entries.push_back(entry);
        Some(lease)
    }
    fn trim_idle(&mut self) {
        // A result too large for idle retention must not evict useful small trees.
        self.entries.retain(|entry| entry.resident.pins.load(Ordering::Acquire) != 0 || entry.bytes <= self.max_bytes);
        loop {
            let idle = self.entries.iter().filter(|entry| entry.resident.pins.load(Ordering::Acquire) == 0);
            let (count, bytes) = idle.fold((0, 0usize), |(count, bytes), entry| (count + 1, bytes.saturating_add(entry.bytes)));
            if count <= self.max_entries && bytes <= self.max_bytes {
                break;
            }
            let Some(victim) = self.entries.iter().position(|entry| entry.resident.pins.load(Ordering::Acquire) == 0) else {
                break;
            };
            self.entries.remove(victim);
        }
    }
    fn update_bytes(&mut self, id: u64, bytes: usize) -> anyhow::Result<()> {
        let entry = self.entries.iter_mut().find(|entry| entry.id == id).ok_or_else(|| anyhow::anyhow!("resident tree evicted"))?;
        entry.bytes = bytes;
        self.trim_idle();
        Ok(())
    }
}

async fn run_resident<SE, PA>(
    actor: &mut QuerySource<PA::ContextData>,
    weak: WeakNode<SE, PA>,
    mut commands: mpsc::Receiver<Command<PA::ContextData>>,
    cleanup: Arc<Notify>,
    entries: &Weak<Mutex<Entries<PA::ContextData>>>,
    id: u64,
    scope: &Identity<PA::ContextData>,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    loop {
        actor.routes.retain(|route| route.owner.strong_count() != 0);
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return Ok(()); };
                let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                let captured = async {
                    node.system.check_not_halted()?;
                    scope.check(&node)?;
                    let snapshot = actor.refresh(&node).await?;
                    actor.check_authorization(&node)?;
                    let entries = entries.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    entries.lock().unwrap().update_bytes(id, actor.accounted_bytes())?;
                    Ok::<_, anyhow::Error>(snapshot)
                }.await;
                let snapshot = match captured {
                    Ok(snapshot) => snapshot,
                    Err(error) => { command.reject(&error); return Err(error); }
                };
                match command {
                    Command::Snapshot(response) => { let _ = response.send(Ok(Capture { revision: actor.revision, snapshot })); }
                    Command::Subscribe { peer, request, fingerprint, query, version, catalog, known, owner, response } => {
                        if response.is_closed() || owner.strong_count() == 0 { continue; }
                        let result = super::prepare_state_stream(&node, peer, request, fingerprint, actor.credentials().to_vec(), Some((query, version)), catalog, actor.revision, snapshot, known);
                        if let Ok(pending) = &result {
                            actor.routes.push(Route { peer, stream: pending.id(), owner });
                        }
                        let _ = response.send(result);
                    }
                    Command::Refresh { peer, stream, number, known, response } => {
                        let result = if actor.routes.iter().any(|route| route.peer == peer && route.stream == stream && route.owner.strong_count() != 0) {
                            super::queue_round(&node, peer, stream, number, actor.revision, snapshot, known)
                        } else { Err(anyhow::anyhow!("query stream unavailable")) };
                        let _ = response.send(result);
                    }
                }
            }
            batch = actor.cursor.next() => {
                let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                node.system.check_not_halted()?;
                scope.check(&node)?;
                actor.publish(&node, batch?).await?;
            }
            _ = cleanup.notified() => { actor.routes.retain(|route| route.owner.strong_count() != 0); }
        }
        let entries = entries.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
        entries.lock().unwrap().update_bytes(id, actor.accounted_bytes())?;
    }
}
