//! The task that owns a resident: it applies the resident's maintenance and
//! answers its leases' snapshot requests and its snapshots' digest requests.

use super::{
    bootstrap::{ensure_catalog_unchanged, fold_candidate, StorageRead, TreeUpdate, MAX_READ_ATTEMPTS},
    identity::{ensure_filter_unchanged, Access, Coverage, Identity},
    lease::{Resident, ResidentSnapshot, SnapshotRequest},
    Lru,
};
use crate::{
    node::{
        state_sync::{
            tree::{DigestRequest, DigestRequests, ID_ROW_BYTES},
            QueryTree, Snapshot,
        },
        Node, WeakNode,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::{MaintenanceSubscription, SourceBatch, SourceError},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_proto as proto;
use ankurah_signals::Wait;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::Ordering, Mutex, Weak},
};
use tokio::sync::{mpsc, oneshot, watch};

/// A resident's state, owned by its actor task: what it admits, every
/// candidate its predicate matches, its tree and its maintenance subscription.
pub(super) struct ResidentActor<CD: ContextData> {
    pub(super) identity: Identity<CD>,
    // IDs only: denied candidates must also be rechecked when opaque policy changes.
    pub(super) candidates: BTreeSet<proto::EntityId>,
    pub(super) tree: QueryTree,
    pub(super) cursor: MaintenanceSubscription<TreeUpdate>,
    pub(super) revision: u64,
    /// How the snapshots this actor hands out ask it for the tree's digests,
    /// once the actor runs.
    pub(super) digest_requests: Option<DigestRequests>,
    /// The tree's version, which a snapshot's holder watches for the tree's
    /// next change.
    pub(super) changes: watch::Sender<u64>,
}

impl<CD: ContextData> ResidentActor<CD> {
    /// Fail with `PolicyChanged` unless the tree holds exactly the candidates
    /// the resident's access admits now: every candidate for an unrestricted
    /// resident, and for a restricted one those its admitting credentials may
    /// read.
    pub(super) async fn check_authorization<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.identity.credentials);
        let entries = self.tree.entries();
        if self.identity.access == Access::Unrestricted || policy.allows_unrestricted_state_reads() {
            // A new grant must also admit candidates previously denied by policy.
            // Returning the old subset would silently lose newly readable rows.
            anyhow::ensure!(entries.keys().eq(self.candidates.iter()), SourceError::PolicyChanged);
            return Ok(());
        }
        let states = current_states(node, self.candidates.iter().copied()).await?;
        for id in &self.candidates {
            let state = states.get(id).ok_or(SourceError::Changed)?;
            anyhow::ensure!(policy.check_read(id, &state.state).is_ok() == entries.contains_key(id), SourceError::PolicyChanged);
        }
        Ok(())
    }

    /// The bytes this resident is charged against the LRU's idle allowance.
    pub(super) fn accounted_bytes(&self) -> usize { self.tree.accounted_bytes().saturating_add(self.candidates.len() * ID_ROW_BYTES) }

    /// Fold `changes` into the candidates and the tree, each entity taking the
    /// entry its last change leaves it.
    pub(super) async fn apply(&mut self, changes: Vec<TreeUpdate>) -> anyhow::Result<()> {
        let mut latest = BTreeMap::new();
        for change in changes {
            fold_candidate(&mut self.candidates, &change);
            latest.insert(change.entity_id, change.entry);
        }
        self.tree.apply(latest).await
    }

    /// Bring the resident up to `revision` with `batch`, the committed changes
    /// it has not applied, when there are any. A complete resident applies them
    /// as they stand; a limited one reads its cut again when they touched it.
    pub(super) async fn advance<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        batch: Option<SourceBatch<TreeUpdate>>,
        revision: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        if let Some(batch) = batch {
            ensure_catalog_unchanged(&self.identity, &batch)?;
            match self.identity.coverage {
                Coverage::Complete => self.apply(batch.changes).await?,
                Coverage::Limited(_) if self.identity.filtered.limit != Some(0) && self.relevant(&batch) => return self.refill(node).await,
                Coverage::Limited(_) => {}
            }
        }
        self.revision = revision;
        Ok(())
    }

    fn relevant(&self, batch: &SourceBatch<TreeUpdate>) -> bool {
        batch.changes.iter().any(|change| change.matches || self.candidates.contains(&change.entity_id))
    }

    /// Read a limited resident's cut again after a commit touched it, fence the
    /// read, and replace the tree's entries.
    async fn refill<SE, PA>(&mut self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.identity.credentials);
        let mut attempts = 0;
        let (read, revision) = loop {
            attempts += 1;
            anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
            let window = self.cursor.fence().await?;
            if let Some(batch) = &window.batch {
                ensure_catalog_unchanged(&self.identity, batch)?;
            }
            let read = StorageRead::of(node, &self.identity, &policy).await?;
            match self.cursor.fence_unchanged_since(window.revision).await {
                Err(SourceError::Changed) => continue,
                result => {
                    let revision = result?.revision;
                    // Keep bounded residents' explicit entity watches bounded
                    // too; predicate routing still observes future entrants.
                    match self
                        .cursor
                        .finish_bootstrap(self.identity.filtered.predicate.clone(), read.candidates.iter().copied(), revision)
                        .await
                    {
                        Err(SourceError::Changed) => continue,
                        result => {
                            result?;
                            break (read, revision.0);
                        }
                    }
                }
            }
        };
        self.candidates = read.candidates;
        self.tree.replace(read.rows).await?;
        self.revision = revision;
        Ok(())
    }

    /// Apply every commit published so far, and snapshot the tree.
    pub(super) async fn refresh<SE, PA>(&mut self, node: &Node<SE, PA>) -> anyhow::Result<Snapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let window = self.cursor.fence().await?;
        self.advance(node, window.batch, window.revision.0).await?;
        self.signal_changes();
        Ok(self.snapshot())
    }

    /// Signal the holders of this resident's snapshots when the tree has
    /// changed since they last heard.
    fn signal_changes(&self) {
        let version = self.tree.version();
        self.changes.send_if_modified(|seen| std::mem::replace(seen, version) != version);
    }

    /// A snapshot of the tree whose first digest read asks this actor for the
    /// tree's digests.
    fn snapshot(&self) -> Snapshot {
        let snapshot = self.tree.snapshot();
        match &self.digest_requests {
            Some(requests) => snapshot.asking(requests),
            None => snapshot,
        }
    }

    /// Answer snapshot and digest requests and apply published batches until
    /// the resident's last handle drops, charging the LRU for the resident
    /// after each.
    async fn run<SE, PA>(
        &mut self,
        weak: WeakNode<SE, PA>,
        mut requests: mpsc::Receiver<SnapshotRequest<CD>>,
        mut digest_requests: mpsc::Receiver<DigestRequest>,
        lru: &Weak<Mutex<Lru<CD>>>,
        id: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        loop {
            tokio::select! {
                request = requests.recv() => {
                    let Some(request) = request else { return Ok(()); };
                    let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    self.answer(&node, request, lru, id).await?;
                }
                request = digest_requests.recv() => {
                    if let Some(request) = request {
                        self.tree.answer(request).await?;
                    }
                }
                batch = self.cursor.next() => {
                    let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    node.system.check_not_halted()?;
                    self.identity.ensure_policy_unchanged(&node)?;
                    let batch = batch?;
                    let revision = batch.revision.0;
                    self.advance(&node, Some(batch), revision).await?;
                    self.signal_changes();
                }
            }
            let lru = lru.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            lru.lock().unwrap().update_bytes(id, self.accounted_bytes())?;
        }
    }

    /// Answer one lease's snapshot request from the resident brought up to
    /// date. A failure to bring the resident up to date stops it; a failure of
    /// the request's own view fails only that request.
    async fn answer<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        request: SnapshotRequest<CD>,
        lru: &Weak<Mutex<Lru<CD>>>,
        id: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let refreshed = async {
            node.system.check_not_halted()?;
            self.identity.ensure_policy_unchanged(node)?;
            let snapshot = self.refresh(node).await?;
            self.check_authorization(node).await?;
            let lru = lru.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            lru.lock().unwrap().update_bytes(id, self.accounted_bytes())?;
            Ok::<_, anyhow::Error>(snapshot)
        }
        .await;
        let snapshot = match refreshed {
            Ok(snapshot) => snapshot,
            Err(error) => {
                // A retry must not reacquire the failed source before
                // the actor's outer cleanup has removed its entry.
                if let Some(lru) = lru.upgrade() {
                    lru.lock().unwrap().slots.retain(|slot| slot.id != id);
                }
                request.reject(&error);
                return Err(error);
            }
        };
        let selected = (|| {
            let view = &request.view;
            let policy = ContextPolicy::from_credentials(&node.policy_agent, &view.credentials);
            ensure_filter_unchanged(&policy, &view.selection, &view.filtered)?;
            self.identity.ensure_holder_admitted(&policy)?;
            self.identity.coverage.select(snapshot, &view.selection, &node.catalog)?.with_spec(
                view.spec.clone(),
                &view.selection,
                &node.catalog,
            )
        })();
        match selected {
            Ok(snapshot) => {
                let _ = request.response.send(Ok(ResidentSnapshot { snapshot, changes: self.changes.subscribe() }));
            }
            // An invalid view does not invalidate the source used by other
            // query owners.
            Err(error) => request.reject(&error),
        }
        Ok(())
    }

    /// Run this actor until its resident is dropped or the node stops, then
    /// remove its slot.
    pub(super) fn spawn<SE, PA>(
        mut self,
        node: &Node<SE, PA>,
        receiver: mpsc::Receiver<SnapshotRequest<CD>>,
        canceled: oneshot::Receiver<()>,
        lru: Weak<Mutex<Lru<CD>>>,
        resident: Weak<Resident<CD>>,
        id: u64,
    ) where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let weak = node.weak();
        let run = node.run.clone();
        let (requests, digest_requests) = mpsc::channel(8);
        self.digest_requests = Some(DigestRequests::new(&requests));
        let actor = self;
        crate::task::spawn(async move {
            // Holding the sender keeps the snapshots' requests answerable while this task runs.
            let _requests = requests;
            let mut actor = actor;
            let result = tokio::select! {
                _ = canceled => Ok(()),
                _ = run.wait_value(false) => Ok(()),
                result = actor.run(weak, receiver, digest_requests, &lru, id) => result,
            };
            if let Some(resident) = resident.upgrade() {
                resident.alive.store(false, Ordering::Release);
            }
            if let Some(lru) = lru.upgrade() {
                lru.lock().unwrap().slots.retain(|slot| slot.id != id);
            }
            if let Err(error) = result {
                tracing::debug!(%error, "resident tree stopped");
            }
        });
    }
}

/// The node's current state of each of `ids` that exists: a resident
/// entity's from memory, and the rest from one storage read.
async fn current_states<SE, PA>(
    node: &Node<SE, PA>,
    ids: impl IntoIterator<Item = proto::EntityId>,
) -> anyhow::Result<BTreeMap<proto::EntityId, proto::EntityState>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut states = BTreeMap::new();
    let mut stored = Vec::new();
    for id in ids {
        match node.entities.get(&id) {
            Some(entity) => {
                states.insert(id, entity.to_entity_state()?);
            }
            None => stored.push(id),
        }
    }
    if !stored.is_empty() {
        for result in node.storage.get_states(stored, &ankql::ast::Predicate::True).await? {
            if let crate::storage::GetStateResult::Found(state) = result {
                states.insert(state.payload.entity_id, state.payload);
            }
        }
    }
    Ok(states)
}
