//! One node-local LRU for every caller and responder using a digest tree.
mod actor;
mod bootstrap;
mod identity;
mod lease;

use self::{
    actor::ResidentActor,
    identity::{compatible_access, compatible_selection, Coverage, Identity, QueryView},
    lease::{Resident, ResidentLease},
};
use super::tree::{serves, spec_for};
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::SourceError,
    session::ContextData,
    storage::StorageEngine,
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto as proto;
pub(crate) use lease::{ResidentPin, ResidentSnapshot, TreeChanges};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{mpsc, oneshot, watch};

const MAX_ENTRIES: usize = 32;
const MAX_BYTES: usize = 128 * 1024 * 1024;
/// Snapshots a comparison takes before a retryable admission failure stands.
const MAX_SNAPSHOT_ATTEMPTS: u32 = 8;
/// The wait before a comparison's second snapshot attempt, doubled before each later one.
const FIRST_SNAPSHOT_RETRY: std::time::Duration = std::time::Duration::from_millis(10);

/// The node's one LRU of resident trees, shared by every requester and
/// responder. A lease pins its resident; idle residents stay maintained until
/// the LRU's count or byte allowance evicts them.
pub(crate) struct ResidentTrees<CD: ContextData> {
    lru: Arc<Mutex<Lru<CD>>>,
}
struct Lru<CD: ContextData> {
    slots: VecDeque<Slot<CD>>,
    /// The misses being admitted now, which a compatible miss waits for
    /// rather than reading storage again.
    building: Vec<Building<CD>>,
    next_id: u64,
    max_entries: usize,
    max_bytes: usize,
}
/// A miss being admitted: its identity, and a signal that ends when the build
/// completes, fails or is dropped.
struct Building<CD: ContextData> {
    id: u64,
    identity: Identity<CD>,
    ended: watch::Receiver<()>,
}
/// Withdraws a build from the LRU's in-progress list, and wakes the misses
/// waiting on it, when the build ends however it ends.
struct BuildRegistration<CD: ContextData> {
    lru: Arc<Mutex<Lru<CD>>>,
    id: u64,
    _ended: watch::Sender<()>,
}
impl<CD: ContextData> Drop for BuildRegistration<CD> {
    fn drop(&mut self) { self.lru.lock().unwrap().building.retain(|build| build.id != self.id); }
}
struct Slot<CD: ContextData> {
    id: u64,
    identity: Identity<CD>,
    resident: Arc<Resident<CD>>,
    bytes: usize,
}

impl<CD: ContextData> Default for ResidentTrees<CD> {
    fn default() -> Self { Self::with_limits(MAX_ENTRIES, MAX_BYTES) }
}

impl<CD: ContextData> ResidentTrees<CD> {
    pub(super) fn with_limits(max_entries: usize, max_bytes: usize) -> Self {
        Self { lru: Arc::new(Mutex::new(Lru { slots: VecDeque::new(), building: Vec::new(), next_id: 0, max_entries, max_bytes })) }
    }
    /// A snapshot of the resident that serves `selection` for `credentials`,
    /// in a tree under `spec` when the peer chose one. It admits the resident
    /// again, once, after a catalog change crossed the admission's storage read
    /// or the snapshot failed with a retryable source error.
    pub(crate) async fn snapshot<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: Option<proto::TreeSpec>,
        credentials: Vec<CD>,
    ) -> anyhow::Result<ResidentSnapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        for attempt in 0..2 {
            let lease = match self.acquire(node, selection.clone(), spec.clone(), credentials.clone()).await {
                Err(error) if attempt == 0 && matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)) => continue,
                result => result?,
            };
            match lease.snapshot().await {
                Err(error) if attempt == 0 && SourceError::is_retryable(&error) => continue,
                result => return result,
            }
        }
        unreachable!("the second attempt returns")
    }

    /// A snapshot as [`Self::snapshot`] takes one, for a comparison that must
    /// not fail while the node's data, catalog or policy changes: such a change
    /// fails an admission retryably, and changes come in bursts, as while a
    /// schema syncs. It takes another after a retryable failure, waiting longer
    /// each time, up to `MAX_SNAPSHOT_ATTEMPTS` snapshots.
    pub(crate) async fn snapshot_through_changes<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: Option<proto::TreeSpec>,
        credentials: Vec<CD>,
    ) -> anyhow::Result<ResidentSnapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut wait = FIRST_SNAPSHOT_RETRY;
        for _ in 1..MAX_SNAPSHOT_ATTEMPTS {
            match self.snapshot(node, selection.clone(), spec.clone(), credentials.clone()).await {
                Err(error) if SourceError::is_retryable(&error) => {
                    futures_timer::Delay::new(wait).await;
                    wait *= 2;
                }
                result => return result,
            }
        }
        self.snapshot(node, selection, spec, credentials).await
    }

    /// A lease of the resident that serves `selection` for `credentials`:
    /// one already admitted or being admitted when it is compatible, otherwise
    /// a new one admitted from storage.
    async fn acquire<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: Option<proto::TreeSpec>,
        credentials: Vec<CD>,
    ) -> anyhow::Result<ResidentLease<CD>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        node.system.require_system_ready()?;
        let peer_spec = spec.is_some();
        let spec = spec.unwrap_or_else(|| spec_for(&selection, &node.catalog));
        anyhow::ensure!(serves(&spec, &selection, &node.catalog), "tree cannot serve selection");
        let requested = Identity::requested(node, selection, spec, credentials)?;
        let proof = |credentials: &[CD]| {
            ContextPolicy::from_credentials(&node.policy_agent, &credentials.to_vec()).allows_unrestricted_state_reads()
        };
        let identity = requested.admission();
        // Wait out any compatible build in progress, then look again; build
        // only when nothing admitted or being admitted serves the request.
        let _registration = loop {
            let mut ended = {
                let mut lru = self.lru.lock().unwrap();
                if let Some(lease) = lru.get(&requested, &node.catalog, peer_spec, proof) {
                    return Ok(lease);
                }
                match lru.building.iter().find(|build| compatible(&build.identity, &requested, &node.catalog, peer_spec, &proof)) {
                    Some(build) => build.ended.clone(),
                    None => {
                        let id = lru.next_id;
                        lru.next_id = id.checked_add(1).ok_or_else(|| anyhow::anyhow!("resident tree identity exhausted"))?;
                        let (sender, ended) = watch::channel(());
                        lru.building.push(Building { id, identity: identity.clone(), ended });
                        break BuildRegistration { lru: self.lru.clone(), id, _ended: sender };
                    }
                }
            };
            // The sender only ever drops: `changed` returns once the build ends.
            let _ = ended.changed().await;
        };
        let actor = ResidentActor::bootstrap(node, identity.clone()).await?;
        node.system.check_not_halted()?;
        let bytes = actor.accounted_bytes();
        let (requests, receiver) = mpsc::channel(8);
        let (stop, canceled) = oneshot::channel();
        let resident = Arc::new(Resident {
            requests,
            _stop: stop,
            alive: AtomicBool::new(true),
            pins: AtomicUsize::new(0),
            lru: Arc::downgrade(&self.lru),
        });
        let lease = ResidentLease::new(resident.clone(), Arc::new(requested.view()));
        let id = {
            let mut lru = self.lru.lock().unwrap();
            let id = lru.next_id;
            lru.next_id = id.checked_add(1).ok_or_else(|| anyhow::anyhow!("resident tree identity exhausted"))?;
            lru.slots.push_back(Slot { id, identity, resident: resident.clone(), bytes });
            id
        };
        actor.spawn(node, receiver, canceled, Arc::downgrade(&self.lru), Arc::downgrade(&resident), id);
        Ok(lease)
    }
}

impl<CD: ContextData> Lru<CD> {
    /// A lease of the resident that best serves `requested`: a complete one
    /// first, then one keyed as requested, then any compatible one. `proof`
    /// tells whether credentials hold a fresh proof of unrestricted state reads.
    fn get(
        &mut self,
        requested: &Identity<CD>,
        catalog: &crate::schema::catalog::CatalogManager,
        peer_spec: bool,
        proof: impl Fn(&[CD]) -> bool,
    ) -> Option<ResidentLease<CD>> {
        self.slots.retain(|slot| slot.resident.alive.load(Ordering::Acquire));
        let serves = |slot: &Slot<CD>| compatible(&slot.identity, requested, catalog, peer_spec, &proof);
        let position = self
            .slots
            .iter()
            .rposition(|slot| matches!(slot.identity.coverage, Coverage::Complete) && serves(slot))
            .or_else(|| self.slots.iter().rposition(|slot| slot.identity.spec == requested.spec && serves(slot)))
            .or_else(|| self.slots.iter().rposition(serves))?;
        let slot = self.slots.remove(position)?;
        let view = Arc::new(QueryView {
            selection: requested.selection.clone(),
            filtered: requested.filtered.clone(),
            spec: if peer_spec { requested.spec.clone() } else { slot.identity.spec.clone() },
            credentials: requested.credentials.clone(),
        });
        let lease = ResidentLease::new(slot.resident.clone(), view);
        self.slots.push_back(slot);
        Some(lease)
    }
    fn trim_idle(&mut self) {
        // A result too large for idle retention must not evict useful small trees.
        self.slots.retain(|slot| slot.resident.pins.load(Ordering::Acquire) != 0 || slot.bytes <= self.max_bytes);
        loop {
            let idle = self.slots.iter().filter(|slot| slot.resident.pins.load(Ordering::Acquire) == 0);
            let (count, bytes) = idle.fold((0, 0usize), |(count, bytes), slot| (count + 1, bytes.saturating_add(slot.bytes)));
            if count <= self.max_entries && bytes <= self.max_bytes {
                break;
            }
            let Some(victim) = self.slots.iter().position(|slot| slot.resident.pins.load(Ordering::Acquire) == 0) else {
                break;
            };
            self.slots.remove(victim);
        }
    }
    fn update_bytes(&mut self, id: u64, bytes: usize) -> anyhow::Result<()> {
        let slot = self.slots.iter_mut().find(|slot| slot.id == id).ok_or_else(|| anyhow::anyhow!("resident tree evicted"))?;
        slot.bytes = bytes;
        self.trim_idle();
        Ok(())
    }
}

/// Whether a resident admitted under `existing` can serve `requested`.
fn compatible<CD: ContextData>(
    existing: &Identity<CD>,
    requested: &Identity<CD>,
    catalog: &crate::schema::catalog::CatalogManager,
    peer_spec: bool,
    proof: impl Fn(&[CD]) -> bool,
) -> bool {
    existing.coverage.permits(&requested.selection)
        && existing.retrieval == requested.retrieval
        && (!peer_spec || existing.spec.key_spec() == requested.spec.key_spec())
        && existing.shares_access(requested, proof)
        && compatible_selection(existing, requested, catalog)
        && compatible_access(existing, requested, catalog)
}

#[cfg(test)]
mod tests;
