//! One node-local LRU for every caller and responder using a digest tree.
mod actor;
mod bootstrap;
mod identity;
mod lease;
mod route;

use self::{
    actor::ResidentActor,
    identity::{compatible_access, compatible_selection, Coverage, Identity, QueryView},
    lease::Resident,
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
pub(crate) use lease::{ResidentLease, ResidentPin, ResidentSnapshot, ResidentSubscription};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{mpsc, oneshot, Notify};

const MAX_ENTRIES: usize = 32;
const MAX_BYTES: usize = 128 * 1024 * 1024;

/// A single cache holds active and idle residents. Active leases pin their
/// entries under the existing operation/stream bounds; the LRU count and byte
/// allowances bound additional idle retention. Frozen rounds remain independent.
pub(crate) struct ResidentTrees<CD: ContextData> {
    lru: Arc<Mutex<Lru<CD>>>,
    // Only misses serialize. Hits never wait for an unrelated storage scan.
    builds: tokio::sync::Mutex<()>,
}
struct Lru<CD: ContextData> {
    slots: VecDeque<Slot<CD>>,
    next_id: u64,
    max_entries: usize,
    max_bytes: usize,
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
        Self {
            lru: Arc::new(Mutex::new(Lru { slots: VecDeque::new(), next_id: 0, max_entries, max_bytes })),
            builds: tokio::sync::Mutex::new(()),
        }
    }
    /// A snapshot of the resident that serves `selection` for `credentials`,
    /// in a tree under `spec` when the peer chose one.
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
        self.with_lease(node, selection, spec, credentials, |lease| async move { lease.snapshot().await }).await
    }

    /// Open a live route for `peer` on the resident that serves `selection`
    /// for `credentials` in a tree under the peer's `spec`, and its first
    /// state reply.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn subscribe<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: proto::TreeSpec,
        credentials: Vec<CD>,
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
        self.with_lease(node, selection, Some(spec), credentials, |lease| {
            let (request, fingerprint, known) = (request.clone(), fingerprint.clone(), known.clone());
            async move { lease.subscribe(node, peer, request, fingerprint, query, version, catalog, known).await }
        })
        .await
    }

    /// Run `with` on a lease of the resident that serves `selection`, retrying
    /// once: admitting again after a catalog change crossed the admission's
    /// storage read, or after `with` failed with a retryable source error.
    async fn with_lease<SE, PA, T, F, Fut>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: Option<proto::TreeSpec>,
        credentials: Vec<CD>,
        with: F,
    ) -> anyhow::Result<T>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
        F: Fn(ResidentLease<CD>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        for attempt in 0..2 {
            let lease = match self.acquire(node, selection.clone(), spec.clone(), credentials.clone()).await {
                Err(error) if attempt == 0 && matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)) => continue,
                result => result?,
            };
            match with(lease).await {
                Err(error) if attempt == 0 && SourceError::is_retryable(&error) => continue,
                result => return result,
            }
        }
        unreachable!("the second attempt returns")
    }

    pub(crate) async fn acquire<SE, PA>(
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
        if let Some(lease) = self.lru.lock().unwrap().get(&requested, &node.catalog, peer_spec, proof) {
            return Ok(lease);
        }
        let _build = self.builds.lock().await;
        if let Some(lease) = self.lru.lock().unwrap().get(&requested, &node.catalog, peer_spec, proof) {
            return Ok(lease);
        }
        let identity = requested.admission();
        let actor = ResidentActor::bootstrap(node, identity.clone()).await?;
        node.system.check_not_halted()?;
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
        actor.spawn(node, receiver, canceled, cleanup, Arc::downgrade(&self.lru), Arc::downgrade(&resident), id);
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
        let compatible = |slot: &Slot<CD>| {
            let existing = &slot.identity;
            existing.coverage.permits(&requested.selection)
                && existing.retrieval == requested.retrieval
                && (!peer_spec || existing.spec.key_spec() == requested.spec.key_spec())
                && existing.shares_access(requested, &proof)
                && compatible_selection(existing, requested, catalog)
                && compatible_access(existing, requested, catalog)
        };
        let position = self
            .slots
            .iter()
            .rposition(|slot| matches!(slot.identity.coverage, Coverage::Complete) && compatible(slot))
            .or_else(|| self.slots.iter().rposition(|slot| slot.identity.spec == requested.spec && compatible(slot)))
            .or_else(|| self.slots.iter().rposition(compatible))?;
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

#[cfg(test)]
mod tests;
