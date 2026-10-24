//! One node-local LRU for every caller and responder using a digest tree.
mod actor;
mod bootstrap;
mod identity;
mod lease;
mod route;

use self::{
    actor::QuerySource,
    identity::{compatible_access, compatible_selection, Coverage, Identity, QueryView},
    lease::Resident,
};
use super::QueryTree;
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::SourceError,
    session::ContextData,
    storage::StorageEngine,
};
use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::TreeSpec;
use ankurah_proto as proto;
pub(crate) use lease::{Capture, ResidentLease, ResidentPin, ResidentSubscription};
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

    pub(crate) fn can_retry(error: &anyhow::Error) -> bool {
        matches!(error.downcast_ref::<SourceError>(), Some(SourceError::Changed | SourceError::Lagged | SourceError::CatalogChanged))
    }
    pub(crate) async fn capture<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: Option<proto::TreeSpec>,
        credentials: Vec<CD>,
    ) -> anyhow::Result<Capture>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        for attempt in 0..2 {
            let resident = match self.acquire(node, selection.clone(), spec.clone(), credentials.clone()).await {
                // Reopen admission and revalidate its layout and policy after a
                // catalog publication crosses the initial storage read.
                Err(error) if attempt == 0 && matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)) => continue,
                result => result?,
            };
            match resident.snapshot().await {
                Err(error) if attempt == 0 && Self::can_retry(&error) => continue,
                result => return result,
            }
        }
        unreachable!()
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
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        let mut filtered = selection.clone();
        filtered.predicate = policy.filter_predicate(filtered.predicate)?;
        let wire_required = spec.is_some();
        let spec = spec.unwrap_or_else(|| QueryTree::spec(&selection, &node.catalog));
        anyhow::ensure!(QueryTree::serves(&spec, &selection, &node.catalog), "tree cannot serve selection");
        let requested = Identity {
            coverage: Coverage::for_selection(&selection),
            selection,
            filtered,
            retrieval: policy.retrieval_predicate(),
            spec,
            credentials,
        };
        if let Some(lease) = self.entries.lock().unwrap().get(&requested, &node.catalog, wire_required) {
            return Ok(lease);
        }
        let _build = self.builds.lock().await;
        if let Some(lease) = self.entries.lock().unwrap().get(&requested, &node.catalog, wire_required) {
            return Ok(lease);
        }
        let view = Arc::new(QueryView {
            selection: requested.selection.clone(),
            filtered: requested.filtered.clone(),
            spec: requested.spec.clone(),
        });
        // A complete resident admits the whole predicate. A cold LIMIT keeps
        // explicit bounded coverage; it cannot satisfy a different selection.
        // The peer's layout does not require loading its broader predicate.
        let spec = TreeSpec::new(requested.spec.key_spec().clone(), requested.selection.predicate.clone());
        let (selection, filtered) = match &requested.coverage {
            Coverage::Complete => {
                (Selection::from(requested.selection.predicate.clone()), Selection::from(requested.filtered.predicate.clone()))
            }
            Coverage::Limited(_) => (requested.selection.clone(), requested.filtered.clone()),
        };
        let identity = Identity { selection, filtered, spec, ..requested };
        let tree = Arc::new(QueryTree::default());
        let captured = bootstrap::capture_filtered_query(
            node,
            identity.filtered.clone(),
            identity.spec.clone(),
            identity.credentials.clone(),
            tree.clone(),
            identity.coverage.clone(),
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
        let lease = ResidentLease::new(resident.clone(), view);
        let scope = identity.clone();
        let id = {
            let mut entries = self.entries.lock().unwrap();
            let id = entries.next_id;
            entries.next_id = id.checked_add(1).ok_or_else(|| anyhow::anyhow!("resident tree identity exhausted"))?;
            entries.entries.push_back(Entry { id, identity, resident: resident.clone(), bytes });
            id
        };
        actor.spawn(node, receiver, canceled, cleanup, Arc::downgrade(&self.entries), Arc::downgrade(&resident), id, scope);
        Ok(lease)
    }
}

impl<CD: ContextData> Entries<CD> {
    fn get(
        &mut self,
        requested: &Identity<CD>,
        catalog: &crate::schema::catalog::CatalogManager,
        wire_required: bool,
    ) -> Option<ResidentLease<CD>> {
        self.entries.retain(|entry| entry.resident.alive.load(Ordering::Acquire));
        let compatible = |entry: &Entry<CD>| {
            let existing = &entry.identity;
            existing.coverage.permits(&requested.selection)
                && existing.credentials == requested.credentials
                && existing.retrieval == requested.retrieval
                && (!wire_required || existing.spec.key_spec() == requested.spec.key_spec())
                && compatible_selection(existing, requested, catalog)
                && compatible_access(existing, requested, catalog)
        };
        let position = self
            .entries
            .iter()
            .rposition(|entry| matches!(entry.identity.coverage, Coverage::Complete) && compatible(entry))
            .or_else(|| self.entries.iter().rposition(|entry| entry.identity.spec == requested.spec && compatible(entry)))
            .or_else(|| self.entries.iter().rposition(compatible))?;
        let entry = self.entries.remove(position)?;
        let view = Arc::new(QueryView {
            selection: requested.selection.clone(),
            filtered: requested.filtered.clone(),
            spec: if wire_required { requested.spec.clone() } else { entry.identity.spec.clone() },
        });
        let lease = ResidentLease::new(entry.resident.clone(), view);
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

#[cfg(test)]
mod tests;
