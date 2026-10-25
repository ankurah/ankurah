//! How a resident is admitted: the projector its maintenance subscription runs
//! on each commit, and the storage read, fenced against the commits that
//! crossed it, that its tree is built from.

use super::{
    actor::ResidentActor,
    identity::{Access, Coverage, Identity},
};
use crate::{
    entity::{Entity, TemporaryEntity},
    node::{
        state_sync::tree::{key_paths, QueryTree, TreeEntry, CLOCK_ENTRY_BYTES},
        Node,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::{ProjectedChange, SourceBatch, SourceError},
    selection::filter::{evaluate_predicate, ValueLookup},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_proto as proto;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::watch;

pub(super) const MAX_READ_ATTEMPTS: usize = 8;
pub(super) type ReadPolicy<'a, PA> = ContextPolicy<'a, PA, ankurah_signals::Mut<Vec<<PA as PolicyAgent>::ContextData>>>;

/// What the maintenance queue carries for one committed entity: its tree entry
/// when the resident admits it, and whether it matches the resident's predicate.
#[derive(Clone, Debug)]
pub(crate) struct TreeUpdate {
    pub entity_id: proto::EntityId,
    pub entry: Option<TreeEntry>,
    pub matches: bool,
}

impl TreeUpdate {
    /// The bytes this update holds in a maintenance queue: the update, and its
    /// entry's address and Clock entries.
    fn queued_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.entry.as_ref().map_or(0, |entry| entry.address().len() + entry.head.len() * CLOCK_ENTRY_BYTES)
    }
}

/// One storage read of what a resident admits: the readable members' entries,
/// and every candidate its predicate matches.
pub(super) struct StorageRead {
    pub(super) rows: BTreeMap<proto::EntityId, TreeEntry>,
    pub(super) candidates: BTreeSet<proto::EntityId>,
}

impl StorageRead {
    /// Read the entities `identity` admits from storage, checking each under `policy`.
    pub(super) async fn of<SE, PA>(
        node: &Node<SE, PA>,
        identity: &Identity<PA::ContextData>,
        policy: &ReadPolicy<'_, PA>,
    ) -> anyhow::Result<Self>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        node.system.check_not_halted()?;
        let mut read = Self { rows: BTreeMap::new(), candidates: BTreeSet::new() };
        let selection = &identity.filtered;
        if selection.limit == Some(0) {
            return Ok(read);
        }
        let paths = key_paths(&identity.spec);
        for state in node.storage.fetch_states(selection).await? {
            let id = state.payload.entity_id;
            let entity = TemporaryEntity::new(id, &state.payload.state)?;
            if !evaluate_predicate(&entity, &selection.predicate)? {
                continue;
            }
            read.candidates.insert(id);
            if identity.access == Access::Restricted && policy.check_read(&id, &state.payload.state).is_err() {
                continue;
            }
            let values = paths.iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
            read.rows.insert(id, TreeEntry::encode(&identity.spec, id, &values, state.payload.state.head)?);
        }
        Ok(read)
    }
}

/// Fail with `CatalogChanged` when `batch` changed the catalog and the
/// resident's selection is not itself a catalog read.
pub(super) fn ensure_catalog_unchanged<CD>(identity: &Identity<CD>, batch: &SourceBatch<TreeUpdate>) -> anyhow::Result<()> {
    anyhow::ensure!(crate::schema::is_catalog_read(&identity.filtered.predicate) || !batch.catalog_changed, SourceError::CatalogChanged);
    Ok(())
}

impl<CD: ContextData> ResidentActor<CD> {
    /// Admit what `identity` selects: subscribe to maintenance, read storage,
    /// fold in the commits that crossed the read, narrow the subscription's
    /// watch to the result at the same fence, and build the tree.
    pub(super) async fn bootstrap<SE, PA>(node: &Node<SE, PA>, identity: Identity<CD>) -> anyhow::Result<Self>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let selection = &identity.filtered;
        anyhow::ensure!(
            match &identity.coverage {
                Coverage::Complete => selection.limit.is_none() && selection.order_by.is_none(),
                Coverage::Limited(original) =>
                    original.limit.is_some() && selection.limit == original.limit && selection.order_by == original.order_by,
            },
            "resident admission does not match its declared coverage"
        );
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &identity.credentials);
        let (mut start, mut cursor) = node.reactor.subscribe_maintenance(projector(node, &identity)).await?;
        let mut rows = BTreeMap::new();
        let mut candidates = BTreeSet::new();
        let mut populated = false;
        let mut attempts = 0;
        loop {
            attempts += 1;
            anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
            if !populated {
                let read = StorageRead::of(node, &identity, &policy).await?;
                (rows, candidates) = (read.rows, read.candidates);
            }
            let window = if matches!(identity.coverage, Coverage::Limited(_)) && selection.limit != Some(0) {
                match cursor.fence_unchanged_since(start).await {
                    Err(SourceError::Changed) => {
                        let window = cursor.fence().await?;
                        if let Some(batch) = &window.batch {
                            ensure_catalog_unchanged(&identity, batch)?;
                        }
                        start = window.revision;
                        populated = false;
                        continue;
                    }
                    result => result?,
                }
            } else {
                let window = cursor.fence().await?;
                if let Some(batch) = &window.batch {
                    ensure_catalog_unchanged(&identity, batch)?;
                    if selection.limit != Some(0) {
                        for change in &batch.changes {
                            fold_candidate(&mut candidates, change);
                            if let Some(entry) = &change.entry {
                                rows.insert(change.entity_id, entry.clone());
                            } else {
                                rows.remove(&change.entity_id);
                            }
                        }
                    }
                }
                window
            };
            // Narrow the bootstrap's broad watch only at the same applied fence.
            match cursor.finish_bootstrap(selection.predicate.clone(), candidates.iter().copied(), window.revision).await {
                Err(SourceError::Changed) => {
                    start = window.revision;
                    populated = matches!(identity.coverage, Coverage::Complete);
                }
                result => {
                    result?;
                    break;
                }
            }
        }
        let tree = QueryTree::new(identity.spec.clone(), rows);
        let changes = watch::Sender::new(tree.version());
        Ok(Self { identity, candidates, tree, cursor, digest_requests: None, changes })
    }
}

/// Track whether `change`'s entity still matches the resident's predicate.
pub(super) fn fold_candidate(candidates: &mut BTreeSet<proto::EntityId>, change: &TreeUpdate) {
    if change.matches {
        candidates.insert(change.entity_id);
    } else {
        candidates.remove(&change.entity_id);
    }
}

/// The projection the reactor runs on each publication for the resident
/// `identity` admits: for each committed entity, whether it matches the
/// resident's predicate, and its tree entry when `identity`'s credentials may
/// read it. One policy, and at most one evaluation of its unrestricted-read
/// proof, serves the whole publication; an unrestricted resident checks no
/// principal's reads, since each principal it serves holds its own proof.
fn projector<SE, PA>(
    node: &Node<SE, PA>,
    identity: &Identity<PA::ContextData>,
) -> impl Fn(&[&Entity]) -> anyhow::Result<Vec<ProjectedChange<TreeUpdate>>> + Send + Sync + 'static
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let weak = node.weak();
    let predicate = identity.filtered.predicate.clone();
    let spec = identity.spec.clone();
    let paths = key_paths(&spec);
    let credentials = identity.credentials.clone();
    let access = identity.access;
    move |entities| {
        let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        let unrestricted = std::cell::OnceCell::new();
        entities
            .iter()
            .map(|committed| {
                let entity_id = committed.id();
                committed.with_read(|entity| {
                    let matches = evaluate_predicate(entity, &predicate)?;
                    // An unrestricted resident admits every match; a restricted
                    // one asks its admitting credentials' policy afresh.
                    let readable = matches
                        && (access == Access::Unrestricted
                            || *unrestricted.get_or_init(|| policy.allows_unrestricted_state_reads())
                            || policy.check_read(&entity_id, &entity.to_state()?).is_ok());
                    let entry = if readable {
                        let values = paths.iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
                        Some(TreeEntry::encode(&spec, entity_id, &values, entity.head().clone())?)
                    } else {
                        None
                    };
                    let update = TreeUpdate { entity_id, entry, matches };
                    Ok(ProjectedChange { bytes: update.queued_bytes(), value: update, member: matches })
                })?
            })
            .collect()
    }
}
