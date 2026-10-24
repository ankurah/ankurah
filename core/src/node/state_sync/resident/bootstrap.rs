//! How a resident is admitted: the projector its maintenance subscription runs
//! on each commit, and the storage read, fenced against the commits that
//! crossed it, that its tree is built from.

use super::{
    actor::ResidentActor,
    identity::{Coverage, Identity},
};
use crate::{
    changes::EntityChange,
    entity::TemporaryEntity,
    node::{
        state_sync::{
            tree::{key_paths, TreeEntry},
            QueryTree,
        },
        Node,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::{
        maintenance::{ProjectedChange, SourceBatch, SourceError},
        ChangeNotification,
    },
    selection::filter::{evaluate_predicate, ValueLookup},
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_digest_tree::CanonicalCodec;
use ankurah_proto as proto;
use std::collections::{BTreeMap, BTreeSet};

pub(super) const MAX_READ_ATTEMPTS: usize = 8;
pub(super) type ReadPolicy<'a, PA> = ContextPolicy<'a, PA, ankurah_signals::Mut<Vec<<PA as PolicyAgent>::ContextData>>>;

/// What the maintenance queue carries for one committed change: the entity's
/// tree entry when the resident admits it, whether it matches the resident's
/// predicate, and the ids of the events it committed. Event bodies stay in
/// storage until a route delivers them.
#[derive(Clone, Debug)]
pub(crate) struct TreeUpdate {
    pub entity_id: proto::EntityId,
    pub entry: Option<TreeEntry>,
    pub matches: bool,
    pub events: Vec<proto::EventId>,
}

/// One storage read of what a resident admits: the readable members' entries,
/// every candidate its predicate matches, and, under limited coverage, the
/// members in the order storage ranked them.
pub(super) struct StorageRead {
    pub(super) rows: BTreeMap<proto::EntityId, TreeEntry>,
    pub(super) candidates: BTreeSet<proto::EntityId>,
    pub(super) ordered: Vec<proto::EntityId>,
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
        let mut read = Self { rows: BTreeMap::new(), candidates: BTreeSet::new(), ordered: Vec::new() };
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
            if policy.check_read(&id, &state.payload.state).is_err() {
                continue;
            }
            if matches!(identity.coverage, Coverage::Limited(_)) {
                read.ordered.push(id);
            }
            let values = paths.iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
            read.rows.insert(
                id,
                TreeEntry {
                    entity_id: id,
                    key: CanonicalCodec::encode_nullable(identity.spec.key_spec(), &values)?,
                    head: state.payload.state.head,
                },
            );
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
        let revision = loop {
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
                        for batch in &window.batches {
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
                for batch in &window.batches {
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
                    break window.revision;
                }
            }
        };
        let tree = QueryTree::build(identity.spec.clone(), rows.into_values().collect()).await?;
        Ok(Self { identity, candidates, tree, cursor, revision: revision.0, routes: Vec::new() })
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

/// The projection the reactor runs on each commit for the resident `identity`
/// admits: whether the committed entity matches its predicate, its tree entry
/// when `identity`'s credentials may read it, and the committed events' ids.
fn projector<SE, PA>(
    node: &Node<SE, PA>,
    identity: &Identity<PA::ContextData>,
) -> impl Fn(&EntityChange, bool) -> anyhow::Result<ProjectedChange<TreeUpdate>> + Send + Sync + 'static
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let weak = node.weak();
    let predicate = identity.filtered.predicate.clone();
    let spec = identity.spec.clone();
    let paths = key_paths(&spec);
    let credentials = identity.credentials.clone();
    move |change, live_events| {
        let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        let entity_id = change.entity().id();
        change.entity().with_read(|entity| {
            let matches = evaluate_predicate(entity, &predicate)?;
            let entry = if matches
                && (policy.allows_unrestricted_state_reads() || policy.check_read(&entity_id, &entity.to_state()?).is_ok())
            {
                let values = paths.iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
                Some(TreeEntry { entity_id, key: CanonicalCodec::encode_nullable(spec.key_spec(), &values)?, head: entity.head().clone() })
            } else {
                None
            };
            let events = if live_events { change.events().iter().map(|event| event.payload.id()).collect() } else { Vec::new() };
            let bytes = 128 + entry.as_ref().map_or(0, |entry| entry.key.len() + entry.head.len() * 40) + events.len() * 32;
            Ok(ProjectedChange { value: TreeUpdate { entity_id, entry, matches, events }, member: matches, bytes })
        })?
    }
}
