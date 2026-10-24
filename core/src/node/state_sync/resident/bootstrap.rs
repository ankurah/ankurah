//! Resident tree admission and its standing reactor maintenance subscription.

use super::identity::Coverage;
#[cfg(test)]
use crate::node::state_sync::Snapshot;
use crate::{
    entity::TemporaryEntity,
    node::{
        state_sync::{tree::TreeEntry, QueryTree},
        Node,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::{
        maintenance::{MaintenanceSubscription, ProjectedChange, SourceBatch, SourceError},
        ChangeNotification,
    },
    selection::filter::{evaluate_predicate, ValueLookup},
    storage::StorageEngine,
};
use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::CanonicalCodec;
use ankurah_proto as proto;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub(super) const MAX_READ_ATTEMPTS: usize = 8;
pub(super) type ReadPolicy<'a, PA> = ContextPolicy<'a, PA, ankurah_signals::Mut<Vec<<PA as PolicyAgent>::ContextData>>>;

/// Only index metadata and event identities cross the maintenance queue.
/// Event bodies are read for active routes when constructing their outgoing messages.
#[derive(Clone, Debug)]
pub(crate) struct TreeUpdate {
    pub entity_id: proto::EntityId,
    pub entry: Option<TreeEntry>,
    pub matches: bool,
    pub events: Vec<proto::EventId>,
}

pub(crate) struct CapturedQuery {
    pub(crate) revision: u64,
    #[cfg(test)]
    pub(crate) snapshot: Snapshot,
    pub(crate) projection: QueryProjection,
    pub(crate) cursor: MaintenanceSubscription<TreeUpdate>,
}

pub(crate) struct QueryProjection {
    pub(super) coverage: Coverage,
    pub(super) selection: Selection<Resolved>,
    pub(super) spec: proto::TreeSpec,
    // IDs only: denied candidates must also be rechecked when opaque policy changes.
    pub(super) candidates: BTreeSet<proto::EntityId>,
    pub(super) bytes: usize,
    pub(super) tree: Arc<QueryTree>,
}

#[cfg(test)]
pub(crate) async fn capture_query<SE, PA>(
    node: &Node<SE, PA>,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    credentials: Vec<PA::ContextData>,
    tree: Arc<QueryTree>,
) -> anyhow::Result<CapturedQuery>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.system.require_system_ready()?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let admission = Selection::from(policy.filter_predicate(spec.predicate().clone())?);
    let mut captured = capture_filtered_query(node, admission, spec, credentials.clone(), tree, Coverage::Complete).await?;
    captured.snapshot = captured.projection.coverage.select(captured.snapshot, &selection, &node.catalog).await?;
    Ok(captured)
}

pub(super) async fn capture_filtered_query<SE, PA>(
    node: &Node<SE, PA>,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    credentials: Vec<PA::ContextData>,
    tree: Arc<QueryTree>,
    coverage: Coverage,
) -> anyhow::Result<CapturedQuery>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    anyhow::ensure!(
        match &coverage {
            Coverage::Complete => selection.limit.is_none() && selection.order_by.is_none(),
            Coverage::Limited(original) =>
                original.limit.is_some() && selection.limit == original.limit && selection.order_by == original.order_by,
        },
        "resident capture does not match its declared coverage"
    );
    let weak = node.weak();
    let subscribed_selection = selection.clone();
    let subscribed_spec = spec.clone();
    let subscribed_paths = QueryTree::key_paths(&spec);
    let subscribed_credentials = credentials.clone();
    let (mut start, mut cursor) = node
        .reactor
        .subscribe_maintenance(move |change, live_events| {
            let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
            let policy = ContextPolicy::from_credentials(&node.policy_agent, &subscribed_credentials);
            let entity_id = change.entity().id();
            change.entity().with_read(|entity| {
                let matches = evaluate_predicate(entity, &subscribed_selection.predicate)?;
                let entry = if matches
                    && (policy.allows_unrestricted_state_reads() || policy.check_read(&entity_id, &entity.to_state()?).is_ok())
                {
                    let values = subscribed_paths.iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
                    Some(TreeEntry {
                        entity_id,
                        key: CanonicalCodec::encode_nullable(subscribed_spec.key_spec(), &values)?,
                        head: entity.head().clone(),
                    })
                } else {
                    None
                };
                let events = if live_events { change.events().iter().map(|event| event.payload.id()).collect() } else { Vec::new() };
                let bytes = 128 + entry.as_ref().map_or(0, |entry| entry.key.len() + entry.head.len() * 40) + events.len() * 32;
                Ok(ProjectedChange { value: TreeUpdate { entity_id, entry, matches, events }, member: matches, bytes })
            })?
        })
        .await?;
    let mut projection = QueryProjection { coverage, selection, spec, candidates: BTreeSet::new(), bytes: 0, tree };
    let mut rows = BTreeMap::new();
    let mut populated = false;
    let mut attempts = 0;
    let revision = loop {
        attempts += 1;
        anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
        if !populated {
            (rows, projection.candidates, _) = projection.read(node, &policy).await?;
        }
        let window = if matches!(projection.coverage, Coverage::Limited(_)) && projection.selection.limit != Some(0) {
            match cursor.validate_limited_read(start).await {
                Err(SourceError::Changed) => {
                    let window = cursor.fence().await?;
                    for batch in &window.batches {
                        projection.check_catalog(batch)?;
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
                projection.check_catalog(batch)?;
                if projection.selection.limit != Some(0) {
                    for change in &batch.changes {
                        projection.fold_candidates(change);
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
        match cursor.finish_bootstrap(projection.selection.predicate.clone(), projection.candidates.iter().copied(), window.revision).await
        {
            Err(SourceError::Changed) => {
                start = window.revision;
                populated = matches!(projection.coverage, Coverage::Complete);
            }
            result => {
                result?;
                break window.revision;
            }
        }
    };
    let snapshot = projection.tree.refresh_entries(projection.spec.clone(), rows.into_values().collect()).await?;
    projection.bytes = snapshot.accounted_bytes();
    #[cfg(not(test))]
    drop(snapshot);
    Ok(CapturedQuery {
        revision: revision.0,
        #[cfg(test)]
        snapshot,
        projection,
        cursor,
    })
}

impl QueryProjection {
    pub(super) fn check_catalog(&self, batch: &SourceBatch<TreeUpdate>) -> anyhow::Result<()> {
        anyhow::ensure!(crate::schema::is_catalog_read(&self.selection.predicate) || !batch.catalog_changed, SourceError::CatalogChanged);
        Ok(())
    }
    pub(super) fn fold_candidates(&mut self, change: &TreeUpdate) {
        if change.matches {
            self.candidates.insert(change.entity_id);
        } else {
            self.candidates.remove(&change.entity_id);
        }
    }
    pub(super) async fn read<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        policy: &ReadPolicy<'_, PA>,
    ) -> anyhow::Result<(BTreeMap<proto::EntityId, TreeEntry>, BTreeSet<proto::EntityId>, Vec<proto::EntityId>)>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        node.system.check_not_halted()?;
        let mut rows = BTreeMap::new();
        let mut candidates = BTreeSet::new();
        let mut ordered = Vec::new();
        let paths = QueryTree::key_paths(&self.spec);
        if self.selection.limit != Some(0) {
            for state in node.storage.fetch_states(&self.selection).await? {
                let id = state.payload.entity_id;
                let entity = TemporaryEntity::new(id, &state.payload.state)?;
                if !evaluate_predicate(&entity, &self.selection.predicate)? {
                    continue;
                }
                candidates.insert(id);
                if policy.check_read(&id, &state.payload.state).is_err() {
                    continue;
                }
                if matches!(self.coverage, Coverage::Limited(_)) {
                    ordered.push(id);
                }
                let values = paths.iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
                rows.insert(
                    id,
                    TreeEntry {
                        entity_id: id,
                        key: CanonicalCodec::encode_nullable(self.spec.key_spec(), &values)?,
                        head: state.payload.state.head,
                    },
                );
            }
        }
        Ok((rows, candidates, ordered))
    }

    pub(super) async fn apply(&mut self, batch: &SourceBatch<TreeUpdate>, delivery: bool) -> anyhow::Result<Vec<TreeUpdate>> {
        let before = self.tree.current_snapshot().await?;
        let mut affected = BTreeSet::new();
        let mut latest = BTreeMap::new();
        for change in &batch.changes {
            self.fold_candidates(change);
            latest.insert(change.entity_id, change.entry.clone());
            if delivery && (before.entries.contains_key(&change.entity_id) || change.entry.is_some()) {
                affected.insert(change.entity_id);
            }
        }
        drop(before); // Do not force a full metadata copy solely for this update's membership check.
        let mut entries = Vec::new();
        let mut removed = Vec::new();
        for (id, entry) in latest {
            if let Some(entry) = entry {
                entries.push(entry);
            } else {
                removed.push(id);
            }
        }
        let snapshot = self.tree.apply_entries(entries, removed).await?;
        self.bytes = snapshot.accounted_bytes();
        Ok(batch.changes.iter().filter(|change| affected.contains(&change.entity_id)).cloned().collect())
    }
}
