//! Resident tree admission and its standing reactor maintenance subscription.
use super::tree::TreeEntry;
use crate::{
    entity::TemporaryEntity,
    node::{
        state_sync::{QueryTree, Snapshot},
        Node,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::{
        maintenance::{MaintenanceSubscription, ProjectedChange, SourceBatch, SourceError, MAX_PENDING_BYTES},
        ChangeNotification,
    },
    selection::filter::{evaluate_predicate, ValueLookup},
    session::ContextData,
    storage::StorageEngine,
};
use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::CanonicalCodec;
use ankurah_proto::{self as proto, Attested};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};
#[cfg(test)]
use {crate::value::Value, tokio::sync::oneshot};

const MAX_READ_ATTEMPTS: usize = 8;
type ReadPolicy<'a, PA> = ContextPolicy<'a, PA, ankurah_signals::Mut<Vec<<PA as PolicyAgent>::ContextData>>>;
type RouteMembership = (BTreeMap<proto::EntityId, proto::QueryMember>, Vec<proto::EntityId>);

/// A limited resident cannot establish completeness for another rank cut.
#[derive(Clone, PartialEq)]
pub(super) enum Coverage {
    Complete,
    Limited(Selection<Resolved>),
}
impl Coverage {
    pub(super) fn for_selection(selection: &Selection<Resolved>) -> Self {
        if selection.limit.is_some() {
            Self::Limited(selection.clone())
        } else {
            Self::Complete
        }
    }
    pub(super) fn permits(&self, selection: &Selection<Resolved>) -> bool {
        match self {
            Self::Complete => true,
            Self::Limited(original) => original == selection,
        }
    }
    pub(super) async fn select(
        &self,
        snapshot: Snapshot,
        selection: &Selection<Resolved>,
        catalog: &crate::schema::catalog::CatalogManager,
    ) -> anyhow::Result<Snapshot> {
        match self {
            Self::Complete => snapshot.select(selection, catalog).await,
            Self::Limited(original) => {
                anyhow::ensure!(original == selection, "limited resident does not cover this selection");
                Ok(snapshot)
            }
        }
    }
}

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
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    // IDs only: denied candidates must also be rechecked when opaque policy changes.
    candidates: BTreeSet<proto::EntityId>,
    bytes: usize,
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
    fn check_catalog(&self, batch: &SourceBatch<TreeUpdate>) -> anyhow::Result<()> {
        anyhow::ensure!(crate::schema::is_catalog_read(&self.selection.predicate) || !batch.catalog_changed, SourceError::CatalogChanged);
        Ok(())
    }
    fn fold_candidates(&mut self, change: &TreeUpdate) {
        if change.matches {
            self.candidates.insert(change.entity_id);
        } else {
            self.candidates.remove(&change.entity_id);
        }
    }
    async fn read<SE, PA>(
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

    async fn apply(&mut self, batch: &SourceBatch<TreeUpdate>, delivery: bool) -> anyhow::Result<Vec<TreeUpdate>> {
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

pub(super) struct Route {
    pub(super) peer: proto::EntityId,
    pub(super) stream: proto::StateStreamId,
    pub(super) owner: Weak<()>,
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) members: BTreeMap<proto::EntityId, proto::QueryMember>,
}

async fn current_state<SE, PA>(node: &Node<SE, PA>, id: proto::EntityId) -> anyhow::Result<Option<proto::EntityState>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    if let Some(entity) = node.entities.get(&id) {
        return Ok(Some(entity.to_entity_state()?));
    }
    Ok(node.storage.get_states(vec![id], &ankql::ast::Predicate::True).await?.into_iter().next().and_then(|result| match result {
        crate::storage::GetStateResult::Found(state) => Some(state.payload),
        _ => None,
    }))
}

impl Route {
    async fn changes<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        projection: &QueryProjection,
        policy: &ReadPolicy<'_, PA>,
        sources: &[TreeUpdate],
    ) -> anyhow::Result<Vec<proto::QueryChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let snapshot = projection.tree.current_snapshot().await?;
        let selected = projection.coverage.select(snapshot, &self.selection, &node.catalog).await?;
        let ordered = match selected.ordered_ids(&self.selection, &node.catalog) {
            Ok(ordered) => ordered,
            Err(error) if self.selection.limit.is_none() && error.is::<super::tree::UnsupportedTreeSelection>() => {
                selected.members().into_values().collect()
            }
            Err(error) => return Err(error),
        };
        let next = selected.query_members();
        self.changes_for_members(node, policy, sources, next, ordered).await
    }
    async fn changes_for_members<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        policy: &ReadPolicy<'_, PA>,
        sources: &[TreeUpdate],
        next: BTreeMap<proto::EntityId, proto::QueryMember>,
        ordered: Vec<proto::EntityId>,
    ) -> anyhow::Result<Vec<proto::QueryChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut events: BTreeMap<proto::EntityId, Vec<proto::EventId>> = BTreeMap::new();
        let mut ids = Vec::new();
        for source in sources {
            if (self.members.contains_key(&source.entity_id) || next.contains_key(&source.entity_id))
                && !events.contains_key(&source.entity_id)
            {
                ids.push(source.entity_id);
            }
            events.entry(source.entity_id).or_default().extend(source.events.iter().cloned());
        }
        // A LIMIT refill can admit unchanged entities. They need data as well.
        for id in ordered {
            if self.members.get(&id) != next.get(&id) && !ids.contains(&id) {
                ids.push(id);
            }
        }
        for id in self.members.keys() {
            if !next.contains_key(id) && !ids.contains(id) {
                ids.push(*id);
            }
        }
        let mut changes = Vec::new();
        for id in ids {
            let member = next.get(&id).cloned();
            let delta = if let Some(state) = current_state(node, id).await? {
                if policy.check_read(&id, &state.state).is_ok() {
                    let event_ids = events.remove(&id).unwrap_or_default();
                    let found = node.storage.get_events(event_ids.clone()).await?;
                    let by_id = found.into_iter().map(|event| (event.payload.id(), event)).collect::<BTreeMap<_, _>>();
                    let mut carried = Vec::new();
                    for event_id in event_ids {
                        let event = by_id.get(&event_id).ok_or_else(|| anyhow::anyhow!("live event unavailable"))?;
                        anyhow::ensure!(event.payload.entity_id == id, "live event entity mismatch");
                        policy.check_read_event(event, &state.state)?;
                        if !carried.iter().any(|existing: &proto::Attested<proto::Event>| existing.payload.id() == event_id) {
                            carried.push(event.clone());
                        }
                    }
                    let attestation = node.policy_agent.attest_state(node, &state);
                    Some(proto::DeltaContent::StateAndEvents {
                        state: Attested::opt(state, attestation).into(),
                        events: carried.into_iter().map(Into::into).collect(),
                    })
                } else {
                    None
                }
            } else {
                None
            };
            changes.push(proto::QueryChange { entity_id: id, member, delta });
        }
        self.members = next;
        Ok(changes)
    }
}

pub(crate) struct QuerySource<CD: ContextData> {
    credentials: Vec<CD>,
    pub(super) projection: QueryProjection,
    pub(super) cursor: MaintenanceSubscription<TreeUpdate>,
    pub(super) revision: u64,
    pub(super) routes: Vec<Route>,
}
impl<CD: ContextData> QuerySource<CD> {
    pub(super) fn credentials(&self) -> &[CD] { &self.credentials }
    pub(super) fn from_capture(credentials: Vec<CD>, capture: CapturedQuery) -> Self {
        Self { credentials, projection: capture.projection, cursor: capture.cursor, revision: capture.revision, routes: Vec::new() }
    }
    pub(super) async fn check_authorization<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let snapshot = self.projection.tree.current_snapshot().await?;
        if policy.allows_unrestricted_state_reads() {
            // A new grant must also admit candidates previously denied by policy.
            // Returning the old subset would silently lose newly readable rows.
            anyhow::ensure!(snapshot.entries.keys().eq(self.projection.candidates.iter()), SourceError::Changed);
            return Ok(());
        }
        for id in &self.projection.candidates {
            let state = current_state(node, *id).await?.ok_or(SourceError::Changed)?;
            anyhow::ensure!(policy.check_read(id, &state.state).is_ok() == snapshot.entries.contains_key(id), SourceError::Changed);
        }
        Ok(())
    }
    pub(super) fn accounted_bytes(&self) -> usize { self.projection.bytes.saturating_add(self.projection.candidates.len() * 64) }
    async fn deliver<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        revision: u64,
        sources: Vec<TreeUpdate>,
        prepared: Option<Vec<anyhow::Result<RouteMembership>>>,
    ) where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let mut retained = Vec::with_capacity(self.routes.len());
        let mut prepared = prepared.map(Vec::into_iter);
        for mut route in std::mem::take(&mut self.routes) {
            let membership = prepared.as_mut().map(|prepared| prepared.next().expect("one prepared membership per route"));
            if route.owner.strong_count() == 0 {
                continue;
            }
            let result = async {
                anyhow::ensure!(
                    policy.filter_predicate(route.selection.predicate.clone())? == route.filtered.predicate,
                    SourceError::Changed
                );
                if let Some(membership) = membership {
                    let (next, ordered) = membership?;
                    route.changes_for_members(node, &policy, &sources, next, ordered).await
                } else {
                    route.changes(node, &self.projection, &policy, &sources).await
                }
            }
            .await;
            match result {
                Ok(changes) => {
                    if changes.is_empty() || super::queue_live(node, route.peer, route.stream, revision, changes).is_ok() {
                        retained.push(route);
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, "resident tree live delivery failed");
                    let weak = node.weak();
                    crate::task::spawn(async move {
                        if let Some(node) = weak.upgrade() {
                            let _ = super::expire_state_stream(&node, route.peer, route.stream).await;
                        }
                    });
                }
            }
        }
        self.routes = retained;
    }
    pub(super) async fn publish<SE, PA>(&mut self, node: &Node<SE, PA>, batch: Arc<SourceBatch<TreeUpdate>>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        self.projection.check_catalog(&batch)?;
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if matches!(self.projection.coverage, Coverage::Limited(_)) {
            if self.projection.selection.limit != Some(0) && self.relevant(&batch) {
                return self.refill(node, vec![batch]).await;
            }
        } else {
            let window = self.cursor.fence().await?;
            let pending = std::iter::once(batch).chain(window.batches).collect();
            return self.advance_complete(node, pending, window.revision.0).await;
        }
        self.revision = batch.revision.0;
        Ok(())
    }

    /// Apply a finite metadata window before publishing its net membership.
    /// Repeated changes keep their event identities but update the index once.
    async fn advance_complete<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        pending: Vec<Arc<SourceBatch<TreeUpdate>>>,
        revision: u64,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut bytes = 0usize;
        let mut changes = Vec::new();
        for batch in pending {
            self.projection.check_catalog(&batch)?;
            anyhow::ensure!(batch.bytes <= MAX_PENDING_BYTES.saturating_sub(bytes), SourceError::Lagged);
            bytes += batch.bytes;
            changes.extend(batch.changes.iter().cloned());
        }
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if !changes.is_empty() {
            let batch =
                SourceBatch { revision: crate::reactor::maintenance::SourceRevision(revision), changes, catalog_changed: false, bytes };
            let sources = self.projection.apply(&batch, !self.routes.is_empty()).await?;
            if !sources.is_empty() {
                self.deliver(node, revision, sources, None).await;
            }
        }
        self.revision = revision;
        Ok(())
    }
    fn relevant(&self, batch: &SourceBatch<TreeUpdate>) -> bool {
        batch.changes.iter().any(|change| change.matches || self.projection.candidates.contains(&change.entity_id))
    }
    async fn refill<SE, PA>(&mut self, node: &Node<SE, PA>, mut pending: Vec<Arc<SourceBatch<TreeUpdate>>>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut bytes = pending.iter().map(|batch| batch.bytes).sum::<usize>();
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let mut attempts = 0;
        let (rows, candidates, ordered, revision) = loop {
            attempts += 1;
            anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
            let window = self.cursor.fence().await?;
            for batch in window.batches {
                self.projection.check_catalog(&batch)?;
                anyhow::ensure!(batch.bytes <= MAX_PENDING_BYTES.saturating_sub(bytes), SourceError::Lagged);
                bytes += batch.bytes;
                pending.push(batch);
            }
            let (rows, candidates, ordered) = self.projection.read(node, &policy).await?;
            match self.cursor.validate_limited_read(window.revision).await {
                Err(SourceError::Changed) => continue,
                result => {
                    let revision = result?.revision;
                    // Keep bounded residents' explicit entity watches bounded
                    // too; predicate routing still observes future entrants.
                    match self
                        .cursor
                        .finish_bootstrap(self.projection.selection.predicate.clone(), candidates.iter().copied(), revision)
                        .await
                    {
                        Err(SourceError::Changed) => continue,
                        result => {
                            result?;
                            break (rows, candidates, ordered, revision.0);
                        }
                    }
                }
            }
        };
        self.projection.candidates = candidates;
        let snapshot = self.projection.tree.refresh_entries(self.projection.spec.clone(), rows.into_values().collect()).await?;
        self.projection.bytes = snapshot.accounted_bytes();
        let sources = pending.into_iter().flat_map(|batch| batch.changes.clone()).collect();
        let members = snapshot.query_members();
        let prepared = self.routes.iter().map(|_| Ok((members.clone(), ordered.clone()))).collect();
        self.deliver(node, revision, sources, Some(prepared)).await;
        self.revision = revision;
        Ok(())
    }
    pub(super) async fn refresh<SE, PA>(&mut self, node: &Node<SE, PA>) -> anyhow::Result<Snapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let window = self.cursor.fence().await?;
        if matches!(self.projection.coverage, Coverage::Limited(_)) {
            let mut relevant = false;
            for batch in &window.batches {
                self.projection.check_catalog(batch)?;
                relevant |= self.projection.selection.limit != Some(0) && self.relevant(batch);
            }
            if relevant {
                self.refill(node, window.batches).await?;
            } else {
                self.revision = window.revision.0;
            }
        } else {
            self.advance_complete(node, window.batches, window.revision.0).await?;
        }
        self.projection.tree.current_snapshot().await
    }
}

#[cfg(test)]
mod tests;
