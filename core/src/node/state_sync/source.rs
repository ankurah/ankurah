//! A query's source projection, driven by immutable commit publications.
use crate::{
    entity::TemporaryEntity,
    node::{
        source_publications::{SourceBatch, SourceChange, SourceCursor, SourceError, MAX_PENDING_BATCHES, MAX_PENDING_BYTES},
        state_sync::{QueryTreeCache, Snapshot},
        Node,
    },
    policy::{ContextPolicy, PolicyAgent},
    selection::filter::{evaluate_predicate, ValueLookup},
    session::ContextData,
    storage::StorageEngine,
    value::Value,
};
use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::CanonicalCodec;
use ankurah_proto::{self as proto, Attested};
use indexmap::IndexMap;
use std::{
    collections::BTreeSet,
    sync::{Arc, Weak},
};
#[cfg(test)]
use tokio::sync::oneshot;

const MAX_READ_ATTEMPTS: usize = 8;

// Limited reads retain storage query order for unchanged refill entrants.
type Rows = IndexMap<proto::EntityId, Row>;
type ReadPolicy<'a, PA> = ContextPolicy<'a, PA, ankurah_signals::Mut<Vec<<PA as PolicyAgent>::ContextData>>>;

#[derive(Clone)]
struct Row {
    state: Arc<proto::EntityState>,
    values: Vec<Option<Value>>,
    key: Vec<u8>,
    bytes: usize,
}

pub(crate) struct CapturedQuery {
    pub(crate) revision: u64,
    #[cfg(test)]
    pub(crate) snapshot: Snapshot,
    pub(crate) projection: QueryProjection,
    pub(crate) cursor: SourceCursor,
}

pub(crate) struct QueryProjection {
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    rows: Rows,
    // Retain denied candidates too: policy can change without an entity commit.
    candidates: Rows,
    candidate_bytes: usize,
    pub(super) tree: Arc<QueryTreeCache>,
}

/// Register before scanning, then fold a complete source window or validate the
/// whole limited selection. No scan holds the source commit gate.
#[cfg(test)]
pub(crate) async fn capture_query<SE, PA>(
    node: &Node<SE, PA>,
    mut selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    credentials: Vec<PA::ContextData>,
    tree: Arc<QueryTreeCache>,
) -> anyhow::Result<CapturedQuery>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.system.require_system_ready()?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    selection.predicate = policy.filter_predicate(selection.predicate)?;
    capture_filtered_query(node, selection, spec, credentials, tree).await
}

pub(super) async fn capture_filtered_query<SE, PA>(
    node: &Node<SE, PA>,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    credentials: Vec<PA::ContextData>,
    tree: Arc<QueryTreeCache>,
) -> anyhow::Result<CapturedQuery>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let (mut start, mut cursor) = node.source_publications.subscribe().await?;
    let mut projection = QueryProjection { selection, spec, rows: Rows::new(), candidates: Rows::new(), candidate_bytes: 0, tree };
    let mut attempts = 0;
    let window = loop {
        attempts += 1;
        anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
        projection.replace_candidates(projection.read(node).await?);
        projection.rows = projection.authorized_rows(&policy);
        if projection.selection.limit.is_some() {
            match cursor.validate(start).await {
                Err(SourceError::Changed) => {
                    let window = cursor.fence().await?;
                    for batch in &window.batches {
                        projection.check_catalog(batch)?;
                    }
                    start = window.revision;
                }
                result => break result?,
            }
        } else {
            let window = cursor.fence().await?;
            for batch in &window.batches {
                projection.check_catalog(batch)?;
                for change in &batch.changes {
                    projection.fold(&policy, change)?;
                }
            }
            break window;
        }
    };
    let snapshot = projection.tree.refresh_captured(projection.spec.clone(), projection.attested_rows(node)).await?;
    #[cfg(not(test))]
    drop(snapshot);
    Ok(CapturedQuery {
        revision: window.revision.0,
        #[cfg(test)]
        snapshot,
        projection,
        cursor,
    })
}

impl QueryProjection {
    fn check_catalog(&self, batch: &SourceBatch) -> anyhow::Result<()> {
        anyhow::ensure!(
            crate::schema::is_catalog_read(&self.selection.predicate)
                || !batch.changes.iter().any(|change| change.state.state.memberships.iter().any(crate::schema::is_catalog_model)),
            "query catalog changed; reopen the query generation"
        );
        Ok(())
    }

    fn candidate(&self, state: Arc<proto::EntityState>) -> anyhow::Result<Option<Row>> {
        let entity = TemporaryEntity::new(state.entity_id, &state.state)?;
        if !evaluate_predicate(&entity, &self.selection.predicate)? {
            return Ok(None);
        }
        let values = QueryTreeCache::key_paths(&self.spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
        let key = CanonicalCodec.encode_nullable(self.spec.key_spec(), &values)?;
        let bytes = (bincode::serialized_size(state.as_ref())? as usize)
            .saturating_mul(4)
            .saturating_add(key.len().saturating_mul(16))
            .saturating_add(2048);
        Ok(Some(Row { state, values, key, bytes }))
    }

    async fn read<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<Rows>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        node.system.check_not_halted()?;
        let states = node.storage.fetch_states(&self.selection).await?;
        let mut rows = Rows::new();
        for state in states {
            if let Some(row) = self.candidate(Arc::new(state.payload))? {
                rows.insert(row.state.entity_id, row);
            }
        }
        Ok(rows)
    }

    fn replace_candidates(&mut self, rows: Rows) {
        self.candidate_bytes = rows.values().map(|row| row.bytes).sum();
        self.candidates = rows;
    }

    fn authorized_rows<PA: PolicyAgent>(&self, policy: &ReadPolicy<'_, PA>) -> Rows {
        self.candidates
            .iter()
            .filter(|(_, row)| policy.check_read(&row.state.entity_id, &row.state.state).is_ok())
            .map(|(id, row)| (*id, row.clone()))
            .collect()
    }

    fn fold<PA: PolicyAgent>(&mut self, policy: &ReadPolicy<'_, PA>, change: &SourceChange) -> anyhow::Result<()> {
        let id = change.state.entity_id;
        if let Some(row) = self.candidate(change.state.clone())? {
            if policy.check_read(&id, &row.state.state).is_ok() {
                self.rows.insert(id, row.clone());
            } else {
                self.rows.swap_remove(&id);
            }
            self.candidate_bytes = self.candidate_bytes.saturating_add(row.bytes);
            if let Some(previous) = self.candidates.insert(id, row) {
                self.candidate_bytes = self.candidate_bytes.saturating_sub(previous.bytes);
            }
        } else {
            self.rows.swap_remove(&id);
            if let Some(previous) = self.candidates.swap_remove(&id) {
                self.candidate_bytes = self.candidate_bytes.saturating_sub(previous.bytes);
            }
        }
        Ok(())
    }

    fn attested<SE, PA>(node: &Node<SE, PA>, row: &Row) -> (Attested<proto::EntityState>, Vec<Option<Value>>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        (Attested::opt((*row.state).clone(), node.policy_agent.attest_state(node, &row.state)), row.values.clone())
    }

    fn attested_rows<SE, PA>(&self, node: &Node<SE, PA>) -> Vec<(Attested<proto::EntityState>, Vec<Option<Value>>)>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        self.rows.values().map(|row| Self::attested(node, row)).collect()
    }

    fn change<SE, PA>(&self, node: &Node<SE, PA>, policy: &ReadPolicy<'_, PA>, change: &SourceChange) -> anyhow::Result<proto::QueryChange>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let entity_id = change.state.entity_id;
        let member = self.rows.get(&entity_id).map(|row| proto::QueryMember { key: row.key.clone(), head: row.state.state.head.clone() });
        let delta = if policy.check_read(&entity_id, &change.state.state).is_ok() {
            for event in &change.events {
                policy.check_read_event(event, &change.state.state)?;
            }
            Some(proto::DeltaContent::StateAndEvents {
                state: Attested::opt((*change.state).clone(), node.policy_agent.attest_state(node, &change.state)).into(),
                events: change.events.iter().cloned().map(Into::into).collect(),
            })
        } else {
            None
        };
        Ok(proto::QueryChange { entity_id, member, delta })
    }

    async fn apply<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        credentials: &Vec<PA::ContextData>,
        batch: &SourceBatch,
        delivery: bool,
    ) -> anyhow::Result<Vec<SourceChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
        let mut changed = Vec::new();
        let mut rows = Vec::new();
        let mut removed = Vec::new();
        for change in &batch.changes {
            let id = change.state.entity_id;
            let was_member = self.rows.contains_key(&id);
            self.fold(&policy, change)?;
            if let Some(row) = self.rows.get(&id) {
                rows.push(Self::attested(node, row));
            } else if was_member {
                removed.push(id);
            }
            if delivery && (was_member || self.rows.contains_key(&id)) {
                changed.push(change.clone());
            }
        }
        self.tree.apply_captured(rows, removed).await?;
        Ok(changed)
    }
}

pub(super) struct Route {
    pub(super) peer: proto::EntityId,
    pub(super) stream: proto::StateStreamId,
    pub(super) owner: Weak<()>,
}

pub(crate) struct QuerySource<CD: ContextData> {
    credentials: Vec<CD>,
    pub(super) projection: QueryProjection,
    pub(super) cursor: SourceCursor,
    pub(super) revision: u64,
    pub(super) routes: Vec<Route>,
}

impl<CD: ContextData> QuerySource<CD> {
    pub(super) fn credentials(&self) -> &[CD] { &self.credentials }

    pub(super) fn from_capture(credentials: Vec<CD>, capture: CapturedQuery) -> Self {
        Self { credentials, projection: capture.projection, cursor: capture.cursor, revision: capture.revision, routes: Vec::new() }
    }

    #[cfg(test)]
    fn new(
        peer: proto::EntityId,
        stream: proto::StateStreamId,
        credentials: Vec<CD>,
        projection: QueryProjection,
        cursor: SourceCursor,
        revision: u64,
    ) -> Self {
        // The direct source tests keep their stream alive independently of a cache lease.
        let _ = (peer, stream);
        Self { credentials, projection, cursor, revision, routes: Vec::new() }
    }

    pub(super) fn check_authorization<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        // External policy changes have no source revision. Reopen rather than
        // authoring different membership at an already published revision.
        for (id, row) in &self.projection.candidates {
            anyhow::ensure!(policy.check_read(id, &row.state.state).is_ok() == self.projection.rows.contains_key(id), SourceError::Changed);
        }
        Ok(())
    }

    pub(super) fn accounted_bytes(&self) -> usize {
        // Per-row accounting includes candidate and visible captures, map nodes,
        // canonical keys, and space for lazily constructed digest rows.
        4096usize.saturating_add(self.projection.candidate_bytes)
    }

    fn deliver<SE, PA>(&mut self, node: &Node<SE, PA>, revision: u64, sources: Vec<SourceChange>, removed: Vec<proto::EntityId>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let changes = sources.iter().map(|source| self.projection.change(node, &policy, source)).collect::<anyhow::Result<Vec<_>>>();
        match changes {
            Ok(mut changes) => {
                changes.extend(removed.into_iter().map(|entity_id| proto::QueryChange { entity_id, member: None, delta: None }));
                self.queue_changes(node, revision, changes);
            }
            Err(error) => {
                // State access and event delivery are separate permissions.
                // An unreadable event fails these routes, not the resident tree.
                tracing::debug!(%error, "resident tree live delivery denied");
                for route in self.routes.drain(..) {
                    let weak = node.weak();
                    crate::task::spawn(async move {
                        if let Some(node) = weak.upgrade() {
                            let _ = super::expire_state_stream(&node, route.peer, route.stream).await;
                        }
                    });
                }
            }
        }
    }

    fn queue_changes<SE, PA>(&mut self, node: &Node<SE, PA>, revision: u64, changes: Vec<proto::QueryChange>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if changes.is_empty() {
            return;
        }
        self.routes.retain(|route| {
            // One slow or disconnected stream cannot poison the shared tree.
            crate::node::state_sync::queue_live(node, route.peer, route.stream, revision, changes.clone()).is_ok()
        });
    }

    pub(super) async fn publish<SE, PA>(&mut self, node: &Node<SE, PA>, batch: Arc<SourceBatch>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        self.projection.check_catalog(&batch)?;
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if self.projection.selection.limit.is_some() {
            let mut relevant = false;
            for change in &batch.changes {
                relevant |= self.projection.candidates.contains_key(&change.state.entity_id)
                    || self.projection.candidate(change.state.clone())?.is_some();
            }
            if relevant {
                self.refill(node, vec![batch]).await?;
            } else {
                self.revision = batch.revision.0;
            }
        } else {
            let changes = self.projection.apply(node, &self.credentials, &batch, !self.routes.is_empty()).await?;
            if !changes.is_empty() {
                self.deliver(node, batch.revision.0, changes, Vec::new());
            }
            self.revision = batch.revision.0;
        }
        Ok(())
    }

    async fn refill<SE, PA>(&mut self, node: &Node<SE, PA>, mut pending: Vec<Arc<SourceBatch>>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        for batch in &pending {
            self.projection.check_catalog(batch)?;
        }
        let mut bytes = pending.iter().map(|batch| batch.bytes).sum::<usize>();
        let mut attempts = 0;
        let (rows, revision) = loop {
            attempts += 1;
            anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
            let window = self.cursor.fence().await?;
            for batch in window.batches {
                self.projection.check_catalog(&batch)?;
                anyhow::ensure!(
                    pending.len() < MAX_PENDING_BATCHES && batch.bytes <= MAX_PENDING_BYTES.saturating_sub(bytes),
                    SourceError::Lagged
                );
                bytes += batch.bytes;
                pending.push(batch);
            }
            let rows = self.projection.read(node).await?;
            match self.cursor.validate(window.revision).await {
                Err(SourceError::Changed) => continue,
                result => break (rows, result?.revision.0),
            }
        };
        self.projection.replace_candidates(rows);
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let rows = self.projection.authorized_rows(&policy);
        let previous = std::mem::replace(&mut self.projection.rows, rows);
        let delivery = self.routes.iter().any(|route| route.owner.strong_count() != 0);
        let mut changed = Vec::new();
        let mut removed = Vec::new();
        let mut included = BTreeSet::new();
        for batch in pending {
            for source in &batch.changes {
                let id = source.state.entity_id;
                if delivery && (previous.contains_key(&id) || self.projection.rows.contains_key(&id)) {
                    changed.push(source.clone());
                    included.insert(id);
                }
            }
        }
        for (id, row) in &self.projection.rows {
            if delivery
                && !included.contains(id)
                && previous.get(id).is_none_or(|before| before.key != row.key || before.state.state.head != row.state.state.head)
            {
                changed.push(SourceChange { state: row.state.clone(), events: Vec::new() });
            }
        }
        for id in previous.keys() {
            if delivery && !included.contains(id) && !self.projection.rows.contains_key(id) {
                // Rank eviction still authors absence even though this entity
                // itself did not change during the validated read.
                removed.push(*id);
            }
        }
        self.projection.tree.refresh_captured(self.projection.spec.clone(), self.projection.attested_rows(node)).await?;
        self.deliver(node, revision, changed, removed);
        self.revision = revision;
        Ok(())
    }

    pub(super) async fn refresh<SE, PA>(&mut self, node: &Node<SE, PA>) -> anyhow::Result<Snapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let window = self.cursor.fence().await?;
        if self.projection.selection.limit.is_some() {
            let mut relevant = false;
            for batch in &window.batches {
                self.projection.check_catalog(batch)?;
                for change in &batch.changes {
                    relevant |= self.projection.candidates.contains_key(&change.state.entity_id)
                        || self.projection.candidate(change.state.clone())?.is_some();
                }
            }
            if relevant {
                self.refill(node, window.batches).await?;
            } else {
                self.revision = window.revision.0;
            }
        } else {
            for batch in window.batches {
                self.publish(node, batch).await?;
            }
            // An empty fence can still advance the source publication boundary.
            self.revision = window.revision.0;
        }
        self.projection.tree.current_snapshot().await
    }
}

#[cfg(test)]
mod tests;
