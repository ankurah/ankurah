//! A query's source projection, driven by immutable commit publications.
use crate::{
    entity::TemporaryEntity,
    node::{
        source_publications::{SourceBatch, SourceChange, SourceCursor, SourceError, MAX_PENDING_BATCHES, MAX_PENDING_BYTES},
        state_sync::{QueryTree, Snapshot},
        Node, WeakNode,
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
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::{mpsc, oneshot};

const MAX_READ_ATTEMPTS: usize = 8;

// Limited reads retain storage query order for unchanged refill entrants.
type Rows = IndexMap<proto::EntityId, Row>;
type ReadPolicy<'a, PA> = ContextPolicy<'a, PA, ankurah_signals::Mut<Vec<<PA as PolicyAgent>::ContextData>>>;

struct Row {
    state: Arc<proto::EntityState>,
    values: Vec<Option<Value>>,
    key: Vec<u8>,
}

pub(crate) struct CapturedQuery {
    pub(crate) revision: u64,
    pub(crate) snapshot: Snapshot,
    pub(super) projection: QueryProjection,
    pub(super) cursor: SourceCursor,
}

pub(super) struct QueryProjection {
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    rows: Rows,
    tree: Arc<QueryTree>,
}

pub(super) struct Refresh {
    pub(super) number: u64,
    pub(super) known: proto::KnownState,
    pub(super) response: oneshot::Sender<anyhow::Result<()>>,
}

/// Register before scanning, then fold a complete source window or validate the
/// whole limited selection. No scan holds the source commit gate.
pub(crate) async fn capture_query<SE, PA>(
    node: &Node<SE, PA>,
    mut selection: Selection<Resolved>,
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
    selection.predicate = policy.filter_predicate(selection.predicate)?;
    let (mut start, mut cursor) = node.source_publications.subscribe().await?;
    let mut projection = QueryProjection { selection, spec, rows: Rows::new(), tree };
    let mut attempts = 0;
    let window = loop {
        attempts += 1;
        anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
        projection.rows = projection.read(node, &credentials).await?;
        if projection.selection.limit.is_some() {
            match cursor.validate(start).await {
                Err(SourceError::Changed) => {
                    start = cursor.fence().await?.revision;
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
    Ok(CapturedQuery { revision: window.revision.0, snapshot, projection, cursor })
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

    fn evaluate<PA: PolicyAgent>(&self, policy: &ReadPolicy<'_, PA>, state: Arc<proto::EntityState>) -> anyhow::Result<Option<Row>> {
        if policy.check_read(&state.entity_id, &state.state).is_err() {
            return Ok(None);
        }
        let entity = TemporaryEntity::new(state.entity_id, &state.state)?;
        if !evaluate_predicate(&entity, &self.selection.predicate)? {
            return Ok(None);
        }
        let values = QueryTree::key_paths(&self.spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
        let key = CanonicalCodec.encode_nullable(self.spec.key_spec(), &values)?;
        Ok(Some(Row { state, values, key }))
    }

    async fn read<SE, PA>(&self, node: &Node<SE, PA>, credentials: &Vec<PA::ContextData>) -> anyhow::Result<Rows>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        node.system.check_not_halted()?;
        let states = node.storage.fetch_states(&self.selection).await?;
        let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
        let mut rows = Rows::new();
        for state in states {
            if let Some(row) = self.evaluate(&policy, Arc::new(state.payload))? {
                rows.insert(row.state.entity_id, row);
            }
        }
        Ok(rows)
    }

    fn fold<PA: PolicyAgent>(&mut self, policy: &ReadPolicy<'_, PA>, change: &SourceChange) -> anyhow::Result<()> {
        if let Some(row) = self.evaluate(policy, change.state.clone())? {
            self.rows.insert(change.state.entity_id, row);
        } else {
            self.rows.swap_remove(&change.state.entity_id);
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
    ) -> anyhow::Result<Vec<proto::QueryChange>>
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
            if was_member || self.rows.contains_key(&id) {
                changed.push(self.change(node, &policy, change)?);
            }
        }
        self.tree.apply_captured(rows, removed).await?;
        Ok(changed)
    }
}

pub(super) struct QuerySource<CD: ContextData> {
    peer: proto::EntityId,
    comparison: proto::ComparisonId,
    credentials: Vec<CD>,
    projection: QueryProjection,
    cursor: SourceCursor,
    revision: u64,
}

impl<CD: ContextData> QuerySource<CD> {
    pub(super) fn new(
        peer: proto::EntityId,
        comparison: proto::ComparisonId,
        credentials: Vec<CD>,
        projection: QueryProjection,
        cursor: SourceCursor,
        revision: u64,
    ) -> Self {
        Self { peer, comparison, credentials, projection, cursor, revision }
    }

    pub(super) async fn run<SE, PA>(mut self, weak: WeakNode<SE, PA>, mut commands: mpsc::Receiver<Refresh>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        loop {
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else { return Ok(()); };
                    let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    let snapshot = match self.refresh(&node).await {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            let _ = command.response.send(Err(anyhow::anyhow!("query capture failed: {error}")));
                            return Err(error);
                        }
                    };
                    // Bad or premature round requests do not invalidate an
                    // otherwise sound source projection.
                    let result = crate::node::state_sync::schedule_round(
                        &node, self.peer, self.comparison, command.number, self.revision, snapshot, command.known,
                    ).await;
                    let _ = command.response.send(result);
                }
                batch = self.cursor.next() => {
                    let batch = batch?;
                    let node = weak.upgrade().ok_or_else(|| anyhow::anyhow!("node dropped"))?;
                    self.publish(&node, batch).await?;
                }
            }
        }
    }

    async fn publish<SE, PA>(&mut self, node: &Node<SE, PA>, batch: Arc<SourceBatch>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        self.projection.check_catalog(&batch)?;
        if self.projection.selection.limit.is_some() {
            let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
            let mut relevant = false;
            for change in &batch.changes {
                relevant |= self.projection.rows.contains_key(&change.state.entity_id)
                    || self.projection.evaluate(&policy, change.state.clone())?.is_some();
            }
            if relevant {
                self.refill(node, vec![batch]).await?;
            } else {
                self.revision = batch.revision.0;
            }
        } else {
            let changes = self.projection.apply(node, &self.credentials, &batch).await?;
            if !changes.is_empty() {
                crate::node::state_sync::queue_live(node, self.peer, self.comparison, batch.revision.0, changes)?;
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
            let rows = self.projection.read(node, &self.credentials).await?;
            match self.cursor.validate(window.revision).await {
                Err(SourceError::Changed) => continue,
                result => break (rows, result?.revision.0),
            }
        };
        let previous = std::mem::replace(&mut self.projection.rows, rows);
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let mut changed = Vec::new();
        let mut included = BTreeSet::new();
        for batch in pending {
            for source in &batch.changes {
                let id = source.state.entity_id;
                if previous.contains_key(&id) || self.projection.rows.contains_key(&id) {
                    changed.push(self.projection.change(node, &policy, source)?);
                    included.insert(id);
                }
            }
        }
        for (id, row) in &self.projection.rows {
            if !included.contains(id)
                && previous.get(id).is_none_or(|before| before.key != row.key || before.state.state.head != row.state.state.head)
            {
                changed.push(self.projection.change(node, &policy, &SourceChange { state: row.state.clone(), events: Vec::new() })?);
            }
        }
        for id in previous.keys() {
            if !included.contains(id) && !self.projection.rows.contains_key(id) {
                // Rank eviction still authors absence even though this entity
                // itself did not change during the validated read.
                changed.push(proto::QueryChange { entity_id: *id, member: None, delta: None });
            }
        }
        self.projection.tree.refresh_captured(self.projection.spec.clone(), self.projection.attested_rows(node)).await?;
        crate::node::state_sync::queue_live(node, self.peer, self.comparison, revision, changed)?;
        self.revision = revision;
        Ok(())
    }

    async fn refresh<SE, PA>(&mut self, node: &Node<SE, PA>) -> anyhow::Result<Snapshot>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let window = self.cursor.fence().await?;
        if self.projection.selection.limit.is_some() {
            let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
            let mut relevant = false;
            for batch in &window.batches {
                self.projection.check_catalog(batch)?;
                for change in &batch.changes {
                    relevant |= self.projection.rows.contains_key(&change.state.entity_id)
                        || self.projection.evaluate(&policy, change.state.clone())?.is_some();
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
