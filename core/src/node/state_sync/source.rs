//! Complete or explicitly limited resident coverage, maintained by commit publications.
#[cfg(test)]
use crate::entity::TemporaryEntity;
use crate::{
    entity::CapturedEntity,
    node::{
        source_publications::{SourceBatch, SourceChange, SourceCursor, SourceError, MAX_PENDING_BATCHES, MAX_PENDING_BYTES},
        state_sync::{QueryTreeCache, Snapshot},
        Node,
    },
    policy::{ContextPolicy, PolicyAgent},
    selection::{
        filter::{evaluate_predicate, ValueLookup},
        ordering::compare_items,
    },
    session::ContextData,
    storage::StorageEngine,
    value::Value,
};
use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::CanonicalCodec;
use ankurah_proto::{self as proto, Attested};
use indexmap::IndexMap;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};
#[cfg(test)]
use tokio::sync::oneshot;

const MAX_READ_ATTEMPTS: usize = 8;

type Rows = IndexMap<proto::EntityId, Arc<Row>>;

/// Which selections the resident has actually captured. A limited result never
/// proves complete admission for its TreeSpec's predicate.
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
            Self::Limited(captured) => captured == selection,
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
            Self::Limited(captured) => {
                anyhow::ensure!(captured == selection, "limited resident does not cover this selection");
                Ok(snapshot)
            }
        }
    }
}
type ReadPolicy<'a, PA> = ContextPolicy<'a, PA, ankurah_signals::Mut<Vec<<PA as PolicyAgent>::ContextData>>>;

struct Row {
    state: Arc<CapturedEntity>,
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
    pub(super) coverage: Coverage,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    rows: Rows,
    // Retain denied candidates too: policy can change without an entity commit.
    candidates: Rows,
    candidate_bytes: usize,
    pub(super) tree: Arc<QueryTreeCache>,
}

/// Test helper for a complete resident with an independently selected query view.
#[cfg(test)]
pub(crate) async fn capture_query<SE, PA>(
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
    node.system.require_system_ready()?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let admission = Selection::from(policy.filter_predicate(spec.predicate().clone())?);
    let mut captured = capture_filtered_query(node, admission, spec, credentials, tree, Coverage::Complete).await?;
    captured.snapshot = captured.snapshot.select(&selection, &node.catalog).await?;
    Ok(captured)
}

pub(super) async fn capture_filtered_query<SE, PA>(
    node: &Node<SE, PA>,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    credentials: Vec<PA::ContextData>,
    tree: Arc<QueryTreeCache>,
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
    let (mut start, mut cursor) = node.source_publications.subscribe().await?;
    let mut projection =
        QueryProjection { coverage, selection, spec, rows: Rows::new(), candidates: Rows::new(), candidate_bytes: 0, tree };
    let mut attempts = 0;
    let window = loop {
        attempts += 1;
        anyhow::ensure!(attempts <= MAX_READ_ATTEMPTS, SourceError::Changed);
        if projection.selection.limit != Some(0) {
            projection.replace_candidates(projection.read(node).await?);
            projection.rows = projection.authorized_rows(&policy);
        }
        if matches!(projection.coverage, Coverage::Limited(_)) && projection.selection.limit != Some(0) {
            match cursor.validate_limited_read(start).await {
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
                if projection.selection.limit != Some(0) {
                    for change in &batch.changes {
                        projection.fold(&policy, change)?;
                    }
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

    fn candidate(&self, state: Arc<CapturedEntity>) -> anyhow::Result<Option<Arc<Row>>> {
        let entity = state.read()?;
        if !evaluate_predicate(&entity, &self.selection.predicate)? {
            return Ok(None);
        }
        let values = QueryTreeCache::key_paths(&self.spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
        let key = CanonicalCodec.encode_nullable(self.spec.key_spec(), &values)?;
        let bytes = (bincode::serialized_size(state.as_ref())? as usize)
            .saturating_mul(4)
            .saturating_add(key.len().saturating_mul(16))
            .saturating_add(2048)
            .saturating_add(state.lookup_bytes());
        Ok(Some(Arc::new(Row { state, values, key, bytes })))
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
            if let Some(row) = self.candidate(Arc::new(CapturedEntity::new(state.payload)))? {
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
            .filter(|(_, row)| policy.check_read_captured(&row.state).is_ok())
            .map(|(id, row)| (*id, row.clone()))
            .collect()
    }

    fn fold<PA: PolicyAgent>(&mut self, policy: &ReadPolicy<'_, PA>, change: &SourceChange) -> anyhow::Result<()> {
        let id = change.state.entity_id;
        if let Some(row) = self.candidate(change.state.clone())? {
            if policy.check_read_captured(&row.state).is_ok() {
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

    fn attested<SE, PA>(node: &Node<SE, PA>, row: &Row) -> (super::tree::SharedState, Vec<Option<Value>>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        (
            super::tree::SharedState {
                payload: row.state.clone(),
                attestations: proto::AttestationSet(node.policy_agent.attest_state(node, &row.state).into_iter().collect()),
            },
            row.values.clone(),
        )
    }

    fn attested_rows<SE, PA>(&self, node: &Node<SE, PA>) -> Vec<(super::tree::SharedState, Vec<Option<Value>>)>
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
        let delta = if policy.check_read_captured(&change.state).is_ok() {
            for event in &change.events {
                policy.check_read_captured_event(event, &change.state)?;
            }
            Some(proto::DeltaContent::StateAndEvents {
                state: Attested::opt(change.state.to_entity_state(), node.policy_agent.attest_state(node, &change.state)).into(),
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
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) members: BTreeMap<proto::EntityId, proto::QueryMember>,
}

impl Route {
    /// Membership belongs to the subscription, not to its shared resident tree.
    /// Unlimited queries only inspect changed rows. A limited query also finds
    /// unchanged rows entering its result when another member leaves the cut.
    async fn changes<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        projection: &QueryProjection,
        policy: &ReadPolicy<'_, PA>,
        sources: &[SourceChange],
    ) -> anyhow::Result<Vec<proto::QueryChange>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut updates = BTreeMap::new();
        if self.selection.limit.is_some() {
            let snapshot = projection.tree.current_snapshot().await?;
            let snapshot = projection.coverage.select(snapshot, &self.selection, &node.catalog).await?;
            updates.extend(self.members.keys().map(|id| (*id, None)));
            updates.extend(snapshot.query_members().into_iter().map(|(id, member)| (id, Some(member))));
        } else {
            for source in sources {
                let id = source.state.entity_id;
                let member = if let Some(row) = projection.rows.get(&id) {
                    let entity = row.state.read()?;
                    evaluate_predicate(&entity, &self.selection.predicate)?
                        .then(|| proto::QueryMember { key: row.key.clone(), head: row.state.state.head.clone() })
                } else {
                    None
                };
                updates.insert(id, member);
            }
        }

        let mut changes = Vec::new();
        let mut included = BTreeSet::new();
        for source in sources {
            let id = source.state.entity_id;
            let member = updates.get(&id).cloned().unwrap_or_else(|| self.members.get(&id).cloned());
            if self.members.contains_key(&id) || member.is_some() {
                let mut change = projection.change(node, policy, source)?;
                change.member = member;
                changes.push(change);
                included.insert(id);
            }
        }
        let mut entrants = Vec::new();
        let mut removed = Vec::new();
        for (id, member) in &updates {
            if included.contains(id) || self.members.get(id) == member.as_ref() {
                continue;
            }
            if member.is_some() {
                let row = projection.rows.get(id).ok_or_else(|| anyhow::anyhow!("query member missing from resident tree"))?;
                let source = SourceChange { state: row.state.clone(), events: Vec::new() };
                let mut change = projection.change(node, policy, &source)?;
                change.member = member.clone();
                entrants.push((change, row.state.read()?));
            } else {
                removed.push(proto::QueryChange { entity_id: *id, member: None, delta: None });
            }
        }
        // Commit changes keep their publication order. Unchanged replacements
        // follow query order, as they did when storage supplied the refill.
        if let Some(order) = &self.selection.order_by {
            entrants.sort_by(|(a_change, a), (b_change, b)| {
                compare_items(a, b, order).then_with(|| a_change.entity_id.cmp(&b_change.entity_id))
            });
        }
        changes.extend(entrants.into_iter().map(|(change, _)| change));
        changes.extend(removed);
        // Event authorization must succeed for the complete publication before
        // advancing this route's membership. Other routes remain independent.
        for (id, member) in updates {
            if let Some(member) = member {
                self.members.insert(id, member);
            } else {
                self.members.remove(&id);
            }
        }
        Ok(changes)
    }
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

    pub(super) fn check_authorization<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        // External policy changes have no source revision. Reopen rather than
        // authoring different membership at an already published revision.
        for (id, row) in &self.projection.candidates {
            anyhow::ensure!(policy.check_read_captured(&row.state).is_ok() == self.projection.rows.contains_key(id), SourceError::Changed);
        }
        Ok(())
    }

    pub(super) fn accounted_bytes(&self) -> usize {
        // Per-row accounting includes candidate and visible captures, map nodes,
        // canonical keys, and space for lazily constructed digest rows.
        4096usize.saturating_add(self.projection.candidate_bytes)
    }

    async fn deliver<SE, PA>(&mut self, node: &Node<SE, PA>, revision: u64, sources: Vec<SourceChange>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let mut retained = Vec::with_capacity(self.routes.len());
        for mut route in std::mem::take(&mut self.routes) {
            if route.owner.strong_count() == 0 {
                continue;
            }
            let result = async {
                anyhow::ensure!(
                    policy.filter_predicate(route.selection.predicate.clone())? == route.filtered.predicate,
                    SourceError::Changed
                );
                route.changes(node, &self.projection, &policy, &sources).await
            }
            .await;
            match result {
                Ok(changes) => {
                    if changes.is_empty() || super::queue_live(node, route.peer, route.stream, revision, changes).is_ok() {
                        retained.push(route);
                    }
                }
                Err(error) => {
                    // An invalid view or unreadable event fails this route, not
                    // the resident tree or an unrelated subscription using it.
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

    pub(super) async fn publish<SE, PA>(&mut self, node: &Node<SE, PA>, batch: Arc<SourceBatch>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        self.projection.check_catalog(&batch)?;
        self.routes.retain(|route| route.owner.strong_count() != 0);
        if matches!(self.projection.coverage, Coverage::Limited(_)) {
            if self.projection.selection.limit != Some(0) && self.relevant(&batch)? {
                return self.refill(node, vec![batch]).await;
            }
        } else {
            let changes = self.projection.apply(node, &self.credentials, &batch, !self.routes.is_empty()).await?;
            if !changes.is_empty() {
                self.deliver(node, batch.revision.0, changes).await;
            }
        }
        self.revision = batch.revision.0;
        Ok(())
    }

    fn relevant(&self, batch: &SourceBatch) -> anyhow::Result<bool> {
        for change in &batch.changes {
            if self.projection.candidates.contains_key(&change.state.entity_id)
                || self.projection.candidate(change.state.clone())?.is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A bounded resident must reread its rank cut when membership can change:
    /// a departed row's unchanged replacement may never have been captured.
    async fn refill<SE, PA>(&mut self, node: &Node<SE, PA>, mut pending: Vec<Arc<SourceBatch>>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
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
            match self.cursor.validate_limited_read(window.revision).await {
                Err(SourceError::Changed) => continue,
                result => break (rows, result?.revision.0),
            }
        };
        self.projection.replace_candidates(rows);
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        let rows = self.projection.authorized_rows(&policy);
        let previous = std::mem::replace(&mut self.projection.rows, rows);
        self.projection.tree.refresh_captured(self.projection.spec.clone(), self.projection.attested_rows(node)).await?;
        let sources = pending
            .into_iter()
            .flat_map(|batch| batch.changes.clone())
            .filter(|change| previous.contains_key(&change.state.entity_id) || self.projection.rows.contains_key(&change.state.entity_id))
            .collect();
        // Even a window containing no carried events can change ranked membership.
        self.deliver(node, revision, sources).await;
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
                if self.projection.selection.limit != Some(0) {
                    relevant |= self.relevant(batch)?;
                }
            }
            if relevant {
                // Refill may consume later batches too. Never overwrite its newer
                // revision or replay the rest of this already-drained window.
                self.refill(node, window.batches).await?;
            } else {
                self.revision = window.revision.0;
            }
        } else {
            for batch in window.batches {
                self.publish(node, batch).await?;
            }
            self.revision = window.revision.0;
        }
        self.projection.tree.current_snapshot().await
    }
}

#[cfg(test)]
mod tests;
