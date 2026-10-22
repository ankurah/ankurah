use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{Arc, Weak},
};

use ankql::ast::{Resolved, Selection};
use ankurah_core_types::{KeySpec, PropertyPath, ValueType};
use ankurah_digest_tree::{
    leaf_address, CanonicalCodec, Digest, DigestTree, HeadHash, LeafChange, LeafData, NodePrefix, PropertyTypes, TreeMatch, TreeRequest,
    TreeTransaction,
};
use ankurah_proto::{self as proto, Attested, EntityId, EntityState, TreeSpanDigest, TreeSpec};

use crate::{
    entity::{CapturedEntity, TemporaryEntity},
    schema::catalog::CatalogManager,
    selection::{
        filter::{evaluate_predicate, ValueLookup},
        ordering::compare_items,
    },
    value::Value,
};

/// A local attestation of a shared immutable capture. Only emission needs an
/// owned protocol payload; the serialized representation is identical.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename = "Attested")]
pub(crate) struct SharedState {
    #[serde(serialize_with = "serialize_capture")]
    pub payload: Arc<CapturedEntity>,
    pub attestations: proto::AttestationSet,
}
impl SharedState {
    pub fn to_owned(&self) -> Attested<EntityState> {
        Attested { payload: self.payload.to_entity_state(), attestations: self.attestations.clone() }
    }
}
impl From<Attested<EntityState>> for SharedState {
    fn from(state: Attested<EntityState>) -> Self {
        Self { payload: Arc::new(CapturedEntity::new(state.payload)), attestations: state.attestations }
    }
}
fn serialize_capture<S: serde::Serializer>(state: &Arc<CapturedEntity>, serializer: S) -> Result<S::Ok, S::Error> {
    serde::Serialize::serialize(state.as_ref(), serializer)
}

type States = BTreeMap<EntityId, Arc<SharedState>>;
type Members = BTreeMap<Vec<u8>, EntityId>;
type Cache = tokio::sync::Mutex<Option<Cached>>;

const OFFER_BYTES: u64 = 16 * 1024;
const OFFER_SPANS: usize = 128;
const OFFER_REFINEMENTS: usize = 32;
const BULK_REFRESH_CHANGES: usize = 128;

/// One resident entry's captured rows and lazy digest. The node LRU owns its
/// identity and update feed; comparison snapshots freeze independent versions.
/// Sparse updates change leaves in place; dense updates defer digest construction.
#[derive(Default)]
pub(crate) struct QueryTreeCache(Arc<Cache>);

struct Cached {
    spec: TreeSpec,
    tree: Option<DigestTree>,
    rows: BTreeMap<EntityId, CapturedRow>,
    states: Arc<States>,
    missing_counts: Vec<usize>,
    members: Arc<Members>,
    generation: Arc<()>,
}

/// The key and serialized state are captured from the same entity version.
struct CapturedRow {
    key: Vec<u8>,
    state: Arc<SharedState>,
    missing_parts: Vec<usize>,
}

impl CapturedRow {
    fn member(&self) -> (Vec<u8>, EntityId) {
        let id = self.state.payload.entity_id;
        (leaf_address(&self.key, id), id)
    }

    fn leaf(&self) -> LeafData {
        LeafData {
            key: self.key.clone(),
            entity_id: self.state.payload.entity_id,
            head: HeadHash::of(self.state.payload.state.head.ids().map(|id| id.clone().to_bytes())),
        }
    }
}

pub(crate) struct Snapshot {
    // A comparison may use the peer's compatible spec without changing the
    // resident tree's admission or addressing identity.
    pub spec: TreeSpec,
    pub(crate) resident: Option<super::resident::ResidentPin>,
    physical_spec: TreeSpec,
    pub states: Arc<States>,
    tree: Option<TreeTransaction>,
    ranges: Option<Vec<proto::TreeSpan>>,
    members: Arc<Members>,
    missing_counts: Vec<usize>,
    cache: Weak<Cache>,
    generation: Arc<()>,
}

struct CatalogTypes<'a>(&'a CatalogManager);
impl PropertyTypes for CatalogTypes<'_> {
    fn value_type(&self, path: &PropertyPath) -> Option<ValueType> {
        if !path.subpath.is_empty() {
            return None;
        }
        match path.property_id() {
            proto::PropertyId::Id => Some(ValueType::EntityId),
            proto::PropertyId::System(property) => Some(crate::schema::resolver::system_property_value_type(property)),
            proto::PropertyId::EntityId(id) => {
                self.0.property_by_id(&id).ok().flatten().and_then(|property| ValueType::from_property_str(&property.value_type))
            }
        }
    }
}

impl QueryTreeCache {
    pub fn spec(selection: &Selection<Resolved>, catalog: &CatalogManager) -> TreeSpec {
        // Materialization preserves every Selection, including expressions and
        // JSON paths the tree's scalar range planner cannot address exactly.
        TreeSpec::for_selection(selection, &CatalogTypes(catalog)).unwrap_or_else(|error| {
            tracing::debug!(%error, "query uses entity-id digest keys");
            TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone())
        })
    }

    pub fn offered_spec(selection: &Selection<Resolved>, catalog: &CatalogManager, known: &proto::KnownState) -> anyhow::Result<TreeSpec> {
        anyhow::ensure!(Self::serves(&known.spec, selection, catalog), "unsupported comparison tree spec");
        Ok(known.spec.clone())
    }

    pub fn request(selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<TreeRequest> {
        let mut membership = selection.clone();
        if membership.limit.is_none() {
            // Unbounded query ordering does not affect digest membership and
            // must not require types for otherwise unused ordering paths.
            membership.order_by = None;
        }
        Ok(TreeRequest::for_selection(&membership, &CatalogTypes(catalog))?)
    }

    pub fn serves(spec: &TreeSpec, selection: &Selection<Resolved>, catalog: &CatalogManager) -> bool {
        let types = CatalogTypes(catalog);
        if Self::key_paths(spec).iter().zip(&spec.key_spec().keyparts).any(|(path, part)| types.value_type(path) != Some(part.value_type))
            || CanonicalCodec.encode_nullable(spec.key_spec(), &vec![None; spec.key_spec().keyparts.len()]).is_err()
        {
            return false;
        }
        // Unknown scalar types and opaque predicates can still use an exact
        // admission with entity-ID keys. Their limited views order captured
        // states with the same comparator used by storage.
        if spec.predicate() == &selection.predicate && (selection.limit.is_none() || spec.key_spec().keyparts.is_empty()) {
            return true;
        }
        Self::request(selection, catalog).ok().is_some_and(|request| spec.serves(&request).is_some())
    }

    pub fn key_paths(spec: &TreeSpec) -> Vec<PropertyPath> {
        spec.key_spec()
            .keyparts
            .iter()
            .map(|part| {
                let mut path = PropertyPath::from(part.key);
                path.subpath = part.sub_path.clone().unwrap_or_default();
                path
            })
            .collect()
    }

    /// Values must be captured with the state under the same entity read lock.
    pub async fn refresh_captured(
        &self,
        spec: TreeSpec,
        rows: Vec<(impl Into<SharedState>, Vec<Option<Value>>)>,
    ) -> anyhow::Result<Snapshot> {
        let rows = rows
            .into_iter()
            .map(|(state, values)| {
                let state = state.into();
                let key = CanonicalCodec.encode_nullable(spec.key_spec(), &values)?;
                let missing_parts = values.iter().enumerate().filter_map(|(index, value)| value.is_none().then_some(index)).collect();
                Ok((state, key, missing_parts))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.refresh_keyed(spec, rows).await
    }

    #[cfg(test)]
    pub async fn refresh(&self, spec: TreeSpec, states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> {
        let rows = states
            .into_iter()
            .map(|state| {
                let key = key(&spec, &state)?;
                let missing_parts = CanonicalCodec
                    .split(spec.key_spec(), &key)?
                    .iter()
                    .enumerate()
                    .filter_map(|(index, part)| (part[0] == 0).then_some(index))
                    .collect();
                Ok((state.into(), key, missing_parts))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.refresh_keyed(spec, rows).await
    }

    /// Apply immutable source rows without recapturing the rest of a standing query.
    /// All encoding and tree work succeeds before the cached row set is replaced.
    pub async fn apply_captured(
        &self,
        rows: Vec<(impl Into<SharedState>, Vec<Option<Value>>)>,
        removed: Vec<EntityId>,
    ) -> anyhow::Result<()> {
        let mut cache = self.0.lock().await;
        let cached = cache.as_mut().ok_or_else(|| anyhow::anyhow!("query tree is not initialized"))?;
        let rows: BTreeMap<_, _> = rows
            .into_iter()
            .map(|(state, values)| {
                let state = state.into();
                let key = CanonicalCodec.encode_nullable(cached.spec.key_spec(), &values)?;
                let missing_parts = values.iter().enumerate().filter_map(|(index, value)| value.is_none().then_some(index)).collect();
                Ok((state.payload.entity_id, CapturedRow { key, state: Arc::new(state), missing_parts }))
            })
            .collect::<anyhow::Result<_>>()?;
        let mut changed = BTreeMap::new();
        for id in removed {
            if cached.rows.contains_key(&id) {
                changed.insert(id, None);
            }
        }
        for (id, after) in rows {
            if cached
                .rows
                .get(&id)
                .is_none_or(|before| before.key != after.key || before.state.payload.state.head != after.state.payload.state.head)
            {
                changed.insert(id, Some(after));
            }
        }
        if changed.is_empty() {
            return Ok(());
        }
        let bulk = changed.len() >= BULK_REFRESH_CHANGES && changed.len() >= cached.rows.len().div_ceil(4);
        // Cancellation during an awaited tree mutation must leave a rebuildable
        // cache, rather than a digest whose rows still describe the old version.
        let prior_tree = cached.tree.take();
        let mut tx = if bulk { None } else { prior_tree.as_ref().map(DigestTree::begin) };
        if let Some(tx) = &mut tx {
            for (id, after) in &changed {
                tx.apply_change(LeafChange {
                    before: cached.rows.get(id).map(CapturedRow::leaf),
                    after: after.as_ref().map(CapturedRow::leaf),
                })
                .await?;
            }
        }
        if let Some(tx) = tx {
            tx.commit().await?;
        }
        let members = Arc::make_mut(&mut cached.members);
        let states = Arc::make_mut(&mut cached.states);
        for (id, after) in changed {
            if let Some(before) = cached.rows.remove(&id) {
                members.remove(&before.member().0);
                states.remove(&id);
                for index in before.missing_parts {
                    cached.missing_counts[index] -= 1;
                }
            }
            if let Some(after) = after {
                let (address, id) = after.member();
                members.insert(address, id);
                states.insert(id, after.state.clone());
                for index in &after.missing_parts {
                    cached.missing_counts[*index] += 1;
                }
                cached.rows.insert(id, after);
            }
        }
        cached.tree = if bulk { None } else { prior_tree };
        cached.generation = Arc::new(());
        Ok(())
    }

    pub async fn current_snapshot(&self) -> anyhow::Result<Snapshot> {
        let cache = self.0.lock().await;
        let cached = cache.as_ref().ok_or_else(|| anyhow::anyhow!("query tree is not initialized"))?;
        Ok(self.snapshot_cached(cached))
    }

    fn snapshot_cached(&self, cached: &Cached) -> Snapshot {
        Snapshot {
            spec: cached.spec.clone(),
            resident: None,
            physical_spec: cached.spec.clone(),
            tree: cached.tree.as_ref().map(DigestTree::begin),
            ranges: None,
            states: cached.states.clone(),
            members: cached.members.clone(),
            missing_counts: cached.missing_counts.clone(),
            cache: Arc::downgrade(&self.0),
            generation: cached.generation.clone(),
        }
    }

    async fn refresh_keyed(&self, spec: TreeSpec, rows: Vec<(SharedState, Vec<u8>, Vec<usize>)>) -> anyhow::Result<Snapshot> {
        let rows: BTreeMap<_, _> = rows
            .into_iter()
            .map(|(state, key, missing_parts)| (state.payload.entity_id, CapturedRow { key, state: Arc::new(state), missing_parts }))
            .collect();
        let mut missing_counts = vec![0; spec.key_spec().keyparts.len()];
        for row in rows.values() {
            for index in &row.missing_parts {
                missing_counts[*index] += 1;
            }
        }
        let mut cache = self.0.lock().await;
        if cache.as_ref().is_none_or(|cached| cached.spec != spec) {
            let members = rows.values().map(CapturedRow::member).collect();
            let states = Arc::new(rows.iter().map(|(id, row)| (*id, row.state.clone())).collect());
            *cache = Some(Cached {
                spec: spec.clone(),
                tree: None,
                rows,
                states,
                missing_counts,
                members: Arc::new(members),
                generation: Arc::new(()),
            });
        } else {
            let cached = cache.as_mut().unwrap();
            let changes = cached
                .rows
                .iter()
                .filter(|(id, before)| rows.get(id).is_none_or(|after| after.state.payload.state.head != before.state.payload.state.head))
                .count()
                + rows.keys().filter(|id| !cached.rows.contains_key(id)).count();
            // Individual topology maintenance wins for sparse changes. A
            // dense inventory fill is cheaper to construct once on demand.
            let bulk = changes >= BULK_REFRESH_CHANGES && changes >= cached.rows.len().div_ceil(4);
            let prior_tree = cached.tree.take();
            let mut tx = if bulk { None } else { prior_tree.as_ref().map(DigestTree::begin) };
            let mut members = cached.members.clone();
            let mut changed = false;
            for (id, before) in &cached.rows {
                let after = rows.get(id);
                if after.is_some_and(|after| after.state.payload.state.head == before.state.payload.state.head) {
                    continue;
                }
                changed = true;
                Arc::make_mut(&mut members).remove(&before.member().0);
                if let Some(after) = after {
                    let (address, id) = after.member();
                    Arc::make_mut(&mut members).insert(address, id);
                }
                if let Some(tx) = &mut tx {
                    tx.apply_change(LeafChange { before: Some(before.leaf()), after: after.map(CapturedRow::leaf) }).await?;
                }
            }
            for (id, after) in &rows {
                if !cached.rows.contains_key(id) {
                    changed = true;
                    let (address, id) = after.member();
                    Arc::make_mut(&mut members).insert(address, id);
                    if let Some(tx) = &mut tx {
                        tx.apply_change(LeafChange { before: None, after: Some(after.leaf()) }).await?;
                    }
                }
            }
            if let Some(tx) = tx {
                tx.commit().await?;
            }
            cached.tree = if bulk { None } else { prior_tree };
            cached.states = Arc::new(rows.iter().map(|(id, row)| (*id, row.state.clone())).collect());
            cached.missing_counts = missing_counts;
            cached.rows = rows;
            cached.members = members;
            if changed {
                cached.generation = Arc::new(());
            }
        }
        let cached = cache.as_ref().unwrap();
        Ok(self.snapshot_cached(cached))
    }
}

pub(super) fn key(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<Vec<u8>> {
    if spec.key_spec().keyparts.is_empty() {
        return Ok(Vec::new());
    }
    let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
    let values = QueryTreeCache::key_paths(spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
    Ok(CanonicalCodec.encode_nullable(spec.key_spec(), &values)?)
}

impl Snapshot {
    /// Select a query view without changing the maintained tree. The view's
    /// absent addresses remain empty when answering a peer's wider bounds.
    pub async fn select(mut self, selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<Self> {
        anyhow::ensure!(self.ranges.is_none(), "a query view cannot be selected again");
        if self.physical_spec.predicate() == &selection.predicate && selection.limit.is_none() {
            return Ok(self);
        }
        if selection.limit == Some(0) {
            self.restrict(Vec::new());
            return Ok(self);
        }
        let request = QueryTreeCache::request(selection, catalog).ok();
        if let Some(request) = request.filter(|request| self.physical_spec.serves(request).is_some()) {
            if selection.limit.is_none() && self.physical_spec.serves(&request) == Some(TreeMatch::Whole) {
                return Ok(self);
            }
            // Storage keeps missing values first in both directions. Reversing
            // canonical null-first keys would put them last for DESC.
            if selection.limit.is_none() || !self.has_descending_nulls(selection)? {
                return self.select_request(&request).await;
            }
        } else {
            anyhow::ensure!(
                self.physical_spec.predicate() == &selection.predicate && self.physical_spec.key_spec().keyparts.is_empty(),
                "unsupported query tree scope"
            );
        }
        self.select_ordered(selection).await
    }

    fn has_descending_nulls(&self, selection: &Selection<Resolved>) -> anyhow::Result<bool> {
        let descending = selection
            .order_by
            .iter()
            .flatten()
            .filter(|item| item.direction == ankql::ast::OrderDirection::Desc)
            .map(|item| &item.path)
            .collect::<Vec<_>>();
        if descending.is_empty() {
            return Ok(false);
        }
        let paths = QueryTreeCache::key_paths(&self.physical_spec);
        if descending.iter().all(|path| {
            **path == PropertyPath::id() || paths.iter().position(|key| key == *path).is_some_and(|index| self.missing_counts[index] == 0)
        }) {
            return Ok(false);
        }
        for state in self.states.values() {
            let entity = state.payload.read()?;
            if descending.iter().any(|path| entity.value_at(path).is_none()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn select_ordered(self, selection: &Selection<Resolved>) -> anyhow::Result<Self> {
        // Opaque ordering and null placement use the same comparator as storage,
        // over this captured version's already authorized states. No second read
        // can race the source revision or block unrelated views of the tree.
        let mut rows = Vec::with_capacity(self.states.len());
        for (id, state) in self.states.iter() {
            let entity = state.payload.read()?;
            if self.physical_spec.predicate() == &selection.predicate || evaluate_predicate(&entity, &selection.predicate).unwrap_or(false)
            {
                rows.push((*id, entity));
            }
        }
        if let Some(order) = &selection.order_by {
            rows.sort_by(|(a_id, a), (b_id, b)| compare_items(a, b, order).then_with(|| a_id.cmp(b_id)));
        }
        let limit = selection.limit.map_or(usize::MAX, |limit| usize::try_from(limit).unwrap_or(usize::MAX));
        let ids = rows.into_iter().take(limit).map(|(id, _)| id).collect();
        self.select_ids(&ids).await
    }

    async fn select_request(mut self, request: &TreeRequest) -> anyhow::Result<Self> {
        let ranges = self.physical_spec.scope_addresses(request, &self.members)?;
        self.restrict(ranges);
        Ok(self)
    }

    /// Fallback for selections whose ordering the scalar planner cannot express.
    /// The caller selects these identities by ordering this snapshot's captured states.
    async fn select_ids(mut self, ids: &BTreeSet<EntityId>) -> anyhow::Result<Self> {
        anyhow::ensure!(self.ranges.is_none(), "a query view cannot be selected again");
        anyhow::ensure!(ids.iter().all(|id| self.states.contains_key(id)), "selected identity is absent from captured tree");
        let ranges = self
            .members
            .iter()
            .filter(|(_, id)| ids.contains(id))
            .map(|(address, _)| proto::TreeSpan::new(Bound::Included(address.clone()), Bound::Included(address.clone())))
            .collect();
        self.restrict(ranges);
        Ok(self)
    }

    pub fn with_spec(mut self, spec: TreeSpec, selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.physical_spec.key_spec() == spec.key_spec()
                && QueryTreeCache::serves(&self.physical_spec, selection, catalog)
                && QueryTreeCache::serves(&spec, selection, catalog),
            "incompatible comparison tree spec"
        );
        self.spec = spec;
        Ok(self)
    }

    fn restrict(&mut self, ranges: Vec<proto::TreeSpan>) {
        let mut members = Members::new();
        for range in ranges.iter().filter(|range| !range.is_empty()) {
            members.extend(self.members.range((range.start.clone(), range.end.clone())).map(|(address, id)| (address.clone(), *id)));
        }
        self.states = Arc::new(members.values().map(|id| (*id, self.states[id].clone())).collect());
        self.members = Arc::new(members);
        self.ranges = Some(ranges);
    }

    async fn build_tree(&self) -> anyhow::Result<DigestTree> {
        let tree = DigestTree::new(self.physical_spec.clone());
        let mut tx = tree.begin();
        let leaves = self.members.iter().map(|(address, id)| LeafData {
            key: address[..address.len() - EntityId::BYTE_LEN].to_vec(),
            entity_id: *id,
            head: HeadHash::of(self.states[id].payload.state.head.ids().map(|id| id.clone().to_bytes())),
        });
        tx.build(leaves).await?;
        tx.commit().await?;
        Ok(tree)
    }

    async fn tree(&mut self) -> anyhow::Result<&mut TreeTransaction> {
        if self.tree.is_none() {
            // A narrowed snapshot builds only its selected rows. It must never
            // read or populate the complete resident's digest cache.
            if let Some(cache) = self.cache.upgrade().filter(|_| self.ranges.is_none()) {
                let mut cache = cache.lock().await;
                if let Some(cached) = cache.as_mut().filter(|cached| Arc::ptr_eq(&cached.generation, &self.generation)) {
                    if cached.tree.is_none() {
                        cached.tree = Some(self.build_tree().await?);
                    }
                    self.tree = cached.tree.as_ref().map(DigestTree::begin);
                }
            }
            // An older snapshot must never read a refreshed cache generation.
            if self.tree.is_none() {
                self.tree = Some(self.build_tree().await?.begin());
            }
        }
        Ok(self.tree.as_mut().unwrap())
    }

    pub async fn span_digest(&mut self, bounds: proto::TreeSpan) -> anyhow::Result<TreeSpanDigest> {
        let ranges = match &self.ranges {
            Some(ranges) => ranges.iter().filter_map(|range| intersect_span(range, &bounds)).collect::<Vec<_>>(),
            None => vec![bounds.clone()],
        };
        if ranges.is_empty() {
            return Ok(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: Digest::empty() });
        }
        let tree = self.tree().await?;
        let scope = tree.scope_ranges(ranges);
        Ok(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: tree.digest(&scope).await? })
    }

    pub async fn offer(&mut self) -> anyhow::Result<proto::KnownState> {
        if let Some(inventory) = self.initial_inventory() {
            return Ok(inventory);
        }
        // A bounded disjoint frontier recognizes both equal inventories and
        // small changed spans without serial refinement exchanges.
        let root = self.span_digest(super::whole()).await?;
        let mut frontier = vec![(root, true)];
        for _ in 0..OFFER_REFINEMENTS {
            let Some(index) = frontier
                .iter()
                .enumerate()
                .filter(|(_, (head, expandable))| *expandable && head.digest.count() > super::HEAD_PAGE as i64)
                .max_by_key(|(_, (head, _))| head.digest.count())
                .map(|(index, _)| index)
            else {
                break;
            };
            frontier[index].1 = false;
            let children = self.refine(&frontier[index].0).await?;
            if children.as_slice() == [frontier[index].0.clone()] {
                continue;
            }
            let mut candidate = frontier.clone();
            candidate.splice(index..=index, children.into_iter().map(|head| (head, true)));
            if candidate.len() > OFFER_SPANS {
                continue;
            }
            let inventory = proto::KnownState {
                spec: self.spec.clone(),
                spans: candidate.iter().map(|(head, _)| proto::SpanContent::Digest(head.clone())).collect(),
            };
            if bincode::serialized_size(&inventory)? <= OFFER_BYTES {
                frontier = candidate;
            }
        }
        Ok(proto::KnownState {
            spec: self.spec.clone(),
            spans: frontier.into_iter().map(|(head, _)| proto::SpanContent::Digest(head)).collect(),
        })
    }

    fn initial_inventory(&self) -> Option<proto::KnownState> {
        if self.members.len() > super::INITIAL_INVENTORY_HEADS {
            return None;
        }
        let mut minimum_bytes = 0usize;
        for (address, id) in self.members.iter() {
            let head = &self.states[id].payload.state.head;
            minimum_bytes = minimum_bytes.saturating_add(address.len()).saturating_add(head.len().saturating_mul(32));
            if minimum_bytes > super::INITIAL_INVENTORY_BYTES {
                return None;
            }
        }
        let heads = proto::PackedHeads::from_rows(
            self.members
                .iter()
                .map(|(address, id)| (address[..address.len() - EntityId::BYTE_LEN].as_ref(), *id, &self.states[id].payload.state.head)),
        );
        let inventory = proto::KnownState {
            spec: self.spec.clone(),
            spans: vec![proto::SpanContent::Heads(proto::TreeSpanInventory { bounds: super::whole(), heads })],
        };
        super::budget::offer_bytes(&inventory).ok().filter(|size| *size <= super::INITIAL_INVENTORY_BYTES as u64).map(|_| inventory)
    }

    pub fn page(&self, bounds: proto::TreeSpan) -> anyhow::Result<proto::TreeSpanInventory> {
        use std::ops::Bound;
        let empty = match (&bounds.start, &bounds.end) {
            (Bound::Included(start), Bound::Included(end)) => start > end,
            (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start >= end,
            _ => false,
        };
        let rows = if empty {
            Vec::new()
        } else {
            self.members.range((bounds.start.clone(), bounds.end.clone())).take(super::HEAD_PAGE + 1).collect::<Vec<_>>()
        };
        let mut count = 0;
        let mut bytes = 0;
        for (index, (address, id)) in rows.iter().take(super::HEAD_PAGE).enumerate() {
            let head = &self.states[*id].payload.state.head;
            let end = if index + 1 == rows.len() { bounds.end.clone() } else { Bound::Included((*address).clone()) };
            // Preserve the decoded-work budget even when repeated keys compress well.
            let overhead = 4 + bincode::serialized_size(&proto::TreeSpan::new(bounds.start.clone(), end))? + 8;
            let size = 8 + address.len() as u64 + 8 + 36 * head.len() as u64;
            if overhead + bytes + size > super::PAGE_BYTES as u64 {
                anyhow::ensure!(count > 0, "single head inventory exceeds byte limit");
                break;
            }
            bytes += size;
            count += 1;
        }
        let end = if count == rows.len() { bounds.end } else { Bound::Included(rows[count - 1].0.clone()) };
        let heads = proto::PackedHeads::from_rows(
            rows.iter()
                .take(count)
                .map(|(address, id)| (address[..address.len() - EntityId::BYTE_LEN].as_ref(), **id, &self.states[*id].payload.state.head)),
        );
        let page = proto::TreeSpanInventory { bounds: proto::TreeSpan::new(bounds.start, end), heads };
        anyhow::ensure!(
            bincode::serialized_size(&proto::StateAnswer::Heads(page.clone()))? <= super::PAGE_BYTES as u64,
            "head inventory bounds exceed byte limit"
        );
        Ok(page)
    }

    pub async fn refine(&mut self, head: &TreeSpanDigest) -> anyhow::Result<Vec<TreeSpanDigest>> {
        if self.ranges.as_ref().is_some_and(Vec::is_empty) {
            return Ok(vec![TreeSpanDigest { digest: Digest::empty(), ..head.clone() }]);
        }
        let mut children = self.tree().await?.refine(head).await?;
        if self.ranges.is_some() {
            for child in &mut children {
                child.digest = self.span_digest(child.bounds.clone()).await?.digest;
            }
        }
        Ok(children)
    }

    pub fn members(&self) -> Members { (*self.members).clone() }

    pub fn query_members(&self) -> BTreeMap<EntityId, proto::QueryMember> {
        self.members
            .iter()
            .map(|(address, id)| {
                (
                    *id,
                    proto::QueryMember {
                        key: address[..address.len() - EntityId::BYTE_LEN].to_vec(),
                        head: self.states[id].payload.state.head.clone(),
                    },
                )
            })
            .collect()
    }

    pub fn states_in(&self, bounds: &proto::TreeSpan, limit: usize) -> impl Iterator<Item = (&[u8], &SharedState)> {
        self.members
            .range((bounds.start.clone(), bounds.end.clone()))
            .take(limit)
            .map(|(address, id)| (address.as_slice(), self.states[id].as_ref()))
    }
}

fn intersect_span(left: &proto::TreeSpan, right: &proto::TreeSpan) -> Option<proto::TreeSpan> {
    let start = if left.cmp_start(right).is_ge() { left.start.clone() } else { right.start.clone() };
    let left_end = proto::TreeSpan::new(Bound::Unbounded, left.end.clone());
    let right_end = proto::TreeSpan::new(Bound::Unbounded, right.end.clone());
    let end = if left_end.is_subset_of(&right_end) { left.end.clone() } else { right.end.clone() };
    let result = proto::TreeSpan::new(start, end);
    (!result.is_empty()).then_some(result)
}

#[cfg(test)]
mod tests;
