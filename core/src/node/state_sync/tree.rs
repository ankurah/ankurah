use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

use ankql::ast::{Resolved, Selection};
use ankurah_core_types::{KeySpec, PropertyPath, ValueType};
use ankurah_digest_tree::{
    leaf_address, CanonicalCodec, DigestTree, HeadHash, LeafChange, LeafData, NodePrefix, PropertyTypes, TreeTransaction,
};
use ankurah_proto::{self as proto, Attested, EntityId, EntityState, TreeSpanDigest, TreeSpec};

use crate::{
    entity::{Entity, TemporaryEntity},
    schema::catalog::CatalogManager,
    selection::filter::ValueLookup,
    value::Value,
};

type States = BTreeMap<EntityId, Arc<Attested<EntityState>>>;
type Members = BTreeMap<Vec<u8>, EntityId>;
type Cache = tokio::sync::Mutex<Option<Cached>>;

const OFFER_BYTES: u64 = 16 * 1024;
const OFFER_REGIONS: usize = 128;
const OFFER_REFINEMENTS: usize = 32;
const BULK_REFRESH_CHANGES: usize = 128;

/// Retained by an active query; Fetch uses a temporary instance. Sparse refreshes
/// update changed leaves in place; dense refreshes reconstruct the digest on demand.
#[derive(Default)]
pub(crate) struct QueryTree(Arc<Cache>);

struct Cached {
    spec: TreeSpec,
    tree: Option<DigestTree>,
    rows: BTreeMap<EntityId, CapturedRow>,
    members: Arc<Members>,
    generation: Arc<()>,
}

/// The key and serialized state are captured from the same entity version.
struct CapturedRow {
    key: Vec<u8>,
    state: Arc<Attested<EntityState>>,
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
    pub spec: TreeSpec,
    pub states: States,
    tree: Option<TreeTransaction>,
    members: Arc<Members>,
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

impl QueryTree {
    pub fn spec(selection: &Selection<Resolved>, catalog: &CatalogManager) -> TreeSpec {
        // Materialization preserves every Selection, including expressions and
        // JSON paths the tree's scalar range planner cannot address exactly.
        TreeSpec::for_selection(selection, &CatalogTypes(catalog)).unwrap_or_else(|error| {
            tracing::debug!(%error, "query uses entity-id digest keys");
            TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone())
        })
    }

    pub fn offered_spec(selection: &Selection<Resolved>, catalog: &CatalogManager, known: &proto::KnownState) -> anyhow::Result<TreeSpec> {
        let default = Self::spec(selection, catalog);
        let by_id = TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone());
        anyhow::ensure!(known.spec == default || known.spec == by_id, "unsupported comparison tree spec");
        Ok(known.spec.clone())
    }

    pub async fn snapshot(
        &self,
        selection: &Selection<Resolved>,
        catalog: &CatalogManager,
        entities: Vec<Entity>,
    ) -> anyhow::Result<Snapshot> {
        // Keep a valid layout even if catalog arrival now permits a richer one.
        let cached = self.0.lock().await.as_ref().map(|cached| cached.spec.clone());
        let spec = cached
            .filter(|spec| {
                TreeSpec::new(KeySpec::new(vec![]), spec.predicate().clone())
                    == TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone())
            })
            .unwrap_or_else(|| Self::spec(selection, catalog));
        let paths = Self::key_paths(&spec);
        let rows = entities
            .into_iter()
            .map(|entity| {
                let (state, values) = entity.to_entity_state_with_values(&paths)?;
                Ok((Attested::opt(state, None), values))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.refresh_captured(spec, rows).await
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
        rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>,
    ) -> anyhow::Result<Snapshot> {
        let rows = rows
            .into_iter()
            .map(|(state, values)| {
                let key = CanonicalCodec.encode_nullable(spec.key_spec(), &values)?;
                Ok((state, key))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.refresh_keyed(spec, rows).await
    }

    pub async fn refresh(&self, spec: TreeSpec, states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> {
        let rows = states
            .into_iter()
            .map(|state| {
                let key = key(&spec, &state)?;
                Ok((state, key))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.refresh_keyed(spec, rows).await
    }

    /// Apply immutable source rows without recapturing the rest of a standing query.
    /// All encoding and tree work succeeds before the cached row set is replaced.
    pub async fn apply_captured(
        &self,
        rows: Vec<(Attested<EntityState>, Vec<Option<Value>>)>,
        removed: Vec<EntityId>,
    ) -> anyhow::Result<()> {
        let mut cache = self.0.lock().await;
        let cached = cache.as_mut().ok_or_else(|| anyhow::anyhow!("query tree is not initialized"))?;
        let rows: BTreeMap<_, _> = rows
            .into_iter()
            .map(|(state, values)| {
                let key = CanonicalCodec.encode_nullable(cached.spec.key_spec(), &values)?;
                Ok((state.payload.entity_id, CapturedRow { key, state: Arc::new(state) }))
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
        for (id, after) in changed {
            if let Some(before) = cached.rows.remove(&id) {
                members.remove(&before.member().0);
            }
            if let Some(after) = after {
                let (address, id) = after.member();
                members.insert(address, id);
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
            tree: cached.tree.as_ref().map(DigestTree::begin),
            states: cached.rows.iter().map(|(id, row)| (*id, row.state.clone())).collect(),
            members: cached.members.clone(),
            cache: Arc::downgrade(&self.0),
            generation: cached.generation.clone(),
        }
    }

    async fn refresh_keyed(&self, spec: TreeSpec, rows: Vec<(Attested<EntityState>, Vec<u8>)>) -> anyhow::Result<Snapshot> {
        let rows: BTreeMap<_, _> =
            rows.into_iter().map(|(state, key)| (state.payload.entity_id, CapturedRow { key, state: Arc::new(state) })).collect();
        let mut cache = self.0.lock().await;
        if cache.as_ref().is_none_or(|cached| cached.spec != spec) {
            let members = rows.values().map(CapturedRow::member).collect();
            *cache = Some(Cached { spec: spec.clone(), tree: None, rows, members: Arc::new(members), generation: Arc::new(()) });
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
    let values = QueryTree::key_paths(spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
    Ok(CanonicalCodec.encode_nullable(spec.key_spec(), &values)?)
}

impl Snapshot {
    async fn build_tree(&self) -> anyhow::Result<DigestTree> {
        let tree = DigestTree::new(self.spec.clone());
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
            if let Some(cache) = self.cache.upgrade() {
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

    pub async fn head(&mut self, bounds: proto::TreeSpan) -> anyhow::Result<TreeSpanDigest> {
        let tree = self.tree().await?;
        let scope = tree.scope_ranges([bounds.clone()]);
        Ok(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: tree.digest(&scope).await? })
    }

    pub async fn offer(&mut self) -> anyhow::Result<proto::KnownState> {
        if let Some(inventory) = self.initial_inventory() {
            return Ok(inventory);
        }
        // A bounded disjoint frontier recognizes both equal inventories and
        // small changed regions without serial refinement exchanges.
        let root = self.head(super::whole()).await?;
        let mut frontier = vec![(root, true)];
        for _ in 0..OFFER_REFINEMENTS {
            let Some(index) = frontier
                .iter()
                .enumerate()
                .filter(|(_, (head, expandable))| *expandable && head.digest.count() > super::LEAF_PAGE as i64)
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
            if candidate.len() > OFFER_REGIONS {
                continue;
            }
            let inventory = proto::KnownState {
                spec: self.spec.clone(),
                regions: candidate.iter().map(|(head, _)| proto::SpanContent::Digest(head.clone())).collect(),
            };
            if bincode::serialized_size(&inventory)? <= OFFER_BYTES {
                frontier = candidate;
            }
        }
        Ok(proto::KnownState {
            spec: self.spec.clone(),
            regions: frontier.into_iter().map(|(head, _)| proto::SpanContent::Digest(head)).collect(),
        })
    }

    fn initial_inventory(&self) -> Option<proto::KnownState> {
        if self.members.len() > super::INITIAL_INVENTORY_LEAVES {
            return None;
        }
        let mut leaves = Vec::with_capacity(self.members.len());
        let mut minimum_bytes = 0usize;
        for (address, id) in self.members.iter() {
            let head = &self.states[id].payload.state.head;
            // Bound cloning before encoding: keys, entity IDs and event IDs
            // alone cannot exceed the request budget. The exact check below
            // also includes clock generations, lengths, bounds and TreeSpec.
            minimum_bytes = minimum_bytes.saturating_add(address.len()).saturating_add(head.len().saturating_mul(32));
            if minimum_bytes > super::INITIAL_INVENTORY_BYTES {
                return None;
            }
            leaves.push(proto::KeyedEntityHead {
                key: address[..address.len() - EntityId::BYTE_LEN].to_vec(),
                entity: proto::EntityHead { entity_id: *id, head: head.clone() },
            });
        }
        let inventory = proto::KnownState {
            spec: self.spec.clone(),
            regions: vec![proto::SpanContent::Heads(proto::TreeSpanInventory { bounds: super::whole(), leaves })],
        };
        bincode::serialized_size(&inventory).ok().filter(|size| *size <= super::INITIAL_INVENTORY_BYTES as u64).map(|_| inventory)
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
            self.members.range((bounds.start.clone(), bounds.end.clone())).take(super::LEAF_PAGE + 1).collect::<Vec<_>>()
        };
        let mut leaves = Vec::new();
        let mut bytes = 0;
        for (index, (address, id)) in rows.iter().take(super::LEAF_PAGE).enumerate() {
            let leaf = proto::KeyedEntityHead {
                key: address[..address.len() - EntityId::BYTE_LEN].to_vec(),
                entity: proto::EntityHead { entity_id: **id, head: self.states[*id].payload.state.head.clone() },
            };
            let end = if index + 1 == rows.len() { bounds.end.clone() } else { Bound::Included((*address).clone()) };
            let overhead = bincode::serialized_size(&proto::StateAnswer::Leaves(proto::TreeSpanInventory {
                bounds: proto::TreeSpan::new(bounds.start.clone(), end),
                leaves: Vec::new(),
            }))?;
            let size = bincode::serialized_size(&leaf)?;
            if overhead + bytes + size > super::PAGE_BYTES as u64 {
                anyhow::ensure!(!leaves.is_empty(), "single leaf inventory exceeds byte limit");
                break;
            }
            bytes += size;
            leaves.push(leaf);
        }
        let end = if leaves.len() == rows.len() {
            bounds.end
        } else {
            Bound::Included(leaf_address(&leaves.last().unwrap().key, leaves.last().unwrap().entity.entity_id))
        };
        let page = proto::TreeSpanInventory { bounds: proto::TreeSpan::new(bounds.start, end), leaves };
        anyhow::ensure!(
            bincode::serialized_size(&proto::StateAnswer::Leaves(page.clone()))? <= super::PAGE_BYTES as u64,
            "leaf inventory bounds exceed byte limit"
        );
        Ok(page)
    }

    pub async fn refine(&mut self, head: &TreeSpanDigest) -> anyhow::Result<Vec<TreeSpanDigest>> {
        Ok(self.tree().await?.refine(head).await?)
    }

    pub fn members(&self) -> Members { (*self.members).clone() }

    pub fn states_in(&self, bounds: &proto::TreeSpan, limit: usize) -> impl Iterator<Item = (&[u8], &Attested<EntityState>)> {
        self.members
            .range((bounds.start.clone(), bounds.end.clone()))
            .take(limit)
            .map(|(address, id)| (address.as_slice(), self.states[id].as_ref()))
    }
}

#[cfg(test)]
mod tests;
