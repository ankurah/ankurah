use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

use ankql::ast::{Resolved, Selection};
use ankurah_core_types::{KeySpec, PropertyPath, ValueType};
use ankurah_digest_tree::{
    leaf_address, CanonicalCodec, DigestTree, HeadHash, LeafChange, LeafData, NodePrefix, PropertyTypes, TreeTransaction,
};
use ankurah_proto::{self as proto, Attested, EntityId, EntityState, TreeHead, TreeSpec};

use crate::{entity::TemporaryEntity, schema::catalog::CatalogManager, selection::filter::ValueLookup};

type States = BTreeMap<EntityId, Arc<Attested<EntityState>>>;
type Members = BTreeMap<Vec<u8>, EntityId>;
type Cache = tokio::sync::Mutex<Option<Cached>>;

/// Retained by an active query; Fetch uses a temporary instance. Refresh changes
/// only leaves whose entity head or membership changed, preserving the tree layout.
#[derive(Default)]
pub(crate) struct QueryTree(Arc<Cache>);

struct Cached {
    spec: TreeSpec,
    tree: Option<DigestTree>,
    states: States,
    members: Arc<Members>,
    generation: Arc<()>,
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
        TreeSpec::for_selection(selection, &CatalogTypes(catalog))
            .unwrap_or_else(|_| TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone()))
    }

    pub fn offered_spec(
        selection: &Selection<Resolved>,
        catalog: &CatalogManager,
        known: &Option<proto::KnownState>,
    ) -> anyhow::Result<TreeSpec> {
        let default = Self::spec(selection, catalog);
        if let Some(known) = known {
            let by_id = TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone());
            anyhow::ensure!(known.spec == default || known.spec == by_id, "unsupported comparison tree spec");
            Ok(known.spec.clone())
        } else {
            Ok(default)
        }
    }

    pub async fn snapshot(
        &self,
        selection: &Selection<Resolved>,
        catalog: &CatalogManager,
        states: Vec<Attested<EntityState>>,
    ) -> anyhow::Result<Snapshot> {
        // Keep a valid layout even if catalog arrival now permits a richer one.
        let cached = self.0.lock().await.as_ref().map(|cached| cached.spec.clone());
        let spec = cached
            .filter(|spec| {
                TreeSpec::new(KeySpec::new(vec![]), spec.predicate().clone())
                    == TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone())
            })
            .unwrap_or_else(|| Self::spec(selection, catalog));
        self.refresh(spec, states).await
    }

    pub async fn refresh(&self, spec: TreeSpec, states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> {
        let states: States = states.into_iter().map(|state| (state.payload.entity_id, Arc::new(state))).collect();
        let mut cache = self.0.lock().await;
        if cache.as_ref().is_none_or(|cached| cached.spec != spec) {
            let members = states.values().map(|state| member(&spec, state)).collect::<anyhow::Result<Members>>()?;
            *cache = Some(Cached { spec: spec.clone(), tree: None, states, members: Arc::new(members), generation: Arc::new(()) });
        } else {
            let cached = cache.as_mut().unwrap();
            let mut tx = cached.tree.as_ref().map(DigestTree::begin);
            let mut members = cached.members.clone();
            let mut changed = false;
            for (id, before) in &cached.states {
                let after = states.get(id);
                if after.is_some_and(|after| after.payload.state.head == before.payload.state.head) {
                    continue;
                }
                changed = true;
                Arc::make_mut(&mut members).remove(&member(&spec, before)?.0);
                if let Some(after) = after {
                    let (address, id) = member(&spec, after)?;
                    Arc::make_mut(&mut members).insert(address, id);
                }
                if let Some(tx) = &mut tx {
                    tx.apply_change(LeafChange {
                        before: Some(leaf(&spec, before)?),
                        after: after.map(|state| leaf(&spec, state)).transpose()?,
                    })
                    .await?;
                }
            }
            for (id, after) in &states {
                if !cached.states.contains_key(id) {
                    changed = true;
                    let (address, id) = member(&spec, after)?;
                    Arc::make_mut(&mut members).insert(address, id);
                    if let Some(tx) = &mut tx {
                        tx.apply_change(LeafChange { before: None, after: Some(leaf(&spec, after)?) }).await?;
                    }
                }
            }
            if let Some(tx) = tx {
                tx.commit().await?;
            }
            cached.states = states;
            cached.members = members;
            if changed {
                cached.generation = Arc::new(());
            }
        }
        let cached = cache.as_ref().unwrap();
        Ok(Snapshot {
            spec,
            tree: cached.tree.as_ref().map(DigestTree::begin),
            states: cached.states.clone(),
            members: cached.members.clone(),
            cache: Arc::downgrade(&self.0),
            generation: cached.generation.clone(),
        })
    }
}

fn key(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<Vec<u8>> {
    if spec.key_spec().keyparts.is_empty() {
        return Ok(Vec::new());
    }
    let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
    let values = spec
        .key_spec()
        .keyparts
        .iter()
        .map(|part| {
            let mut path = PropertyPath::from(part.key);
            path.subpath = part.sub_path.clone().unwrap_or_default();
            entity.value_at(&path)
        })
        .collect::<Vec<_>>();
    Ok(CanonicalCodec.encode_nullable(spec.key_spec(), &values)?)
}

fn member(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<(Vec<u8>, EntityId)> {
    Ok((leaf_address(&key(spec, state)?, state.payload.entity_id), state.payload.entity_id))
}

fn leaf(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<LeafData> {
    Ok(LeafData {
        key: key(spec, state)?,
        entity_id: state.payload.entity_id,
        head: HeadHash::of(state.payload.state.head.ids().map(|id| id.clone().to_bytes())),
    })
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

    pub async fn head(&mut self, bounds: proto::AddressRange) -> anyhow::Result<TreeHead> {
        let tree = self.tree().await?;
        let scope = tree.scope_ranges([bounds.clone()]);
        Ok(TreeHead { prefix: NodePrefix::root(), bounds, digest: tree.digest(&scope).await? })
    }

    pub async fn offer(&mut self) -> anyhow::Result<proto::KnownState> {
        if self.members.len() <= super::LEAF_PAGE {
            let inventory =
                proto::KnownState { spec: self.spec.clone(), regions: vec![proto::KnownRegion::Leaves(self.page(super::whole()).await?)] };
            if bincode::serialized_size(&inventory)? <= super::FRAME_BYTES as u64 {
                return Ok(inventory);
            }
        }
        Ok(proto::KnownState { spec: self.spec.clone(), regions: vec![proto::KnownRegion::Summary(self.head(super::whole()).await?)] })
    }

    pub async fn page(&mut self, bounds: proto::AddressRange) -> anyhow::Result<proto::LeafPage> {
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
        let end = if rows.len() > super::LEAF_PAGE { std::ops::Bound::Included(rows[super::LEAF_PAGE - 1].0.clone()) } else { bounds.end };
        let leaves = rows
            .into_iter()
            .take(super::LEAF_PAGE)
            .map(|(address, id)| proto::KnownLeaf {
                key: address[..address.len() - EntityId::BYTE_LEN].to_vec(),
                entity: proto::KnownEntity { entity_id: *id, head: self.states[id].payload.state.head.clone() },
            })
            .collect();
        Ok(proto::LeafPage { bounds: proto::AddressRange::new(bounds.start, end), leaves })
    }

    pub async fn refine(&mut self, head: &TreeHead) -> anyhow::Result<Vec<TreeHead>> { Ok(self.tree().await?.refine(head).await?) }

    pub fn members(&self) -> Members { (*self.members).clone() }
}

#[cfg(test)]
mod tests;
