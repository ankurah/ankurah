use std::{collections::BTreeMap, sync::Arc};

use ankql::ast::{Resolved, Selection};
use ankurah_core_types::{KeySpec, PropertyPath, ValueType};
use ankurah_digest_tree::{CanonicalCodec, DigestTree, HeadHash, LeafChange, LeafData, NodePrefix, PropertyTypes, TreeTransaction};
use ankurah_proto::{self as proto, Attested, EntityId, EntityState, TreeHead, TreeSpec};

use crate::{entity::TemporaryEntity, schema::catalog::CatalogManager, selection::filter::ValueLookup};

type States = BTreeMap<EntityId, Arc<Attested<EntityState>>>;

/// Retained by an active query; Fetch uses a temporary instance. Refresh changes
/// only leaves whose entity head or membership changed, preserving the tree layout.
#[derive(Default)]
pub(crate) struct QueryTree(tokio::sync::Mutex<Option<Cached>>);

struct Cached {
    tree: DigestTree,
    states: States,
}

pub(crate) struct Snapshot {
    pub spec: TreeSpec,
    pub tree: TreeTransaction,
    pub states: States,
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
        let cached = self.0.lock().await.as_ref().map(|cached| cached.tree.spec().clone());
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
        if cache.as_ref().is_none_or(|cached| cached.tree.spec() != &spec) {
            let tree = DigestTree::new(spec.clone());
            let mut tx = tree.begin();
            let leaves = states.values().map(|state| leaf(&spec, state)).collect::<anyhow::Result<Vec<_>>>()?;
            tx.build(leaves).await?;
            tx.commit().await?;
            *cache = Some(Cached { tree, states });
        } else {
            let cached = cache.as_mut().unwrap();
            let mut tx = cached.tree.begin();
            for (id, before) in &cached.states {
                let after = states.get(id);
                if after.is_some_and(|after| after.payload.state.head == before.payload.state.head) {
                    continue;
                }
                tx.apply_change(LeafChange {
                    before: Some(leaf(&spec, before)?),
                    after: after.map(|state| leaf(&spec, state)).transpose()?,
                })
                .await?;
            }
            for (id, after) in &states {
                if !cached.states.contains_key(id) {
                    tx.apply_change(LeafChange { before: None, after: Some(leaf(&spec, after)?) }).await?;
                }
            }
            tx.commit().await?;
            cached.states = states;
        }
        let cached = cache.as_ref().unwrap();
        Ok(Snapshot { spec, tree: cached.tree.begin(), states: cached.states.clone() })
    }
}

fn leaf(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<LeafData> {
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
    Ok(LeafData {
        key: CanonicalCodec.encode_nullable(spec.key_spec(), &values)?,
        entity_id: state.payload.entity_id,
        head: HeadHash::of(state.payload.state.head.ids().map(|id| id.clone().to_bytes())),
    })
}

impl Snapshot {
    pub async fn head(&mut self, bounds: proto::AddressRange) -> anyhow::Result<TreeHead> {
        let scope = self.tree.scope_ranges([bounds.clone()]);
        Ok(TreeHead { prefix: NodePrefix::root(), bounds, digest: self.tree.digest(&scope).await? })
    }

    pub async fn offer(&mut self) -> anyhow::Result<proto::KnownState> {
        Ok(proto::KnownState { spec: self.spec.clone(), heads: vec![self.head(super::whole()).await?] })
    }

    pub async fn page(&mut self, bounds: proto::AddressRange) -> anyhow::Result<proto::LeafPage> {
        let rows = self.tree.leaves(&bounds, super::LEAF_PAGE + 1).await?;
        let end = if rows.len() > super::LEAF_PAGE { std::ops::Bound::Included(rows[super::LEAF_PAGE - 1].address()) } else { bounds.end };
        let leaves = rows
            .into_iter()
            .take(super::LEAF_PAGE)
            .map(|row| proto::KnownLeaf {
                key: row.key,
                entity: proto::KnownEntity { entity_id: row.entity_id, head: self.states[&row.entity_id].payload.state.head.clone() },
            })
            .collect();
        Ok(proto::LeafPage { bounds: proto::AddressRange::new(bounds.start, end), leaves })
    }
}
