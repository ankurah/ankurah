//! Trees and snapshots built from entity states, for tests that need a tree
//! without a resident.
use super::{key_paths, QueryTree, Snapshot, TreeEntry};
use crate::{entity::TemporaryEntity, selection::filter::ValueLookup};
use ankurah_digest_tree::CanonicalCodec;
use ankurah_proto::{Attested, EntityState, TreeSpec};

impl QueryTree {
    /// A tree under `spec` holding `states`, keyed as a resident's storage read
    /// keys them.
    pub(crate) async fn of_states(spec: TreeSpec, states: Vec<Attested<EntityState>>) -> anyhow::Result<Self> {
        let entries = states.iter().map(|state| entry_of(&spec, state)).collect::<anyhow::Result<Vec<_>>>()?;
        Self::build(spec, entries).await
    }

    /// Make the tree hold exactly `states`, and snapshot it.
    pub(crate) async fn refresh_states(&mut self, states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> {
        let entries = states.iter().map(|state| entry_of(self.tree.spec(), state)).collect::<anyhow::Result<Vec<_>>>()?;
        self.replace(entries).await?;
        Ok(self.snapshot())
    }
}

/// A snapshot of a tree under `spec` holding `states`.
pub(crate) async fn snapshot_of(spec: TreeSpec, states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> {
    Ok(QueryTree::of_states(spec, states).await?.snapshot())
}

/// The entry a tree under `spec` keeps for `state`.
pub(crate) fn entry_of(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<TreeEntry> {
    let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
    let values = key_paths(spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
    Ok(TreeEntry {
        entity_id: state.payload.entity_id,
        key: CanonicalCodec::encode_nullable(spec.key_spec(), &values)?,
        head: state.payload.state.head.clone(),
    })
}
