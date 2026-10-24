//! A resident's maintained digest tree and the frozen snapshots read from it.
mod snapshot;
mod spans;
mod spec;

pub(crate) use snapshot::{Snapshot, UnsupportedTreeSelection};

use std::{collections::BTreeMap, sync::Arc};

#[cfg(test)]
use crate::{entity::TemporaryEntity, selection::filter::ValueLookup};
use ankurah_digest_tree::{leaf_address, CanonicalCodec, DigestTree, HeadHash, LeafChange, LeafInput, TreeError};
#[cfg(test)]
use ankurah_proto::{Attested, EntityState};
use ankurah_proto::{Clock, EntityId, TreeSpec};

type Entries = BTreeMap<EntityId, TreeEntry>;
type Members = BTreeMap<Vec<u8>, EntityId>;

const BULK_REFRESH_CHANGES: usize = 128;
/// Builds and bulk rebuilds over at least this many leaves run on a blocking
/// thread: over memory maps they never yield, and at about 5 µs to hash a
/// leaf, or 2.4 µs to decode a stored one, they hold a thread for milliseconds.
#[cfg(not(target_arch = "wasm32"))]
const BLOCKING_BUILD_LEAVES: usize = 1024;

/// The entity metadata retained with a leaf. Payloads remain in entity/storage ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    pub entity_id: EntityId,
    pub key: Vec<u8>,
    pub head: Clock,
}

impl TreeEntry {
    fn address(&self) -> Vec<u8> { leaf_address(&self.key, self.entity_id) }

    fn leaf(&self) -> LeafInput {
        LeafInput { key: self.key.clone(), entity_id: self.entity_id, head: HeadHash::of(self.head.ids().map(|id| id.clone().to_bytes())) }
    }
}

/// A resident's fixed-spec index, updated by its reactor maintenance subscription.
/// Initialization and every subsequent update publish a complete digest and matching heads.
#[derive(Default)]
pub(crate) struct QueryTree(tokio::sync::Mutex<Option<Index>>);

struct Index {
    tree: DigestTree,
    entries: Arc<Entries>,
    members: Arc<Members>,
    missing_counts: Vec<usize>,
}

impl QueryTree {
    pub async fn refresh_entries(&self, spec: TreeSpec, entries: Vec<TreeEntry>) -> anyhow::Result<Snapshot> {
        let entries = checked_entries(&spec, entries)?;
        let mut current = self.0.lock().await;
        match current.as_mut() {
            Some(index) => {
                anyhow::ensure!(index.tree.spec() == &spec, "different TreeSpec requires a different tree");
                let mut changes = BTreeMap::new();
                for (id, before) in index.entries.iter() {
                    let after = entries.get(id);
                    if after != Some(before) {
                        changes.insert(*id, after.cloned());
                    }
                }
                for (id, after) in &entries {
                    if !index.entries.contains_key(id) {
                        changes.insert(*id, Some(after.clone()));
                    }
                }
                index.apply(changes).await?;
            }
            None => *current = Some(Index::build(spec, entries).await?),
        }
        Ok(current.as_ref().unwrap().snapshot())
    }

    pub async fn apply_entries(&self, entries: Vec<TreeEntry>, removed: Vec<EntityId>) -> anyhow::Result<Snapshot> {
        let mut current = self.0.lock().await;
        let index = current.as_mut().ok_or_else(|| anyhow::anyhow!("query tree is not initialized"))?;
        let entries = checked_entries(index.tree.spec(), entries)?;
        let mut changes = BTreeMap::new();
        for id in removed {
            if index.entries.contains_key(&id) {
                changes.insert(id, None);
            }
        }
        for (id, entry) in entries {
            if index.entries.get(&id) != Some(&entry) {
                changes.insert(id, Some(entry));
            } else {
                changes.remove(&id);
            }
        }
        index.apply(changes).await?;
        Ok(index.snapshot())
    }

    pub async fn current_snapshot(&self) -> anyhow::Result<Snapshot> {
        let current = self.0.lock().await;
        Ok(current.as_ref().ok_or_else(|| anyhow::anyhow!("query tree is not initialized"))?.snapshot())
    }

    #[cfg(test)]
    pub async fn refresh(&self, spec: TreeSpec, states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> {
        let entries = states.into_iter().map(|state| entry_from_state(&spec, &state)).collect::<anyhow::Result<Vec<_>>>()?;
        self.refresh_entries(spec, entries).await
    }
}

fn checked_entries(spec: &TreeSpec, entries: Vec<TreeEntry>) -> anyhow::Result<Entries> {
    let mut checked = Entries::new();
    for entry in entries {
        CanonicalCodec::split(spec.key_spec(), &entry.key)?;
        anyhow::ensure!(checked.insert(entry.entity_id, entry).is_none(), "duplicate tree entity");
    }
    Ok(checked)
}

fn missing_parts(spec: &TreeSpec, entry: &TreeEntry) -> Vec<usize> {
    CanonicalCodec::split(spec.key_spec(), &entry.key)
        .expect("tree entries have validated canonical keys")
        .iter()
        .enumerate()
        .filter_map(|(index, part)| (part[0] == 0).then_some(index))
        .collect()
}

impl Index {
    async fn build(spec: TreeSpec, entries: Entries) -> anyhow::Result<Self> {
        let leaves: Vec<LeafInput> = entries.values().map(TreeEntry::leaf).collect();
        let tree = run_blocking_if_large(leaves.len(), build_tree(spec.clone(), leaves)).await??;
        let mut missing_counts = vec![0; spec.key_spec().keyparts.len()];
        for entry in entries.values() {
            for index in missing_parts(&spec, entry) {
                missing_counts[index] += 1;
            }
        }
        let members = entries.values().map(|entry| (entry.address(), entry.entity_id)).collect();
        Ok(Self { tree, entries: Arc::new(entries), members: Arc::new(members), missing_counts })
    }

    async fn apply(&mut self, changes: BTreeMap<EntityId, Option<TreeEntry>>) -> anyhow::Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let entries = &self.entries;
        let leaf_changes = changes
            .iter()
            .map(|(id, after)| LeafChange { before: entries.get(id).map(TreeEntry::leaf), after: after.as_ref().map(TreeEntry::leaf) });
        let mut tx = self.tree.begin();
        if changes.len() >= BULK_REFRESH_CHANGES && changes.len() >= entries.len().div_ceil(4) {
            // A batch this large costs less to fold in by rebuilding every
            // tree node from the stored leaves than change by change.
            let leaf_changes: Vec<LeafChange> = leaf_changes.collect();
            let leaves = entries.len() + leaf_changes.len();
            tx = run_blocking_if_large(leaves, async move { tx.rebuild_with_changes(leaf_changes).await.map(|()| tx) }).await??;
        } else {
            tx.apply_changes(leaf_changes).await?;
        }
        // Memory commit has no suspension after publication; metadata is replaced
        // under the same owner lock before another reader can capture the index.
        tx.commit().await?;
        let entries = Arc::make_mut(&mut self.entries);
        let members = Arc::make_mut(&mut self.members);
        for (id, after) in changes {
            if let Some(before) = entries.remove(&id) {
                members.remove(&before.address());
                for index in missing_parts(self.tree.spec(), &before) {
                    self.missing_counts[index] -= 1;
                }
            }
            if let Some(after) = after {
                members.insert(after.address(), id);
                for index in missing_parts(self.tree.spec(), &after) {
                    self.missing_counts[index] += 1;
                }
                entries.insert(id, after);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            spec: self.tree.spec().clone(),
            resident: None,
            physical_spec: self.tree.spec().clone(),
            tree: self.tree.begin(),
            ranges: None,
            entries: self.entries.clone(),
            members: self.members.clone(),
            missing_counts: self.missing_counts.clone(),
        }
    }
}

/// Await `work`, an operation on a tree over memory maps that hashes or decodes
/// about `leaves` leaves, on a blocking thread when they number at least
/// [`BLOCKING_BUILD_LEAVES`], so that it holds no executor thread while it runs.
#[cfg(not(target_arch = "wasm32"))]
async fn run_blocking_if_large<R: Send + 'static>(
    leaves: usize,
    work: impl std::future::Future<Output = R> + Send + 'static,
) -> anyhow::Result<R> {
    if leaves < BLOCKING_BUILD_LEAVES {
        return Ok(work.await);
    }
    Ok(tokio::task::spawn_blocking(move || futures::executor::block_on(work)).await?)
}

/// Await `work` where it is: wasm32 has no thread to block.
#[cfg(target_arch = "wasm32")]
async fn run_blocking_if_large<R>(_leaves: usize, work: impl std::future::Future<Output = R>) -> anyhow::Result<R> { Ok(work.await) }

/// A tree defined by `spec` holding `leaves`.
async fn build_tree(spec: TreeSpec, leaves: Vec<LeafInput>) -> Result<DigestTree, TreeError> {
    let tree = DigestTree::new(spec);
    let mut tx = tree.begin();
    tx.build(leaves).await?;
    tx.commit().await?;
    Ok(tree)
}

#[cfg(test)]
fn entry_from_state(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<TreeEntry> {
    let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
    let values = QueryTree::key_paths(spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
    Ok(TreeEntry {
        entity_id: state.payload.entity_id,
        key: CanonicalCodec::encode_nullable(spec.key_spec(), &values)?,
        head: state.payload.state.head.clone(),
    })
}

#[cfg(test)]
mod tests;
