//! A resident's maintained digest tree and the frozen snapshots read from it.
mod snapshot;
mod spans;
mod spec;

pub(crate) use snapshot::{Snapshot, UnsupportedTreeSelection};
pub(crate) use spec::{accept_peer_spec, key_paths, request_for, serves, spec_for};

use std::{collections::BTreeMap, sync::Arc};

use ankurah_digest_tree::{leaf_address, CanonicalCodec, DigestTree, HeadHash, LeafChange, LeafInput, TreeError};
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

/// A resident's index under one fixed TreeSpec: each admitted entity's key and
/// Clock, and the digest tree over them, changed together by every commit its
/// maintenance subscription applies.
pub(crate) struct QueryTree {
    tree: DigestTree,
    entries: Arc<Entries>,
    members: Arc<Members>,
    missing_counts: Vec<usize>,
}

impl QueryTree {
    /// A tree under `spec` holding `entries`.
    pub(crate) async fn build(spec: TreeSpec, entries: Vec<TreeEntry>) -> anyhow::Result<Self> {
        let entries = checked_entries(&spec, entries)?;
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

    /// Make the tree hold exactly `entries`, changing only the entries that differ.
    pub(crate) async fn replace(&mut self, entries: Vec<TreeEntry>) -> anyhow::Result<()> {
        let entries = checked_entries(self.tree.spec(), entries)?;
        let mut changes = BTreeMap::new();
        for (id, before) in self.entries.iter() {
            let after = entries.get(id);
            if after != Some(before) {
                changes.insert(*id, after.cloned());
            }
        }
        for (id, after) in &entries {
            if !self.entries.contains_key(id) {
                changes.insert(*id, Some(after.clone()));
            }
        }
        self.apply_changes(changes).await
    }

    /// Store `entries`, replacing any earlier entry of the same entity, and
    /// remove the entities in `removed`.
    pub(crate) async fn apply(&mut self, entries: Vec<TreeEntry>, removed: Vec<EntityId>) -> anyhow::Result<()> {
        let entries = checked_entries(self.tree.spec(), entries)?;
        let mut changes = BTreeMap::new();
        for id in removed {
            if self.entries.contains_key(&id) {
                changes.insert(id, None);
            }
        }
        for (id, entry) in entries {
            if self.entries.get(&id) != Some(&entry) {
                changes.insert(id, Some(entry));
            } else {
                changes.remove(&id);
            }
        }
        self.apply_changes(changes).await
    }

    /// Conservative resident accounting for head/key metadata and digest-map rows.
    pub fn accounted_bytes(&self) -> usize {
        self.entries.values().fold(4096usize, |total, entry| {
            total
                .saturating_add(1024)
                .saturating_add(entry.key.capacity().saturating_mul(4))
                .saturating_add(entry.head.len().saturating_mul(36))
        })
    }

    /// Each admitted entity's entry.
    pub(crate) fn entries(&self) -> &Entries { &self.entries }

    /// A frozen view of the tree as it stands now; later changes do not reach it.
    pub(crate) fn snapshot(&self) -> Snapshot {
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

    async fn apply_changes(&mut self, changes: BTreeMap<EntityId, Option<TreeEntry>>) -> anyhow::Result<()> {
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
        // Commit the digests before changing the entries, so that a failed
        // commit leaves both as they were.
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
pub(crate) mod fixtures;
#[cfg(test)]
mod tests;
