//! A resident tree: each member's address and Clock, kept current on every
//! commit; the digest tree over them, built the first time anything reads a
//! digest and maintained on every commit after; and the frozen snapshots read
//! from both.
mod footprint;
mod snapshot;
mod spans;
mod spec;

pub(crate) use footprint::{CLOCK_ENTRY_BYTES, ID_ROW_BYTES};
pub(crate) use snapshot::{DigestRequest, DigestRequests, Snapshot};
pub(crate) use spec::{entity_id_spec, key_paths, request_for, serves, spec_for, tree_keys_rank_like_storage};

use self::footprint::Footprint;
use ankurah_core_types::Value;
use ankurah_digest_tree::{leaf_address, CanonicalCodec, DigestTree, HeadHash, LeafChange, LeafInput, TreeError};
use ankurah_proto::{Clock, EntityId, TreeSpec};
use std::{collections::BTreeMap, sync::Arc};

/// Each member's entry, by entity id.
type Entries = BTreeMap<EntityId, TreeEntry>;
/// Each member's entity id, by the address its entry holds.
type Members = BTreeMap<Arc<[u8]>, EntityId>;

/// A batch changing at least this many entries, and at least a quarter of the
/// tree, is folded in by rebuilding the tree nodes from the stored leaves.
const BULK_REFRESH_CHANGES: usize = 128;
/// Builds and bulk rebuilds over at least this many leaves run on a blocking
/// thread: over memory maps they never yield, and at about 5 µs to hash a
/// leaf, or 2.4 µs to decode a stored one, they hold a thread for milliseconds.
#[cfg(not(target_arch = "wasm32"))]
const BLOCKING_BUILD_LEAVES: usize = 1024;

/// What a resident tree keeps of one member: its leaf address, the key its values
/// encode followed by its entity id, and its Clock. Payloads stay in storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    address: Arc<[u8]>,
    pub head: Clock,
}

impl TreeEntry {
    /// Entity `id`'s entry at `head`, filed under the key that `values`, one per
    /// key part of `spec`, encode.
    pub(crate) fn encode(spec: &TreeSpec, id: EntityId, values: &[Option<Value>], head: Clock) -> anyhow::Result<Self> {
        let key = CanonicalCodec::encode_nullable(spec.key_spec(), values)?;
        Ok(Self { address: leaf_address(&key, id).into(), head })
    }

    /// The leaf address: the key followed by the entity id.
    pub(crate) fn address(&self) -> &[u8] { &self.address }

    /// The key the entity is filed under.
    pub(crate) fn key(&self) -> &[u8] { &self.address[..self.address.len() - EntityId::BYTE_LEN] }

    fn leaf(&self, entity_id: EntityId) -> LeafInput {
        LeafInput { key: self.key().to_vec(), entity_id, head: HeadHash::of(self.head.ids().map(|id| id.clone().to_bytes())) }
    }
}

/// A resident tree's index under one fixed TreeSpec: each admitted entity's
/// address and Clock, kept current by every commit its maintenance
/// subscription applies, and the digest tree over them once anything has read
/// a digest. After its first build the digest tree changes with every commit,
/// so it is never stale.
pub(crate) struct QueryTree {
    spec: Arc<TreeSpec>,
    entries: Arc<Entries>,
    members: Arc<Members>,
    footprint: Footprint,
    digests: Option<DigestTree>,
    /// How many times the entries have changed: a snapshot taken at another
    /// version cannot read this tree's digests.
    version: u64,
}

impl QueryTree {
    /// A tree under `spec` holding `entries`, with no digests until one is read.
    pub(crate) fn new(spec: TreeSpec, entries: Entries) -> Self {
        let members = entries.iter().map(|(id, entry)| (entry.address.clone(), *id)).collect();
        let mut footprint = Footprint::default();
        for entry in entries.values() {
            footprint.add(entry);
        }
        Self { spec: Arc::new(spec), entries: Arc::new(entries), members: Arc::new(members), footprint, digests: None, version: 0 }
    }

    /// Make the tree hold exactly `entries`, changing only the entries that differ.
    pub(crate) async fn replace(&mut self, entries: Entries) -> anyhow::Result<()> {
        let mut changes = BTreeMap::new();
        for (id, before) in self.entries.iter() {
            let after = entries.get(id);
            if after != Some(before) {
                changes.insert(*id, after.cloned());
            }
        }
        for (id, after) in entries {
            if !self.entries.contains_key(&id) {
                changes.insert(id, Some(after));
            }
        }
        self.apply_changes(changes).await
    }

    /// Give each entity in `changes` its new entry, or remove it for `None`.
    pub(crate) async fn apply(&mut self, changes: BTreeMap<EntityId, Option<TreeEntry>>) -> anyhow::Result<()> {
        let changes = changes.into_iter().filter(|(id, after)| self.entries.get(id) != after.as_ref()).collect();
        self.apply_changes(changes).await
    }

    /// Each admitted entity's entry.
    pub(crate) fn entries(&self) -> &Entries { &self.entries }

    /// How many times the entries have changed.
    pub(crate) fn version(&self) -> u64 { self.version }

    /// The bytes the LRU charges this tree: its digest rows only once they are built.
    pub(crate) fn accounted_bytes(&self) -> usize { self.footprint.bytes(self.digests.is_some()) }

    /// A frozen view of the tree as it stands now; later changes do not reach it.
    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot::new(
            self.spec.clone(),
            self.entries.clone(),
            self.members.clone(),
            self.digests.as_ref().map(DigestTree::begin),
            self.version,
        )
    }

    /// Answer a snapshot's request for this tree's digests: build them if
    /// nothing has read a digest yet, and hand back a read view of them,
    /// unless the entries have changed since the snapshot.
    pub(crate) async fn answer(&mut self, request: DigestRequest) -> anyhow::Result<()> {
        let digests = if request.version == self.version { Some(self.digests().await?.begin()) } else { None };
        request.answer(digests);
        Ok(())
    }

    async fn digests(&mut self) -> anyhow::Result<&DigestTree> {
        let tree = match self.digests.take() {
            Some(tree) => tree,
            None => build_tree_over(&self.spec, &self.entries).await?,
        };
        Ok(self.digests.insert(tree))
    }

    async fn apply_changes(&mut self, changes: BTreeMap<EntityId, Option<TreeEntry>>) -> anyhow::Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        if let Some(tree) = &self.digests {
            let entries = &self.entries;
            let leaf_changes = changes.iter().map(|(id, after)| LeafChange {
                before: entries.get(id).map(|entry| entry.leaf(*id)),
                after: after.as_ref().map(|entry| entry.leaf(*id)),
            });
            let mut tx = tree.begin();
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
        }
        let entries = Arc::make_mut(&mut self.entries);
        let members = Arc::make_mut(&mut self.members);
        for (id, after) in changes {
            if let Some(before) = entries.remove(&id) {
                members.remove(&before.address);
                self.footprint.remove(&before);
            }
            if let Some(after) = after {
                members.insert(after.address.clone(), id);
                self.footprint.add(&after);
                entries.insert(id, after);
            }
        }
        self.version += 1;
        Ok(())
    }
}

/// A digest tree under `spec` holding `entries`, built on a blocking thread
/// when it is large.
async fn build_tree_over(spec: &TreeSpec, entries: &Entries) -> anyhow::Result<DigestTree> {
    let leaves: Vec<LeafInput> = entries.iter().map(|(id, entry)| entry.leaf(*id)).collect();
    Ok(run_blocking_if_large(leaves.len(), build_tree(spec.clone(), leaves)).await??)
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
