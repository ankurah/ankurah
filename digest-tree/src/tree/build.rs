//! Building a tree whole from its leaves, and rebuilding a subtree's tree
//! nodes from the leaves stored beneath it.

use std::collections::BTreeMap;
use std::ops::Bound;

use super::change::{LeafChange, LeafInput};
use super::TreeTransaction;
use crate::address::{leaf_address, CanonicalCodec, NodePrefix, TreeSpan};
use crate::digest::{Digest, DigestGroup};
use crate::error::TreeError;
use crate::store::{EncodedLeaf, MapAccess, MapName};

/// How many leaves a build hashes and encodes before writing them, so that
/// the encoded leaves waiting to be written stay few however large the build.
const PREPARATION_BATCH_LEAVES: usize = 1024;

/// How many leaves a batch holds before it is hashed on rayon's threads.
/// Smaller batches, a small build or the end of a large one, are hashed on
/// the build's own thread: at about 5 µs a leaf they take a few milliseconds,
/// which spreading over threads would shorten by little while taking the
/// threads from the application.
#[cfg(not(target_arch = "wasm32"))]
const PARALLEL_PREPARATION_MIN_LEAVES: usize = 1024;

/// Hash and encode each leaf filed at its address, a large batch on rayon's
/// global thread pool, which the application configures.
fn prepare_leaves<G: DigestGroup>(leaves: Vec<(Vec<u8>, LeafInput)>) -> Vec<EncodedLeaf<G>> {
    #[cfg(not(target_arch = "wasm32"))]
    if leaves.len() >= PARALLEL_PREPARATION_MIN_LEAVES {
        use rayon::prelude::*;

        return leaves.into_par_iter().map(|(address, leaf)| EncodedLeaf::new(address, leaf.into_row())).collect();
    }
    leaves.into_iter().map(|(address, leaf)| EncodedLeaf::new(address, leaf.into_row())).collect()
}

/// The most leaves a bucket holds: a bucket that grows past it splits. A
/// subtree merges back into one bucket only once it shrinks to half this, so
/// that a bucket near the threshold does not split and merge over and over.
pub const BUCKET_CAPACITY: usize = 64;

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// Replace the entire tree with these already-admitted leaves. Duplicate
    /// addresses are rejected. The replacement is atomic at commit; failure
    /// or cancellation prevents this transaction from committing.
    ///
    /// On native targets, a large build hashes and encodes its leaves on
    /// rayon's global thread pool, and writes them in order. Over the memory
    /// maps a build runs to its end without yielding, so an async caller runs
    /// a large one where a thread may block, such as Tokio's
    /// `spawn_blocking`.
    pub async fn build(&mut self, leaves: impl IntoIterator<Item = LeafInput>) -> Result<(), TreeError> {
        self.generation += 1;
        let mut guard = self.mutation_guard();
        let result = guard.replace_leaves(leaves).await;
        guard.finish(result)
    }

    async fn replace_leaves(&mut self, leaves: impl IntoIterator<Item = LeafInput>) -> Result<(), TreeError> {
        self.transaction.change_structure();
        // Preserve the first invalid key or duplicate in input order before
        // workers perform the independent hashing and row encoding.
        let mut admitted = BTreeMap::new();
        for leaf in leaves {
            CanonicalCodec::check(self.spec.key_spec(), &leaf.key)?;
            let address = leaf_address(&leaf.key, leaf.entity_id);
            if admitted.insert(address, leaf).is_some() {
                return Err(TreeError::DuplicateLeaf);
            }
        }
        for map in [MapName::Leaves, MapName::Lookup, MapName::Nodes] {
            let entries = self.transaction.scan(map, (Bound::Unbounded, Bound::Unbounded), usize::MAX).await?;
            for (key, value) in entries {
                self.transaction.delete_if(map, &key, &value).await?;
            }
        }
        let mut admitted = admitted.into_iter();
        let mut encoded = Vec::with_capacity(admitted.len());
        while admitted.len() != 0 {
            let batch = admitted.by_ref().take(PREPARATION_BATCH_LEAVES).collect();
            let prepared = prepare_leaves::<G>(batch);
            for leaf in &prepared {
                self.insert_leaf(leaf).await?;
            }
            encoded.extend(prepared);
        }
        // These leaves already hold the admitted leaf elements. Building the
        // summaries from them avoids reading and decoding our own writes.
        self.write_subtree(&NodePrefix::root(), &encoded).await
    }

    /// Apply a batch of leaf changes to the leaves and the entity lookup, each
    /// checked as [`TreeTransaction::apply_change`] checks it, then rebuild
    /// every tree node from the leaves stored: each leaf the batch did not
    /// write is decoded from its row rather than hashed again. When a batch
    /// changes a large share of a tree, this costs less than maintaining the
    /// tree nodes change by change. Failure or cancellation prevents this
    /// transaction from committing.
    pub async fn rebuild_with_changes(&mut self, changes: impl IntoIterator<Item = LeafChange>) -> Result<(), TreeError> {
        self.generation += 1;
        let mut guard = self.mutation_guard();
        let result = guard.change_leaves_and_rebuild(changes).await;
        guard.finish(result)
    }

    async fn change_leaves_and_rebuild(&mut self, changes: impl IntoIterator<Item = LeafChange>) -> Result<(), TreeError> {
        self.transaction.change_structure();
        // The new leaves are hashed together, as a build hashes its leaves,
        // before the first await, so that the caller's iterator is not held
        // across awaits.
        let changes: Vec<LeafChange> = changes.into_iter().collect();
        let after =
            changes.iter().filter_map(|change| change.after.clone()).map(|input| (leaf_address(&input.key, input.entity_id), input));
        let mut encoded = prepare_leaves::<G>(after.collect()).into_iter();
        let mut written = BTreeMap::new();
        for change in changes {
            let after = change.after.is_some().then(|| encoded.next().expect("a leaf was prepared for every change that files one"));
            let Some(resolved) = self.resolve(change.before, after).await? else { continue };
            self.write_leaves(&resolved).await?;
            if let Some(leaf) = resolved.before {
                written.remove(leaf.address());
            }
            if let Some(leaf) = resolved.after {
                written.insert(leaf.address().to_vec(), leaf);
            }
        }
        let leaves = self.all_leaves_reusing(&TreeSpan::all(), &mut written).await?;
        self.write_subtree(&NodePrefix::root(), &leaves).await
    }

    pub(super) async fn rebuild_subtree(&mut self, prefix: &NodePrefix) -> Result<(), TreeError> {
        let leaves = self.all_leaves(&TreeSpan::under(prefix)).await?;
        self.write_subtree(prefix, &leaves).await
    }

    async fn write_subtree(&mut self, prefix: &NodePrefix, leaves: &[EncodedLeaf<G>]) -> Result<(), TreeError> {
        // This fence is deliberately conservative: structural maintenance
        // retries after any intervening commit; ordinary leaf changes only
        // watch the topology and can append their deltas concurrently.
        self.transaction.change_structure();
        let start = prefix.to_bytes();
        let end = prefix.subtree_key_end();
        let entries = self
            .transaction
            .scan(MapName::Nodes, (Bound::Included(&start), end.as_deref().map_or(Bound::Unbounded, Bound::Excluded)), usize::MAX)
            .await?;
        for (key, value) in entries {
            self.transaction.delete_if(MapName::Nodes, &key, &value).await?;
        }

        enum Step {
            Build { prefix: NodePrefix, from: usize, to: usize },
            Join(NodePrefix),
        }
        let mut pending = vec![Step::Build { prefix: prefix.clone(), from: 0, to: leaves.len() }];
        let mut completed = Vec::<Digest<G>>::new();
        while let Some(step) = pending.pop() {
            let (prefix, from, to) = match step {
                Step::Build { prefix, from, to } => (prefix, from, to),
                Step::Join(prefix) => {
                    let right = completed.pop().expect("right subtree completed before join");
                    let left = completed.pop().expect("left subtree completed before join");
                    let digest = left + right;
                    self.insert_node_base(&prefix, digest).await?;
                    completed.push(digest);
                    continue;
                }
            };
            let rows = &leaves[from..to];
            if rows.len() <= BUCKET_CAPACITY {
                let digest = rows.iter().map(|leaf| Digest::from(leaf.row().element)).sum();
                self.insert_node_base(&prefix, digest).await?;
                completed.push(digest);
                continue;
            }
            let first = rows.first().expect("a split bucket holds leaves").address();
            let last = rows.last().expect("a split bucket holds leaves").address();
            let common = first.iter().zip(last).take_while(|(a, b)| a == b).count();
            if common == first.len().min(last.len()) {
                return Err(TreeError::InvalidKey("leaf addresses must be prefix-free"));
            }
            let depth = (common * 8) as u32 + (first[common] ^ last[common]).leading_zeros();
            let left = NodePrefix::of(first, depth + 1);
            let right = NodePrefix::of(last, depth + 1);
            let middle = from + rows.partition_point(|leaf| left.contains_address(leaf.address()));
            // Sum each terminal bucket once, then combine its completed
            // children instead of summing all leaves again at every ancestor.
            pending.push(Step::Join(prefix));
            pending.push(Step::Build { prefix: right, from: middle, to });
            pending.push(Step::Build { prefix: left, from, to: middle });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
