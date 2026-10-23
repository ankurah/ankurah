//! Building a tree whole from its leaves, and rebuilding a subtree's tree
//! nodes from the leaves stored beneath it.

use std::collections::BTreeMap;
use std::ops::Bound;

use super::change::LeafInput;
use super::TreeTransaction;
use crate::address::{CanonicalCodec, NodePrefix, TreeSpan};
use crate::digest::{Digest, DigestGroup};
use crate::error::TreeError;
use crate::store::{LeafRow, MapAccess, MapName, NodeRow, PreparedLeaf};

#[cfg(not(target_arch = "wasm32"))]
const PARALLEL_PREPARATION_MIN_LEAVES: usize = 1024;
const PREPARATION_BATCH_LEAVES: usize = 1024;

#[cfg(not(target_arch = "wasm32"))]
fn preparation_pool() -> Option<&'static rayon::ThreadPool> {
    use std::sync::OnceLock;

    static POOL: OnceLock<Option<rayon::ThreadPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism().map_or(1, |count| count.get().min(4));
        if workers == 1 {
            return None;
        }
        rayon::ThreadPoolBuilder::new().num_threads(workers).build().ok()
    })
    .as_ref()
}

fn prepare_leaves<G: DigestGroup>(leaves: Vec<LeafInput>) -> Vec<PreparedLeaf<G>> {
    #[cfg(not(target_arch = "wasm32"))]
    if leaves.len() >= PARALLEL_PREPARATION_MIN_LEAVES {
        use rayon::prelude::*;

        if let Some(pool) = preparation_pool() {
            return pool.install(|| leaves.into_par_iter().map(|leaf| PreparedLeaf::new(leaf.row::<G>())).collect());
        }
    }
    leaves.into_iter().map(|leaf| PreparedLeaf::new(leaf.row::<G>())).collect()
}

/// The target maximum number of leaves in a terminal bucket.
/// Merging at half this size avoids repeatedly splitting and merging near the threshold.
pub const BUCKET_CAPACITY: usize = 64;

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// Replace the entire tree with these already-admitted leaves. Duplicate
    /// addresses are rejected. The replacement is atomic at commit; failure
    /// or cancellation prevents this transaction from committing.
    ///
    /// On native targets, large batches prepare leaf hashes and encodings on
    /// a shared pool of up to four workers; transaction writes stay serial.
    /// Builds fall back to serial preparation when workers are unavailable.
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
            CanonicalCodec::split(self.spec.key_spec(), &leaf.key)?;
            let address = crate::leaf_address(&leaf.key, leaf.entity_id);
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
        let mut admitted = admitted.into_values();
        let mut rows = Vec::with_capacity(admitted.len());
        while admitted.len() != 0 {
            let batch = admitted.by_ref().take(PREPARATION_BATCH_LEAVES).collect();
            let prepared = prepare_leaves::<G>(batch);
            for leaf in &prepared {
                self.put_prepared_leaf(leaf, None).await?;
            }
            rows.extend(prepared.into_iter().map(PreparedLeaf::into_row));
        }
        // These rows already contain the admitted leaf elements. Building the
        // summaries from them avoids reading and decoding our own writes.
        self.write_subtree(&NodePrefix::root(), &rows).await
    }

    pub(super) async fn rebuild_subtree(&mut self, prefix: &NodePrefix) -> Result<(), TreeError> {
        let leaves = self.all_leaves(&TreeSpan::under(prefix)).await?;
        self.write_subtree(prefix, &leaves).await
    }

    async fn write_subtree(&mut self, prefix: &NodePrefix, leaves: &[LeafRow<G>]) -> Result<(), TreeError> {
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
                    self.put_node_base(&prefix, None, NodeRow { digest }).await?;
                    completed.push(digest);
                    continue;
                }
            };
            let rows = &leaves[from..to];
            if rows.len() <= BUCKET_CAPACITY {
                let digest = rows.iter().map(|row| row.element).map(Digest::from).sum();
                self.put_node_base(&prefix, None, NodeRow { digest }).await?;
                completed.push(digest);
                continue;
            }
            let first = rows.first().unwrap().address();
            let last = rows.last().unwrap().address();
            let common = first.iter().zip(&last).take_while(|(a, b)| a == b).count();
            if common == first.len().min(last.len()) {
                return Err(TreeError::InvalidKey("leaf addresses must be prefix-free"));
            }
            let depth = (common * 8) as u32 + (first[common] ^ last[common]).leading_zeros();
            let left = NodePrefix::of(&first, depth + 1);
            let right = NodePrefix::of(&last, depth + 1);
            let middle = from + rows.partition_point(|row| left.contains_address(&row.address()));
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
