//! The record cache against a model. Two nodes on one database, one with a
//! cache too small for a tree's node rows and one with room for them all,
//! change one tree in turn; they land some commits, long after and in any
//! order, and not others, drop some storage transactions, and hold reads
//! open across each other's commits, in an order a seeded generator picks.
//! An engine that doesn't keep a read's state across a commit to its tree has
//! its reads of the tree ended before each commit instead. Every row a read
//! is answered with is the row its snapshot's generation holds, and every
//! commit leaves the tree's digests as a model of its members says.

use crate::sync::conformance::fixtures::TestStores;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use ankurah_digest_tree::test_support::{member_digest, sum_digests};
use ankurah_digest_tree::{DigestGroup, Encode, ReadTrx, RowKey, Span, Store};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::fixtures::{assert_holds, by_number, cached, created, item, rows_now, Id, Item, Label, Number, Rows, Tree};
use super::TestDatabase;
use crate::storage::{StorageCommitResult, StorageEngine, StorageTransaction};
use crate::sync::{AppliedTrees, CacheSettings, StorageEngineSnapshot, TreeChange, TreeId};

/// One node: its engine, its storage adapter, and its handle on the tree.
struct Node<SE: StorageEngine + 'static, G: DigestGroup> {
    engine: Arc<SE>,
    trees: TestStores<SE>,
    tree: Tree<SE, G>,
}

/// One run of the model: the engine it reads the tree's rows through, the
/// two nodes, what the tree holds, and its rows at every generation.
struct Run<SE: StorageEngine + 'static, G: DigestGroup> {
    engine: Arc<SE>,
    nodes: [Node<SE, G>; 2],

    /// The members the tree holds, by id: each one's number and label.
    members: BTreeMap<u64, (i64, Vec<u8>)>,

    /// The tree's rows at each generation, the last the current one.
    history: Vec<Rows>,

    /// Every row key any generation held, of the rows the cache keeps.
    cached_keys: BTreeSet<Vec<u8>>,

    /// Every other row key any generation held.
    other_keys: BTreeSet<Vec<u8>>,

    /// Reads begun and still open, each with the generation it began at.
    reads: Vec<(StorageEngineSnapshot<SE, Number, Label, G>, usize)>,

    /// Whether the database keeps a read's state across a commit to its tree:
    /// where it doesn't, the reads end before each commit.
    keeps_a_reads_state_across_commits: bool,

    /// Commits whose storage transactions committed, each with its result,
    /// whose owner has yet to say they landed.
    landings: Vec<(AppliedTrees<SE, Number, Id, Label, G>, StorageCommitResult)>,

    next_id: u64,
    rng: SmallRng,
}

impl<SE: StorageEngine + 'static, G: DigestGroup> Run<SE, G> {
    /// A run from `seed`, on a tree of its own on `db`.
    async fn new(db: &impl TestDatabase<Engine = SE>, seed: u64) -> anyhow::Result<Self> {
        let engine = db.engine().await?;
        let creator = TestStores::new(Arc::clone(&engine), CacheSettings { bytes: 0 });
        let members: BTreeMap<u64, (i64, Vec<u8>)> = (0..400).map(|id| (id, (id as i64 * 7 % 1000, Vec::new()))).collect();

        let id = TreeId::new(format!("tree {seed}"));
        let tree: Tree<SE, G> =
            created(&creator, &engine, id.as_str(), members.iter().map(|(id, (number, _))| item!(*id, *number))).await?;
        let first = rows_now(&engine, &tree).await?;

        // One node has room for a few node rows of whichever group, and evicts
        // as it goes; the other keeps every row it meets.
        let node_row = first.iter().filter(|(key, _)| cached(key)).map(|(key, row)| key.len() + row.len()).max().unwrap();
        let caches = [CacheSettings { bytes: 6 * node_row }, CacheSettings { bytes: 1 << 24 }];

        let mut opened = Vec::new();
        for cache in caches {
            let engine = db.engine().await?;
            let trees = TestStores::new(Arc::clone(&engine), cache);
            let tree = trees.open(&id).await?;
            opened.push(Node { engine, trees, tree });
        }

        let mut run = Self {
            engine,
            nodes: opened.try_into().map_err(|_| anyhow::anyhow!("two nodes"))?,
            members,
            history: Vec::new(),
            cached_keys: BTreeSet::new(),
            other_keys: BTreeSet::new(),
            reads: Vec::new(),
            keeps_a_reads_state_across_commits: db.keeps_a_reads_state_across_commits(),
            landings: Vec::new(),
            next_id: 1000,
            rng: SmallRng::seed_from_u64(seed),
        };

        run.record(first);
        Ok(run)
    }

    /// One step, picked at random.
    async fn step(&mut self) -> anyhow::Result<()> {
        match self.rng.gen_range(0..100) {
            0..25 => self.commit().await,
            25..35 => self.land(),
            35..50 => self.begin_read().await,
            50..75 => self.read_keys().await,
            75..85 => self.end_read(),
            _ => self.read_digests().await,
        }
    }

    /// A commit by either node of a few random changes: its storage
    /// transaction committed, and its landing kept for later, or forgotten,
    /// as when its outcome isn't known; or its storage transaction dropped.
    async fn commit(&mut self) -> anyhow::Result<()> {
        let node = self.rng.gen_range(0..2);
        let mut members = self.members.clone();
        let mut changes = self.nodes[node].tree.transaction();

        for _ in 0..self.rng.gen_range(1..8) {
            let held: Vec<u64> = members.keys().copied().collect();
            let id = held[self.rng.gen_range(0..held.len())];
            let (number, label) = members[&id].clone();

            match self.rng.gen_range(0..4) {
                0 => {
                    let (new, number) = (self.next_id, self.rng.gen_range(0..1000));
                    self.next_id += 1;
                    changes.insert(item!(new, number))?;
                    members.insert(new, (number, Vec::new()));
                }

                1 => {
                    changes.remove(item!(id, number, label))?;
                    members.remove(&id);
                }

                2 => {
                    let moved = self.rng.gen_range(0..1000);
                    changes.update(item!(id, number, label.clone()), item!(id, moved, label.clone()))?;
                    members.insert(id, (moved, label));
                }

                _ => {
                    let relabelled = self.rng.gen_range(0..u32::MAX).to_be_bytes().to_vec();
                    changes.update(item!(id, number, label), item!(id, number, relabelled.clone()))?;
                    members.insert(id, (number, relabelled));
                }
            }
        }

        let mut trx = self.nodes[node].engine.transaction();
        let applied = self.nodes[node].trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
        assert!(applied.refused().is_empty() && applied.retired().is_empty(), "{:?}", applied.refused());

        if self.rng.gen_range(0..5) == 0 {
            drop(trx);
            return Ok(());
        }

        if !self.keeps_a_reads_state_across_commits {
            self.reads.clear();
        }

        let committed = trx.commit().await?.committed()?;
        if self.rng.gen_range(0..4) > 0 {
            self.landings.push((applied, committed));
        }

        self.members = members;
        let now = rows_now(&self.engine, &self.nodes[node].tree).await?;
        self.record(now);

        self.check_digest(node, i64::MIN, i64::MAX).await
    }

    /// One of the commits kept for landing, picked at random, lands.
    fn land(&mut self) -> anyhow::Result<()> {
        if !self.landings.is_empty() {
            let index = self.rng.gen_range(0..self.landings.len());
            let (applied, committed) = self.landings.swap_remove(index);
            applied.landed(&committed);
        }

        Ok(())
    }

    /// A read of the tree's rows begun on either node.
    async fn begin_read(&mut self) -> anyhow::Result<()> {
        let node = self.rng.gen_range(0..2);
        let read = self.nodes[node].tree.store().begin_read().await?;
        self.reads.push((read, self.history.len() - 1));

        Ok(())
    }

    /// Some rows read by key through an open read, most of them of the kind
    /// the cache keeps: each is the row its generation held, whatever the
    /// cache did since the read began.
    async fn read_keys(&mut self) -> anyhow::Result<()> {
        if self.reads.is_empty() {
            return Ok(());
        }

        let mut asked = Vec::new();
        for _ in 0..self.rng.gen_range(1..8) {
            let keys = if self.rng.gen_range(0..5) > 0 { &self.cached_keys } else { &self.other_keys };
            asked.push(keys.iter().nth(self.rng.gen_range(0..keys.len())).expect("a key of each kind").clone());
        }

        let keys = asked.iter().map(|key| RowKey::decode(key)).collect::<Result<Vec<_>, _>>()?;
        let index = self.rng.gen_range(0..self.reads.len());
        let (read, generation) = &mut self.reads[index];

        for (key, row) in asked.iter().zip(read.get_many(&keys).await?) {
            assert_eq!(
                row.map(|row| row.encode()).as_ref(),
                self.history[*generation].get(key),
                "generation {generation}'s row under {key:?}"
            );
        }

        Ok(())
    }

    fn end_read(&mut self) -> anyhow::Result<()> {
        if !self.reads.is_empty() {
            let index = self.rng.gen_range(0..self.reads.len());
            self.reads.swap_remove(index);
        }

        Ok(())
    }

    /// The digest of the members of a random range of numbers, read through
    /// either node.
    async fn read_digests(&mut self) -> anyhow::Result<()> {
        let (node, low) = (self.rng.gen_range(0..2), self.rng.gen_range(0..1000));
        let high = self.rng.gen_range(low..=1000);

        self.check_digest(node, low, high).await
    }

    /// The digest a read through `node` gives of the members numbered from
    /// `low` up to `high`, against the model's.
    async fn check_digest(&self, node: usize, low: i64, high: i64) -> anyhow::Result<()> {
        let within = self.members.iter().filter(|(_, (number, _))| (low..high).contains(number));
        let digests = within
            .map(|(id, (number, label))| member_digest::<_, _, G>(&by_number(), &item!(*id, *number, label.clone())))
            .collect::<Result<Vec<_>, _>>()?;

        let read = self.nodes[node].tree.select(low..high)?.span_digest(Span::all()).await?;
        assert_eq!(read.digest, sum_digests(digests)?, "node {node}'s digest of the members numbered {low} to {high}");
        Ok(())
    }

    /// Note `rows` as the next generation's.
    fn record(&mut self, rows: Rows) {
        for key in rows.keys() {
            match cached(key) {
                true => self.cached_keys.insert(key.clone()),
                false => self.other_keys.insert(key.clone()),
            };
        }

        self.history.push(rows);
    }

    /// The members the model holds.
    fn members(&self) -> Vec<Item> { self.members.iter().map(|(id, (number, label))| item!(*id, *number, label.clone())).collect() }
}

/// A few short runs of random steps, each from its own seed on a tree of its
/// own, after which both nodes' handles hold what the model does: enough to
/// show that an engine's snapshots, commits and change log behave as the
/// model assumes. Core's own tests run the model far longer, over the test
/// engine.
pub async fn reads_commits_landings_and_evictions_in_any_order_never_serve_a_row_of_another_generation<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    runs::<G, D>(db, 0..2, 40).await
}

/// Runs of `steps` random steps, one from each of `seeds`, each on a tree of
/// its own, after which both nodes' handles hold what the model does.
pub(crate) async fn runs<G: DigestGroup, D: TestDatabase>(db: &D, seeds: Range<u64>, steps: usize) -> anyhow::Result<()> {
    for seed in seeds {
        let mut run = Run::<D::Engine, G>::new(db, seed).await?;
        for _ in 0..steps {
            run.step().await?;
        }

        for node in &run.nodes {
            assert_holds(&node.tree, &run.members()).await?;
        }
    }

    Ok(())
}
