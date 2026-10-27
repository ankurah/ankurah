//! What a tree costs over the memory store: building it, per member, and
//! committing changes to it, per change, whether a change replaces a member's
//! payload under its key, moves it to another key, comes in a transaction of
//! a hundred, or in one large enough that the tree is laid out anew.
//!
//! `cargo bench -p ankurah-digest-tree --bench tree`.

use std::borrow::Cow;
use std::convert::Infallible;

use ankurah_digest_tree::{DigestTree, Member, MemberId, MemoryStore, Payload, Ristretto255, Row, RowKey};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use tokio::runtime::Runtime;

/// A sixteen-byte id, from a counter.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Id(u128);

impl MemberId for Id {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Owned(self.0.to_be_bytes().to_vec()) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { bytes.try_into().ok().map(u128::from_be_bytes).map(Id) }
}

/// A version, standing for a member's state.
#[derive(Clone, Copy)]
struct Version(u64);

impl Payload for Version {
    fn digest_bytes(&self) -> Cow<'_, [u8]> { Cow::Owned(self.0.to_be_bytes().to_vec()) }
}

/// An album, keyed by the year it came out.
#[derive(Clone, Copy)]
struct Album {
    id: Id,
    year: i64,
    version: Version,
}

/// The key spec the albums are keyed by: the year each came out.
#[derive(Clone)]
struct Year;

impl Member<Year> for Album {
    type Id = Id;
    type Payload = Version;
    type KeyError = Infallible;

    fn id(&self) -> Id { self.id }

    /// The year's big-endian bytes with the sign bit flipped, which sort as
    /// the years do.
    fn key(&self, _: &Year) -> Result<Vec<u8>, Infallible> { Ok((self.year as u64 ^ 1 << 63).to_be_bytes().to_vec()) }

    fn payload(&self) -> Version { self.version }
}

type Tree = DigestTree<Year, Id, Version, Ristretto255, MemoryStore<RowKey, Row<Year, Version, Ristretto255>>>;

/// The `n`th album: years spread so that buckets fill unevenly.
fn album(n: u64, version: u64) -> Album {
    Album { id: Id(u128::from(n).wrapping_mul(0x9e37_79b9_7f4a_7c15)), year: (n * 7919 % 1000) as i64, version: Version(version) }
}

fn build(runtime: &Runtime, members: u64) -> Tree {
    runtime.block_on(DigestTree::build(MemoryStore::new(), Year, (0..members).map(|n| album(n, 0)))).expect("distinct ids")
}

fn building(c: &mut Criterion) {
    let runtime = Runtime::new().expect("a runtime");
    let mut group = c.benchmark_group("build");

    for members in [1024, 16_384] {
        group.throughput(Throughput::Elements(members));
        group.bench_function(format!("{members} members"), |b| b.iter(|| build(&runtime, members)));
    }

    group.finish();
}

/// Commit, in one transaction, updates from each old version to its new one.
fn commit(runtime: &Runtime, tree: &Tree, changes: impl IntoIterator<Item = (Album, Album)>) {
    let mut trx = tree.transaction();
    for (old, new) in changes {
        trx.update(old, new).expect("a version the tree holds");
    }

    runtime.block_on(trx.commit()).expect("a commit in memory");
}

fn committing(c: &mut Criterion) {
    let runtime = Runtime::new().expect("a runtime");
    let members = 16_384;
    let tree = build(&runtime, members);

    // The version the tree holds of each member, which each change names.
    let mut held: Vec<Album> = (0..members).map(|n| album(n, 0)).collect();
    let mut group = c.benchmark_group("commit, to a tree of 16,384 members");
    group.throughput(Throughput::Elements(1));

    let mut version = 0;
    group.bench_function("a payload replaced under its key", |b| {
        b.iter(|| {
            version += 1;
            let n = (version % members) as usize;
            let new = Album { version: Version(version), ..held[n] };

            commit(&runtime, &tree, [(held[n], new)]);
            held[n] = new;
        })
    });

    group.bench_function("a member moved to another key", |b| {
        b.iter(|| {
            version += 1;
            let n = (version % members) as usize;

            // Each pass over the members moves each one to the other side of
            // its own key, so every commit moves a member.
            let mut moved = album(n as u64, version);
            moved.year += 1000 * ((version / members) as i64 % 2 * 2 - 1);

            commit(&runtime, &tree, [(held[n], moved)]);
            held[n] = moved;
        })
    });

    group.throughput(Throughput::Elements(100));

    group.bench_function("a transaction of 100 changes", |b| {
        b.iter(|| {
            version += 1;
            let changed: Vec<usize> = (0..100).map(|i| ((version * 100 + i) % members) as usize).collect();
            let news: Vec<Album> = changed.iter().map(|&n| Album { version: Version(version), ..held[n] }).collect();

            commit(&runtime, &tree, changed.iter().zip(&news).map(|(&n, &new)| (held[n], new)));
            changed.into_iter().zip(news).for_each(|(n, new)| held[n] = new);
        })
    });

    group.finish();

    // A transaction changing half of a tree, which lays the tree out anew.
    let mut group = c.benchmark_group("commit, to a tree of 1,024 members");
    group.throughput(Throughput::Elements(512));

    group.bench_function("a transaction of 512 changes", |b| {
        b.iter_batched(
            || build(&runtime, 1024),
            |tree| commit(&runtime, &tree, (0..512).map(|n| (album(n * 2, 0), album(n * 2, 1)))),
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group!(benches, building, committing);
criterion_main!(benches);
