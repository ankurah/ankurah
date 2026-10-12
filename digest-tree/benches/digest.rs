//! What the digest primitives cost in each group, to compare ristretto255, the
//! default, with LtHash16, and ristretto255 with the figures the digest tree
//! design rests on: 5.0 µs to hash a leaf into ristretto255, 66 ns to update a
//! tree node whose digest is held uncompressed, and 5.1 µs to update a tree
//! node through the compressed encoding.
//!
//! `cargo bench -p ankurah-digest-tree --bench digest`. Each benchmark cycles
//! through 4,096 fixed inputs rather than repeating one. ristretto255's
//! benchmarks keep the names they had before LtHash16's joined them, so their
//! figures compare with earlier runs; LtHash16's carry its name in front.
//! Earlier runs reported ristretto255's leaf hash, the one renamed since, as
//! "leaf point (hash_to_ristretto255 of one leaf)", and its figures compare
//! all the same. Each group prints its row sizes before its benchmarks.

use std::hint::black_box;

use ankurah_core_types::EntityId;
use ankurah_digest_tree::digest::{Digest, DigestGroup, HeadHash, Leaf, LeafElement, LtHash16, Ristretto255};
use criterion::{criterion_group, criterion_main, Criterion};

const INPUTS: usize = 4096;

/// SplitMix64, so the inputs are fixed without a random number generator.
struct Bytes(u64);

impl Bytes {
    fn array<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        for chunk in out.chunks_mut(8) {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            chunk.copy_from_slice(&(z ^ (z >> 31)).to_be_bytes()[..chunk.len()]);
        }
        out
    }
}

/// Leaves of an index keyed by one 64-bit integer (an eight-byte canonical
/// key), each entity with a one-event head.
struct Leaves {
    keys: Vec<[u8; 8]>,
    entity_ids: Vec<EntityId>,
    event_ids: Vec<[u8; 32]>,
    heads: Vec<HeadHash>,
}

impl Leaves {
    fn new() -> Self {
        let mut bytes = Bytes(0x00a1_1ce0);
        let keys = (0..INPUTS).map(|_| bytes.array()).collect();
        let entity_ids = (0..INPUTS).map(|_| EntityId::from_bytes(bytes.array())).collect();
        let event_ids: Vec<[u8; 32]> = (0..INPUTS).map(|_| bytes.array()).collect();
        let heads = event_ids.iter().map(|id| HeadHash::of([*id])).collect();
        Self { keys, entity_ids, event_ids, heads }
    }

    fn leaf(&self, i: usize) -> Leaf<'_> { Leaf::new(&self.keys[i], self.entity_ids[i], self.heads[i]) }
}

fn primitives(c: &mut Criterion) {
    let leaves = Leaves::new();

    c.bench_function("head hash of a one-event head", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            HeadHash::of([black_box(leaves.event_ids[i])])
        })
    });

    group::<Ristretto255>(c, &leaves, "ristretto255", "", "leaf element (hash_to_ristretto255 of one leaf)");
    group::<LtHash16>(c, &leaves, "lthash16", "lthash16: ", "leaf element (BLAKE3 extendable output of one leaf)");
}

/// The benchmarks of the group `G`, called `label`, each named with `prefix`
/// in front.
fn group<G: DigestGroup>(c: &mut Criterion, leaves: &Leaves, label: &str, prefix: &str, leaf_hash: &str) {
    println!(
        "{label}: an element encodes in {} bytes, a digest row is {} bytes, a leaf element row {} bytes",
        G::ELEMENT_LEN,
        Digest::<G>::ROW_LEN,
        LeafElement::<G>::ROW_LEN
    );
    let name = |benchmark: &str| format!("{prefix}{benchmark}");
    let elements: Vec<LeafElement<G>> = (0..INPUTS).map(|i| leaves.leaf(i).element()).collect();
    // One change's difference to a tree node: the new leaf's element minus the old.
    let differences: Vec<Digest<G>> = (0..INPUTS).map(|i| Digest::from(elements[i]) - Digest::from(elements[(i + 1) % INPUTS])).collect();

    c.bench_function(&name(leaf_hash), |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(&black_box(leaves.leaf(i)).element::<G>());
        })
    });

    c.bench_function(&name("node update in memory (add one commit's difference)"), |b| {
        let mut nodes: Vec<Digest<G>> = elements.iter().map(|element| Digest::from(*element)).collect();
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            nodes[i] += black_box(differences[i]);
            black_box(&nodes[i]);
        })
    });

    // An in-memory codec benchmark: the rows are byte vectors in a Vec, so it
    // measures decoding, adding and encoding, and excludes reading and writing
    // the maps, building the change's difference, and batching a tree node's
    // updates into one decode and one encode.
    c.bench_function(&name("node update through its row (decode, add, encode)"), |b| {
        let mut rows: Vec<Vec<u8>> = elements.iter().map(|element| Digest::from(*element).to_row_bytes()).collect();
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            let node = Digest::<G>::from_row_bytes(black_box(&rows[i])).expect("a row this benchmark wrote");
            // A decoded count may be any i64, so the update is checked.
            rows[i] = node.checked_add(differences[i]).expect("the benchmark's counts stay at one").to_row_bytes();
        })
    });

    let rows: Vec<Vec<u8>> = elements.iter().map(|element| Digest::from(*element).to_row_bytes()).collect();
    c.bench_function(&name("digest row decode"), |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(&Digest::<G>::from_row_bytes(black_box(&rows[i])).expect("a row this benchmark wrote"));
        })
    });

    let digests: Vec<Digest<G>> = elements.iter().map(|element| Digest::from(*element)).collect();
    c.bench_function(&name("digest row encode"), |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(digests[i]).to_row_bytes()
        })
    });

    // Reading a tree node as a reader or compaction does: its base row and the
    // delta rows appended since, each decoded and summed.
    let deltas: Vec<Vec<u8>> = differences.iter().map(Digest::to_row_bytes).collect();
    for count in [8, 64] {
        c.bench_function(&name(&format!("node read through its rows (decode and sum a base and {count} deltas)")), |b| {
            let mut i = 0;
            b.iter(|| {
                i = (i + 1) % INPUTS;
                let mut node = Digest::<G>::from_row_bytes(black_box(&rows[i])).expect("a row this benchmark wrote");
                for d in 0..count {
                    let delta = Digest::from_row_bytes(black_box(&deltas[(i + d) % INPUTS])).expect("a row this benchmark wrote");
                    node = node.checked_add(delta).expect("the benchmark's counts stay at one");
                }
                black_box(&node);
            })
        });
    }
}

criterion_group!(benches, primitives);
criterion_main!(benches);
