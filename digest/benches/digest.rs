//! What the digest primitives cost, to compare with the figures the digest
//! tree design rests on: 5.0 µs to hash a leaf to the curve, 66 ns to update a
//! node whose digest is held uncompressed, and 5.1 µs to update a node through
//! the compressed encoding.
//!
//! `cargo bench -p ankurah-digest`. Every benchmark cycles through 4,096
//! distinct inputs so that no single input stays in cache.

use std::hint::black_box;

use ankurah_core_types::EntityId;
use ankurah_digest::{Digest, HeadHash, Leaf, LeafPoint, DIGEST_ROW_LEN};
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

fn primitives(c: &mut Criterion) {
    let mut bytes = Bytes(0x00a1_1ce0);
    // Leaves of an index keyed by one 64-bit integer (an eight-byte canonical
    // key), each entity with a one-event head.
    let keys: Vec<[u8; 8]> = (0..INPUTS).map(|_| bytes.array()).collect();
    let entity_ids: Vec<EntityId> = (0..INPUTS).map(|_| EntityId::from_bytes(bytes.array())).collect();
    let event_ids: Vec<[u8; 32]> = (0..INPUTS).map(|_| bytes.array()).collect();
    let heads: Vec<HeadHash> = event_ids.iter().map(|id| HeadHash::of([*id])).collect();
    let leaf = |i: usize| Leaf::new(&keys[i], entity_ids[i], heads[i]);
    let points: Vec<LeafPoint> = (0..INPUTS).map(|i| leaf(i).point()).collect();
    // One commit's difference to a node: the new leaf's point minus the old.
    let differences: Vec<Digest> = (0..INPUTS).map(|i| Digest::from(points[i]) - Digest::from(points[(i + 1) % INPUTS])).collect();

    c.bench_function("head hash of a one-event head", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            HeadHash::of([black_box(event_ids[i])])
        })
    });

    c.bench_function("leaf point (hash_to_ristretto255 of one leaf)", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(leaf(i)).point()
        })
    });

    c.bench_function("node update in memory (add one commit's difference)", |b| {
        let mut nodes: Vec<Digest> = points.iter().map(|point| Digest::from(*point)).collect();
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            nodes[i] += black_box(differences[i]);
            nodes[i]
        })
    });

    c.bench_function("node update through its row (decode, add, encode)", |b| {
        let mut rows: Vec<[u8; DIGEST_ROW_LEN]> = points.iter().map(|point| Digest::from(*point).to_row_bytes()).collect();
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            let node = Digest::from_row_bytes(black_box(&rows[i])).expect("a row this benchmark wrote") + differences[i];
            rows[i] = node.to_row_bytes();
            rows[i]
        })
    });

    let rows: Vec<[u8; DIGEST_ROW_LEN]> = points.iter().map(|point| Digest::from(*point).to_row_bytes()).collect();
    c.bench_function("digest row decode", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            Digest::from_row_bytes(black_box(&rows[i])).expect("a row this benchmark wrote")
        })
    });

    let digests: Vec<Digest> = points.iter().map(|point| Digest::from(*point)).collect();
    c.bench_function("digest row encode", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(digests[i]).to_row_bytes()
        })
    });
}

criterion_group!(benches, primitives);
criterion_main!(benches);
