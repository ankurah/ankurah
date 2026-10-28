//! What a member's digest costs in each group, ristretto255, the default,
//! against LtHash16: hashing a member's leaf, adding a digest to a node's in
//! memory, a node's stored row decoded and encoded, and a node updated through
//! its stored row.
//!
//! `cargo bench -p ankurah-digest-tree --features test-support --bench
//! digest`. Each benchmark cycles through 4,096 fixed members rather than
//! repeating one, and carries its group's name in front.

use std::borrow::Cow;
use std::convert::Infallible;
use std::hint::black_box;

use ankurah_core_types::key_spec::{IndexKeyPart, KeySpec};
use ankurah_core_types::{Value, ValueType};
use ankurah_digest_tree::test_support::{member_digest, sum_digests};
use ankurah_digest_tree::{Digest, DigestGroup, Encode, LtHash16, Member, MemberId, Payload, Ristretto255, Row, StoreError};
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

/// A sixteen-byte id.
#[derive(Clone, PartialEq, Eq)]
struct Id([u8; 16]);

impl MemberId for Id {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(&self.0) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { bytes.try_into().ok().map(Id) }
}

/// A 32-byte payload, the size of a hash of a member's state.
#[derive(Clone)]
struct Fingerprint([u8; 32]);

impl Payload for Fingerprint {
    fn digest_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(&self.0) }
}

impl Encode for Fingerprint {
    fn encode(&self) -> Vec<u8> { self.0.to_vec() }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> { bytes.try_into().map(Fingerprint).map_err(StoreError::permanent) }
}

/// The property albums are keyed by: the year each came out.
#[derive(Clone)]
struct Year;

impl Encode for Year {
    fn encode(&self) -> Vec<u8> { Vec::new() }

    fn decode(_: &[u8]) -> Result<Self, StoreError> { Ok(Year) }
}

/// An album, keyed by the year it came out.
struct Album {
    id: Id,
    year: i64,
    fingerprint: Fingerprint,
}

impl Member<Year> for Album {
    type Id = Id;
    type Payload = Fingerprint;
    type KeyError = Infallible;

    fn id(&self) -> Id { self.id.clone() }

    /// An album's one value: its year.
    fn value(&self, _: &IndexKeyPart<Year>) -> Result<Option<Value>, Infallible> { Ok(Some(Value::I64(self.year))) }

    fn payload(&self) -> Fingerprint { self.fingerprint.clone() }
}

fn albums() -> Vec<Album> {
    let mut bytes = Bytes(0x00a1_1ce0);
    (0..INPUTS)
        .map(|_| Album { id: Id(bytes.array()), year: i64::from_be_bytes(bytes.array()), fingerprint: Fingerprint(bytes.array()) })
        .collect()
}

fn primitives(c: &mut Criterion) {
    let albums = albums();
    group::<Ristretto255>(c, &albums, "ristretto255");
    group::<LtHash16>(c, &albums, "lthash16");
}

/// The benchmarks of the group `G`, called `label`, each named with `label`
/// in front.
fn group<G: DigestGroup>(c: &mut Criterion, albums: &[Album], label: &str) {
    let name = |benchmark: &str| format!("{label}: {benchmark}");
    let years = KeySpec::new(vec![IndexKeyPart::asc(Year, ValueType::I64)]);
    let digests: Vec<Digest<G>> = albums.iter().map(|album| member_digest(&years, album).expect("an album's key")).collect();

    c.bench_function(&name("a member's digest (its key made, its leaf hashed)"), |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(member_digest::<_, _, G>(&years, black_box(&albums[i])).expect("an album's key"));
        })
    });

    c.bench_function(&name("a member's digest added to a node's, in memory"), |b| {
        let mut nodes = digests.clone();
        let mut i = 0;

        b.iter(|| {
            i = (i + 1) % INPUTS;
            nodes[i] = black_box(sum_digests([nodes[i], black_box(digests[(i + 1) % INPUTS])]).expect("counts far from the limit"));
        })
    });

    let rows: Vec<Vec<u8>> = digests.iter().map(|digest| Row::<Year, Fingerprint, G>::Node(*digest).encode()).collect();
    c.bench_function(&name("a stored node row decoded"), |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(Row::<Year, Fingerprint, G>::decode(black_box(&rows[i])).expect("a row this benchmark wrote"));
        })
    });

    c.bench_function(&name("a node row encoded"), |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % INPUTS;
            black_box(Row::<Year, Fingerprint, G>::Node(black_box(digests[i])).encode());
        })
    });

    c.bench_function(&name("a node updated through its stored row (decoded, added to, encoded)"), |b| {
        let mut rows = rows.clone();
        let mut i = 0;

        b.iter(|| {
            i = (i + 1) % INPUTS;

            let Row::Node(node) = Row::<Year, Fingerprint, G>::decode(&rows[i]).expect("a row this benchmark wrote") else {
                unreachable!("a node row")
            };

            let updated = sum_digests([node, digests[(i + 1) % INPUTS]]).expect("counts far from the limit");
            rows[i] = Row::<Year, Fingerprint, G>::Node(updated).encode();
        })
    });
}

criterion_group!(benches, primitives);
criterion_main!(benches);
