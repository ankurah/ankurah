//! What a packed head inventory costs at each step of its life. `encode` builds one
//! from borrowed heads, as a responder does for a page and a requester for its offer.
//! `receive` decodes a `SpanContent::Heads` and validates the inventory against its
//! spec, as every peer does with what it receives. `traverse` reads every key, entity
//! ID and clock tip through the lending cursor. Each fixture's wire size and unshared
//! size print before the benchmarks.
//!
//! `cargo bench -p ankurah-proto --bench inventory`.

use std::{hint::black_box, ops::Bound};

use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
use ankurah_digest_tree::{leaf_address, CanonicalCodec, NodePrefix};
use ankurah_proto::{Clock, EntityId, EventId, PackedHeads, SpanContent, TreeSpan, TreeSpanInventory, TreeSpec};
use criterion::{criterion_group, criterion_main, Criterion};
use sha2::{Digest as _, Sha256};

/// One inventory to measure, with the owned heads it is encoded from.
struct Fixture {
    name: String,
    spec: TreeSpec,
    heads: Vec<(Vec<u8>, EntityId, Clock)>,
    inventory: TreeSpanInventory,
}

/// `count` heads with `tips` tips each, keyed as `kind` names; `clipped` bounds the
/// inventory by its first and last addresses instead of the whole tree.
fn fixture(kind: &str, count: usize, tips: usize, clipped: bool) -> Fixture {
    let value_type = match kind {
        "id" => None,
        "numeric" => Some(ValueType::I64),
        "random" => Some(ValueType::Binary),
        _ => Some(ValueType::String),
    };
    let key_spec =
        KeySpec::new(value_type.map(|ty| IndexKeyPart::asc(PropertyId::EntityId(EntityId::from_bytes([9; 32])), ty)).into_iter().collect());
    let mut heads = Vec::new();
    for n in 0..count {
        let seed: [u8; 32] = Sha256::digest(format!("prefix-inventory-{kind}-{n}").as_bytes()).into();
        let value = match kind {
            "id" => vec![],
            "numeric" => vec![Some(Value::I64(1_000_000 + n as i64))],
            "random" => vec![Some(Value::Binary(seed[..24].to_vec()))],
            "shared" => vec![Some(Value::String(format!("acme/projects/a-long-shared-key/{n:08}")))],
            "clusters" => vec![Some(Value::String(format!("{:02}/projects/cluster-shared/{n:08}", n % 16)))],
            "duplicate" => vec![Some(Value::String("acme/exactly-the-same-key".into()))],
            "wide" => vec![Some(Value::String(format!("{}{n:08}", "x".repeat(4096))))],
            "nullable" => vec![if n % 8 == 0 { None } else { Some(Value::String(format!("a\0{:02}/{}", n % 16, "z".repeat(n % 20)))) }],
            _ => panic!("unknown fixture"),
        };
        let key = CanonicalCodec::encode_nullable(&key_spec, &value).unwrap();
        let head = Clock::new(
            (0..tips)
                .map(|tip| {
                    let id: [u8; 32] = Sha256::digest(format!("event-{n}-{tip}").as_bytes()).into();
                    let generation = if tips > 1 { [1, 250, 251, 65536, u32::MAX][tip % 5] } else { 1 };
                    (generation, EventId::from_bytes(id))
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        heads.push((key, EntityId::from_bytes(seed), head));
    }
    heads.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let bounds = match (clipped, heads.first(), heads.last()) {
        (true, Some(first), Some(last)) => {
            TreeSpan::new(Bound::Included(leaf_address(&first.0, first.1)), Bound::Included(leaf_address(&last.0, last.1)))
        }
        _ => TreeSpan::under(&NodePrefix::root()),
    };
    let packed = PackedHeads::encode(heads.iter().map(|(key, id, head)| (key.as_slice(), *id, head))).unwrap();
    Fixture {
        name: format!("{kind}_{count}_tips{tips}{}", if clipped { "_clipped" } else { "" }),
        spec: TreeSpec::new(key_spec, ankql::ast::Predicate::True),
        heads,
        inventory: TreeSpanInventory { bounds, heads: packed },
    }
}

fn fixtures() -> Vec<Fixture> {
    let mut fixtures = Vec::new();
    for kind in ["id", "numeric", "random", "shared", "clusters", "duplicate"] {
        for count in [128, 2048] {
            fixtures.push(fixture(kind, count, 1, false));
        }
    }
    fixtures.extend([fixture("wide", 32, 1, false), fixture("shared", 8, 256, false), fixture("nullable", 128, 2, true)]);
    fixtures
}

fn encode(c: &mut Criterion, fixtures: &[Fixture]) {
    let mut group = c.benchmark_group("encode");
    for fixture in fixtures {
        let heads = || fixture.heads.iter().map(|(key, id, head)| (key.as_slice(), *id, head));
        group.bench_function(&fixture.name, |b| b.iter(|| PackedHeads::encode(black_box(heads())).unwrap()));
    }
    group.finish();
}

fn receive(c: &mut Criterion, fixtures: &[Fixture]) {
    let mut group = c.benchmark_group("receive");
    for fixture in fixtures {
        let bytes = bincode::serialize(&SpanContent::Heads(fixture.inventory.clone())).unwrap();
        group.bench_function(&fixture.name, |b| {
            b.iter(|| {
                let SpanContent::Heads(inventory) = bincode::deserialize(black_box(&bytes)).unwrap() else { unreachable!("heads") };
                inventory.validate(&fixture.spec).unwrap();
                inventory
            })
        });
    }
    group.finish();
}

fn traverse(c: &mut Criterion, fixtures: &[Fixture]) {
    let mut group = c.benchmark_group("traverse");
    for fixture in fixtures {
        group.bench_function(&fixture.name, |b| {
            b.iter(|| {
                let mut read = 0usize;
                let mut cursor = black_box(&fixture.inventory.heads).cursor();
                while let Some(head) = cursor.next() {
                    read += head.key.len() + usize::from(head.entity.entity_id.to_bytes()[0]);
                    for (generation, id) in head.entity.head.entries() {
                        read += generation as usize + usize::from(id.as_bytes()[0]);
                    }
                }
                read
            })
        });
    }
    group.finish();
}

fn inventory(c: &mut Criterion) {
    let fixtures = fixtures();
    for fixture in &fixtures {
        let heads = &fixture.inventory.heads;
        let wire = bincode::serialized_size(heads).unwrap();
        println!("{}: {} heads, {wire} wire bytes, {} unshared bytes", fixture.name, heads.len(), heads.unshared_bytes());
    }
    encode(c, &fixtures);
    receive(c, &fixtures);
    traverse(c, &fixtures);
}

criterion_group!(benches, inventory);
criterion_main!(benches);
