//! Benchmark-only prototype; never selected by the production protocol.
//! Run: cargo bench -p ankurah-proto --bench prefix_inventory > results.json
//! PREFIX_CHECK_ONLY=1 runs correctness checks without timing. PREFIX_SAMPLES
//! (default 9) and PREFIX_BATCH_MS (default 10) control repeated measurements.

#[path = "prefix_inventory/codec.rs"]
mod codec;
#[path = "prefix_inventory/measure.rs"]
mod measure;

use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
use ankurah_digest_tree::{leaf_address, CanonicalCodec, NodePrefix};
use ankurah_proto::{Clock, EntityHead, EntityId, EventId, KeyedEntityHead, SpanContent, TreeSpan, TreeSpanInventory};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::{
    collections::hash_map::DefaultHasher,
    hash::Hasher,
    hint::black_box,
    ops::Bound,
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOCATOR: measure::Allocator = measure::Allocator;

struct Fixture {
    name: String,
    spec: KeySpec<PropertyId>,
    page: TreeSpanInventory,
}

fn fixture(kind: &str, count: usize, tips: usize, clipped: bool) -> Fixture {
    let value_type = match kind {
        "id" => None,
        "numeric" => Some(ValueType::I64),
        "random" => Some(ValueType::Binary),
        _ => Some(ValueType::String),
    };
    let spec =
        KeySpec::new(value_type.map(|ty| IndexKeyPart::asc(PropertyId::EntityId(EntityId::from_bytes([9; 32])), ty)).into_iter().collect());
    let mut leaves = Vec::new();
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
        let key = CanonicalCodec.encode_nullable(&spec, &value).unwrap();
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
        leaves.push(KeyedEntityHead { key, entity: EntityHead { entity_id: EntityId::from_bytes(seed), head } });
    }
    leaves.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.entity.entity_id.cmp(&b.entity.entity_id)));
    let bounds = if clipped && !leaves.is_empty() {
        TreeSpan::new(
            Bound::Included(leaf_address(&leaves[0].key, leaves[0].entity.entity_id)),
            Bound::Included(leaf_address(&leaves[count - 1].key, leaves[count - 1].entity.entity_id)),
        )
    } else {
        TreeSpan::under(&NodePrefix::root())
    };
    Fixture {
        name: format!("{kind}_{count}_tips{tips}{}", if clipped { "_clipped" } else { "" }),
        spec,
        page: TreeSpanInventory { bounds, leaves },
    }
}

fn fixtures() -> Vec<Fixture> {
    let mut result = vec![fixture("id", 0, 0, false), fixture("shared", 1, 1, false), fixture("id", 8, 1, false)];
    for kind in ["id", "numeric", "random", "shared", "clusters", "duplicate"] {
        for count in [128, 2048] {
            result.push(fixture(kind, count, 1, false));
        }
    }
    result.extend([
        fixture("wide", 32, 1, false),
        fixture("shared", 8, 256, false),
        fixture("nullable", 128, 2, true),
        fixture("shared", 128, 1, true),
        fixture("random", 1, 0, false),
    ]);
    result
}

// Common consumption endpoint: read every key, entity ID and normalized clock tip.
// Hashing is identical for owned and borrowed representations and is included in timing.
fn consume_owned(page: &TreeSpanInventory) -> u64 {
    let mut hash = DefaultHasher::new();
    for leaf in &page.leaves {
        hash.write(&leaf.key);
        hash.write(&leaf.entity.entity_id.to_bytes());
        for (generation, id) in leaf.entity.head.entries() {
            hash.write_u32(*generation);
            hash.write(id.as_bytes());
        }
    }
    hash.finish()
}

fn consume_borrowed(bytes: &[u8]) -> u64 {
    let mut hash = DefaultHasher::new();
    codec::visit(bytes, |key, id, clock| {
        hash.write(key);
        hash.write(id);
        clock.visit(|generation, id| {
            hash.write_u32(generation);
            hash.write(id);
        });
    })
    .unwrap();
    hash.finish()
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    CurrentOwned,
    CurrentOwnedControl,
    CurrentBorrowed,
    CurrentRetained,
    CurrentValidatedRetained,
    PrefixOwned,
    PrefixBorrowed,
    PrefixRetained,
    PrefixValidatedRetained,
}
impl Mode {
    const ALL: [Self; 9] = [
        Self::CurrentOwned,
        Self::CurrentOwnedControl,
        Self::CurrentBorrowed,
        Self::CurrentRetained,
        Self::CurrentValidatedRetained,
        Self::PrefixOwned,
        Self::PrefixBorrowed,
        Self::PrefixRetained,
        Self::PrefixValidatedRetained,
    ];
    fn prefix(self) -> bool {
        matches!(self, Self::PrefixOwned | Self::PrefixBorrowed | Self::PrefixRetained | Self::PrefixValidatedRetained)
    }
    fn run(self, input: &[u8]) -> u64 {
        let input = black_box(input);
        match self {
            Self::CurrentOwned => {
                let SpanContent::Heads(page) = bincode::deserialize(input).unwrap() else { panic!("inventory") };
                consume_owned(&page)
            }
            Self::CurrentOwnedControl | Self::PrefixOwned => consume_owned(&codec::owned(input).unwrap()),
            Self::CurrentBorrowed | Self::PrefixBorrowed => consume_borrowed(input),
            Self::CurrentRetained | Self::PrefixRetained => {
                // Conservative retained-buffer path: includes allocating/copying the input.
                // No claim that the existing connectors provide zero-copy ownership.
                let retained = input.to_vec();
                consume_borrowed(&retained)
            }
            Self::CurrentValidatedRetained | Self::PrefixValidatedRetained => {
                let retained = input.to_vec();
                // A conservative two-pass control: reject malformed bytes anywhere
                // in the inventory before letting a consumer observe the first row.
                codec::visit(&retained, |_, _, _| {}).unwrap();
                consume_borrowed(&retained)
            }
        }
    }
}

#[derive(Serialize)]
struct Measurement {
    mode: Mode,
    iterations_per_sample: usize,
    samples_ns: Vec<f64>,
    median_ns: f64,
    allocations: measure::Allocations,
}
#[derive(Serialize)]
struct Case {
    name: String,
    rows: usize,
    raw_key_bytes: usize,
    clock_tips: usize,
    current_bytes: usize,
    prefix_bytes: usize,
    choose_prefix: bool,
    measurements: Vec<Measurement>,
}

fn batch(mode: Mode, bytes: &[u8], iterations: usize) -> Duration {
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(mode.run(bytes));
    }
    start.elapsed()
}

fn main() {
    codec::malformed_checks();
    let check_only = std::env::var_os("PREFIX_CHECK_ONLY").is_some();
    let samples: usize = std::env::var("PREFIX_SAMPLES").unwrap_or("9".into()).parse().unwrap();
    let target_ms: u64 = std::env::var("PREFIX_BATCH_MS").unwrap_or("10".into()).parse().unwrap();
    assert!(samples >= 3 && target_ms > 0);
    let mut cases = Vec::new();
    for fixture in fixtures() {
        for leaf in &fixture.page.leaves {
            CanonicalCodec.split(&fixture.spec, &leaf.key).unwrap();
        }
        let current = bincode::serialize(&SpanContent::Heads(fixture.page.clone())).unwrap();
        assert_eq!(current, codec::encode(&fixture.page, false), "plain packed control must equal production bytes");
        let prefix = codec::encode(&fixture.page, true);
        codec::verify(&fixture.page, &current);
        codec::verify(&fixture.page, &prefix);
        let expected = consume_owned(&fixture.page);
        let mut measurements = Vec::new();
        for mode in Mode::ALL {
            let bytes = if mode.prefix() { &prefix } else { &current };
            assert_eq!(mode.run(bytes), expected, "{} {mode:?}", fixture.name);
            if check_only {
                continue;
            }
            for _ in 0..8 {
                black_box(mode.run(bytes));
            }
            let allocations = measure::allocations(|| {
                black_box(mode.run(bytes));
            });
            assert_eq!(
                allocations,
                measure::allocations(|| {
                    black_box(mode.run(bytes));
                })
            );
            let mut iterations = 1;
            while batch(mode, bytes, iterations) < Duration::from_millis(target_ms) && iterations < 1 << 22 {
                iterations *= 2;
            }
            measurements.push(Measurement { mode, iterations_per_sample: iterations, samples_ns: Vec::new(), median_ns: 0.0, allocations });
        }
        // Rotate mode order each round rather than timing all samples of one mode first.
        for round in 0..if check_only { 0 } else { samples } {
            for offset in 0..measurements.len() {
                let index = (offset + round) % measurements.len();
                let measured = &mut measurements[index];
                let bytes = if measured.mode.prefix() { &prefix } else { &current };
                let elapsed = batch(measured.mode, bytes, measured.iterations_per_sample);
                measured.samples_ns.push(elapsed.as_nanos() as f64 / measured.iterations_per_sample as f64);
            }
        }
        for measured in &mut measurements {
            let mut ordered = measured.samples_ns.clone();
            ordered.sort_by(f64::total_cmp);
            measured.median_ns = ordered[ordered.len() / 2];
        }
        cases.push(Case {
            name: fixture.name,
            rows: fixture.page.leaves.len(),
            raw_key_bytes: fixture.page.leaves.iter().map(|leaf| leaf.key.len()).sum(),
            clock_tips: fixture.page.leaves.iter().map(|leaf| leaf.entity.head.len()).sum(),
            current_bytes: current.len(),
            prefix_bytes: prefix.len(),
            choose_prefix: prefix.len() < current.len(),
            measurements,
        });
    }
    let output = serde_json::json!({
        "prototype": "benchmark-only; production codec unchanged", "check_only": check_only,
        "platform": { "os": std::env::consts::OS, "arch": std::env::consts::ARCH },
        "rustc": std::process::Command::new("rustc").arg("--version").output().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned()).ok(),
        "samples": samples, "target_batch_ms": target_ms,
        "endpoint": "decode and visit/hash every key, ID and normalized clock tip; output drop included",
        "allocation_scope": "decode+visit only; fixture and existing wire buffer excluded; retained modes include one wire-buffer copy",
        "allocator": "System wrapper; allocation counting disabled during timings, enabled flag check remains",
        "wire_scope": "one SpanContent::Heads envelope; TreeSpec and outer request excluded",
        "limits": "fixtures fit 2048 heads and a 256KiB compact blob; prototype also bounds expanded key work",
        "not_measured": ["network", "browser", "whole-query readiness", "encoder time", "construction of source inventory",
            "KeySpec/order/membership validation during timing", "production transport adoption"],
        "cases": cases,
    });
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
