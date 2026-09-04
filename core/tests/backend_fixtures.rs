//! Property-backend fixture tests for ankurah-core.
//!
//! The proto fixtures pin the *envelope*: `StateBuffers` and `Operation.diff` are
//! opaque `Vec<u8>` at the proto layer. These fixtures pin what is *inside* those
//! byte vectors - the encodings each property backend produces - which is the half of
//! the wire format the proto fixtures cannot reach.
//!
//! Each fixture is one buffer plus a `.json` sidecar recording what a correct decoder
//! must produce from it, so a symmetric encode/decode bug cannot pass.
//!
//! Backends covered: `lww` and `yrs` are the only two registered in
//! `core/src/property/backend/mod.rs`. `pn_counter.rs` exists on disk but its `mod`
//! declaration is commented out and its trait impl no longer matches
//! `PropertyBackend`, so it cannot be constructed and gets no fixtures.
//!
//! The yrs backend's own encodings live in `proto/tests/yrs_v2_fixtures.rs`, because
//! `YrsBackend::new` draws a random client id and cannot be driven reproducibly; that
//! file drives a raw `yrs::Doc` with a fixed client id through the identical calls.
//!
//! - If `OVERWRITE_FIXTURES` env var is set: write both the `.bin` and the `.json`.
//! - If NOT set: read both and assert they match exactly.
//!
//! Run with `OVERWRITE_FIXTURES=1 cargo test -p ankurah-core --test backend_fixtures` to regenerate.
//!
//! See `core/test_fixtures/README.md` for the fixture inventory.
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use ankurah_core::entity::Entity;
use ankurah_core::property::backend::{LWWBackend, PropertyBackend};
use ankurah_core::property::PropertyName;
use ankurah_core::value::Value;
use ankurah_proto::{CollectionId, EntityId, Event, OperationSet};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_fixtures")
        .join(name)
}

const ENCODING: &str = "bincode 1.3 `serialize` defaults: fixed-width integers, little-endian, \
     u64 sequence/string length prefixes, u32 enum variant tags, 1-byte Option tag, \
     no length prefix on fixed-size arrays";

fn check_or_write_bytes(name: &str, data: &[u8]) {
    let path = fixture_path(name);
    let overwrite = std::env::var("OVERWRITE_FIXTURES").is_ok();

    if overwrite {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!("Wrote fixture: {} ({} bytes)", path.display(), data.len());
    } else if !path.exists() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!(
            "Generated missing fixture: {} ({} bytes)",
            path.display(),
            data.len()
        );
    } else {
        let expected = fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "Failed to read fixture {}: {}. Run with OVERWRITE_FIXTURES=1 to generate.",
                path.display(),
                e
            )
        });
        assert_eq!(
            data,
            &expected[..],
            "Fixture mismatch for {} ({} bytes actual vs {} bytes expected)",
            name,
            data.len(),
            expected.len()
        );
    }
}

/// Write or verify a single-buffer fixture plus its sidecar.
///
/// `extra` carries the per-kind fields: `decoded` for state buffers, the `envelope`
/// breakdown for operation diffs, and so on.
fn write_fixture(
    name: &str,
    data: &[u8],
    produced_by: &str,
    scenario: &[&str],
    extra: Vec<(&str, serde_json::Value)>,
) {
    let mut root = serde_json::Map::new();
    root.insert("fixture".into(), name.into());
    root.insert("encoding".into(), ENCODING.into());
    root.insert("produced_by".into(), produced_by.into());
    root.insert(
        "scenario".into(),
        serde_json::to_value(scenario.to_vec()).unwrap(),
    );
    root.insert("total_len".into(), data.len().into());
    for (key, value) in extra {
        root.insert(key.into(), value);
    }

    let sidecar_name = name
        .strip_suffix(".bin")
        .map(|stem| format!("{stem}.json"))
        .unwrap_or_else(|| panic!("fixture name {} must end in .bin", name));
    let mut text = serde_json::to_string_pretty(&serde_json::Value::Object(root)).unwrap();
    text.push('\n');

    check_or_write_bytes(name, data);
    check_or_write_bytes(&sidecar_name, text.as_bytes());
}

/// Test-local mirror of `core::property::backend::lww::LWWDiff`, whose fields are
/// private to that module. Same field order and types, so bincode reads the real
/// diff into it - this only reads the bytes back to describe them, it does not
/// change how they are produced.
#[derive(Serialize, Deserialize)]
struct LwwDiffMirror {
    version: u8,
    data: Vec<u8>,
}

type LwwMap = BTreeMap<PropertyName, Option<Value>>;

/// Pull an LWW operation's diff apart and describe its layout byte by byte.
///
/// The version field is the single most fragile thing in this encoding: it is a
/// bare `u8`, one byte, immediately followed by the `u64` length prefix of the
/// nested buffer. A port that widens it to u32 shifts every subsequent byte.
fn describe_lww_diff(diff: &[u8]) -> Vec<(&'static str, serde_json::Value)> {
    let parsed: LwwDiffMirror =
        bincode::deserialize(diff).expect("LWW diff must decode as LWWDiff { u8, Vec<u8> }");
    let inner: LwwMap =
        bincode::deserialize(&parsed.data).expect("LWW diff payload must decode as the change map");

    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "type".into(),
        "LWWDiff { version: u8, data: Vec<u8> }".into(),
    );
    envelope.insert("version".into(), parsed.version.into());
    envelope.insert("version_offset".into(), 0.into());
    envelope.insert("version_width_bytes".into(), 1.into());
    envelope.insert("data_length_prefix_offset".into(), 1.into());
    envelope.insert("data_length_prefix_width_bytes".into(), 8.into());
    envelope.insert("data_offset".into(), 9.into());
    envelope.insert("data_len".into(), parsed.data.len().into());

    vec![
        ("envelope", serde_json::Value::Object(envelope)),
        ("decoded_data", serde_json::to_value(&inner).unwrap()),
    ]
}

/// Serialize an LWW state buffer and describe what it decodes to, asserting on the
/// way through that the backend reads its own buffer back unchanged.
fn lww_state(backend: &LWWBackend) -> (Vec<u8>, serde_json::Value) {
    let buffer = backend.to_state_buffer().unwrap();
    let decoded: LwwMap = bincode::deserialize(&buffer).unwrap();
    let round_tripped = LWWBackend::from_state_buffer(&buffer).unwrap();
    assert_eq!(
        round_tripped.property_values(),
        decoded,
        "LWW state buffer must round-trip through from_state_buffer"
    );
    let json = serde_json::to_value(&decoded).unwrap();
    (buffer, json)
}

fn entity_id(seed: u8) -> EntityId {
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = seed.wrapping_add(i as u8);
    }
    EntityId::from_bytes(bytes)
}

const NON_ASCII_KEY: &str = "名前";
const NON_ASCII_VALUE: &str = "café 日本語 🚀 مرحبا";

// ---- LWW backend -------------------------------------------------------------

/// An LWW backend with nothing in it. The buffer is a bare `0u64` length prefix:
/// eight zero bytes, not zero bytes and not a u32 zero.
#[test]
fn test_lww_empty_state() {
    let backend = LWWBackend::new();
    let (buffer, decoded) = lww_state(&backend);
    assert_eq!(buffer.len(), 8, "empty BTreeMap is a bare u64 length prefix");

    write_fixture(
        "lww/empty_state.bin",
        &buffer,
        "LWWBackend::to_state_buffer()",
        &["LWWBackend::new()"],
        vec![("decoded", decoded)],
    );
}

/// The create / set / set-again scenario, captured at every step: two state buffers
/// and the two operations between them. The second operation is the interesting one -
/// it carries only the properties that changed, not the whole map.
#[test]
fn test_lww_create_set_set() {
    let backend = LWWBackend::new();

    // --- first set ---
    backend.set("name".to_string(), Some(Value::String("Alice".to_string())));
    backend.set("count".to_string(), Some(Value::I32(1)));

    let (state_1, decoded_1) = lww_state(&backend);
    write_fixture(
        "lww/state_after_first_set.bin",
        &state_1,
        "LWWBackend::to_state_buffer()",
        &[
            "LWWBackend::new()",
            "set(\"name\", Some(String(\"Alice\")))",
            "set(\"count\", Some(I32(1)))",
        ],
        vec![("decoded", decoded_1)],
    );

    let ops_1 = backend
        .to_operations()
        .unwrap()
        .expect("two uncommitted sets must yield one operation");
    assert_eq!(ops_1.len(), 1, "LWW batches all changes into one operation");
    write_fixture(
        "lww/op_first_set.bin",
        &ops_1[0].diff,
        "LWWBackend::to_operations()[0].diff",
        &[
            "LWWBackend::new()",
            "set(\"name\", Some(String(\"Alice\")))",
            "set(\"count\", Some(I32(1)))",
            "to_operations()",
        ],
        describe_lww_diff(&ops_1[0].diff),
    );

    // Collecting operations marks every entry committed, so an immediate second call
    // reports no change at all - `None`, not an operation with an empty map.
    assert!(
        backend.to_operations().unwrap().is_none(),
        "a second to_operations() with no intervening set must be None"
    );

    // --- second set: change one property, clear another ---
    backend.set("count".to_string(), Some(Value::I32(2)));
    backend.set("cleared".to_string(), None);

    let (state_2, decoded_2) = lww_state(&backend);
    write_fixture(
        "lww/state_after_second_set.bin",
        &state_2,
        "LWWBackend::to_state_buffer()",
        &[
            "... state_after_first_set",
            "set(\"count\", Some(I32(2)))",
            "set(\"cleared\", None)",
        ],
        vec![("decoded", decoded_2)],
    );

    let ops_2 = backend.to_operations().unwrap().expect("second set changed two properties");
    let mut extra = describe_lww_diff(&ops_2[0].diff);
    extra.push((
        "note",
        "Carries only `count` and `cleared` - `name` was committed by the previous \
         to_operations() and is absent. A port that resends the whole map produces \
         a longer buffer with different bytes."
            .into(),
    ));
    write_fixture(
        "lww/op_second_set.bin",
        &ops_2[0].diff,
        "LWWBackend::to_operations()[0].diff",
        &[
            "... op_first_set",
            "set(\"count\", Some(I32(2)))",
            "set(\"cleared\", None)",
            "to_operations()",
        ],
        extra,
    );

    // Applying op_second_set to a backend restored from state_after_first_set must
    // reproduce state_after_second_set byte for byte.
    let replayed = LWWBackend::from_state_buffer(&state_1).unwrap();
    replayed.apply_operations(&ops_2).unwrap();
    assert_eq!(
        replayed.to_state_buffer().unwrap(),
        state_2,
        "state_1 + op_2 must equal state_2"
    );
}

/// Every `core::value::Value` variant in one state buffer, plus a `None`. This is the
/// only place the Value enum's variant tags and payload widths are pinned.
#[test]
fn test_lww_all_value_types() {
    let backend = LWWBackend::new();

    // Keys are deliberately ordered so the BTreeMap's sorted output is not the
    // insertion order - a port that preserves insertion order fails here.
    backend.set("v_i16_min".to_string(), Some(Value::I16(i16::MIN)));
    backend.set("v_i16_max".to_string(), Some(Value::I16(i16::MAX)));
    backend.set("v_i32_min".to_string(), Some(Value::I32(i32::MIN)));
    backend.set("v_i32_max".to_string(), Some(Value::I32(i32::MAX)));
    backend.set("v_i64_min".to_string(), Some(Value::I64(i64::MIN)));
    backend.set("v_i64_max".to_string(), Some(Value::I64(i64::MAX)));
    // 2^53 + 1: the first integer a JS `number` cannot hold exactly.
    backend.set(
        "v_i64_beyond_js_safe".to_string(),
        Some(Value::I64(9_007_199_254_740_993)),
    );
    backend.set("v_f64_zero".to_string(), Some(Value::F64(0.0)));
    backend.set("v_f64_neg_zero".to_string(), Some(Value::F64(-0.0)));
    backend.set("v_f64_fraction".to_string(), Some(Value::F64(0.1 + 0.2)));
    backend.set("v_f64_min".to_string(), Some(Value::F64(f64::MIN)));
    backend.set("v_f64_max".to_string(), Some(Value::F64(f64::MAX)));
    backend.set("v_bool_true".to_string(), Some(Value::Bool(true)));
    backend.set("v_bool_false".to_string(), Some(Value::Bool(false)));
    backend.set(
        "v_string".to_string(),
        Some(Value::String("hello".to_string())),
    );
    backend.set("v_string_empty".to_string(), Some(Value::String(String::new())));
    backend.set(
        "v_entity_id".to_string(),
        Some(Value::EntityId(entity_id(0x00))),
    );
    backend.set(
        "v_object".to_string(),
        Some(Value::Object(vec![0xDE, 0xAD, 0xBE, 0xEF])),
    );
    backend.set("v_object_empty".to_string(), Some(Value::Object(vec![])));
    backend.set("v_binary".to_string(), Some(Value::Binary(vec![0x00, 0xFF])));
    // Value::Json carries `#[serde(with = "json_as_bytes")]`: the wire form is the
    // UTF-8 bytes of the serialized JSON behind a u64 length prefix, so the sidecar's
    // decoded form for this key is a byte array, not the JSON document.
    backend.set(
        "v_json".to_string(),
        Some(Value::Json(serde_json::json!({"a": 1, "b": [true, null]}))),
    );
    backend.set("v_none".to_string(), None);

    let (buffer, decoded) = lww_state(&backend);
    write_fixture(
        "lww/all_value_types.bin",
        &buffer,
        "LWWBackend::to_state_buffer()",
        &["one set() per Value variant, plus one set(_, None)"],
        vec![
            ("decoded", decoded),
            (
                "note",
                "Keys are emitted in BTreeMap (byte-lexicographic) order, not insertion \
                 order. `v_json` decodes to the UTF-8 bytes of its JSON document because \
                 Value::Json is serialized through `json_as_bytes`; \
                 `v_none` is a present key whose value is absent, which is not the same \
                 as the key being missing."
                    .into(),
            ),
        ],
    );
}

/// Non-ASCII property names and string values. BTreeMap orders keys by UTF-8 bytes,
/// which is not the order a JS `Array.prototype.sort` on the same strings produces
/// for every alphabet.
#[test]
fn test_lww_non_ascii() {
    let backend = LWWBackend::new();
    backend.set(
        NON_ASCII_KEY.to_string(),
        Some(Value::String(NON_ASCII_VALUE.to_string())),
    );
    backend.set(
        "café".to_string(),
        Some(Value::String("cafe\u{0301}".to_string())),
    );
    backend.set("🚀".to_string(), Some(Value::String("🌍".to_string())));
    backend.set("a\u{0}b".to_string(), Some(Value::String("x\u{0}y".to_string())));

    let (buffer, decoded) = lww_state(&backend);
    write_fixture(
        "lww/non_ascii.bin",
        &buffer,
        "LWWBackend::to_state_buffer()",
        &[
            "set(\"名前\", Some(String(\"café 日本語 🚀 مرحبا\")))",
            "set(\"café\", Some(String(\"cafe\\u{301}\")))  // composed key, decomposed value",
            "set(\"🚀\", Some(String(\"🌍\")))",
            "set(\"a\\0b\", Some(String(\"x\\0y\")))",
        ],
        vec![
            ("decoded", decoded),
            (
                "note",
                "Key order is by UTF-8 bytes. The `café` entry pairs a precomposed key \
                 with a decomposed value: a port that normalizes either one produces \
                 different bytes."
                    .into(),
            ),
        ],
    );
}

/// An operation whose changed-value map is empty cannot arise from `to_operations()`,
/// which returns `None` instead. This pins the boundary explicitly so a port knows
/// the empty operation is not a thing it should ever emit.
#[test]
fn test_lww_no_change_yields_none() {
    let backend = LWWBackend::new();
    assert!(
        backend.to_operations().unwrap().is_none(),
        "a backend with no sets must yield None, not an operation carrying an empty map"
    );

    backend.set("k".to_string(), Some(Value::I32(1)));
    assert!(backend.to_operations().unwrap().is_some());
    assert!(
        backend.to_operations().unwrap().is_none(),
        "committed entries must not be re-emitted"
    );

    // Setting a property to the value it already holds still counts as a change:
    // LWW tracks a committed flag, not value equality.
    backend.set("k".to_string(), Some(Value::I32(1)));
    let ops = backend
        .to_operations()
        .unwrap()
        .expect("re-setting an identical value must still produce an operation");
    write_fixture(
        "lww/op_idempotent_set.bin",
        &ops[0].diff,
        "LWWBackend::to_operations()[0].diff",
        &[
            "set(\"k\", Some(I32(1)))",
            "to_operations()  // committed",
            "set(\"k\", Some(I32(1)))  // same value again",
            "to_operations()",
        ],
        {
            let mut extra = describe_lww_diff(&ops[0].diff);
            extra.push((
                "note",
                "LWW tracks a per-entry committed flag, not value equality, so setting a \
                 property to the value it already holds produces a real operation. A port \
                 that suppresses no-op writes emits nothing here and diverges."
                    .into(),
            ));
            extra
        },
    );
}

// ---- Entity state and event assembly -----------------------------------------

/// A proto `EntityState` assembled the way the core does it: an `Entity`, an LWW
/// backend reached through `get_backend`, and `to_entity_state()`. This is the join
/// between the two layers - the LWW buffer from the fixtures above appears here
/// nested inside `State.state_buffers` under the key `"lww"`.
///
/// Only the LWW backend is touched, because `get_backend::<YrsBackend>()` would
/// construct a `yrs::Doc` with a random client id and make the fixture irreproducible.
#[test]
fn test_entity_state_assembly() {
    let entity = Entity::create(entity_id(0x42), CollectionId::from("album"));
    let lww = entity.get_backend::<LWWBackend>().unwrap();
    lww.set("name".to_string(), Some(Value::String("Ice Nine".to_string())));
    lww.set("year".to_string(), Some(Value::I32(1993)));
    lww.set(
        NON_ASCII_KEY.to_string(),
        Some(Value::String(NON_ASCII_VALUE.to_string())),
    );

    let entity_state = entity.to_entity_state().unwrap();
    assert!(
        entity_state.state.head.is_empty(),
        "a freshly created entity has an empty head"
    );
    assert_eq!(
        entity_state.state.state_buffers.keys().collect::<Vec<_>>(),
        vec!["lww"],
        "only the backend that was reached should be present"
    );

    let bytes = bincode::serialize(&entity_state).unwrap();
    write_fixture(
        "entity_state.bin",
        &bytes,
        "bincode::serialize(&Entity::to_entity_state())",
        &[
            "Entity::create(entity_id(0x42), CollectionId::from(\"album\"))",
            "get_backend::<LWWBackend>()",
            "set(\"name\", Some(String(\"Ice Nine\")))",
            "set(\"year\", Some(I32(1993)))",
            "set(\"名前\", Some(String(\"café 日本語 🚀 مرحبا\")))",
            "to_entity_state()",
        ],
        vec![
            (
                "decoded",
                serde_json::to_value(&entity_state).unwrap(),
            ),
            (
                "state_buffers_decoded",
                serde_json::to_value(
                    entity_state
                        .state
                        .state_buffers
                        .iter()
                        .map(|(name, buf)| {
                            let map: LwwMap = bincode::deserialize(buf).unwrap();
                            (name.clone(), map)
                        })
                        .collect::<BTreeMap<_, _>>(),
                )
                .unwrap(),
            ),
            (
                "note",
                "`decoded` shows the proto EntityState with state_buffers as raw byte \
                 arrays, which is what the wire carries. `state_buffers_decoded` shows \
                 the same buffers after the lww backend decodes them, which is what a \
                 correct port must recover from those bytes."
                    .into(),
            ),
        ],
    );
}

/// A proto `Event` assembled from a backend's operations, and the `EventId` it
/// hashes to.
///
/// `Entity::generate_commit_event` is `pub(crate)`, so this mirrors what it does:
/// collect `to_operations()` for each backend into an `OperationSet` keyed by backend
/// name, and pair it with the entity's current head as `parent`. The `EventId` then
/// pins the whole chain, since `EventId::from_parts` hashes the bincode encodings of
/// entity_id, operations and parent together.
#[test]
fn test_event_assembly() {
    let entity = Entity::create(entity_id(0x42), CollectionId::from("album"));
    let lww = entity.get_backend::<LWWBackend>().unwrap();
    lww.set("name".to_string(), Some(Value::String("Ice Nine".to_string())));
    lww.set("year".to_string(), Some(Value::I32(1993)));

    let mut operations = BTreeMap::new();
    let ops = lww.to_operations().unwrap().expect("two sets are pending");
    operations.insert(LWWBackend::property_backend_name(), ops);

    let event = Event {
        entity_id: entity.id(),
        collection: entity.collection().clone(),
        operations: OperationSet(operations),
        parent: entity.head(),
    };
    assert!(
        event.is_entity_create(),
        "an event with an empty parent clock is an entity create"
    );

    let bytes = bincode::serialize(&event).unwrap();
    write_fixture(
        "event.bin",
        &bytes,
        "bincode::serialize(&Event) assembled from LWWBackend::to_operations()",
        &[
            "Entity::create(entity_id(0x42), CollectionId::from(\"album\"))",
            "get_backend::<LWWBackend>()",
            "set(\"name\", Some(String(\"Ice Nine\")))",
            "set(\"year\", Some(I32(1993)))",
            "OperationSet({ \"lww\": to_operations() })",
            "parent = entity.head()  // empty: this is a create",
        ],
        vec![
            ("decoded", serde_json::to_value(&event).unwrap()),
            (
                "event_id_base64",
                event.id().to_base64().into(),
            ),
            (
                "operations_decoded",
                serde_json::to_value(
                    event
                        .operations
                        .iter()
                        .map(|(name, ops)| {
                            let diffs: Vec<LwwMap> = ops
                                .iter()
                                .map(|op| {
                                    let parsed: LwwDiffMirror =
                                        bincode::deserialize(&op.diff).unwrap();
                                    bincode::deserialize(&parsed.data).unwrap()
                                })
                                .collect();
                            (name.clone(), diffs)
                        })
                        .collect::<BTreeMap<_, _>>(),
                )
                .unwrap(),
            ),
            (
                "note",
                "`event_id_base64` is EventId::from_parts(entity_id, operations, parent), \
                 a SHA-256 over the three bincode encodings in that order. Matching it \
                 proves the port encoded all three identically at once; collection is \
                 deliberately not part of the hash."
                    .into(),
            ),
        ],
    );
}
