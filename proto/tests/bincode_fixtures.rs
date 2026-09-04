//! Bincode fixture tests for ankurah-proto types.
//!
//! Each test builds a `Fixture`: a byte buffer holding the bincode encoding of a
//! sequence of known, deterministic values, plus a JSON sidecar recording, for each
//! value, its label, its type, its byte offset and length inside the buffer, and the
//! value a correct decoder must produce.
//!
//! The sidecar exists because byte-for-byte round-tripping alone cannot catch a
//! *symmetric* decode bug: a port that swaps two same-typed adjacent fields on both
//! encode and decode round-trips perfectly while being wrong. The sidecar pins the
//! decoded values, and the offsets pin how many bytes each value is allowed to consume.
//!
//! - If `OVERWRITE_FIXTURES` env var is set: write both the `.bin` and the `.json`.
//! - If NOT set: read both and assert they match exactly.
//!
//! Run with `OVERWRITE_FIXTURES=1 cargo test -p ankurah-proto --test bincode_fixtures` to regenerate.
//!
//! See `proto/test_fixtures/README.md` for the fixture inventory.
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::path::PathBuf;

use serde::Serialize;

use ankurah_proto::auth::{Attestation, AttestationSet, Attested, AuthData, Principal};
use ankurah_proto::clock::Clock;
use ankurah_proto::collection::CollectionId;
use ankurah_proto::data::{
    EntityState, Event, EventFragment, EventId, Operation, OperationSet, State, StateBuffers,
    StateFragment,
};
use ankurah_proto::message::{Message, NodeMessage};
use ankurah_proto::peering::Presence;
use ankurah_proto::request::{
    CausalAssertion, CausalAssertionFragment, CausalRelation, DeltaContent, EntityDelta,
    KnownEntity, NodeRequest, NodeRequestBody, NodeResponse, NodeResponseBody, RequestId,
};
use ankurah_proto::sys;
use ankurah_proto::transaction::TransactionId;
use ankurah_proto::update::{
    MembershipChange, NodeUpdate, NodeUpdateAck, NodeUpdateAckBody, NodeUpdateBody,
    SubscriptionUpdateItem, UpdateContent, UpdateId,
};
use ankurah_proto::EntityId;
use ankurah_proto::QueryId;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_fixtures")
        .join(name)
}

/// The bincode configuration every fixture in this file was produced under.
/// Recorded in each sidecar so a consumer can never have to guess.
const ENCODING: &str = "bincode 1.3 `serialize` defaults: fixed-width integers, little-endian, \
     u64 sequence/string length prefixes, u32 enum variant tags, 1-byte Option tag, \
     no length prefix on fixed-size arrays";

/// Accumulates bincode bytes and the matching expected-value sidecar.
struct Fixture {
    name: &'static str,
    bytes: Vec<u8>,
    items: Vec<serde_json::Value>,
}

impl Fixture {
    fn new(name: &'static str) -> Self {
        Fixture {
            name,
            bytes: Vec::new(),
            items: Vec::new(),
        }
    }

    /// Append `value`'s bincode encoding and record its JSON form as the expected decode.
    ///
    /// `ty` is the Rust type name; `label` names this value's role within the fixture.
    fn add<T: Serialize>(&mut self, label: &str, ty: &str, value: &T) {
        let json = serde_json::to_value(value).unwrap_or_else(|e| {
            panic!(
                "{}: value {} of type {} has no JSON form ({}); use add_debug instead",
                self.name, label, ty, e
            )
        });
        self.record(label, ty, value, ("json", json));
    }

    /// Same as `add`, but records `{:?}` instead of JSON.
    ///
    /// For values serde_json cannot represent faithfully - non-finite f64 being the only
    /// case in this crate, since serde_json silently turns NaN and the infinities into
    /// `null`, which would make the sidecar lie about what the bytes decode to.
    fn add_debug<T: Serialize + Debug>(&mut self, label: &str, ty: &str, value: &T) {
        let debug = serde_json::Value::String(format!("{:?}", value));
        self.record(label, ty, value, ("debug", debug));
    }

    fn record<T: Serialize>(
        &mut self,
        label: &str,
        ty: &str,
        value: &T,
        (form, expected): (&str, serde_json::Value),
    ) {
        let offset = self.bytes.len();
        let encoded = bincode::serialize(value)
            .unwrap_or_else(|e| panic!("{}: bincode failed for {}: {}", self.name, label, e));
        self.bytes.extend_from_slice(&encoded);

        let mut item = serde_json::Map::new();
        item.insert("label".into(), label.into());
        item.insert("type".into(), ty.into());
        item.insert("offset".into(), offset.into());
        item.insert("len".into(), encoded.len().into());
        item.insert(form.into(), expected);
        self.items.push(serde_json::Value::Object(item));
    }

    /// Write or verify both the `.bin` and its `.json` sidecar.
    fn finish(self) {
        let sidecar_name = self
            .name
            .strip_suffix(".bin")
            .map(|stem| format!("{stem}.json"))
            .unwrap_or_else(|| panic!("fixture name {} must end in .bin", self.name));

        let sidecar = {
            let mut root = serde_json::Map::new();
            root.insert("fixture".into(), self.name.into());
            root.insert("encoding".into(), ENCODING.into());
            root.insert("total_len".into(), self.bytes.len().into());
            root.insert("items".into(), serde_json::Value::Array(self.items));
            let mut text = serde_json::to_string_pretty(&serde_json::Value::Object(root)).unwrap();
            text.push('\n');
            text
        };

        check_or_write_bytes(self.name, &self.bytes);
        check_or_write_bytes(&sidecar_name, sidecar.as_bytes());
    }
}

fn check_or_write_bytes(name: &str, data: &[u8]) {
    let path = fixture_path(name);
    let overwrite = std::env::var("OVERWRITE_FIXTURES").is_ok();

    if overwrite {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!("Wrote fixture: {}", path.display());
    } else if !path.exists() {
        // Auto-generate on first run if fixture doesn't exist yet
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!("Generated missing fixture: {}", path.display());
    } else {
        let expected = fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "Failed to read fixture {}: {}. Run with OVERWRITE_FIXTURES=1 to generate.",
                path.display(),
                e
            )
        });
        assert_eq!(data, &expected[..], "Fixture mismatch for {}", name);
    }
}

// ---- Helper constructors for deterministic values ----

fn make_entity_id(seed: u8) -> EntityId {
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = seed.wrapping_add(i as u8);
    }
    EntityId::from_bytes(bytes)
}

fn make_event_id(seed: u8) -> EventId {
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = seed.wrapping_add(i as u8);
    }
    EventId::from_bytes(bytes)
}

/// Construct a TransactionId from a known JSON string (Crockford Base32 ULID).
/// We use JSON round-tripping because the inner Ulid field is private.
/// The ULID string "00000000000000000000000001" is deterministic.
fn make_transaction_id(seed: u8) -> TransactionId {
    // Construct a ULID string: 26 chars of Crockford Base32, all zeros except last byte encodes seed
    let ulid_str = format!("0000000000000000000000{:04}", seed);
    serde_json::from_value::<TransactionId>(serde_json::Value::String(ulid_str)).unwrap()
}

/// Construct a RequestId from a known JSON string.
fn make_request_id(seed: u8) -> RequestId {
    let ulid_str = format!("0000000000000000000000{:04}", seed);
    serde_json::from_value::<RequestId>(serde_json::Value::String(ulid_str)).unwrap()
}

/// Construct a QueryId deterministically using the test() constructor.
fn make_query_id(id: u64) -> QueryId {
    QueryId::test(id)
}

/// Construct an UpdateId from a known JSON string.
fn make_update_id(seed: u8) -> UpdateId {
    let ulid_str = format!("0000000000000000000000{:04}", seed);
    serde_json::from_value::<UpdateId>(serde_json::Value::String(ulid_str)).unwrap()
}

fn make_collection_id(name: &str) -> CollectionId {
    CollectionId::from(name)
}

fn make_clock_empty() -> Clock {
    Clock::new(vec![])
}

fn make_clock_single() -> Clock {
    Clock::new(vec![make_event_id(0x10)])
}

fn make_clock_multi() -> Clock {
    Clock::new(vec![
        make_event_id(0x10),
        make_event_id(0x30),
        make_event_id(0x50),
    ])
}

fn make_operation(diff: &[u8]) -> Operation {
    Operation {
        diff: diff.to_vec(),
    }
}

fn make_operation_set() -> OperationSet {
    let mut map = BTreeMap::new();
    map.insert(
        "backend_a".to_string(),
        vec![make_operation(&[0xAA, 0xBB]), make_operation(&[0xCC])],
    );
    map.insert(
        "backend_b".to_string(),
        vec![make_operation(&[0xDD, 0xEE, 0xFF])],
    );
    OperationSet(map)
}

fn make_state_buffers() -> StateBuffers {
    let mut map = BTreeMap::new();
    map.insert("buf_alpha".to_string(), vec![0x01, 0x02, 0x03]);
    map.insert("buf_beta".to_string(), vec![0x04, 0x05]);
    StateBuffers(map)
}

fn make_state() -> State {
    State {
        state_buffers: make_state_buffers(),
        head: make_clock_single(),
    }
}

fn make_attestation(bytes: &[u8]) -> Attestation {
    Attestation(bytes.to_vec())
}

fn make_attestation_set_empty() -> AttestationSet {
    AttestationSet(vec![])
}

fn make_attestation_set_two() -> AttestationSet {
    AttestationSet(vec![
        make_attestation(&[0x42, 0x43]),
        make_attestation(&[0x44, 0x45, 0x46]),
    ])
}

fn make_state_fragment() -> StateFragment {
    StateFragment {
        state: make_state(),
        attestations: make_attestation_set_two(),
    }
}

fn make_event_fragment() -> EventFragment {
    EventFragment {
        operations: make_operation_set(),
        parent: make_clock_single(),
        attestations: make_attestation_set_empty(),
    }
}

fn make_entity_state() -> EntityState {
    EntityState {
        entity_id: make_entity_id(0x00),
        collection: make_collection_id("test_collection"),
        state: make_state(),
    }
}

fn make_event() -> Event {
    Event {
        collection: make_collection_id("test_collection"),
        entity_id: make_entity_id(0x00),
        operations: make_operation_set(),
        parent: make_clock_single(),
    }
}

fn make_selection() -> ankql::ast::Selection {
    // A simple selection: name = 'test'
    ankql::ast::Selection {
        predicate: ankql::ast::Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(ankql::ast::PathExpr::simple("name"))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(ankql::ast::Literal::String(
                "test".to_string(),
            ))),
        },
        order_by: None,
        limit: None,
    }
}

fn make_known_entity() -> KnownEntity {
    KnownEntity {
        entity_id: make_entity_id(0x20),
        head: make_clock_single(),
    }
}

fn make_causal_assertion_fragment() -> CausalAssertionFragment {
    CausalAssertionFragment {
        relation: CausalRelation::Equal,
        attestations: make_attestation_set_two(),
    }
}

// ---- Non-ASCII string corpus -------------------------------------------------
//
// Each entry breaks a different naive assumption a TypeScript port can make:
//   NON_ASCII_2BYTE     - 2-byte UTF-8 sequence
//   NON_ASCII_3BYTE     - 3-byte UTF-8 sequence
//   NON_ASCII_4BYTE     - 4-byte UTF-8, i.e. a surrogate pair in a JS string, so
//                         `str.length` (UTF-16 units) is not the byte length
//   NON_ASCII_COMBINING - decomposed "e" + combining acute: two scalars, one grapheme
//   NON_ASCII_RTL       - right-to-left script
//   NON_ASCII_NUL       - an interior NUL, legal in a Rust String and in bincode,
//                         fatal to any C-string-shaped reader

const NON_ASCII_2BYTE: &str = "café";
const NON_ASCII_3BYTE: &str = "日本語";
const NON_ASCII_4BYTE: &str = "🚀🌍";
const NON_ASCII_COMBINING: &str = "cafe\u{0301}";
const NON_ASCII_RTL: &str = "مرحبا";
const NON_ASCII_NUL: &str = "a\u{0}b";
const NON_ASCII_MIXED: &str = "café 日本語 🚀 مرحبا";

// ---- Fixture Tests ----

#[test]
fn test_ids_fixture() {
    let entity_id = EntityId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);

    let event_id = EventId::from_bytes([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ]);

    let transaction_id = make_transaction_id(0x10);
    let request_id = make_request_id(0x20);
    let query_id = make_query_id(42);
    let update_id = make_update_id(0x30);
    let collection_id = make_collection_id("test_collection");

    let mut f = Fixture::new("ids.bin");
    f.add("entity_id", "EntityId", &entity_id);
    f.add("event_id", "EventId", &event_id);
    f.add("transaction_id", "TransactionId", &transaction_id);
    f.add("request_id", "RequestId", &request_id);
    f.add("query_id", "QueryId", &query_id);
    f.add("update_id", "UpdateId", &update_id);
    f.add("collection_id", "CollectionId", &collection_id);
    f.finish();
}

#[test]
fn test_clock_fixture() {
    let empty = make_clock_empty();
    let single = make_clock_single();
    let multi = make_clock_multi();

    let mut f = Fixture::new("clock.bin");
    f.add("empty", "Clock", &empty);
    f.add("single", "Clock", &single);
    f.add("multi", "Clock", &multi);
    f.finish();
}

#[test]
fn test_auth_fixture() {
    let auth_data_bytes = AuthData(vec![0x01, 0x02, 0x03]);
    let auth_data_empty = AuthData(vec![]);
    let attestation = make_attestation(&[0x42, 0x43]);
    let attestation_set_empty = make_attestation_set_empty();
    let attestation_set_two = make_attestation_set_two();

    // Attested<EntityState>
    let attested_entity_state = Attested {
        payload: make_entity_state(),
        attestations: make_attestation_set_two(),
    };

    let mut f = Fixture::new("auth.bin");
    f.add("auth_data_bytes", "AuthData", &auth_data_bytes);
    f.add("auth_data_empty", "AuthData", &auth_data_empty);
    f.add("attestation", "Attestation", &attestation);
    f.add(
        "attestation_set_empty",
        "AttestationSet",
        &attestation_set_empty,
    );
    f.add("attestation_set_two", "AttestationSet", &attestation_set_two);
    f.add(
        "attested_entity_state",
        "Attested<EntityState>",
        &attested_entity_state,
    );
    f.finish();
}

#[test]
fn test_data_fixture() {
    let operation = make_operation(&[0xAA, 0xBB, 0xCC]);
    let operation_set = make_operation_set();
    let state_buffers = make_state_buffers();
    let state = make_state();
    let state_fragment = make_state_fragment();
    let event = make_event();
    let event_fragment = make_event_fragment();
    let entity_state = make_entity_state();

    let mut f = Fixture::new("data.bin");
    f.add("operation", "Operation", &operation);
    f.add("operation_set", "OperationSet", &operation_set);
    f.add("state_buffers", "StateBuffers", &state_buffers);
    f.add("state", "State", &state);
    f.add("state_fragment", "StateFragment", &state_fragment);
    f.add("event", "Event", &event);
    f.add("event_fragment", "EventFragment", &event_fragment);
    f.add("entity_state", "EntityState", &entity_state);
    f.finish();
}

#[test]
fn test_request_fixture() {
    let node_request = NodeRequest {
        id: make_request_id(0x01),
        to: make_entity_id(0x10),
        from: make_entity_id(0x20),
        body: NodeRequestBody::Get {
            collection: make_collection_id("users"),
            ids: vec![make_entity_id(0x30)],
        },
    };

    let commit_tx = NodeRequestBody::CommitTransaction {
        id: make_transaction_id(0x01),
        events: vec![Attested {
            payload: make_event(),
            attestations: make_attestation_set_empty(),
        }],
    };

    let get = NodeRequestBody::Get {
        collection: make_collection_id("users"),
        ids: vec![make_entity_id(0x40), make_entity_id(0x50)],
    };

    let get_events = NodeRequestBody::GetEvents {
        collection: make_collection_id("users"),
        event_ids: vec![make_event_id(0x60)],
    };

    let fetch = NodeRequestBody::Fetch {
        collection: make_collection_id("users"),
        selection: make_selection(),
        known_matches: vec![make_known_entity()],
    };

    let subscribe = NodeRequestBody::SubscribeQuery {
        query_id: make_query_id(99),
        collection: make_collection_id("users"),
        selection: make_selection(),
        version: 1,
        known_matches: vec![],
    };

    let mut f = Fixture::new("request.bin");
    f.add("node_request", "NodeRequest", &node_request);
    f.add("commit_transaction", "NodeRequestBody", &commit_tx);
    f.add("get", "NodeRequestBody", &get);
    f.add("get_events", "NodeRequestBody", &get_events);
    f.add("fetch", "NodeRequestBody", &fetch);
    f.add("subscribe_query", "NodeRequestBody", &subscribe);
    f.finish();
}

#[test]
fn test_response_fixture() {
    let node_response = NodeResponse {
        request_id: make_request_id(0x01),
        from: make_entity_id(0x10),
        to: make_entity_id(0x20),
        body: NodeResponseBody::Success,
    };

    let commit_complete = NodeResponseBody::CommitComplete {
        id: make_transaction_id(0x02),
    };

    let fetch_resp = NodeResponseBody::Fetch(vec![EntityDelta {
        entity_id: make_entity_id(0x30),
        collection: make_collection_id("users"),
        content: DeltaContent::StateSnapshot {
            state: make_state_fragment(),
        },
    }]);

    let get_resp = NodeResponseBody::Get(vec![Attested {
        payload: make_entity_state(),
        attestations: make_attestation_set_empty(),
    }]);

    let get_events_resp = NodeResponseBody::GetEvents(vec![Attested {
        payload: make_event(),
        attestations: make_attestation_set_empty(),
    }]);

    let query_subscribed = NodeResponseBody::QuerySubscribed {
        query_id: make_query_id(42),
        deltas: vec![EntityDelta {
            entity_id: make_entity_id(0x40),
            collection: make_collection_id("posts"),
            content: DeltaContent::EventBridge {
                events: vec![make_event_fragment()],
            },
        }],
    };

    let success = NodeResponseBody::Success;
    let error = NodeResponseBody::Error("something went wrong".to_string());

    let mut f = Fixture::new("response.bin");
    f.add("node_response", "NodeResponse", &node_response);
    f.add("commit_complete", "NodeResponseBody", &commit_complete);
    f.add("fetch", "NodeResponseBody", &fetch_resp);
    f.add("get", "NodeResponseBody", &get_resp);
    f.add("get_events", "NodeResponseBody", &get_events_resp);
    f.add("query_subscribed", "NodeResponseBody", &query_subscribed);
    f.add("success", "NodeResponseBody", &success);
    f.add("error", "NodeResponseBody", &error);
    f.finish();
}

#[test]
fn test_causal_fixture() {
    let equal = CausalRelation::Equal;
    let strict_descends = CausalRelation::StrictDescends;
    let strict_ascends = CausalRelation::StrictAscends;

    let diverged_since = CausalRelation::DivergedSince {
        meet: make_clock_single(),
        subject: Clock::new(vec![make_event_id(0xA0)]),
        other: Clock::new(vec![make_event_id(0xB0)]),
    };

    let disjoint_some = CausalRelation::Disjoint {
        gca: Some(make_clock_single()),
        subject_root: make_event_id(0xC0),
        other_root: make_event_id(0xD0),
    };

    let disjoint_none = CausalRelation::Disjoint {
        gca: None,
        subject_root: make_event_id(0xE0),
        other_root: make_event_id(0xF0),
    };

    let budget_exceeded = CausalRelation::BudgetExceeded {
        subject: Clock::new(vec![make_event_id(0x01)]),
        other: Clock::new(vec![make_event_id(0x02)]),
    };

    let causal_assertion_fragment = CausalAssertionFragment {
        relation: CausalRelation::StrictDescends,
        attestations: make_attestation_set_two(),
    };

    let mut f = Fixture::new("causal.bin");
    f.add("equal", "CausalRelation", &equal);
    f.add("strict_descends", "CausalRelation", &strict_descends);
    f.add("strict_ascends", "CausalRelation", &strict_ascends);
    f.add("diverged_since", "CausalRelation", &diverged_since);
    f.add("disjoint_gca_some", "CausalRelation", &disjoint_some);
    f.add("disjoint_gca_none", "CausalRelation", &disjoint_none);
    f.add("budget_exceeded", "CausalRelation", &budget_exceeded);
    f.add(
        "causal_assertion_fragment",
        "CausalAssertionFragment",
        &causal_assertion_fragment,
    );
    f.finish();
}

#[test]
fn test_delta_fixture() {
    let state_snapshot = DeltaContent::StateSnapshot {
        state: make_state_fragment(),
    };

    let event_bridge = DeltaContent::EventBridge {
        events: vec![make_event_fragment()],
    };

    let state_and_relation = DeltaContent::StateAndRelation {
        state: make_state_fragment(),
        relation: make_causal_assertion_fragment(),
    };

    let entity_delta_snapshot = EntityDelta {
        entity_id: make_entity_id(0x10),
        collection: make_collection_id("test_collection"),
        content: DeltaContent::StateSnapshot {
            state: make_state_fragment(),
        },
    };

    let entity_delta_bridge = EntityDelta {
        entity_id: make_entity_id(0x20),
        collection: make_collection_id("test_collection"),
        content: DeltaContent::EventBridge {
            events: vec![make_event_fragment()],
        },
    };

    let entity_delta_state_rel = EntityDelta {
        entity_id: make_entity_id(0x30),
        collection: make_collection_id("test_collection"),
        content: DeltaContent::StateAndRelation {
            state: make_state_fragment(),
            relation: make_causal_assertion_fragment(),
        },
    };

    let known_entity = make_known_entity();

    let mut f = Fixture::new("delta.bin");
    f.add("state_snapshot", "DeltaContent", &state_snapshot);
    f.add("event_bridge", "DeltaContent", &event_bridge);
    f.add("state_and_relation", "DeltaContent", &state_and_relation);
    f.add(
        "entity_delta_state_snapshot",
        "EntityDelta",
        &entity_delta_snapshot,
    );
    f.add(
        "entity_delta_event_bridge",
        "EntityDelta",
        &entity_delta_bridge,
    );
    f.add(
        "entity_delta_state_and_relation",
        "EntityDelta",
        &entity_delta_state_rel,
    );
    f.add("known_entity", "KnownEntity", &known_entity);
    f.finish();
}

#[test]
fn test_update_fixture() {
    let node_update = NodeUpdate {
        id: make_update_id(0x01),
        from: make_entity_id(0x10),
        to: make_entity_id(0x20),
        body: NodeUpdateBody::SubscriptionUpdate {
            items: vec![SubscriptionUpdateItem {
                entity_id: make_entity_id(0x30),
                collection: make_collection_id("users"),
                content: UpdateContent::EventOnly(vec![make_event_fragment()]),
                predicate_relevance: vec![(make_query_id(1), MembershipChange::Initial)],
            }],
        },
    };

    let subscription_update_item = SubscriptionUpdateItem {
        entity_id: make_entity_id(0x40),
        collection: make_collection_id("posts"),
        content: UpdateContent::StateAndEvent(make_state_fragment(), vec![make_event_fragment()]),
        predicate_relevance: vec![
            (make_query_id(2), MembershipChange::Add),
            (make_query_id(3), MembershipChange::Remove),
        ],
    };

    let event_only = UpdateContent::EventOnly(vec![make_event_fragment()]);
    let state_and_event =
        UpdateContent::StateAndEvent(make_state_fragment(), vec![make_event_fragment()]);

    let membership_initial = MembershipChange::Initial;
    let membership_add = MembershipChange::Add;
    let membership_remove = MembershipChange::Remove;

    let node_update_ack = NodeUpdateAck {
        id: make_update_id(0x02),
        from: make_entity_id(0x50),
        to: make_entity_id(0x60),
        body: NodeUpdateAckBody::Success,
    };

    let ack_success = NodeUpdateAckBody::Success;
    let ack_error = NodeUpdateAckBody::Error("update failed".to_string());

    let mut f = Fixture::new("update.bin");
    f.add("node_update", "NodeUpdate", &node_update);
    f.add(
        "subscription_update_item",
        "SubscriptionUpdateItem",
        &subscription_update_item,
    );
    f.add("event_only", "UpdateContent", &event_only);
    f.add("state_and_event", "UpdateContent", &state_and_event);
    f.add("membership_initial", "MembershipChange", &membership_initial);
    f.add("membership_add", "MembershipChange", &membership_add);
    f.add("membership_remove", "MembershipChange", &membership_remove);
    f.add("node_update_ack", "NodeUpdateAck", &node_update_ack);
    f.add("ack_success", "NodeUpdateAckBody", &ack_success);
    f.add("ack_error", "NodeUpdateAckBody", &ack_error);
    f.finish();
}

#[test]
fn test_message_fixture() {
    let presence_msg = Message::Presence(Presence {
        node_id: make_entity_id(0x01),
        durable: true,
        system_root: None,
    });

    let peer_request = Message::PeerMessage(NodeMessage::Request {
        auth: vec![AuthData(vec![0xAA, 0xBB])],
        request: NodeRequest {
            id: make_request_id(0x10),
            to: make_entity_id(0x20),
            from: make_entity_id(0x30),
            body: NodeRequestBody::Get {
                collection: make_collection_id("users"),
                ids: vec![make_entity_id(0x40)],
            },
        },
    });

    let node_msg_request = NodeMessage::Request {
        auth: vec![],
        request: NodeRequest {
            id: make_request_id(0x50),
            to: make_entity_id(0x60),
            from: make_entity_id(0x70),
            body: NodeRequestBody::Get {
                collection: make_collection_id("items"),
                ids: vec![],
            },
        },
    };

    let node_msg_response = NodeMessage::Response(NodeResponse {
        request_id: make_request_id(0x80),
        from: make_entity_id(0x90),
        to: make_entity_id(0xA0),
        body: NodeResponseBody::Success,
    });

    let node_msg_update = NodeMessage::Update(NodeUpdate {
        id: make_update_id(0x01),
        from: make_entity_id(0xB0),
        to: make_entity_id(0xC0),
        body: NodeUpdateBody::SubscriptionUpdate { items: vec![] },
    });

    let node_msg_update_ack = NodeMessage::UpdateAck(NodeUpdateAck {
        id: make_update_id(0x02),
        from: make_entity_id(0xD0),
        to: make_entity_id(0xE0),
        body: NodeUpdateAckBody::Success,
    });

    let node_msg_unsub = NodeMessage::UnsubscribeQuery {
        from: make_entity_id(0xF0),
        query_id: make_query_id(77),
    };

    let mut f = Fixture::new("message.bin");
    f.add("message_presence", "Message", &presence_msg);
    f.add("message_peer_request", "Message", &peer_request);
    f.add("node_message_request", "NodeMessage", &node_msg_request);
    f.add("node_message_response", "NodeMessage", &node_msg_response);
    f.add("node_message_update", "NodeMessage", &node_msg_update);
    f.add(
        "node_message_update_ack",
        "NodeMessage",
        &node_msg_update_ack,
    );
    f.add(
        "node_message_unsubscribe_query",
        "NodeMessage",
        &node_msg_unsub,
    );
    f.finish();
}

#[test]
fn test_presence_fixture() {
    let presence_durable_no_root = Presence {
        node_id: make_entity_id(0x01),
        durable: true,
        system_root: None,
    };

    let presence_ephemeral_with_root = Presence {
        node_id: make_entity_id(0x02),
        durable: false,
        system_root: Some(Attested {
            payload: make_entity_state(),
            attestations: make_attestation_set_two(),
        }),
    };

    let mut f = Fixture::new("presence.bin");
    f.add(
        "presence_durable_no_root",
        "Presence",
        &presence_durable_no_root,
    );
    f.add(
        "presence_ephemeral_with_root",
        "Presence",
        &presence_ephemeral_with_root,
    );
    f.finish();
}

#[test]
fn test_system_fixture() {
    let sys_root = sys::Item::SysRoot;
    let collection_item = sys::Item::Collection {
        name: "users".to_string(),
    };

    let mut f = Fixture::new("system.bin");
    f.add("sys_root", "sys::Item", &sys_root);
    f.add("collection", "sys::Item", &collection_item);
    f.finish();
}

#[test]
fn test_causal_assertion_fixture() {
    let causal_assertion = CausalAssertion {
        entity_id: make_entity_id(0x10),
        subject: make_clock_single(),
        other: make_clock_multi(),
        relation: CausalRelation::StrictDescends,
    };

    let causal_assertion_equal = CausalAssertion {
        entity_id: make_entity_id(0x20),
        subject: make_clock_empty(),
        other: make_clock_empty(),
        relation: CausalRelation::Equal,
    };

    let mut f = Fixture::new("causal_assertion.bin");
    f.add("strict_descends", "CausalAssertion", &causal_assertion);
    f.add("equal", "CausalAssertion", &causal_assertion_equal);
    f.finish();
}

#[test]
fn test_principal_fixture() {
    let principal = Principal {};

    let mut f = Fixture::new("principal.bin");
    f.add("principal", "Principal", &principal);
    f.finish();
}

#[test]
fn test_attested_event_fixture() {
    let attested_event = Attested {
        payload: make_event(),
        attestations: make_attestation_set_two(),
    };

    let attested_event_empty_attestations = Attested {
        payload: make_event(),
        attestations: make_attestation_set_empty(),
    };

    let mut f = Fixture::new("attested_event.bin");
    f.add("two_attestations", "Attested<Event>", &attested_event);
    f.add(
        "empty_attestations",
        "Attested<Event>",
        &attested_event_empty_attestations,
    );
    f.finish();
}

// ---- Gap fixtures ------------------------------------------------------------
//
// These are new files rather than additions to the fixtures above, so that every
// pre-existing `.bin` stays byte-identical and the TypeScript tests already
// consuming them keep passing unchanged.

/// `sys::Item::Other` is the catch-all variant, bincode tag 2. A reader that only
/// knows SysRoot and Collection mis-decodes it silently. Also covers a collection
/// name that is non-ASCII, and one that is empty.
#[test]
fn test_system_other_fixture() {
    let other = sys::Item::Other;
    let collection_non_ascii = sys::Item::Collection {
        name: NON_ASCII_MIXED.to_string(),
    };
    let collection_empty = sys::Item::Collection {
        name: String::new(),
    };

    let mut f = Fixture::new("system_other.bin");
    f.add("other", "sys::Item", &other);
    f.add("collection_non_ascii", "sys::Item", &collection_non_ascii);
    f.add("collection_empty_name", "sys::Item", &collection_empty);
    f.finish();
}

/// Every integer width that appears anywhere in the proto/ankql wire format, each
/// in the type that actually carries it. A port that widened everything to u32, or
/// narrowed a u64 length prefix, fails here and nowhere else.
#[test]
fn test_integer_widths_fixture() {
    use ankql::ast::{ComparisonOperator, Expr, Literal, PathExpr, Predicate, Selection};

    fn literal(lit: Literal) -> Selection {
        Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("n"))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(lit)),
            },
            order_by: None,
            limit: None,
        }
    }

    let mut f = Fixture::new("integer_widths.bin");

    // bool: 1 byte, 0x00 / 0x01
    f.add(
        "bool_false",
        "Presence.durable",
        &Presence {
            node_id: make_entity_id(0x00),
            durable: false,
            system_root: None,
        },
    );
    f.add(
        "bool_true",
        "Presence.durable",
        &Presence {
            node_id: make_entity_id(0x00),
            durable: true,
            system_root: None,
        },
    );

    // u32: SubscribeQuery.version is the only bare u32 field on the wire.
    for (label, version) in [
        ("u32_zero", 0u32),
        ("u32_one", 1u32),
        ("u32_max", u32::MAX),
        ("u32_high_byte_only", 0x0100_0000u32),
    ] {
        f.add(
            label,
            "NodeRequestBody::SubscribeQuery.version",
            &NodeRequestBody::SubscribeQuery {
                query_id: make_query_id(0),
                collection: make_collection_id("c"),
                selection: make_selection(),
                version,
                known_matches: vec![],
            },
        );
    }

    // u64: Selection.limit, and (implicitly) every sequence/string length prefix.
    for (label, limit) in [
        ("u64_none", None),
        ("u64_zero", Some(0u64)),
        ("u64_one", Some(1u64)),
        ("u64_u32_max_plus_one", Some(u32::MAX as u64 + 1)),
        ("u64_max", Some(u64::MAX)),
    ] {
        let mut selection = make_selection();
        selection.limit = limit;
        f.add(label, "Selection.limit: Option<u64>", &selection);
    }

    // i16 / i32 / i64: signed, two's complement, little-endian - carried by ankql literals.
    f.add("i16_min", "Literal::I16", &literal(Literal::I16(i16::MIN)));
    f.add("i16_neg_one", "Literal::I16", &literal(Literal::I16(-1)));
    f.add("i16_max", "Literal::I16", &literal(Literal::I16(i16::MAX)));
    f.add("i32_min", "Literal::I32", &literal(Literal::I32(i32::MIN)));
    f.add("i32_neg_one", "Literal::I32", &literal(Literal::I32(-1)));
    f.add("i32_max", "Literal::I32", &literal(Literal::I32(i32::MAX)));
    f.add("i64_min", "Literal::I64", &literal(Literal::I64(i64::MIN)));
    f.add("i64_neg_one", "Literal::I64", &literal(Literal::I64(-1)));
    f.add("i64_max", "Literal::I64", &literal(Literal::I64(i64::MAX)));
    // 2^53 + 1: the first integer a JS `number` cannot hold exactly.
    f.add(
        "i64_beyond_js_safe_integer",
        "Literal::I64",
        &literal(Literal::I64(9_007_199_254_740_993)),
    );

    // Fixed-size arrays: EntityId is [u8; 16] and EventId is [u8; 32], both written
    // with NO length prefix. A Vec<u8> of the same content is written WITH one. Each
    // pair below is the direct comparison.
    f.add(
        "fixed_array_16_entity_id",
        "EntityId ([u8; 16], no length prefix)",
        &make_entity_id(0x00),
    );
    f.add(
        "vec_u8_16_operation",
        "Operation.diff (Vec<u8>, u64 length prefix)",
        &make_operation(&(0u8..16).collect::<Vec<u8>>()),
    );
    f.add(
        "fixed_array_32_event_id",
        "EventId ([u8; 32], no length prefix)",
        &make_event_id(0x00),
    );
    f.add(
        "vec_u8_32_operation",
        "Operation.diff (Vec<u8>, u64 length prefix)",
        &make_operation(&(0u8..32).collect::<Vec<u8>>()),
    );

    // Enum variant tags are u32, not u8: NodeResponseBody::Error is tag 6.
    f.add(
        "enum_tag_6",
        "NodeResponseBody::Error",
        &NodeResponseBody::Error(String::new()),
    );

    f.finish();
}

/// Non-ASCII text in every string-bearing position on the wire. Rust strings are
/// UTF-8 behind a byte-count prefix; JS strings are UTF-16, so `str.length` is the
/// wrong number for every one of these.
#[test]
fn test_unicode_fixture() {
    use ankql::ast::{ComparisonOperator, Expr, Literal, PathExpr, Predicate, Selection};

    let mut ops = BTreeMap::new();
    ops.insert(
        NON_ASCII_3BYTE.to_string(),
        vec![make_operation(&[0x01, 0x02])],
    );
    ops.insert(NON_ASCII_4BYTE.to_string(), vec![make_operation(&[0x03])]);
    let operation_set = OperationSet(ops);

    let mut bufs = BTreeMap::new();
    bufs.insert(NON_ASCII_2BYTE.to_string(), vec![0x0A, 0x0B]);
    bufs.insert(NON_ASCII_RTL.to_string(), vec![0x0C]);
    bufs.insert(NON_ASCII_NUL.to_string(), vec![]);
    let state_buffers = StateBuffers(bufs);

    let entity_state = EntityState {
        entity_id: make_entity_id(0x77),
        collection: make_collection_id(NON_ASCII_MIXED),
        state: State {
            state_buffers: state_buffers.clone(),
            head: make_clock_single(),
        },
    };

    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr {
                steps: vec![NON_ASCII_3BYTE.to_string(), NON_ASCII_4BYTE.to_string()],
            })),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String(NON_ASCII_MIXED.to_string()))),
        },
        order_by: None,
        limit: None,
    };

    let mut f = Fixture::new("unicode.bin");
    f.add(
        "collection_id_2byte",
        "CollectionId",
        &make_collection_id(NON_ASCII_2BYTE),
    );
    f.add(
        "collection_id_3byte",
        "CollectionId",
        &make_collection_id(NON_ASCII_3BYTE),
    );
    f.add(
        "collection_id_4byte_surrogate_pair",
        "CollectionId",
        &make_collection_id(NON_ASCII_4BYTE),
    );
    f.add(
        "collection_id_combining_mark",
        "CollectionId",
        &make_collection_id(NON_ASCII_COMBINING),
    );
    f.add(
        "collection_id_rtl",
        "CollectionId",
        &make_collection_id(NON_ASCII_RTL),
    );
    f.add(
        "collection_id_interior_nul",
        "CollectionId",
        &make_collection_id(NON_ASCII_NUL),
    );
    f.add(
        "collection_id_empty",
        "CollectionId",
        &make_collection_id(""),
    );
    f.add(
        "operation_set_non_ascii_keys",
        "OperationSet",
        &operation_set,
    );
    f.add(
        "state_buffers_non_ascii_keys",
        "StateBuffers",
        &state_buffers,
    );
    f.add("entity_state_non_ascii", "EntityState", &entity_state);
    f.add("selection_non_ascii", "ankql::ast::Selection", &selection);
    f.add(
        "response_error_non_ascii",
        "NodeResponseBody::Error",
        &NodeResponseBody::Error(NON_ASCII_MIXED.to_string()),
    );
    f.add(
        "update_ack_error_empty",
        "NodeUpdateAckBody::Error",
        &NodeUpdateAckBody::Error(String::new()),
    );
    f.finish();
}

/// Every empty container the wire format admits. Each is a bare `0u64` length prefix
/// and nothing else, which is exactly where an off-by-one reader loses the next field.
#[test]
fn test_empty_collections_fixture() {
    let empty_operation_set = OperationSet(BTreeMap::new());
    let empty_state_buffers = StateBuffers(BTreeMap::new());
    let empty_state = State {
        state_buffers: StateBuffers(BTreeMap::new()),
        head: make_clock_empty(),
    };
    let empty_operation = make_operation(&[]);

    // A backend key present but mapping to an empty operation list, and a state
    // buffer present but zero-length: both are "empty inside non-empty".
    let mut ops = BTreeMap::new();
    ops.insert("empty_list".to_string(), Vec::<Operation>::new());
    ops.insert("one_empty_diff".to_string(), vec![make_operation(&[])]);
    let sparse_operation_set = OperationSet(ops);

    let mut bufs = BTreeMap::new();
    bufs.insert("zero_len".to_string(), Vec::<u8>::new());
    let sparse_state_buffers = StateBuffers(bufs);

    let mut f = Fixture::new("empty_collections.bin");
    f.add("clock_empty", "Clock", &make_clock_empty());
    f.add("operation_empty_diff", "Operation", &empty_operation);
    f.add("operation_set_empty", "OperationSet", &empty_operation_set);
    f.add(
        "operation_set_empty_inner",
        "OperationSet",
        &sparse_operation_set,
    );
    f.add("state_buffers_empty", "StateBuffers", &empty_state_buffers);
    f.add(
        "state_buffers_empty_inner",
        "StateBuffers",
        &sparse_state_buffers,
    );
    f.add("state_empty", "State", &empty_state);
    f.add(
        "attestation_zero_length",
        "Attestation",
        &make_attestation(&[]),
    );
    f.add(
        "attestation_set_one_empty",
        "AttestationSet",
        &AttestationSet(vec![make_attestation(&[])]),
    );
    f.add(
        "event_fragment_empty",
        "EventFragment",
        &EventFragment {
            operations: OperationSet(BTreeMap::new()),
            parent: make_clock_empty(),
            attestations: make_attestation_set_empty(),
        },
    );
    f.add(
        "state_fragment_empty",
        "StateFragment",
        &StateFragment {
            state: State {
                state_buffers: StateBuffers(BTreeMap::new()),
                head: make_clock_empty(),
            },
            attestations: make_attestation_set_empty(),
        },
    );
    f.add(
        "entity_state_empty",
        "EntityState",
        &EntityState {
            entity_id: make_entity_id(0x00),
            collection: make_collection_id(""),
            state: State {
                state_buffers: StateBuffers(BTreeMap::new()),
                head: make_clock_empty(),
            },
        },
    );
    f.add(
        "attested_entity_state_empty",
        "Attested<EntityState>",
        &Attested {
            payload: EntityState {
                entity_id: make_entity_id(0x00),
                collection: make_collection_id(""),
                state: State {
                    state_buffers: StateBuffers(BTreeMap::new()),
                    head: make_clock_empty(),
                },
            },
            attestations: make_attestation_set_empty(),
        },
    );
    f.finish();
}

/// The `ankql::ast` types reach the wire inside `Fetch` and `SubscribeQuery`, but
/// the fixtures above only ever carry one trivial `name = 'test'` comparison. This
/// walks every variant of every AST enum, plus recursion.
#[test]
fn test_ankql_ast_fixture() {
    use ankql::ast::{
        ComparisonOperator, Expr, InfixOperator, Literal, OrderByItem, OrderDirection, PathExpr,
        Predicate, Selection,
    };

    let mut f = Fixture::new("ankql_ast.bin");

    // --- Literal: all 10 variants ---
    f.add("literal_i16", "Literal", &Literal::I16(-12345));
    f.add("literal_i32", "Literal", &Literal::I32(-1234567890));
    f.add("literal_i64", "Literal", &Literal::I64(-1234567890123456789));
    f.add("literal_f64_zero", "Literal", &Literal::F64(0.0));
    f.add("literal_f64_neg_zero", "Literal", &Literal::F64(-0.0));
    // 0.30000000000000004 - pins the IEEE-754 bits, not a decimal rendering
    f.add("literal_f64_fraction", "Literal", &Literal::F64(0.1 + 0.2));
    f.add("literal_f64_min", "Literal", &Literal::F64(f64::MIN));
    f.add("literal_f64_max", "Literal", &Literal::F64(f64::MAX));
    f.add("literal_f64_epsilon", "Literal", &Literal::F64(f64::EPSILON));
    // serde_json turns NaN and the infinities into `null`, which would make the
    // sidecar lie about the bytes; these three record the Debug form instead.
    f.add_debug("literal_f64_nan", "Literal", &Literal::F64(f64::NAN));
    f.add_debug("literal_f64_inf", "Literal", &Literal::F64(f64::INFINITY));
    f.add_debug(
        "literal_f64_neg_inf",
        "Literal",
        &Literal::F64(f64::NEG_INFINITY),
    );
    f.add("literal_bool_true", "Literal", &Literal::Bool(true));
    f.add("literal_bool_false", "Literal", &Literal::Bool(false));
    f.add(
        "literal_string",
        "Literal",
        &Literal::String("hello".to_string()),
    );
    f.add(
        "literal_string_empty",
        "Literal",
        &Literal::String(String::new()),
    );
    f.add(
        "literal_string_non_ascii",
        "Literal",
        &Literal::String(NON_ASCII_MIXED.to_string()),
    );
    // Literal::EntityId holds a bare `Ulid`, not a proto EntityId: it is written as a
    // 26-char Crockford Base32 STRING behind a u64 length prefix, not as 16 raw bytes.
    f.add(
        "literal_entity_id_ulid_as_string",
        "Literal",
        &Literal::EntityId(make_entity_id(0x00).to_ulid()),
    );
    f.add(
        "literal_object",
        "Literal",
        &Literal::Object(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    );
    f.add("literal_object_empty", "Literal", &Literal::Object(vec![]));
    f.add(
        "literal_binary",
        "Literal",
        &Literal::Binary(vec![0x00, 0xFF]),
    );
    // Literal::Json carries `#[serde(with = "json_as_bytes")]`: the wire form is the
    // UTF-8 bytes of the serialized JSON behind a u64 length prefix, so the sidecar's
    // JSON form is that byte array, not the JSON document.
    f.add(
        "literal_json_object",
        "Literal (json_as_bytes)",
        &Literal::Json(serde_json::json!({"a": 1, "b": [true, null]})),
    );
    f.add(
        "literal_json_null",
        "Literal (json_as_bytes)",
        &Literal::Json(serde_json::Value::Null),
    );

    // --- PathExpr ---
    f.add("path_simple", "PathExpr", &PathExpr::simple("name"));
    f.add(
        "path_multi_step",
        "PathExpr",
        &PathExpr {
            steps: vec![
                "licensing".to_string(),
                "territory".to_string(),
                "code".to_string(),
            ],
        },
    );

    // --- Expr: all 6 variants ---
    f.add("expr_literal", "Expr", &Expr::Literal(Literal::I32(7)));
    f.add("expr_path", "Expr", &Expr::Path(PathExpr::simple("name")));
    f.add("expr_predicate", "Expr", &Expr::Predicate(Predicate::True));
    f.add(
        "expr_infix",
        "Expr",
        &Expr::InfixExpr {
            left: Box::new(Expr::Path(PathExpr::simple("qty"))),
            operator: InfixOperator::Multiply,
            right: Box::new(Expr::Literal(Literal::I64(3))),
        },
    );
    f.add(
        "expr_list",
        "Expr",
        &Expr::ExprList(vec![
            Expr::Literal(Literal::I32(1)),
            Expr::Literal(Literal::I32(2)),
            Expr::Literal(Literal::I32(3)),
        ]),
    );
    f.add("expr_list_empty", "Expr", &Expr::ExprList(vec![]));
    f.add("expr_placeholder", "Expr", &Expr::Placeholder);

    // --- InfixOperator: all 4 ---
    f.add("infix_add", "InfixOperator", &InfixOperator::Add);
    f.add("infix_subtract", "InfixOperator", &InfixOperator::Subtract);
    f.add("infix_multiply", "InfixOperator", &InfixOperator::Multiply);
    f.add("infix_divide", "InfixOperator", &InfixOperator::Divide);

    // --- ComparisonOperator: all 8, each inside a real Comparison ---
    for (label, op) in [
        ("cmp_equal", ComparisonOperator::Equal),
        ("cmp_not_equal", ComparisonOperator::NotEqual),
        ("cmp_greater_than", ComparisonOperator::GreaterThan),
        (
            "cmp_greater_than_or_equal",
            ComparisonOperator::GreaterThanOrEqual,
        ),
        ("cmp_less_than", ComparisonOperator::LessThan),
        (
            "cmp_less_than_or_equal",
            ComparisonOperator::LessThanOrEqual,
        ),
        ("cmp_in", ComparisonOperator::In),
        ("cmp_between", ComparisonOperator::Between),
    ] {
        f.add(
            label,
            "Predicate::Comparison",
            &Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("n"))),
                operator: op,
                right: Box::new(Expr::Literal(Literal::I32(1))),
            },
        );
    }

    // --- Predicate: all 7 variants, including nesting ---
    f.add("predicate_true", "Predicate", &Predicate::True);
    f.add("predicate_false", "Predicate", &Predicate::False);
    f.add("predicate_placeholder", "Predicate", &Predicate::Placeholder);
    f.add(
        "predicate_is_null",
        "Predicate",
        &Predicate::IsNull(Box::new(Expr::Path(PathExpr::simple("deleted_at")))),
    );
    f.add(
        "predicate_not",
        "Predicate",
        &Predicate::Not(Box::new(Predicate::True)),
    );
    f.add(
        "predicate_and",
        "Predicate",
        &Predicate::And(Box::new(Predicate::True), Box::new(Predicate::False)),
    );
    f.add(
        "predicate_or",
        "Predicate",
        &Predicate::Or(Box::new(Predicate::False), Box::new(Predicate::True)),
    );
    f.add(
        "predicate_nested",
        "Predicate",
        &Predicate::And(
            Box::new(Predicate::Or(
                Box::new(Predicate::Comparison {
                    left: Box::new(Expr::Path(PathExpr::simple("status"))),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(Expr::Literal(Literal::String("active".to_string()))),
                }),
                Box::new(Predicate::IsNull(Box::new(Expr::Path(PathExpr::simple(
                    "status",
                ))))),
            )),
            Box::new(Predicate::Not(Box::new(Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr {
                    steps: vec!["licensing".to_string(), "territory".to_string()],
                })),
                operator: ComparisonOperator::In,
                right: Box::new(Expr::ExprList(vec![
                    Expr::Literal(Literal::String("US".to_string())),
                    Expr::Literal(Literal::String("CA".to_string())),
                ])),
            }))),
        ),
    );

    // --- OrderDirection / OrderByItem / Selection ---
    f.add("order_asc", "OrderDirection", &OrderDirection::Asc);
    f.add("order_desc", "OrderDirection", &OrderDirection::Desc);
    f.add(
        "order_by_item",
        "OrderByItem",
        &OrderByItem {
            path: PathExpr::simple("created_at"),
            direction: OrderDirection::Desc,
        },
    );
    f.add(
        "selection_bare",
        "Selection",
        &Selection {
            predicate: Predicate::True,
            order_by: None,
            limit: None,
        },
    );
    // Some(vec![]) and None are different bytes but the same "no ordering" meaning:
    // a port that collapses them breaks byte parity.
    f.add(
        "selection_order_by_empty_vec",
        "Selection",
        &Selection {
            predicate: Predicate::True,
            order_by: Some(vec![]),
            limit: None,
        },
    );
    f.add(
        "selection_full",
        "Selection",
        &Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("name"))),
                operator: ComparisonOperator::NotEqual,
                right: Box::new(Expr::Literal(Literal::String(NON_ASCII_MIXED.to_string()))),
            },
            order_by: Some(vec![
                OrderByItem {
                    path: PathExpr::simple("created_at"),
                    direction: OrderDirection::Desc,
                },
                OrderByItem {
                    path: PathExpr {
                        steps: vec!["licensing".to_string(), "territory".to_string()],
                    },
                    direction: OrderDirection::Asc,
                },
            ]),
            limit: Some(100),
        },
    );

    f.finish();
}

/// `NodeRequestBody` cases the original request fixture does not reach: empty
/// vectors in every variant, and a non-trivial selection inside Fetch/SubscribeQuery.
#[test]
fn test_request_edge_fixture() {
    use ankql::ast::{
        ComparisonOperator, Expr, Literal, OrderByItem, OrderDirection, PathExpr, Predicate,
        Selection,
    };

    let rich_selection = Selection {
        predicate: Predicate::And(
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("status"))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("active".to_string()))),
            }),
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("score"))),
                operator: ComparisonOperator::GreaterThanOrEqual,
                right: Box::new(Expr::Literal(Literal::F64(1.5))),
            }),
        ),
        order_by: Some(vec![OrderByItem {
            path: PathExpr::simple("score"),
            direction: OrderDirection::Desc,
        }]),
        limit: Some(u32::MAX as u64 + 1),
    };

    let mut f = Fixture::new("request_edge.bin");
    f.add(
        "commit_transaction_no_events",
        "NodeRequestBody",
        &NodeRequestBody::CommitTransaction {
            id: make_transaction_id(0x09),
            events: vec![],
        },
    );
    f.add(
        "commit_transaction_two_events",
        "NodeRequestBody",
        &NodeRequestBody::CommitTransaction {
            id: make_transaction_id(0x0A),
            events: vec![
                Attested {
                    payload: make_event(),
                    attestations: make_attestation_set_two(),
                },
                Attested {
                    payload: Event {
                        collection: make_collection_id(NON_ASCII_3BYTE),
                        entity_id: make_entity_id(0x11),
                        operations: OperationSet(BTreeMap::new()),
                        parent: make_clock_empty(),
                    },
                    attestations: make_attestation_set_empty(),
                },
            ],
        },
    );
    f.add(
        "get_no_ids",
        "NodeRequestBody",
        &NodeRequestBody::Get {
            collection: make_collection_id(""),
            ids: vec![],
        },
    );
    f.add(
        "get_events_no_ids",
        "NodeRequestBody",
        &NodeRequestBody::GetEvents {
            collection: make_collection_id(NON_ASCII_4BYTE),
            event_ids: vec![],
        },
    );
    f.add(
        "fetch_rich_selection_no_known_matches",
        "NodeRequestBody",
        &NodeRequestBody::Fetch {
            collection: make_collection_id("users"),
            selection: rich_selection.clone(),
            known_matches: vec![],
        },
    );
    f.add(
        "subscribe_query_max_version_two_known",
        "NodeRequestBody",
        &NodeRequestBody::SubscribeQuery {
            query_id: make_query_id(u64::MAX),
            collection: make_collection_id("users"),
            selection: rich_selection,
            version: u32::MAX,
            known_matches: vec![
                make_known_entity(),
                KnownEntity {
                    entity_id: make_entity_id(0x99),
                    head: make_clock_empty(),
                },
            ],
        },
    );
    f.add(
        "node_request_all_zero_ids",
        "NodeRequest",
        &NodeRequest {
            id: make_request_id(0x00),
            to: make_entity_id(0x00),
            from: make_entity_id(0x00),
            body: NodeRequestBody::Get {
                collection: make_collection_id(""),
                ids: vec![],
            },
        },
    );
    f.finish();
}

/// `NodeResponseBody` cases: every collection-bearing variant with an empty
/// collection, and the error string at its boundaries.
#[test]
fn test_response_edge_fixture() {
    let mut f = Fixture::new("response_edge.bin");
    f.add(
        "fetch_empty",
        "NodeResponseBody",
        &NodeResponseBody::Fetch(vec![]),
    );
    f.add(
        "get_empty",
        "NodeResponseBody",
        &NodeResponseBody::Get(vec![]),
    );
    f.add(
        "get_events_empty",
        "NodeResponseBody",
        &NodeResponseBody::GetEvents(vec![]),
    );
    f.add(
        "query_subscribed_empty",
        "NodeResponseBody",
        &NodeResponseBody::QuerySubscribed {
            query_id: make_query_id(0),
            deltas: vec![],
        },
    );
    f.add(
        "query_subscribed_three_deltas",
        "NodeResponseBody",
        &NodeResponseBody::QuerySubscribed {
            query_id: make_query_id(7),
            deltas: vec![
                EntityDelta {
                    entity_id: make_entity_id(0x01),
                    collection: make_collection_id("a"),
                    content: DeltaContent::StateSnapshot {
                        state: make_state_fragment(),
                    },
                },
                EntityDelta {
                    entity_id: make_entity_id(0x02),
                    collection: make_collection_id(NON_ASCII_2BYTE),
                    content: DeltaContent::EventBridge { events: vec![] },
                },
                EntityDelta {
                    entity_id: make_entity_id(0x03),
                    collection: make_collection_id("c"),
                    content: DeltaContent::StateAndRelation {
                        state: make_state_fragment(),
                        relation: CausalAssertionFragment {
                            relation: CausalRelation::Disjoint {
                                gca: None,
                                subject_root: make_event_id(0x01),
                                other_root: make_event_id(0x02),
                            },
                            attestations: make_attestation_set_empty(),
                        },
                    },
                },
            ],
        },
    );
    f.add(
        "error_empty_string",
        "NodeResponseBody",
        &NodeResponseBody::Error(String::new()),
    );
    f.add(
        "error_non_ascii",
        "NodeResponseBody",
        &NodeResponseBody::Error(NON_ASCII_MIXED.to_string()),
    );
    f.finish();
}

/// Update-path cases: empty item lists, empty predicate relevance, and a
/// `StateAndEvent` whose event vector is empty.
#[test]
fn test_update_edge_fixture() {
    let mut f = Fixture::new("update_edge.bin");
    f.add(
        "subscription_update_no_items",
        "NodeUpdateBody",
        &NodeUpdateBody::SubscriptionUpdate { items: vec![] },
    );
    f.add(
        "event_only_empty",
        "UpdateContent",
        &UpdateContent::EventOnly(vec![]),
    );
    f.add(
        "state_and_event_no_events",
        "UpdateContent",
        &UpdateContent::StateAndEvent(make_state_fragment(), vec![]),
    );
    f.add(
        "item_no_predicate_relevance",
        "SubscriptionUpdateItem",
        &SubscriptionUpdateItem {
            entity_id: make_entity_id(0x00),
            collection: make_collection_id(""),
            content: UpdateContent::EventOnly(vec![]),
            predicate_relevance: vec![],
        },
    );
    f.add(
        "item_three_predicate_relevance",
        "SubscriptionUpdateItem",
        &SubscriptionUpdateItem {
            entity_id: make_entity_id(0x05),
            collection: make_collection_id(NON_ASCII_RTL),
            content: UpdateContent::StateAndEvent(
                make_state_fragment(),
                vec![make_event_fragment(), make_event_fragment()],
            ),
            predicate_relevance: vec![
                (make_query_id(0), MembershipChange::Initial),
                (make_query_id(u64::MAX), MembershipChange::Add),
                (make_query_id(1), MembershipChange::Remove),
            ],
        },
    );
    f.add(
        "update_ack_error_non_ascii",
        "NodeUpdateAckBody",
        &NodeUpdateAckBody::Error(NON_ASCII_MIXED.to_string()),
    );
    f.finish();
}

/// `EventId::from_parts` hashes the bincode encodings of `entity_id`, `operations`
/// and `parent` with SHA-256, in that order. An id that matches here proves the
/// TypeScript encoder produced identical bytes for all three at once - sharper than
/// comparing any one of them, because the hash cannot be matched field by field.
///
/// The `.bin` holds each computed EventId (32 raw bytes) followed by the `Event` it
/// was derived from, so a failing port can see exactly what it was supposed to hash.
#[test]
fn test_event_id_derivation_fixture() {
    let mut f = Fixture::new("event_id_derivation.bin");

    let cases: Vec<(&str, Event)> = vec![
        (
            "genesis_empty_parent",
            Event {
                collection: make_collection_id("test_collection"),
                entity_id: make_entity_id(0x00),
                operations: OperationSet(BTreeMap::new()),
                parent: make_clock_empty(),
            },
        ),
        ("standard_event", make_event()),
        ("non_ascii_backend_keys", {
            let mut ops = BTreeMap::new();
            ops.insert(
                NON_ASCII_3BYTE.to_string(),
                vec![make_operation(&[0x01, 0x02, 0x03])],
            );
            Event {
                collection: make_collection_id(NON_ASCII_MIXED),
                entity_id: make_entity_id(0xAB),
                operations: OperationSet(ops),
                parent: make_clock_multi(),
            }
        }),
        ("collection_is_not_hashed", {
            // Same entity_id/operations/parent as `standard_event` but a different
            // collection. `from_parts` deliberately excludes collection, so this id
            // must come out IDENTICAL to `standard_event`'s.
            let mut event = make_event();
            event.collection = make_collection_id("a_completely_different_collection");
            event
        }),
    ];

    for (label, event) in &cases {
        f.add(label, "EventId (= Event::id())", &event.id());
    }
    for (label, event) in &cases {
        f.add(&format!("{label}__input_event"), "Event", event);
    }
    f.finish();
}
