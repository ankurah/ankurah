/// Bincode fixture tests for ankurah-proto types.
///
/// Each test function serializes known, deterministic values to bincode.
/// - If `OVERWRITE_FIXTURES` env var is set: write the binary to the fixture file.
/// - If NOT set: read the fixture file and assert the bytes match exactly.
///
/// Run with `OVERWRITE_FIXTURES=1 cargo test -p ankurah-proto --test bincode_fixtures` to regenerate.
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use ankurah_proto::auth::{Attestation, AttestationSet, Attested, AuthData};
use ankurah_proto::clock::Clock;
use ankurah_proto::collection::CollectionId;
use ankurah_proto::data::{
    EntityState, Event, EventFragment, EventId, Operation, OperationSet, State, StateBuffers,
    StateFragment,
};
use ankurah_proto::message::{Message, NodeMessage};
use ankurah_proto::peering::Presence;
use ankurah_proto::request::{
    CausalAssertionFragment, CausalRelation, DeltaContent, EntityDelta, KnownEntity, NodeRequest,
    NodeRequestBody, NodeResponse, NodeResponseBody, RequestId,
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

fn check_or_write_fixture(name: &str, data: &[u8]) {
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
        assert_eq!(
            data,
            &expected[..],
            "Fixture mismatch for {}",
            name
        );
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
        vec![
            make_operation(&[0xAA, 0xBB]),
            make_operation(&[0xCC]),
        ],
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

// ---- Fixture Tests ----

#[test]
fn test_ids_fixture() {
    let entity_id = EntityId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);

    let event_id = EventId::from_bytes([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
        24, 25, 26, 27, 28, 29, 30, 31,
    ]);

    let transaction_id = make_transaction_id(0x10);
    let request_id = make_request_id(0x20);
    let query_id = make_query_id(42);
    let update_id = make_update_id(0x30);
    let collection_id = make_collection_id("test_collection");

    let mut data = Vec::new();
    data.extend(bincode::serialize(&entity_id).unwrap());
    data.extend(bincode::serialize(&event_id).unwrap());
    data.extend(bincode::serialize(&transaction_id).unwrap());
    data.extend(bincode::serialize(&request_id).unwrap());
    data.extend(bincode::serialize(&query_id).unwrap());
    data.extend(bincode::serialize(&update_id).unwrap());
    data.extend(bincode::serialize(&collection_id).unwrap());

    check_or_write_fixture("ids.bin", &data);
}

#[test]
fn test_clock_fixture() {
    let empty = make_clock_empty();
    let single = make_clock_single();
    let multi = make_clock_multi();

    let mut data = Vec::new();
    data.extend(bincode::serialize(&empty).unwrap());
    data.extend(bincode::serialize(&single).unwrap());
    data.extend(bincode::serialize(&multi).unwrap());

    check_or_write_fixture("clock.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&auth_data_bytes).unwrap());
    data.extend(bincode::serialize(&auth_data_empty).unwrap());
    data.extend(bincode::serialize(&attestation).unwrap());
    data.extend(bincode::serialize(&attestation_set_empty).unwrap());
    data.extend(bincode::serialize(&attestation_set_two).unwrap());
    data.extend(bincode::serialize(&attested_entity_state).unwrap());

    check_or_write_fixture("auth.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&operation).unwrap());
    data.extend(bincode::serialize(&operation_set).unwrap());
    data.extend(bincode::serialize(&state_buffers).unwrap());
    data.extend(bincode::serialize(&state).unwrap());
    data.extend(bincode::serialize(&state_fragment).unwrap());
    data.extend(bincode::serialize(&event).unwrap());
    data.extend(bincode::serialize(&event_fragment).unwrap());
    data.extend(bincode::serialize(&entity_state).unwrap());

    check_or_write_fixture("data.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&node_request).unwrap());
    data.extend(bincode::serialize(&commit_tx).unwrap());
    data.extend(bincode::serialize(&get).unwrap());
    data.extend(bincode::serialize(&get_events).unwrap());
    data.extend(bincode::serialize(&fetch).unwrap());
    data.extend(bincode::serialize(&subscribe).unwrap());

    check_or_write_fixture("request.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&node_response).unwrap());
    data.extend(bincode::serialize(&commit_complete).unwrap());
    data.extend(bincode::serialize(&fetch_resp).unwrap());
    data.extend(bincode::serialize(&get_resp).unwrap());
    data.extend(bincode::serialize(&get_events_resp).unwrap());
    data.extend(bincode::serialize(&query_subscribed).unwrap());
    data.extend(bincode::serialize(&success).unwrap());
    data.extend(bincode::serialize(&error).unwrap());

    check_or_write_fixture("response.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&equal).unwrap());
    data.extend(bincode::serialize(&strict_descends).unwrap());
    data.extend(bincode::serialize(&strict_ascends).unwrap());
    data.extend(bincode::serialize(&diverged_since).unwrap());
    data.extend(bincode::serialize(&disjoint_some).unwrap());
    data.extend(bincode::serialize(&disjoint_none).unwrap());
    data.extend(bincode::serialize(&budget_exceeded).unwrap());
    data.extend(bincode::serialize(&causal_assertion_fragment).unwrap());

    check_or_write_fixture("causal.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&state_snapshot).unwrap());
    data.extend(bincode::serialize(&event_bridge).unwrap());
    data.extend(bincode::serialize(&state_and_relation).unwrap());
    data.extend(bincode::serialize(&entity_delta_snapshot).unwrap());
    data.extend(bincode::serialize(&entity_delta_bridge).unwrap());
    data.extend(bincode::serialize(&entity_delta_state_rel).unwrap());
    data.extend(bincode::serialize(&known_entity).unwrap());

    check_or_write_fixture("delta.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&node_update).unwrap());
    data.extend(bincode::serialize(&subscription_update_item).unwrap());
    data.extend(bincode::serialize(&event_only).unwrap());
    data.extend(bincode::serialize(&state_and_event).unwrap());
    data.extend(bincode::serialize(&membership_initial).unwrap());
    data.extend(bincode::serialize(&membership_add).unwrap());
    data.extend(bincode::serialize(&membership_remove).unwrap());
    data.extend(bincode::serialize(&node_update_ack).unwrap());
    data.extend(bincode::serialize(&ack_success).unwrap());
    data.extend(bincode::serialize(&ack_error).unwrap());

    check_or_write_fixture("update.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&presence_msg).unwrap());
    data.extend(bincode::serialize(&peer_request).unwrap());
    data.extend(bincode::serialize(&node_msg_request).unwrap());
    data.extend(bincode::serialize(&node_msg_response).unwrap());
    data.extend(bincode::serialize(&node_msg_update).unwrap());
    data.extend(bincode::serialize(&node_msg_update_ack).unwrap());
    data.extend(bincode::serialize(&node_msg_unsub).unwrap());

    check_or_write_fixture("message.bin", &data);
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

    let mut data = Vec::new();
    data.extend(bincode::serialize(&presence_durable_no_root).unwrap());
    data.extend(bincode::serialize(&presence_ephemeral_with_root).unwrap());

    check_or_write_fixture("presence.bin", &data);
}

#[test]
fn test_system_fixture() {
    let sys_root = sys::Item::SysRoot;
    let collection_item = sys::Item::Collection {
        name: "users".to_string(),
    };

    let mut data = Vec::new();
    data.extend(bincode::serialize(&sys_root).unwrap());
    data.extend(bincode::serialize(&collection_item).unwrap());

    check_or_write_fixture("system.bin", &data);
}
