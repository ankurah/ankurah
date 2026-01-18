//! Tests for LWW Causal Register fix: per-property last-write competitors.
//!
//! These tests verify that:
//! 1. Stored per-property last-write values compete even when not in current head
//! 2. Concurrent candidates resolve by lexicographic EventId
//! 3. Causal descendants win over ancestors
//! 4. Missing DAG info causes InsufficientCausalInfo error

mod common;
use ankurah::{core::node::nocache, policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;

/// Test: Concurrent candidates resolve by EventId (lexicographic tiebreak)
///
/// Two truly concurrent events writing to the same property.
/// Higher EventId wins.
#[tokio::test]
async fn test_lww_concurrent_lexicographic_fallback() -> Result<()> {
    use ankurah::core::event_dag::DagCausalContext;
    use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
    use ankurah::core::value::Value;
    use ankurah::proto::{Clock, CollectionId, EntityId, Event, EventId, Operation, OperationSet};
    use std::collections::BTreeMap;

    fn make_lww_operation(title: &str) -> Operation {
        use ankurah::core::value::Value;
        use serde::{Deserialize, Serialize};
        use std::collections::BTreeMap;
        #[derive(Serialize, Deserialize)]
        struct LWWDiff {
            version: u8,
            data: Vec<u8>,
        }
        let changes: BTreeMap<String, Option<Value>> =
            [(String::from("title"), Some(Value::String(title.to_string())))]
                .into_iter()
                .collect();
        let data = bincode::serialize(&changes).unwrap();
        let diff = bincode::serialize(&LWWDiff { version: 1, data }).unwrap();
        Operation { diff }
    }

    // Genesis event (empty operations = entity creation)
    let collection = CollectionId::fixed_name("test");
    let entity_id = EntityId::from_bytes([0u8; 16]);

    // Create genesis event (parent is empty clock = create)
    let genesis_event = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: OperationSet(BTreeMap::new()),
        parent: Clock::default(),
    };
    let genesis_id = genesis_event.id();

    // Create two events with same parent (concurrent)
    // Use different titles to get different EventIds
    let event_a = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-A")]);
            OperationSet(ops)
        },
        parent: Clock::from(genesis_id.clone()),
    };
    let id_a = event_a.id();

    let event_b = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-B")]);
            OperationSet(ops)
        },
        parent: Clock::from(genesis_id.clone()),
    };
    let id_b = event_b.id();

    // Create DAG: genesis <- A, genesis <- B (A and B concurrent)
    let mut dag = BTreeMap::new();
    dag.insert(genesis_id.clone(), vec![]);
    dag.insert(id_a.clone(), vec![genesis_id.clone()]);
    dag.insert(id_b.clone(), vec![genesis_id.clone()]);
    let context = DagCausalContext::new(&dag);

    // Create LWW backend
    let backend = LWWBackend::new();

    // Apply both events in same layer (concurrent)
    backend.apply_layer_with_context(&[], &[&event_a, &event_b], &[], &context)?;

    // The one with higher EventId should win
    let title = backend.get(&"title".to_string());
    let expected_title = if id_b > id_a { "Title-B" } else { "Title-A" };
    assert_eq!(
        title,
        Some(Value::String(expected_title.to_string())),
        "Higher EventId should win for concurrent events"
    );

    Ok(())
}

/// Test: Causal descendant wins even if lexicographically lower
///
/// If event B descends from event A (B > A causally), B wins regardless of EventId.
#[tokio::test]
async fn test_lww_causal_descends_wins() -> Result<()> {
    use ankurah::core::event_dag::DagCausalContext;
    use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
    use ankurah::core::value::Value;
    use ankurah::proto::{Clock, CollectionId, EntityId, Event, Operation, OperationSet};
    use std::collections::BTreeMap;

    fn make_lww_operation(title: &str) -> Operation {
        use ankurah::core::value::Value;
        use serde::{Deserialize, Serialize};
        use std::collections::BTreeMap;
        #[derive(Serialize, Deserialize)]
        struct LWWDiff {
            version: u8,
            data: Vec<u8>,
        }
        let changes: BTreeMap<String, Option<Value>> =
            [(String::from("title"), Some(Value::String(title.to_string())))]
                .into_iter()
                .collect();
        let data = bincode::serialize(&changes).unwrap();
        let diff = bincode::serialize(&LWWDiff { version: 1, data }).unwrap();
        Operation { diff }
    }

    let collection = CollectionId::fixed_name("test");
    let entity_id = EntityId::from_bytes([0u8; 16]);

    // Genesis event
    let genesis_event = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: OperationSet(BTreeMap::new()),
        parent: Clock::default(),
    };
    let genesis_id = genesis_event.id();

    // Event A sets title
    let event_a = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-A")]);
            OperationSet(ops)
        },
        parent: Clock::from(genesis_id.clone()),
    };
    let id_a = event_a.id();

    // Event B descends from A (B's parent is A)
    let event_b = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-B")]);
            OperationSet(ops)
        },
        parent: Clock::from(id_a.clone()), // B's parent is A
    };
    let id_b = event_b.id();

    // Create DAG: genesis <- A <- B (B descends from A)
    let mut dag = BTreeMap::new();
    dag.insert(genesis_id.clone(), vec![]);
    dag.insert(id_a.clone(), vec![genesis_id.clone()]);
    dag.insert(id_b.clone(), vec![id_a.clone()]);
    let context = DagCausalContext::new(&dag);

    // Create LWW backend and first apply A
    let backend = LWWBackend::new();
    backend.apply_operations_with_event(
        event_a.operations.get("lww").unwrap(),
        id_a.clone(),
    )?;

    // Verify A's value is stored
    assert_eq!(backend.get(&"title".to_string()), Some(Value::String("Title-A".to_string())));

    // Now apply B in a layer, with A in current_head
    // Since B descends from A, B should win regardless of EventId comparison
    backend.apply_layer_with_context(&[], &[&event_b], &[id_a.clone()], &context)?;

    let title = backend.get(&"title".to_string());
    assert_eq!(
        title,
        Some(Value::String("Title-B".to_string())),
        "Causal descendant (B) should win over ancestor (A) regardless of EventId"
    );

    Ok(())
}

/// Test: Stored ancestor not in DAG as key still works via cycle detection
///
/// When stored event A is an ancestor of layer event B, A appears in DAG only
/// as a parent reference (dag[B] = [A]), not as a key. The algorithm should
/// still correctly determine that B > A causally.
#[tokio::test]
async fn test_lww_stored_ancestor_not_in_dag_key() -> Result<()> {
    use ankurah::core::event_dag::DagCausalContext;
    use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
    use ankurah::core::value::Value;
    use ankurah::proto::{Clock, CollectionId, EntityId, Event, Operation, OperationSet};
    use std::collections::BTreeMap;

    fn make_lww_operation(title: &str) -> Operation {
        use ankurah::core::value::Value;
        use serde::{Deserialize, Serialize};
        use std::collections::BTreeMap;
        #[derive(Serialize, Deserialize)]
        struct LWWDiff {
            version: u8,
            data: Vec<u8>,
        }
        let changes: BTreeMap<String, Option<Value>> =
            [(String::from("title"), Some(Value::String(title.to_string())))]
                .into_iter()
                .collect();
        let data = bincode::serialize(&changes).unwrap();
        let diff = bincode::serialize(&LWWDiff { version: 1, data }).unwrap();
        Operation { diff }
    }

    let collection = CollectionId::fixed_name("test");
    let entity_id = EntityId::from_bytes([0u8; 16]);

    // Genesis event (creates entity)
    let genesis_event = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: OperationSet(BTreeMap::new()),
        parent: Clock::default(),
    };
    let genesis_id = genesis_event.id();

    // Event A sets title (parent is genesis)
    let event_a = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-A")]);
            OperationSet(ops)
        },
        parent: Clock::from(genesis_id.clone()),
    };
    let id_a = event_a.id();

    // Event B updates title (parent is A)
    let event_b = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-B")]);
            OperationSet(ops)
        },
        parent: Clock::from(id_a.clone()),
    };
    let id_b = event_b.id();

    // DAG only contains B, not A or genesis (simulating partial DAG from layer)
    // A appears only as a parent reference: dag[B] = [A]
    let mut dag = BTreeMap::new();
    dag.insert(id_b.clone(), vec![id_a.clone()]);
    let context = DagCausalContext::new(&dag);

    // Backend has stored value from A
    let backend = LWWBackend::new();
    backend.apply_operations_with_event(
        event_a.operations.get("lww").unwrap(),
        id_a.clone(),
    )?;
    assert_eq!(backend.get(&"title".to_string()), Some(Value::String("Title-A".to_string())));

    // Apply B in a layer - A is stored but not in DAG as key
    // Algorithm should determine B > A via cycle detection
    backend.apply_layer_with_context(&[], &[&event_b], &[id_a.clone()], &context)?;

    let title = backend.get(&"title".to_string());
    assert_eq!(
        title,
        Some(Value::String("Title-B".to_string())),
        "Layer event B should win over stored ancestor A even when A not in DAG as key"
    );

    Ok(())
}

/// Test: Missing DAG info causes InsufficientCausalInfo error
#[tokio::test]
async fn test_lww_insufficient_dag_bails() -> Result<()> {
    use ankurah::core::error::MutationError;
    use ankurah::core::event_dag::DagCausalContext;
    use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
    use ankurah::core::value::Value;
    use ankurah::proto::{Clock, CollectionId, EntityId, Event, Operation, OperationSet};
    use std::collections::BTreeMap;

    fn make_lww_operation(title: &str) -> Operation {
        use ankurah::core::value::Value;
        use serde::{Deserialize, Serialize};
        use std::collections::BTreeMap;
        #[derive(Serialize, Deserialize)]
        struct LWWDiff {
            version: u8,
            data: Vec<u8>,
        }
        let changes: BTreeMap<String, Option<Value>> =
            [(String::from("title"), Some(Value::String(title.to_string())))]
                .into_iter()
                .collect();
        let data = bincode::serialize(&changes).unwrap();
        let diff = bincode::serialize(&LWWDiff { version: 1, data }).unwrap();
        Operation { diff }
    }

    let collection = CollectionId::fixed_name("test");
    let entity_id = EntityId::from_bytes([0u8; 16]);

    // Genesis event
    let genesis_event = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: OperationSet(BTreeMap::new()),
        parent: Clock::default(),
    };
    let genesis_id = genesis_event.id();

    // Event A
    let event_a = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-A")]);
            OperationSet(ops)
        },
        parent: Clock::from(genesis_id.clone()),
    };
    let id_a = event_a.id();

    // Event B (concurrent to A)
    let event_b = Event {
        collection: collection.clone(),
        entity_id: entity_id.clone(),
        operations: {
            let mut ops = BTreeMap::new();
            ops.insert("lww".to_string(), vec![make_lww_operation("Title-B")]);
            OperationSet(ops)
        },
        parent: Clock::from(genesis_id.clone()),
    };
    let id_b = event_b.id();

    // INCOMPLETE DAG: only has genesis and A, NOT B
    let mut dag = BTreeMap::new();
    dag.insert(genesis_id.clone(), vec![]);
    dag.insert(id_a.clone(), vec![genesis_id.clone()]);
    // B is NOT in DAG
    let context = DagCausalContext::new(&dag);

    let backend = LWWBackend::new();

    // Store A's value
    backend.apply_operations_with_event(
        event_a.operations.get("lww").unwrap(),
        id_a.clone(),
    )?;

    // Try to apply B which is not in DAG
    // This should fail because B is not in DAG and we can't determine relationship
    let result = backend.apply_layer_with_context(&[], &[&event_b], &[id_a.clone()], &context);

    match result {
        Err(MutationError::InsufficientCausalInfo { event_a: ea, event_b: eb }) => {
            // Expected - verify it mentions the right events
            // One of these should be id_b (the unknown one)
            assert!(ea == id_b || eb == id_b, "Error should mention the unknown event id_b");
            Ok(())
        }
        Ok(_) => {
            panic!("Expected InsufficientCausalInfo error, but got Ok")
        }
        Err(e) => {
            panic!("Expected InsufficientCausalInfo error, but got: {:?}", e)
        }
    }
}

/// Test: Stored value from non-head event competes in multi-node scenario
///
/// This is the full integration test for the bug scenario.
#[tokio::test]
async fn test_lww_full_scenario_non_head_competes() -> Result<()> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e = ephemeral.context(DEFAULT_CONTEXT)?;
    let mut dag = TestDag::new();

    // 1. Create entity on durable
    let record_id = {
        let trx = ctx_d.begin();
        let record = trx.create(&Record { title: "Genesis".to_owned(), artist: "Original".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // 2. Ephemeral subscribes and gets entity
    let _lq = ctx_e.query_wait::<RecordView>(
        nocache(format!("id = '{}'", record_id).as_str())?
    ).await?;
    let record_e = ctx_e.get::<RecordView>(record_id).await?;
    assert_eq!(record_e.title().unwrap(), "Genesis");

    // 3. Durable makes changes: B1 (title), B2 (artist only)
    {
        let record_d = ctx_d.get::<RecordView>(record_id).await?;
        let trx = ctx_d.begin();
        record_d.edit(&trx)?.title().set(&"Title-from-D".to_owned())?;
        dag.enumerate(trx.commit_and_return_events().await?); // B1
    }
    {
        let record_d = ctx_d.get::<RecordView>(record_id).await?;
        let trx = ctx_d.begin();
        record_d.edit(&trx)?.artist().set(&"Artist-from-D".to_owned())?;
        dag.enumerate(trx.commit_and_return_events().await?); // B2
    }

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 4. Now ephemeral should have the state with title from B1, artist from B2
    let final_e = ctx_e.get::<RecordView>(record_id).await?;
    assert_eq!(final_e.title().unwrap(), "Title-from-D");
    assert_eq!(final_e.artist().unwrap(), "Artist-from-D");

    Ok(())
}
