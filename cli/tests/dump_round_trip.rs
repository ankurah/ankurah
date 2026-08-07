use std::{collections::BTreeSet, path::Path, sync::Arc};

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_cli::dump::{dump, load, validate, DumpError};
use ankurah_core::property::backend::{LWWBackend, PropertyBackend};
use ankurah_core::storage::{StorageDump, StorageDumpItem, StorageEngine};
use ankurah_core::value::Value;
use ankurah_proto::{Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State, StateBuffers};
use ankurah_storage_postgres_0_9::Postgres;
use ankurah_storage_sled_0_9::SledStorageEngine;
use ankurah_storage_sqlite_0_9::SqliteStorageEngine;
use anyhow::Result;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{pin_mut, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    name: String,
    year: String,
}

#[tokio::test]
async fn sqlite_and_sled_round_trip_without_materialized_values() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.sqlite");
    let source = Arc::new(SqliteStorageEngine::open(&source_path).await?);
    let album_id = populate(source.clone()).await?;

    let dump_file = directory.path().join("sqlite.akdump");
    let sqlite_summary = dump(source.as_ref(), "sqlite", &dump_file).await?;
    assert_eq!(sqlite_summary.states, 2);
    assert_eq!(sqlite_summary.events, 3);
    assert_eq!(validate(&dump_file)?, sqlite_summary);
    assert!(matches!(
        dump(source.as_ref(), "sqlite", &dump_file).await,
        Err(DumpError::OutputExists(path)) if path == dump_file
    ));

    let tampered = directory.path().join("tampered.akdump");
    tamper_header(&dump_file, &tampered)?;
    assert!(matches!(validate(&tampered), Err(DumpError::ChecksumMismatch { .. })));

    let broken_causality = directory.path().join("broken-causality.akdump");
    make_state_head_stale_and_resign(&dump_file, &broken_causality)?;
    let untouched = SledStorageEngine::with_path(directory.path().join("untouched-sled"))?;
    assert!(matches!(load(&untouched, &broken_causality).await, Err(DumpError::InvalidRecord { .. })));
    assert_dump_empty(&untouched).await?;

    let unsafe_property = directory.path().join("unsafe-property.akdump");
    add_reserved_property_and_resign(&dump_file, &unsafe_property)?;
    let untouched = SledStorageEngine::with_path(directory.path().join("second-untouched-sled"))?;
    assert!(matches!(load(&untouched, &unsafe_property).await, Err(DumpError::InvalidRecord { .. })));
    assert_dump_empty(&untouched).await?;

    let sled_path = directory.path().join("target-sled");
    let sled = SledStorageEngine::with_path(sled_path)?;
    assert_eq!(load(&sled, &dump_file).await?, sqlite_summary);
    compare_entity(source.as_ref(), &sled, album_id).await?;
    assert_materialized_query(&sled).await?;
    assert!(matches!(load(&sled, &dump_file).await, Err(DumpError::TargetNotEmpty)));

    let sled_dump = directory.path().join("sled.akdump");
    assert_eq!(dump(&sled, "sled", &sled_dump).await?, sqlite_summary);

    let sqlite_target = SqliteStorageEngine::open(directory.path().join("target.sqlite")).await?;
    assert_eq!(load(&sqlite_target, &sled_dump).await?, sqlite_summary);
    compare_entity(&sled, &sqlite_target, album_id).await?;
    assert_materialized_query(&sqlite_target).await?;
    Ok(())
}

#[tokio::test]
async fn postgres_loads_and_dumps_the_same_logical_dump() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source = Arc::new(SqliteStorageEngine::open(directory.path().join("source.sqlite")).await?);
    let album_id = populate(source.clone()).await?;
    let dump_file = directory.path().join("source.akdump");
    let summary = dump(source.as_ref(), "sqlite", &dump_file).await?;

    let container = postgres::Postgres::default().with_db_name("ankurah").with_user("postgres").with_password("postgres").start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("host={host} port={port} user=postgres password=postgres dbname=ankurah");
    let postgres = Postgres::open(&url).await?;

    assert_eq!(load(&postgres, &dump_file).await?, summary);
    compare_entity(source.as_ref(), &postgres, album_id).await?;
    assert_materialized_query(&postgres).await?;

    let postgres_dump = directory.path().join("postgres.akdump");
    assert_eq!(dump(&postgres, "postgres", &postgres_dump).await?, summary);
    assert_eq!(validate(&postgres_dump)?, summary);

    let restored = SledStorageEngine::with_path(directory.path().join("postgres-dump-target"))?;
    assert_eq!(load(&restored, &postgres_dump).await?, summary);
    compare_entity(&postgres, &restored, album_id).await?;
    assert_materialized_query(&restored).await?;

    assert_dump_stream_crosses_cursor_pages(&postgres).await?;
    Ok(())
}

#[tokio::test]
async fn sqlite_dump_stream_crosses_cursor_pages_without_skips() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    assert_dump_stream_crosses_cursor_pages(&storage).await
}

#[tokio::test]
async fn sled_dump_stream_crosses_cursor_pages_without_skips() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    assert_dump_stream_crosses_cursor_pages(&storage).await
}

async fn assert_dump_stream_crosses_cursor_pages<E>(storage: &E) -> Result<()>
where E: StorageEngine + StorageDump {
    const RECORDS: usize = 513;

    let collection_id = CollectionId::from("cursor_page");
    let collection = storage.collection(&collection_id).await?;
    let mut expected_events = BTreeSet::new();
    let mut expected_states = BTreeSet::new();
    for _ in 0..RECORDS {
        let entity_id = EntityId::new();
        let event = Attested::opt(
            Event { collection: collection_id.clone(), entity_id, operations: OperationSet(Default::default()), parent: Clock::default() },
            None,
        );
        let state = Attested::opt(
            EntityState {
                entity_id,
                collection: collection_id.clone(),
                state: State { state_buffers: StateBuffers::default(), head: event.payload.id().into() },
            },
            None,
        );
        expected_events.insert(event.payload.id());
        expected_states.insert(entity_id);
        assert!(collection.add_event(&event).await?);
        assert!(collection.set_state(state).await?);
    }

    let items = storage.dump().await?;
    pin_mut!(items);
    let mut events = BTreeSet::new();
    let mut states = BTreeSet::new();
    let mut saw_state = false;
    while let Some(item) = items.next().await {
        match item? {
            StorageDumpItem::Event(event) => {
                assert!(!saw_state, "dump emitted an event after a state");
                if event.payload.collection == collection_id {
                    events.insert(event.payload.id());
                }
            }
            StorageDumpItem::State(state) => {
                saw_state = true;
                if state.payload.collection == collection_id {
                    states.insert(state.payload.entity_id);
                }
            }
        }
    }
    assert_eq!(events, expected_events);
    assert_eq!(states, expected_states);
    Ok(())
}

async fn populate(storage: Arc<SqliteStorageEngine>) -> Result<EntityId> {
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context(c)?;

    let transaction = context.begin();
    let album = transaction.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await?;
    let album_id = album.id();
    transaction.commit().await?;

    let transaction = context.begin();
    let album = transaction.get::<Album>(&album_id).await?;
    album.name().insert(0, "(o. ")?;
    transaction.commit().await?;

    drop(context);
    drop(node);
    Ok(album_id)
}

async fn assert_dump_empty<E: StorageDump>(engine: &E) -> Result<()> {
    let items = engine.dump().await?;
    pin_mut!(items);
    assert!(items.next().await.is_none());
    Ok(())
}

async fn compare_entity<A: StorageEngine, B: StorageEngine>(source: &A, target: &B, id: EntityId) -> Result<()> {
    let collection_id = CollectionId::from("album");
    let source = source.collection(&collection_id).await?;
    let target = target.collection(&collection_id).await?;
    assert_eq!(target.get_state(id).await?, source.get_state(id).await?);

    let mut source_events = source.dump_entity_events(id).await?.iter().map(serde_json::to_value).collect::<Result<Vec<_>, _>>()?;
    let mut target_events = target.dump_entity_events(id).await?.iter().map(serde_json::to_value).collect::<Result<Vec<_>, _>>()?;
    source_events.sort_by_key(serde_json::Value::to_string);
    target_events.sort_by_key(serde_json::Value::to_string);
    assert_eq!(target_events, source_events);
    Ok(())
}

async fn assert_materialized_query<E: StorageEngine>(engine: &E) -> Result<()> {
    let collection = engine.collection(&CollectionId::from("album")).await?;
    let selection = ankql::parser::parse_selection("name = '(o. The rest of the owl'")?;
    assert_eq!(collection.fetch_states(&selection).await?.len(), 1);
    Ok(())
}

fn tamper_header(source: &Path, target: &Path) -> Result<()> {
    let mut bytes = std::fs::read(source)?;
    let marker = format!("\"producer_version\":\"{}\"", env!("CARGO_PKG_VERSION"));
    let marker = marker.as_bytes();
    let start = bytes.windows(marker.len()).position(|window| window == marker).expect("producer version in header");
    let last_version_byte = &mut bytes[start + marker.len() - 2];
    *last_version_byte = if *last_version_byte == b'0' { b'1' } else { b'0' };
    std::fs::write(target, bytes)?;
    Ok(())
}

fn make_state_head_stale_and_resign(source: &Path, target: &Path) -> Result<()> {
    let contents = std::fs::read_to_string(source)?;
    let mut records = contents.lines().map(serde_json::from_str::<serde_json::Value>).collect::<Result<Vec<_>, _>>()?;
    let mut event_counts = std::collections::HashMap::<String, usize>::new();
    let mut creation_events = std::collections::HashMap::<String, String>::new();
    for event in records.iter().filter(|record| record.get("kind").and_then(|value| value.as_str()) == Some("event")) {
        let entity = event["entity_id"].as_str().expect("event entity id").to_owned();
        *event_counts.entry(entity.clone()).or_default() += 1;
        if event["parent"].as_array().is_some_and(Vec::is_empty) {
            creation_events.insert(entity, event["id"].as_str().expect("event id").to_owned());
        }
    }
    let entity =
        event_counts.iter().find_map(|(entity, count)| (*count > 1).then(|| entity.clone())).expect("entity with an event descendant");
    let creation = creation_events.get(&entity).expect("creation event").clone();
    let state = records
        .iter_mut()
        .find(|record| {
            record.get("kind").and_then(|value| value.as_str()) == Some("state")
                && record.get("entity_id").and_then(|value| value.as_str()) == Some(&entity)
        })
        .expect("state for entity with descendant");
    state["head"] = serde_json::json!([creation]);

    resign_records(records, target)
}

fn add_reserved_property_and_resign(source: &Path, target: &Path) -> Result<()> {
    let contents = std::fs::read_to_string(source)?;
    let mut records = contents.lines().map(serde_json::from_str::<serde_json::Value>).collect::<Result<Vec<_>, _>>()?;
    let backend = LWWBackend::new();
    backend.set("id".to_owned(), Some(Value::String("unsafe".to_owned())));
    let operations = backend.to_operations()?.expect("reserved property operation");
    backend.apply_operations_with_event(&operations, EventId::from_bytes([9; 32]))?;
    let state_buffer = URL_SAFE_NO_PAD.encode(backend.to_state_buffer()?);
    let state = records
        .iter_mut()
        .find(|record| {
            record.get("kind").and_then(|value| value.as_str()) == Some("state")
                && record.get("state_buffers").and_then(|value| value.get("lww")).is_some()
        })
        .expect("LWW state record");
    state["state_buffers"]["lww"] = serde_json::Value::String(state_buffer);

    resign_records(records, target)
}

fn resign_records(mut records: Vec<serde_json::Value>, target: &Path) -> Result<()> {
    let mut checksum = Sha256::new();
    let end_index = records.len() - 1;
    let mut output = Vec::new();
    for record in &records[..end_index] {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        checksum.update(&line);
        output.extend_from_slice(&line);
    }
    let digest = checksum.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    records[end_index]["sha256"] = serde_json::Value::String(digest);
    serde_json::to_writer(&mut output, &records[end_index])?;
    output.push(b'\n');
    std::fs::write(target, output)?;
    Ok(())
}
