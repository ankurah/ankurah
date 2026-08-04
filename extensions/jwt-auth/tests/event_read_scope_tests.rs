//! Row-level read scopes must gate an entity's events, not only its state.
//!
//! A scope rule narrows which rows a token may read. The state path applies it
//! per row, but events replay the same CRDT content, so a token that is refused
//! a row and then granted that row's events reads the row anyway. These tests
//! hold the two paths to the same verdict on one fixture, and pin what happens
//! when the serving node has no current state to evaluate the rule against.

mod common;

use ankurah::{Model, Node, Ref};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_core::policy::PolicyAgent;
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext, JwtKeys, PolicyConfig};
use ankurah_proto as proto;
use ankurah_storage_sled::SledStorageEngine;
use jwt_simple::prelude::Duration;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub name: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopedRecord {
    pub account: Ref<Account>,
    #[active_type(LWW)]
    pub label: String,
}

/// A collection with no scope rules, for the control case: its rows are settled
/// by the collection gate alone.
#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct Note {
    #[active_type(LWW)]
    pub text: String,
}

const CONFIG_JSON: &str = r#"{
    "roles": {
        "AccountReader": ["read_scoped_records", "read_notes"]
    },
    "collections": {
        "scopedrecord": {
            "read": "read_scoped_records",
            "write": null,
            "scope": [
                { "filter": "account = $jwt.custom.account_id" }
            ]
        },
        "note": {
            "read": "read_notes",
            "write": null
        }
    }
}"#;

fn configured_agent(keys: &ankurah_jwt_auth::SigningKeys) -> anyhow::Result<JwtAgent> {
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    Ok(agent)
}

fn reader_claims(account_id: proto::EntityId) -> JwtClaims {
    let mut custom = serde_json::Map::new();
    custom.insert("account_id".to_string(), serde_json::Value::String(account_id.to_base64()));
    JwtClaims { sub: "reader-1".into(), roles: vec!["AccountReader".into()], email: "reader@example.com".into(), name: None, custom }
}

/// One record per account, each carrying a create event and an update event.
struct Fixture {
    allowed_account_id: proto::EntityId,
    allowed_record_id: proto::EntityId,
    denied_record_id: proto::EntityId,
    allowed_events: Vec<proto::Attested<proto::Event>>,
    denied_events: Vec<proto::Attested<proto::Event>>,
}

async fn build_fixture(root: &ankurah::Context) -> anyhow::Result<Fixture> {
    let (allowed_account_id, denied_account_id) = {
        let trx = root.begin();
        let allowed = trx.create(&Account { name: "Allowed".into() }).await?;
        let denied = trx.create(&Account { name: "Denied".into() }).await?;
        let ids = (allowed.id(), denied.id());
        trx.commit().await?;
        ids
    };

    let (allowed_record_id, denied_record_id) = {
        let trx = root.begin();
        let allowed = trx.create(&ScopedRecord { account: allowed_account_id.into(), label: "allowed".into() }).await?;
        let denied = trx.create(&ScopedRecord { account: denied_account_id.into(), label: "denied".into() }).await?;
        let ids = (allowed.id(), denied.id());
        trx.commit().await?;
        ids
    };

    // A second event per record, so the batch carries more than creates.
    {
        let trx = root.begin();
        root.get::<ScopedRecordView>(allowed_record_id).await?.edit(&trx)?.label().set(&"allowed-updated".to_string())?;
        root.get::<ScopedRecordView>(denied_record_id).await?.edit(&trx)?.label().set(&"denied-updated".to_string())?;
        trx.commit().await?;
    }

    let records = root.collection(&ScopedRecord::collection()).await?;
    let allowed_events = records.dump_entity_events(allowed_record_id).await?;
    let denied_events = records.dump_entity_events(denied_record_id).await?;
    assert_eq!(allowed_events.len(), 2, "fixture should carry a create and an update per record");
    assert_eq!(denied_events.len(), 2, "fixture should carry a create and an update per record");

    Ok(Fixture { allowed_account_id, allowed_record_id, denied_record_id, allowed_events, denied_events })
}

/// A peer asks a serving node for events by id. The serving node must hand back
/// only the events of rows the asking token can read, and the same token asking
/// for those rows' states must get the matching answer.
#[tokio::test]
async fn get_events_applies_the_same_row_scope_as_get_states() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    server.system.create().await?;

    let root = server.context(JwtContext::system())?;
    let fixture = build_fixture(&root).await?;

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let claims = reader_claims(fixture.allowed_account_id);
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let reader = JwtContext::from_claims(claims, token);

    // The event path: ask for both records' events in one batch.
    let event_ids: Vec<proto::EventId> =
        fixture.allowed_events.iter().chain(fixture.denied_events.iter()).map(|event| event.payload.id()).collect();
    let response =
        client.request(server.id, &reader, proto::NodeRequestBody::GetEvents { collection: ScopedRecord::collection(), event_ids }).await?;
    let served = match response {
        proto::NodeResponseBody::GetEvents(events) => events,
        other => anyhow::bail!("expected a GetEvents response, got {other:?}"),
    };
    let served_ids: HashSet<proto::EventId> = served.iter().map(|event| event.payload.id()).collect();

    for event in &fixture.denied_events {
        assert!(
            !served_ids.contains(&event.payload.id()),
            "an event of a row outside the token's read scope must not be served (event {:?} of record {})",
            event.payload.id(),
            fixture.denied_record_id
        );
    }
    for event in &fixture.allowed_events {
        assert!(
            served_ids.contains(&event.payload.id()),
            "an event of a row inside the token's read scope must still be served (event {:?} of record {})",
            event.payload.id(),
            fixture.allowed_record_id
        );
    }

    // The state path, same token, same two rows: the verdicts must line up.
    let response = client
        .request(
            server.id,
            &reader,
            proto::NodeRequestBody::Get {
                collection: ScopedRecord::collection(),
                ids: vec![fixture.allowed_record_id, fixture.denied_record_id],
            },
        )
        .await?;
    let states = match response {
        proto::NodeResponseBody::Get(states) => states,
        other => anyhow::bail!("expected a Get response, got {other:?}"),
    };
    let state_ids: HashSet<proto::EntityId> = states.iter().map(|state| state.payload.entity_id).collect();
    assert!(!state_ids.contains(&fixture.denied_record_id), "the state path must deny the same row the event path denied");
    assert!(state_ids.contains(&fixture.allowed_record_id), "the state path must allow the same row the event path allowed");

    Ok(())
}

/// The row scope is evaluated against the entity's current state, so a serving
/// node that has no state for the entity has nothing to evaluate: it refuses.
/// A collection carrying no scope rules is a different case -- the collection
/// gate already settled it, and a missing state changes nothing there.
#[tokio::test]
async fn missing_current_state_denies_scoped_events_and_spares_unscoped_ones() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = configured_agent(&keys)?;

    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    let fixture = build_fixture(&root).await?;

    let note_id = {
        let trx = root.begin();
        let note = trx.create(&Note { text: "unscoped".into() }).await?;
        let id = note.id();
        trx.commit().await?;
        id
    };
    let note_event = root.collection(&Note::collection()).await?.dump_entity_events(note_id).await?.remove(0);

    let records = root.collection(&ScopedRecord::collection()).await?;
    let allowed_state = records.get_state(fixture.allowed_record_id).await?.payload.state;
    let denied_state = records.get_state(fixture.denied_record_id).await?.payload.state;

    let claims = reader_claims(fixture.allowed_account_id);
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let reader = JwtContext::from_claims(claims, token);

    let allowed_event = fixture.allowed_events[0].clone();
    let denied_event = fixture.denied_events[0].clone();

    // Fail closed: a scoped collection with no state to evaluate is refused,
    // even for an event whose row the token could otherwise read.
    assert!(
        agent.check_read_event(&reader, &allowed_event, None).is_err(),
        "a scoped event with no current state must be refused rather than admitted unevaluated"
    );
    assert!(agent.check_read_event(&reader, &denied_event, None).is_err(), "a scoped event with no current state must be refused");

    // With state, the event verdict matches the state verdict row for row.
    assert!(agent.check_read_event(&reader, &allowed_event, Some(&allowed_state)).is_ok());
    assert!(agent.check_read_event(&reader, &denied_event, Some(&denied_state)).is_err());
    assert!(agent.check_read(&reader, &fixture.allowed_record_id, &ScopedRecord::collection(), &allowed_state).is_ok());
    assert!(agent.check_read(&reader, &fixture.denied_record_id, &ScopedRecord::collection(), &denied_state).is_err());

    // Control: no scope rules on `note`, so the collection gate is the whole
    // decision and a missing state is not a refusal.
    assert!(
        agent.check_read_event(&reader, &note_event, None).is_ok(),
        "a collection with no scope rules must stay readable when no state is available"
    );

    Ok(())
}
