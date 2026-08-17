//! Row-level read scopes must gate an entity's events, not only its state.
//!
//! A scope rule narrows which rows a token may read. The state path applies it
//! per row, but events replay the same CRDT content, so a token that is refused
//! a row and then granted that row's events reads the row anyway. These tests
//! hold the two paths to the same verdict on one fixture, and pin the rest of
//! what the event path owes that verdict: a privileged caller still reads
//! everything, several credentials read their union, and the state the rule is
//! evaluated against must be the event's own entity's, must actually account
//! for the event, and comes from the resident entity when one is held.

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

/// Three accounts, one record each, every record carrying a create event and an
/// update event. `allowed` and `second` are each readable by one of the two
/// tokens the plural case unions; `denied` is readable by neither.
struct Fixture {
    allowed_account_id: proto::EntityId,
    second_account_id: proto::EntityId,
    allowed_record_id: proto::EntityId,
    denied_record_id: proto::EntityId,
    allowed_events: Vec<proto::Attested<proto::Event>>,
    second_events: Vec<proto::Attested<proto::Event>>,
    denied_events: Vec<proto::Attested<proto::Event>>,
}

async fn build_fixture(root: &ankurah::Context) -> anyhow::Result<Fixture> {
    let (allowed_account_id, second_account_id, denied_account_id) = {
        let trx = root.begin();
        let allowed = trx.create(&Account { name: "Allowed".into() }).await?;
        let second = trx.create(&Account { name: "Second".into() }).await?;
        let denied = trx.create(&Account { name: "Denied".into() }).await?;
        let ids = (allowed.id(), second.id(), denied.id());
        trx.commit().await?;
        ids
    };

    let (allowed_record_id, second_record_id, denied_record_id) = {
        let trx = root.begin();
        let allowed = trx.create(&ScopedRecord { account: allowed_account_id.into(), label: "allowed".into() }).await?;
        let second = trx.create(&ScopedRecord { account: second_account_id.into(), label: "second".into() }).await?;
        let denied = trx.create(&ScopedRecord { account: denied_account_id.into(), label: "denied".into() }).await?;
        let ids = (allowed.id(), second.id(), denied.id());
        trx.commit().await?;
        ids
    };

    // A second event per record, so the batch carries more than creates.
    {
        let trx = root.begin();
        root.get::<ScopedRecordView>(allowed_record_id).await?.edit(&trx)?.label().set(&"allowed-updated".to_string())?;
        root.get::<ScopedRecordView>(second_record_id).await?.edit(&trx)?.label().set(&"second-updated".to_string())?;
        root.get::<ScopedRecordView>(denied_record_id).await?.edit(&trx)?.label().set(&"denied-updated".to_string())?;
        trx.commit().await?;
    }

    let records = root.collection(&ScopedRecord::collection()).await?;
    let allowed_events = records.dump_entity_events(allowed_record_id).await?;
    let second_events = records.dump_entity_events(second_record_id).await?;
    let denied_events = records.dump_entity_events(denied_record_id).await?;
    for events in [&allowed_events, &second_events, &denied_events] {
        assert_eq!(events.len(), 2, "fixture should carry a create and an update per record");
    }

    Ok(Fixture { allowed_account_id, second_account_id, allowed_record_id, denied_record_id, allowed_events, second_events, denied_events })
}

/// Ask a serving node for events by id and collect what it hands back.
async fn served_event_ids<C>(
    client: &Node<SledStorageEngine, JwtAgent>,
    server_id: proto::EntityId,
    cdata: &C,
    event_ids: Vec<proto::EventId>,
) -> anyhow::Result<HashSet<proto::EventId>>
where
    C: ankurah_core::util::Iterable<JwtContext>,
{
    let response =
        client.request(server_id, cdata, proto::NodeRequestBody::GetEvents { collection: ScopedRecord::collection(), event_ids }).await?;
    match response {
        proto::NodeResponseBody::GetEvents(events) => Ok(events.iter().map(|event| event.payload.id()).collect()),
        other => anyhow::bail!("expected a GetEvents response, got {other:?}"),
    }
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
    let served_ids = served_event_ids(&client, server.id, &reader, event_ids).await?;

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

/// The row scope is evaluated against the entity as the serving node
/// currently knows it, so a node that has nothing for the entity has nothing
/// to evaluate: it refuses. A collection carrying no scope rules is a
/// different case -- the collection gate already settled it, and a missing
/// entity changes nothing there, because the verdict never fetches it.
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

    // A second node that holds none of the fixture's entities: getters built
    // against it find nothing current, which is the missing-state case.
    let empty_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    empty_node.system.create().await?;

    let claims = reader_claims(fixture.allowed_account_id);
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let reader = JwtContext::from_claims(claims, token);

    let allowed_event = fixture.allowed_events[0].clone();
    let denied_event = fixture.denied_events[0].clone();

    // Fail closed: a scoped collection with nothing current to evaluate is
    // refused, even for an event whose row the token could otherwise read.
    assert!(
        agent
            .check_read_event(&reader, &allowed_event, &empty_node.entity_getter(fixture.allowed_record_id, ScopedRecord::collection()))
            .await
            .is_err(),
        "a scoped event with nothing current to evaluate must be refused rather than admitted unevaluated"
    );
    assert!(
        agent
            .check_read_event(&reader, &denied_event, &empty_node.entity_getter(fixture.denied_record_id, ScopedRecord::collection()))
            .await
            .is_err(),
        "a scoped event with nothing current to evaluate must be refused"
    );

    // With the rows present, the event verdict matches the state verdict row
    // for row. The getters are bound to each event's own entity by
    // construction, which is what retired the old hazard of pairing an event
    // with some other, readable row's state.
    let records = root.collection(&ScopedRecord::collection()).await?;
    let allowed_state = records.get_state(fixture.allowed_record_id).await?.payload;
    let denied_state = records.get_state(fixture.denied_record_id).await?.payload;
    assert!(agent
        .check_read_event(&reader, &allowed_event, &node.entity_getter(fixture.allowed_record_id, ScopedRecord::collection()))
        .await
        .is_ok());
    assert!(agent
        .check_read_event(&reader, &denied_event, &node.entity_getter(fixture.denied_record_id, ScopedRecord::collection()))
        .await
        .is_err());
    assert!(agent.check_read(&reader, &fixture.allowed_record_id, &ScopedRecord::collection(), &allowed_state.state).is_ok());
    assert!(agent.check_read(&reader, &fixture.denied_record_id, &ScopedRecord::collection(), &denied_state.state).is_err());

    // Control: no scope rules on `note`, so the collection gate is the whole
    // decision. The getter aims at a node that holds nothing, and the verdict
    // must not care: it never fetches.
    assert!(
        agent.check_read_event(&reader, &note_event, &empty_node.entity_getter(note_id, Note::collection())).await.is_ok(),
        "a collection with no scope rules must stay readable when nothing current is available"
    );

    Ok(())
}

/// A privileged context is admitted before any scope rule is consulted, and
/// the event path must keep that so: `Root` reads every row's events,
/// including rows no token's scope would admit -- and the verdict never
/// fetches the row. The getters below aim at a node that holds none of these
/// entities, so a verdict that fetched would find nothing and refuse; every
/// `Ok` is proof the privileged path decided without looking.
///
/// The assertion sits at the agent rather than on the wire because `Root` is a
/// local-only context -- it cannot produce auth data, so it never travels as a
/// request credential.
#[tokio::test]
async fn a_privileged_context_reads_events_of_rows_no_scope_admits() -> anyhow::Result<()> {
    let keys = common::test_keys();
    let agent = configured_agent(&keys)?;

    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    let fixture = build_fixture(&root).await?;

    let empty_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    empty_node.system.create().await?;
    let privileged = JwtContext::system();

    for event in fixture.allowed_events.iter().chain(fixture.denied_events.iter()) {
        assert!(
            agent
                .check_read_event(&privileged, event, &empty_node.entity_getter(event.payload.entity_id, ScopedRecord::collection()))
                .await
                .is_ok(),
            "a privileged context must read every event, without the row being fetched at all"
        );
    }
    assert!(
        agent
            .check_read_event(
                &privileged,
                &fixture.denied_events[0],
                &node.entity_getter(fixture.denied_record_id, ScopedRecord::collection())
            )
            .await
            .is_ok(),
        "a privileged context must read a row its scope rules would refuse"
    );

    Ok(())
}

/// A caller can act under several credentials at once. Reads take the union:
/// the events served are exactly those of the rows some member admits, and a
/// row no member admits stays hidden even though a member could read the
/// collection.
#[tokio::test]
async fn plural_credentials_read_the_union_of_their_rows_events() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    server.system.create().await?;

    let root = server.context(JwtContext::system())?;
    let fixture = build_fixture(&root).await?;

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let mut readers = Vec::new();
    for account_id in [fixture.allowed_account_id, fixture.second_account_id] {
        let claims = reader_claims(account_id);
        let token = keys.sign(&claims, Duration::from_hours(1))?;
        readers.push(JwtContext::from_claims(claims, token));
    }

    let all_events: Vec<&proto::Attested<proto::Event>> =
        fixture.allowed_events.iter().chain(fixture.second_events.iter()).chain(fixture.denied_events.iter()).collect();
    let served_ids = served_event_ids(&client, server.id, &readers, all_events.iter().map(|event| event.payload.id()).collect()).await?;

    let expected: HashSet<proto::EventId> =
        fixture.allowed_events.iter().chain(fixture.second_events.iter()).map(|event| event.payload.id()).collect();
    assert_eq!(
        served_ids, expected,
        "the union of two tokens must be served exactly the events of the rows they jointly admit -- not the third account's row ({}), \
         and not a subset of their own",
        fixture.denied_record_id
    );

    Ok(())
}

/// A commit publishes its event before it persists the entity's state, so a
/// serving node can hold an event its stored state does not yet account for --
/// and that event may be the very one that moves the row out of the caller's
/// scope. An event the authorizing state cannot speak for is not served.
///
/// The window is staged rather than raced: the serving node below is given the
/// whole event chain but only the state as of the create, and never
/// materializes the entity, so nothing fresher is available to authorize
/// against.
#[tokio::test]
async fn an_event_the_authorizing_state_does_not_cover_is_not_served() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let origin = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    origin.system.create().await?;
    let origin_root = origin.context(JwtContext::system())?;
    let fixture = build_fixture(&origin_root).await?;

    let origin_records = origin_root.collection(&ScopedRecord::collection()).await?;
    let create_event = fixture.allowed_events.iter().find(|event| event.payload.parent.is_empty()).expect("a create event");
    let update_event = fixture.allowed_events.iter().find(|event| !event.payload.parent.is_empty()).expect("an update event");
    let mut state_at_create = origin_records.get_state(fixture.allowed_record_id).await?;
    state_at_create.payload.state.head = proto::Clock::from(create_event.payload.id());

    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    server.system.create().await?;
    let server_records = server.context(JwtContext::system())?.collection(&ScopedRecord::collection()).await?;
    for event in &fixture.allowed_events {
        server_records.add_event(event).await?;
    }
    server_records.set_state(state_at_create).await?;

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let claims = reader_claims(fixture.allowed_account_id);
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let reader = JwtContext::from_claims(claims, token);

    let served_ids = served_event_ids(&client, server.id, &reader, vec![create_event.payload.id(), update_event.payload.id()]).await?;

    assert!(
        served_ids.contains(&create_event.payload.id()),
        "the event the stored state accounts for must still be served, or the refusal below proves nothing"
    );
    assert!(!served_ids.contains(&update_event.payload.id()), "an event newer than the state that authorized it must not be served");

    Ok(())
}

/// The stored state buffer is a rebuildable cache of the event log, and the
/// EventOnly apply path leaves it behind: it commits events and advances the
/// in-memory entity without rewriting the buffer. Authorizing from storage alone
/// would therefore judge the row several events out of date, so a resident
/// entity is consulted first and outranks what storage holds.
///
/// Here the same node holds the entity resident and up to date while its stored
/// state is rewound to the create. The update event must still be served: the
/// resident entity accounts for it even though the buffer does not.
#[tokio::test]
async fn a_resident_entity_outranks_a_rewound_stored_state() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    server.system.create().await?;
    let root = server.context(JwtContext::system())?;
    let fixture = build_fixture(&root).await?;

    let records = root.collection(&ScopedRecord::collection()).await?;
    let create_event = fixture.allowed_events.iter().find(|event| event.payload.parent.is_empty()).expect("a create event");
    let update_event = fixture.allowed_events.iter().find(|event| !event.payload.parent.is_empty()).expect("an update event");

    // Hold the entity resident at its true head, then rewind what storage says.
    let _resident = root.get::<ScopedRecordView>(fixture.allowed_record_id).await?;
    let mut rewound = records.get_state(fixture.allowed_record_id).await?;
    rewound.payload.state.head = proto::Clock::from(create_event.payload.id());
    records.set_state(rewound).await?;

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), configured_agent(&keys)?);
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let claims = reader_claims(fixture.allowed_account_id);
    let token = keys.sign(&claims, Duration::from_hours(1))?;
    let reader = JwtContext::from_claims(claims, token);

    let served_ids = served_event_ids(&client, server.id, &reader, vec![create_event.payload.id(), update_event.payload.id()]).await?;
    assert_eq!(
        served_ids,
        HashSet::from([create_event.payload.id(), update_event.payload.id()]),
        "the resident entity accounts for both events, so a stale stored state must not withhold either"
    );

    Ok(())
}
