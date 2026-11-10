//! A node attests an event or a state only when its policy agent requests it, and seals the
//! requested statement with its own key. It keeps and relays every attestation it receives as
//! it came, whether or not it can read it.

mod common;

use ankql::ast::{Predicate, Resolved};
use ankurah::core::{node::NodeInner, node::NodeKey, util::Iterable};
use ankurah::entity::Entity;
use ankurah::error::ValidationError;
use ankurah::policy::{AccessDenied, AttestationRequest, DefaultContext, PolicyAgent};
use async_trait::async_trait;
use common::*;
use proto::{AttestationSet, Attested, SealedAttestation, Statement, Subject};
use std::sync::{Arc, Mutex};

/// Permissive in what it allows. It requests the attestations it is given, on every event it
/// checks and on every state its node serves, records the events its validation hook receives,
/// and trusts the publishers it lists.
#[derive(Clone)]
struct Witness {
    event_request: Option<AttestationRequest>,
    state_request: Option<AttestationRequest>,
    trusted: Vec<proto::Identity>,
    received: Arc<Mutex<Vec<Attested<proto::Event>>>>,
}

impl Witness {
    fn allowing() -> Self { Self { event_request: None, state_request: None, trusted: Vec::new(), received: Arc::default() } }

    /// The event with `id` as this agent's validation hook last received it.
    fn received(&self, id: &proto::EventId) -> Option<Attested<proto::Event>> {
        self.received.lock().unwrap().iter().rev().find(|event| event.payload.id() == *id).cloned()
    }
}

#[async_trait]
impl PolicyAgent for Witness {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _: &NodeInner<SE, Self>,
        cdata: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _: &Node<SE, Self>,
        auth: &A,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &Entity,
        _: &Entity,
        _: &proto::Event,
    ) -> Result<Option<AttestationRequest>, AccessDenied> {
        Ok(self.event_request.clone())
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        self.received.lock().unwrap().push(event.clone());
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<AttestationRequest> {
        self.state_request.clone()
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(Predicate::True)
    }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn trusted_publishers(&self) -> Vec<proto::Identity> { self.trusted.clone() }
}

fn node<SE: StorageEngine + Send + Sync + 'static>(storage: SE, durable: bool, agent: Witness) -> Node<SE, Witness> {
    if durable {
        Node::new_durable(Arc::new(storage), agent)
    } else {
        Node::new(Arc::new(storage), agent)
    }
}

/// A request for a statement of a kind only this test defines.
fn request(kind: u8, payload: &[u8]) -> AttestationRequest {
    AttestationRequest { kind: proto::EntityId::from_bytes([kind; 32]), payload: payload.to_vec() }
}

/// The statement `attester` makes about `subject` when its agent requests `request`.
fn statement_by(attester: proto::Identity, subject: Subject, request: &AttestationRequest) -> Statement {
    Statement { attester, subject, kind: request.kind, payload: request.payload.clone() }
}

/// The statements `attestations` seal, in order.
fn unsealed(attestations: &[SealedAttestation]) -> Vec<Statement> {
    attestations.iter().map(|attestation| attestation.unseal().expect("an attestation a node sealed unseals")).collect()
}

/// The client seals what its agent requests on its local commit, and the durable server seals what
/// its agent requests on admitting that remote commit, after the client's; a node whose agent
/// requests nothing attaches nothing.
#[tokio::test]
async fn each_node_that_checks_an_event_attaches_exactly_the_attestation_its_agent_requests() -> anyhow::Result<()> {
    let (client_request, server_request) = (request(1, b"by the client"), request(2, b"by the server"));
    for (client_asks, server_asks) in [(false, false), (true, false), (false, true), (true, true)] {
        let server_agent = Witness { event_request: server_asks.then(|| server_request.clone()), ..Witness::allowing() };
        let client_agent = Witness { event_request: client_asks.then(|| client_request.clone()), ..Witness::allowing() };
        let server = node(SledStorageEngine::new_test()?, true, server_agent);
        let client = node(SledStorageEngine::new_test()?, false, client_agent);
        let _connection = LocalProcessConnection::new(&client, &server).await?;
        let transaction = client.context_async(DEFAULT_CONTEXT).await?.begin();
        let id = transaction.create(&Album { name: "attested".into(), year: "2026".into() }).await?.id();
        transaction.commit().await?;

        let by_client = |event: &proto::Event| client_asks.then(|| statement_by(client.identity(), Subject::event(event), &client_request));
        let by_server = |event: &proto::Event| server_asks.then(|| statement_by(server.identity(), Subject::event(event), &server_request));
        let committed = client.storage.dump_entity_events(id).await?;
        assert!(!committed.is_empty());
        for event in committed {
            assert_eq!(unsealed(&event.attestations), Vec::from_iter(by_client(&event.payload)), "the client's commit");
        }
        let admitted = server.storage.dump_entity_events(id).await?;
        assert!(!admitted.is_empty());
        for event in admitted {
            let expected = Vec::from_iter(by_client(&event.payload).into_iter().chain(by_server(&event.payload)));
            assert_eq!(unsealed(&event.attestations), expected, "the server's admission of the client's commit");
        }
    }
    Ok(())
}

/// A durable node attaches to a state it serves the attestation its agent requests for that state,
/// sealed with its own key, and none when its agent requests none; an ephemeral node attaches none.
#[tokio::test]
async fn a_served_state_carries_the_attestation_a_durable_nodes_agent_requests() -> anyhow::Result<()> {
    for state_request in [None, Some(request(3, b"served"))] {
        let agent = Witness { state_request: state_request.clone(), ..Witness::allowing() };
        let server = node(SledStorageEngine::new_test()?, true, agent.clone());
        let client = node(SledStorageEngine::new_test()?, false, agent);
        let _connection = LocalProcessConnection::new(&client, &server).await?;
        let transaction = client.context_async(DEFAULT_CONTEXT).await?.begin();
        let id = transaction.create(&Album { name: "served".into(), year: "2026".into() }).await?.id();
        transaction.commit().await?;

        let get = || proto::NodeRequestBody::Get { ids: vec![id] };
        let served = |response: proto::NodeResponseBody| match response {
            proto::NodeResponseBody::Get(results) => match results.as_slice() {
                [proto::GetResult::Found(state)] => state.clone(),
                other => panic!("expected the state, got {other:?}"),
            },
            other => panic!("expected a Get response, got {other:?}"),
        };
        let from_server = served(client.request(server.id, &DEFAULT_CONTEXT, get()).await?);
        let from_client = served(server.request(client.id, &DEFAULT_CONTEXT, get()).await?);

        assert!(from_client.attestations.is_empty(), "an ephemeral node attests no state it serves");
        let expected = state_request.map(|request| statement_by(server.identity(), Subject::state(&from_server.payload), &request));
        assert_eq!(unsealed(&from_server.attestations), Vec::from_iter(expected));
    }
    Ok(())
}

/// A durable node between two others keeps and relays every attestation on an event as it came,
/// on sled and on sqlite: one of a kind nobody defines, sealed by the committing node at its
/// agent's request, and bytes that do not parse at all. The third node's agent receives them
/// unchanged, its store keeps them, and the durable node serves them unchanged.
#[tokio::test]
async fn a_relaying_node_keeps_every_attestation_on_an_event_unchanged() -> anyhow::Result<()> {
    relay_keeps_attestations([SledStorageEngine::new_test()?, SledStorageEngine::new_test()?, SledStorageEngine::new_test()?]).await?;
    let sqlite = ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory;
    relay_keeps_attestations([sqlite().await?, sqlite().await?, sqlite().await?]).await
}

async fn relay_keeps_attestations<SE: StorageEngine + Send + Sync + 'static>([origin, relay, reader]: [SE; 3]) -> anyhow::Result<()> {
    let unknown_kind = request(9, b"read by no build");
    let origin = node(origin, false, Witness { event_request: Some(unknown_kind.clone()), ..Witness::allowing() });
    let relay = node(relay, true, Witness::allowing());
    let reader_agent = Witness::allowing();
    let reader = node(reader, false, reader_agent.clone());
    let _origin_connection = LocalProcessConnection::new(&origin, &relay).await?;
    let _reader_connection = LocalProcessConnection::new(&reader, &relay).await?;
    // Holding a subscription is what has the relay push later commits to the reader.
    let _albums = reader.context_async(DEFAULT_CONTEXT).await?.query_wait::<AlbumView>("true").await?;

    // The origin's own commit, on which its agent's request seals one attestation of the unknown kind.
    let transaction = origin.context_async(DEFAULT_CONTEXT).await?.begin();
    let id = transaction.create(&Album { name: "relayed".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;
    let [genesis]: [Attested<proto::Event>; 1] = origin.storage.dump_entity_events(id).await?.try_into().expect("one genesis");
    assert_eq!(genesis.attestations.len(), 1);

    // A commit the origin sends as it is, carrying bytes that do not parse beside another key's attestation of the unknown kind.
    let update = proto::Event::update(id, relay.storage.get_state(id).await?.payload.state.head, proto::OperationSet::default());
    let foreign = NodeKey::from_seed([5; 32]).attest(Subject::event(&update), unknown_kind.kind, unknown_kind.payload);
    let update =
        Attested { attestations: AttestationSet(vec![SealedAttestation(b"not an attestation".to_vec()), foreign]), payload: update };
    let commit = proto::NodeRequestBody::CommitTransaction { id: proto::TransactionId::new(), events: vec![update.clone()] };
    let response = origin.request(relay.id, &DEFAULT_CONTEXT, commit).await?;
    assert!(matches!(response, proto::NodeResponseBody::CommitComplete { .. }), "{response:?}");

    let sent = [genesis, update];
    let ids: Vec<_> = sent.iter().map(|event| event.payload.id()).collect();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while reader.storage.get_events(ids.clone()).await.map_or(0, |events| events.len()) < sent.len() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    for event in &sent {
        let id = event.payload.id();
        assert_eq!(relay.storage.get_events(vec![id.clone()]).await?, vec![event.clone()], "the relay stores it as it came");
        assert_eq!(reader_agent.received(&id).as_ref(), Some(event), "the reader's agent receives it as it came");
        assert_eq!(reader.storage.get_events(vec![id.clone()]).await?, vec![event.clone()], "the reader stores it as it came");
        let served = match reader.request(relay.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::GetEvents { event_ids: vec![id] }).await? {
            proto::NodeResponseBody::GetEvents(events) => events,
            other => panic!("expected events, got {other:?}"),
        };
        assert_eq!(served, vec![event.clone()], "the relay serves it as it came");
    }
    Ok(())
}

/// The trait's default is the shipped list (pinned against the file by core's
/// own test); an agent's override replaces it.
#[test]
fn an_overridden_trusted_publisher_list_replaces_the_shipped_one() {
    let publisher = NodeKey::from_seed([4; 32]).identity();
    assert!(!PermissiveAgent::new().trusted_publishers().contains(&publisher));
    assert_eq!(Witness { trusted: vec![publisher], ..Witness::allowing() }.trusted_publishers(), vec![publisher]);
}
