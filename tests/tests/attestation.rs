//! Attestations a node signs and the check every received one passes: a
//! committed attestation on each event a node commits, an admitted one when
//! its policy agent asks, a state attestation on each state a durable node
//! serves, and stripping of any that fail before a policy hook sees them.

mod common;

use ankql::ast::{Predicate, Resolved};
use ankurah::core::{node::NodeInner, node::NodeKey, test_helpers::commit_transaction, util::Iterable};
use ankurah::entity::Entity;
use ankurah::error::ValidationError;
use ankurah::policy::{AccessDenied, Admission, DefaultContext, PolicyAgent};
use async_trait::async_trait;
use common::*;
use proto::{Attestation, AttestationBody, Attested, AttesterRole};
use std::sync::{Arc, Mutex};

/// Permissive in what it allows. It records the attestations its validation
/// hooks receive, attests admitted events when given claims, and trusts the
/// publishers it lists.
#[derive(Clone)]
struct Witness {
    admission_claims: Option<Vec<u8>>,
    trusted: Vec<proto::Identity>,
    events: Arc<Mutex<Vec<Attested<proto::Event>>>>,
    states: Arc<Mutex<Vec<Attested<proto::EntityState>>>>,
}

impl Witness {
    fn allowing() -> Self { Self { admission_claims: None, trusted: Vec::new(), events: Arc::default(), states: Arc::default() } }

    fn attesting(claims: Vec<u8>) -> Self { Self { admission_claims: Some(claims), ..Self::allowing() } }

    fn received_event(&self, id: &proto::EventId) -> Option<Attested<proto::Event>> {
        self.events.lock().unwrap().iter().rev().find(|event| event.payload.id() == *id).cloned()
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
    ) -> Result<Admission, AccessDenied> {
        Ok(match &self.admission_claims {
            Some(claims) => Admission::Attest { claims: claims.clone() },
            None => Admission::Allow,
        })
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<Vec<u8>> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        self.states.lock().unwrap().push(state.clone());
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

type WitnessNode = Node<SledStorageEngine, Witness>;

fn node(durable: bool, agent: Witness) -> anyhow::Result<WitnessNode> {
    let storage = Arc::new(SledStorageEngine::new_test()?);
    Ok(if durable { Node::new_durable(storage, agent) } else { Node::new(storage, agent) })
}

/// The one attestation in `attestations` by `attester` in `role`, if it verifies and names `event`.
fn by<'a>(attestations: &'a [Attestation], attester: proto::Identity, role: AttesterRole, event: &proto::Event) -> Vec<&'a Attestation> {
    attestations
        .iter()
        .filter(|attestation| {
            attestation.attester == attester
                && matches!(&attestation.body, AttestationBody::Event { role: r, .. } if *r == role)
                && attestation.verify()
                && attestation.covers_event(event)
        })
        .collect()
}

#[tokio::test]
async fn every_committed_event_carries_its_committers_attestation_which_the_receiver_verifies() -> anyhow::Result<()> {
    let server_agent = Witness::allowing();
    let client_agent = Witness::allowing();
    let server = node(true, server_agent.clone())?;
    let client = node(false, client_agent.clone())?;
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    let client_context = client.context_async(DEFAULT_CONTEXT).await?;
    // Holding a subscription is what has the server push its later commits to the client.
    let _albums = client_context.query_wait::<AlbumView>("true").await?;

    let transaction = client_context.begin();
    let id = transaction.create(&Album { name: "relayed".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;
    for stored in server.storage.dump_entity_events(id).await? {
        assert_eq!(
            by(&stored.attestations, client.identity(), AttesterRole::Committed, &stored.payload).len(),
            1,
            "the server keeps the client's attestation"
        );
        assert!(
            by(&stored.attestations, server.identity(), AttesterRole::Admitted, &stored.payload).is_empty(),
            "an allowing policy adds none"
        );
    }

    let server_context = server.context_async(DEFAULT_CONTEXT).await?;
    let transaction = server_context.begin();
    server_context.get::<AlbumView>(id).await?.edit(&transaction)?.year()?.replace("2027")?;
    transaction.commit().await?;
    let update =
        server.storage.dump_entity_events(id).await?.into_iter().find(|event| !event.payload.is_entity_create()).expect("the edit");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while client_agent.received_event(&update.payload.id()).is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    let received = client_agent.received_event(&update.payload.id()).unwrap();
    assert_eq!(by(&received.attestations, server.identity(), AttesterRole::Committed, &received.payload).len(), 1);

    for state in client_agent.states.lock().unwrap().iter() {
        let [attestation] = state.attestations.0.as_slice() else {
            panic!("one state attestation per served state, got {:?}", state.attestations)
        };
        assert_eq!(attestation.attester, server.identity(), "the durable server attests every state it serves");
        assert!(attestation.verify() && attestation.covers_state(&state.payload));
    }
    assert!(!client_agent.states.lock().unwrap().is_empty(), "the client received served states");
    Ok(())
}

#[tokio::test]
async fn an_attesting_policy_adds_an_admitted_attestation_with_its_claims() -> anyhow::Result<()> {
    let server = node(true, Witness::attesting(b"admitted by policy".to_vec()))?;
    let client = node(false, Witness::allowing())?;
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    let transaction = client.context_async(DEFAULT_CONTEXT).await?.begin();
    let id = transaction.create(&Album { name: "admitted".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;

    for stored in server.storage.dump_entity_events(id).await? {
        let admitted = by(&stored.attestations, server.identity(), AttesterRole::Admitted, &stored.payload);
        let [admitted] = admitted.as_slice() else { panic!("one admitted attestation, got {:?}", stored.attestations) };
        assert!(matches!(&admitted.body, AttestationBody::Event { claims, .. } if claims == b"admitted by policy"));
        assert_eq!(by(&stored.attestations, client.identity(), AttesterRole::Committed, &stored.payload).len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn attestations_that_fail_are_stripped_before_any_policy_hook_or_storage_sees_them() -> anyhow::Result<()> {
    let client_agent = Witness::allowing();
    let server = node(true, Witness::allowing())?;
    let client = node(false, client_agent.clone())?;
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    let server_context = server.context_async(DEFAULT_CONTEXT).await?;
    let client_context = client.context_async(DEFAULT_CONTEXT).await?;
    let _relay = client_context.query_wait::<AlbumView>("true").await?;
    let transaction = server_context.begin();
    let id = transaction.create(&Album { name: "target".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;
    let head = client_context.get::<AlbumView>(id).await?.entity().head();

    let key = NodeKey::generate();
    let forged_update = |head: proto::Clock| {
        let event = proto::Event::update(id, head, proto::OperationSet::default());
        let genuine = key.attest(AttestationBody::event(&event, AttesterRole::Committed, vec![]));
        let mut tampered = genuine.clone();
        tampered.body = AttestationBody::event(&event, AttesterRole::Admitted, vec![9]);
        let other = proto::Event::update(id, proto::Clock::singleton(&event), proto::OperationSet::default());
        let transplanted = key.attest(AttestationBody::event(&other, AttesterRole::Committed, vec![]));
        (Attested { payload: event, attestations: proto::AttestationSet(vec![tampered, genuine.clone(), transplanted]) }, genuine)
    };

    // A subscription update: the client's validation hook sees only what verified.
    let (delivered, genuine) = forged_update(head.clone());
    let item = proto::SubscriptionUpdateItem {
        entity_id: id,
        content: proto::UpdateContent::EventOnly(vec![delivered.clone().into()]),
        predicate_relevance: vec![],
    };
    let update = proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: server.id,
        to: client.id,
        body: proto::NodeUpdateBody::SubscriptionUpdate { items: vec![item] },
    };
    client.handle_message(proto::NodeMessage::Update(update)).await?;
    let seen = client_agent.received_event(&delivered.payload.id()).expect("the hook saw the event");
    assert_eq!(seen.attestations.0, vec![genuine.clone()]);

    // A committed transaction: the server stores only what verified.
    let (committed, genuine) = forged_update(server.storage.get_state(id).await?.payload.state.head);
    commit_transaction(&server, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![committed.clone()]).await?;
    let stored = server
        .storage
        .dump_entity_events(id)
        .await?
        .into_iter()
        .find(|event| event.payload.id() == committed.payload.id())
        .expect("stored");
    assert_eq!(stored.attestations.0, vec![genuine]);
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
