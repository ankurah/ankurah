//! Credentials in a sync: a responder builds each query's account, pushes
//! and repairs under that query's own credentials, and a Fetch's credentials
//! join those a client applies its peer's pushes under only while the Fetch
//! is in flight.

use super::*;
use ankql::ast::{Predicate, Resolved};
use ankurah::core::{
    entity::Entity,
    error::ValidationError,
    node::{ContextData, NodeInner},
    schema::catalog::SysModelRowView,
    util::Iterable,
};
use ankurah::policy::{AccessDenied, PolicyAgent};
use async_trait::async_trait;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};

/// A credential of one byte, which the agent signs as itself.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Credential(u8);
impl ContextData for Credential {}

/// The credential that may read what the agent hides.
const READS_HIDDEN: Credential = Credential(1);

/// An agent that hides `hidden` from credentials without [`READS_HIDDEN`],
/// and records the credentials of each event request it checks and the
/// query of each subscribe.
#[derive(Clone, Default)]
struct CredentialAgent {
    hidden: Arc<Mutex<HashSet<EntityId>>>,
    event_requests: Arc<Mutex<Vec<(Vec<proto::EventId>, BTreeSet<Credential>)>>>,
    subscribes: Arc<Mutex<Vec<proto::QueryId>>>,
}

#[async_trait]
impl PolicyAgent for CredentialAgent {
    type ContextData = Credential;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        credentials: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Credential>,
    {
        Ok(credentials.iterable().map(|credential| proto::AuthData(vec![credential.0])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &Node<SE, Self>,
        auth: &A,
        request: &proto::NodeRequest,
    ) -> Result<Vec<Credential>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        let credentials: Vec<_> = auth.iterable().map(|auth| Credential(auth.0.first().copied().unwrap_or(0))).collect();
        match &request.body {
            proto::NodeRequestBody::GetEvents { event_ids } => {
                self.event_requests.lock().unwrap().push((event_ids.clone(), credentials.iter().cloned().collect()))
            }
            proto::NodeRequestBody::SubscribeQuery { query_id, .. } => self.subscribes.lock().unwrap().push(*query_id),
            _ => {}
        }
        Ok(credentials)
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _credential: &Credential,
        _before: &Entity,
        _after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer: &EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &Node<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer: &EntityId,
        _state: &proto::Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C>(&self, _credentials: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Credential> {
        Ok(Predicate::True)
    }

    fn check_reads<C>(&self, credentials: &C, states: &[(&EntityId, &proto::State)]) -> HashMap<EntityId, AccessDenied>
    where C: Iterable<Credential> {
        if credentials.iterable().any(|credential| *credential == READS_HIDDEN) {
            return HashMap::new();
        }
        let hidden = self.hidden.lock().unwrap();
        states
            .iter()
            .filter(|(id, _)| hidden.contains(id))
            .map(|(id, _)| (**id, AccessDenied::ByPolicy("hidden from these credentials")))
            .collect()
    }

    fn check_write(&self, _credential: &Credential, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer: &EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

type CredentialNode = Node<SledStorageEngine, CredentialAgent>;

/// A durable server under `server_agent` and an ephemeral client under an
/// agent that hides nothing, connected through a gate that holds each
/// message to the client that `hold` matches.
async fn gated_under(
    server_agent: CredentialAgent,
    hold: impl Fn(&proto::NodeMessage) -> bool + Send + Sync + 'static,
) -> anyhow::Result<(CredentialNode, CredentialNode, GatedConnection)> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), CredentialAgent::default());
    let (connection, _gate) = GatedConnection::new(&server, &client, hold).await;
    client.wait_ready().await?;
    Ok((server, client, connection))
}

#[tokio::test]
async fn a_responder_answers_pushes_and_repairs_each_query_under_its_own_credentials() -> anyhow::Result<()> {
    let server_agent = CredentialAgent::default();
    let received = Received::default();
    let (server_node, client_node, _connection) = {
        let received = received.clone();
        gated_under(server_agent.clone(), move |message| {
            received.record(message);
            false
        })
        .await?
    };
    let writer = server_node.context_async(Credential(0)).await?;
    let trx = writer.begin();
    let secret = trx.create(&Record { title: "secret".into(), artist: "shared".into() }).await?.id();
    let open = trx.create(&Record { title: "open".into(), artist: "shared".into() }).await?.id();
    trx.commit().await?;
    server_agent.hidden.lock().unwrap().insert(secret);

    let reader = client_node.context_async(READS_HIDDEN).await?;
    let readable = reader.query_wait::<RecordView>(nocache("artist = 'shared'")?).await?;
    assert_eq!(sorted(readable.ids()), sorted(vec![secret, open]));
    // The same client subscribes again under credentials that cannot read the
    // secret, while its first query, which can, stands with the server.
    let other = client_node.context_async(Credential(2)).await?;
    let limited = other.query_wait::<RecordView>(nocache("artist = 'shared'")?).await?;

    let account = |query: proto::QueryId| {
        received.responses().into_iter().find_map(|body| match body {
            proto::NodeResponseBody::QuerySubscribed { query_id, target } if query_id == query => listed(&target),
            _ => None,
        })
    };
    let initial = |query: proto::QueryId| -> HashSet<EntityId> {
        let tag = [(query, proto::MembershipChange::Initial)];
        received.updates().into_iter().flatten().filter(|item| item.predicate_relevance == tag).map(|item| item.entity_id).collect()
    };
    assert!(account(readable.query_id()).unwrap().contains_key(&secret), "the first query's account lists the secret");
    assert!(initial(readable.query_id()).contains(&secret), "and its push carries it");
    assert!(!account(limited.query_id()).unwrap().contains_key(&secret), "the second query's account does not");
    assert!(!initial(limited.query_id()).contains(&secret), "nor does its push");

    // The second query's own copy lists the secret the client holds, so its
    // client asks for it under the second query's credentials, and learns the
    // server holds none of it to send.
    let synced: Vec<_> = received
        .responses()
        .into_iter()
        .filter_map(|body| match body {
            proto::NodeResponseBody::EntitiesSynced { held, unavailable } => Some((held, unavailable)),
            _ => None,
        })
        .collect();
    assert!(synced.iter().any(|(_, unavailable)| unavailable.contains(&secret)), "the repair finds the secret unavailable: {synced:?}");
    assert!(synced.iter().all(|(held, _)| held.iter().all(|entity| entity.entity_id != secret)), "and never reports it held");
    Ok(())
}

/// A Fetch from the given context, which ends when the Fetch does.
type StartFetch = fn(Context) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

/// A live query under one credential stands with the server while a Fetch
/// under another, whose answer is lost, stays in flight; then the Fetch is
/// dropped. Each time, the client misses a live update of the standing
/// query's member, so applying the next one asks the server for the missed
/// event under the credentials the client applies its peer's pushes under:
/// both while the Fetch is in flight, the standing query's alone after.
async fn a_fetch_widens_push_credentials_only_until_it_is_dropped(start_fetch: StartFetch) -> anyhow::Result<()> {
    let server_agent = CredentialAgent::default();
    // Once armed, the gate loses the next Fetch answer, and while `losing`
    // names an entity, the next update that pushes it.
    let (lose_answer, answer_lost) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let losing: Arc<Mutex<Option<EntityId>>> = Arc::default();
    let lost = Arc::new(AtomicBool::new(false));
    let (server_node, client_node, _connection) = {
        let (lose_answer, answer_lost, losing, lost) = (lose_answer.clone(), answer_lost.clone(), losing.clone(), lost.clone());
        gated_under(server_agent.clone(), move |message| {
            let fetch_answer =
                matches!(message, proto::NodeMessage::Response(response) if matches!(response.body, proto::NodeResponseBody::Fetch(_)));
            if fetch_answer && lose_answer.swap(false, Ordering::SeqCst) {
                answer_lost.store(true, Ordering::SeqCst);
                return true;
            }
            let update = losing.lock().unwrap().is_some_and(|id| pushes(message, id));
            update && !lost.swap(true, Ordering::SeqCst)
        })
        .await?
    };
    let writer = server_node.context_async(Credential(0)).await?;
    let trx = writer.begin();
    let standing = trx.create(&Record { title: "standing".into(), artist: "0".into() }).await?.id();
    trx.create(&Record { title: "fetched".into(), artist: "0".into() }).await?;
    trx.commit().await?;
    let watcher = client_node.context_async(Credential(2)).await?;
    let query = watcher.query_wait::<RecordView>(nocache("title = 'standing'")?).await?;

    // The client misses the update of one edit, and applying the next asks
    // for the missed event: return the credentials that request carried.
    let edit_after_a_lost_update = |edit: u32| {
        let (writer, server_agent, losing, lost, server_node) =
            (writer.clone(), server_agent.clone(), losing.clone(), lost.clone(), server_node.clone());
        async move {
            lost.store(false, Ordering::SeqCst);
            *losing.lock().unwrap() = Some(standing);
            let trx = writer.begin();
            trx.get::<Record>(&standing).await?.artist()?.set(&format!("{edit}"))?;
            trx.commit().await?;
            let missed: Vec<_> = server_node.storage.get_state(standing).await?.payload.state.head.ids().cloned().collect();
            wait_until("the edit's update is lost", || lost.load(Ordering::SeqCst)).await;
            *losing.lock().unwrap() = None;
            let trx = writer.begin();
            trx.get::<Record>(&standing).await?.artist()?.set(&format!("{edit} again"))?;
            trx.commit().await?;
            let asked = || {
                server_agent
                    .event_requests
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(ids, _)| ids == &missed)
                    .map(|(_, credentials)| credentials.clone())
            };
            wait_until("the client asks for the missed event", || asked().is_some()).await;
            anyhow::Ok(asked().unwrap())
        }
    };

    let fetching = client_node.context_async(READS_HIDDEN).await?;
    lose_answer.store(true, Ordering::SeqCst);
    let fetch = tokio::spawn(start_fetch(fetching));
    wait_until("the Fetch's answer is lost", || answer_lost.load(Ordering::SeqCst)).await;
    assert_eq!(edit_after_a_lost_update(1).await?, BTreeSet::from([READS_HIDDEN, Credential(2)]), "the Fetch in flight widens them");

    fetch.abort();
    assert!(fetch.await.unwrap_err().is_cancelled());
    assert_eq!(edit_after_a_lost_update(2).await?, BTreeSet::from([Credential(2)]), "the dropped Fetch no longer does");
    wait_until("the standing query follows its member", || query.peek()[0].artist().unwrap() == "2 again").await;
    assert_eq!(
        server_agent.subscribes.lock().unwrap().iter().filter(|id| **id == query.query_id()).count(),
        1,
        "without subscribing again"
    );
    Ok(())
}

#[tokio::test]
async fn a_dropped_fetch_stops_widening_the_credentials_its_peers_pushes_apply_under() -> anyhow::Result<()> {
    a_fetch_widens_push_credentials_only_until_it_is_dropped(|context| {
        Box::pin(async move { context.fetch::<RecordView>("title = 'fetched'").await.map(drop).map_err(Into::into) })
    })
    .await
}

#[tokio::test]
async fn a_dropped_catalog_fetch_stops_widening_them_as_well() -> anyhow::Result<()> {
    // A catalog read bypasses the server's read policy, but the client
    // registers it under its context's credentials as it does any Fetch.
    a_fetch_widens_push_credentials_only_until_it_is_dropped(|context| {
        Box::pin(async move { context.fetch::<SysModelRowView>("true").await.map(drop).map_err(Into::into) })
    })
    .await
}
