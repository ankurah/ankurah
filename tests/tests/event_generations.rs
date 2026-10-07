//! Generations through real commits, storage reloads, and peer snapshots.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, proto, View};
use anyhow::Result;
use common::{
    durable_sled_setup, ephemeral_sled_setup, GatedConnection, MessageGate, Node, PermissiveAgent, Record, RecordView, SledStorageEngine,
    StorageEngine,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

/// Connect `client` to `server` through a tap on the wire that records every
/// event the client asks the server for and holds nothing.
async fn connect_recording_event_requests(
    client: &TestNode,
    server: &TestNode,
) -> (GatedConnection, MessageGate, Arc<Mutex<Vec<proto::EventId>>>) {
    let requested: Arc<Mutex<Vec<proto::EventId>>> = Arc::default();
    let recorder = requested.clone();
    let (connection, gate) = GatedConnection::new(client, server, move |message: &proto::NodeMessage| {
        if let proto::NodeMessage::Request { request, .. } = message {
            if let proto::NodeRequestBody::GetEvents { event_ids } = &request.body {
                recorder.lock().unwrap().extend(event_ids.iter().cloned());
            }
        }
        false
    })
    .await;
    (connection, gate, requested)
}

/// A chain counts 1, 2, 3; unequal concurrent tips survive resident eviction
/// and reload without event reads, and the next commit counts from their maximum.
#[tokio::test]
async fn commits_stamp_from_the_head_including_after_multitip_rehydration() -> Result<()> {
    let storage = Arc::new(CountEventReads { inner: SledStorageEngine::new_test()?, reads: AtomicUsize::new(0) });
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let ctx = node.context_async(c).await?;

    let trx = ctx.begin();
    let id = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?.id();
    let mut chain = trx.commit_and_return_events().await?;
    let record = ctx.get::<RecordView>(id).await?;
    let stale = ctx.begin();
    record.edit(&stale)?.artist()?.set(&"stale".to_owned())?;
    for title in ["t1", "t2"] {
        let trx = ctx.begin();
        record.edit(&trx)?.title()?.set(&title.to_owned())?;
        chain.extend(trx.commit_and_return_events().await?);
    }
    assert_eq!(chain.iter().map(proto::Event::generation).collect::<Vec<_>>(), [1, 2, 3]);

    // Forked from the genesis before the chain, so it extends generation 1 however late it commits.
    let stale = stale.commit_and_return_events().await?.remove(0);
    assert_eq!(stale.generation(), 2);
    let stored = node.storage.get_state(id).await?.payload.state;
    assert_eq!(stored.head, proto::Clock::new(vec![(3, chain[2].id()), (2, stale.id())])?, "each tip retains its own generation");
    drop(record);
    assert!(node.get_resident_entity(id).is_none(), "the entity must actually be evicted");
    let reads_before = storage.reads.load(Ordering::SeqCst);
    let record = ctx.get::<RecordView>(id).await?;
    assert_eq!(record.entity().head(), stored.head, "reload retains each tip's generation");
    assert_eq!(storage.reads.load(Ordering::SeqCst), reads_before, "rehydration needs no event reads");

    let merge = ctx.begin();
    record.edit(&merge)?.title()?.set(&"merged".to_owned())?;
    let merged = merge.commit_and_return_events().await?;
    assert_eq!(merged[0].parent.len(), 2, "the merge extends both tips");
    assert_eq!(merged[0].generation(), 4, "one more than the greater tip, generation 3");
    let stored = node.storage.get_state(id).await?.payload.state;
    assert_eq!(stored.head, proto::Clock::singleton(&merged[0]));
    Ok(())
}

/// Sled preserves the update, including the parent annotations that determine its generation.
#[tokio::test]
async fn a_stamped_update_reads_back_from_sled_unchanged() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(c).await?;

    let trx = ctx.begin();
    let record = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
    let id = record.id();
    // Editing after the id is demanded makes an update on the frozen genesis, in the same commit.
    record.title()?.set(&"t1".to_owned())?;
    let update = trx.commit_and_return_events().await?.into_iter().find(|event| !event.is_entity_create()).expect("an update");
    assert_eq!(update.entity_id, id);
    assert_eq!(update.generation(), 2);
    let stored = node.storage.get_events(vec![update.id()]).await?;
    assert_eq!(stored.into_iter().map(|event| event.payload).collect::<Vec<_>>(), vec![update]);
    Ok(())
}

/// Editing a snapshot uses its parent annotations without fetching the head event.
#[tokio::test]
async fn an_ephemeral_node_stamps_an_edit_of_a_snapshot_without_fetching_its_head_event() -> Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let (_connection, _gate, requested) = connect_recording_event_requests(&client, &server).await;
    client.system.wait_system_ready().await?;
    let server_ctx = server.context_async(c).await?;
    let client_ctx = client.context_async(c).await?;

    let trx = server_ctx.begin();
    let id = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?.id();
    trx.commit().await?;
    let genesis = proto::EventId::from(id.to_bytes());
    let record = client_ctx.get::<RecordView>(id).await?;
    assert!(client.storage.get_events(vec![genesis.clone()]).await?.is_empty(), "a snapshot brings no events");
    let snapshot = client.storage.get_state(id).await?.payload.state;
    assert_eq!(snapshot.head, proto::Clock::genesis(genesis.clone()));

    let trx = client_ctx.begin();
    record.edit(&trx)?.title()?.set(&"edited".to_owned())?;
    let update = trx.commit_and_return_events().await?.remove(0);
    assert_eq!(update.generation(), 2);
    assert_eq!(record.entity().head(), proto::Clock::singleton(&update));
    assert!(!requested.lock().unwrap().contains(&genesis), "nothing asked the server for the head event");
    Ok(())
}

/// Receiving an honest direct extension of a snapshot needs no parent payloads.
#[tokio::test]
async fn an_ephemeral_node_admits_an_honest_update_over_parents_it_does_not_hold_without_fetching() -> Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let (_connection, _gate, requested) = connect_recording_event_requests(&client, &server).await;
    client.system.wait_system_ready().await?;
    let server_ctx = server.context_async(c).await?;
    let client_ctx = client.context_async(c).await?;
    // A standing query with the server lets the client accept pushed updates from it.
    let _relay = client_ctx.query_wait::<RecordView>("title = 'no-such-title'").await?;

    let trx = server_ctx.begin();
    let id = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?.id();
    trx.commit().await?;
    let genesis = proto::EventId::from(id.to_bytes());
    let record = client_ctx.get::<RecordView>(id).await?;
    let trx = server_ctx.begin();
    server_ctx.get::<RecordView>(id).await?.edit(&trx)?.title()?.set(&"t1".to_owned())?;
    let update = trx.commit_and_return_events().await?.remove(0);
    assert_eq!(update.parent, proto::Clock::genesis(genesis.clone()));

    let item = proto::SubscriptionUpdateItem {
        entity_id: id,
        content: proto::UpdateContent::EventOnly(vec![proto::Attested::opt(update, None).into()]),
        predicate_relevance: vec![],
    };
    let body = proto::NodeUpdateBody::SubscriptionUpdate { items: vec![item] };
    client
        .handle_message(proto::NodeMessage::Update(proto::NodeUpdate { id: proto::UpdateId::new(), from: server.id, to: client.id, body }))
        .await?;

    assert_eq!(record.title()?, "t1", "the honest update is admitted");
    assert!(!requested.lock().unwrap().contains(&genesis), "nothing asked the server for the parent");
    assert!(client.storage.get_events(vec![genesis]).await?.is_empty(), "nor was the parent cached");
    Ok(())
}

/// Count reads at the actual storage boundary while leaving Sled's behavior intact.
struct CountEventReads {
    inner: SledStorageEngine,
    reads: AtomicUsize,
}

#[async_trait::async_trait]
impl StorageEngine for CountEventReads {
    type Value = <SledStorageEngine as StorageEngine>::Value;
    type Transaction<'a> = <SledStorageEngine as StorageEngine>::Transaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { self.inner.transaction() }

    async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, ankurah::error::RetrievalError> {
        self.inner.get_state(id).await
    }

    async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<ankql::ast::Resolved>,
    ) -> Result<Vec<proto::Attested<proto::EntityState>>, ankurah::error::RetrievalError> {
        self.inner.fetch_states(selection).await
    }

    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, ankurah::error::RetrievalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_events(ids).await
    }

    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, ankurah::error::RetrievalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.dump_entity_events(id).await
    }

    async fn delete_all(&self) -> Result<bool, ankurah::error::MutationError> { self.inner.delete_all().await }

    async fn list_materializations(&self) -> Result<Vec<proto::ModelId>, ankurah::error::RetrievalError> {
        self.inner.list_materializations().await
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn ankurah::core::schema::CatalogResolver>) {
        self.inner.set_catalog_resolver(resolver);
    }
}
