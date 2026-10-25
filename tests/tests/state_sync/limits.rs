//! Limited results: a cut over tied members, a client's own cut that differs
//! from the server's, and what a limited Fetch reads of the client's copy.

use super::*;
use ankql::ast::{Predicate, Resolved, Selection};
use ankurah::core::{
    error::RetrievalError,
    storage::{CatalogResolver, GetStateResult},
};
use async_trait::async_trait;

fn ids(pets: Vec<PetView>) -> Vec<EntityId> { pets.iter().map(|pet| pet.id()).collect() }

#[tokio::test]
async fn a_limited_query_over_tied_members_holds_the_servers_members() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let trx = server.begin();
    // Two pets tie for the cut's first place.
    for (n, age) in ["1", "1", "2", "3", "4"].into_iter().enumerate() {
        trx.create(&Pet { name: format!("pet {n}"), age: age.into() }).await?;
    }
    trx.commit().await?;
    let selection = "age >= '1' ORDER BY age ASC LIMIT 3";
    let cut = ids(server.fetch(selection).await?);
    assert_eq!(ids(client.fetch(selection).await?), cut, "a Fetch breaks the tie as the server does");
    let query = client.query_wait::<PetView>(nocache(selection)?).await?;
    assert_eq!(query.ids(), cut, "and so does a live query");

    // The first of the tied members leaves, and the next pet fills the cut.
    let trx = server.begin();
    trx.get::<Pet>(&cut[0]).await?.age()?.replace("0")?;
    trx.commit().await?;
    let cut = ids(server.fetch(selection).await?);
    wait_until("the live query's cut is the server's again", || query.ids() == cut).await;
    assert_eq!(ids(client.fetch(selection).await?), cut);
    Ok(())
}

#[tokio::test]
async fn a_live_query_whose_cut_differs_from_the_servers_converges_over_a_shared_broad_tree() -> anyhow::Result<()> {
    let server_node = durable_sled_setup().await?;
    let server = server_node.context_async(DEFAULT_CONTEXT).await?;
    let trx = server.begin();
    let mut pets = Vec::new();
    for (name, age) in [("a", "08"), ("b", "10"), ("c", "12"), ("d", "15")] {
        pets.push(trx.create(&Pet { name: name.into(), age: age.into() }).await?.id());
    }
    trx.commit().await?;
    let client_node = ephemeral_sled_setup().await?;
    let (connection, offers) = connect_recording_offers(&client_node, &server_node).await?;
    let client = client_node.context_async(DEFAULT_CONTEXT).await?;
    assert_eq!(client.fetch::<PetView>("age > '07' ORDER BY age").await?.len(), 4);
    let broad = specs(&offers, None).pop().unwrap();
    drop(connection);
    server_node.deregister_peer(client_node.id);
    client_node.deregister_peer(server_node.id);

    // While the client is away, the first of the first two moves to the end
    // and the last moves into them.
    let trx = server.begin();
    trx.get::<Pet>(&pets[0]).await?.age()?.replace("20")?;
    trx.get::<Pet>(&pets[3]).await?.age()?.replace("09")?;
    trx.commit().await?;

    let (_connection, offers) = connect_recording_offers(&client_node, &server_node).await?;
    let selection = "age > '07' ORDER BY age LIMIT 2";
    let query = client.query_wait::<PetView>(nocache(selection)?).await?;
    let offer = offers.lock().unwrap().iter().find(|(id, _)| *id == Some(query.query_id())).map(|(_, known)| known.clone()).unwrap();
    assert_eq!(offer.spec, broad, "the live query offers the broad Fetch's tree");
    let offered: Vec<_> = listed(&offer).expect("a cut of two is listed by its heads").into_keys().collect();
    assert_eq!(offered, sorted(vec![pets[0], pets[1]]), "describing the copy's own first two");
    assert_eq!(query.ids(), [pets[3], pets[1]], "and it is ready with the server's");
    assert_eq!(query.peek()[0].age()?, "09");
    assert_eq!(ids(client.fetch(selection).await?), [pets[3], pets[1]]);
    assert_eq!(specs(&offers, None).pop(), Some(broad), "a limited Fetch offers the broad tree as well");
    Ok(())
}

/// Sled storage that records how many rows each selection read of one model
/// returns.
struct Counted {
    inner: SledStorageEngine,
    model: Mutex<Option<proto::ModelId>>,
    reads: Mutex<Vec<usize>>,
}

#[async_trait]
impl StorageEngine for Counted {
    type Value = <SledStorageEngine as StorageEngine>::Value;
    type Transaction<'a> = <SledStorageEngine as StorageEngine>::Transaction<'a>;
    fn transaction(&self) -> Self::Transaction<'_> { self.inner.transaction() }
    async fn get_state(&self, id: EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
        self.inner.get_state(id).await
    }
    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
        let states = self.inner.fetch_states(selection).await?;
        if self.model.lock().unwrap().is_some_and(|model| selection.predicate.referenced_models().contains(&model)) {
            self.reads.lock().unwrap().push(states.len());
        }
        Ok(states)
    }
    async fn get_states(&self, ids: Vec<EntityId>, predicate: &Predicate<Resolved>) -> Result<Vec<GetStateResult>, RetrievalError> {
        self.inner.get_states(ids, predicate).await
    }
    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.get_events(ids).await
    }
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }
    async fn delete_all(&self) -> Result<bool, MutationError> { self.inner.delete_all().await }
    async fn list_materializations(&self) -> Result<Vec<proto::ModelId>, RetrievalError> { self.inner.list_materializations().await }
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { self.inner.set_catalog_resolver(resolver) }
}

#[tokio::test]
async fn a_limited_fetch_reads_no_more_of_the_clients_copy_than_its_cut() -> anyhow::Result<()> {
    let server_node = durable_sled_setup().await?;
    let storage = Arc::new(Counted { inner: SledStorageEngine::new_test()?, model: Mutex::default(), reads: Mutex::default() });
    let client_node = Node::new(storage.clone(), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&server_node, &client_node).await?;
    client_node.wait_ready().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    create_pets(&server, "1", (0..64).map(|n| format!("pet {n:04}"))).await?;
    *storage.model.lock().unwrap() = Some(client.resolve_model_id::<Pet>().await?);
    assert_eq!(client.fetch::<PetView>("name >= 'pet'").await?.len(), 64, "the client's copy holds every match");
    storage.reads.lock().unwrap().clear();

    let limited = client.fetch::<PetView>("name >= 'pet' ORDER BY name LIMIT 4").await?;
    let names = limited.iter().map(|pet| pet.name()).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(names, ["pet 0000", "pet 0001", "pet 0002", "pet 0003"]);
    let reads = storage.reads.lock().unwrap().clone();
    assert!(!reads.is_empty() && reads.iter().all(|rows| *rows <= 4), "each read of the copy keeps to the cut: {reads:?}");
    Ok(())
}
