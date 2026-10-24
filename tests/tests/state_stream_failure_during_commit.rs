//! A state stream that fails while one of its updates is committing must leave
//! the receiver's resident entity agreeing with its storage.
//!
//! The receiver applies each pushed update in an application job that first
//! commits storage and then advances the resident entity. When the stream
//! fails, the receiver drops its in-flight jobs. Sled finishes a commit on a
//! blocking thread even after its caller is dropped, so a job dropped while it
//! awaits its commit leaves storage holding the update and the resident entity
//! without it. This test holds that window open with a storage wrapper and
//! fails the stream by disconnecting the peers.
mod common;

use ankurah::core::error::{MutationError, RetrievalError};
use ankurah::core::storage::{StorageCommitOutcome, StorageTransaction};
use ankurah::proto::{Attested, Clock, EntityState, Event, EventId};
use ankurah::{ModelId, Mutable};
use anyhow::Result;
use async_trait::async_trait;
use common::*;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn a_stream_failure_during_a_commit_leaves_the_resident_entity_matching_storage() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let hold = Arc::new(CommitHold::default());
    let client_storage = Arc::new(HoldingEngine { inner: SledStorageEngine::new_test()?, hold: hold.clone() });
    let client = Node::new(client_storage.clone(), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let connection = LocalProcessConnection::new(&client, &server).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let query = reader.query_wait::<PetView>(nocache("name = 'Rex'")?).await?;
    query.wait_durable_answered().await?;
    assert_eq!(query.ids(), [pet.id()]);

    // The server's edit reaches the client as a pushed update; hold the
    // client's job once its storage commit has landed.
    hold.arm(pet.id());
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("2")?;
    trx.commit().await?;
    tokio::time::timeout(Duration::from_secs(10), hold.held.notified()).await?;

    // Disconnecting fails the stream while the job waits for its commit.
    drop(connection);
    if tokio::time::timeout(Duration::from_secs(10), hold.dropped.notified()).await.is_err() {
        // The client kept the job: let its commit return and the job finish.
        hold.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            while client.get_resident_entity(pet.id()).map(|entity| entity.head()) != Some(stored_head(&client_storage, &pet).await) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
    }

    let resident = client.get_resident_entity(pet.id()).expect("the live query holds the pet").head();
    assert_eq!(
        resident,
        stored_head(&client_storage, &pet).await,
        "the client's resident pet must hold every update its storage committed"
    );
    Ok(())
}

async fn stored_head(storage: &HoldingEngine, pet: &PetView) -> Clock {
    storage.get_state(pet.id()).await.expect("the client stored the pet").payload.state.head
}

/// Holds the caller of one entity's next storage commit after the commit has
/// landed, as a busy blocking thread holds the task awaiting it.
#[derive(Default)]
struct CommitHold {
    target: std::sync::Mutex<Option<EntityId>>,
    /// Signaled when the target's commit has landed and its caller is held.
    held: tokio::sync::Notify,
    /// Signaled when the held caller is dropped instead of resuming.
    dropped: tokio::sync::Notify,
    /// Lets a held caller resume.
    release: tokio::sync::Notify,
}

impl CommitHold {
    fn arm(&self, entity: EntityId) { *self.target.lock().unwrap() = Some(entity); }

    /// Whether a transaction that wrote `entities` is the one to hold, disarming the hold if so.
    fn take(&self, entities: &[EntityId]) -> bool {
        let mut target = self.target.lock().unwrap();
        let hit = target.is_some_and(|entity| entities.contains(&entity));
        if hit {
            *target = None;
        }
        hit
    }
}

/// Signals [`CommitHold::dropped`] unless the held caller resumed.
struct DropSignal<'a> {
    hold: &'a CommitHold,
    resumed: bool,
}

impl Drop for DropSignal<'_> {
    fn drop(&mut self) {
        if !self.resumed {
            self.hold.dropped.notify_one();
        }
    }
}

/// Sled storage whose transactions can hold one commit's caller.
struct HoldingEngine {
    inner: SledStorageEngine,
    hold: Arc<CommitHold>,
}

#[async_trait]
impl StorageEngine for HoldingEngine {
    type Value = ();
    type Transaction<'a> = HoldingTransaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> {
        HoldingTransaction { hold: &self.hold, inner: self.inner.transaction(), entities: Vec::new() }
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> { self.inner.get_state(id).await }

    async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<ankql::ast::Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.inner.fetch_states(selection).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        self.inner.get_events(event_ids).await
    }

    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }

    async fn delete_all(&self) -> Result<bool, MutationError> { self.inner.delete_all().await }

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> { self.inner.list_materializations().await }

    // Forwarded: the trait default drops the resolver, and sled needs it to
    // write model materializations.
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn ankurah::core::schema::CatalogResolver>) {
        self.inner.set_catalog_resolver(resolver);
    }
}

/// A sled transaction that remembers which entities it wrote, so its commit
/// can hold the caller when it wrote the armed entity.
struct HoldingTransaction<'a> {
    hold: &'a CommitHold,
    inner: <SledStorageEngine as StorageEngine>::Transaction<'a>,
    entities: Vec<EntityId>,
}

#[async_trait]
impl StorageTransaction for HoldingTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> { self.inner.add_events(events).await }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
        self.entities.push(state.payload.entity_id);
        self.inner.set_state(expected_head, state).await
    }

    async fn commit(self) -> Result<StorageCommitOutcome, MutationError> {
        let Self { hold, inner, entities } = self;
        let outcome = inner.commit().await?;
        if matches!(outcome, StorageCommitOutcome::Committed(_)) && hold.take(&entities) {
            let mut signal = DropSignal { hold, resumed: false };
            hold.held.notify_one();
            hold.release.notified().await;
            signal.resumed = true;
        }
        Ok(outcome)
    }
}
