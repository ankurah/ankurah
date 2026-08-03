//! Racing activations on one livequery must converge to the NEWEST
//! selection: whatever a superseded activation does on its way out, the
//! successor must land, serve, and wake waiters. The interleave is
//! forced with a storage engine that delays `fetch_states` by a marker
//! embedded in the selection, so the test is deterministic rather than
//! a timing coin flip. Concurrent activations of one query are not
//! serialized (issue #146); the broader coherence map for that substrate
//! is issue #431. This pin guards the convergence invariant against any
//! cleanup on the superseded path taking down its successor.

mod common;
use common::*;

use ankurah::core::livequery::{LocalStatus, QueryStatus, RemoteStatus};
use ankurah::core::storage::{StorageCollection, StorageEngine};
use ankurah::error::MutationError;
use ankurah::proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId};
use ankurah::signals::Peek;
use ankurah::{Node, PermissiveAgent};
use std::sync::Arc;
use std::time::Duration;

/// A `fetch_states` delay keyed on a marker inside the selection, so each
/// activation's local fetch can be given its own duration.
struct DelayEngine(Arc<SledStorageEngine>);

fn delay_for(selection: &ankql::ast::Selection) -> Option<Duration> {
    let text = format!("{:?}", selection.predicate);
    if text.contains("SLOW_A") {
        Some(Duration::from_millis(300))
    } else if text.contains("SLOW_B") {
        Some(Duration::from_millis(3000))
    } else {
        None
    }
}

#[async_trait::async_trait]
impl StorageEngine for DelayEngine {
    type Value = Vec<u8>;
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, ankurah::error::RetrievalError> {
        Ok(Arc::new(DelayCollection(self.0.collection(id).await?)))
    }
    async fn delete_all_collections(&self) -> Result<bool, MutationError> { self.0.delete_all_collections().await }
}

struct DelayCollection(Arc<dyn StorageCollection>);

#[async_trait::async_trait]
impl StorageCollection for DelayCollection {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> { self.0.set_state(state).await }
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, ankurah::error::RetrievalError> { self.0.get_state(id).await }
    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, ankurah::error::RetrievalError> {
        if let Some(d) = delay_for(selection) {
            tokio::time::sleep(d).await;
        }
        self.0.fetch_states(selection).await
    }
    async fn add_event(&self, e: &Attested<Event>) -> Result<bool, MutationError> { self.0.add_event(e).await }
    async fn get_events(&self, ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, ankurah::error::RetrievalError> {
        self.0.get_events(ids).await
    }
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, ankurah::error::RetrievalError> {
        self.0.dump_entity_events(id).await
    }
}

/// v2's activation fetches for 300ms; v3 is minted at 100ms, inside
/// that fetch, so v2 completes while already superseded, with v3's own
/// activation (a 3000ms fetch) still in flight. Whatever the superseded
/// v2 path does on its way out, v3 must land at Active version 3
/// serving its one matching row, and a waiter must wake. The failure
/// shape this discriminates: any superseded-path cleanup that removes
/// the registration its successor is about to update leaves the
/// livequery permanently deregistered, stale-statused, and unwakeable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn superseding_selection_survives_a_stale_activations_clawback() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(DelayEngine(Arc::new(SledStorageEngine::new_test().unwrap()))), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    {
        let trx = ctx.begin();
        trx.create(&Album { name: "SLOW_A".into(), year: "a".into() }).await?;
        trx.create(&Album { name: "SLOW_B".into(), year: "b".into() }).await?;
        trx.commit().await?;
    }

    let lq = ctx.query_wait::<AlbumView>("name = 'nothing'").await?;
    assert_eq!(lq.status().peek(), QueryStatus { local: LocalStatus::Active { version: 1 }, remote: RemoteStatus::None });

    lq.update_selection("name = 'SLOW_A'")?; // v2: activation with a 300ms fetch
    tokio::time::sleep(Duration::from_millis(100)).await;
    lq.update_selection("name = 'SLOW_B'")?; // v3: activation with a 3000ms fetch

    let waited = tokio::time::timeout(Duration::from_millis(6000), lq.wait_initialized()).await;
    assert!(waited.is_ok(), "a waiter on the newest selection must wake");

    assert_eq!(lq.ids().len(), 1, "the v3 selection matches one row; the livequery must serve it, got {:?}", lq.ids());
    assert_eq!(
        lq.status().peek(),
        QueryStatus { local: LocalStatus::Active { version: 3 }, remote: RemoteStatus::None },
        "the newest selection must be the one serving"
    );
    Ok(())
}
