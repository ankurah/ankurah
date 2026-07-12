//! GClock pin (iii), sled arm (D2 M4, plan REV 5 section K home 3): the
//! per-tip head generations survive set_state and rehydration. Sled
//! bincodes the whole StateFragment, so the annotation rides the struct;
//! this pin keeps that true if sled ever grows a bespoke record layout.
//! Exercises both row-decode paths (get_state and fetch_states), a
//! multi-tip head, and the u32::MAX saturation sentinel.

use std::collections::BTreeMap;
use std::sync::Arc;

use ankurah::ankql;
use ankurah::core::schema::CatalogResolver;
use ankurah::core::storage::StorageEngine;
use ankurah::proto;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;

fn event_id(b: u8) -> proto::EventId { proto::EventId::from_bytes([b; 32]) }

struct AlbumModelResolver {
    model: proto::EntityId,
}

impl CatalogResolver for AlbumModelResolver {
    fn resolve(&self, _collection: &str, _name: &str) -> Option<proto::EntityId> { None }
    fn name_for(&self, _id: &proto::EntityId) -> Option<String> { None }
    fn model_id_for(&self, collection: &str) -> Option<proto::EntityId> { (collection == "album").then_some(self.model) }
}

#[tokio::test]
async fn gclock_survives_set_state_and_rehydration() -> Result<()> {
    let engine = SledStorageEngine::new_test()?;
    let model = proto::EntityId::from_bytes([0xEE; 16]);
    let resolver: Arc<dyn CatalogResolver> = Arc::new(AlbumModelResolver { model });
    engine.set_catalog_resolver(Arc::downgrade(&resolver));
    let collection = engine.collection(&"album".into()).await?;

    let entity_id = proto::EntityId::new();
    let (e1, e2) = (event_id(1), event_id(2));
    let head = proto::Clock::from(vec![e1.clone(), e2.clone()]);
    let head_generations = proto::GClock::new(vec![(7, e1.clone()), (u32::MAX, e2.clone())]);
    let state = proto::State {
        state_buffers: proto::StateBuffers(BTreeMap::new()),
        head: head.clone(),
        head_generations: head_generations.clone(),
    };
    let attested = proto::Attested::opt(proto::EntityState { entity_id, model, state }, None);

    collection.set_state(attested).await?;

    // get_state row decode
    let read = collection.get_state(entity_id).await?;
    assert_eq!(read.payload.model, model, "the bare engine restores the model envelope through its resolver");
    assert_eq!(read.payload.state.head, head, "the head round-trips (precondition)");
    assert_eq!(
        read.payload.state.head_generations, head_generations,
        "GClock pin (iii): the per-tip head generations must survive set_state + get_state rehydration"
    );

    // fetch_states row decode (a separate decode path in the engine)
    let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    let fetched = collection.fetch_states(&all).await?;
    let row = fetched.iter().find(|s| s.payload.entity_id == entity_id).expect("the row is fetchable");
    assert_eq!(row.payload.model, model, "fetch_states restores the same resolved model envelope");
    assert_eq!(
        row.payload.state.head_generations, head_generations,
        "GClock pin (iii): the per-tip head generations must survive fetch_states rehydration"
    );

    Ok(())
}
