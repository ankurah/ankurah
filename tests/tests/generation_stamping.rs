mod common;

use ankurah::proto;
use anyhow::Result;
use common::*;
use std::collections::HashMap;

/// Recompute every event's topological level by walking parent edges over the
/// dumped event set: depth(genesis) = 1, depth(e) = 1 + max(depth(parents)).
/// This is the brute-force oracle R-D2-2a compares stamped generations
/// against (plan REV 4 section 3 M2; 266-A defines the level). The recursion
/// is a transient assertion-time walk over event payloads, not a store.
fn brute_force_depths(events: &[proto::Attested<proto::Event>]) -> HashMap<proto::EventId, u32> {
    let by_id: HashMap<proto::EventId, &proto::Event> = events.iter().map(|e| (e.payload.id(), &e.payload)).collect();

    fn depth(id: &proto::EventId, by_id: &HashMap<proto::EventId, &proto::Event>, memo: &mut HashMap<proto::EventId, u32>) -> u32 {
        if let Some(d) = memo.get(id) {
            return *d;
        }
        let event = by_id.get(id).unwrap_or_else(|| panic!("parent event {id} missing from the dumped set; the oracle needs full lineage"));
        let d = if event.parent.is_empty() {
            1
        } else {
            1 + event.parent.iter().map(|p| depth(p, by_id, memo)).max().expect("nonempty parent clock")
        };
        memo.insert(id.clone(), d);
        d
    }

    let mut memo = HashMap::new();
    for e in events {
        depth(&e.payload.id(), &by_id, &mut memo);
    }
    memo
}

/// Assert every stored event's carried generation equals its brute-force
/// topological depth, and return (event count, max depth) for shape checks.
async fn assert_stamps_equal_depth(ctx: &ankurah::Context, entity_id: proto::EntityId, shape: &str) -> Result<(usize, u32)> {
    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(entity_id).await?;
    let depths = brute_force_depths(&events);
    for e in &events {
        let id = e.payload.id();
        let expected = depths[&id];
        assert_eq!(
            e.payload.generation, expected,
            "R-D2-2a [{shape}]: event {id} carries generation {} but its brute-force topological depth is {expected}",
            e.payload.generation
        );
    }
    Ok((events.len(), depths.values().copied().max().unwrap_or(0)))
}

/// R-D2-2a, linear shape: a chain of sequential commits stamps 1, 2, 3.
/// The committer stamps 1 + max(parent generations) at event creation
/// (plan REV 4, D2-2); genesis is exactly 1.
#[tokio::test]
async fn r_d2_2a_linear_stamps_equal_brute_force_depth() -> Result<()> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx.get::<RecordView>(rec_id).await?;
    for i in 1..=2 {
        let trx = ctx.begin();
        view.edit(&trx)?.title().set(&format!("t{i}"))?;
        trx.commit().await?;
    }

    let (count, max_depth) = assert_stamps_equal_depth(&ctx, rec_id, "linear").await?;
    assert_eq!(count, 3, "genesis plus two edits");
    assert_eq!(max_depth, 3, "the chain is three levels deep");
    Ok(())
}

/// R-D2-2a, diamond shape: two concurrent edits of the same head (both
/// generation 2) and a merge edit over the two-tip antichain (generation 3).
#[tokio::test]
async fn r_d2_2a_diamond_stamps_equal_brute_force_depth() -> Result<()> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx.get::<RecordView>(rec_id).await?;

    // Two transactions fork the same genesis head: concurrent siblings.
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();
    view.edit(&trx1)?.title().set(&"left".to_owned())?;
    view.edit(&trx2)?.artist().set(&"right".to_owned())?;
    trx1.commit().await?;
    trx2.commit().await?;

    // The resident head is now the two-sibling antichain; one more edit
    // parents on both tips: the merge point of the diamond.
    assert_eq!(view.entity().head().len(), 2, "precondition: two-tip antichain");
    let trx = ctx.begin();
    view.edit(&trx)?.title().set(&"merged".to_owned())?;
    trx.commit().await?;

    let (count, max_depth) = assert_stamps_equal_depth(&ctx, rec_id, "diamond").await?;
    assert_eq!(count, 4, "genesis, two siblings, one merge");
    assert_eq!(max_depth, 3, "the merge sits one level above the siblings");
    Ok(())
}

/// R-D2-2a, crossed shape: two CONCURRENT merge edits over the same two-tip
/// antichain (both generation 3, crossing the diamond), then a final merge
/// over those two (generation 4). Exercises max() over parents whose own
/// generations already differ from their count-based positions.
#[tokio::test]
async fn r_d2_2a_crossed_stamps_equal_brute_force_depth() -> Result<()> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = ctx.get::<RecordView>(rec_id).await?;

    // Level 2: concurrent siblings of genesis.
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();
    view.edit(&trx1)?.title().set(&"b".to_owned())?;
    view.edit(&trx2)?.artist().set(&"c".to_owned())?;
    trx1.commit().await?;
    trx2.commit().await?;
    assert_eq!(view.entity().head().len(), 2, "precondition: {{B, C}} antichain");

    // Level 3, crossed: two concurrent merges both parented on {B, C}.
    let trx3 = ctx.begin();
    let trx4 = ctx.begin();
    view.edit(&trx3)?.title().set(&"d".to_owned())?;
    view.edit(&trx4)?.artist().set(&"e".to_owned())?;
    trx3.commit().await?;
    trx4.commit().await?;
    assert_eq!(view.entity().head().len(), 2, "precondition: {{D, E}} antichain");

    // Level 4: the final merge over the crossed pair.
    let trx = ctx.begin();
    view.edit(&trx)?.title().set(&"f".to_owned())?;
    trx.commit().await?;

    let (count, max_depth) = assert_stamps_equal_depth(&ctx, rec_id, "crossed").await?;
    assert_eq!(count, 6, "genesis, two siblings, two crossed merges, one final merge");
    assert_eq!(max_depth, 4, "the final merge is four levels deep");
    Ok(())
}
