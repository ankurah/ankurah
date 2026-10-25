//! A resident builds its digest tree the first time anything reads a digest,
//! and maintains it on every commit after.
use super::*;
use crate::node::state_sync::tree::QueryTree;

/// The bytes the LRU charges each resident it holds, in LRU order.
fn charges<CD: ContextData>(cache: &ResidentTrees<CD>) -> Vec<usize> {
    cache.lru.lock().unwrap().slots.iter().map(|slot| slot.bytes).collect()
}

/// The bytes the LRU charges the residents admitting `selection`'s predicate.
fn charges_for<CD: ContextData>(cache: &ResidentTrees<CD>, selection: &Selection<Resolved>) -> Vec<usize> {
    cache
        .lru
        .lock()
        .unwrap()
        .slots
        .iter()
        .filter(|slot| slot.identity.selection.predicate == selection.predicate)
        .map(|slot| slot.bytes)
        .collect()
}

/// The digest over every address of a tree freshly built from `snapshot`'s entries.
async fn fresh_digest(snapshot: &Snapshot) -> anyhow::Result<proto::TreeSpanDigest> {
    let tree = QueryTree::new(snapshot.spec.clone(), (*snapshot.entries).clone());
    tree.snapshot().span_digest(proto::TreeSpan::all()).await
}

async fn node_with(values: &[&str]) -> anyhow::Result<(TestNode, Vec<proto::Event>)> {
    let node = node().await?;
    let mut events = Vec::new();
    for value in values {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    Ok((node, events))
}

#[tokio::test]
async fn an_inventory_sized_offer_builds_no_digest_tree() -> anyhow::Result<()> {
    let (node, _) = node_with(&["01", "02", "03"]).await?;
    let selection = selection(None);
    let lease = node.resident_trees.acquire(&node, selection.clone(), None, vec![DEFAULT_CONTEXT]).await?;
    let offer = lease.snapshot().await?.snapshot.offer().await?;
    assert!(matches!(offer.spans.as_slice(), [proto::SpanContent::Heads(_)]), "three heads fit one inventory");
    // Answering a request follows charging for what the read before it built.
    lease.snapshot().await?;
    let offered = charges_for(&node.resident_trees, &selection);
    lease.snapshot().await?.snapshot.span_digest(proto::TreeSpan::all()).await?;
    lease.snapshot().await?;
    let read = charges_for(&node.resident_trees, &selection);
    assert_eq!(read.len(), 1);
    assert!(read[0] > offered[0], "the offer read no digest, so this first digest read built the tree");
    Ok(())
}

#[tokio::test]
async fn the_first_digest_read_builds_the_resident_tree_once_and_it_matches_a_fresh_build() -> anyhow::Result<()> {
    let (node, events) = node_with(&["01", "02", "03"]).await?;
    let lease = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    let admitted = charges(&node.resident_trees);
    let mut first = lease.snapshot().await?.snapshot;
    let digest = first.span_digest(proto::TreeSpan::all()).await?;
    assert_eq!(digest, fresh_digest(&first).await?);
    let mut second = lease.snapshot().await?.snapshot;
    let built = charges(&node.resident_trees);
    assert!(built[0] > admitted[0], "the first digest read built the tree");
    assert_eq!(second.span_digest(proto::TreeSpan::all()).await?, digest);
    lease.snapshot().await?;
    assert_eq!(charges(&node.resident_trees), built, "a later read reuses the tree the first read built");

    apply(&node, &update(&events[0], "09")?).await?;
    let mut current = lease.snapshot().await?.snapshot;
    assert_ne!(current.span_digest(proto::TreeSpan::all()).await?.digest, digest.digest);
    assert_eq!(current.span_digest(proto::TreeSpan::all()).await?, fresh_digest(&current).await?, "the built tree is maintained");
    Ok(())
}

#[tokio::test]
async fn a_snapshot_taken_before_the_resident_advances_answers_digests_of_its_own_entries() -> anyhow::Result<()> {
    let (node, events) = node_with(&["01", "02", "03"]).await?;
    let lease = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    let mut before = lease.snapshot().await?.snapshot;
    apply(&node, &update(&events[0], "09")?).await?;
    let mut after = lease.snapshot().await?.snapshot;
    // The resident builds its tree at the newer entries first; the older
    // snapshot must not read that tree.
    let current = after.span_digest(proto::TreeSpan::all()).await?;
    let frozen = before.span_digest(proto::TreeSpan::all()).await?;
    assert_eq!(frozen, fresh_digest(&before).await?);
    assert_ne!(frozen.digest, current.digest);
    assert_eq!(before.entries[&events[0].entity_id].head, proto::Clock::singleton(&events[0]));
    Ok(())
}

#[tokio::test]
async fn the_lru_charges_a_resident_for_its_digest_tree_only_once_it_is_built() -> anyhow::Result<()> {
    let (node, _) = node_with(&["01", "02", "03"]).await?;
    let lease = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    let admitted = charges(&node.resident_trees);
    lease.snapshot().await?.snapshot.span_digest(proto::TreeSpan::all()).await?;
    lease.snapshot().await?;
    let built = charges(&node.resident_trees);
    assert!(built[0] > admitted[0], "admission builds no digest tree and is charged for none");
    Ok(())
}
