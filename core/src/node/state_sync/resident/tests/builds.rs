//! Resident builds: an unrelated build never blocks another, and a compatible one is shared.
use super::*;

#[tokio::test]
async fn an_unindexed_limit_builds_independently_without_blocking_the_shared_tree() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let second = genesis("02")?;
    apply(&node, &second).await?;
    let broad = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    let tree = broad.identity();
    let by_id = broad.snapshot().await?.snapshot.spec.clone();
    assert!(by_id.key_spec().keyparts.is_empty());

    // ORDER BY is not represented in this tree's entity-ID keys, so the
    // limited selection needs its own bounded resident and storage read.
    let (entered, release) = hold_fetch(&node);
    let pending = node.resident_trees.acquire(&node, selection(Some(1)), Some(by_id), vec![DEFAULT_CONTEXT]);
    tokio::pin!(pending);
    assert!(futures::poll!(&mut pending).is_pending());
    entered.await?;
    let changed = update(&initial, "09")?;
    apply(&node, &changed).await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if broad.snapshot().await?.snapshot.entries[&initial.entity_id].head == proto::Clock::singleton(&changed) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    release.send(()).unwrap();
    let limited = pending.await?;
    assert!(!Weak::ptr_eq(&tree, &limited.identity()), "unindexed ordering cannot borrow the broad resident");
    let after = limited.snapshot().await?;
    assert_eq!(after.snapshot.entries.keys().copied().collect::<Vec<_>>(), vec![second.entity_id]);

    let current = broad.snapshot().await?;
    assert_eq!(current.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&changed));
    assert_eq!(node.resident_trees.len(), 2);
    Ok(())
}

#[tokio::test]
async fn an_unrelated_miss_builds_while_another_build_waits_on_storage() -> anyhow::Result<()> {
    let node = node().await?;
    apply(&node, &genesis("01")?).await?;
    let (entered, release) = hold_fetch(&node);
    let first = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(first);
    assert!(futures::poll!(&mut first).is_pending());
    entered.await?;
    let other = Selection::from(Predicate::MemberOf(proto::ModelId::EntityId(proto::EntityId::from_bytes([25; 32]))));
    let unrelated =
        tokio::time::timeout(std::time::Duration::from_secs(2), node.resident_trees.acquire(&node, other, None, vec![DEFAULT_CONTEXT]))
            .await
            .expect("an unrelated miss does not wait for another build's storage read")?;
    assert!(unrelated.snapshot().await?.snapshot.entries.is_empty());
    release.send(()).unwrap();
    assert_eq!(first.await?.snapshot().await?.snapshot.entries.len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_compatible_miss_waits_for_the_build_in_progress_and_shares_it() -> anyhow::Result<()> {
    let node = node().await?;
    apply(&node, &genesis("01")?).await?;
    let fetches = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let first = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(first);
    assert!(futures::poll!(&mut first).is_pending());
    entered.await?;
    let second = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]);
    tokio::pin!(second);
    assert!(futures::poll!(&mut second).is_pending(), "a compatible miss waits for the build in progress");
    release.send(()).unwrap();
    let (first, second) = (first.await?, second.await?);
    assert!(Weak::ptr_eq(&first.identity(), &second.identity()));
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed) - fetches, 1, "the waiting miss reads storage no further");
    Ok(())
}
