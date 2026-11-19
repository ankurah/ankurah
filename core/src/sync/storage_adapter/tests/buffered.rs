//! Buffered tree versions retain the durable cache's generation boundary.

use std::sync::{atomic::AtomicU64, Arc};

use ankurah_digest_tree::{DigestGroup, DigestTree, Encode, ReadTrx, RowKey, Span, Store, StoreTrx, WriteItem};

use super::Tree;
use crate::storage::{MapRead, MapRows, MapSnapshot, MapWrite, StateCursor, StorageEngine, StorageTransaction};
use crate::sync::conformance::fixtures::{assert_holds, by_number, cached, created, item, rows_now, stores};
use crate::sync::conformance::in_both_groups;
use crate::sync::storage_adapter::layout::{entry::Status, log};
use crate::sync::TreeId;
use crate::test_utils::TestStorage;

async fn managed<G: DigestGroup>(tree: &Tree<G>) -> anyhow::Result<()> {
    tree.store().load_buffered(Arc::new(AtomicU64::new(0))).await?;
    tree.store().advance(StateCursor::new(vec![0]), 0, 0);
    Ok(())
}

fn node_reads<G: DigestGroup>(engine: &TestStorage, tree: &Tree<G>) -> usize {
    let rows = tree.store().buffered().base.incarnation.rows();
    engine
        .recorded_map_reads()
        .iter()
        .filter(|read| match read {
            MapRead::Get { map, keys } => map == &rows && keys.iter().any(|key| cached(key)),
            _ => false,
        })
        .count()
}

async fn buffered_reads_use_the_base_cache_without_publishing_pending_rows<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", (0..300).map(|id| item!(id, id as i64))).await?;
    let durable: Tree<G> = stores.open(&TreeId::new("tree")).await?;
    managed(&tree).await?;

    engine.record_map_reads();
    assert_eq!(tree.view().count(..).await?, 300);
    assert_eq!(node_reads(&engine, &tree), 0, "a clean managed tree uses cached trie rows");

    let mut changes = tree.transaction();
    changes.insert(item!(1000, 150))?;
    changes.prepare().await.apply(tree.store().buffered_transaction().await?).await?.landed();
    engine.record_map_reads();
    assert_eq!(tree.view().count(..).await?, 301);
    assert_eq!(node_reads(&engine, &tree), 0, "pending rows overlay the cached durable version");
    assert_eq!(durable.view().count(..).await?, 300, "the durable cache never contains pending rows");

    tree.store().flush().await?;
    engine.record_map_reads();
    assert_eq!(tree.view().count(..).await?, 301);
    assert_eq!(node_reads(&engine, &tree), 0, "the flush advances the durable cache");
    assert_eq!(durable.view().count(..).await?, 301);

    let version = tree.store().buffered();
    let mut read = engine.map_snapshot(&[]).await?;
    let mut answer = read.map_read(vec![log::read_after(version.base.incarnation.log(), version.base.generation - 1)]).await?;
    let MapRows::Ranged(entries) = answer.remove(0) else { panic!("change log range") };
    let changed = log::changes(tree.store().id(), version.base.generation - 1, version.base.generation, entries).unwrap();
    assert!(!changed.is_empty());
    assert!(changed.iter().all(|(key, _)| cached(key)), "the cache log excludes leaf payloads");
    Ok(())
}

async fn deletes_clears_and_transaction_writes_override_cached_rows<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
    managed(&tree).await?;
    let meta = tree.store().begin_read().await?.get(&RowKey::Meta).await?.unwrap();
    let map = tree.store().buffered().base.incarnation.rows();

    let mut version = (*tree.store().buffered()).clone();
    version.writes.add(vec![MapWrite::Delete { map, key: RowKey::Meta.encode() }]);
    tree.store().publish(Arc::new(version));
    assert!(tree.store().begin_read().await?.get(&RowKey::Meta).await?.is_none());

    let mut transaction = tree.store().buffered_transaction().await?;
    transaction.write(vec![WriteItem::Put(RowKey::Meta, meta.clone())]).await?;
    assert_eq!(transaction.get(&RowKey::Meta).await?.unwrap().encode(), meta.encode());
    drop(transaction);
    assert!(tree.store().begin_read().await?.get(&RowKey::Meta).await?.is_none(), "unlanded transaction rows stay private");

    tree.store().clear_buffered();
    assert!(tree.store().begin_read().await?.get(&RowKey::Meta).await?.is_none());
    Ok(())
}

async fn a_clear_invalidates_other_readers_cached_rows<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let local = stores(&engine);
    let remote = stores(&engine);
    let tree: Tree<G> = created(&local, &engine, "tree", (0..300).map(|id| item!(id, id as i64))).await?;
    let observer: Tree<G> = remote.open(&TreeId::new("tree")).await?;
    let before = rows_now(&engine, &tree).await?;
    let keys = before.keys().filter(|key| cached(key)).map(|key| RowKey::decode(key)).collect::<Result<Vec<_>, _>>()?;
    observer.store().begin_read().await?.get_many(&keys).await?;
    managed(&tree).await?;

    tree.store().clear_buffered();
    let transaction = tree.store().buffered_transaction().await?;
    DigestTree::create_in(tree.store().clone(), transaction, by_number(), [item!(999, 999)]).await?.landed();
    tree.store().advance(StateCursor::new(vec![1]), 0, 1);
    tree.store().flush().await?;
    assert_holds(&observer, &[item!(999, 999)]).await?;

    let after = rows_now(&engine, &tree).await?;
    let actual = observer.store().begin_read().await?.get_many(&keys).await?;
    for (key, row) in keys.iter().zip(actual) {
        assert_eq!(row.map(|row| row.encode()), after.get(&key.encode()).cloned(), "cached key {key:?}");
    }
    let version = tree.store().buffered();
    let answer = engine
        .map_snapshot(&[])
        .await?
        .map_read(vec![MapRead::Get { map: version.base.incarnation.log(), keys: vec![log::key(version.base.generation)] }])
        .await?;
    assert_eq!(answer, vec![MapRows::Got(vec![Some(log::whole_tree())])]);
    Ok(())
}

async fn a_rebased_adapter_reads_the_new_incarnations_cache<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let local = stores(&engine);
    let tree: Tree<G> = created(&local, &engine, "tree", [item!(1, 1)]).await?;
    managed(&tree).await?;
    tree.view().span_digest(Span::all()).await?;
    let old = tree.store().buffered().base.incarnation;

    let mut transaction = engine.transaction();
    local.drop(&mut transaction, &TreeId::new("tree")).await?;
    transaction.commit().await?.committed()?;
    let replacement: Tree<G> = created(&local, &engine, "tree", [item!(2, 2), item!(3, 3)]).await?;
    managed(&tree).await?;
    assert_ne!(tree.store().buffered().base.incarnation, old);
    assert_holds(&tree, &[item!(2, 2), item!(3, 3)]).await?;
    assert_eq!(tree.view().span_digest(Span::all()).await?.digest, replacement.view().span_digest(Span::all()).await?.digest);
    Ok(())
}

async fn an_initial_build_stays_private_until_its_cursor_is_complete<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
    managed(&tree).await?;
    let store = tree.store().working();
    store.clear_buffered();
    let transaction = store.buffered_transaction().await?;
    let working: Tree<G> = DigestTree::create_in(store.clone(), transaction, by_number(), [item!(2, 2)]).await?.landed();
    store.flush().await?;
    assert_eq!(store.buffered().base.status, Status::Building);
    assert_holds(&working, &[item!(2, 2)]).await?;
    let opened: Result<Tree<G>, _> = stores.open(&TreeId::new("tree")).await;
    assert!(matches!(crate::sync::conformance::fixtures::cause(&opened), Some(crate::sync::Error::Building(_))));

    store.advance(StateCursor::new(vec![2]), 0, 1);
    store.flush().await?;
    assert_eq!(store.buffered().base.status, Status::Ready);
    let complete: Tree<G> = stores.open(&TreeId::new("tree")).await?;
    assert_holds(&complete, &[item!(2, 2)]).await?;
    Ok(())
}

async fn dropping_a_managed_tree_clears_its_member_lookup_and_cursor<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
    managed(&tree).await?;
    tree.store().set_member_rows(vec![(vec![1], Some(vec![2]))]);
    tree.store().flush().await?;
    let members = tree.store().buffered().base.incarnation.members();
    let read = MapRead::Get { map: members, keys: vec![Vec::new(), vec![1]] };
    let mut snapshot = engine.map_snapshot(&[]).await?;
    let [MapRows::Got(before)]: [_; 1] = snapshot.map_read(vec![read.clone()]).await?.try_into().unwrap() else { panic!("member lookup") };
    assert!(before.iter().all(Option::is_some));
    drop(snapshot);
    let mut trx = engine.transaction();
    crate::sync::storage_adapter::drop(&mut trx, tree.store().id()).await?;
    trx.commit().await?.committed()?;
    let mut snapshot = engine.map_snapshot(&[]).await?;
    let [MapRows::Got(after)]: [_; 1] = snapshot.map_read(vec![read]).await?.try_into().unwrap() else { panic!("member lookup") };
    assert!(after.iter().all(Option::is_none));
    Ok(())
}

async fn rebuilding_never_reuses_the_previous_members_or_cursor<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
    managed(&tree).await?;
    tree.store().set_member_rows(vec![(vec![1], Some(vec![2]))]);
    tree.store().flush().await?;
    let mut trx = engine.transaction();
    let replacement = crate::sync::storage_adapter::rebuild(
        engine.clone(),
        tree.store().inner().cache.clone(),
        &mut trx,
        TreeId::new("tree"),
        by_number(),
        [item!(2, 2)],
    )
    .await?;
    trx.commit().await?.committed()?;
    let replacement: Tree<G> = replacement.landed();
    replacement.store().load_buffered(Arc::new(AtomicU64::new(0))).await?;
    assert!(replacement.store().buffered().cursor.is_none(), "the replacement needs a complete source scan");
    let working = replacement.store().working();
    assert_eq!(working.member_rows(&[vec![1]]).await?, vec![None]);
    Ok(())
}

async fn a_stale_initializer_cannot_replace_ready_or_building_rows<G: DigestGroup>() -> anyhow::Result<()> {
    for building in [false, true] {
        let engine = Arc::new(TestStorage::default());
        let stores = stores(&engine);
        let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
        managed(&tree).await?;
        if building {
            tree.store().clear_buffered();
            let trx = tree.store().buffered_transaction().await?;
            DigestTree::create_in(tree.store().clone(), trx, by_number(), [item!(1, 1)]).await?.landed();
            tree.store().flush().await?;
        }
        let mut trx = engine.transaction();
        let result = crate::sync::storage_adapter::initialize::<_, _, _, _, G, _>(
            engine.clone(),
            tree.store().inner().cache.clone(),
            &mut trx,
            TreeId::new("tree"),
            by_number(),
            [item!(2, 2)],
        )
        .await;
        assert!(matches!(result, Err(crate::error::MutationError::WriteConflict)));
        drop(trx);
        assert_holds(&tree, &[item!(1, 1)]).await?;
    }
    Ok(())
}

in_both_groups!(
    #[tokio::test]
    buffered_reads_use_the_base_cache_without_publishing_pending_rows,
    deletes_clears_and_transaction_writes_override_cached_rows,
    a_clear_invalidates_other_readers_cached_rows,
    a_rebased_adapter_reads_the_new_incarnations_cache,
    an_initial_build_stays_private_until_its_cursor_is_complete,
    dropping_a_managed_tree_clears_its_member_lookup_and_cursor,
    rebuilding_never_reuses_the_previous_members_or_cursor,
    a_stale_initializer_cannot_replace_ready_or_building_rows,
);
