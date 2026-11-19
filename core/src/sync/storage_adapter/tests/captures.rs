//! Expiration never turns a captured version into a newer or partial version.

use std::ops::Bound;
use std::sync::{atomic::AtomicU64, Arc};

use ankurah_digest_tree::{DigestGroup, DigestTree, Encode, ReadTrx, RowKey, Store};

use super::Tree;
use crate::storage::{MapRead, MapRows, MapSnapshot, MapWrite, ScanDirection, StateCursor, StorageEngine, StorageTransaction};
use crate::sync::conformance::fixtures::{assert_holds, by_number, commit, created, item, rows_now, stores};
use crate::sync::conformance::in_both_groups;
use crate::sync::storage_adapter::layout::history;
use crate::sync::{TreeChange, TreeId};
use crate::test_utils::TestStorage;

async fn capture<G: DigestGroup>(tree: &Tree<G>) -> anyhow::Result<Tree<G>> {
    tree.store().load_buffered(Arc::new(AtomicU64::new(0))).await?;
    tree.store().advance(StateCursor::new(vec![0]), 0, 0);
    Ok(DigestTree::open(tree.store().capture()).await?)
}

async fn assert_expired<G: DigestGroup>(tree: &Tree<G>) {
    let read = tree.store().begin_read().await;
    assert!(matches!(read, Err(error) if error.is_retryable()), "an unavailable capture must fail before answering any row");
}

async fn capture_ranges_restore_deleted_and_overwritten_rows_in_both_directions<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", (0..100).map(|id| item!(id, id as i64))).await?;
    let before = rows_now(&engine, &tree).await?;
    let frozen = capture(&tree).await?;
    let writer: Tree<G> = stores.open(&TreeId::new("tree")).await?;
    commit(&stores, &engine, &writer, |changes| {
        for id in 0..100 {
            if id % 3 == 0 {
                changes.remove(item!(id, id as i64))?;
            } else {
                changes.update(item!(id, id as i64), item!(id, 200 - id as i64, "changed"))?;
            }
        }
        changes.insert(item!(1000, -1))
    })
    .await?;
    let keys = before.keys().map(|key| RowKey::decode(key)).collect::<Result<Vec<_>, _>>()?;
    let mut read = frozen.store().begin_read().await?;
    for (key, row) in keys.iter().zip(read.get_many(&keys).await?) {
        assert_eq!(row.map(|row| row.encode()), before.get(&key.encode()).cloned());
    }
    // Include a bounded range with a short limit: newer rows must neither take
    // the old rows' slots nor hide deleted rows beyond the first storage batch.
    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        for limit in [1, 7, usize::MAX] {
            let start = &keys[3];
            let end = &keys[keys.len() - 2];
            let actual = read.range(Bound::Excluded(start), Bound::Included(end), direction, limit).await?;
            let mut expected: Vec<_> = before.range((Bound::Excluded(start.encode()), Bound::Included(end.encode()))).collect();
            if direction == ScanDirection::Reverse {
                expected.reverse();
            }
            expected.truncate(limit.min(expected.len()));
            assert_eq!(
                actual.into_iter().map(|(key, row)| (key.encode(), row.encode())).collect::<Vec<_>>(),
                expected.into_iter().map(|(key, row)| (key.clone(), row.clone())).collect::<Vec<_>>()
            );
        }
    }
    Ok(())
}

async fn a_rolled_back_history_write_preserves_the_capture_and_the_next_commit<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
    let frozen = capture(&tree).await?;
    let writer: Tree<G> = stores.open(&TreeId::new("tree")).await?;
    let mut changes = writer.transaction();
    changes.remove(item!(1, 1))?;
    let mut transaction = engine.transaction();
    let applied = stores.apply(&mut transaction, vec![TreeChange::Changes(changes)]).await?;
    drop(transaction);
    drop(applied);
    assert_holds(&frozen, &[item!(1, 1)]).await?;
    commit(&stores, &engine, &writer, |changes| changes.insert(item!(2, 2))).await?;
    assert_holds(&frozen, &[item!(1, 1)]).await?;
    assert_holds(&writer, &[item!(1, 1), item!(2, 2)]).await
}

async fn the_history_window_expires_without_affecting_new_captures<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let stores = stores(&engine);
    let tree: Tree<G> = created(&stores, &engine, "tree", [item!(0, 0)]).await?;
    let frozen = capture(&tree).await?;
    let writer: Tree<G> = stores.open(&TreeId::new("tree")).await?;
    for id in 1..=history::GENERATIONS as u64 {
        commit(&stores, &engine, &writer, |changes| changes.update(item!(0, (id - 1) as i64), item!(0, id as i64))).await?;
    }
    assert_holds(&frozen, &[item!(0, 0)]).await?;
    let recent = capture(&writer).await?;
    let durable: Tree<G> = stores.open(&TreeId::new("tree")).await?;
    commit(&stores, &engine, &durable, |changes| changes.update(item!(0, 64), item!(0, 65))).await?;
    assert_expired(&frozen).await;
    assert_holds(&recent, &[item!(0, 64)]).await?;
    assert_holds(&durable, &[item!(0, 65)]).await
}

async fn replacement_clear_drop_and_reset_expire_captures<G: DigestGroup>() -> anyhow::Result<()> {
    for operation in ["rebuild", "clear", "drop", "reset"] {
        let engine = Arc::new(TestStorage::default());
        let stores = stores(&engine);
        let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
        let frozen = capture(&tree).await?;
        match operation {
            "rebuild" => {
                let mut transaction = engine.transaction();
                let rebuilt = stores.rebuild(&mut transaction, TreeId::new("tree"), by_number(), [item!(2, 2)]).await?;
                transaction.commit().await?.committed()?;
                let _: Tree<G> = rebuilt.landed();
            }
            "clear" => {
                tree.store().clear_buffered();
                let transaction = tree.store().buffered_transaction().await?;
                DigestTree::create_in(tree.store().clone(), transaction, by_number(), [item!(2, 2)]).await?.landed();
                tree.store().advance(StateCursor::new(vec![1]), 0, 1);
                tree.store().flush().await?;
            }
            "drop" => {
                let map = tree.store().buffered().base.incarnation.history();
                tree.store().flush().await?;
                let mut transaction = engine.transaction();
                stores.drop(&mut transaction, &TreeId::new("tree")).await?;
                transaction.commit().await?.committed()?;
                let [MapRows::Ranged(history)]: [_; 1] = engine
                    .map_snapshot(&[])
                    .await?
                    .map_read(vec![MapRead::Range {
                        map,
                        start: Bound::Unbounded,
                        end: Bound::Unbounded,
                        direction: ScanDirection::Forward,
                        limit: usize::MAX,
                    }])
                    .await?
                    .try_into()
                    .unwrap()
                else {
                    panic!("history range")
                };
                assert!(history.is_empty(), "dropping a tree releases its history");
                let _: Tree<G> = created(&stores, &engine, "tree", [item!(2, 2)]).await?;
            }
            "reset" => {
                engine.delete_all().await?;
            }
            _ => unreachable!(),
        }
        assert_expired(&frozen).await;
    }
    Ok(())
}

async fn missing_and_malformed_history_restart_only_the_capture<G: DigestGroup>() -> anyhow::Result<()> {
    for replacement in [None, Some(vec![]), Some(vec![255]), Some(vec![0, 0, 0, 0, 5, 1])] {
        let engine = Arc::new(TestStorage::default());
        let stores = stores(&engine);
        let tree: Tree<G> = created(&stores, &engine, "tree", [item!(1, 1)]).await?;
        let frozen = capture(&tree).await?;
        let writer: Tree<G> = stores.open(&TreeId::new("tree")).await?;
        commit(&stores, &engine, &writer, |changes| changes.insert(item!(2, 2))).await?;
        commit(&stores, &engine, &writer, |changes| changes.insert(item!(3, 3))).await?;
        let map = tree.store().buffered().base.incarnation.history();
        let key = 1u64.to_be_bytes().to_vec();
        let write = match replacement {
            Some(value) => MapWrite::Put { map, key, value },
            None => MapWrite::Delete { map, key },
        };
        let mut transaction = engine.transaction();
        transaction.map_write(vec![write]).await?;
        transaction.commit().await?.committed()?;
        assert_expired(&frozen).await;
        assert_holds(&writer, &[item!(1, 1), item!(2, 2), item!(3, 3)]).await?;
    }
    Ok(())
}

in_both_groups!(
    #[tokio::test]
    capture_ranges_restore_deleted_and_overwritten_rows_in_both_directions,
    a_rolled_back_history_write_preserves_the_capture_and_the_next_commit,
    the_history_window_expires_without_affecting_new_captures,
    replacement_clear_drop_and_reset_expire_captures,
    missing_and_malformed_history_restart_only_the_capture,
);
