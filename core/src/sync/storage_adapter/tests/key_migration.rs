use super::*;
use crate::storage::{MapRead, MapRows, MapSnapshot, MapWrite};
use crate::sync::{CacheSettings, TreeManager};
use ankql::ast::{Predicate, Selection};
use ankurah_digest_tree::{Encode, ReadTrx, RowKey, Store};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, State, StateBuffers};

#[tokio::test]
async fn a_manager_rebuilds_legacy_keys_but_refuses_future_encodings() -> anyhow::Result<()> {
    for legacy in [true, false] {
        let storage = Arc::new(TestStorage::default());
        let manager =
            TreeManager::<_, Ristretto255>::new(storage.clone(), Arc::new(tokio::sync::Mutex::new(())), CacheSettings { bytes: 4096 }, 0);
        let state = |byte| {
            Attested::opt(
                EntityState {
                    entity_id: EntityId::from_bytes([byte; 32]),
                    state: State {
                        head: Clock::default(),
                        state_buffers: StateBuffers(Default::default()),
                        memberships: Default::default(),
                    },
                },
                None,
            )
        };
        storage.set_state(state(1));
        let selection = Selection { predicate: Predicate::True, order_by: None, limit: None };
        let tree = manager.open(selection.clone()).await?;
        let id = tree.store().id().clone();
        let inner = tree.store().inner();
        let rows = inner.rows.clone();
        let lock = inner.lock.clone();
        let mut read = tree.store().begin_read().await?;
        let mut metadata = read.get(&RowKey::Meta).await?.unwrap().encode();
        drop(read);
        if legacy {
            metadata[0] = 0;
            metadata.remove(2); // The legacy metadata tag has no key-codec byte.
        } else {
            metadata[2] = u8::MAX;
        }
        drop(tree);
        drop(manager);
        let mut trx = storage.transaction();
        trx.map_lock_and_read(&[lock.clone()], vec![]).await?;
        trx.map_write(vec![MapWrite::Put { map: rows.clone(), key: RowKey::Meta.encode(), value: metadata.clone() }]).await?;
        trx.commit().await?.committed()?;
        storage.set_state(state(2));

        let manager =
            TreeManager::<_, Ristretto255>::new(storage.clone(), Arc::new(tokio::sync::Mutex::new(())), CacheSettings { bytes: 4096 }, 0);
        let reopened = manager.open(selection).await;
        if legacy {
            let tree = reopened?;
            assert_eq!(tree.store().id(), &id);
            assert_eq!(tree.view().count(..).await?, 2);
            assert_eq!(tree.audit().await?.members, 2);
        } else {
            assert!(reopened.is_err(), "a newer codec must not be downgraded");
            let mut snapshot = storage.map_snapshot(&[lock]).await?;
            let values = snapshot.map_read(vec![MapRead::Get { map: rows, keys: vec![RowKey::Meta.encode()] }]).await?;
            assert!(matches!(&values[..], [MapRows::Got(rows)] if rows == &[Some(metadata)]));
        }
    }
    Ok(())
}
