//! The keypair a node looks up in its storage engine as it initializes.

use super::*;
use crate::node::key_lookup::{keypair_lock, keypair_map, KEYPAIR};
use crate::storage::{MapWrite, StorageEngine, StorageTransaction};

#[tokio::test]
async fn a_node_takes_the_keypair_its_storage_engine_keeps() {
    let storage = Arc::new(TestStorage::default());
    let first = Node::new(storage.clone(), PermissiveAgent::new()).wait_id().await.unwrap();
    assert_eq!(Node::new_durable(storage, PermissiveAgent::new()).wait_id().await.unwrap(), first);
}

#[tokio::test]
async fn a_node_that_starts_while_another_writes_the_first_keypair_waits_for_it_and_takes_it() {
    let storage = Arc::new(TestStorage::default());
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *storage.hold_map_write.lock().unwrap() = Some((entered, release_rx));
    let first = Node::new(storage.clone(), PermissiveAgent::new());
    entered_rx.await.unwrap();

    let second = Node::new(storage.clone(), PermissiveAgent::new());
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(second.id(), None, "the second lookup waits for the first one's lock");

    release.send(()).unwrap();
    assert_eq!(first.wait_id().await.unwrap(), second.wait_id().await.unwrap());
}

#[tokio::test]
async fn a_lookup_whose_commit_meets_a_write_conflict_reads_again_and_stores_one_keypair() {
    let storage = Arc::new(TestStorage::default());
    storage.record_map_reads();
    storage.conflict_next_commit();
    let id = Node::new(storage.clone(), PermissiveAgent::new()).wait_id().await.unwrap();

    let keypair_reads = storage.recorded_map_reads().into_iter().filter(|read| *read.map() == keypair_map()).count();
    assert_eq!(keypair_reads, 2, "the lookup reads the keypair again after the conflict");
    assert_eq!(Node::new(storage, PermissiveAgent::new()).wait_id().await.unwrap(), id);
}

#[tokio::test]
async fn a_node_stays_unkeyed_while_another_transaction_holds_the_keypair_lock_and_its_presence_waits() {
    let storage = Arc::new(TestStorage::default());
    let mut holder = storage.transaction();
    holder.map_lock_and_read(&[keypair_lock()], vec![]).await.unwrap();
    let node = Node::new(storage.clone(), PermissiveAgent::new());

    let challenge = proto::Challenge::random();
    let presence = node.presence(Dialing, &challenge);
    tokio::pin!(presence);
    assert!(futures::poll!(&mut presence).is_pending(), "the presence waits for the keypair");
    assert_eq!(node.id(), None);

    drop(holder);
    let presence = tokio::time::timeout(std::time::Duration::from_secs(2), presence).await.unwrap().unwrap().presence;
    assert_eq!(node.id(), Some(presence.node_id()));
}

#[tokio::test]
async fn a_stored_entry_that_is_not_a_node_key_halts_the_node() {
    let storage = Arc::new(TestStorage::default());
    let mut trx = storage.transaction();
    trx.map_write(vec![MapWrite::Put { map: keypair_map(), key: KEYPAIR.to_vec(), value: b"not a node key".to_vec() }]).await.unwrap();
    trx.commit().await.unwrap();

    let node = Node::new(storage, PermissiveAgent::new());
    assert!(matches!(node.wait_id().await, Err(NodeHaltReason::KeypairLookup(_))));
    assert!(matches!(node.state().value(), NodeState::Halted(NodeHaltReason::KeypairLookup(_))));
}

#[tokio::test]
async fn a_storage_wipe_takes_the_keypair_and_the_next_node_generates_a_new_one() {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new(storage.clone(), PermissiveAgent::new());
    let wiped = node.wait_id().await.unwrap();
    node.reset().await.unwrap();
    assert_ne!(Node::new(storage, PermissiveAgent::new()).wait_id().await.unwrap(), wiped);
}
