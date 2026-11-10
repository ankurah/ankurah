use crate::node::{Node, NodeKey};
use crate::policy::PermissiveAgent;
use crate::test_utils::TestStorage;
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_durable_node_keeps_its_identity_across_a_restart_with_its_persisted_key() {
    let dir = std::env::temp_dir().join(format!("ankurah-node-identity-{}", NodeKey::generate().node_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("node.key");
    let storage = Arc::new(TestStorage::default());

    let keyed = || Node::builder(storage.clone(), PermissiveAgent::new()).durable().key(NodeKey::load_or_create(&path).unwrap()).build();
    let first = keyed().id;
    let restarted = keyed().id;
    assert_eq!(restarted, first);
    std::fs::remove_dir_all(&dir).unwrap();

    let unkeyed = Node::new_durable(storage, PermissiveAgent::new()).id;
    assert_ne!(unkeyed, first, "a durable node given no key draws a fresh identity");
}

#[tokio::test]
async fn an_ephemeral_node_draws_a_fresh_identity_each_time() {
    let storage = Arc::new(TestStorage::default());
    let first = Node::new(storage.clone(), PermissiveAgent::new()).id;
    assert_ne!(Node::new(storage, PermissiveAgent::new()).id, first);
}
