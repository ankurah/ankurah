mod common;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;

#[tokio::test]
async fn test_system() -> Result<()> {
    
    let engine = Arc::new(SledStorageEngine::new_test().unwrap());
    {
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
        eprintln!("Started node");

        node.system.initialize().await?;
        eprintln!("Created system");

        let root = node.system.root();
        eprintln!("System Root: {:?}", root);
        let items = node.system.items();
        eprintln!("System Items: {:?}", items);
    }

    {
        let node = Node::new_durable(engine, PermissiveAgent::new());
        eprintln!("Started node");

        // assert that this fails because the system already exists
        assert!(node.system.initialize().await.is_err());
        eprintln!("Failed Successfully - system already exists");

        let root = node.system.root();
        eprintln!("System Root: {:?}", root);
        let items = node.system.items();
        eprintln!("System Items: {:?}", items);
    }
    Ok(())
}
