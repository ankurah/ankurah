//! Factory traits for creating nodes and connectors in a topology-agnostic way.

use ankurah::Node;
use ankurah_core::policy::PolicyAgent;
use ankurah_core::storage::StorageEngine;
use anyhow::Result;

/// Factory for creating nodes with specific storage and policy types.
/// The same factory can be used for durable or ephemeral nodes depending on how it's configured.
pub trait NodeFactory<SE, PA>: Send + Sync
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Creates a node with the given index and durability flag.
    /// The index can be used to differentiate nodes in multi-node topologies.
    fn create_node(&self, index: usize, durable: bool) -> Result<Node<SE, PA>>;

    /// Returns a human-readable identifier for this factory (e.g., "postgres-bb8-pool-10", "sled-test").
    fn identifier(&self) -> String;
}

/// Factory for creating connectors between nodes.
pub trait ConnectorFactory<SE1, PA1, SE2, PA2>: Send + Sync
where
    SE1: StorageEngine + Send + Sync + 'static,
    PA1: PolicyAgent + Send + Sync + 'static,
    SE2: StorageEngine + Send + Sync + 'static,
    PA2: PolicyAgent + Send + Sync + 'static,
{
    /// Creates a connector between two nodes.
    /// Returns a handle that keeps the connection alive.
    fn create_connector(
        &self,
        from: &Node<SE1, PA1>,
        to: &Node<SE2, PA2>,
    ) -> impl std::future::Future<Output = Result<Box<dyn std::any::Any + Send>>> + Send;

    /// Returns a human-readable identifier for this factory (e.g., "local-process", "websocket").
    fn identifier(&self) -> String;
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah::PermissiveAgent;
    use ankurah_storage_sled::SledStorageEngine;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockNodeFactory {
        call_count: Arc<AtomicUsize>,
    }

    impl NodeFactory<SledStorageEngine, PermissiveAgent> for MockNodeFactory {
        fn create_node(&self, _index: usize, durable: bool) -> Result<Node<SledStorageEngine, PermissiveAgent>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let storage = SledStorageEngine::new_test()?;
            let node = if durable {
                Node::new_durable(Arc::new(storage), PermissiveAgent::new())
            } else {
                Node::new(Arc::new(storage), PermissiveAgent::new())
            };
            Ok(node)
        }

        fn identifier(&self) -> String { "mock-sled".to_string() }
    }

    #[tokio::test]
    async fn test_factory_invocation_counts() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let factory = MockNodeFactory { call_count: call_count.clone() };

        // Create 2 durable and 3 ephemeral nodes
        for i in 0..2 {
            factory.create_node(i, true).unwrap();
        }
        for i in 0..3 {
            factory.create_node(i, false).unwrap();
        }

        assert_eq!(call_count.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn test_factory_identifier() {
        let factory = MockNodeFactory { call_count: Arc::new(AtomicUsize::new(0)) };
        assert_eq!(factory.identifier(), "mock-sled");
    }
}
