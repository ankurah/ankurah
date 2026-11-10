use std::sync::Arc;

use rand::{rngs::SmallRng, SeedableRng};

use super::peers::plugin::{DuplicatePluginName, NodePlugin, NodePlugins};
use super::{Node, NodeKey};
use crate::policy::PolicyAgent;
use crate::storage::StorageEngine;

/// A node's construction: whether it is durable, its key, and its plugins. Made by
/// [`Node::builder`].
pub struct NodeBuilder<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    engine: Arc<SE>,
    policy_agent: PA,
    durable: bool,
    key: Option<NodeKey>,
    plugins: Vec<Box<dyn NodePlugin<SE, PA>>>,
}

impl<SE, PA> NodeBuilder<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(super) fn new(engine: Arc<SE>, policy_agent: PA) -> Self {
        Self { engine, policy_agent, durable: false, key: None, plugins: Vec::new() }
    }

    /// Build a durable node rather than an ephemeral one.
    pub fn durable(mut self) -> Self {
        self.durable = true;
        self
    }

    /// Give the node the identity `key`, typically one the embedder persists; see
    /// [`NodeKey::load_or_create`]. A node given no key draws a fresh one.
    pub fn key(mut self, key: NodeKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Attach `plugin`. The node asks its plugins in the order plugged.
    pub fn plug(mut self, plugin: impl NodePlugin<SE, PA>) -> Self {
        self.plugins.push(Box::new(plugin));
        self
    }

    /// Start the node, unless two of its plugins share a name.
    pub fn build(self) -> Result<Node<SE, PA>, DuplicatePluginName> {
        let plugins = NodePlugins::new(self.plugins)?;
        Ok(Node::start(self.engine, self.policy_agent, self.durable, SmallRng::from_entropy(), self.key, plugins))
    }
}
