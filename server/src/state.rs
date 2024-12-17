use ankurah_core::node::Node;
use std::sync::Arc;

#[derive(Clone)]
pub struct ServerState {
    node: Arc<Node>,
}

impl ServerState {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }
}
