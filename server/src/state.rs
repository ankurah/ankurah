use ankurah_core::node::Node;
use std::sync::Arc;

#[derive(Clone)]
pub struct ServerState {
    #[allow(unused)]
    node: Arc<Node>,
}

impl ServerState {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    #[allow(unused)]
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }
}
