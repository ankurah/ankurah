use ankurah_core::node::Node;
use ankurah_core::storage::SledStorageEngine;
use std::{ops::Deref, sync::Arc};

// use crate::storage;
use anyhow::Result;

#[derive(Clone)]
pub struct AppState(Arc<AppStateInner>);
pub struct AppStateInner {
    node: Arc<Node>,
}

impl AppState {
    pub fn new() -> Result<Self> {
        // let storage = storage::StorageEngine::new()?;
        Ok(Self(Arc::new(AppStateInner {
            node: Node::new(SledStorageEngine::new().unwrap()),
        })))
    }
}

impl Deref for AppState {
    type Target = Arc<AppStateInner>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
