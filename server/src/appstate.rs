use std::{ops::Deref, sync::Arc};

use crate::storage;
use anyhow::Result;

#[derive(Clone)]
pub struct AppState(Arc<AppStateInner>);
pub struct AppStateInner {
    pub storage: storage::StorageEngine,
}

impl AppState {
    pub fn new() -> Result<Self> {
        let storage = storage::StorageEngine::new()?;
        Ok(Self(Arc::new(AppStateInner { storage })))
    }
}

impl Deref for AppState {
    type Target = Arc<AppStateInner>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
