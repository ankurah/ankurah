use tracing::Level;

use ankurah::{
    changes::{ChangeKind, ChangeSet},
    model::View,
    proto, Model,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Model)]
pub struct Pet {
    pub name: String,
    pub age: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

// Initialize tracing for tests
#[ctor::ctor]
fn init_tracing() { tracing_subscriber::fmt().with_max_level(Level::INFO).with_test_writer().init(); }

#[allow(unused)]
pub fn changeset_watcher<R: View + Send + Sync + 'static>(
) -> (Box<dyn Fn(ChangeSet<R>) + Send + Sync>, Box<dyn Fn() -> Vec<Vec<(proto::ID, ChangeKind)>> + Send + Sync>) {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let watcher = {
        let changes = changes.clone();
        Box::new(move |changeset: ChangeSet<R>| {
            changes.lock().unwrap().push(changeset);
        })
    };

    let check = Box::new(move || {
        let changes: Vec<Vec<(proto::ID, ChangeKind)>> =
            changes.lock().unwrap().drain(..).map(|c| c.changes.iter().map(|c| (c.entity().id(), c.into())).collect()).collect();
        changes
    });

    (watcher, check)
}
