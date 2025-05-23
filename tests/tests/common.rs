use tracing::Level;

use ankurah::{
    changes::{ChangeKind, ChangeSet},
    model::View,
    // property::{value::LWW, YrsString},
    proto,
    Model,
};
use serde::{Deserialize, Serialize};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Model, Serialize, Deserialize)]
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
fn init_tracing() {
    // if LOG_LEVEL env var is set, use it
    if let Ok(level) = std::env::var("LOG_LEVEL") {
        tracing_subscriber::fmt().with_max_level(Level::from_str(&level).unwrap()).with_test_writer().init();
    } else {
        tracing_subscriber::fmt().with_max_level(Level::INFO).with_test_writer().init();
    }
}

#[allow(unused)]
pub fn changeset_watcher<R: View + Send + Sync + 'static>(
) -> (Box<dyn Fn(ChangeSet<R>) + Send + Sync>, Box<dyn Fn() -> Vec<Vec<(proto::EntityId, ChangeKind)>> + Send + Sync>) {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let watcher = {
        let changes = changes.clone();
        Box::new(move |changeset: ChangeSet<R>| {
            changes.lock().unwrap().push(changeset);
        })
    };

    let check = Box::new(move || {
        let changes: Vec<Vec<(proto::EntityId, ChangeKind)>> =
            changes.lock().unwrap().drain(..).map(|c| c.changes.iter().map(|c| (c.entity().id(), c.into())).collect()).collect();
        changes
    });

    (watcher, check)
}

/// A generic watcher that allows extracting arbitrary data from the ChangeSet via a user-provided closure over ItemChange.
#[allow(unused)]
pub fn watcher<R, T, F>(extract: F) -> (Box<dyn Fn(ChangeSet<R>) + Send + Sync>, Box<dyn Fn() -> Vec<Vec<T>> + Send + Sync>)
where
    R: View + Send + Sync + 'static,
    T: Send + 'static,
    F: Fn(ankurah::changes::ItemChange<R>) -> T + Send + Sync + 'static,
{
    let changes = Arc::new(Mutex::new(Vec::new()));
    let watcher = {
        let changes = changes.clone();
        Box::new(move |changeset: ChangeSet<R>| {
            changes.lock().unwrap().push(changeset);
        })
    };

    let check = Box::new(move || {
        let changes: Vec<Vec<T>> =
            changes.lock().unwrap().drain(..).map(|mut cset| cset.changes.into_iter().map(|c| extract(c)).collect()).collect();
        changes
    });

    (watcher, check)
}
