use std::sync::{Arc, Mutex};

use tracing::Level;

use ankurah_core::{
    changes::{ChangeSet, RecordChangeKind},
    property::value::YrsString,
};
use ankurah_derive::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    #[active_value(YrsString)]
    pub name: String,
    #[active_value(YrsString)]
    pub year: String,
}

// Initialize tracing for tests
#[ctor::ctor]
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_test_writer()
        .init();
}

pub fn changeset_watcher() -> (impl Fn(ChangeSet), impl Fn() -> Vec<RecordChangeKind>) {
    // Track received changesets
    let received = Arc::new(Mutex::new(Vec::new()));

    // Helper function to check received changesets
    let check_received = {
        let received = received.clone();
        move || {
            let mut changesets = received.lock().unwrap();
            let result: Vec<RecordChangeKind> = (*changesets)
                .iter()
                .flat_map(|c: &ChangeSet| c.changes.iter().map(|change| change.kind.clone()))
                .collect();
            changesets.clear();
            result
        }
    };

    let watcher = move |changeset| {
        let mut received = received.lock().unwrap();
        received.push(changeset);
    };

    (watcher, check_received)
}
