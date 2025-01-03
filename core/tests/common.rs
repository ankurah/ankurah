use std::sync::{Arc, Mutex};

use tracing::Level;

use ankurah_core::{
    changes::{ChangeSet, RecordChange, RecordChangeKind},
    property::value::YrsString,
};
use ankurah_derive::Model;
use serde::{Deserialize, Serialize};
use std::sync::mpsc;

#[derive(Debug, Clone, Model)]
pub struct Pet {
    #[active_value(YrsString)]
    pub name: String,
    #[active_value(YrsString)]
    pub age: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
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

pub fn changeset_watcher() -> (
    Box<dyn Fn(ChangeSet) + Send + Sync>,
    Box<dyn Fn() -> Vec<RecordChangeKind>>,
) {
    let (tx, rx) = mpsc::channel();
    let watcher = Box::new(move |changeset: ChangeSet| {
        tx.send(changeset).unwrap();
    });

    let check = Box::new(move || {
        match rx.try_recv() {
            Ok(changeset) => changeset.changes.iter().map(|c| c.kind()).collect(),
            Err(_) => vec![], // Return empty vec instead of panicking
        }
    });

    (watcher, check)
}
