use tracing::Level;

use ankurah_core::{
    changes::{ChangeSet, ItemChange},
    model::View,
    property::value::YrsString,
};
use ankurah_derive::Model;
use serde::{Deserialize, Serialize};
use std::sync::mpsc;

use ankurah_proto as proto;

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
fn init_tracing() { tracing_subscriber::fmt().with_max_level(Level::INFO).with_test_writer().init(); }

#[derive(Debug, Clone, PartialEq)]
pub enum ChangeKind {
    Initial,
    Add,
    Remove,
    Update,
}

impl<R> From<&ItemChange<R>> for ChangeKind {
    fn from(change: &ItemChange<R>) -> Self {
        match change {
            ItemChange::Initial { .. } => ChangeKind::Initial,
            ItemChange::Add { .. } => ChangeKind::Add,
            ItemChange::Remove { .. } => ChangeKind::Remove,
            ItemChange::Update { .. } => ChangeKind::Update,
        }
    }
}

#[allow(unused)]
pub fn changeset_watcher<R: View + Send + Sync + 'static>(
) -> (Box<dyn Fn(ChangeSet<R>) + Send + Sync>, Box<dyn Fn() -> Vec<(proto::ID, ChangeKind)>>) {
    let (tx, rx) = mpsc::channel();
    let watcher = Box::new(move |changeset: ChangeSet<R>| {
        tx.send(changeset).unwrap();
    });

    let check = Box::new(move || {
        match rx.try_recv() {
            Ok(changeset) => changeset.changes.iter().map(|c| (c.entity().id(), c.into())).collect(),
            Err(_) => vec![], // Return empty vec instead of panicking
        }
    });

    (watcher, check)
}
