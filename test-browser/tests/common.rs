#![cfg(target_arch = "wasm32")]

use ankurah::{
    changes::{ChangeKind, ChangeSet},
    model::View,
    proto, Model,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;
use wasm_bindgen::prelude::*;

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

use tracing_subscriber::fmt::format::Pretty;
use tracing_subscriber::prelude::*;
use tracing_web::{performance_layer, MakeWebConsoleWriter};

// #[wasm_bindgen(start)]
// fn init_tracing() {
//     let fmt_layer = tracing_subscriber::fmt::layer()
//         .with_ansi(true) // Only partially supported across browsers
//         .without_time() // std::time is not available in browsers, see note below
//         .with_writer(MakeWebConsoleWriter::new()); // write events to the console
//     let perf_layer = performance_layer().with_details_from_fields(Pretty::default());

//     tracing_subscriber::registry().with(fmt_layer).with(perf_layer).init(); // Install these as subscribers to tracing events

//     todo!("write your awesome application");
// }

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
