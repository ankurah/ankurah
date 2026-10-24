//! What every executor must agree on, checked natively on rusqlite and on
//! wasm32 on node:sqlite and better-sqlite3.

#[path = "../common/mod.rs"]
mod common;

mod calls;
mod connections;
mod statements;
mod values;

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Stamp {
    #[active_type(LWW)]
    pub name: String,
    #[active_type(LWW)]
    pub timestamp: i64,
}
