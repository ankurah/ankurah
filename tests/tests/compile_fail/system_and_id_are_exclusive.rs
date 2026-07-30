//! A system model's identity is built in; it cannot also bind a catalog
//! entity id.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(system = "Model", id = "AAAAAAAAAAAAAAAAAAAAAA")]
pub struct SystemWithId {
    pub name: String,
}

fn main() {}
