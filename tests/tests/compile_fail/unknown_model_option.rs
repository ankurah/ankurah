//! Struct-level `#[model(...)]` options form a closed vocabulary. A typo
//! must not silently discard a schema- or identity-affecting declaration.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(systm = "Model")]
pub struct UnknownModelOption {
    pub name: String,
}

fn main() {}
