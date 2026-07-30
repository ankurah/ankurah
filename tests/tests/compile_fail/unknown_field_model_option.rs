//! Field-level `#[model(...)]` options form a closed vocabulary too. Only
//! the bare `ephemeral` flag is currently valid on a field.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct UnknownFieldModelOption {
    #[model(ephemral)]
    pub scratch: String,
}

fn main() {}
