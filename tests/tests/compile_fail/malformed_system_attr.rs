//! A `#[model(...)]` attribute value must be a string literal. An unquoted
//! value is refused outright; silently dropping it would turn an intended
//! system model into an ordinary one.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(system = Model)]
pub struct BadSystemAttr {
    pub name: String,
}

fn main() {}
