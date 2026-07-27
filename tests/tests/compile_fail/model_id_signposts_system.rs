//! `#[model(id = "...")]` takes the EntityId of an existing catalog
//! entity. A value that does not parse as one is refused, and the
//! diagnostic points at the system attribute (built-in identities are not
//! catalog entities).

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(id = "not-an-entity-id")]
pub struct WrongKindOfId {
    pub name: String,
}

fn main() {}
