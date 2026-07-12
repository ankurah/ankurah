//! An explicit-id binding must use the canonical URL-safe no-pad spelling;
//! unused bits in the final symbol may not create aliases for one EntityId.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct NoncanonicalIdModel {
    // The canonical spelling of thirty-two zero bytes ends in `A`; changing
    // only the unused trailing bits to `B` must be rejected.
    #[property(id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB")]
    pub label: String,
}

fn main() {}
