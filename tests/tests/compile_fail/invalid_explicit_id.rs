//! An explicit-id binding must be URL-safe base64 of exactly 32
//! bytes (an EntityId); a too-short value is refused at derive time.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct BadIdModel {
    #[property(id = "tooshort")]
    pub label: String,
}

fn main() {}
