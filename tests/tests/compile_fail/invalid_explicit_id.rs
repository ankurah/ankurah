//! An explicit-id binding (RFC 5.9) must be URL-safe base64 of exactly 16
//! bytes (an EntityId); a too-short value is refused at derive time.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct BadIdModel {
    #[property(id = "tooshort")]
    pub label: String,
}

fn main() {}
