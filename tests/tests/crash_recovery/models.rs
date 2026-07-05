//! Models shared by the crash-recovery scenarios. Kept separate from the top
//! `common.rs` because this is an isolated test binary with its own module tree.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Model, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}
