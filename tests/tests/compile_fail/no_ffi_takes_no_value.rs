//! `no_ffi` is deliberately a bare flag, keeping its grammar distinct from
//! the string-valued `base`, `system`, and `id` options.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(no_ffi = true)]
pub struct ValuedNoFfi {
    pub name: String,
}

fn main() {}
