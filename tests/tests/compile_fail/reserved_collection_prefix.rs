//! A user model whose collection carries the reserved `_ankurah_` prefix
//! is refused at derive time (specs/model-property-metadata/rfc.md section
//! 4). The collection id is the lowercased struct name, so this struct
//! claims `_ankurah_evil`.

use ankurah::Model;
use serde::{Deserialize, Serialize};

#[allow(non_camel_case_types)]
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct _ankurah_evil {
    pub name: String,
}

fn main() {}
