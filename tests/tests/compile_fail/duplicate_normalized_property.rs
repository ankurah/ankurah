use ankurah::Model;
use serde::{Deserialize, Serialize};

#[allow(non_snake_case)]
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct DuplicateProperty {
    pub name: String,
    pub Name: String,
}

fn main() {}
