use ankurah::Model;

pub struct Album {
    pub name: String,
}

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
// #[wasm_bindgen]
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Session {
    pub date_connected: String,
    pub ip_address: String,
    pub node_id: String,
    // #[cfg(not(feature = "wasm"))]
    // #[model(ephemeral)]
    // #[wasm_bindgen(skip)]
    // frobnicator: Frobnicator,
}

#[derive(Default, Clone, Debug)]
pub struct Frobnicator {}
