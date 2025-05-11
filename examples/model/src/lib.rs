use ankurah::{Model, Property, Ref};
#[cfg(feature = "wasm")]
use tsify::Tsify;

use serde::{Deserialize, Serialize};
#[derive(Model, Serialize, Deserialize)]
pub struct Artist {
    pub added: String, //later use DateTime, but not yet,
    pub name: String,
    pub years_active: YearsActive,
}

#[derive(Model, Serialize, Deserialize)]
pub struct Album {
    // pub artist: Ref<Artist>,
    pub name: String,
    // pub year: u32,
    // pub producer: Option<Ref<Producer>>,
}

#[derive(Model, Serialize, Deserialize)]
pub struct Producer {
    name: String,
}

// Example of a "complex" property whih is usable in JS/TS
#[derive(Property, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct YearsActive {
    pub start: u32,
    pub end: u32,
}
