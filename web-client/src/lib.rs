pub mod client;
pub mod utils;

pub use hydra_proto as proto;
use wasm_bindgen::prelude::*;

#[cfg(feature = "start")]
#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    wasm_logger::init(wasm_logger::Config::default());
    Ok(())
}

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("Hello, web2!");
}
