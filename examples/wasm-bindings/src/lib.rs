pub use ankurah_web_client::Client;
use example_model::*;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    wasm_logger::init(wasm_logger::Config::default());

    let _album = Album {
        name: "Test Name".to_string(),
    };

    Ok(())
}
