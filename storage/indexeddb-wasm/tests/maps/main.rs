//! IndexedDB's maps in a browser: the storage contract's map conformance
//! suite, records put or removed through the browser's own API, a reset
//! dropped part way, and the upgrades of a database by several tabs.

mod records;
mod resets;
mod suite;
mod upgrades;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

/// What the browser threw, as a test's failure.
fn thrown(error: wasm_bindgen::JsValue) -> anyhow::Error { anyhow::anyhow!("the browser threw {error:?}") }
