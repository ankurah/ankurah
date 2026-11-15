//! Digest-tree reads and maintenance across IndexedDB transactions and tabs.

mod reads;
mod support;
mod tabs;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
