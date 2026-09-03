//! Compile-fail coverage for `#[derive(Model)]` schema validation.

#[test]
fn derive_model_rejections() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/reserved_collection_prefix.rs");
    t.compile_fail("tests/compile_fail/invalid_explicit_id.rs");
    t.compile_fail("tests/compile_fail/noncanonical_explicit_id.rs");
    t.compile_fail("tests/compile_fail/duplicate_normalized_property.rs");
}
