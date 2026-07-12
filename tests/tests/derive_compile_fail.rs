//! Compile-fail coverage for the `#[derive(Model)]` schema attributes
//! (work package A11a; specs/model-property-metadata/rfc.md sections 4,
//! 5.9). trybuild compiles each fixture as its own crate and asserts it
//! fails with the pinned diagnostic in the matching `.stderr`.
//!
//! Kept behind `cfg(not(miri))` and gated on a stable-ish message: the
//! error strings are our own (the reserved-prefix message and the
//! explicit-id validation message), so they do not drift with the
//! compiler. If a future rustc reflows the surrounding cascade, regenerate
//! with `TRYBUILD=overwrite cargo test -p ankurah-tests --test
//! derive_compile_fail`.

#[test]
fn derive_model_rejections() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/reserved_collection_prefix.rs");
    t.compile_fail("tests/compile_fail/invalid_explicit_id.rs");
    t.compile_fail("tests/compile_fail/noncanonical_explicit_id.rs");
}
