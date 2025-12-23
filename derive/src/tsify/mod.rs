//! Vendored subset of the tsify crate for generating TypeScript type definitions.
//!
//! We use this to generate POJO interfaces for Model structs (e.g., `interface Message { ... }`).
//!
//! TODO: Strip this down to just the parts we actually use - currently includes much more
//! than needed for our Model type generation use case.

pub(crate) mod attrs;
pub(crate) mod comments;
pub(crate) mod container;
pub(crate) mod decl;
pub(crate) mod error_tracker;
pub(crate) mod parser;
pub(crate) mod type_alias;
pub(crate) mod typescript;
pub(crate) mod wasm_bindgen;
