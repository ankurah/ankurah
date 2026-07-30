//! Deterministic source-derived identities for compiled models and fields.
//!
//! `#[derive(Model)]` computes one of these per struct and per active field,
//! entirely at compile time, from source names alone: a struct id hashes
//! `{module_path}::{StructName}` and a field id hashes
//! `{module_path}::{StructName}::{field_name}`, FNV-1a with 128-bit
//! parameters over the UTF-8 bytes. The same source names always produce the
//! same id, on every build and every machine, so the id survives rebuilds,
//! carries zero runtime cost, and keeps simulation runs byte-deterministic.
//! Renaming a module, struct, or field changes the id; changing a field's
//! TYPE does not, which is what a schema-migration hint wants to be true.
//!
//! The ids ride schema registration as optional hints. The server accepts
//! and ignores them today; they exist so a future migration path can
//! recognize a declaration by its source names when everything else about
//! it has changed.

use serde::{Deserialize, Serialize};

/// The FNV-1a 128-bit offset basis: every hash starts from this value.
const FNV_OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;
/// The FNV-1a 128-bit prime multiplier.
const FNV_PRIME: u128 = 0x0000000001000000000000000000013b;

/// Feed `bytes` through the FNV-1a round function, starting from `hash`:
/// xor each byte in, then multiply by the prime.
const fn fnv1a_extend(mut hash: u128, bytes: &[u8]) -> u128 {
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u128;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    hash
}

/// Hash path segments joined by `::` (without materializing the joined
/// string, which const contexts cannot do).
const fn hash_segments(segments: &[&str]) -> u128 {
    let mut hash = FNV_OFFSET_BASIS;
    let mut i = 0;
    while i < segments.len() {
        if i > 0 {
            hash = fnv1a_extend(hash, b"::");
        }
        hash = fnv1a_extend(hash, segments[i].as_bytes());
        i += 1;
    }
    hash
}

/// A model struct's deterministic source identity: the FNV-1a-128 hash of
/// `{module_path}::{StructName}`. Computed const by `#[derive(Model)]` with
/// the expanding crate's own `module_path!()`, so two structs sharing a name
/// in different modules get distinct ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UniqueStructId(u128);

impl UniqueStructId {
    /// Hash a struct identity from its module path and declared struct name.
    pub const fn from_names(module_path: &str, struct_name: &str) -> Self { Self(hash_segments(&[module_path, struct_name])) }
}

/// A model field's deterministic source identity: the FNV-1a-128 hash of
/// `{module_path}::{StructName}::{field_name}`. The input is names only, so
/// the id survives a change of the field's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UniqueFieldId(u128);

impl UniqueFieldId {
    /// Hash a field identity from its module path, declared struct name, and
    /// declared field name.
    pub const fn from_names(module_path: &str, struct_name: &str, field_name: &str) -> Self {
        Self(hash_segments(&[module_path, struct_name, field_name]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hash is a pure function of its inputs, and its exact output is a
    /// compatibility surface: these constants were computed independently
    /// (FNV-1a, 128-bit offset basis and prime, over the `::`-joined names)
    /// and must never change.
    #[test]
    fn hash_values_are_pinned() {
        assert_eq!(UniqueStructId::from_names("a::b", "C"), UniqueStructId(0x082a2280294ff78da90bfe94e1a13963));
        assert_eq!(UniqueFieldId::from_names("a::b", "C", "title"), UniqueFieldId(0x66830444d40c6f0182ac989f15e00253));
        // Deterministic: recomputing yields the identical id.
        assert_eq!(UniqueStructId::from_names("a::b", "C"), UniqueStructId::from_names("a::b", "C"));
        assert_eq!(UniqueFieldId::from_names("a::b", "C", "title"), UniqueFieldId::from_names("a::b", "C", "title"));
    }

    /// Every name segment participates: the same field name under a
    /// different module or struct, and a different field under the same
    /// module and struct, all get distinct ids.
    #[test]
    fn every_segment_distinguishes() {
        // Same struct and field names, different module.
        assert_ne!(
            UniqueFieldId::from_names("crate_a::rows", "Album", "title"),
            UniqueFieldId::from_names("crate_b::rows", "Album", "title")
        );
        // Same module and struct, different field.
        assert_ne!(
            UniqueFieldId::from_names("crate_a::rows", "Album", "title"),
            UniqueFieldId::from_names("crate_a::rows", "Album", "year")
        );
        // Same module and field, different struct.
        assert_ne!(
            UniqueFieldId::from_names("crate_a::rows", "Album", "title"),
            UniqueFieldId::from_names("crate_a::rows", "Track", "title")
        );
        // Struct ids distinguish by module too.
        assert_ne!(UniqueStructId::from_names("crate_a::rows", "Album"), UniqueStructId::from_names("crate_b::rows", "Album"));
        // The separator keeps segment boundaries unambiguous: ("a::b", "C")
        // and ("a", "b::C") produce the same joined string BY DESIGN (both
        // name the path a::b::C); shifting a name across the boundary
        // without reproducing the path does change the id.
        assert_ne!(UniqueStructId::from_names("a::b", "C"), UniqueStructId::from_names("a::bC", ""));
    }
}
