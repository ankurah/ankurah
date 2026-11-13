//! Compare and sort entity ids by their decoded bytes.
//!
//! Tables store ids as URL-safe base64, matching the representation Ankurah prints. PostgreSQL
//! sorts that text by collation, while the in-memory evaluator compares the ids' 32 bytes.
//! Entity id range comparisons and ORDER BY id use the decoded bytes. The entity table indexes
//! the decoding expression. Queries and the index share [`SQL`] so their expressions match,
//! allowing PostgreSQL to use the expression index.

/// SQL expression that decodes the `id` column to bytes.
pub(crate) const SQL: &str = r#"decode(translate("id", '-_', '+/') || '=', 'base64')"#;
