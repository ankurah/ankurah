//! Query substitution values for FFI bindings.
//!
//! UniFFI doesn't support variadic arguments or generic iterators, so we provide
//! a concrete enum type for query parameter substitution.
//!
//! TODO: Investigate if there's a better approach in future UniFFI versions.
//! Ideally we'd accept something closer to `impl Into<ankql::ast::Expr>` directly.

use ankurah_proto::EntityId;

/// Value type for query parameter substitution in FFI contexts.
///
/// Used with `fetch()`, `query()`, etc. to fill in `?` placeholders:
/// ```ignore
/// ops.fetch(ctx, "name = ?", vec![QueryValue::String("Alice".into())])
/// ```
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[derive(Debug, Clone)]
pub enum QueryValue {
    /// String value
    String(String),
    /// 64-bit integer
    Int(i64),
    /// 64-bit float
    Float(f64),
    /// Boolean
    Bool(bool),
    /// Entity ID (as base64 string for FFI compatibility)
    EntityId(String),
}

impl TryFrom<QueryValue> for ankql::ast::Expr {
    type Error = ankql::error::ParseError;

    fn try_from(value: QueryValue) -> Result<Self, Self::Error> {
        use ankql::ast::{Expr, Literal};

        Ok(match value {
            QueryValue::String(s) => Expr::Literal(Literal::String(s)),
            QueryValue::Int(i) => Expr::Literal(Literal::I64(i)),
            QueryValue::Float(f) => Expr::Literal(Literal::F64(f)),
            QueryValue::Bool(b) => Expr::Literal(Literal::Bool(b)),
            QueryValue::EntityId(s) => {
                let id = EntityId::from_base64(&s)
                    .map_err(|e| ankql::error::ParseError::InvalidPredicate(format!("Invalid EntityId: {}", e)))?;
                Expr::Literal(Literal::EntityId(id.to_ulid()))
            }
        })
    }
}

// Convenience conversions for Rust usage
impl From<String> for QueryValue {
    fn from(s: String) -> Self { QueryValue::String(s) }
}

impl From<&str> for QueryValue {
    fn from(s: &str) -> Self { QueryValue::String(s.to_string()) }
}

impl From<i64> for QueryValue {
    fn from(i: i64) -> Self { QueryValue::Int(i) }
}

impl From<i32> for QueryValue {
    fn from(i: i32) -> Self { QueryValue::Int(i as i64) }
}

impl From<f64> for QueryValue {
    fn from(f: f64) -> Self { QueryValue::Float(f) }
}

impl From<bool> for QueryValue {
    fn from(b: bool) -> Self { QueryValue::Bool(b) }
}

impl From<EntityId> for QueryValue {
    fn from(id: EntityId) -> Self { QueryValue::EntityId(id.to_base64()) }
}
