use crate::grammar;
use std::convert::Infallible;
use thiserror::Error;

#[cfg(feature = "wasm")]
use wasm_bindgen;

/// Custom error type for parsing errors
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Syntax error: {0}")]
    SyntaxError(String),
    #[error("Empty expression")]
    EmptyExpression,
    #[error("Expected {expected}, got {got:?}")]
    UnexpectedRule { expected: &'static str, got: grammar::Rule },
    #[error("Invalid predicate: {0}")]
    InvalidPredicate(String),
    #[error("Missing {0} operand")]
    MissingOperand(&'static str),
}

impl From<Infallible> for ParseError {
    fn from(_: Infallible) -> Self { unreachable!("Infallible can never be constructed") }
}

#[cfg(feature = "wasm")]
impl From<ParseError> for wasm_bindgen::JsValue {
    fn from(error: ParseError) -> Self { wasm_bindgen::JsValue::from_str(&error.to_string()) }
}

/// A selection reached a consumer that requires it to be fully resolved and
/// populated, but it was not. Raised by [`crate::ast::Selection::check`].
#[derive(Debug, Error)]
pub enum SelectionError {
    /// A property reference that never resolved to a durable identity. It
    /// carries only a name, which a storage engine would have to guess a
    /// column from.
    #[error("unresolved property path `{0}`: a selection must be resolved to property ids before it is read")]
    UnresolvedPath(String),
    /// A placeholder that was never filled in by
    /// [`crate::ast::Selection::populate`], so it carries no value to compare.
    #[error("unpopulated placeholder: a selection must be populated before it is read")]
    UnpopulatedPlaceholder,
}

/// Custom error type for SQL generation errors
#[derive(Debug, Error)]
pub enum SqlGenerationError {
    #[error("Placeholder count mismatch: expected {expected}, found {found}")]
    PlaceholderCountMismatch { expected: usize, found: usize },
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(&'static str),
}
