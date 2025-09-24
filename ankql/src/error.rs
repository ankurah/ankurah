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
