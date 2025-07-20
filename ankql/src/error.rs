use crate::grammar;
use thiserror::Error;

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
