use crate::grammar;

/// Custom error type for parsing errors
#[derive(Debug)]
pub enum ParseError {
    SyntaxError(String),
    EmptyExpression,
    UnexpectedRule { expected: &'static str, got: grammar::Rule },
    InvalidPredicate(String),
    MissingOperand(&'static str),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SyntaxError(msg) => write!(f, "Syntax error: {}", msg),
            Self::EmptyExpression => write!(f, "Empty expression"),
            Self::UnexpectedRule { expected, got } => {
                write!(f, "Expected {}, got {:?}", expected, got)
            }
            Self::InvalidPredicate(msg) => write!(f, "Invalid predicate: {}", msg),
            Self::MissingOperand(side) => write!(f, "Missing {} operand", side),
        }
    }
}

impl std::error::Error for ParseError {}
