use crate::ast;
use crate::ast::Predicate;
use crate::error::ParseError;
use crate::parser;
use std::convert::TryFrom;

impl<'a> TryFrom<&'a str> for Predicate {
    type Error = ParseError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> { parser::parse_selection(value) }
}
impl TryFrom<String> for Predicate {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> { parser::parse_selection(&value) }
}

impl TryFrom<ast::Expr> for Predicate {
    type Error = ParseError;

    fn try_from(value: ast::Expr) -> Result<Self, Self::Error> {
        match value {
            ast::Expr::Predicate(p) => Ok(p),
            _ => Err(ParseError::InvalidPredicate("Expression is not a predicate".into())),
        }
    }
}
