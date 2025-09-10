use crate::ast::{self, Expr, Selection};
use crate::ast::{Literal, Predicate};
use crate::error::ParseError;
use crate::parser;
use std::convert::TryFrom;

impl<'a> TryFrom<&'a str> for Predicate {
    type Error = ParseError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> { Ok(parser::parse_selection(value)?.predicate) }
}
impl TryFrom<String> for Predicate {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> { Ok(parser::parse_selection(&value)?.predicate) }
}
impl<'a> TryFrom<&'a str> for Selection {
    type Error = ParseError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> { parser::parse_selection(value) }
}
impl TryFrom<String> for Selection {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> { parser::parse_selection(&value) }
}

impl TryFrom<ast::Expr> for Predicate {
    type Error = ParseError;

    fn try_from(value: ast::Expr) -> Result<Self, Self::Error> {
        match value {
            ast::Expr::Predicate(p) => Ok(p),
            ast::Expr::Placeholder => Ok(Predicate::Placeholder),
            ast::Expr::Literal(ast::Literal::Boolean(true)) => Ok(Predicate::True),
            ast::Expr::Literal(ast::Literal::Boolean(false)) => Ok(Predicate::False),
            _ => Err(ParseError::InvalidPredicate("Expression is not a predicate".into())),
        }
    }
}
