use crate::ast::Predicate;
use crate::error::ParseError;
use crate::parser;
use std::convert::TryFrom;

impl<'a> TryFrom<&'a str> for Predicate {
    type Error = ParseError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        parser::parse_selection(value)
    }
}
