use crate::ast::Predicate;
use crate::ast::{self, Selection};
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
            ast::Expr::Literal(ast::Literal::Bool(true)) => Ok(Predicate::True),
            ast::Expr::Literal(ast::Literal::Bool(false)) => Ok(Predicate::False),
            _ => Err(ParseError::InvalidPredicate("Expression is not a predicate".into())),
        }
    }
}

#[cfg(feature = "wasm")]
impl TryFrom<wasm_bindgen::JsValue> for ast::Expr {
    type Error = ParseError;

    fn try_from(value: wasm_bindgen::JsValue) -> Result<Self, Self::Error> {
        if value.is_null() || value.is_undefined() {
            // FIXME / HACK - we should probably have a NULL literal
            return Ok(ast::Expr::Literal(ast::Literal::String("NULL_IMPROBLE_VALUE".to_string())));
        }

        // Try string first
        if let Some(s) = value.as_string() {
            return Ok(ast::Expr::Literal(ast::Literal::String(s)));
        }

        // Try boolean
        if let Some(b) = value.as_bool() {
            return Ok(ast::Expr::Literal(ast::Literal::Bool(b)));
        }

        // Try number
        if let Some(n) = value.as_f64() {
            // Use I64 for integers, F64 for floats
            if n.fract() == 0.0 {
                let n_int = n as i64;
                return Ok(ast::Expr::Literal(ast::Literal::I64(n_int)));
            } else {
                return Ok(ast::Expr::Literal(ast::Literal::F64(n)));
            }
        }

        // If nothing matches, return error
        Err(ParseError::InvalidPredicate("Unsupported JsValue type for conversion to Expr".into()))
    }
}
