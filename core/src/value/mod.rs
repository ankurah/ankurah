mod cast;
mod collatable;
#[cfg(feature = "wasm")]
mod wasm;

pub use cast::CastError;

use ankurah_proto as proto;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    // Numbers
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),

    Bool(bool),
    String(String),
    EntityId(proto::EntityId),
    Object(Vec<u8>),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    I16,
    I32,
    I64,
    F64,
    Bool,
    String,
    EntityId,
    Object,
    Binary,
}

impl ValueType {
    pub fn of(v: &Value) -> Self {
        match v {
            Value::I16(_) => ValueType::I16,
            Value::I32(_) => ValueType::I32,
            Value::I64(_) => ValueType::I64,
            Value::F64(_) => ValueType::F64,
            Value::Bool(_) => ValueType::Bool,
            Value::String(_) => ValueType::String,
            Value::EntityId(_) => ValueType::EntityId,
            Value::Object(_) => ValueType::Object,
            Value::Binary(_) => ValueType::Binary,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::I16(int) => write!(f, "{:?}", int),
            Value::I32(int) => write!(f, "{:?}", int),
            Value::I64(int) => write!(f, "{:?}", int),
            Value::F64(float) => write!(f, "{:?}", float),
            Value::Bool(bool) => write!(f, "{:?}", bool),
            Value::String(string) => write!(f, "{:?}", string),
            Value::EntityId(entity_id) => write!(f, "{}", entity_id),
            Value::Object(object) => write!(f, "{:?}", object),
            Value::Binary(binary) => write!(f, "{:?}", binary),
        }
    }
}

impl From<ankql::ast::Literal> for Value {
    fn from(literal: ankql::ast::Literal) -> Self {
        match literal {
            ankql::ast::Literal::String(s) => Value::String(s),
            ankql::ast::Literal::Integer(i) => {
                // Use I32 as default, only use I64 if value doesn't fit in I32
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Value::I32(i as i32)
                } else {
                    Value::I64(i)
                }
            }
            ankql::ast::Literal::Float(f) => Value::F64(f),
            ankql::ast::Literal::Boolean(b) => Value::Bool(b),
        }
    }
}

impl From<&ankql::ast::Literal> for Value {
    fn from(literal: &ankql::ast::Literal) -> Self {
        match literal {
            ankql::ast::Literal::String(s) => Value::String(s.clone()),
            ankql::ast::Literal::Integer(i) => {
                // Use I32 as default, only use I64 if value doesn't fit in I32
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    Value::I32(*i as i32)
                } else {
                    Value::I64(*i)
                }
            }
            ankql::ast::Literal::Float(f) => Value::F64(*f),
            ankql::ast::Literal::Boolean(b) => Value::Bool(*b),
        }
    }
}
