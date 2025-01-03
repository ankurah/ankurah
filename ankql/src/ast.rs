use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Literal),
    Identifier(Identifier),
    Predicate(Predicate),
    InfixExpr {
        left: Box<Expr>,
        operator: InfixOperator,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Identifier {
    Property(String),
    CollectionProperty(String, String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    Comparison {
        left: Box<Expr>,
        operator: ComparisonOperator,
        right: Box<Expr>,
    },
    IsNull(Box<Expr>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,              // =
    NotEqual,           // <> or !=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    In,                 // IN
    Between,            // BETWEEN
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InfixOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
}
