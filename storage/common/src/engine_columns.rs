//! Storage-engine column paths and the shared AST lowering walk.

use ankql::ast::{Expr, OrderByItem, Predicate, Selection, Stage};

/// A selection an engine can read: every path is one of its own columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineColumns;

impl Stage for EngineColumns {
    type Path = ColumnPath;
}

/// A column in an engine's own storage, plus any JSON sub-path into the value
/// that column holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPath {
    pub column: String,
    pub subpath: Vec<String>,
}

impl ColumnPath {
    pub fn new(column: impl Into<String>, subpath: Vec<String>) -> Self { Self { column: column.into(), subpath } }

    pub fn simple(column: impl Into<String>) -> Self { Self { column: column.into(), subpath: Vec::new() } }

    pub fn is_simple(&self) -> bool { self.subpath.is_empty() }
}

impl std::fmt::Display for ColumnPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.column)?;
        for step in &self.subpath {
            write!(f, ".{}", step)?;
        }
        Ok(())
    }
}

pub fn lower_selection<S: Stage>(selection: &Selection<S>, column: &impl Fn(&S::Path) -> ColumnPath) -> Selection<EngineColumns> {
    Selection {
        predicate: lower_predicate(&selection.predicate, column),
        order_by: selection
            .order_by
            .as_ref()
            .map(|items| items.iter().map(|item| OrderByItem { path: column(&item.path), direction: item.direction.clone() }).collect()),
        limit: selection.limit,
    }
}

pub fn lower_predicate<S: Stage>(predicate: &Predicate<S>, column: &impl Fn(&S::Path) -> ColumnPath) -> Predicate<EngineColumns> {
    match predicate {
        Predicate::Comparison { left, operator, right } => Predicate::Comparison {
            left: Box::new(lower_expr(left, column)),
            operator: operator.clone(),
            right: Box::new(lower_expr(right, column)),
        },
        Predicate::IsNull(expr) => Predicate::IsNull(Box::new(lower_expr(expr, column))),
        Predicate::And(left, right) => Predicate::And(Box::new(lower_predicate(left, column)), Box::new(lower_predicate(right, column))),
        Predicate::Or(left, right) => Predicate::Or(Box::new(lower_predicate(left, column)), Box::new(lower_predicate(right, column))),
        Predicate::Not(inner) => Predicate::Not(Box::new(lower_predicate(inner, column))),
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,
        Predicate::Placeholder => Predicate::Placeholder,
    }
}

pub fn lower_expr<S: Stage>(expr: &Expr<S>, column: &impl Fn(&S::Path) -> ColumnPath) -> Expr<EngineColumns> {
    match expr {
        Expr::Literal(value) => Expr::Literal(value.clone()),
        Expr::Path(path) => Expr::Path(column(path)),
        Expr::Predicate(predicate) => Expr::Predicate(lower_predicate(predicate, column)),
        Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
            left: Box::new(lower_expr(left, column)),
            operator: operator.clone(),
            right: Box::new(lower_expr(right, column)),
        },
        Expr::ExprList(items) => Expr::ExprList(items.iter().map(|item| lower_expr(item, column)).collect()),
        Expr::Placeholder => Expr::Placeholder,
    }
}
