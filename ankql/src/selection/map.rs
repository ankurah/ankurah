use crate::ast::{Expr, OrderByItem, Predicate, Selection, Stage};

/// Map references between stages without changing predicate structure or literal values.
pub fn map_references<S: Stage, T: Stage>(
    selection: &Selection<S>,
    path: &impl Fn(&S::Path) -> T::Path,
    model: &impl Fn(&S::ModelId) -> T::ModelId,
) -> Selection<T> {
    Selection {
        predicate: map_predicate(&selection.predicate, path, model),
        order_by: selection
            .order_by
            .as_ref()
            .map(|items| items.iter().map(|item| OrderByItem { path: path(&item.path), direction: item.direction.clone() }).collect()),
        limit: selection.limit,
    }
}

fn map_predicate<S: Stage, T: Stage>(
    predicate: &Predicate<S>,
    path: &impl Fn(&S::Path) -> T::Path,
    model: &impl Fn(&S::ModelId) -> T::ModelId,
) -> Predicate<T> {
    match predicate {
        Predicate::Comparison { left, operator, right } => Predicate::Comparison {
            left: Box::new(map_expr(left, path, model)),
            operator: operator.clone(),
            right: Box::new(map_expr(right, path, model)),
        },
        Predicate::IsNull(expr) => Predicate::IsNull(Box::new(map_expr(expr, path, model))),
        Predicate::And(left, right) => {
            Predicate::And(Box::new(map_predicate(left, path, model)), Box::new(map_predicate(right, path, model)))
        }
        Predicate::Or(left, right) => {
            Predicate::Or(Box::new(map_predicate(left, path, model)), Box::new(map_predicate(right, path, model)))
        }
        Predicate::Not(inner) => Predicate::Not(Box::new(map_predicate(inner, path, model))),
        Predicate::MemberOf(id) => Predicate::MemberOf(model(id)),
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,
        Predicate::Placeholder => Predicate::Placeholder,
    }
}

fn map_expr<S: Stage, T: Stage>(
    expr: &Expr<S>,
    path: &impl Fn(&S::Path) -> T::Path,
    model: &impl Fn(&S::ModelId) -> T::ModelId,
) -> Expr<T> {
    match expr {
        Expr::Literal(value) => Expr::Literal(value.clone()),
        Expr::Path(reference) => Expr::Path(path(reference)),
        Expr::Predicate(predicate) => Expr::Predicate(map_predicate(predicate, path, model)),
        Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
            left: Box::new(map_expr(left, path, model)),
            operator: operator.clone(),
            right: Box::new(map_expr(right, path, model)),
        },
        Expr::ExprList(items) => Expr::ExprList(items.iter().map(|item| map_expr(item, path, model)).collect()),
        Expr::Placeholder => Expr::Placeholder,
    }
}
