//! Type resolution for query AST preparation.
//!
//! The TypeResolver determines the ValueType for paths in queries, enabling proper
//! literal conversion before execution. This is a temporary heuristic-based solution
//! until Phase 3 schema metadata is implemented.
//!
//! Current heuristics:
//! - Multi-step paths (e.g., `data.number`) → ValueType::Json (nested JSON traversal)
//! - `id` field → ValueType::EntityId
//! - Simple paths → infer from the literal being compared
//!
//! ## AST Mutation (Temporary)
//!
//! Until we have a proper MIR (Mid-level IR) tree, we temporarily mutate the AST
//! in place via `prepare_predicate`. This converts literals to `Value::Json`
//! when comparing against JSON paths, ensuring type-aware comparison semantics
//! match across Postgres, Sled, and in-memory filtering.

use crate::value::{Value, ValueType};
use ankql::ast::{Expr, PathExpr, Predicate, Selection};

/// Determines ValueType for paths in queries.
///
/// TODO(Phase 3): Replace heuristics with proper schema lookup from System tables.
#[derive(Debug, Clone, Default)]
pub struct TypeResolver;

impl TypeResolver {
    pub fn new() -> Self { TypeResolver }

    /// Resolve the ValueType for a path expression.
    ///
    /// Returns:
    /// - `ValueType::Json` for multi-step paths (nested JSON traversal)
    /// - `ValueType::EntityId` for the "id" field
    /// - `None` for simple paths (caller should use literal's inherent type)
    pub fn resolve_path(&self, path: &PathExpr) -> Option<ValueType> {
        // Multi-step paths are JSON subfield traversals
        if !path.is_simple() {
            return Some(ValueType::Json);
        }

        // The "id" field is always EntityId
        if path.first() == "id" {
            return Some(ValueType::EntityId);
        }

        // For simple paths, return None to indicate the literal's type should be used
        None
    }

    /// Resolve the type for an expression (path or literal).
    fn resolve_expr_type(&self, expr: &Expr) -> Option<ValueType> {
        match expr {
            Expr::Path(path) => self.resolve_path(path),
            Expr::Literal(lit) => Some(lit.value_type()),
            _ => None,
        }
    }

    /// Convert an expression's literal to the target type if needed.
    fn convert_expr(&self, expr: Expr, target_type: Option<ValueType>) -> Expr {
        match (expr, target_type) {
            (Expr::Literal(lit), Some(target)) => match lit.cast_to(target) {
                Ok(casted) => Expr::Literal(casted),
                Err(_) => Expr::Literal(lit), // Fallback if cast fails
            },
            (other, _) => other,
        }
    }

    /// Resolve types in a selection, returning a new selection with converted literals.
    /// Eventually this will return a TAST (Typed AST).
    pub fn resolve_selection_types(&self, selection: Selection) -> Selection {
        Selection { predicate: self.resolve_types(selection.predicate), order_by: selection.order_by, limit: selection.limit }
    }

    /// Resolve types in a predicate, returning a new predicate with converted literals.
    ///
    /// Uses `resolve_path` to determine field types, then converts literals on the
    /// other side of comparisons to match. Eventually this will return a TAST.
    pub fn resolve_types(&self, predicate: Predicate) -> Predicate {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                // Look up types from paths
                let left_type = self.resolve_expr_type(&left);
                let right_type = self.resolve_expr_type(&right);

                // Convert literals based on the type from the other side
                let new_left = self.convert_expr(*left, right_type);
                let new_right = self.convert_expr(*right, left_type);

                Predicate::Comparison { left: Box::new(new_left), operator, right: Box::new(new_right) }
            }
            Predicate::And(left, right) => Predicate::And(Box::new(self.resolve_types(*left)), Box::new(self.resolve_types(*right))),
            Predicate::Or(left, right) => Predicate::Or(Box::new(self.resolve_types(*left)), Box::new(self.resolve_types(*right))),
            Predicate::Not(inner) => Predicate::Not(Box::new(self.resolve_types(*inner))),
            // These don't need type resolution
            Predicate::IsNull(_) | Predicate::True | Predicate::False | Predicate::Placeholder => predicate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_simple_path() {
        let resolver = TypeResolver::new();
        let path = PathExpr::simple("name");
        assert_eq!(resolver.resolve_path(&path), None);
    }

    #[test]
    fn test_resolve_id_path() {
        let resolver = TypeResolver::new();
        let path = PathExpr::simple("id");
        assert_eq!(resolver.resolve_path(&path), Some(ValueType::EntityId));
    }

    #[test]
    fn test_resolve_json_path() {
        let resolver = TypeResolver::new();
        let path = PathExpr { steps: vec!["data".to_string(), "number".to_string()] };
        assert_eq!(resolver.resolve_path(&path), Some(ValueType::Json));
    }

    #[test]
    fn test_resolve_types_converts_literal_for_json_path() {
        use ankql::ast::ComparisonOperator;

        let resolver = TypeResolver::new();

        // data.number = 9 → literal should be converted to Json
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr { steps: vec!["data".to_string(), "number".to_string()] })),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::I64(9))),
        };

        let resolved = resolver.resolve_types(predicate);

        // Check that the literal was converted to Json
        if let Predicate::Comparison { right, .. } = resolved {
            match *right {
                Expr::Literal(Value::Json(serde_json::Value::Number(n))) => {
                    assert_eq!(n.as_i64(), Some(9));
                }
                other => panic!("Expected Json(Number), got {:?}", other),
            }
        } else {
            panic!("Expected Comparison predicate");
        }
    }

    #[test]
    fn test_resolve_types_leaves_simple_path_literal_alone() {
        use ankql::ast::ComparisonOperator;

        let resolver = TypeResolver::new();

        // name = "test" → literal should NOT be converted (simple path)
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("name"))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::String("test".to_string()))),
        };

        let resolved = resolver.resolve_types(predicate);

        // Check that the literal was NOT converted
        if let Predicate::Comparison { right, .. } = resolved {
            match *right {
                Expr::Literal(Value::String(s)) => {
                    assert_eq!(s, "test");
                }
                other => panic!("Expected String literal, got {:?}", other),
            }
        } else {
            panic!("Expected Comparison predicate");
        }
    }
}
