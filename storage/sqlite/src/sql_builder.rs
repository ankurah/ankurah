//! SQL builder for SQLite queries
//!
//! Converts AnkQL predicates to SQLite-compatible SQL WHERE clauses.

use ankql::ast::{ComparisonOperator, Expr, Literal, OrderByItem, OrderDirection, Predicate, Selection};
use ankurah_core::EntityId;
use thiserror::Error;

use crate::error::SqliteError;

#[derive(Debug, Error, Clone)]
pub enum SqlGenerationError {
    #[error("Placeholder found in predicate - placeholders should be replaced before predicate processing")]
    PlaceholderFound,
    #[error("Unsupported expression type: {0}")]
    UnsupportedExpression(&'static str),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(&'static str),
}

impl From<SqlGenerationError> for SqliteError {
    fn from(err: SqlGenerationError) -> Self { SqliteError::SqlGeneration(err.to_string()) }
}

/// Result of splitting a predicate for SQLite execution.
#[derive(Debug, Clone)]
pub struct SplitPredicate {
    /// Predicate that can be pushed down to SQLite WHERE clause
    pub sql_predicate: Predicate,
    /// Predicate that must be evaluated in Rust after fetching (Predicate::True if nothing remains)
    pub remaining_predicate: Predicate,
}

impl SplitPredicate {
    /// Check if there's any remaining predicate that needs post-filtering
    pub fn needs_post_filter(&self) -> bool { !matches!(self.remaining_predicate, Predicate::True) }
}

/// Split a predicate into parts that can be pushed down to SQLite vs evaluated post-fetch.
pub fn split_predicate_for_sqlite(predicate: &Predicate) -> SplitPredicate {
    let (sql_pred, remaining_pred) = split_predicate_recursive(predicate);
    SplitPredicate { sql_predicate: sql_pred, remaining_predicate: remaining_pred }
}

fn split_predicate_recursive(predicate: &Predicate) -> (Predicate, Predicate) {
    match predicate {
        Predicate::Comparison { left, operator: _, right } => {
            if can_pushdown_comparison(left, right) {
                (predicate.clone(), Predicate::True)
            } else {
                (Predicate::True, predicate.clone())
            }
        }

        Predicate::And(left, right) => {
            let (left_sql, left_remaining) = split_predicate_recursive(left);
            let (right_sql, right_remaining) = split_predicate_recursive(right);

            let sql_pred = match (&left_sql, &right_sql) {
                (Predicate::True, Predicate::True) => Predicate::True,
                (Predicate::True, _) => right_sql,
                (_, Predicate::True) => left_sql,
                _ => Predicate::And(Box::new(left_sql), Box::new(right_sql)),
            };

            let remaining_pred = match (&left_remaining, &right_remaining) {
                (Predicate::True, Predicate::True) => Predicate::True,
                (Predicate::True, _) => right_remaining,
                (_, Predicate::True) => left_remaining,
                _ => Predicate::And(Box::new(left_remaining), Box::new(right_remaining)),
            };

            (sql_pred, remaining_pred)
        }

        Predicate::Or(left, right) => {
            let (left_sql, left_remaining) = split_predicate_recursive(left);
            let (right_sql, right_remaining) = split_predicate_recursive(right);

            if matches!(left_remaining, Predicate::True) && matches!(right_remaining, Predicate::True) {
                (predicate.clone(), Predicate::True)
            } else {
                let sql_pred = match (&left_sql, &right_sql) {
                    (Predicate::True, Predicate::True) => Predicate::True,
                    (Predicate::True, _) => right_sql,
                    (_, Predicate::True) => left_sql,
                    _ => Predicate::Or(Box::new(left_sql), Box::new(right_sql)),
                };
                (sql_pred, predicate.clone())
            }
        }

        Predicate::Not(inner) => {
            let (inner_sql, inner_remaining) = split_predicate_recursive(inner);
            if matches!(inner_remaining, Predicate::True) {
                (Predicate::Not(Box::new(inner_sql)), Predicate::True)
            } else {
                (Predicate::True, predicate.clone())
            }
        }

        Predicate::IsNull(expr) => {
            if can_pushdown_expr(expr) {
                (predicate.clone(), Predicate::True)
            } else {
                (Predicate::True, predicate.clone())
            }
        }

        Predicate::True => (Predicate::True, Predicate::True),
        Predicate::False => (Predicate::False, Predicate::True),
        Predicate::Placeholder => (Predicate::True, predicate.clone()),
    }
}

fn can_pushdown_comparison(left: &Expr, right: &Expr) -> bool { can_pushdown_expr(left) && can_pushdown_expr(right) }

fn can_pushdown_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Path(path) => !path.steps.is_empty(),
        Expr::ExprList(exprs) => exprs.iter().all(can_pushdown_expr),
        Expr::Predicate(_) => false,
        Expr::InfixExpr { .. } => false,
        Expr::Placeholder => false,
    }
}

/// SQL builder for SQLite queries
pub struct SqlBuilder {
    sql: String,
    params: Vec<rusqlite::types::Value>,
    fields: Vec<String>,
    table_name: Option<String>,
}

impl Default for SqlBuilder {
    fn default() -> Self { Self::new() }
}

impl SqlBuilder {
    pub fn new() -> Self { Self { sql: String::new(), params: Vec::new(), fields: Vec::new(), table_name: None } }

    pub fn with_fields<T: Into<String>>(fields: Vec<T>) -> Self {
        Self { sql: String::new(), params: Vec::new(), fields: fields.into_iter().map(|f| f.into()).collect(), table_name: None }
    }

    pub fn table_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.table_name = Some(name.into());
        self
    }

    fn push_sql(&mut self, s: &str) { self.sql.push_str(s); }

    fn push_param(&mut self, value: rusqlite::types::Value) {
        self.sql.push('?');
        self.params.push(value);
    }

    pub fn build(self) -> Result<(String, Vec<rusqlite::types::Value>), SqlGenerationError> {
        if self.fields.is_empty() || self.table_name.is_none() {
            // Just return WHERE clause
            return Ok((self.sql, self.params));
        }

        let fields_clause = self.fields.iter().map(|field| format!(r#""{}""#, field.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
        let table = self.table_name.unwrap();
        let sql = format!(r#"SELECT {} FROM "{}" WHERE {}"#, fields_clause, table.replace('"', "\"\""), self.sql);

        Ok((sql, self.params))
    }

    #[allow(dead_code)]
    pub fn build_where_clause(self) -> (String, Vec<rusqlite::types::Value>) { (self.sql, self.params) }

    pub fn expr(&mut self, expr: &Expr) -> Result<(), SqlGenerationError> {
        match expr {
            Expr::Placeholder => return Err(SqlGenerationError::PlaceholderFound),
            Expr::Literal(lit) => self.literal(lit),
            Expr::Path(path) => {
                if path.is_simple() {
                    // Single-step path: regular column reference
                    let escaped = path.first().replace('"', "\"\"");
                    self.push_sql(&format!(r#""{}""#, escaped));
                } else {
                    // Multi-step path: JSONB traversal
                    // SQLite's -> operator returns JSONB, but for comparisons we need to extract the value.
                    // Use json_extract() with the full JSON path for reliable comparisons.
                    let first = path.first().replace('"', "\"\"");
                    // Build JSON path: $.step1.step2.step3
                    let json_path = if path.steps.len() == 2 {
                        format!("$.{}", path.steps[1].replace('\'', "''"))
                    } else {
                        format!("$.{}", path.steps.iter().skip(1).map(|s| s.replace('\'', "''")).collect::<Vec<_>>().join("."))
                    };
                    self.push_sql(&format!(r#"json_extract("{}", '{}')"#, first, json_path));
                }
            }
            Expr::ExprList(exprs) => {
                self.push_sql("(");
                for (i, expr) in exprs.iter().enumerate() {
                    if i > 0 {
                        self.push_sql(", ");
                    }
                    self.expr(expr)?;
                }
                self.push_sql(")");
            }
            _ => return Err(SqlGenerationError::UnsupportedExpression("Only literal, path, and list expressions are supported")),
        }
        Ok(())
    }

    fn literal(&mut self, lit: &Literal) {
        match lit {
            Literal::String(s) => self.push_param(rusqlite::types::Value::Text(s.clone())),
            Literal::I64(i) => self.push_param(rusqlite::types::Value::Integer(*i)),
            Literal::F64(f) => self.push_param(rusqlite::types::Value::Real(*f)),
            Literal::Bool(b) => self.push_param(rusqlite::types::Value::Integer(if *b { 1 } else { 0 })),
            Literal::I16(i) => self.push_param(rusqlite::types::Value::Integer(*i as i64)),
            Literal::I32(i) => self.push_param(rusqlite::types::Value::Integer(*i as i64)),
            Literal::EntityId(ulid) => self.push_param(rusqlite::types::Value::Text(EntityId::from_ulid(*ulid).to_base64())),
            Literal::Object(bytes) => self.push_param(rusqlite::types::Value::Blob(bytes.clone())),
            Literal::Binary(bytes) => self.push_param(rusqlite::types::Value::Blob(bytes.clone())),
            // For JSON literals, extract the raw SQL value since json_extract() returns SQL types.
            // json.to_string() would produce "US" (with quotes) but we need just US.
            Literal::Json(json) => match json {
                serde_json::Value::String(s) => self.push_param(rusqlite::types::Value::Text(s.clone())),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        self.push_param(rusqlite::types::Value::Integer(i));
                    } else if let Some(f) = n.as_f64() {
                        self.push_param(rusqlite::types::Value::Real(f));
                    } else {
                        // Fallback: serialize as text
                        self.push_param(rusqlite::types::Value::Text(n.to_string()));
                    }
                }
                serde_json::Value::Bool(b) => self.push_param(rusqlite::types::Value::Integer(if *b { 1 } else { 0 })),
                serde_json::Value::Null => self.push_param(rusqlite::types::Value::Null),
                // For arrays and objects, serialize as JSON text
                _ => self.push_param(rusqlite::types::Value::Text(json.to_string())),
            },
        }
    }

    pub fn comparison_op(&mut self, op: &ComparisonOperator) -> Result<(), SqlGenerationError> {
        self.push_sql(comparison_op_to_sql(op)?);
        Ok(())
    }

    pub fn predicate(&mut self, predicate: &Predicate) -> Result<(), SqlGenerationError> {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                // Emit: left op right
                // JSONB paths use json_extract() which returns SQL values, so direct comparison works
                self.expr(left)?;
                self.push_sql(" ");
                self.comparison_op(operator)?;
                self.push_sql(" ");
                self.expr(right)?;
            }
            Predicate::And(left, right) => {
                self.predicate(left)?;
                self.push_sql(" AND ");
                self.predicate(right)?;
            }
            Predicate::Or(left, right) => {
                self.push_sql("(");
                self.predicate(left)?;
                self.push_sql(" OR ");
                self.predicate(right)?;
                self.push_sql(")");
            }
            Predicate::Not(pred) => {
                self.push_sql("NOT (");
                self.predicate(pred)?;
                self.push_sql(")");
            }
            Predicate::IsNull(expr) => {
                self.expr(expr)?;
                self.push_sql(" IS NULL");
            }
            Predicate::True => {
                self.push_sql("1=1");
            }
            Predicate::False => {
                self.push_sql("1=0");
            }
            Predicate::Placeholder => {
                return Err(SqlGenerationError::PlaceholderFound);
            }
        }
        Ok(())
    }

    pub fn selection(&mut self, selection: &Selection) -> Result<(), SqlGenerationError> {
        self.predicate(&selection.predicate)?;

        if let Some(order_by_items) = &selection.order_by {
            self.push_sql(" ORDER BY ");
            for (i, order_by) in order_by_items.iter().enumerate() {
                if i > 0 {
                    self.push_sql(", ");
                }
                self.order_by_item(order_by)?;
            }
        }

        if let Some(limit) = selection.limit {
            self.push_sql(&format!(" LIMIT {}", limit));
        }

        Ok(())
    }

    pub fn order_by_item(&mut self, order_by: &OrderByItem) -> Result<(), SqlGenerationError> {
        // Handle JSON paths the same way as in expr() - use -> operator for multi-step paths
        if order_by.path.is_simple() {
            // Single-step path: regular column reference
            let escaped = order_by.path.first().replace('"', "\"\"");
            self.push_sql(&format!(r#""{}""#, escaped));
        } else {
            // Multi-step path: JSONB traversal using -> operator
            let first = order_by.path.first().replace('"', "\"\"");
            self.push_sql(&format!(r#""{}""#, first));

            for step in order_by.path.steps.iter().skip(1) {
                let escaped = step.replace('\'', "''");
                // Use -> to keep as JSONB (not ->> which extracts as text)
                self.push_sql(&format!("->'{}'", escaped));
            }
        }

        match order_by.direction {
            OrderDirection::Asc => self.push_sql(" ASC"),
            OrderDirection::Desc => self.push_sql(" DESC"),
        }

        Ok(())
    }
}

fn comparison_op_to_sql(op: &ComparisonOperator) -> Result<&'static str, SqlGenerationError> {
    Ok(match op {
        ComparisonOperator::Equal => "=",
        ComparisonOperator::NotEqual => "<>",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
        ComparisonOperator::In => "IN",
        ComparisonOperator::Between => return Err(SqlGenerationError::UnsupportedOperator("BETWEEN operator is not yet supported")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::parser::parse_selection;

    #[test]
    fn test_simple_equality() {
        let selection = parse_selection("name = 'Alice'").unwrap();
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, params) = sql.build_where_clause();

        assert_eq!(sql_string, r#""name" = ?"#);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_and_condition() {
        let selection = parse_selection("name = 'Alice' AND age = 30").unwrap();
        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection).unwrap();
        let (sql_string, params) = sql.build().unwrap();

        assert_eq!(sql_string, r#"SELECT "id", "name", "age" FROM "users" WHERE "name" = ? AND "age" = ?"#);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_json_path() {
        let selection = parse_selection("data.status = 'active'").unwrap();
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, _) = sql.build_where_clause();

        // Uses json_extract() for reliable comparisons with BLOB JSONB columns
        assert_eq!(sql_string, r#"json_extract("data", '$.status') = ?"#);
    }

    #[test]
    fn test_json_nested_path() {
        let selection = parse_selection("data.user.name = 'Alice'").unwrap();
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, _) = sql.build_where_clause();

        // Uses json_extract() with nested path for reliable comparisons
        assert_eq!(sql_string, r#"json_extract("data", '$.user.name') = ?"#);
    }

    #[test]
    fn test_json_numeric_comparison() {
        let selection = parse_selection("data.count > 10").unwrap();
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, _) = sql.build_where_clause();

        // Numeric comparison with json_extract() - SQLite handles numeric comparison correctly
        assert_eq!(sql_string, r#"json_extract("data", '$.count') > ?"#);
    }

    #[test]
    fn test_in_operator() {
        let selection = parse_selection("name IN ('Alice', 'Bob')").unwrap();
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, params) = sql.build_where_clause();

        assert_eq!(sql_string, r#""name" IN (?, ?)"#);
        assert_eq!(params.len(), 2);
    }
}
