//! SQL builder for SQLite queries
//!
//! Converts AnkQL predicates to SQLite-compatible SQL WHERE clauses.

use crate::{error::SqliteError, id_bytes};
use ankql::ast::{Comparison, ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core_types::Value;
use ankurah_core_types::ValueType;
use ankurah_storage_common::{materialization_join::entity_id_bound, EngineColumns};
use std::collections::BTreeMap;
use thiserror::Error;

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
    pub sql_predicate: Predicate<Resolved>,
    /// Predicate that must be evaluated in Rust after fetching (Predicate::True if nothing remains)
    pub remaining_predicate: Predicate<Resolved>,
}

impl SplitPredicate {
    /// Check if there's any remaining predicate that needs post-filtering
    pub fn needs_post_filter(&self) -> bool { !matches!(self.remaining_predicate, Predicate::True) }
}

/// Split a predicate into parts that can be pushed down to SQLite vs evaluated post-fetch.
pub fn split_predicate_for_sqlite(predicate: &Predicate<Resolved>) -> SplitPredicate {
    let (sql_pred, remaining_pred) = split_predicate_recursive(predicate);
    SplitPredicate { sql_predicate: sql_pred, remaining_predicate: remaining_pred }
}

fn split_predicate_recursive(predicate: &Predicate<Resolved>) -> (Predicate<Resolved>, Predicate<Resolved>) {
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
        Predicate::MemberOf(_) => (predicate.clone(), Predicate::True),
        Predicate::Placeholder => (Predicate::True, predicate.clone()),
    }
}

fn can_pushdown_comparison(left: &Expr<Resolved>, right: &Expr<Resolved>) -> bool { can_pushdown_expr(left) && can_pushdown_expr(right) }

fn can_pushdown_expr(expr: &Expr<Resolved>) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Path(_) => true,
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
    order_types: BTreeMap<String, ValueType>,
}

impl Default for SqlBuilder {
    fn default() -> Self { Self::new() }
}

impl SqlBuilder {
    pub fn order_types(&mut self, types: BTreeMap<String, ValueType>) { self.order_types = types; }

    pub fn new() -> Self {
        Self { sql: String::new(), params: Vec::new(), fields: Vec::new(), table_name: None, order_types: BTreeMap::new() }
    }

    pub fn with_fields<T: Into<String>>(fields: Vec<T>) -> Self {
        Self {
            sql: String::new(),
            params: Vec::new(),
            fields: fields.into_iter().map(|f| f.into()).collect(),
            table_name: None,
            order_types: BTreeMap::new(),
        }
    }

    pub fn table_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.table_name = Some(name.into());
        self
    }

    pub(super) fn push_sql(&mut self, s: &str) { self.sql.push_str(s); }

    pub(super) fn push_param(&mut self, value: rusqlite::types::Value) {
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

    pub fn expr(&mut self, expr: &Expr<EngineColumns>) -> Result<(), SqlGenerationError> {
        match expr {
            Expr::Placeholder => return Err(SqlGenerationError::PlaceholderFound),
            Expr::Literal(lit) => self.literal(lit),
            Expr::Path(path) => {
                if path.is_simple() {
                    // Single-step path: regular column reference
                    let escaped = path.column.replace('"', "\"\"");
                    self.push_sql(&format!(r#""{}""#, escaped));
                } else {
                    // json_extract preserves scalar SQL types; -> always returns JSON text.
                    let first = path.column.replace('"', "\"\"");
                    // Build JSON path: $.step1.step2.step3
                    let json_path = if path.subpath.len() == 1 {
                        format!("$.{}", path.subpath[0].replace('\'', "''"))
                    } else {
                        format!("$.{}", path.subpath.iter().map(|s| s.replace('\'', "''")).collect::<Vec<_>>().join("."))
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

    fn literal(&mut self, lit: &Value) {
        match lit {
            Value::String(s) => self.push_param(rusqlite::types::Value::Text(s.clone())),
            Value::I64(i) => self.push_param(rusqlite::types::Value::Integer(*i)),
            Value::F64(f) => self.push_param(rusqlite::types::Value::Real(*f)),
            Value::Bool(b) => self.push_param(rusqlite::types::Value::Integer(if *b { 1 } else { 0 })),
            Value::I16(i) => self.push_param(rusqlite::types::Value::Integer(*i as i64)),
            Value::I32(i) => self.push_param(rusqlite::types::Value::Integer(*i as i64)),
            Value::EntityId(id) => self.push_param(rusqlite::types::Value::Text(id.to_base64())),
            Value::NodeId(id) => self.push_param(rusqlite::types::Value::Blob(id.to_bytes().to_vec())),
            Value::Object(bytes) => self.push_param(rusqlite::types::Value::Blob(bytes.clone())),
            Value::Binary(bytes) => self.push_param(rusqlite::types::Value::Blob(bytes.clone())),
            // For JSON literals, extract the raw SQL value since json_extract() returns SQL types.
            // json.to_string() would produce "US" (with quotes) but we need just US.
            Value::Json(json) => match json {
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

    pub fn predicate(&mut self, predicate: &Predicate<EngineColumns>) -> Result<(), SqlGenerationError> {
        if let Some((operator, id)) = Comparison::try_from(predicate).ok().and_then(entity_id_bound) {
            self.push_sql(id_bytes::SQL);
            self.push_sql(" ");
            self.comparison_op(&operator)?;
            self.push_sql(" ");
            self.push_param(rusqlite::types::Value::Blob(id.to_bytes().to_vec()));
            return Ok(());
        }
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
            Predicate::MemberOf(model) => {
                let column = ankurah_storage_common::materialization_join::membership_column(model);
                self.push_sql(&format!("\"{}\"", column.replace('"', "\"\"")));
            }
            Predicate::Placeholder => {
                return Err(SqlGenerationError::PlaceholderFound);
            }
        }
        Ok(())
    }

    pub fn selection(&mut self, selection: &Selection<EngineColumns>) -> Result<(), SqlGenerationError> {
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

    fn order_expression(&mut self, order_by: &OrderByItem<EngineColumns>) -> Result<(), SqlGenerationError> {
        if order_by.path.is_simple() && order_by.path.column == "id" {
            // In `MaterializationJoin`'s projection, `id` is the entity id.
            self.push_sql(id_bytes::SQL);
        } else if !order_by.path.subpath.is_empty() || self.order_types.get(&order_by.path.column) == Some(&ValueType::Json) {
            let column = order_by.path.column.replace('"', "\"\"");
            self.push_sql(&format!(r#"ankurah_json_order_key(json("{}"), "#, column));
            self.push_param(rusqlite::types::Value::Text(
                serde_json::to_string(&order_by.path.subpath).expect("JSON path strings serialize"),
            ));
            self.push_sql(")");
        } else {
            // Single-step path: regular column reference
            let escaped = order_by.path.column.replace('"', "\"\"");
            match self.order_types.get(&order_by.path.column) {
                Some(ValueType::EntityId) => self.push_sql(&format!(r#"ankurah_id_bytes("{}")"#, escaped)),
                _ => self.push_sql(&format!(r#""{}""#, escaped)),
            }
        }

        Ok(())
    }

    pub fn order_by_item(&mut self, order_by: &OrderByItem<EngineColumns>) -> Result<(), SqlGenerationError> {
        self.order_expression(order_by)?;
        match order_by.direction {
            OrderDirection::Asc => self.push_sql(" ASC NULLS LAST"),
            OrderDirection::Desc => self.push_sql(" DESC NULLS FIRST"),
        }

        Ok(())
    }
    /// Strictly after a captured tuple, using the same expressions and NULL
    /// placement as ORDER BY. This is deliberately separate from predicates.
    pub(super) fn after_order(&mut self, order: &[OrderByItem<EngineColumns>], values: &[Option<Value>]) -> Result<(), SqlGenerationError> {
        if order.len() != values.len() {
            return Err(SqlGenerationError::UnsupportedExpression("order boundary has the wrong number of values"));
        }
        let Some((part, rest)) = order.split_first() else {
            self.push_sql("FALSE");
            return Ok(());
        };
        let value = &values[0];
        self.push_sql("(");
        match (value, &part.direction) {
            (None, OrderDirection::Asc) => self.push_sql("FALSE"),
            (None, OrderDirection::Desc) => {
                self.order_expression(part)?;
                self.push_sql(" IS NOT NULL");
            }
            (Some(value), direction) => {
                self.push_sql("(");
                self.order_expression(part)?;
                self.push_sql(if *direction == OrderDirection::Asc { " > " } else { " < " });
                self.order_argument(part, value)?;
                // Entity identities are never NULL. Keep their comparison a
                // direct index bound instead of adding an impossible NULL arm.
                if *direction == OrderDirection::Asc && !(part.path.is_simple() && part.path.column == "id") {
                    self.push_sql(" OR ");
                    self.order_expression(part)?;
                    self.push_sql(" IS NULL");
                }
                self.push_sql(")");
            }
        }
        if !rest.is_empty() {
            self.push_sql(" OR (");
            self.order_expression(part)?;
            if let Some(value) = value {
                self.push_sql(" IS ");
                self.order_argument(part, value)?;
            } else {
                self.push_sql(" IS NULL");
            }
            self.push_sql(" AND ");
            self.after_order(rest, &values[1..])?;
            self.push_sql(")");
        }
        self.push_sql(")");
        Ok(())
    }

    fn order_argument(&mut self, part: &OrderByItem<EngineColumns>, value: &Value) -> Result<(), SqlGenerationError> {
        if part.path.subpath.is_empty()
            && (part.path.column == "id" || self.order_types.get(&part.path.column) == Some(&ValueType::EntityId))
        {
            let Value::EntityId(value) = value else {
                return Err(SqlGenerationError::UnsupportedExpression("an entity-id order boundary requires an entity id"));
            };
            self.push_param(rusqlite::types::Value::Blob(value.to_bytes().to_vec()));
            return Ok(());
        }
        if !part.path.subpath.is_empty() || self.order_types.get(&part.path.column) == Some(&ValueType::Json) {
            self.push_param(rusqlite::types::Value::Blob(ankurah_core_types::ordering::value_bytes(value)));
            return Ok(());
        }
        self.expr(&Expr::Literal(value.clone()))
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
    use ankql::selection::map_references;
    use ankurah_storage_common::ColumnPath;

    fn lowered(query: &str) -> Selection<EngineColumns> {
        map_references(&parse_selection(query).unwrap(), &|path| ColumnPath::new(path.first(), path.steps[1..].to_vec()), &|model| {
            *model.as_id().expect("model ID in physical-column fixture")
        })
    }

    #[test]
    fn test_simple_equality() {
        let selection = lowered("name = 'Alice'");
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, params) = sql.build_where_clause();

        assert_eq!(sql_string, r#""name" = ?"#);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_and_condition() {
        let selection = lowered("name = 'Alice' AND age = 30");
        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection).unwrap();
        let (sql_string, params) = sql.build().unwrap();

        assert_eq!(sql_string, r#"SELECT "id", "name", "age" FROM "users" WHERE "name" = ? AND "age" = ?"#);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_json_path() {
        let selection = lowered("data.status = 'active'");
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, _) = sql.build_where_clause();

        // Uses json_extract() for reliable comparisons with BLOB JSONB columns
        assert_eq!(sql_string, r#"json_extract("data", '$.status') = ?"#);
    }

    #[test]
    fn test_json_nested_path() {
        let selection = lowered("data.user.name = 'Alice'");
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, _) = sql.build_where_clause();

        // Uses json_extract() with nested path for reliable comparisons
        assert_eq!(sql_string, r#"json_extract("data", '$.user.name') = ?"#);
    }

    #[test]
    fn test_json_numeric_comparison() {
        let selection = lowered("data.count > 10");
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, _) = sql.build_where_clause();

        // Numeric comparison with json_extract() - SQLite handles numeric comparison correctly
        assert_eq!(sql_string, r#"json_extract("data", '$.count') > ?"#);
    }

    #[test]
    fn test_in_operator() {
        let selection = lowered("name IN ('Alice', 'Bob')");
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, params) = sql.build_where_clause();

        assert_eq!(sql_string, r#""name" IN (?, ?)"#);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn a_comparison_that_orders_the_entity_id_compares_bytes() {
        use ankurah_core_types::EntityId;
        use rusqlite::types::Value::Blob;
        let (low, high) = (EntityId::from_bytes([0x10; 32]), EntityId::from_bytes([0xF0; 32]));
        let compare = |operator, id| Predicate::Comparison {
            left: Box::new(Expr::Path(ColumnPath::simple("id"))),
            operator,
            right: Box::new(Expr::Literal(Value::EntityId(id))),
        };
        let predicate = Predicate::And(
            Box::new(compare(ComparisonOperator::GreaterThanOrEqual, low)),
            Box::new(compare(ComparisonOperator::LessThanOrEqual, high)),
        );
        let mut sql = SqlBuilder::new();
        sql.predicate(&predicate).unwrap();
        let (sql_string, params) = sql.build_where_clause();

        assert_eq!(sql_string, r#"ankurah_id_bytes("id") >= ? AND ankurah_id_bytes("id") <= ?"#);
        assert_eq!(params, vec![Blob(low.to_bytes().to_vec()), Blob(high.to_bytes().to_vec())]);
    }

    #[test]
    fn order_by_id_orders_by_the_ids_bytes() {
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![
                OrderByItem { path: ColumnPath::simple("id"), direction: OrderDirection::Desc },
                OrderByItem { path: ColumnPath::simple("p_name"), direction: OrderDirection::Asc },
            ]),
            limit: None,
        };
        let mut sql = SqlBuilder::new();
        sql.selection(&selection).unwrap();
        let (sql_string, _) = sql.build_where_clause();

        assert_eq!(sql_string, r#"1=1 ORDER BY ankurah_id_bytes("id") DESC NULLS FIRST, "p_name" ASC NULLS LAST"#);
    }
}
