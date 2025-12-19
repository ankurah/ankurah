use ankql::ast::{ComparisonOperator, Expr, Literal, OrderByItem, OrderDirection, Predicate, Selection};
use ankurah_core::{error::RetrievalError, EntityId};
use thiserror::Error;
use tokio_postgres::types::ToSql;

#[derive(Debug, Error, Clone)]
pub enum SqlGenerationError {
    #[error("Placeholder found in predicate - placeholders should be replaced before predicate processing")]
    PlaceholderFound,
    #[error("Unsupported expression type: {0}")]
    UnsupportedExpression(&'static str),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(&'static str),
    #[error("SqlBuilder requires both fields and table_name to be set for complete SELECT generation, or neither for WHERE-only mode")]
    IncompleteConfiguration,
}

/// Result of splitting a predicate for PostgreSQL execution.
///
/// "Pushdown" refers to moving predicate evaluation from the application layer
/// down to the database layer. Some predicates can be translated to SQL and
/// executed by PostgreSQL (pushdown), while others must be evaluated in Rust
/// after fetching results (e.g., future features like Ref traversal).
#[derive(Debug, Clone)]
pub struct SplitPredicate {
    /// Predicate that can be pushed down to PostgreSQL WHERE clause
    pub sql_predicate: Predicate,
    /// Predicate that must be evaluated in Rust after fetching (Predicate::True if nothing remains)
    pub remaining_predicate: Predicate,
}

impl SplitPredicate {
    /// Check if there's any remaining predicate that needs post-filtering
    pub fn needs_post_filter(&self) -> bool { !matches!(self.remaining_predicate, Predicate::True) }
}

/// Split a predicate into parts that can be pushed down to PostgreSQL vs evaluated post-fetch.
///
/// **Pushdown-capable** (translated to SQL):
/// - Simple column comparisons (single-step paths like `name = 'value'`)
/// - JSONB path comparisons (multi-step paths like `data.field = 'value'`)
/// - AND/OR/NOT combinations of pushdown-capable predicates
/// - IS NULL, TRUE, FALSE
///
/// **Requires post-filtering** (evaluated in Rust):
/// - Future: Ref traversals, complex expressions
pub fn split_predicate_for_postgres(predicate: &Predicate) -> SplitPredicate {
    // Walk the predicate tree and classify each leaf comparison.
    // If ANY part of an OR branch can't be pushed down, the whole OR must be post-filtered.
    // For AND, we can split: pushdown what we can, post-filter the rest.

    let (sql_pred, remaining_pred) = split_predicate_recursive(predicate);

    SplitPredicate { sql_predicate: sql_pred, remaining_predicate: remaining_pred }
}

/// Recursively split a predicate into (pushdown, remaining) parts.
fn split_predicate_recursive(predicate: &Predicate) -> (Predicate, Predicate) {
    match predicate {
        // Leaf predicates - check if they support pushdown
        Predicate::Comparison { left, operator: _, right } => {
            if can_pushdown_comparison(left, right) {
                (predicate.clone(), Predicate::True)
            } else {
                // Can't pushdown - keep for post-filter
                (Predicate::True, predicate.clone())
            }
        }

        // AND: can split - pushdown what we can, keep the rest
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

        // OR: if any branch can't be fully pushed down, keep the whole OR for post-filter
        // (but still pushdown what we can to reduce row count)
        Predicate::Or(left, right) => {
            let (left_sql, left_remaining) = split_predicate_recursive(left);
            let (right_sql, right_remaining) = split_predicate_recursive(right);

            // If both branches fully support pushdown, pushdown the whole OR
            if matches!(left_remaining, Predicate::True) && matches!(right_remaining, Predicate::True) {
                (predicate.clone(), Predicate::True)
            } else {
                // Partial pushdown - still send what we can to reduce rows,
                // but must also post-filter with the full OR
                let sql_pred = match (&left_sql, &right_sql) {
                    (Predicate::True, Predicate::True) => Predicate::True,
                    (Predicate::True, _) => right_sql,
                    (_, Predicate::True) => left_sql,
                    _ => Predicate::Or(Box::new(left_sql), Box::new(right_sql)),
                };
                (sql_pred, predicate.clone())
            }
        }

        // NOT: pushdown if inner supports pushdown
        Predicate::Not(inner) => {
            let (inner_sql, inner_remaining) = split_predicate_recursive(inner);
            if matches!(inner_remaining, Predicate::True) {
                (Predicate::Not(Box::new(inner_sql)), Predicate::True)
            } else {
                // Can't pushdown the NOT - keep whole thing for post-filter
                (Predicate::True, predicate.clone())
            }
        }

        // IS NULL - pushdown if expression supports pushdown
        Predicate::IsNull(expr) => {
            if can_pushdown_expr(expr) {
                (predicate.clone(), Predicate::True)
            } else {
                (Predicate::True, predicate.clone())
            }
        }

        Predicate::True => (Predicate::True, Predicate::True),
        Predicate::False => (Predicate::False, Predicate::True),
        Predicate::Placeholder => (Predicate::True, predicate.clone()), // Shouldn't happen, but be safe
    }
}

/// Check if a comparison can be pushed down to PostgreSQL.
fn can_pushdown_comparison(left: &Expr, right: &Expr) -> bool { can_pushdown_expr(left) && can_pushdown_expr(right) }

/// Check if an expression can be pushed down to PostgreSQL SQL.
///
/// Returns true if the expression can be translated to valid PostgreSQL syntax.
/// Currently supports:
/// - Literals (strings, numbers, booleans, etc.)
/// - Simple column paths (`name`) - regular column reference
/// - Multi-step paths (`data.field`) - JSONB traversal via `->` and `->>`
/// - Expression lists (for IN clauses)
///
/// NOT pushdown-capable (will be post-filtered in Rust):
/// - Nested predicates as expressions
/// - Infix expressions (not yet implemented)
/// - Placeholders (should be replaced before we get here)
///
/// HACK: We currently infer "JSON property" from multi-step paths. This works for Phase 1
/// where only Json properties support nested traversal.
///
/// TODO(Phase 3 - Schema Registry): Once we have property type metadata, we can:
/// 1. Know definitively if a path traverses a Json property vs Ref<T>
/// 2. Ref<T> traversal will NOT be pushable (requires entity joins)
/// 3. Distinguish Json traversal from Ref<T> traversal based on schema
fn can_pushdown_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Path(path) => {
            // All paths are currently pushdown-capable:
            // - Single-step: regular column reference
            // - Multi-step: JSONB traversal (inferred as Json property for now)
            //
            // HACK: We assume multi-step paths are Json properties.
            // TODO(Phase 3 - Schema Registry): Check property type to distinguish
            // Json traversal (pushable) from Ref<T> traversal (not pushable).
            !path.steps.is_empty()
        }
        Expr::ExprList(exprs) => exprs.iter().all(can_pushdown_expr),
        Expr::Predicate(_) => false,     // Nested predicates - not supported in SQL expressions
        Expr::InfixExpr { .. } => false, // Not yet supported
        Expr::Placeholder => false,      // Should be replaced before we get here
    }
}

impl From<SqlGenerationError> for RetrievalError {
    fn from(err: SqlGenerationError) -> Self { RetrievalError::StorageError(Box::new(err)) }
}

pub enum SqlExpr {
    Sql(String),
    Argument(Box<dyn ToSql + Send + Sync>),
}

pub struct SqlBuilder {
    expressions: Vec<SqlExpr>,
    fields: Vec<String>,
    table_name: Option<String>,
}

impl Default for SqlBuilder {
    fn default() -> Self { Self::new() }
}

impl SqlBuilder {
    pub fn new() -> Self { Self { expressions: Vec::new(), fields: Vec::new(), table_name: None } }

    pub fn with_fields<T: Into<String>>(fields: Vec<T>) -> Self {
        Self { expressions: Vec::new(), fields: fields.into_iter().map(|f| f.into()).collect(), table_name: None }
    }

    pub fn table_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.table_name = Some(name.into());
        self
    }

    pub fn push(&mut self, expr: SqlExpr) { self.expressions.push(expr); }

    pub fn arg(&mut self, arg: impl ToSql + Send + Sync + 'static) {
        self.push(SqlExpr::Argument(Box::new(arg) as Box<dyn ToSql + Send + Sync>));
    }

    pub fn sql(&mut self, s: impl AsRef<str>) { self.push(SqlExpr::Sql(s.as_ref().to_owned())); }

    pub fn build(self) -> Result<(String, Vec<Box<dyn ToSql + Send + Sync>>), SqlGenerationError> {
        let mut counter = 1;
        let mut where_clause = String::new();
        let mut args = Vec::new();

        // Build WHERE clause from expressions
        for expr in self.expressions {
            match expr {
                SqlExpr::Argument(arg) => {
                    where_clause += &format!("${}", counter);
                    args.push(arg);
                    counter += 1;
                }
                SqlExpr::Sql(s) => {
                    where_clause += &s;
                }
            }
        }

        // Build complete SELECT statement - fields and table are required
        if self.fields.is_empty() || self.table_name.is_none() {
            return Err(SqlGenerationError::IncompleteConfiguration);
        }

        let fields_clause = self.fields.iter().map(|field| format!(r#""{}""#, field.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
        let table = self.table_name.unwrap();
        let sql = format!(r#"SELECT {} FROM "{}" WHERE {}"#, fields_clause, table.replace('"', "\"\""), where_clause);

        Ok((sql, args))
    }

    pub fn build_where_clause(self) -> (String, Vec<Box<dyn ToSql + Send + Sync>>) {
        let mut counter = 1;
        let mut where_clause = String::new();
        let mut args = Vec::new();

        // Build WHERE clause from expressions
        for expr in self.expressions {
            match expr {
                SqlExpr::Argument(arg) => {
                    where_clause += &format!("${}", counter);
                    args.push(arg);
                    counter += 1;
                }
                SqlExpr::Sql(s) => {
                    where_clause += &s;
                }
            }
        }

        (where_clause, args)
    }

    // --- AST flattening ---
    pub fn expr(&mut self, expr: &Expr) -> Result<(), SqlGenerationError> {
        match expr {
            Expr::Placeholder => return Err(SqlGenerationError::PlaceholderFound),
            Expr::Literal(lit) => match lit {
                Literal::String(s) => self.arg(s.to_owned()),
                Literal::I64(int) => self.arg(*int),
                Literal::F64(float) => self.arg(*float),
                Literal::Bool(bool) => self.arg(*bool),
                Literal::I16(i) => self.arg(*i),
                Literal::I32(i) => self.arg(*i),
                Literal::EntityId(ulid) => self.arg(EntityId::from_ulid(*ulid).to_base64()),
                Literal::Object(bytes) => self.arg(bytes.clone()),
                Literal::Binary(bytes) => self.arg(bytes.clone()),
            },
            Expr::Path(path) => {
                if path.is_simple() {
                    // Single-step path: regular column reference "column_name"
                    let escaped = path.first().replace('"', "\"\"");
                    self.sql(format!(r#""{}""#, escaped));
                } else {
                    // Multi-step path: JSONB traversal "column"->'nested'->'path'
                    // Use -> for ALL steps to preserve JSONB type for proper comparison semantics.
                    // The comparison will use ::jsonb cast on literals to ensure type-aware comparison.
                    let first = path.first().replace('"', "\"\"");
                    self.sql(format!(r#""{}""#, first));

                    for step in path.steps.iter().skip(1) {
                        let escaped = step.replace('\'', "''");
                        // Always use -> to keep as JSONB (not ->> which extracts as text)
                        self.sql(format!("->'{}'", escaped));
                    }
                }
            }
            Expr::ExprList(exprs) => {
                self.sql("(");
                for (i, expr) in exprs.iter().enumerate() {
                    if i > 0 {
                        self.sql(", ");
                    }
                    match expr {
                        Expr::Placeholder => return Err(SqlGenerationError::PlaceholderFound),
                        Expr::Literal(lit) => match lit {
                            Literal::String(s) => self.arg(s.to_owned()),
                            Literal::I64(int) => self.arg(*int),
                            Literal::F64(float) => self.arg(*float),
                            Literal::Bool(bool) => self.arg(*bool),
                            Literal::I16(i) => self.arg(*i),
                            Literal::I32(i) => self.arg(*i),
                            Literal::EntityId(ulid) => self.arg(EntityId::from_ulid(*ulid).to_base64()),
                            Literal::Object(bytes) => self.arg(bytes.clone()),
                            Literal::Binary(bytes) => self.arg(bytes.clone()),
                        },
                        _ => {
                            return Err(SqlGenerationError::UnsupportedExpression(
                                "Only literal expressions and placeholders are supported in IN lists",
                            ))
                        }
                    }
                }
                self.sql(")");
            }
            _ => return Err(SqlGenerationError::UnsupportedExpression("Only literal, identifier, and list expressions are supported")),
        }
        Ok(())
    }

    /// Emit a literal expression with ::jsonb cast for proper JSONB comparison semantics.
    /// This ensures that comparisons like `"data"->'count' > '10'::jsonb` work correctly
    /// with PostgreSQL's type-aware JSONB comparison (numeric vs lexicographic).
    pub fn expr_as_jsonb(&mut self, expr: &Expr) -> Result<(), SqlGenerationError> {
        match expr {
            Expr::Literal(lit) => {
                // For literals, we need to cast to jsonb
                // PostgreSQL will compare jsonb values with proper type semantics
                match lit {
                    Literal::String(s) => {
                        // String literals need to be JSON strings: '"value"'::jsonb
                        // Escape for JSON (backslash and quote) then for SQL (single quotes)
                        let json_escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                        let sql_escaped = format!("\"{}\"", json_escaped).replace('\'', "''");
                        self.sql(format!("'{}'::jsonb", sql_escaped));
                    }
                    Literal::I64(n) => self.sql(format!("'{}'::jsonb", n)),
                    Literal::F64(n) => self.sql(format!("'{}'::jsonb", n)),
                    Literal::Bool(b) => self.sql(format!("'{}'::jsonb", b)),
                    Literal::I16(n) => self.sql(format!("'{}'::jsonb", n)),
                    Literal::I32(n) => self.sql(format!("'{}'::jsonb", n)),
                    // EntityId and binary types don't make sense as JSONB
                    Literal::EntityId(_) | Literal::Object(_) | Literal::Binary(_) => {
                        // Fall back to regular expression (will likely fail comparison, but that's correct)
                        self.expr(expr)?;
                    }
                }
                Ok(())
            }
            // For non-literals, just emit normally (they're already JSONB paths or complex expressions)
            _ => self.expr(expr),
        }
    }

    pub fn comparison_op(&mut self, op: &ComparisonOperator) -> Result<(), SqlGenerationError> {
        self.sql(comparison_op_to_sql(op)?);
        Ok(())
    }

    pub fn predicate(&mut self, predicate: &Predicate) -> Result<(), SqlGenerationError> {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                // Check if either side is a JSONB path (multi-step path)
                let left_is_jsonb = matches!(left.as_ref(), Expr::Path(p) if !p.is_simple());
                let right_is_jsonb = matches!(right.as_ref(), Expr::Path(p) if !p.is_simple());

                self.expr(left)?;
                self.sql(" ");
                self.comparison_op(operator)?;
                self.sql(" ");

                if left_is_jsonb && matches!(right.as_ref(), Expr::Literal(_)) {
                    // Comparing JSONB path to literal: cast literal to jsonb
                    self.expr_as_jsonb(right)?;
                } else if right_is_jsonb && matches!(left.as_ref(), Expr::Literal(_)) {
                    // Comparing literal to JSONB path: cast literal to jsonb
                    self.expr_as_jsonb(right)?;
                } else {
                    self.expr(right)?;
                }
            }
            Predicate::And(left, right) => {
                self.predicate(left)?;
                self.sql(" AND ");
                self.predicate(right)?;
            }
            Predicate::Or(left, right) => {
                self.sql("(");
                self.predicate(left)?;
                self.sql(" OR ");
                self.predicate(right)?;
                self.sql(")");
            }
            Predicate::Not(pred) => {
                self.sql("NOT (");
                self.predicate(pred)?;
                self.sql(")");
            }
            Predicate::IsNull(expr) => {
                self.expr(expr)?;
                self.sql(" IS NULL");
            }
            Predicate::True => {
                self.sql("TRUE");
            }
            Predicate::False => {
                self.sql("FALSE");
            }
            Predicate::Placeholder => {
                return Err(SqlGenerationError::PlaceholderFound);
            }
        }
        Ok(())
    }

    pub fn selection(&mut self, selection: &Selection) -> Result<(), SqlGenerationError> {
        // Add the predicate (WHERE clause)
        self.predicate(&selection.predicate)?;

        // Add ORDER BY clause if present
        if let Some(order_by_items) = &selection.order_by {
            self.sql(" ORDER BY ");
            for (i, order_by) in order_by_items.iter().enumerate() {
                if i > 0 {
                    self.sql(", ");
                }
                self.order_by_item(order_by)?;
            }
        }

        // Add LIMIT clause if present
        if let Some(limit) = selection.limit {
            self.sql(" LIMIT ");
            self.arg(limit as i64); // PostgreSQL expects i64 for LIMIT
        }

        Ok(())
    }

    pub fn order_by_item(&mut self, order_by: &OrderByItem) -> Result<(), SqlGenerationError> {
        // Generate the path expression
        for (i, step) in order_by.path.steps.iter().enumerate() {
            if i > 0 {
                self.sql(".");
            }
            // Escape any existing quotes in the step by doubling them
            let escaped_step = step.replace('"', "\"\"");
            self.sql(format!(r#""{}""#, escaped_step));
        }

        // Add the direction
        match order_by.direction {
            OrderDirection::Asc => self.sql(" ASC"),
            OrderDirection::Desc => self.sql(" DESC"),
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
    use anyhow::Result;

    fn assert_args<'a, 'b>(args: &Vec<Box<dyn ToSql + Send + Sync>>, expected: &Vec<Box<dyn ToSql + Send + Sync>>) {
        // TODO: Maybe actually encoding these and comparing bytes?
        assert_eq!(format!("{:?}", args), format!("{:?}", expected));
    }

    #[test]
    fn test_simple_equality() -> Result<()> {
        let selection = parse_selection("name = 'Alice'").unwrap();
        let mut sql = SqlBuilder::new();
        sql.selection(&selection)?;

        let (sql_string, args) = sql.build_where_clause();
        assert_eq!(sql_string, r#""name" = $1"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_and_condition() -> Result<()> {
        let selection = parse_selection("name = 'Alice' AND age = 30").unwrap();
        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, r#"SELECT "id", "name", "age" FROM "users" WHERE "name" = $1 AND "age" = $2"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new(30)];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_complex_condition() -> Result<()> {
        let selection = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= 30 AND age <= 40").unwrap();

        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(
            sql_string,
            r#"SELECT "id", "name", "age" FROM "users" WHERE ("name" = $1 OR "name" = $2) AND "age" >= $3 AND "age" <= $4"#
        );
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new("Charlie"), Box::new(30), Box::new(40)];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_including_collection_identifier() -> Result<()> {
        // Tests multi-step path SQL generation using JSONB operators.
        // HACK: We infer "JSON property" from multi-step paths (e.g., `person.name`).
        // TODO(Phase 3 - Schema Registry): With property metadata, we can distinguish
        // Json traversal from Ref<T> traversal and generate appropriate SQL.
        let selection = parse_selection("person.name = 'Alice'").unwrap();

        let mut sql = SqlBuilder::with_fields(vec!["id", "name"]);
        sql.table_name("people");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        // Multi-step paths generate JSONB syntax: -> with ::jsonb cast for proper comparison
        assert_eq!(sql_string, r#"SELECT "id", "name" FROM "people" WHERE "person"->'name' = '"Alice"'::jsonb"#);
        // No args - the value is inlined as ::jsonb cast
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_false_predicate() -> Result<()> {
        let mut sql = SqlBuilder::with_fields(vec!["id"]);
        sql.table_name("test");
        sql.predicate(&Predicate::False)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, r#"SELECT "id" FROM "test" WHERE FALSE"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_in_operator() -> Result<()> {
        let selection = parse_selection("name IN ('Alice', 'Bob', 'Charlie')").unwrap();
        let mut sql = SqlBuilder::with_fields(vec!["id", "name"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, r#"SELECT "id", "name" FROM "users" WHERE "name" IN ($1, $2, $3)"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new("Bob"), Box::new("Charlie")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_placeholder_error() {
        let mut sql = SqlBuilder::with_fields(vec!["id"]);
        sql.table_name("test");
        let err = sql.predicate(&Predicate::Placeholder).expect_err("Expected an error");
        assert!(matches!(err, SqlGenerationError::PlaceholderFound));
    }

    #[test]
    fn test_selection_with_order_by() -> Result<()> {
        use ankql::ast::{OrderByItem, OrderDirection, PathExpr, Selection};

        let base_selection = ankql::parser::parse_selection("name = 'Alice'").unwrap();
        let selection = Selection {
            predicate: base_selection.predicate,
            order_by: Some(vec![OrderByItem { path: PathExpr::simple("created_at"), direction: OrderDirection::Desc }]),
            limit: None,
        };

        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "created_at"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, r#"SELECT "id", "name", "created_at" FROM "users" WHERE "name" = $1 ORDER BY "created_at" DESC"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_selection_with_limit() -> Result<()> {
        let base_selection = ankql::parser::parse_selection("age > 18").unwrap();
        let selection = Selection { predicate: base_selection.predicate, order_by: None, limit: Some(10) };

        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, r#"SELECT "id", "name", "age" FROM "users" WHERE "age" > $1 LIMIT $2"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new(18i64), Box::new(10i64)];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_selection_with_order_by_and_limit() -> Result<()> {
        use ankql::ast::{OrderByItem, OrderDirection, PathExpr, Selection};

        let base_selection = ankql::parser::parse_selection("status = 'active'").unwrap();
        let selection = Selection {
            predicate: base_selection.predicate,
            order_by: Some(vec![
                OrderByItem { path: PathExpr::simple("priority"), direction: OrderDirection::Desc },
                OrderByItem { path: PathExpr::simple("created_at"), direction: OrderDirection::Asc },
            ]),
            limit: Some(5),
        };

        let mut sql = SqlBuilder::with_fields(vec!["id", "status", "priority", "created_at"]);
        sql.table_name("tasks");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(
            sql_string,
            r#"SELECT "id", "status", "priority", "created_at" FROM "tasks" WHERE "status" = $1 ORDER BY "priority" DESC, "created_at" ASC LIMIT $2"#
        );
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("active"), Box::new(5i64)];
        assert_args(&args, &expected);
        Ok(())
    }

    // ============================================================================
    // JSONB SQL Generation Tests
    // These verify that multi-step paths generate correct PostgreSQL JSONB syntax.
    //
    // Key design decision: Use -> (not ->>) with ::jsonb cast on literals.
    // This ensures PostgreSQL's type-aware JSONB comparison:
    // - Numeric comparisons are numeric (not lexicographic)
    // - Cross-type comparisons return false (e.g., 9::jsonb != '"9"'::jsonb)
    // ============================================================================
    mod jsonb_sql_tests {
        use super::*;
        use ankql::ast::PathExpr;

        #[test]
        fn test_two_step_json_path() -> Result<()> {
            // licensing.territory = 'US' should use -> and ::jsonb cast
            let selection = parse_selection("licensing.territory = 'US'").unwrap();
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            // String literal becomes '"US"'::jsonb (JSON string)
            assert_eq!(sql_string, r#""licensing"->'territory' = '"US"'::jsonb"#);
            Ok(())
        }

        #[test]
        fn test_three_step_json_path() -> Result<()> {
            // licensing.rights.holder should become "licensing"->'rights'->'holder'
            let selection = parse_selection("licensing.rights.holder = 'Label'").unwrap();
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, r#""licensing"->'rights'->'holder' = '"Label"'::jsonb"#);
            Ok(())
        }

        #[test]
        fn test_four_step_json_path() -> Result<()> {
            // a.b.c.d should become "a"->'b'->'c'->'d'
            let selection = parse_selection("a.b.c.d = 'value'").unwrap();
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, r#""a"->'b'->'c'->'d' = '"value"'::jsonb"#);
            Ok(())
        }

        #[test]
        fn test_json_path_with_numeric_comparison() -> Result<()> {
            // Using -> with ::jsonb ensures proper numeric comparison:
            // - "data"->'count' returns JSONB number
            // - '10'::jsonb is JSONB number
            // - JSONB numeric comparison is numeric (9 < 10), not lexicographic ("9" > "10")
            let selection = parse_selection("data.count > 10").unwrap();
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, r#""data"->'count' > '10'::jsonb"#);
            Ok(())
        }

        #[test]
        fn test_mixed_simple_and_json_paths() -> Result<()> {
            // name = 'test' AND data.status = 'active'
            // Simple path uses $1, JSON path uses ::jsonb cast
            let selection = parse_selection("name = 'test' AND data.status = 'active'").unwrap();
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, r#""name" = $1 AND "data"->'status' = '"active"'::jsonb"#);
            Ok(())
        }

        #[test]
        fn test_json_path_escaping() -> Result<()> {
            // Field with quote in path step - should escape properly
            // Note: This tests the SQL escaping, not JSON key escaping
            let mut sql = SqlBuilder::new();
            let path = PathExpr { steps: vec!["data".to_string(), "it's".to_string()] };
            sql.expr(&Expr::Path(path))?;
            let (sql_string, _) = sql.build_where_clause();

            // Just the path, no comparison - still uses ->
            assert_eq!(sql_string, r#""data"->'it''s'"#);
            Ok(())
        }

        #[test]
        fn test_json_path_with_boolean() -> Result<()> {
            let selection = parse_selection("data.active = true").unwrap();
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, r#""data"->'active' = 'true'::jsonb"#);
            Ok(())
        }

        #[test]
        fn test_json_path_with_float() -> Result<()> {
            // Note: AnkQL parser may parse this as i64, but the principle stands
            let selection = parse_selection("data.score >= 95").unwrap();
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, r#""data"->'score' >= '95'::jsonb"#);
            Ok(())
        }
    }

    // ============================================================================
    // Predicate Split Tests
    // These verify that split_predicate_for_postgres correctly classifies predicates
    // ============================================================================
    mod predicate_split_tests {
        use super::*;

        #[test]
        fn test_simple_predicate_fully_pushable() {
            let selection = parse_selection("name = 'Alice'").unwrap();
            let split = split_predicate_for_postgres(&selection.predicate);

            // Simple predicate should be fully pushable
            assert!(!split.needs_post_filter());
            assert!(matches!(split.remaining_predicate, Predicate::True));
        }

        #[test]
        fn test_json_path_predicate_pushable() {
            // Multi-step paths ARE pushed down using JSONB operators.
            // HACK: We infer "JSON property" from multi-step paths.
            // TODO(Phase 3 - Schema Registry): Once we have property metadata,
            // we can distinguish Json traversal (pushable) from Ref<T> (not pushable).
            let selection = parse_selection("licensing.territory = 'US'").unwrap();
            let split = split_predicate_for_postgres(&selection.predicate);

            // JSON path IS pushable via JSONB syntax
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_and_with_all_pushable() {
            let selection = parse_selection("name = 'test' AND licensing.status = 'active'").unwrap();
            let split = split_predicate_for_postgres(&selection.predicate);

            // Both parts pushable (simple path + JSON path) = whole thing pushable
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_or_with_all_pushable() {
            let selection = parse_selection("name = 'a' OR name = 'b'").unwrap();
            let split = split_predicate_for_postgres(&selection.predicate);

            // Both branches pushable = whole OR pushable
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_complex_nested_predicate() {
            let selection = parse_selection("(name = 'test' OR data.type = 'special') AND status = 'active'").unwrap();
            let split = split_predicate_for_postgres(&selection.predicate);

            // All parts are pushable (simple paths + JSON paths)
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_not_predicate_pushable() {
            let selection = parse_selection("NOT (status = 'deleted')").unwrap();
            let split = split_predicate_for_postgres(&selection.predicate);

            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_is_null_pushable() {
            let selection = parse_selection("name IS NULL").unwrap();
            let split = split_predicate_for_postgres(&selection.predicate);

            assert!(!split.needs_post_filter());
        }

        // Test for future: when we have unpushable predicates (e.g., Ref traversal)
        // #[test]
        // fn test_unpushable_predicate_goes_to_remaining() {
        //     // When we add Ref traversal, this test would verify:
        //     // let selection = parse_selection("artist.name = 'Radiohead'").unwrap();
        //     // let split = split_predicate_for_postgres(&selection.predicate);
        //     // assert!(split.needs_post_filter());
        //     // assert!(matches!(split.sql_predicate, Predicate::True));
        // }
    }
}
