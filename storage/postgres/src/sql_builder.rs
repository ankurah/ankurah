use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core::error::RetrievalError;
use ankurah_core_types::Value;
use ankurah_storage_common::EngineColumns;
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
    pub sql_predicate: Predicate<Resolved>,
    /// Predicate that must be evaluated in Rust after fetching (Predicate::True if nothing remains)
    pub remaining_predicate: Predicate<Resolved>,
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
pub fn split_predicate_for_postgres(predicate: &Predicate<Resolved>) -> SplitPredicate {
    // Walk the predicate tree and classify each leaf comparison.
    // If ANY part of an OR branch can't be pushed down, the whole OR must be post-filtered.
    // For AND, we can split: pushdown what we can, post-filter the rest.

    let (sql_pred, remaining_pred) = split_predicate_recursive(predicate);

    SplitPredicate { sql_predicate: sql_pred, remaining_predicate: remaining_pred }
}

/// Recursively split a predicate into (pushdown, remaining) parts.
fn split_predicate_recursive(predicate: &Predicate<Resolved>) -> (Predicate<Resolved>, Predicate<Resolved>) {
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
fn can_pushdown_comparison(left: &Expr<Resolved>, right: &Expr<Resolved>) -> bool { can_pushdown_expr(left) && can_pushdown_expr(right) }

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
fn can_pushdown_expr(expr: &Expr<Resolved>) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Path(_) => {
            // All resolved references are pushdown-capable:
            // - No sub-path: regular column reference
            // - Sub-path: JSONB traversal (inferred as Json property for now)
            //
            // HACK: We assume sub-paths are Json properties.
            // TODO(Phase 3 - Schema Registry): Check property type to distinguish
            // Json traversal (pushable) from Ref<T> traversal (not pushable).
            true
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
    pub fn expr(&mut self, expr: &Expr<EngineColumns>) -> Result<(), SqlGenerationError> {
        match expr {
            Expr::Placeholder => return Err(SqlGenerationError::PlaceholderFound),
            Expr::Literal(lit) => match lit {
                Value::String(s) => self.arg(s.to_owned()),
                Value::I64(int) => self.arg(*int),
                Value::F64(float) => self.arg(*float),
                Value::Bool(bool) => self.arg(*bool),
                Value::I16(i) => self.arg(*i),
                Value::I32(i) => self.arg(*i),
                Value::EntityId(id) => self.arg(id.to_base64()),
                Value::Object(bytes) => self.arg(bytes.clone()),
                Value::Binary(bytes) => self.arg(bytes.clone()),
                Value::Json(json) => self.arg(json.clone()),
            },
            Expr::Path(path) => {
                // Column names are always quoted (a property id's rendering is
                // URL-safe base64, which contains '-' and '_').
                let column = path.column.replace('"', "\"\"");
                self.sql(format!(r#""{}""#, column));
                // Sub-path: JSONB traversal "column"->'nested'->'path'.
                // Use -> for ALL steps to preserve JSONB type for proper
                // comparison semantics (literals get ::jsonb casts).
                for step in &path.subpath {
                    let escaped = step.replace('\'', "''");
                    // Always use -> to keep as JSONB (not ->> which extracts as text)
                    self.sql(format!("->'{}'", escaped));
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
                            Value::String(s) => self.arg(s.to_owned()),
                            Value::I64(int) => self.arg(*int),
                            Value::F64(float) => self.arg(*float),
                            Value::Bool(bool) => self.arg(*bool),
                            Value::I16(i) => self.arg(*i),
                            Value::I32(i) => self.arg(*i),
                            Value::EntityId(id) => self.arg(id.to_base64()),
                            Value::Object(bytes) => self.arg(bytes.clone()),
                            Value::Binary(bytes) => self.arg(bytes.clone()),
                            Value::Json(json) => self.arg(json.clone()),
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

    pub fn comparison_op(&mut self, op: &ComparisonOperator) -> Result<(), SqlGenerationError> {
        self.sql(comparison_op_to_sql(op)?);
        Ok(())
    }

    pub fn predicate(&mut self, predicate: &Predicate<EngineColumns>) -> Result<(), SqlGenerationError> {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                // A literal compared against a JSONB traversal arrives as a
                // canonicalized Value::Json (origin resolution types subpath
                // comparisons as Json), and Json args bind as jsonb
                // parameters -- so both sides of `"col"->'k' = $N` compare
                // with PostgreSQL's type-aware jsonb semantics. No inline
                // cast form exists anymore; it was dead for resolved inputs
                // before the stage collapse and wrongly resurrected by it.
                self.expr(left)?;
                self.sql(" ");
                self.comparison_op(operator)?;
                self.sql(" ");
                self.expr(right)?;
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

    pub fn selection(&mut self, selection: &Selection<EngineColumns>) -> Result<(), SqlGenerationError> {
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

    pub fn order_by_item(&mut self, order_by: &OrderByItem<EngineColumns>) -> Result<(), SqlGenerationError> {
        // The sort column is quoted; sub-path steps address into a JSONB
        // column.
        let column = order_by.path.column.replace('"', "\"\"");
        self.sql(format!(r#""{}""#, column));
        for step in &order_by.path.subpath {
            let escaped_step = step.replace('"', "\"\"");
            self.sql(format!(r#"."{}""#, escaped_step));
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
    use ankurah_core::schema::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty};
    use ankurah_proto::{EntityId, ModelId, PropertyId};
    use anyhow::Result;

    /// Stands in for the catalog: the builder consumes RESOLVED selections
    /// (fetch binds every name to a PropertyId before the engine sees it),
    /// so these shape tests resolve through a fixture first. Ids forge from
    /// the name's own bytes, and `col` rebuilds the rendering for the
    /// expected SQL.
    struct FixtureResolver;

    fn pid(name: &str) -> PropertyId {
        let mut bytes = [0u8; 32];
        for (i, byte) in name.bytes().take(32).enumerate() {
            bytes[i] = byte;
        }
        PropertyId::EntityId(EntityId::from_bytes(bytes))
    }

    fn col(name: &str) -> String { pid(name).to_string() }

    impl ModelResolver for FixtureResolver {
        fn resolve_property(&self, _model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            // `age` is compared numerically in these tests; everything else
            // is a string. (Subpath comparisons type as Json inside the walk
            // and never consult this.)
            let value_type = if name == "age" { ankurah_core::value::ValueType::I64 } else { ankurah_core::value::ValueType::String };
            Ok(Some(ResolvedProperty { id: pid(name), value_type }))
        }
    }

    /// Bind a query's names, as the query boundary does before an engine
    /// sees it. This is the stage the predicate split reads.
    fn resolved(query: &str) -> ankql::ast::Selection<Resolved> {
        let model = ModelId::EntityId(EntityId::from_bytes([0x77; 32]));
        resolve_selection(&model, &FixtureResolver, parse_selection(query).unwrap()).unwrap()
    }

    /// Resolve a query and lower it into this engine's columns, the way
    /// `fetch_states` does before it builds SQL.
    fn lowered(query: &str) -> ankql::ast::Selection<EngineColumns> { crate::lower::lower(&resolved(query)) }

    /// The column a test name is stored under, as a whole-column path.
    fn cpath(name: &str) -> ankurah_storage_common::ColumnPath { ankurah_storage_common::ColumnPath::simple(col(name)) }

    fn assert_args<'a, 'b>(args: &Vec<Box<dyn ToSql + Send + Sync>>, expected: &Vec<Box<dyn ToSql + Send + Sync>>) {
        // TODO: Maybe actually encoding these and comparing bytes?
        assert_eq!(format!("{:?}", args), format!("{:?}", expected));
    }

    #[test]
    fn test_simple_equality() -> Result<()> {
        let selection = lowered("name = 'Alice'");
        let mut sql = SqlBuilder::new();
        sql.selection(&selection)?;

        let (sql_string, args) = sql.build_where_clause();
        assert_eq!(sql_string, format!(r#""{}" = $1"#, col("name")));
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_and_condition() -> Result<()> {
        let selection = lowered("name = 'Alice' AND age = 30");
        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(
            sql_string,
            format!(r#"SELECT "id", "name", "age" FROM "users" WHERE "{}" = $1 AND "{}" = $2"#, col("name"), col("age"))
        );
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new(30)];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_complex_condition() -> Result<()> {
        let selection = lowered("(name = 'Alice' OR name = 'Charlie') AND age >= 30 AND age <= 40");

        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(
            sql_string,
            format!(
                r#"SELECT "id", "name", "age" FROM "users" WHERE ("{n}" = $1 OR "{n}" = $2) AND "{a}" >= $3 AND "{a}" <= $4"#,
                n = col("name"),
                a = col("age")
            )
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
        let selection = lowered("person.name = 'Alice'");

        let mut sql = SqlBuilder::with_fields(vec!["id", "name"]);
        sql.table_name("people");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        // Multi-step paths generate JSONB syntax: -> traversal with the
        // canonicalized Json literal bound as a jsonb parameter.
        assert_eq!(sql_string, format!(r#"SELECT "id", "name" FROM "people" WHERE "{}"->'name' = $1"#, col("person")));
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new(serde_json::json!("Alice"))];
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
        let selection = lowered("name IN ('Alice', 'Bob', 'Charlie')");
        let mut sql = SqlBuilder::with_fields(vec!["id", "name"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, format!(r#"SELECT "id", "name" FROM "users" WHERE "{}" IN ($1, $2, $3)"#, col("name")));
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
        use ankql::ast::{OrderByItem, OrderDirection, Selection};

        let base_selection = lowered("name = 'Alice'");
        // ORDER BY keys reach the builder in this engine's columns, like every other path.
        let selection = Selection {
            predicate: base_selection.predicate,
            order_by: Some(vec![OrderByItem { path: cpath("created_at"), direction: OrderDirection::Desc }]),
            limit: None,
        };

        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "created_at"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(
            sql_string,
            format!(r#"SELECT "id", "name", "created_at" FROM "users" WHERE "{}" = $1 ORDER BY "{}" DESC"#, col("name"), col("created_at"))
        );
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_selection_with_limit() -> Result<()> {
        let base_selection = lowered("age > 18");
        let selection = Selection { predicate: base_selection.predicate, order_by: None, limit: Some(10) };

        let mut sql = SqlBuilder::with_fields(vec!["id", "name", "age"]);
        sql.table_name("users");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, format!(r#"SELECT "id", "name", "age" FROM "users" WHERE "{}" > $1 LIMIT $2"#, col("age")));
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new(18i64), Box::new(10i64)];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_selection_with_order_by_and_limit() -> Result<()> {
        use ankql::ast::{OrderByItem, OrderDirection, Selection};

        let base_selection = lowered("status = 'active'");
        // ORDER BY keys reach the builder in this engine's columns, like every other path.
        let selection = Selection {
            predicate: base_selection.predicate,
            order_by: Some(vec![
                OrderByItem { path: cpath("priority"), direction: OrderDirection::Desc },
                OrderByItem { path: cpath("created_at"), direction: OrderDirection::Asc },
            ]),
            limit: Some(5),
        };

        let mut sql = SqlBuilder::with_fields(vec!["id", "status", "priority", "created_at"]);
        sql.table_name("tasks");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(
            sql_string,
            format!(
                r#"SELECT "id", "status", "priority", "created_at" FROM "tasks" WHERE "{}" = $1 ORDER BY "{}" DESC, "{}" ASC LIMIT $2"#,
                col("status"),
                col("priority"),
                col("created_at")
            )
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

        #[test]
        fn test_two_step_json_path() -> Result<()> {
            // licensing.territory = 'US' should use -> and ::jsonb cast
            let selection = lowered("licensing.territory = 'US'");
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            // The canonicalized JSON string binds as a jsonb parameter
            assert_eq!(sql_string, format!(r#""{}"->'territory' = $1"#, col("licensing")));
            Ok(())
        }

        #[test]
        fn test_three_step_json_path() -> Result<()> {
            // licensing.rights.holder should become "licensing"->'rights'->'holder'
            let selection = lowered("licensing.rights.holder = 'Label'");
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, format!(r#""{}"->'rights'->'holder' = $1"#, col("licensing")));
            Ok(())
        }

        #[test]
        fn test_four_step_json_path() -> Result<()> {
            // a.b.c.d should become "a"->'b'->'c'->'d'
            let selection = lowered("a.b.c.d = 'value'");
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, format!(r#""{}"->'b'->'c'->'d' = $1"#, col("a")));
            Ok(())
        }

        #[test]
        fn test_json_path_with_numeric_comparison() -> Result<()> {
            // Using -> with ::jsonb ensures proper numeric comparison:
            // - "data"->'count' returns JSONB number
            // - '10'::jsonb is JSONB number
            // - JSONB numeric comparison is numeric (9 < 10), not lexicographic ("9" > "10")
            let selection = lowered("data.count > 10");
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, format!(r#""{}"->'count' > $1"#, col("data")));
            Ok(())
        }

        #[test]
        fn test_mixed_simple_and_json_paths() -> Result<()> {
            // name = 'test' AND data.status = 'active'
            // Simple path uses $1, JSON path uses ::jsonb cast
            let selection = lowered("name = 'test' AND data.status = 'active'");
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, format!(r#""{}" = $1 AND "{}"->'status' = $2"#, col("name"), col("data")));
            Ok(())
        }

        #[test]
        fn test_json_path_escaping() -> Result<()> {
            // Field with quote in path step - should escape properly
            // Note: This tests the SQL escaping, not JSON key escaping
            let mut sql = SqlBuilder::new();
            let path = ankurah_storage_common::ColumnPath::new(col("data"), vec!["it's".to_string()]);
            sql.expr(&Expr::Path(path))?;
            let (sql_string, _) = sql.build_where_clause();

            // Just the path, no comparison - still uses ->
            assert_eq!(sql_string, format!(r#""{}"->'it''s'"#, col("data")));
            Ok(())
        }

        #[test]
        fn test_json_path_with_boolean() -> Result<()> {
            let selection = lowered("data.active = true");
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, format!(r#""{}"->'active' = $1"#, col("data")));
            Ok(())
        }

        #[test]
        fn test_json_path_with_float() -> Result<()> {
            // Note: AnkQL parser may parse this as i64, but the principle stands
            let selection = lowered("data.score >= 95");
            let mut sql = SqlBuilder::new();
            sql.selection(&selection)?;
            let (sql_string, _) = sql.build_where_clause();

            assert_eq!(sql_string, format!(r#""{}"->'score' >= $1"#, col("data")));
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
            let selection = resolved("name = 'Alice'");
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
            let selection = resolved("licensing.territory = 'US'");
            let split = split_predicate_for_postgres(&selection.predicate);

            // JSON path IS pushable via JSONB syntax
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_and_with_all_pushable() {
            let selection = resolved("name = 'test' AND licensing.status = 'active'");
            let split = split_predicate_for_postgres(&selection.predicate);

            // Both parts pushable (simple path + JSON path) = whole thing pushable
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_or_with_all_pushable() {
            let selection = resolved("name = 'a' OR name = 'b'");
            let split = split_predicate_for_postgres(&selection.predicate);

            // Both branches pushable = whole OR pushable
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_complex_nested_predicate() {
            let selection = resolved("(name = 'test' OR data.type = 'special') AND status = 'active'");
            let split = split_predicate_for_postgres(&selection.predicate);

            // All parts are pushable (simple paths + JSON paths)
            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_not_predicate_pushable() {
            let selection = resolved("NOT (status = 'deleted')");
            let split = split_predicate_for_postgres(&selection.predicate);

            assert!(!split.needs_post_filter());
        }

        #[test]
        fn test_is_null_pushable() {
            let selection = resolved("name IS NULL");
            let split = split_predicate_for_postgres(&selection.predicate);

            assert!(!split.needs_post_filter());
        }

        // Test for future: when we have unpushable predicates (e.g., Ref traversal)
        // #[test]
        // fn test_unpushable_predicate_goes_to_remaining() {
        //     // When we add Ref traversal, this test would verify:
        //     // let selection = resolved("artist.name = 'Radiohead'");
        //     // let split = split_predicate_for_postgres(&selection.predicate);
        //     // assert!(split.needs_post_filter());
        //     // assert!(matches!(split.sql_predicate, Predicate::True));
        // }
    }
}
