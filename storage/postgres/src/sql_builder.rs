use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, OrderByItem, OrderDirection, Predicate, Selection};
use ankurah_core::error::RetrievalError;
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
                Literal::Integer(int) => self.arg(*int),
                Literal::Float(float) => self.arg(*float),
                Literal::Boolean(bool) => self.arg(*bool),
            },
            Expr::Identifier(id) => match id {
                Identifier::Property(name) => {
                    // Escape any existing quotes in the property name by doubling them
                    let escaped_name = name.replace('"', "\"\"");
                    self.sql(format!(r#""{}""#, escaped_name));
                }
                Identifier::CollectionProperty(collection, name) => {
                    // Escape quotes in both collection and property names
                    let escaped_collection = collection.replace('"', "\"\"");
                    let escaped_name = name.replace('"', "\"\"");
                    self.sql(format!(r#""{}"."{}""#, escaped_collection, escaped_name));
                }
            },
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
                            Literal::Integer(int) => self.arg(*int),
                            Literal::Float(float) => self.arg(*float),
                            Literal::Boolean(bool) => self.arg(*bool),
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

    pub fn predicate(&mut self, predicate: &Predicate) -> Result<(), SqlGenerationError> {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
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
        // Generate the identifier
        match &order_by.identifier {
            Identifier::Property(name) => {
                // Escape any existing quotes in the property name by doubling them
                let escaped_name = name.replace('"', "\"\"");
                self.sql(format!(r#""{}""#, escaped_name));
            }
            Identifier::CollectionProperty(collection, name) => {
                // Escape quotes in both collection and property names
                let escaped_collection = collection.replace('"', "\"\"");
                let escaped_name = name.replace('"', "\"\"");
                self.sql(format!(r#""{}"."{}""#, escaped_collection, escaped_name));
            }
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
        let selection = parse_selection("person.name = 'Alice'").unwrap();

        let mut sql = SqlBuilder::with_fields(vec!["id", "name"]);
        sql.table_name("people");
        sql.selection(&selection)?;
        let (sql_string, args) = sql.build()?;

        assert_eq!(sql_string, r#"SELECT "id", "name" FROM "people" WHERE "person"."name" = $1"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
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
        use ankql::ast::{Identifier, OrderByItem, OrderDirection, Selection};

        let base_selection = ankql::parser::parse_selection("name = 'Alice'").unwrap();
        let selection = Selection {
            predicate: base_selection.predicate,
            order_by: Some(vec![OrderByItem {
                identifier: Identifier::Property("created_at".to_string()),
                direction: OrderDirection::Desc,
            }]),
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
        use ankql::ast::{Identifier, OrderByItem, OrderDirection, Selection};

        let base_selection = ankql::parser::parse_selection("status = 'active'").unwrap();
        let selection = Selection {
            predicate: base_selection.predicate,
            order_by: Some(vec![
                OrderByItem { identifier: Identifier::Property("priority".to_string()), direction: OrderDirection::Desc },
                OrderByItem { identifier: Identifier::Property("created_at".to_string()), direction: OrderDirection::Asc },
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
}
