use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
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
}

pub enum SqlExpr {
    Sql(String),
    Argument(Box<dyn ToSql + Send + Sync>),
}

pub struct Sql(Vec<SqlExpr>);

impl Default for Sql {
    fn default() -> Self { Self::new() }
}

impl Sql {
    pub fn new() -> Self { Self(Vec::new()) }

    pub fn push(&mut self, expr: SqlExpr) { self.0.push(expr); }

    pub fn arg(&mut self, arg: impl ToSql + Send + Sync + 'static) {
        self.push(SqlExpr::Argument(Box::new(arg) as Box<dyn ToSql + Send + Sync>));
    }

    pub fn sql(&mut self, s: impl AsRef<str>) { self.push(SqlExpr::Sql(s.as_ref().to_owned())); }

    pub fn collapse(self) -> (String, Vec<Box<dyn ToSql + Send + Sync>>) {
        let mut counter = 1;
        let mut sql = String::new();
        let mut args = Vec::new();

        for expr in self.0 {
            match expr {
                SqlExpr::Argument(arg) => {
                    sql += &format!("${}", counter);
                    args.push(arg);
                    counter += 1;
                }
                SqlExpr::Sql(s) => {
                    sql += &s;
                }
            }
        }

        (sql, args)
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

    pub fn comparison_op(&mut self, op: &ComparisonOperator) -> Result<(), SqlGenerationError> { Ok(self.sql(comparison_op_to_sql(op)?)) }

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
        let predicate = parse_selection("name = 'Alice'").unwrap();
        let mut sql = Sql::new();
        sql.predicate(&predicate)?;

        let (sql_string, args) = sql.collapse();
        assert_eq!(sql_string, r#""name" = $1"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_and_condition() -> Result<()> {
        let predicate = parse_selection("name = 'Alice' AND age = 30").unwrap();
        let mut sql = Sql::new();
        sql.predicate(&predicate)?;
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, r#""name" = $1 AND "age" = $2"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new(30)];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_complex_condition() -> Result<()> {
        let predicate = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= 30 AND age <= 40").unwrap();

        let mut sql = Sql::new();
        sql.predicate(&predicate)?;
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, r#"("name" = $1 OR "name" = $2) AND "age" >= $3 AND "age" <= $4"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new("Charlie"), Box::new(30), Box::new(40)];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_including_collection_identifier() -> Result<()> {
        let predicate = parse_selection("person.name = 'Alice'").unwrap();

        let mut sql = Sql::new();
        sql.predicate(&predicate)?;
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, r#""person"."name" = $1"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_false_predicate() -> Result<()> {
        let mut sql = Sql::new();
        sql.predicate(&Predicate::False)?;
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, "FALSE");
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_in_operator() -> Result<()> {
        let predicate = parse_selection("name IN ('Alice', 'Bob', 'Charlie')").unwrap();
        let mut sql = Sql::new();
        sql.predicate(&predicate)?;
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, r#""name" IN ($1, $2, $3)"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new("Bob"), Box::new("Charlie")];
        assert_args(&args, &expected);
        Ok(())
    }

    #[test]
    fn test_placeholder_error() {
        let mut sql = Sql::new();
        let err = sql.predicate(&Predicate::Placeholder).expect_err("Expected an error");
        assert!(matches!(err, SqlGenerationError::PlaceholderFound));
    }
}
