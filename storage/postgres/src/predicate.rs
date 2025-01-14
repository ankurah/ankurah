use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use tokio_postgres::types::ToSql;

pub enum SqlExpr {
    Sql(String),
    Argument(Box<dyn ToSql + Send + Sync>),
}

pub struct Sql(Vec<SqlExpr>);

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
    pub fn expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Literal(lit) => match lit {
                Literal::String(s) => self.arg(s.to_owned()),
                Literal::Integer(int) => self.arg(*int),
                Literal::Float(float) => self.arg(*float),
                Literal::Boolean(bool) => self.arg(*bool),
            },
            Expr::Identifier(id) => match id {
                Identifier::Property(name) => self.sql(format!(r#""{}""#, name)),
                Identifier::CollectionProperty(collection, name) => {
                    self.sql(format!(r#""{}"."{}""#, collection, name));
                }
            },
            _ => unimplemented!("Only literal and identifier expressions are supported"),
        }
    }

    pub fn comparison_op(&mut self, op: &ComparisonOperator) { self.sql(comparison_op_to_sql(op)); }

    pub fn predicate(&mut self, predicate: &Predicate) {
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                self.expr(left);
                self.sql(" ");
                self.comparison_op(operator);
                self.sql(" ");
                self.expr(right);
            }
            Predicate::And(left, right) => {
                self.predicate(left);
                self.sql(" AND ");
                self.predicate(right);
            }
            Predicate::Or(left, right) => {
                self.sql("(");
                self.predicate(left);
                self.sql(" OR ");
                self.predicate(right);
                self.sql(")");
            }
            Predicate::Not(pred) => {
                self.sql("NOT (");
                self.predicate(pred);
                self.sql("NOT )");
            }
            Predicate::IsNull(expr) => {
                self.expr(expr);
                self.sql(" IS NULL");
            }
            Predicate::True => {
                self.sql("TRUE");
            }
        }
    }
}

fn comparison_op_to_sql(op: &ComparisonOperator) -> &'static str {
    match op {
        ComparisonOperator::Equal => "=",
        ComparisonOperator::NotEqual => "<>",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
        _ => unimplemented!("Only basic comparison operators are supported"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::parser::parse_selection;

    fn assert_args<'a, 'b>(args: &Vec<Box<dyn ToSql + Send + Sync>>, expected: &Vec<Box<dyn ToSql + Send + Sync>>) {
        // TODO: Maybe actually encoding these and comparing bytes?
        assert_eq!(format!("{:?}", args), format!("{:?}", expected));
    }

    #[test]
    fn test_simple_equality() {
        let predicate = parse_selection("name = 'Alice'").unwrap();
        let mut sql = Sql::new();
        sql.predicate(&predicate);

        let (sql_string, args) = sql.collapse();
        assert_eq!(sql_string, r#""name" = $1"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
    }

    #[test]
    fn test_and_condition() {
        let predicate = parse_selection("name = 'Alice' AND age = 30").unwrap();
        let mut sql = Sql::new();
        sql.predicate(&predicate);
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, r#""name" = $1 AND "age" = $2"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new(30)];
        assert_args(&args, &expected);
    }

    #[test]
    fn test_complex_condition() {
        let predicate = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= 30 AND age <= 40").unwrap();

        let mut sql = Sql::new();
        sql.predicate(&predicate);
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, r#"("name" = $1 OR "name" = $2) AND "age" >= $3 AND "age" <= $4"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice"), Box::new("Charlie"), Box::new(30), Box::new(40)];
        assert_args(&args, &expected);
    }

    #[test]
    fn test_including_collection_identifier() {
        let predicate = parse_selection("person.name = 'Alice'").unwrap();

        let mut sql = Sql::new();
        sql.predicate(&predicate);
        let (sql_string, args) = sql.collapse();

        assert_eq!(sql_string, r#""person"."name" = $1"#);
        let expected: Vec<Box<dyn ToSql + Send + Sync>> = vec![Box::new("Alice")];
        assert_args(&args, &expected);
    }
}
