use crate::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};

fn generate_expr_sql(expr: &Expr) -> String {
    match expr {
        Expr::Literal(lit) => match lit {
            Literal::String(s) => format!("'{}'", s),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
        },
        Expr::Identifier(id) => match id {
            Identifier::Property(name) => format!(r#""{}""#, name),
            Identifier::CollectionProperty(collection, name) => {
                format!(r#""{}"."{}""#, collection, name)
            }
        },
        _ => unimplemented!("Only literal and identifier expressions are supported"),
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

pub fn generate_selection_sql(predicate: &Predicate) -> String {
    match predicate {
        Predicate::Comparison { left, operator, right } => {
            format!("{} {} {}", generate_expr_sql(left), comparison_op_to_sql(operator), generate_expr_sql(right))
        }
        Predicate::And(left, right) => {
            format!("{} AND {}", generate_selection_sql(left), generate_selection_sql(right))
        }
        Predicate::Or(left, right) => {
            format!("({} OR {})", generate_selection_sql(left), generate_selection_sql(right))
        }
        Predicate::Not(pred) => format!("NOT ({})", generate_selection_sql(pred)),
        Predicate::IsNull(expr) => format!("{} IS NULL", generate_expr_sql(expr)),
        Predicate::True => "".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_selection;

    #[test]
    fn test_simple_equality() {
        let predicate = parse_selection("name = 'Alice'").unwrap();
        let sql = generate_selection_sql(&predicate);
        assert_eq!(sql, r#""name" = 'Alice'"#);
    }

    #[test]
    fn test_and_condition() {
        let predicate = parse_selection("name = 'Alice' AND age = '30'").unwrap();
        let sql = generate_selection_sql(&predicate);
        assert_eq!(sql, r#""name" = 'Alice' AND "age" = '30'"#);
    }

    #[test]
    fn test_complex_condition() {
        let predicate = parse_selection("(name = 'Alice' OR name = 'Charlie') AND age >= '30' AND age <= '40'").unwrap();
        let sql = generate_selection_sql(&predicate);
        assert_eq!(sql, r#"("name" = 'Alice' OR "name" = 'Charlie') AND "age" >= '30' AND "age" <= '40'"#);
    }

    #[test]
    fn test_including_collection_identifier() {
        let predicate = parse_selection("person.name = 'Alice'").unwrap();
        let sql = generate_selection_sql(&predicate);
        assert_eq!(sql, r#""person"."name" = 'Alice'"#);
    }
}
