use crate::error::RetrievalError;
use crate::schema::CollectionSchema;
use crate::value::{Value, ValueType};
use ankql::ast::{Expr, Literal, Predicate};
use anyhow::Result;

/// Cast all literals in a predicate based on field names using a CollectionSchema
pub fn cast_predicate_types<S: CollectionSchema>(predicate: Predicate, schema: &S) -> Result<Predicate, RetrievalError> {
    match predicate {
        Predicate::Comparison { left, operator, right } => {
            // Handle both cases: field = literal AND literal = field
            match (left.as_ref(), right.as_ref()) {
                // Case 1: field = literal (cast literal to field type)
                (Expr::Identifier(identifier), Expr::Literal(literal)) => {
                    let target_type = schema.field_type(identifier)?;
                    let cast_literal = cast_literal_to_type(literal.clone(), target_type)?;
                    Ok(Predicate::Comparison { left, operator, right: Box::new(cast_literal) })
                }
                // Case 2: literal = field (cast literal to field type)
                (Expr::Literal(literal), Expr::Identifier(identifier)) => {
                    let target_type = schema.field_type(identifier)?;
                    let cast_literal = cast_literal_to_type(literal.clone(), target_type)?;
                    Ok(Predicate::Comparison { left: Box::new(cast_literal), operator, right })
                }
                // For all other cases, recursively cast both sides
                _ => {
                    let cast_left = cast_expr_types(*left, schema)?;
                    let cast_right = cast_expr_types(*right, schema)?;
                    Ok(Predicate::Comparison { left: Box::new(cast_left), operator, right: Box::new(cast_right) })
                }
            }
        }
        Predicate::IsNull(expr) => Ok(Predicate::IsNull(Box::new(cast_expr_types(*expr, schema)?))),
        Predicate::And(left, right) => {
            Ok(Predicate::And(Box::new(cast_predicate_types(*left, schema)?), Box::new(cast_predicate_types(*right, schema)?)))
        }
        Predicate::Or(left, right) => {
            Ok(Predicate::Or(Box::new(cast_predicate_types(*left, schema)?), Box::new(cast_predicate_types(*right, schema)?)))
        }
        Predicate::Not(pred) => Ok(Predicate::Not(Box::new(cast_predicate_types(*pred, schema)?))),
        Predicate::True | Predicate::False | Predicate::Placeholder => Ok(predicate),
    }
}

/// Cast all literals in an expression based on field names
fn cast_expr_types<S: CollectionSchema>(expr: Expr, schema: &S) -> Result<Expr, RetrievalError> {
    match expr {
        Expr::Literal(literal) => Ok(Expr::Literal(literal)), // Literals are cast in context
        Expr::Identifier(identifier) => Ok(Expr::Identifier(identifier)),
        Expr::Predicate(predicate) => Ok(Expr::Predicate(cast_predicate_types(predicate, schema)?)),
        Expr::InfixExpr { left, operator, right } => Ok(Expr::InfixExpr {
            left: Box::new(cast_expr_types(*left, schema)?),
            operator,
            right: Box::new(cast_expr_types(*right, schema)?),
        }),
        Expr::ExprList(exprs) => {
            let cast_exprs = exprs.into_iter().map(|e| cast_expr_types(e, schema)).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::ExprList(cast_exprs))
        }
        Expr::Placeholder => Ok(Expr::Placeholder),
    }
}

/// Cast a literal to a specific type using the Value casting system
fn cast_literal_to_type(literal: Literal, target_type: ValueType) -> Result<Expr, RetrievalError> {
    // Convert Literal -> Value -> cast -> Literal -> Expr
    let value: Value = literal.into();
    let cast_value = value.cast_to(target_type).map_err(|e| RetrievalError::StorageError(format!("Type casting error: {}", e).into()))?;
    let cast_literal: Literal = cast_value.into();
    Ok(Expr::Literal(cast_literal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::PropertyError;
    use ankql::ast::{ComparisonOperator, Identifier};
    use ankurah_proto::EntityId;

    // Test schema implementation
    struct TestSchema;

    impl CollectionSchema for TestSchema {
        fn field_type(&self, identifier: &Identifier) -> Result<ValueType, PropertyError> {
            match identifier {
                Identifier::Property(name) => match name.as_str() {
                    "id" => Ok(ValueType::EntityId),
                    _ => Ok(ValueType::String),
                },
                Identifier::CollectionProperty(_collection, property) => match property.as_str() {
                    "id" => Ok(ValueType::EntityId),
                    _ => Ok(ValueType::String),
                },
            }
        }
    }

    #[test]
    fn test_cast_id_field_string_to_entity_id() {
        let entity_id = EntityId::new();
        let base64_str = entity_id.to_base64();

        // Create a predicate: id = "base64_string"
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Identifier(Identifier::Property("id".to_string()))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String(base64_str.clone()))),
        };

        let schema = TestSchema;
        let cast_predicate = cast_predicate_types(predicate, &schema).unwrap();

        // Verify the string literal was cast to EntityId
        if let Predicate::Comparison { right, .. } = cast_predicate {
            if let Expr::Literal(Literal::EntityId(ulid)) = *right {
                assert_eq!(EntityId::from_ulid(ulid), entity_id);
            } else {
                panic!("Expected EntityId literal, got {:?}", right);
            }
        } else {
            panic!("Expected Comparison predicate");
        }
    }

    #[test]
    fn test_cast_literal_equals_field() {
        let entity_id = EntityId::new();
        let base64_str = entity_id.to_base64();

        // Create a predicate: "base64_string" = id (literal on left side)
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Literal(Literal::String(base64_str.clone()))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Identifier(Identifier::Property("id".to_string()))),
        };

        let schema = TestSchema;
        let cast_predicate = cast_predicate_types(predicate, &schema).unwrap();

        // Verify the string literal was cast to EntityId
        if let Predicate::Comparison { left, .. } = cast_predicate {
            if let Expr::Literal(Literal::EntityId(ulid)) = *left {
                assert_eq!(EntityId::from_ulid(ulid), entity_id);
            } else {
                panic!("Expected EntityId literal, got {:?}", left);
            }
        } else {
            panic!("Expected Comparison predicate");
        }
    }

    #[test]
    fn test_cast_complex_predicate() {
        let entity_id = EntityId::new();
        let base64_str = entity_id.to_base64();

        // Create a complex predicate: id = "base64_string" AND name = "test"
        let predicate = Predicate::And(
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier::Property("id".to_string()))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String(base64_str.clone()))),
            }),
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier::Property("name".to_string()))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("test".to_string()))),
            }),
        );

        let schema = TestSchema;
        let cast_predicate = cast_predicate_types(predicate, &schema).unwrap();

        // Verify the casting worked correctly
        if let Predicate::And(left_pred, right_pred) = cast_predicate {
            // Check id field was cast to EntityId
            if let Predicate::Comparison { right, .. } = *left_pred {
                if let Expr::Literal(Literal::EntityId(ulid)) = *right {
                    assert_eq!(EntityId::from_ulid(ulid), entity_id);
                } else {
                    panic!("Expected EntityId literal for id field");
                }
            }

            // Check name field remained as String
            if let Predicate::Comparison { right, .. } = *right_pred {
                if let Expr::Literal(Literal::String(s)) = *right {
                    assert_eq!(s, "test");
                } else {
                    panic!("Expected String literal for name field");
                }
            }
        } else {
            panic!("Expected And predicate");
        }
    }
}
