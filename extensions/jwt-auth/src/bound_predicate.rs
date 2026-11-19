use ankql::ast::{Expr, Predicate, Resolved};
use ankurah_core::policy::AccessDenied;
use ankurah_core_types::{Value, ValueType};
use ankurah_proto::ModelId;

use crate::{JwtClaims, PolicyCatalog};

/// A scope's resolved property paths and the types of its remaining claim parameters.
pub(crate) struct BoundPredicate {
    pub(crate) predicate: Predicate<Resolved>,
    pub(crate) parameters: Vec<(String, Option<ValueType>)>,
}

impl BoundPredicate {
    pub(crate) fn from_stored(predicate: Predicate<Resolved>, parameters: Vec<(String, Option<ValueType>)>) -> Result<Self, AccessDenied> {
        let placeholders = parameters.iter().map(|_| Expr::<Resolved>::Placeholder);
        predicate.clone().populate(placeholders)?;
        Ok(Self { predicate, parameters })
    }

    pub(crate) fn bind(filter: &str, model: &ModelId, catalog: &dyn PolicyCatalog) -> Result<Self, AccessDenied> {
        let (predicate, variables) = crate::variables::parse_template(filter)?;
        let predicate =
            catalog.resolve_predicate(model, predicate).map_err(|_| AccessDenied::ByPolicy("policy property bindings are not ready"))?;
        let mut types = Vec::new();
        parameter_types(&predicate, model, catalog, &mut types)?;
        Ok(Self { predicate, parameters: variables.into_iter().zip(types).collect() })
    }

    pub(crate) fn populate(&self, claims: &JwtClaims) -> Result<Predicate<Resolved>, AccessDenied> {
        let values = self
            .parameters
            .iter()
            .map(|(variable, target)| {
                let value = crate::variables::resolve_variable(variable, claims)?;
                let value = match target {
                    Some(target) => Value::String(value)
                        .cast_to(*target)
                        .map_err(|_| AccessDenied::ByPolicy("claim value does not match policy property type"))?,
                    None => {
                        let Expr::Literal(value) = crate::variables::typed_expr::<Resolved>(value) else { unreachable!() };
                        value
                    }
                };
                Ok(Expr::Literal(value))
            })
            .collect::<Result<Vec<_>, AccessDenied>>()?;
        self.predicate.clone().populate(values).map_err(Into::into)
    }
}

fn parameter_types(
    predicate: &Predicate<Resolved>,
    model: &ModelId,
    catalog: &dyn PolicyCatalog,
    types: &mut Vec<Option<ValueType>>,
) -> Result<(), AccessDenied> {
    match predicate {
        Predicate::Comparison { left, right, .. } => {
            let target = |expr: &Expr<Resolved>| match expr {
                Expr::Path(path) => catalog
                    .property_type(model, &path.property_id())
                    .map(Some)
                    .map_err(|_| AccessDenied::ByPolicy("policy property type is unavailable")),
                _ => Ok(None),
            };
            expression_parameters(left, target(right)?, model, catalog, types)?;
            expression_parameters(right, target(left)?, model, catalog, types)?;
        }
        Predicate::And(left, right) | Predicate::Or(left, right) => {
            parameter_types(left, model, catalog, types)?;
            parameter_types(right, model, catalog, types)?;
        }
        Predicate::Not(inner) => parameter_types(inner, model, catalog, types)?,
        Predicate::IsNull(expr) => expression_parameters(expr, None, model, catalog, types)?,
        _ => {}
    }
    Ok(())
}

fn expression_parameters(
    expr: &Expr<Resolved>,
    target: Option<ValueType>,
    model: &ModelId,
    catalog: &dyn PolicyCatalog,
    types: &mut Vec<Option<ValueType>>,
) -> Result<(), AccessDenied> {
    match expr {
        Expr::Placeholder => types.push(target),
        Expr::ExprList(items) => {
            for item in items {
                expression_parameters(item, target, model, catalog, types)?;
            }
        }
        Expr::Predicate(predicate) => parameter_types(predicate, model, catalog, types)?,
        Expr::InfixExpr { left, right, .. } => {
            expression_parameters(left, None, model, catalog, types)?;
            expression_parameters(right, None, model, catalog, types)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::ComparisonOperator;
    use ankurah_core_types::{EntityId, NodeId, PropertyId};

    /// The claim is cast to the type of the property, so a claim holding a node id is not read as an entity id.
    #[test]
    fn node_id_claims_produce_node_id_literals_for_node_id_properties() {
        let property = PropertyId::EntityId(EntityId::from_bytes([1; 32]));
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Path(property.into())),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Placeholder),
        };
        let bound = BoundPredicate::from_stored(predicate, vec![("$jwt.custom.node".to_owned(), Some(ValueType::NodeId))]).unwrap();
        let node = NodeId::from_bytes([7; 32]);
        let custom = serde_json::Map::from_iter([("node".to_owned(), serde_json::Value::String(node.to_base64()))]);
        let claims = JwtClaims { sub: String::new(), roles: vec![], email: String::new(), name: None, custom };
        let Predicate::Comparison { right, .. } = bound.populate(&claims).unwrap() else {
            panic!("the populated predicate is not a comparison")
        };
        assert_eq!(*right, Expr::Literal(Value::NodeId(node)));
    }
}
