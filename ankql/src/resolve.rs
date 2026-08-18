//! Point-of-use casting of comparison literals in a resolved selection.
//!
//! Binding names to identities happens once, where a query enters the system
//! (`ankurah_core::schema::resolver`), and it canonicalizes each comparison
//! literal to the registered type of the property it is compared against. A
//! consumer that is about to EVALUATE such a comparison still casts again,
//! here, against its own authoritative type: a normalized AST that travelled
//! (over the wire, through a policy agent, out of a cache) is evidence, never
//! proof.

#![deny(missing_docs)]

use ankurah_core_types::{CastError, PropertyId, ValueType};
use thiserror::Error;

use crate::ast::{Expr, Predicate, PropertyPath, Resolved, Selection};

/// A value failed the point-of-use cast requested by an AST consumer.
///
/// Name resolution performs the same kind of cast as early validation, but
/// consumers use this separate operation at their own trust boundary rather
/// than treating a previously normalized AST as proof.
#[derive(Debug, Error, Clone, PartialEq)]
#[error("comparison value for property {property:?} cannot be cast to {target:?}: {source}")]
pub struct ComparisonValueCastError {
    /// The resolved property whose value establishes the comparison type.
    pub property: PropertyId,
    /// The consumer's authoritative target type.
    pub target: ValueType,
    #[source]
    /// The underlying value conversion failure.
    pub source: CastError,
}

impl Selection<Resolved> {
    /// Re-cast comparison literals immediately before a consumer uses them.
    ///
    /// `type_of` supplies that consumer's authoritative type for a whole
    /// resolved property. JSON subpaths and the `id` pseudo-property are typed
    /// here as `Json` and `EntityId` respectively. This deliberately remains a
    /// point-of-use API: callers should invoke it at each execution boundary,
    /// even though the resolution walk already canonicalized the AST at
    /// origin.
    pub fn cast_comparison_values<F>(&self, type_of: &F) -> Result<Self, ComparisonValueCastError>
    where F: Fn(&PropertyPath) -> Option<ValueType> {
        Ok(Self { predicate: cast_predicate_values(&self.predicate, type_of)?, order_by: self.order_by.clone(), limit: self.limit })
    }
}

fn cast_predicate_values<F>(predicate: &Predicate<Resolved>, type_of: &F) -> Result<Predicate<Resolved>, ComparisonValueCastError>
where F: Fn(&PropertyPath) -> Option<ValueType> {
    Ok(match predicate {
        Predicate::Comparison { left, operator, right } => {
            let left_target = comparison_target(left, type_of);
            let right_target = comparison_target(right, type_of);
            let left = match right_target {
                Some((path, target)) => cast_execution_expr(left, path, target)?,
                None => left.as_ref().clone(),
            };
            let right = match left_target {
                Some((path, target)) => cast_execution_expr(right, path, target)?,
                None => right.as_ref().clone(),
            };
            Predicate::Comparison { left: Box::new(left), operator: operator.clone(), right: Box::new(right) }
        }
        Predicate::And(left, right) => {
            Predicate::And(Box::new(cast_predicate_values(left, type_of)?), Box::new(cast_predicate_values(right, type_of)?))
        }
        Predicate::Or(left, right) => {
            Predicate::Or(Box::new(cast_predicate_values(left, type_of)?), Box::new(cast_predicate_values(right, type_of)?))
        }
        Predicate::Not(inner) => Predicate::Not(Box::new(cast_predicate_values(inner, type_of)?)),
        Predicate::IsNull(_) | Predicate::True | Predicate::False | Predicate::Placeholder => predicate.clone(),
    })
}

fn comparison_target<'a, F>(expr: &'a Expr<Resolved>, type_of: &F) -> Option<(&'a PropertyPath, ValueType)>
where F: Fn(&PropertyPath) -> Option<ValueType> {
    let Expr::Path(path) = expr else { return None };
    let target = if !path.subpath.is_empty() {
        ValueType::Json
    } else if path.property_id() == PropertyId::Id {
        ValueType::EntityId
    } else {
        type_of(path)?
    };
    Some((path, target))
}

fn cast_execution_expr(
    expr: &Expr<Resolved>,
    property: &PropertyPath,
    target: ValueType,
) -> Result<Expr<Resolved>, ComparisonValueCastError> {
    Ok(match expr {
        Expr::Literal(value) => Expr::Literal(value.cast_to(target).map_err(|source| ComparisonValueCastError {
            property: property.property_id(),
            target,
            source,
        })?),
        Expr::ExprList(values) => {
            Expr::ExprList(values.iter().map(|value| cast_execution_expr(value, property, target)).collect::<Result<Vec<_>, _>>()?)
        }
        other => other.clone(),
    })
}

#[cfg(test)]
mod tests {
    use ankurah_core_types::{EntityId, Value};

    use super::*;
    use crate::ast::ComparisonOperator;

    fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([0x22; 32])) }

    #[test]
    fn execution_cast_does_not_trust_origin_normalization() {
        let selection: Selection<Resolved> = Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::registered(
                match property() {
                    PropertyId::EntityId(id) => id,
                    _ => unreachable!(),
                },
                "value",
                vec![],
            ))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::I32(42))),
        }
        .into();

        let cast = selection.cast_comparison_values(&|_| Some(ValueType::I64)).unwrap();
        let Predicate::Comparison { right, .. } = cast.predicate else { panic!("expected comparison") };
        assert_eq!(*right, Expr::Literal(Value::I64(42)));
    }
}
