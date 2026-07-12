use ankql::ast::{Expr, OrderByItem, Predicate};
use ankurah_proto::EntityId;

use crate::schema::CatalogResolver;

/// Rewrite a resolved selection into an engine's COLUMN SPACE: every property
/// reference is translated to the physical name assigned by the engine's
/// durable id-to-column map.
pub fn selection_to_column_space(
    collection: &str,
    selection: &ankql::ast::Selection,
    resolver: Option<&dyn CatalogResolver>,
    column_of: &dyn Fn(&EntityId) -> Option<String>,
) -> ankql::ast::Selection {
    let translate_name = |name: &str| -> Option<String> {
        if name == "id" || name.starts_with("__") {
            return None;
        }
        let id = resolver?.resolve(collection, name)?;
        column_of(&id)
    };

    fn walk_expr(expr: &Expr, translate_id: &dyn Fn(&EntityId, &str) -> String, translate_name: &dyn Fn(&str) -> Option<String>) -> Expr {
        match expr {
            Expr::Identifier(identifier) => {
                let column = translate_id(&EntityId::from_bytes(identifier.property), &identifier.name);
                Expr::Identifier(ankql::ast::Identifier {
                    property: identifier.property,
                    name: column,
                    subpath: identifier.subpath.clone(),
                })
            }
            Expr::Path(path) => match path.steps.split_first() {
                Some((first, rest)) => match translate_name(first) {
                    Some(column) => {
                        let mut steps = Vec::with_capacity(path.steps.len());
                        steps.push(column);
                        steps.extend(rest.iter().cloned());
                        Expr::Path(ankql::ast::PathExpr { steps })
                    }
                    None => expr.clone(),
                },
                None => expr.clone(),
            },
            Expr::ExprList(exprs) => Expr::ExprList(exprs.iter().map(|e| walk_expr(e, translate_id, translate_name)).collect()),
            Expr::Predicate(p) => Expr::Predicate(walk_predicate(p, translate_id, translate_name)),
            Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
                left: Box::new(walk_expr(left, translate_id, translate_name)),
                operator: operator.clone(),
                right: Box::new(walk_expr(right, translate_id, translate_name)),
            },
            Expr::Literal(_) | Expr::Placeholder => expr.clone(),
        }
    }

    fn walk_predicate(
        predicate: &Predicate,
        translate_id: &dyn Fn(&EntityId, &str) -> String,
        translate_name: &dyn Fn(&str) -> Option<String>,
    ) -> Predicate {
        match predicate {
            Predicate::Comparison { left, operator, right } => Predicate::Comparison {
                left: Box::new(walk_expr(left, translate_id, translate_name)),
                operator: operator.clone(),
                right: Box::new(walk_expr(right, translate_id, translate_name)),
            },
            Predicate::And(a, b) => Predicate::And(
                Box::new(walk_predicate(a, translate_id, translate_name)),
                Box::new(walk_predicate(b, translate_id, translate_name)),
            ),
            Predicate::Or(a, b) => Predicate::Or(
                Box::new(walk_predicate(a, translate_id, translate_name)),
                Box::new(walk_predicate(b, translate_id, translate_name)),
            ),
            Predicate::Not(inner) => Predicate::Not(Box::new(walk_predicate(inner, translate_id, translate_name))),
            Predicate::IsNull(expr) => Predicate::IsNull(Box::new(walk_expr(expr, translate_id, translate_name))),
            Predicate::True | Predicate::False | Predicate::Placeholder => predicate.clone(),
        }
    }

    let translate_id = |id: &EntityId, display: &str| -> String { column_of(id).unwrap_or_else(|| display.to_string()) };

    let order_by = selection.order_by.as_ref().map(|items| {
        items
            .iter()
            .map(|item| match item.path.steps.split_first() {
                Some((first, rest)) => match item
                    .property
                    .map(|property| translate_id(&EntityId::from_bytes(property), first))
                    .or_else(|| translate_name(first))
                {
                    Some(column) => {
                        let mut steps = Vec::with_capacity(item.path.steps.len());
                        steps.push(column);
                        steps.extend(rest.iter().cloned());
                        OrderByItem { path: ankql::ast::PathExpr { steps }, direction: item.direction.clone(), property: item.property }
                    }
                    None => item.clone(),
                },
                None => item.clone(),
            })
            .collect()
    });

    ankql::ast::Selection {
        predicate: walk_predicate(&selection.predicate, &translate_id, &translate_name),
        order_by,
        limit: selection.limit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::{ComparisonOperator, Identifier, Literal, OrderByItem, OrderDirection, PathExpr, Selection};

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        EntityId::from_bytes(bytes)
    }

    struct OneField {
        name: &'static str,
        id: EntityId,
    }

    impl CatalogResolver for OneField {
        fn resolve(&self, _collection: &str, name: &str) -> Option<EntityId> { (name == self.name).then_some(self.id) }
        fn name_for(&self, id: &EntityId) -> Option<String> { (*id == self.id).then(|| self.name.to_string()) }
    }

    fn title_selection(property: EntityId, order_step: &str) -> Selection {
        Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier { property: property.to_bytes(), name: "title".into(), subpath: vec![] })),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("x".into()))),
            },
            order_by: Some(vec![OrderByItem {
                path: PathExpr::simple(order_step.to_string()),
                direction: OrderDirection::Asc,
                property: None,
            }]),
            limit: None,
        }
    }

    fn predicate_column(selection: &Selection) -> String {
        match &selection.predicate {
            Predicate::Comparison { left, .. } => match left.as_ref() {
                Expr::Identifier(identifier) => identifier.name.clone(),
                other => panic!("unexpected left expr {other:?}"),
            },
            other => panic!("unexpected predicate {other:?}"),
        }
    }

    #[test]
    fn identifier_and_order_by_translate_through_the_map() {
        let property = id(0x11);
        let resolver = OneField { name: "title", id: property };
        let out = selection_to_column_space("record", &title_selection(property, "title"), Some(&resolver), &|queried| {
            (*queried == property).then(|| "title_IpDQ".to_string())
        });
        assert_eq!(predicate_column(&out), "title_IpDQ");
        assert_eq!(out.order_by.unwrap()[0].path.first(), "title_IpDQ");
    }

    #[test]
    fn resolved_order_by_translates_by_id_after_display_rename() {
        let property = id(0x11);
        let mut selection = title_selection(property, "old_title");
        selection.order_by.as_mut().unwrap()[0].property = Some(property.to_bytes());
        let resolver = OneField { name: "title", id: property };
        let out = selection_to_column_space("record", &selection, Some(&resolver), &|queried| {
            (*queried == property).then(|| "old_title".to_string())
        });
        let order = &out.order_by.unwrap()[0];
        assert_eq!(order.path.first(), "old_title");
        assert_eq!(order.property, Some(property.to_bytes()));
    }

    #[test]
    fn map_miss_keeps_the_display_name() {
        let property = id(0x11);
        let resolver = OneField { name: "title", id: property };
        let out = selection_to_column_space("record", &title_selection(property, "title"), Some(&resolver), &|_| None);
        assert_eq!(predicate_column(&out), "title");
        assert_eq!(out.order_by.unwrap()[0].path.first(), "title");
    }

    #[test]
    fn pseudo_and_internal_fields_pass_through() {
        let property = id(0x11);
        let resolver = OneField { name: "title", id: property };
        let out = selection_to_column_space("record", &title_selection(property, "id"), Some(&resolver), &|_| Some("never".to_string()));
        assert_eq!(out.order_by.unwrap()[0].path.first(), "id");
        let out = selection_to_column_space("record", &title_selection(property, "__collection"), Some(&resolver), &|_| {
            Some("never".to_string())
        });
        assert_eq!(out.order_by.unwrap()[0].path.first(), "__collection");
    }

    #[test]
    fn no_resolver_leaves_names_untouched() {
        let property = id(0x11);
        let out = selection_to_column_space("record", &title_selection(property, "title"), None, &|queried| {
            (*queried == property).then(|| "title_IpDQ".to_string())
        });
        assert_eq!(predicate_column(&out), "title_IpDQ");
        assert_eq!(out.order_by.unwrap()[0].path.first(), "title");
    }
}
