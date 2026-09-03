use crate::error::ParseError;
use crate::selection::sql::generate_selection_sql;
use ankurah_core_types::EntityId;
pub use ankurah_core_types::{PropertyId, PropertyPath, SystemProperty, Value};
use serde::{Deserialize, Serialize};

mod stage;
pub use stage::{Parsed, Resolved, Stage};

/// An expression whose paths use stage `S`'s representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "S::Path: Serialize", deserialize = "S::Path: Deserialize<'de>"))]
pub enum Expr<S: Stage> {
    Literal(Value),
    Path(S::Path),
    Predicate(Predicate<S>),
    InfixExpr { left: Box<Expr<S>>, operator: InfixOperator, right: Box<Expr<S>> },
    ExprList(Vec<Expr<S>>), // Handles lists like (1,2,3) in IN clauses
    Placeholder,
}

/// A parsed, model-scoped property name and optional subpath.
#[derive(Debug, Clone, PartialEq)]
pub struct PathExpr {
    pub steps: Vec<String>,
}

impl PathExpr {
    pub fn simple(name: impl Into<String>) -> Self { Self { steps: vec![name.into()] } }

    pub fn is_simple(&self) -> bool { self.steps.len() == 1 }

    pub fn first(&self) -> &str { &self.steps[0] }

    pub fn property(&self) -> &str { self.steps.last().expect("PathExpr must have at least one step") }
}

impl std::fmt::Display for PathExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.steps.join(".")) }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "S::Path: Serialize", deserialize = "S::Path: Deserialize<'de>"))]
pub struct Selection<S: Stage> {
    pub predicate: Predicate<S>,
    pub order_by: Option<Vec<OrderByItem<S>>>,
    pub limit: Option<u64>,
}

/// One sort key at stage `S`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "S::Path: Serialize", deserialize = "S::Path: Deserialize<'de>"))]
pub struct OrderByItem<S: Stage> {
    pub path: S::Path,
    pub direction: OrderDirection,
}

impl<S: Stage> std::fmt::Display for OrderByItem<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            self.path,
            match self.direction {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl<S: Stage> std::fmt::Display for Selection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.predicate)?;
        if let Some(order_by) = &self.order_by {
            write!(f, " ORDER BY ")?;
            for (i, item) in order_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", item)?;
            }
        }
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {}", limit)?;
        }
        Ok(())
    }
}

// Backward compatibility
impl<S: Stage> From<Predicate<S>> for Selection<S> {
    fn from(predicate: Predicate<S>) -> Self { Selection { predicate, order_by: None, limit: None } }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "S::Path: Serialize", deserialize = "S::Path: Deserialize<'de>"))]
pub enum Predicate<S: Stage> {
    Comparison { left: Box<Expr<S>>, operator: ComparisonOperator, right: Box<Expr<S>> },
    IsNull(Box<Expr<S>>),
    And(Box<Predicate<S>>, Box<Predicate<S>>),
    Or(Box<Predicate<S>>, Box<Predicate<S>>),
    Not(Box<Predicate<S>>),
    True,
    False,
    Placeholder,
}

impl<S: Stage> std::fmt::Display for Predicate<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match generate_selection_sql(self, None) {
            Ok(sql) => write!(f, "{}", sql),
            Err(e) => write!(f, "SQL Error: {}", e),
        }
    }
}

impl Selection<Resolved> {
    /// Evaluate the selection with the given properties absent.
    pub fn assume_null(&self, absent: &[PropertyId]) -> Self {
        let order_by = match &self.order_by {
            None => None,
            Some(items) => {
                let retained: Vec<_> = items.iter().filter(|item| !absent.contains(&item.path.property_id())).cloned().collect();
                if retained.is_empty() {
                    None
                } else {
                    Some(retained)
                }
            }
        };

        Self { predicate: self.predicate.assume_null(absent), order_by, limit: self.limit }
    }

    /// Collect the durable property identities referenced by this selection.
    pub fn referenced_properties(&self) -> Vec<PropertyId> {
        let mut properties = self.predicate.referenced_properties();
        if let Some(order_by) = &self.order_by {
            for item in order_by {
                let id = item.path.property_id();
                if !properties.contains(&id) {
                    properties.push(id);
                }
            }
        }
        properties
    }
}

fn expr_referenced_properties(expr: &Expr<Resolved>, properties: &mut Vec<PropertyId>) {
    match expr {
        Expr::Path(path) => {
            let property = path.property_id();
            if !properties.contains(&property) {
                properties.push(property);
            }
        }
        Expr::Predicate(predicate) => {
            for property in predicate.referenced_properties() {
                if !properties.contains(&property) {
                    properties.push(property);
                }
            }
        }
        Expr::InfixExpr { left, right, .. } => {
            expr_referenced_properties(left, properties);
            expr_referenced_properties(right, properties);
        }
        Expr::ExprList(expressions) => {
            for expression in expressions {
                expr_referenced_properties(expression, properties);
            }
        }
        Expr::Literal(_) | Expr::Placeholder => {}
    }
}

impl<S: Stage> Predicate<S> {
    pub fn walk<T, F>(&self, accumulator: T, visitor: &mut F) -> T
    where F: FnMut(T, &Predicate<S>) -> T {
        let accumulator = visitor(accumulator, self);
        match self {
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                let accumulator = left.walk(accumulator, visitor);
                right.walk(accumulator, visitor)
            }
            Predicate::Not(inner) => inner.walk(accumulator, visitor),
            _ => accumulator,
        }
    }

    pub fn populate<I, V, E>(self, values: I) -> Result<Predicate<S>, ParseError>
    where
        I: IntoIterator<Item = V>,
        V: TryInto<Expr<S>, Error = E>,
        E: Into<ParseError>,
    {
        let mut values_iter = values.into_iter();
        let result = self.populate_recursive(&mut values_iter)?;

        if values_iter.next().is_some() {
            return Err(ParseError::InvalidPredicate("Too many values provided for placeholders".to_string()));
        }

        Ok(result)
    }

    fn populate_recursive<I, V, E>(self, values: &mut I) -> Result<Predicate<S>, ParseError>
    where
        I: Iterator<Item = V>,
        V: TryInto<Expr<S>, Error = E>,
        E: Into<ParseError>,
    {
        match self {
            Predicate::Comparison { left, operator, right } => Ok(Predicate::Comparison {
                left: Box::new(left.populate_recursive(values)?),
                operator,
                right: Box::new(right.populate_recursive(values)?),
            }),
            Predicate::And(left, right) => {
                Ok(Predicate::And(Box::new(left.populate_recursive(values)?), Box::new(right.populate_recursive(values)?)))
            }
            Predicate::Or(left, right) => {
                Ok(Predicate::Or(Box::new(left.populate_recursive(values)?), Box::new(right.populate_recursive(values)?)))
            }
            Predicate::Not(pred) => Ok(Predicate::Not(Box::new(pred.populate_recursive(values)?))),
            Predicate::IsNull(expr) => Ok(Predicate::IsNull(Box::new(expr.populate_recursive(values)?))),
            Predicate::True => Ok(Predicate::True),
            Predicate::False => Ok(Predicate::False),
            Predicate::Placeholder => Err(ParseError::InvalidPredicate("Placeholder must be transformed before population".to_string())),
        }
    }
}

impl Predicate<Resolved> {
    pub fn referenced_properties(&self) -> Vec<PropertyId> {
        self.walk(Vec::new(), &mut |mut properties, pred| {
            match pred {
                Predicate::Comparison { left, right, .. } => {
                    for expr in [&**left, &**right] {
                        expr_referenced_properties(expr, &mut properties);
                    }
                }
                Predicate::IsNull(expr) => expr_referenced_properties(expr, &mut properties),
                _ => {}
            }
            properties
        })
    }

    pub fn assume_null(&self, absent: &[PropertyId]) -> Self {
        match self {
            Predicate::Comparison { left, operator, right } => {
                let mut referenced = Vec::new();
                expr_referenced_properties(left, &mut referenced);
                expr_referenced_properties(right, &mut referenced);
                let has_absent = referenced.iter().any(|property| absent.contains(property));

                if has_absent {
                    Predicate::False
                } else {
                    Predicate::Comparison { left: left.clone(), operator: operator.clone(), right: right.clone() }
                }
            }
            Predicate::IsNull(expr) => {
                let mut referenced = Vec::new();
                expr_referenced_properties(expr, &mut referenced);
                if referenced.iter().any(|property| absent.contains(property)) {
                    Predicate::True
                } else {
                    Predicate::IsNull(expr.clone())
                }
            }
            Predicate::And(left, right) => {
                let left = left.assume_null(absent);
                let right = right.assume_null(absent);

                match (&left, &right) {
                    (Predicate::False, _) | (_, Predicate::False) => Predicate::False,
                    (Predicate::True, Predicate::True) => Predicate::True,
                    (Predicate::True, p) | (p, Predicate::True) => p.clone(),
                    _ => Predicate::And(Box::new(left), Box::new(right)),
                }
            }
            Predicate::Or(left, right) => {
                let left = left.assume_null(absent);
                let right = right.assume_null(absent);

                match (&left, &right) {
                    (Predicate::True, _) | (_, Predicate::True) => Predicate::True,
                    (Predicate::False, Predicate::False) => Predicate::False,
                    (Predicate::False, p) | (p, Predicate::False) => p.clone(),
                    _ => Predicate::Or(Box::new(left), Box::new(right)),
                }
            }
            Predicate::Not(pred) => {
                let inner = pred.assume_null(absent);
                match inner {
                    Predicate::True => Predicate::False,
                    Predicate::False => Predicate::True,
                    _ => Predicate::Not(Box::new(inner)),
                }
            }
            Predicate::True => Predicate::True,
            Predicate::False => Predicate::False,
            Predicate::Placeholder => Predicate::Placeholder,
        }
    }
}

impl<S: Stage> Expr<S> {
    fn populate_recursive<I, V, E>(self, values: &mut I) -> Result<Expr<S>, ParseError>
    where
        I: Iterator<Item = V>,
        V: TryInto<Expr<S>, Error = E>,
        E: Into<ParseError>,
    {
        match self {
            Expr::Placeholder => match values.next() {
                Some(value) => Ok(value.try_into().map_err(|e| e.into())?),
                None => Err(ParseError::InvalidPredicate("Not enough values provided for placeholders".to_string())),
            },
            Expr::Literal(lit) => Ok(Expr::Literal(lit)),
            Expr::Path(path) => Ok(Expr::Path(path)),
            Expr::Predicate(pred) => Ok(Expr::Predicate(pred.populate_recursive(values)?)),
            Expr::InfixExpr { left, operator, right } => Ok(Expr::InfixExpr {
                left: Box::new(left.populate_recursive(values)?),
                operator,
                right: Box::new(right.populate_recursive(values)?),
            }),
            Expr::ExprList(exprs) => {
                let mut populated_exprs = Vec::new();
                for expr in exprs {
                    populated_exprs.push(expr.populate_recursive(values)?);
                }
                Ok(Expr::ExprList(populated_exprs))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,              // =
    NotEqual,           // <> or !=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    In,                 // IN
    Between,            // BETWEEN
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InfixOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_selection;

    fn prop_id(name: &str) -> EntityId {
        let mut bytes = [0u8; 32];
        let n = name.as_bytes();
        let len = n.len().min(32);
        bytes[..len].copy_from_slice(&n[..len]);
        EntityId::from_bytes(bytes)
    }

    fn cmp(name: &str) -> Predicate<Resolved> {
        Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::registered(prop_id(name), name, vec![]))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::String("x".to_string()))),
        }
    }

    fn absent(names: &[&str]) -> Vec<PropertyId> { names.iter().map(|n| PropertyId::EntityId(prop_id(n))).collect() }

    #[test]
    fn single_comparison_absent_handling() {
        // Any comparison against an absent property collapses to FALSE.
        assert_eq!(cmp("status").assume_null(&absent(&["status"])), Predicate::False);
        // IS NULL against an absent property is TRUE.
        let is_null = Predicate::IsNull(Box::new(Expr::Path(PropertyPath::registered(prop_id("status"), "status", vec![]))));
        assert_eq!(is_null.assume_null(&absent(&["status"])), Predicate::True);
        // An unrelated absent property leaves the comparison intact.
        assert_eq!(cmp("role").assume_null(&absent(&["other"])), cmp("role"));
    }

    #[test]
    fn nested_predicate_absent_handling() {
        // alpha AND (beta OR charlie)
        let input = Predicate::And(Box::new(cmp("alpha")), Box::new(Predicate::Or(Box::new(cmp("beta")), Box::new(cmp("charlie")))));
        // charlie absent: (beta OR FALSE) -> beta, so alpha AND beta.
        assert_eq!(input.assume_null(&absent(&["charlie"])), Predicate::And(Box::new(cmp("alpha")), Box::new(cmp("beta"))));
        // beta and charlie absent: (FALSE OR FALSE) -> FALSE, so alpha AND FALSE -> FALSE.
        assert_eq!(input.assume_null(&absent(&["beta", "charlie"])), Predicate::False);
        // alpha absent: FALSE AND _ -> FALSE.
        assert_eq!(input.assume_null(&absent(&["alpha"])), Predicate::False);
        // Unrelated absent property: unchanged.
        assert_eq!(input.assume_null(&absent(&["other"])), input);
    }

    #[test]
    fn test_populate_single_placeholder() {
        let selection = parse_selection("name = ?").unwrap();
        let populated = selection.predicate.populate(vec!["Alice"]).unwrap();

        let expected = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("name".to_string()))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::String("Alice".to_string()))),
        };

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_multiple_placeholders() {
        let selection = parse_selection("age > ? AND name = ?").unwrap();
        let values: Vec<Expr<Parsed>> = vec![25i64.into(), "Bob".into()];
        let populated = selection.predicate.populate(values).unwrap();

        let expected = Predicate::And(
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("age".to_string()))),
                operator: ComparisonOperator::GreaterThan,
                right: Box::new(Expr::Literal(Value::I64(25))),
            }),
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("name".to_string()))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Value::String("Bob".to_string()))),
            }),
        );

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_in_clause() {
        let selection = parse_selection("status IN (?, ?, ?)").unwrap();
        let populated = selection.predicate.populate(vec!["active", "pending", "review"]).unwrap();

        let expected = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("status".to_string()))),
            operator: ComparisonOperator::In,
            right: Box::new(Expr::ExprList(vec![
                Expr::Literal(Value::String("active".to_string())),
                Expr::Literal(Value::String("pending".to_string())),
                Expr::Literal(Value::String("review".to_string())),
            ])),
        };

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_mixed_types() {
        let selection = parse_selection("active = ? AND score > ? AND name = ?").unwrap();
        let values: Vec<Expr<Parsed>> = vec![true.into(), 95.5f64.into(), "Charlie".into()];
        let populated = selection.predicate.populate(values).unwrap();

        // Verify the structure is correct
        if let Predicate::And(left, right) = populated {
            if let Predicate::And(inner_left, inner_right) = *left {
                // Check boolean value
                if let Predicate::Comparison { right: val, .. } = *inner_left {
                    assert_eq!(*val, Expr::Literal(Value::Bool(true)));
                }
                // Check float value
                if let Predicate::Comparison { right: val, .. } = *inner_right {
                    assert_eq!(*val, Expr::Literal(Value::F64(95.5)));
                }
            }
            // Check string value
            if let Predicate::Comparison { right: val, .. } = *right {
                assert_eq!(*val, Expr::Literal(Value::String("Charlie".to_string())));
            }
        }
    }

    #[test]
    fn test_populate_too_few_values() {
        let selection = parse_selection("name = ? AND age = ?").unwrap();
        let result = selection.predicate.populate(vec!["Alice"]);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Not enough values"));
    }

    #[test]
    fn test_populate_too_many_values() {
        let selection = parse_selection("name = ?").unwrap();
        let result = selection.predicate.populate(vec!["Alice", "Bob"]);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Too many values"));
    }

    #[test]
    fn test_populate_no_placeholders() {
        let selection = parse_selection("name = 'Alice'").unwrap();
        let populated = selection.clone().predicate.populate(Vec::<String>::new()).unwrap();

        // Should be unchanged
        assert_eq!(populated, selection.predicate);
    }

    // -- Resolved property references --

    /// A Selection whose predicate compares a resolved registered property
    /// (id `[7; 32]`) against a literal.
    fn identifier_selection(name: &str, subpath: Vec<&str>) -> Selection<Resolved> {
        Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Path(PropertyPath::registered(
                    EntityId::from_bytes([7u8; 32]),
                    name,
                    subpath.into_iter().map(|s| s.to_string()).collect(),
                ))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Value::String("US".to_string()))),
            },
            order_by: None,
            limit: None,
        }
    }

    /// The durable identity of the property [`identifier_selection`] builds.
    fn identifier_selection_id() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([7u8; 32])) }

    #[test]
    fn identifier_selection_bincode_roundtrip() {
        for selection in [identifier_selection("name", vec![]), identifier_selection("licensing", vec!["territory"])] {
            let bytes = bincode::serialize(&selection).expect("serialize");
            let decoded: Selection<Resolved> = bincode::deserialize(&bytes).expect("deserialize");
            assert_eq!(decoded, selection);
        }
    }

    #[test]
    fn resolved_order_by_bincode_roundtrip() {
        let property = EntityId::from_bytes([9u8; 32]);
        let mut selection = identifier_selection("score", vec![]);
        selection.order_by =
            Some(vec![OrderByItem { path: PropertyPath::registered(property, "score", vec![]), direction: OrderDirection::Desc }]);

        let bytes = bincode::serialize(&selection).expect("serialize");
        let decoded: Selection<Resolved> = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(decoded, selection);
    }

    #[test]
    fn identifier_selection_json_roundtrip() {
        for selection in [identifier_selection("name", vec![]), identifier_selection("licensing", vec!["rights", "holder"])] {
            let json = serde_json::to_string(&selection).expect("to_json");
            let decoded: Selection<Resolved> = serde_json::from_str(&json).expect("from_json");
            assert_eq!(decoded, selection);
        }
    }

    #[test]
    fn identifier_assume_null_keys_on_identity_not_subpath() {
        let id_pred = identifier_selection("licensing", vec!["territory"]).predicate;

        assert_eq!(id_pred.assume_null(&[identifier_selection_id()]), Predicate::False);
        assert_eq!(id_pred.assume_null(&[PropertyId::EntityId(EntityId::from_bytes([8u8; 32]))]), id_pred);
    }

    #[test]
    fn assume_null_drops_a_sort_on_an_absent_property() {
        let mut selection = identifier_selection("status", vec![]);
        let absent_property = EntityId::from_bytes([9u8; 32]);
        selection.order_by = Some(vec![
            OrderByItem { path: PropertyPath::registered(absent_property, "score", vec![]), direction: OrderDirection::Asc },
            OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Asc },
        ]);

        let folded = selection.assume_null(&[PropertyId::EntityId(absent_property)]);
        assert_eq!(folded.order_by, Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Asc }]));
    }

    #[test]
    fn identifier_referenced_properties_returns_identity() {
        let id_sel = identifier_selection("licensing", vec!["territory"]);
        assert_eq!(id_sel.referenced_properties(), vec![identifier_selection_id()]);
        assert_eq!(identifier_selection("status", vec![]).referenced_properties(), vec![identifier_selection_id()]);
    }

    #[test]
    fn referenced_properties_descends_into_lists() {
        let left = PropertyPath::registered(prop_id("status"), "status", vec![]);
        let listed = PropertyPath::registered(prop_id("alias"), "alias", vec![]);
        let selection = Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Path(left)),
                operator: ComparisonOperator::In,
                right: Box::new(Expr::ExprList(vec![Expr::Path(listed)])),
            },
            order_by: None,
            limit: None,
        };

        assert_eq!(
            selection.referenced_properties(),
            vec![PropertyId::EntityId(prop_id("status")), PropertyId::EntityId(prop_id("alias"))]
        );
        assert_eq!(selection.assume_null(&[PropertyId::EntityId(prop_id("alias"))]).predicate, Predicate::False);
    }

    #[test]
    fn identifier_display_matches_path() {
        let ident = PropertyPath::registered(EntityId::from_bytes([1u8; 32]), "licensing", vec!["territory".to_string()]);
        assert_eq!(ident.to_string(), "licensing.territory");
        assert_eq!(ident.to_string(), PathExpr { steps: vec!["licensing".to_string(), "territory".to_string()] }.to_string());

        let simple = PropertyPath::registered(EntityId::from_bytes([1u8; 32]), "status", vec![]);
        assert_eq!(simple.to_string(), "status");
    }
}

// From implementations for single values that wrap them in Expr::Literal
impl<S: Stage> From<String> for Expr<S> {
    fn from(s: String) -> Expr<S> { Expr::Literal(Value::String(s)) }
}

impl<S: Stage> From<&str> for Expr<S> {
    fn from(s: &str) -> Expr<S> { Expr::Literal(Value::String(s.to_string())) }
}

impl<S: Stage> From<i64> for Expr<S> {
    fn from(i: i64) -> Expr<S> { Expr::Literal(Value::I64(i)) }
}

impl<S: Stage> From<f64> for Expr<S> {
    fn from(f: f64) -> Expr<S> { Expr::Literal(Value::F64(f)) }
}

impl<S: Stage> From<bool> for Expr<S> {
    fn from(b: bool) -> Expr<S> { Expr::Literal(Value::Bool(b)) }
}

impl<S: Stage> From<Value> for Expr<S> {
    fn from(value: Value) -> Expr<S> { Expr::Literal(value) }
}

impl<S: Stage> From<EntityId> for Expr<S> {
    fn from(id: EntityId) -> Self { Expr::Literal(Value::EntityId(id)) }
}

impl<S: Stage> From<&EntityId> for Expr<S> {
    fn from(id: &EntityId) -> Self { Expr::Literal(Value::EntityId(*id)) }
}

// These create Expr::ExprList for use in IN clauses
impl<S: Stage, T> From<Vec<T>> for Expr<S>
where T: Into<Expr<S>>
{
    fn from(vec: Vec<T>) -> Self { Expr::ExprList(vec.into_iter().map(|item| item.into()).collect()) }
}

impl<S: Stage, T, const N: usize> From<[T; N]> for Expr<S>
where T: Into<Expr<S>>
{
    fn from(arr: [T; N]) -> Self { Expr::ExprList(arr.into_iter().map(|item| item.into()).collect()) }
}

impl<S: Stage, T> From<&[T]> for Expr<S>
where T: Into<Expr<S>> + Clone
{
    fn from(slice: &[T]) -> Self { Expr::ExprList(slice.iter().map(|item| item.clone().into()).collect()) }
}

impl<S: Stage, T, const N: usize> From<&[T; N]> for Expr<S>
where T: Into<Expr<S>> + Clone
{
    fn from(arr: &[T; N]) -> Self { Expr::ExprList(arr.iter().map(|item| item.clone().into()).collect()) }
}
