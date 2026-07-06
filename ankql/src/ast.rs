use crate::error::ParseError;
use crate::selection::sql::generate_selection_sql;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Custom serialization for serde_json::Value that stores as bytes.
/// This is needed because bincode doesn't support deserialize_any.
mod json_as_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let bytes = serde_json::to_vec(value).map_err(serde::ser::Error::custom)?;
        bytes.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
    where D: Deserializer<'de> {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        serde_json::from_slice(&bytes).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Literal),
    Path(PathExpr),
    /// A resolved property reference (RFC section 5.3). This is the OUTPUT of a
    /// resolution pass that binds a parse-time `PathExpr` to a concrete property
    /// entity id; the parser NEVER produces it. `PathExpr` remains the parse-time
    /// resolution pass lives in ankurah-core (core/src/schema/resolve.rs) and
    /// runs at the query origin sites; receivers pass Identifiers through.
    /// AST shape it will target.
    Identifier(Identifier),
    Predicate(Predicate),
    InfixExpr {
        left: Box<Expr>,
        operator: InfixOperator,
        right: Box<Expr>,
    },
    ExprList(Vec<Expr>), // New variant for handling lists like (1,2,3) in IN clauses
    Placeholder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
    EntityId(Ulid),
    Object(Vec<u8>),
    Binary(Vec<u8>),
    /// JSON value - used for comparisons against JSON property subfields.
    /// Not parsed from query syntax; created by AST preparation pass.
    /// Serialized as bytes for bincode compatibility.
    #[serde(with = "json_as_bytes")]
    Json(serde_json::Value),
}

/// A dot-separated path like `name` or `licensing.territory`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathExpr {
    pub steps: Vec<String>,
}

impl PathExpr {
    /// Create a single-step path
    pub fn simple(name: impl Into<String>) -> Self { Self { steps: vec![name.into()] } }

    /// Check if this is a single-step path
    pub fn is_simple(&self) -> bool { self.steps.len() == 1 }

    /// Get the first step (always exists)
    pub fn first(&self) -> &str { &self.steps[0] }

    /// Get the property name (last step)
    pub fn property(&self) -> &str { self.steps.last().expect("PathExpr must have at least one step") }
}

impl std::fmt::Display for PathExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.steps.join(".")) }
}

/// A resolved property reference: a property entity id plus the resolved-at
/// display name and any remaining JSON sub-path steps. Produced by the
/// resolution pass (RFC section 5.3); the parser never emits this. See
/// `Expr::Identifier`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identifier {
    /// The defining property entity id (16 bytes; ankql cannot depend on
    /// proto, so this is the raw Ulid exactly like Literal::EntityId).
    pub property: Ulid,
    /// Resolved-at display name, for SQL columns and human output.
    pub name: String,
    /// Remaining JSON sub-path steps (possibly empty).
    pub subpath: Vec<String>,
}

impl Identifier {
    /// The equivalent parse-time path steps: the resolved property name
    /// followed by the JSON sub-path. Used everywhere an Identifier must
    /// behave like the Path it was resolved from (Phase A: name-based
    /// evaluation; the switch to id-based lookup arrives with the v2
    /// integration).
    pub fn path_steps(&self) -> Vec<String> {
        let mut steps = Vec::with_capacity(1 + self.subpath.len());
        steps.push(self.name.clone());
        steps.extend(self.subpath.iter().cloned());
        steps
    }

    /// The referenced property name. The property step is fixed once by the
    /// resolution pass; the subpath does NOT change which property is
    /// referenced.
    pub fn property_name(&self) -> &str { &self.name }

    /// Whether this identifier has no JSON sub-path (a bare column reference).
    pub fn is_simple(&self) -> bool { self.subpath.is_empty() }
}

impl std::fmt::Display for Identifier {
    /// Renders consistently with how PathExpr renders: name then dotted subpath.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)?;
        for step in &self.subpath {
            write!(f, ".{}", step)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Selection {
    pub predicate: Predicate,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub path: PathExpr,
    pub direction: OrderDirection,
}

impl std::fmt::Display for OrderByItem {
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

impl std::fmt::Display for Selection {
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
impl From<Predicate> for Selection {
    fn from(predicate: Predicate) -> Self { Selection { predicate, order_by: None, limit: None } }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    Comparison { left: Box<Expr>, operator: ComparisonOperator, right: Box<Expr> },
    IsNull(Box<Expr>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    True,
    False,
    Placeholder,
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match generate_selection_sql(self, None) {
            Ok(sql) => write!(f, "{}", sql),
            Err(e) => write!(f, "SQL Error: {}", e),
        }
    }
}

impl Selection {
    /// Transform the selection to assume the given columns are NULL.
    /// This filters out ORDER BY items that reference missing columns.
    pub fn assume_null(&self, columns: &[String]) -> Self {
        let order_by = self.order_by.as_ref().map(|items| {
            items
                .iter()
                .filter(|item| {
                    // Use the property name (last step) for column matching
                    let col_name = item.path.property();
                    !columns.contains(&col_name.to_string())
                })
                .cloned()
                .collect::<Vec<_>>()
        });
        // If all ORDER BY items were filtered out, set to None
        let order_by = order_by.and_then(|v| if v.is_empty() { None } else { Some(v) });

        Self { predicate: self.predicate.assume_null(columns), order_by, limit: self.limit }
    }

    /// Collect all column names referenced in this selection (WHERE + ORDER BY).
    /// For JSON paths like `licensing.territory`, returns the column name (`licensing`),
    /// not the JSON path step (`territory`).
    pub fn referenced_columns(&self) -> Vec<String> {
        let mut columns = self.predicate.referenced_columns();
        if let Some(order_by) = &self.order_by {
            for item in order_by {
                // Use first step for column reference (actual PostgreSQL column name)
                let col = item.path.first().to_string();
                if !columns.contains(&col) {
                    columns.push(col);
                }
            }
        }
        columns
    }
}

/// The column name an expression references, if it references a property.
/// For a Path this is the FIRST step (the actual column; for `licensing.territory`
/// the column is `licensing`). For a resolved Identifier this is `name` -- the
/// resolution pass fixed the property step, and the subpath does NOT change which
/// column is referenced. Returns None for expressions that reference no property.
fn expr_referenced_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Path(path) => Some(path.first().to_string()),
        Expr::Identifier(identifier) => Some(identifier.name.clone()),
        _ => None,
    }
}

/// The property name an expression references, if it references a property.
/// For a Path this is the LAST step (`path.property()`). For a resolved
/// Identifier this is `name` (the property step is fixed by the resolution pass;
/// the subpath does NOT change which property is referenced). Returns None for
/// expressions that reference no property.
///
/// Note: for a JSON path like `licensing.territory`, Path::property() returns the
/// last step (`territory`) while expr_referenced_column returns the first
/// (`licensing`); this mirrors the pre-existing first-vs-last inconsistency the
/// resolution pass ultimately collapses. For an Identifier both agree on `name`.
fn expr_referenced_property(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Path(path) => Some(path.property()),
        Expr::Identifier(identifier) => Some(identifier.property_name()),
        _ => None,
    }
}

impl Predicate {
    /// Recursively walk a predicate tree and accumulate results using a closure
    pub fn walk<T, F>(&self, accumulator: T, visitor: &mut F) -> T
    where F: FnMut(T, &Predicate) -> T {
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

    /// Collect all column names referenced in this predicate.
    /// For JSON paths like `licensing.territory`, returns the column name (`licensing`),
    /// not the JSON path step (`territory`).
    pub fn referenced_columns(&self) -> Vec<String> {
        self.walk(Vec::new(), &mut |mut cols, pred| {
            match pred {
                Predicate::Comparison { left, right, .. } => {
                    for expr in [&**left, &**right] {
                        if let Some(col) = expr_referenced_column(expr) {
                            if !cols.contains(&col) {
                                cols.push(col);
                            }
                        }
                    }
                }
                Predicate::IsNull(expr) => {
                    if let Some(col) = expr_referenced_column(expr) {
                        if !cols.contains(&col) {
                            cols.push(col);
                        }
                    }
                }
                _ => {}
            }
            cols
        })
    }

    /// Clones the predicate tree and evaluates comparisons involving missing columns as if they were NULL
    pub fn assume_null(&self, columns: &[String]) -> Self {
        match self {
            Predicate::Comparison { left, operator, right } => {
                // Check if either side references a property that is in our null list.
                // A resolved Identifier references property `name` (the property step
                // is fixed by the resolution pass; the subpath does NOT change which
                // property is referenced), mirroring path.property() for a Path.
                let has_null_path = [&**left, &**right]
                    .iter()
                    .filter_map(|expr| expr_referenced_property(expr))
                    .any(|prop| columns.contains(&prop.to_string()));

                if has_null_path {
                    match operator {
                        // NULL = anything is false
                        ComparisonOperator::Equal => Predicate::False,
                        // NULL != anything is false (NULL comparisons always return NULL in SQL)
                        ComparisonOperator::NotEqual => Predicate::False,
                        // NULL > anything is false
                        ComparisonOperator::GreaterThan => Predicate::False,
                        // NULL >= anything is false
                        ComparisonOperator::GreaterThanOrEqual => Predicate::False,
                        // NULL < anything is false
                        ComparisonOperator::LessThan => Predicate::False,
                        // NULL <= anything is false
                        ComparisonOperator::LessThanOrEqual => Predicate::False,
                        // NULL IN (...) is false
                        ComparisonOperator::In => Predicate::False,
                        // NULL BETWEEN ... is false
                        ComparisonOperator::Between => Predicate::False,
                    }
                } else {
                    // No NULL paths, keep the comparison as is
                    Predicate::Comparison { left: left.clone(), operator: operator.clone(), right: right.clone() }
                }
            }
            Predicate::IsNull(expr) => {
                // If we're explicitly checking for NULL and the referenced property is in
                // our null list, then this evaluates to true. Identifier references its
                // resolved property `name`, mirroring path.property() for a Path.
                match expr_referenced_property(expr) {
                    Some(prop) if columns.contains(&prop.to_string()) => Predicate::True,
                    _ => Predicate::IsNull(expr.clone()),
                }
            }
            Predicate::And(left, right) => {
                let left = left.assume_null(columns);
                let right = right.assume_null(columns);

                // Optimize
                match (&left, &right) {
                    // if either side is false, the whole thing is false
                    (Predicate::False, _) | (_, Predicate::False) => Predicate::False,
                    // if both sides are true, the whole thing is true
                    (Predicate::True, Predicate::True) => Predicate::True,
                    // if one side is true, the whole thing is the other side
                    (Predicate::True, p) | (p, Predicate::True) => p.clone(),
                    _ => Predicate::And(Box::new(left), Box::new(right)),
                }
            }
            Predicate::Or(left, right) => {
                let left = left.assume_null(columns);
                let right = right.assume_null(columns);

                // Optimize
                match (&left, &right) {
                    // if either side is true, the whole thing is true
                    (Predicate::True, _) | (_, Predicate::True) => Predicate::True,
                    // if both sides are false, the whole thing is false
                    (Predicate::False, Predicate::False) => Predicate::False,
                    // if one side is false, the whole thing is the other side
                    (Predicate::False, p) | (p, Predicate::False) => p.clone(),
                    // otherwise, keep the original
                    _ => Predicate::Or(Box::new(left), Box::new(right)),
                }
            }
            Predicate::Not(pred) => {
                let inner = pred.assume_null(columns);
                match inner {
                    Predicate::True => Predicate::False,
                    Predicate::False => Predicate::True,
                    _ => Predicate::Not(Box::new(inner)),
                }
            }
            // These are constants, just clone them
            Predicate::True => Predicate::True,
            Predicate::False => Predicate::False,
            Predicate::Placeholder => Predicate::Placeholder,
        }
    }

    /// Populate placeholders in the predicate with actual values
    pub fn populate<I, V, E>(self, values: I) -> Result<Predicate, ParseError>
    where
        I: IntoIterator<Item = V>,
        V: TryInto<Expr, Error = E>,
        E: Into<ParseError>,
    {
        let mut values_iter = values.into_iter();
        let result = self.populate_recursive(&mut values_iter)?;

        // Check if there are any unused values
        if values_iter.next().is_some() {
            return Err(ParseError::InvalidPredicate("Too many values provided for placeholders".to_string()));
        }

        Ok(result)
    }

    fn populate_recursive<I, V, E>(self, values: &mut I) -> Result<Predicate, ParseError>
    where
        I: Iterator<Item = V>,
        V: TryInto<Expr, Error = E>,
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
            // Placeholder should be transformed to a comparison before population
            Predicate::Placeholder => Err(ParseError::InvalidPredicate("Placeholder must be transformed before population".to_string())),
        }
    }
}

impl Expr {
    fn populate_recursive<I, V, E>(self, values: &mut I) -> Result<Expr, ParseError>
    where
        I: Iterator<Item = V>,
        V: TryInto<Expr, Error = E>,
        E: Into<ParseError>,
    {
        match self {
            Expr::Placeholder => match values.next() {
                Some(value) => Ok(value.try_into().map_err(|e| e.into())?),
                None => Err(ParseError::InvalidPredicate("Not enough values provided for placeholders".to_string())),
            },
            Expr::Literal(lit) => Ok(Expr::Literal(lit)),
            Expr::Path(path) => Ok(Expr::Path(path)),
            // A resolved Identifier is a property reference, not a placeholder; pass through.
            Expr::Identifier(identifier) => Ok(Expr::Identifier(identifier)),
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

    fn nullify_columns(input: &str, null_columns: &[&str]) -> Result<String, ParseError> {
        let selection = parse_selection(input)?;
        let result = selection.predicate.assume_null(&null_columns.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        generate_selection_sql(&result, None).map_err(|_| ParseError::InvalidPredicate("SQL generation failed".to_string()))
    }

    #[test]
    fn test_single_comparison_null_handling() {
        assert_eq!(nullify_columns("status = 'active'", &["status"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("age > 30", &["age"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("count >= 100", &["count"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("name < 'Z'", &["name"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("score <= 90", &["score"]).unwrap(), "FALSE");
        assert_eq!(nullify_columns("status IS NULL", &["status"]).unwrap(), "TRUE");
        assert_eq!(nullify_columns("role = 'admin'", &["other"]).unwrap(), r#""role" = 'admin'"#);
    }

    #[test]
    fn nested_predicate_null_handling() {
        let input = "alpha = 1 AND (beta = 2 OR charlie = 3)";
        assert_eq!(nullify_columns(input, &["charlie"]).unwrap(), r#""alpha" = 1 AND "beta" = 2"#);
        assert_eq!(nullify_columns(input, &["beta", "charlie"]).unwrap(), r#"FALSE"#);
        assert_eq!(nullify_columns(input, &["alpha"]).unwrap(), r#"FALSE"#);
        assert_eq!(nullify_columns(input, &["other"]).unwrap(), r#""alpha" = 1 AND ("beta" = 2 OR "charlie" = 3)"#);
    }

    #[test]
    fn test_populate_single_placeholder() {
        let selection = parse_selection("name = ?").unwrap();
        let populated = selection.predicate.populate(vec!["Alice"]).unwrap();

        let expected = Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("name".to_string()))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("Alice".to_string()))),
        };

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_multiple_placeholders() {
        let selection = parse_selection("age > ? AND name = ?").unwrap();
        let values: Vec<Expr> = vec![25i64.into(), "Bob".into()];
        let populated = selection.predicate.populate(values).unwrap();

        let expected = Predicate::And(
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("age".to_string()))),
                operator: ComparisonOperator::GreaterThan,
                right: Box::new(Expr::Literal(Literal::I64(25))),
            }),
            Box::new(Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr::simple("name".to_string()))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("Bob".to_string()))),
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
                Expr::Literal(Literal::String("active".to_string())),
                Expr::Literal(Literal::String("pending".to_string())),
                Expr::Literal(Literal::String("review".to_string())),
            ])),
        };

        assert_eq!(populated, expected);
    }

    #[test]
    fn test_populate_mixed_types() {
        let selection = parse_selection("active = ? AND score > ? AND name = ?").unwrap();
        let values: Vec<Expr> = vec![true.into(), 95.5f64.into(), "Charlie".into()];
        let populated = selection.predicate.populate(values).unwrap();

        // Verify the structure is correct
        if let Predicate::And(left, right) = populated {
            if let Predicate::And(inner_left, inner_right) = *left {
                // Check boolean value
                if let Predicate::Comparison { right: val, .. } = *inner_left {
                    assert_eq!(*val, Expr::Literal(Literal::Bool(true)));
                }
                // Check float value
                if let Predicate::Comparison { right: val, .. } = *inner_right {
                    assert_eq!(*val, Expr::Literal(Literal::F64(95.5)));
                }
            }
            // Check string value
            if let Predicate::Comparison { right: val, .. } = *right {
                assert_eq!(*val, Expr::Literal(Literal::String("Charlie".to_string())));
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

    // -- Identifier (resolved property reference, RFC 5.3) --

    /// A Selection whose predicate compares a resolved Identifier against a literal.
    fn identifier_selection(name: &str, subpath: Vec<&str>) -> Selection {
        Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier {
                    property: Ulid::from_bytes([7u8; 16]),
                    name: name.to_string(),
                    subpath: subpath.into_iter().map(|s| s.to_string()).collect(),
                })),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("US".to_string()))),
            },
            order_by: None,
            limit: None,
        }
    }

    /// The equivalent Path-based Selection (steps = [name, ..subpath]) for parity checks.
    fn path_selection(name: &str, subpath: Vec<&str>) -> Selection {
        let mut steps = vec![name.to_string()];
        steps.extend(subpath.into_iter().map(|s| s.to_string()));
        Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Path(PathExpr { steps })),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("US".to_string()))),
            },
            order_by: None,
            limit: None,
        }
    }

    #[test]
    fn identifier_selection_bincode_roundtrip() {
        // Simple identifier and a JSON-subpath identifier both survive a bincode round-trip.
        for selection in [identifier_selection("name", vec![]), identifier_selection("licensing", vec!["territory"])] {
            let bytes = bincode::serialize(&selection).expect("serialize");
            let decoded: Selection = bincode::deserialize(&bytes).expect("deserialize");
            assert_eq!(decoded, selection);
        }
    }

    #[test]
    fn identifier_selection_json_roundtrip() {
        for selection in [identifier_selection("name", vec![]), identifier_selection("licensing", vec!["rights", "holder"])] {
            let json = serde_json::to_string(&selection).expect("to_json");
            let decoded: Selection = serde_json::from_str(&json).expect("from_json");
            assert_eq!(decoded, selection);
        }
    }

    #[test]
    fn identifier_assume_null_matches_equivalent_path_simple() {
        // For a SIMPLE field, assume_null on an Identifier makes the same collapse
        // decision as the equivalent Path (both key on the single property step).
        let id_pred = identifier_selection("status", vec![]).predicate;
        let path_pred = path_selection("status", vec![]).predicate;

        // Nulling the referenced property collapses the comparison to False in both forms.
        assert_eq!(id_pred.assume_null(&["status".to_string()]), Predicate::False);
        assert_eq!(path_pred.assume_null(&["status".to_string()]), Predicate::False);

        // Nulling an unrelated property leaves both comparisons intact (unchanged).
        assert_eq!(id_pred.assume_null(&["unrelated".to_string()]), id_pred);
        assert_eq!(path_pred.assume_null(&["unrelated".to_string()]), path_pred);
    }

    #[test]
    fn identifier_assume_null_keys_on_resolved_property_name() {
        // An Identifier references property `name`; its subpath does NOT change which
        // property is referenced. So a JSON-subpath Identifier collapses when `name` is
        // nulled. This is the whole point of the resolution pass: the property step is
        // fixed once, resolving the pre-existing Path first-vs-last-step ambiguity
        // (Path::assume_null keys on the LAST step, so `licensing.territory` there would
        // instead collapse only when "territory" is nulled).
        let id_pred = identifier_selection("licensing", vec!["territory"]).predicate;

        // Nulling the resolved property name collapses to False.
        assert_eq!(id_pred.assume_null(&["licensing".to_string()]), Predicate::False);
        // Nulling a sub-path step does NOT collapse (the subpath is not the property).
        assert_eq!(id_pred.assume_null(&["territory".to_string()]), id_pred);
    }

    #[test]
    fn identifier_referenced_columns_returns_name() {
        // referenced_columns keys on the resolved property name, and matches the
        // column the equivalent Path reports (the first/root step).
        let id_sel = identifier_selection("licensing", vec!["territory"]);
        assert_eq!(id_sel.referenced_columns(), vec!["licensing".to_string()]);

        let path_sel = path_selection("licensing", vec!["territory"]);
        assert_eq!(id_sel.referenced_columns(), path_sel.referenced_columns());

        // A simple identifier reports its name.
        assert_eq!(identifier_selection("status", vec![]).referenced_columns(), vec!["status".to_string()]);
    }

    #[test]
    fn identifier_display_matches_path() {
        // Display renders name then dotted subpath, consistent with PathExpr.
        let ident =
            Identifier { property: Ulid::from_bytes([1u8; 16]), name: "licensing".to_string(), subpath: vec!["territory".to_string()] };
        assert_eq!(ident.to_string(), "licensing.territory");
        assert_eq!(ident.to_string(), PathExpr { steps: vec!["licensing".to_string(), "territory".to_string()] }.to_string());

        let simple = Identifier { property: Ulid::from_bytes([1u8; 16]), name: "status".to_string(), subpath: vec![] };
        assert_eq!(simple.to_string(), "status");
    }
}

// From implementations for single values that wrap them in Expr::Literal
impl From<String> for Expr {
    fn from(s: String) -> Expr { Expr::Literal(Literal::String(s)) }
}

impl From<&str> for Expr {
    fn from(s: &str) -> Expr { Expr::Literal(Literal::String(s.to_string())) }
}

impl From<i64> for Expr {
    fn from(i: i64) -> Expr { Expr::Literal(Literal::I64(i)) }
}

impl From<f64> for Expr {
    fn from(f: f64) -> Expr { Expr::Literal(Literal::F64(f)) }
}

impl From<bool> for Expr {
    fn from(b: bool) -> Expr { Expr::Literal(Literal::Bool(b)) }
}

impl From<Literal> for Expr {
    fn from(lit: Literal) -> Expr { Expr::Literal(lit) }
}

// These create Expr::ExprList for use in IN clauses
impl<T> From<Vec<T>> for Expr
where T: Into<Expr>
{
    fn from(vec: Vec<T>) -> Self { Expr::ExprList(vec.into_iter().map(|item| item.into()).collect()) }
}

impl<T, const N: usize> From<[T; N]> for Expr
where T: Into<Expr>
{
    fn from(arr: [T; N]) -> Self { Expr::ExprList(arr.into_iter().map(|item| item.into()).collect()) }
}

impl<T> From<&[T]> for Expr
where T: Into<Expr> + Clone
{
    fn from(slice: &[T]) -> Self { Expr::ExprList(slice.iter().map(|item| item.clone().into()).collect()) }
}

impl<T, const N: usize> From<&[T; N]> for Expr
where T: Into<Expr> + Clone
{
    fn from(arr: &[T; N]) -> Self { Expr::ExprList(arr.iter().map(|item| item.clone().into()).collect()) }
}
