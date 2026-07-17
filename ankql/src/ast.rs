use crate::error::{ParseError, SelectionError};
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
    PropertyPath(PropertyPath),
    Predicate(Predicate),
    InfixExpr { left: Box<Expr>, operator: InfixOperator, right: Box<Expr> },
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

/// A property reference resolved against the catalog: the `id`
/// pseudo-property, a registered property's stable entity id, or a system
/// property's durable name -- plus any JSON sub-path into the property's
/// value. This is the resolved counterpart of [`PathExpr`], which names a
/// property by string alone. The parser only ever produces `PathExpr`; a
/// `PropertyPath` appears after the resolution pass (in ankurah-core) has
/// bound the reference.
///
/// `label` is carried as its own field, for every arm, rather than folded
/// into one arm of `id`'s type the way it used to be. Today a `System`
/// label always equals its `PropertyId::System` name, and the `id`
/// pseudo-property's label is always the literal `"id"` -- both look
/// recoverable from `id` alone, but that is an accident of the current
/// arms, not a rule resolution should lean on: a future path -> `PropertyId`
/// resolution is not guaranteed a label derivable from the id after the
/// fact, and the source label (usable only for `Display`, never a physical
/// storage name) must survive regardless of which arm minted the id.
/// Keeping `label` next to `id`, rather than nesting `id` inside a second
/// enum shaped almost exactly like [`PropertyId`] just to bolt a label onto
/// every arm, is what makes that guarantee cheap to keep.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyPath {
    id: PropertyId,
    /// The input name from the unparsed AST which was resolved to this id (usable only for Display)
    label: String,
    /// JSON sub-path into the property's value; empty for a plain reference.
    pub subpath: Vec<String>,
}

/// The serializable, durable address of a resolved property: a registered
/// catalog id, or a system-property name. This is what a storage engine keys
/// its durable property-to-physical-address map on (how each engine physically
/// addresses a property is its own private concern), so it
/// is a genuinely public type precisely BECAUSE it is that key. Engines use it
/// as an opaque key (store it, compare it, serialize it), with one sanctioned
/// exception: a `System` property's name IS its durable identity, and is the
/// seed for that engine's sanitized physical name. A registered id never
/// yields a name here; engines seed a display name for it from the catalog
/// (`CatalogResolver::name_for`) at column-assignment time, then pin their
/// own physical name for good.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PropertyId {
    /// The "id" property. each entity has one of these
    Id,
    /// The entity id of the property registration. Held as a raw `Ulid`
    /// because ankql cannot depend on proto's `EntityId` (same as
    /// [`Literal::EntityId`]).
    EntityId(Ulid),
    /// A system-defined property, identified globally by name: the same name is
    /// the same property in every collection (a system property is an implicit
    /// entity id in its own right, not a per-collection string).
    System { name: String },
}

impl PropertyPath {
    /// A resolved reference to a registered user-model property, addressed by
    /// its stable catalog id. `label` is the name that resolved to this id,
    /// retained for `Display` only (it must never seed a physical storage name).
    pub fn registered(id: Ulid, label: impl Into<String>, subpath: Vec<String>) -> Self {
        Self { id: PropertyId::EntityId(id), label: label.into(), subpath }
    }

    /// A resolved reference to a system/catalog property, addressed by name:
    /// the frozen bootstrap base case has no catalog entry to mint an id from.
    pub fn system(name: impl Into<String>, subpath: Vec<String>) -> Self {
        let name = name.into();
        Self { id: PropertyId::System { name: name.clone() }, label: name, subpath }
    }

    /// A resolved reference to the `id` pseudo-property (every entity's primary
    /// key), addressed by its own [`PropertyId::Id`] rather than a catalog id or
    /// a name.
    pub fn id(subpath: Vec<String>) -> Self { Self { id: PropertyId::Id, label: "id".to_string(), subpath } }

    // pub fn path_steps(&self) -> Vec<String> {
    //     whoops, path_steps leaks name. can't do that. who's using this?
    //     let mut steps = Vec::with_capacity(1 + self.subpath.len());
    //     steps.push(self.name.clone());
    //     steps.extend(self.subpath.iter().cloned());
    //     steps
    // }

    // fn name deliberately removed so no one can interrogate the name of a property Identifier

    /// This property's serializable, durable address (see [`PropertyId`]).
    /// A storage engine keys its durable property-to-physical map on the returned
    /// value and uses it as an opaque key (see [`PropertyId`] for the one
    /// sanctioned exception, a `System` name). When a display name is genuinely
    /// needed, pick by which name is meant: the `Display` impl on
    /// [`PropertyPath`] gives the name AS WRITTEN in this ankql statement (the
    /// resolved-from label), whereas the catalog (`CatalogResolver::name_for`
    /// on a registered id) gives the property's CURRENT name; the two can
    /// diverge after a rename.
    pub fn id_or_systemname(&self) -> PropertyId { self.id.clone() }

    /// True when there is no JSON sub-path: a plain property reference.
    pub fn is_simple(&self) -> bool { self.subpath.is_empty() }
}

impl std::fmt::Display for PropertyPath {
    /// Human-readable ONLY (never a physical storage name): the resolved-from
    /// label, then any sub-path dotted on.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label)?;
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
    pub key: OrderKey,
    pub direction: OrderDirection,
}

/// A sort key, mirroring `Expr::Path` vs `Expr::PropertyPath`: the parser
/// produces the raw [`OrderKey::Path`] form, and the resolution pass rewrites it
/// to [`OrderKey::Property`] -- including the `id` pseudo-property, which
/// resolves to [`PropertyPath::id`] (carrying its own [`PropertyId::Id`]), not
/// to a catalog id. The resolved arm keeps its name private just like
/// [`PropertyPath`]; a storage engine keys its sort on the identifier's
/// `id_or_systemname()`, never on a name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderKey {
    Path(PathExpr),
    Property(PropertyPath),
}

impl std::fmt::Display for OrderKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderKey::Path(path) => write!(f, "{}", path),
            OrderKey::Property(identifier) => write!(f, "{}", identifier),
        }
    }
}

impl std::fmt::Display for OrderByItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            self.key,
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
    /// Verify that this selection is fully resolved and populated: every
    /// property reference carries a durable [`PropertyId`], and no placeholder
    /// is left unfilled.
    ///
    /// A storage engine addresses a property only by identity, translating a
    /// [`PropertyId`] to its assigned column through its own durable map. A raw
    /// [`Expr::Path`] leaves it nothing to address but a name, and a name is
    /// exactly what must not reach it: names move under rename, columns do not,
    /// so guessing a column from one silently reads the wrong data. An engine
    /// therefore calls this before reading, and an unresolved selection fails
    /// loudly at the boundary rather than emitting SQL against a column that
    /// does not exist, or that belongs to some other property.
    ///
    /// Resolution (`ankurah-core`'s `resolve_selection`) is what establishes
    /// this: it binds every reference to an id or fails closed. Anything that
    /// hand-builds a selection and skips that pass is the caller this catches.
    pub fn check(&self) -> Result<(), SelectionError> {
        self.predicate.check()?;
        for item in self.order_by.iter().flatten() {
            if let OrderKey::Path(path) = &item.key {
                return Err(SelectionError::UnresolvedPath(path.to_string()));
            }
        }
        Ok(())
    }

    /// Transform the selection to assume the given properties are absent
    /// (evaluate to NULL). This also drops ORDER BY items that sort on an
    /// absent property. Properties are matched by durable identity, so a
    /// rename cannot change which sort key or comparison is affected.
    pub fn assume_null(&self, absent: &[PropertyId]) -> Self {
        let order_by = self.order_by.as_ref().map(|items| {
            items
                .iter()
                .filter(|item| match &item.key {
                    // A sort on an absent property is dropped. `id` resolves to
                    // `OrderKey::Property` carrying `PropertyId::Id`, which is
                    // never reported absent (engines pin it to the primary key),
                    // so it is kept; a raw `OrderKey::Path` (only the degenerate
                    // empty path survives resolution) carries no identity and is
                    // likewise kept.
                    OrderKey::Property(identifier) => !absent.contains(&identifier.id_or_systemname()),
                    OrderKey::Path(_) => true,
                })
                .cloned()
                .collect::<Vec<_>>()
        });
        // If all ORDER BY items were filtered out, set to None
        let order_by = order_by.and_then(|v| if v.is_empty() { None } else { Some(v) });

        Self { predicate: self.predicate.assume_null(absent), order_by, limit: self.limit }
    }

    /// Collect the durable addresses of every property referenced in this
    /// selection (WHERE + ORDER BY), including the `id` pseudo-property (as
    /// [`PropertyId::Id`]). Only a raw, unresolved `Path`/`OrderKey::Path` key --
    /// which carries no identity -- is omitted; after resolution every reference
    /// is a resolved identifier and is reported.
    pub fn referenced_properties(&self) -> Vec<PropertyId> {
        let mut properties = self.predicate.referenced_properties();
        if let Some(order_by) = &self.order_by {
            for item in order_by {
                if let OrderKey::Property(identifier) = &item.key {
                    let id = identifier.id_or_systemname();
                    if !properties.contains(&id) {
                        properties.push(id);
                    }
                }
            }
        }
        properties
    }
}

/// The durable property address an expression references, if any. A resolved
/// [`PropertyPath`] yields its [`PropertyId`]; everything else -- a literal, a
/// raw (unresolved) `Path`, an expression list -- addresses no durable property.
/// The `id` pseudo-property resolves to a [`PropertyPath`] carrying
/// [`PropertyId::Id`], so it IS reported here, not omitted.
///
/// A resolved identifier carries a single identity, so the old JSON
/// first-step-vs-last-step asymmetry (a `Path` reported its root by first
/// step but its property by last step) no longer arises: `licensing.territory`
/// resolves to the `licensing` property, subpath `[territory]`, and this returns
/// that one property's address regardless of the subpath.
fn expr_referenced_property(expr: &Expr) -> Option<PropertyId> {
    match expr {
        Expr::PropertyPath(identifier) => Some(identifier.id_or_systemname()),
        _ => None,
    }
}

/// See [`Selection::check`]. Exhaustive over [`Expr`] on purpose: a new variant
/// that can carry a property reference must decide, here, whether it is
/// addressable by identity.
fn expr_check(expr: &Expr) -> Result<(), SelectionError> {
    match expr {
        Expr::Path(path) => Err(SelectionError::UnresolvedPath(path.to_string())),
        Expr::Placeholder => Err(SelectionError::UnpopulatedPlaceholder),
        Expr::PropertyPath(_) | Expr::Literal(_) => Ok(()),
        Expr::ExprList(exprs) => exprs.iter().try_for_each(expr_check),
        Expr::Predicate(predicate) => predicate.check(),
        Expr::InfixExpr { left, operator: _, right } => {
            expr_check(left)?;
            expr_check(right)
        }
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

    /// See [`Selection::check`].
    pub fn check(&self) -> Result<(), SelectionError> {
        match self {
            Predicate::Comparison { left, operator: _, right } => {
                expr_check(left)?;
                expr_check(right)
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                left.check()?;
                right.check()
            }
            Predicate::Not(inner) => inner.check(),
            Predicate::IsNull(expr) => expr_check(expr),
            Predicate::True | Predicate::False => Ok(()),
            Predicate::Placeholder => Err(SelectionError::UnpopulatedPlaceholder),
        }
    }

    /// Collect the durable addresses of every property referenced in this
    /// predicate, including the `id` pseudo-property (as [`PropertyId::Id`]).
    /// Only a raw, unresolved `Path` reference carries no identity and is
    /// omitted; after resolution every reference is a resolved identifier.
    /// [`Selection::check`] is what guarantees no such reference reaches an
    /// engine, so an engine's absence scan sees every property it will emit.
    pub fn referenced_properties(&self) -> Vec<PropertyId> {
        self.walk(Vec::new(), &mut |mut properties, pred| {
            match pred {
                Predicate::Comparison { left, right, .. } => {
                    for expr in [&**left, &**right] {
                        if let Some(property) = expr_referenced_property(expr) {
                            if !properties.contains(&property) {
                                properties.push(property);
                            }
                        }
                    }
                }
                Predicate::IsNull(expr) => {
                    if let Some(property) = expr_referenced_property(expr) {
                        if !properties.contains(&property) {
                            properties.push(property);
                        }
                    }
                }
                _ => {}
            }
            properties
        })
    }

    /// Clones the predicate tree and evaluates comparisons involving absent
    /// properties as if they were NULL. Properties are matched by durable
    /// identity ([`PropertyId`]); a resolved identifier's subpath does not
    /// change which property is referenced.
    pub fn assume_null(&self, absent: &[PropertyId]) -> Self {
        match self {
            Predicate::Comparison { left, operator, right } => {
                // Check if either side references a property that is in our absent list.
                let has_absent =
                    [&**left, &**right].iter().filter_map(|expr| expr_referenced_property(expr)).any(|property| absent.contains(&property));

                if has_absent {
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
                // An explicit IS NULL check against an absent property is TRUE.
                // A resolved identifier's subpath does not change its identity.
                match expr_referenced_property(expr) {
                    Some(property) if absent.contains(&property) => Predicate::True,
                    _ => Predicate::IsNull(expr.clone()),
                }
            }
            Predicate::And(left, right) => {
                let left = left.assume_null(absent);
                let right = right.assume_null(absent);

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
                let left = left.assume_null(absent);
                let right = right.assume_null(absent);

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
                let inner = pred.assume_null(absent);
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
            Expr::PropertyPath(identifier) => Ok(Expr::PropertyPath(identifier)),
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

    /// A deterministic property id for a name, so a test can name the identity
    /// it marks absent. `assume_null` keys on identity, not on the name.
    fn prop_id(name: &str) -> Ulid {
        let mut bytes = [0u8; 16];
        let n = name.as_bytes();
        let len = n.len().min(16);
        bytes[..len].copy_from_slice(&n[..len]);
        Ulid::from_bytes(bytes)
    }

    /// `<name> = 'x'` as a RESOLVED comparison against a registered property.
    fn cmp(name: &str) -> Predicate {
        Predicate::Comparison {
            left: Box::new(Expr::PropertyPath(PropertyPath::registered(prop_id(name), name, vec![]))),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("x".to_string()))),
        }
    }

    /// The absent-property list addressing the named properties by identity.
    fn absent(names: &[&str]) -> Vec<PropertyId> { names.iter().map(|n| PropertyId::EntityId(prop_id(n))).collect() }

    #[test]
    fn single_comparison_absent_handling() {
        // Any comparison against an absent property collapses to FALSE.
        assert_eq!(cmp("status").assume_null(&absent(&["status"])), Predicate::False);
        // IS NULL against an absent property is TRUE.
        let is_null = Predicate::IsNull(Box::new(Expr::PropertyPath(PropertyPath::registered(prop_id("status"), "status", vec![]))));
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

    // -- Identifier (resolved property reference) --

    /// A Selection whose predicate compares a resolved registered property
    /// (id `[7; 16]`) against a literal.
    fn identifier_selection(name: &str, subpath: Vec<&str>) -> Selection {
        Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::PropertyPath(PropertyPath::registered(
                    Ulid::from_bytes([7u8; 16]),
                    name,
                    subpath.into_iter().map(|s| s.to_string()).collect(),
                ))),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("US".to_string()))),
            },
            order_by: None,
            limit: None,
        }
    }

    /// The durable identity of the property [`identifier_selection`] builds.
    fn identifier_selection_id() -> PropertyId { PropertyId::EntityId(Ulid::from_bytes([7u8; 16])) }

    /// `left = 'US'`, for whatever operand is handed in.
    fn cmp_with(left: Expr) -> Predicate {
        Predicate::Comparison {
            left: Box::new(left),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Literal::String("US".to_string()))),
        }
    }

    fn raw_path(name: &str) -> Expr { Expr::Path(PathExpr::simple(name)) }

    #[test]
    fn check_passes_every_resolved_identity_arm() {
        // All three PropertyId arms are addressable by identity, so all three pass.
        for left in [
            Expr::PropertyPath(PropertyPath::registered(Ulid::from_bytes([7u8; 16]), "status", vec![])),
            Expr::PropertyPath(PropertyPath::system("collection", vec![])),
            Expr::PropertyPath(PropertyPath::id(vec![])),
        ] {
            let selection = Selection { predicate: cmp_with(left), order_by: None, limit: None };
            assert!(selection.check().is_ok());
        }
        // A subpath into a resolved property is still addressable by identity.
        assert!(identifier_selection("licensing", vec!["territory"]).check().is_ok());
    }

    #[test]
    fn check_rejects_raw_path_and_names_it() {
        let selection = Selection { predicate: cmp_with(raw_path("collection")), order_by: None, limit: None };
        let err = selection.check().expect_err("a raw path carries no identity");
        assert!(matches!(&err, SelectionError::UnresolvedPath(p) if p == "collection"), "got {err:?}");
    }

    #[test]
    fn check_recurses_through_every_nesting_form() {
        // Each form buries the same raw path one level down. A check that fails
        // to recurse would pass these, which is precisely the leak it exists to
        // catch, so assert the walk actually reaches them.
        let buried = cmp_with(raw_path("collection"));
        let forms = [
            Predicate::And(Box::new(Predicate::True), Box::new(buried.clone())),
            Predicate::Or(Box::new(Predicate::True), Box::new(buried.clone())),
            Predicate::Not(Box::new(buried.clone())),
            Predicate::IsNull(Box::new(raw_path("collection"))),
            cmp_with(Expr::ExprList(vec![raw_path("collection")])),
            cmp_with(Expr::Predicate(buried.clone())),
            cmp_with(Expr::InfixExpr {
                left: Box::new(raw_path("collection")),
                operator: InfixOperator::Add,
                right: Box::new(Expr::Literal(Literal::I32(1))),
            }),
        ];
        for predicate in forms {
            let selection = Selection { predicate: predicate.clone(), order_by: None, limit: None };
            assert!(matches!(selection.check(), Err(SelectionError::UnresolvedPath(_))), "did not recurse into {predicate:?}");
        }
    }

    #[test]
    fn check_rejects_unpopulated_placeholders() {
        // A placeholder survives resolution untouched, so an unpopulated one is
        // its own class of leak: it carries no value to compare against.
        for predicate in [Predicate::Placeholder, cmp_with(Expr::Placeholder)] {
            let selection = Selection { predicate, order_by: None, limit: None };
            assert!(matches!(selection.check(), Err(SelectionError::UnpopulatedPlaceholder)));
        }
    }

    #[test]
    fn check_covers_order_by_not_just_the_predicate() {
        let resolved = cmp_with(Expr::PropertyPath(PropertyPath::id(vec![])));
        let order_by = |key| Selection {
            predicate: resolved.clone(),
            order_by: Some(vec![OrderByItem { key, direction: OrderDirection::Asc }]),
            limit: None,
        };

        assert!(order_by(OrderKey::Property(PropertyPath::system("name", vec![]))).check().is_ok());

        let err = order_by(OrderKey::Path(PathExpr::simple("name"))).check().expect_err("a raw sort key carries no identity");
        assert!(matches!(&err, SelectionError::UnresolvedPath(p) if p == "name"), "got {err:?}");
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
    fn resolved_order_by_bincode_roundtrip() {
        let property = Ulid::from_bytes([9u8; 16]);
        let mut selection = identifier_selection("score", vec![]);
        selection.order_by = Some(vec![OrderByItem {
            key: OrderKey::Property(PropertyPath::registered(property, "score", vec![])),
            direction: OrderDirection::Desc,
        }]);

        let bytes = bincode::serialize(&selection).expect("serialize");
        let decoded: Selection = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(decoded, selection);
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
    fn identifier_assume_null_keys_on_identity_not_subpath() {
        // A resolved identifier carries a single identity; its subpath does NOT
        // change which property it references. So a JSON-subpath identifier
        // collapses when the PROPERTY is absent, and is untouched when some
        // unrelated identity is absent. This is the whole point of the
        // resolution pass: identity is fixed once, removing the old Path
        // first-vs-last-step ambiguity.
        let id_pred = identifier_selection("licensing", vec!["territory"]).predicate;

        // The property itself absent -> collapse to False.
        assert_eq!(id_pred.assume_null(&[identifier_selection_id()]), Predicate::False);
        // Some unrelated identity absent (e.g. a would-be subpath step) -> unchanged.
        assert_eq!(id_pred.assume_null(&[PropertyId::System { name: "territory".to_string() }]), id_pred);
    }

    #[test]
    fn identifier_referenced_properties_returns_identity() {
        // referenced_properties reports the resolved identity, regardless of subpath.
        let id_sel = identifier_selection("licensing", vec!["territory"]);
        assert_eq!(id_sel.referenced_properties(), vec![identifier_selection_id()]);

        // A simple identifier reports the same identity shape.
        assert_eq!(identifier_selection("status", vec![]).referenced_properties(), vec![identifier_selection_id()]);
    }

    #[test]
    fn identifier_display_matches_path() {
        // Display renders the resolved-from label then dotted subpath, consistent with PathExpr.
        let ident = PropertyPath::registered(Ulid::from_bytes([1u8; 16]), "licensing", vec!["territory".to_string()]);
        assert_eq!(ident.to_string(), "licensing.territory");
        assert_eq!(ident.to_string(), PathExpr { steps: vec!["licensing".to_string(), "territory".to_string()] }.to_string());

        let simple = PropertyPath::registered(Ulid::from_bytes([1u8; 16]), "status", vec![]);
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
