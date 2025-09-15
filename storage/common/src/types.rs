use ankurah_core::property::PropertyValue;

use crate::index_spec::IndexSpec;

// TODO
// Build IndexBounds per keypart from WHERE/ORDER BY (like PG’s per-column ScanKey/IndexBounds).
// Run a normalization pass to produce one CanonicalRange:
// Walk columns left→right, accumulate equality prefix.
// At first non-equality column, materialize the side(s); UnboundedHigh(_) ⇒ upper=None; UnboundedLow(_) ⇒ shorten lower tuple.
// Collapse stacked ±∞ at the first occurrence.
// Detect empties via lexicographic compare with open/closed flags.
// Lowering:
// Engines with true sentinels/unbounded: map None appropriately.
// IndexedDB: upper=None ⇒ IDBKeyRange.lowerBound(lower, open) + stop on prefix change; finite-finite ⇒ IDBKeyRange.bound(...); empty ⇒ no scan.
// This gives you PG-style, per-column correctness in the IR and a clean, safe path to concrete engine ranges.

// --- Plan (similar to PG IndexScan/IndexOnlyScan inputs) --------------------------------
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    Index {
        index_spec: IndexSpec,                        // key order (ASC/DESC per part)
        scan_direction: ScanDirection,                // engine scan direction
        bounds: IndexBounds,                          // per-column bounds (planner IR)
        remaining_predicate: ankql::ast::Predicate,   // residual quals
        order_by_spill: Vec<ankql::ast::OrderByItem>, // extra sort keys
    },
    TableScan {
        bounds: IndexBounds, // primary key bounds (empty if no constraints). TODO: Consider renaming IndexBounds to KeyBounds for clarity
        scan_direction: ScanDirection, // forward/reverse based on primary key ORDER BY
        remaining_predicate: ankql::ast::Predicate, // all predicates (no index to satisfy any)
        order_by_spill: Vec<ankql::ast::OrderByItem>, // ORDER BY fields not satisfied by scan direction
    },
    EmptyScan, // "scan" over an emptyset - the query can never match anything
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

// --- Types & sentinels -------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
    I16,
    I32,
    I64,
    Bool,
    String,
    Object,
    Binary,
}

impl ValueType {
    pub fn of(v: &PropertyValue) -> Self {
        match v {
            PropertyValue::I16(_) => ValueType::I16,
            PropertyValue::I32(_) => ValueType::I32,
            PropertyValue::I64(_) => ValueType::I64,
            PropertyValue::Bool(_) => ValueType::Bool,
            PropertyValue::String(_) => ValueType::String,
            PropertyValue::Object(_) => ValueType::Object,
            PropertyValue::Binary(_) => ValueType::Binary,
        }
    }
}

/// Planner-only atom for a single column position (PG: like a Datum + flags).
#[derive(Debug, Clone, PartialEq)]
pub enum KeyDatum {
    Val(PropertyValue),
    NegInfinity(ValueType), // -∞ for this column’s type
    PosInfinity(ValueType), // +∞ for this column’s type
}

impl KeyDatum {
    pub fn ty(&self) -> ValueType {
        match self {
            KeyDatum::Val(v) => ValueType::of(v),
            KeyDatum::NegInfinity(t) | KeyDatum::PosInfinity(t) => *t,
        }
    }
}

impl From<PropertyValue> for KeyDatum {
    fn from(v: PropertyValue) -> Self { KeyDatum::Val(v) }
}

// --- Endpoints & per-column bounds (PG: per-column ScanKey / bound) ----------

/// Endpoint for one side of a column bound (PG: strategy + flags collapsed).
#[derive(Debug, Clone, PartialEq)]
pub enum Endpoint {
    UnboundedLow(ValueType),                    // (-∞, …  for this column)
    UnboundedHigh(ValueType),                   // …, +∞) for this column
    Value { datum: KeyDatum, inclusive: bool }, // <= / <  or >= / >
}

impl Endpoint {
    pub fn incl(v: PropertyValue) -> Self { Endpoint::Value { datum: KeyDatum::Val(v), inclusive: true } }
    pub fn excl(v: PropertyValue) -> Self { Endpoint::Value { datum: KeyDatum::Val(v), inclusive: false } }
}

/// Bound for a single index column, in index key order (PG: per keypart).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumnBound {
    pub column: String, // column / keypart name (optional but handy)
    pub low: Endpoint,  // lower endpoint for this column
    pub high: Endpoint, // upper endpoint for this column
}

// --- Multi-column bounds (PG: IndexBounds) -----------------------------------

/// Full multi-column bounds for an index scan (PG: IndexBounds).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexBounds {
    pub keyparts: Vec<IndexColumnBound>, // one per index column, in order
}

impl IndexBounds {
    pub fn new(keyparts: Vec<IndexColumnBound>) -> Self { Self { keyparts } }
    pub fn empty() -> Self { Self { keyparts: vec![] } }
}

// --- Canonical, lexicographic interval after normalization -------------------

/// Canonical lexicographic interval (possibly open-ended) ready for lowering.
/// lower/upper: (tuple, open?) where open==true means exclusive.
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalRange {
    pub lower: Option<(Vec<PropertyValue>, bool)>, // None => unbounded low
    pub upper: Option<(Vec<PropertyValue>, bool)>, // None => unbounded high
}
