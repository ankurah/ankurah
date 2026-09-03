//! Property-path representations used through the query pipeline.

use crate::ast::{PathExpr, PropertyPath};

/// Fixes one path representation for an entire query tree.
pub trait Stage: Clone + std::fmt::Debug + PartialEq + 'static {
    /// How a property reference is written at this stage.
    type Path: Clone + std::fmt::Debug + std::fmt::Display + PartialEq;
}

/// Source-level property names, meaningful only within a model scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parsed;

impl Stage for Parsed {
    type Path = PathExpr;
}

/// Property references bound to durable identities; this stage crosses the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved;

impl Stage for Resolved {
    type Path = PropertyPath;
}
