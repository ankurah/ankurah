//! Property and model references used through the query pipeline.

use crate::ast::{ModelId, PathExpr, PropertyPath};

/// Fixes property and model reference representations for an entire query tree.
pub trait Stage: Clone + std::fmt::Debug + PartialEq + 'static {
    /// How a property reference is written at this stage.
    type Path: ankurah_core_types::Path + Clone + std::fmt::Debug + std::fmt::Display + PartialEq;
    /// How a model reference is written at this stage.
    type ModelId: Clone + std::fmt::Debug + std::fmt::Display + Ord;
}

/// A model reference before label resolution.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, strum::Display)]
pub enum ModelRef {
    #[strum(transparent)]
    Id(ModelId),
    #[strum(transparent)]
    Label(String),
}

impl ModelRef {
    pub fn as_id(&self) -> Option<&ModelId> {
        match self {
            Self::Id(id) => Some(id),
            Self::Label(_) => None,
        }
    }
}

impl From<ModelId> for ModelRef {
    fn from(id: ModelId) -> Self { Self::Id(id) }
}

impl From<String> for ModelRef {
    fn from(label: String) -> Self { Self::Label(label) }
}

impl From<&str> for ModelRef {
    fn from(label: &str) -> Self { Self::Label(label.into()) }
}

/// Unresolved model references and model-scoped property names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parsed;

impl Stage for Parsed {
    type Path = PathExpr;
    type ModelId = ModelRef;
}

/// Property and model references bound to durable identities; this stage crosses the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved;

impl Stage for Resolved {
    type Path = PropertyPath;
    type ModelId = ModelId;
}
