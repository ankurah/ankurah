//! The lifecycle stage of a selection: which single representation its
//! property paths take at that point in its life.
//!
//! A selection is parsed, then resolved, then handed to a storage engine, and
//! each of those lives addresses a property differently: by the name the
//! caller wrote, by the durable identity that name bound to, and by whatever
//! the engine's own vocabulary is. A stage names one of those lives and fixes
//! ONE path type for it, so a tree cannot carry two forms at once and no
//! consumer needs a refusal arm for the form it does not accept.
//!
//! [`Parsed`] and [`Resolved`] live here with the generic AST they
//! parameterize, even though the walk between them lives in ankurah-core with
//! the catalog it binds against: the wire structs carry
//! `Selection<Resolved>`, and ankurah-proto sits below core, so a stage the
//! AST cannot name would leave those structs unwritable. A stage for an
//! engine's own vocabulary is defined by the crate that owns that vocabulary.

use crate::ast::{PathExpr, PropertyPath};

/// One stage of a selection's life, naming the path representation its tree
/// carries. The trait is open on purpose: a storage engine addresses
/// properties in its own terms (a column name, a document field) and defines
/// the stage for them alongside the lowering that produces it, without this
/// crate having to know about it.
pub trait Stage: Clone + std::fmt::Debug + PartialEq + 'static {
    /// How a property reference is written at this stage.
    type Path: Clone + std::fmt::Debug + std::fmt::Display + PartialEq;
}

/// A selection as written: property paths are the source-level names the
/// caller typed. Only [`crate::parser`] and the `selection!` macro produce
/// this stage, and it is deliberately not serializable -- a name binds to an
/// identity in the model scope it was written against, so it cannot travel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parsed;

impl Stage for Parsed {
    type Path = PathExpr;
}

/// A selection whose every property reference has been bound to a durable
/// [`PropertyId`](ankurah_core_types::PropertyId) by the one resolution walk
/// (`ankurah_core::schema::resolver::resolve_selection`), which owns that
/// binding because the catalog it binds against lives in core. This is the
/// only stage that crosses the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved;

impl Stage for Resolved {
    type Path = PropertyPath;
}
