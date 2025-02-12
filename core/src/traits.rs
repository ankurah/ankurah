use std::ops::Deref;
use std::sync::{Arc, Weak};

use crate::connector::PeerSender;
use crate::{
    error::RetrievalError,
    policy::AccessResult,
    proto::{CollectionId, Event, NodeId, State, ID},
};
use ankql::ast::Predicate;
use ankurah_proto as proto;
use async_trait::async_trait;
use std::sync::Arc;

/// Optional trait that allows storage operations to be scoped to a specific namespace.
/// For multitenancy or otherwise. Presumably the Context will implement this trait.
/// Storage engines may implement namespace-aware storage to partition data.
pub trait Namespace {
    /// Returns the namespace for this context, if any
    fn namespace(&self) -> Option<&str>;
}
