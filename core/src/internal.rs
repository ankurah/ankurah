//! Crate-internal conveniences.

/// The names most core files speak: `use crate::internal::prelude::*;`
/// replaces the per-file import block for them. Explicit imports and local
/// definitions shadow the glob, so adoption is per-file.
pub(crate) mod prelude {
    #![allow(unused_imports)] // the roster serves adopting files, not this one

    pub(crate) use crate::changes::{ChangeSet, EntityChange};
    pub(crate) use crate::entity::Entity;
    pub(crate) use crate::error::{MutationError, RetrievalError};
    pub(crate) use crate::livequery::{EntityLiveQuery, LiveQuery, WeakEntityLiveQuery};
    pub(crate) use crate::model::View;
    pub(crate) use crate::node::{MatchArgs, Node, NodeRef, NodeType, TNodeErased, WeakNode};
    pub(crate) use crate::policy::{AccessDenied, PolicyAgent};
    pub(crate) use crate::resultset::EntityResultSet;
    pub(crate) use crate::schema::catalog::CatalogManager;
    pub(crate) use crate::schema::{ModelStructDescriptor, SchemaEpoch};
    pub(crate) use crate::session::SessionSet;
    pub(crate) use crate::storage::{StorageCollectionWrapper, StorageEngine};
    pub(crate) use crate::transaction::Transaction;
    pub(crate) use ankurah_proto::{self as proto, CollectionId};
}
