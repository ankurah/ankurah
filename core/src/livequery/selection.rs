//! A selection's path into a running query: admission -- property names
//! bound to durable identities and the policy's narrowing ANDed in, one
//! judgment -- plus the construction helpers the entry paths share.

use std::sync::Arc;

use ankql::ast::{Parsed, Resolved};
use ankurah_proto::{self as proto, CollectionId};
use ankurah_signals::Mut;
use tracing::debug;

use crate::{
    entity::Entity,
    error::RetrievalError,
    node::erased::ErasedNodeRef,
    policy::PolicyAgent,
    reactor::fetch_gap::{GapFetcher, QueryGapFetcher},
    resultset::EntityResultSet,
    session::SessionSet,
    storage::StorageEngine,
    Node,
};

use super::{EntityLiveQuery, Inner};

/// Admit a selection for `collection_id`: bind every property name to its
/// durable identity and canonicalize comparison values, then let the policy
/// narrow what came back. The agent ANDs its own conditions in in the same
/// resolved vocabulary the reactor and the relay consume, so nothing past
/// this point is left to bind.
///
/// `schema` is the compiled declaration a typed query was written against,
/// and `None` for a raw one that names its collection by string: the typed
/// form binds field names through the descriptor's cells, the raw form
/// through the catalog's current display names.
///
/// A catalog collection skips the agent entirely, on both counts
/// ([`crate::schema::reads_bypass_policy`]): the catalog projection runs
/// before this node has a credential to be judged under, and it is what makes
/// every other query resolvable.
pub(super) fn admit<SE, PA>(
    node: &Node<SE, PA>,
    sessions: &SessionSet<PA::ContextData>,
    schema: Option<&'static crate::schema::ModelStructDescriptor>,
    collection_id: &CollectionId,
    selection: ankql::ast::Selection<Parsed>,
) -> Result<ankql::ast::Selection<Resolved>, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let exempt = crate::schema::reads_bypass_policy(collection_id);
    // One credential snapshot for the whole derivation; re-derivation
    // on change arrives with https://github.com/ankurah/ankurah/pull/426.
    let cdata = sessions.current();
    if !exempt {
        node.policy_agent.can_access_collection(&cdata, collection_id)?;
    }
    let mut selection = match schema {
        Some(schema) => node.catalog.resolve_selection_with_descriptor(node, schema, selection)?,
        None => node.catalog.resolve_selection(collection_id, selection)?,
    };
    if !exempt {
        selection.predicate = node.policy_agent.filter_predicate(&cdata, collection_id, selection.predicate)?;
    }
    Ok(selection)
}

/// Helper: create the Inner shared by every constructor. `selection` is the
/// admitted selection, or `None` when [`start_admitted`] installs it a moment
/// later; nothing runs against the query until one is there.
pub(super) fn create_inner<SE, PA>(
    node: &Node<SE, PA>,
    node_ref: Box<dyn ErasedNodeRef>,
    schema: Option<&'static crate::schema::ModelStructDescriptor>,
    parsed: ankql::ast::Selection<Parsed>,
    collection_id: CollectionId,
    selection: Option<ankql::ast::Selection<Resolved>>,
    sessions: SessionSet<PA::ContextData>,
) -> (Arc<Inner>, proto::QueryId)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let subscription = node.reactor.subscribe();

    let resultset = EntityResultSet::empty();
    let query_id = proto::QueryId::new();
    let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(&node, sessions));

    let inner = Arc::new(Inner {
        query_id,
        node: node_ref,
        subscription,
        resultset: resultset.clone(),
        error: Mut::new(None),
        initialized: tokio::sync::Notify::new(),
        initialized_version: std::sync::atomic::AtomicU32::new(0), // 0 means uninitialized
        durable_version: std::sync::atomic::AtomicU32::new(0),
        durable_notify: tokio::sync::Notify::new(),
        current_version: std::sync::atomic::AtomicU32::new(1), // Start at version 1
        selection: Mut::new(selection.map(|selection| (selection, 1))), // Start with version 1
        collection_id: collection_id.clone(),
        gap_fetcher,
        schema,
        parsed: Mut::new(parsed),
    });

    (inner, query_id)
}

/// Install an admitted selection and set the query running for this node
/// kind: a durable node (and any cached query) activates against local
/// storage, and an ephemeral node registers the query with its relay, whose
/// established callback activates it once the remote's deltas are applied.
/// A cached ephemeral query does BOTH -- storage serves what it already holds
/// while the remote subscription refreshes it; these are not alternatives.
pub(super) fn start_admitted<SE, PA>(
    inner: &Arc<Inner>,
    node: &Node<SE, PA>,
    me: &EntityLiveQuery,
    selection: ankql::ast::Selection<Resolved>,
    cached: bool,
    sessions: SessionSet<PA::ContextData>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let query_id = inner.query_id;
    inner.selection.set(Some((selection.clone(), 1)));

    let has_relay = node.subscription_relay.is_some();
    if cached || !has_relay {
        let inner = inner.clone();
        debug!("LiveQuery spawning initialization task for predicate {}", query_id);
        crate::task::spawn(async move {
            debug!("LiveQuery initialization task starting for predicate {}", query_id);
            if let Err(e) = inner.activate(1).await {
                debug!("LiveQuery initialization failed for predicate {}: {}", query_id, e);
                inner.fail_initialization(e);
            } else {
                debug!("LiveQuery initialization completed for predicate {}", query_id);
            }
        });
    }
    if has_relay {
        node.subscribe_remote_query(query_id, inner.collection_id.clone(), selection, sessions, 1, me.weak());
    } else {
        // No relay: this query's own storage is the authority, and it
        // answers each version as it starts.
        inner.mark_durable_answered(1);
    }
}

/// An admission that could not be judged synchronously, finished in a
/// spawned task with failures landing in the query's error slot.
pub(super) enum DeferredAdmission {
    /// A typed entry whose declaration this system has never been told
    /// about: healing it means registering, which awaits the allocator.
    Typed(&'static crate::schema::ModelStructDescriptor, ankql::ast::Selection<Parsed>),
    /// A raw entry that failed to admit before the catalog synced: only a
    /// synced catalog makes a raw miss authoritative, so it re-admits once
    /// the durable's catalog rows are applied.
    Raw(ankql::ast::Selection<Parsed>),
}
