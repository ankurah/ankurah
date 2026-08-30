//! The node-held registry of live queries: what a system reset sweeps.

use ankql::ast::Parsed;
use ankurah_proto::{self as proto, CollectionId};

use crate::{node::WeakNode, policy::PolicyAgent, session::SessionSet, storage::StorageEngine, util::safemap::SafeMap};

use super::selection::admit;
use super::WeakEntityLiveQuery;

/// The node-held registry of every live query on that node: one entry per
/// query, weak in both directions, entered at construction and removed when
/// the query's `Inner` drops. A system reset sweeps it -- every retained
/// query is re-admitted under the new system through the same lanes as
/// construction.
pub struct LiveQueryRegistry<SE, PA: PolicyAgent> {
    entries: SafeMap<proto::QueryId, RegistryEntry<SE, PA>>,
}

/// One registered query: a weak handle to the query itself plus what its
/// reset re-admission needs beyond the query's own retained state -- the
/// session set it was admitted under and a weak handle to its typed node.
pub(super) struct RegistryEntry<SE, PA: PolicyAgent> {
    pub(super) query: WeakEntityLiveQuery,
    pub(super) collection: CollectionId,
    pub(super) sessions: SessionSet<PA::ContextData>,
    pub(super) node: WeakNode<SE, PA>,
}

impl<SE, PA: PolicyAgent> Clone for RegistryEntry<SE, PA> {
    fn clone(&self) -> Self {
        Self { query: self.query.clone(), collection: self.collection.clone(), sessions: self.sessions.clone(), node: self.node.clone() }
    }
}

impl<SE, PA: PolicyAgent> LiveQueryRegistry<SE, PA> {
    pub(crate) fn new() -> Self { Self { entries: SafeMap::new() } }

    pub(super) fn insert(&self, query_id: proto::QueryId, entry: RegistryEntry<SE, PA>) { self.entries.insert(query_id, entry); }

    pub(crate) fn remove(&self, query_id: &proto::QueryId) { self.entries.remove(query_id); }
}

impl<SE, PA> LiveQueryRegistry<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// A system reset voids every registered query's resolved selection (its
    /// identities are epoch-scoped). Called right after the reactor clears
    /// the resultsets: bump each query's version synchronously so both waits
    /// go stale immediately, then re-admit every retained name-form input
    /// under the new system in one spawned task.
    pub(crate) fn system_reset(&self) {
        let mut sweeps = Vec::new();
        for (query_id, entry) in self.entries.to_vec() {
            let Some(query) = entry.query.upgrade() else {
                self.remove(&query_id);
                continue;
            };
            let inner = &query.0;
            let version = inner.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            inner.resultset.set_loaded(false);
            sweeps.push(ResetSweepEntry {
                query: entry.query,
                node: entry.node,
                sessions: entry.sessions,
                schema: inner.schema,
                parsed: inner.parsed.value(),
                collection: inner.collection_id.clone(),
                version,
            });
        }
        if !sweeps.is_empty() {
            crate::task::spawn(readmit_after_reset(sweeps));
        }
    }
}

/// One re-admission unit of a system-reset sweep: the query's retained
/// admission input plus the handles the admission lanes need. Weak on both
/// the query and the node -- the sweep must keep neither alive.
struct ResetSweepEntry<SE, PA: PolicyAgent> {
    query: WeakEntityLiveQuery,
    node: WeakNode<SE, PA>,
    sessions: SessionSet<PA::ContextData>,
    schema: Option<&'static crate::schema::ModelStructDescriptor>,
    parsed: ankql::ast::Selection<Parsed>,
    collection: CollectionId,
    version: u32,
}

/// The sweep behind [`LiveQueryRegistry::system_reset`]: wait out the join
/// that follows the reset, then re-admit each query under the new system --
/// typed registration for a typed entry, catalog sync for a raw one -- and
/// install the result under the version its reset assigned. A query or node
/// that died in the meantime is skipped; a failed re-admission lands in that
/// query's error slot and the sweep continues.
async fn readmit_after_reset<SE, PA>(sweeps: Vec<ResetSweepEntry<SE, PA>>)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    for sweep in sweeps {
        // Wait without keeping the node alive through the wait. The first
        // entry rides out the join here; readiness makes the rest instant.
        let Some(system) = sweep.node.upgrade().map(|node| node.system.clone()) else { continue };
        system.wait_system_ready().await;
        let Some(node) = sweep.node.upgrade() else { continue };
        let admitted = match sweep.schema {
            Some(schema) => match crate::context::register_for_read(&node, &sweep.sessions, schema).await {
                Ok(()) => admit(&node, &sweep.sessions, Some(schema), &sweep.collection, sweep.parsed),
                Err(error) => Err(error),
            },
            None => match node.catalog.wait_synced().await {
                Ok(()) => admit(&node, &sweep.sessions, None, &sweep.collection, sweep.parsed),
                Err(error) => Err(error),
            },
        };
        let Some(query) = sweep.query.upgrade() else { continue };
        match admitted {
            Ok(resolved) => {
                if node.subscription_relay.is_some() {
                    // The reset wiped local storage, and the peer's delta
                    // baseline with it: a standing subscription updated in
                    // place would be diffed against rows this node no longer
                    // holds and answered with nothing. Re-subscribe -- the
                    // peer resets its baseline to this request's own
                    // known_matches and serves the full selection again.
                    query.0.selection.set(Some((resolved.clone(), sweep.version)));
                    node.subscribe_remote_query(
                        query.0.query_id,
                        sweep.collection.clone(),
                        resolved,
                        sweep.sessions.clone(),
                        sweep.version,
                        query.weak(),
                    );
                } else if let Err(error) = query.install_selection_update(Box::new(node), resolved, sweep.version) {
                    query.0.fail_initialization(error);
                }
            }
            Err(error) => query.0.fail_initialization(error),
        }
    }
}
