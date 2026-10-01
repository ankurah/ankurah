use crate::{context::SchemaResolver, error::RetrievalError, schema::ModelStructDescriptor};
use ankql::ast::{Parsed, Resolved, Selection};
use futures::future::BoxFuture;

/// A livequery's selection, resolved now or as soon as the node can resolve it.
///
/// Resolution turns field names into property IDs, adds `MemberOf(model)`, and applies the
/// context's read policy. A livequery subscribes only once it holds a resolved selection.
///
/// ```text
/// ctx.query::<AlbumView>("year > 2020")   catalog loaded and has Album    =>  Resolved(selection)
/// ctx.query::<AlbumView>("year > 2020")   catalog loading, or no Album    =>  Pending(future)
/// ctx.query::<AlbumView>("bogus = 1")     unknown field                   =>  Err(..) immediately
/// ```
///
/// A `Pending` future waits for the system and catalog, registers the model if the catalog lacks it,
/// then resolves. The livequery installs its result unless a newer selection has replaced it.
pub(crate) enum QueryResolution {
    /// Ready to subscribe immediately.
    Resolved(Selection<Resolved>),
    /// Readiness or registration still pending; the livequery spawns this future and installs its result.
    Pending(BoxFuture<'static, Result<Selection<Resolved>, RetrievalError>>),
}

impl QueryResolution {
    /// Resolve now if possible, deferring only when readiness or a missing binding blocks it.
    /// Every other error, such as an unknown field, returns immediately.
    pub(crate) fn prepare(
        resolver: &dyn SchemaResolver,
        schema: &'static ModelStructDescriptor,
        selection: Selection<Parsed>,
    ) -> Result<Self, RetrievalError> {
        match resolver.resolve_query_selection(schema, selection.clone()) {
            Ok(selection) => Ok(Self::Resolved(selection)),
            Err(RetrievalError::NodeNotReady | RetrievalError::UnboundDeclaration { .. }) => {
                Ok(Self::Pending(resolver.resolve_query_selection_when_ready(schema, selection)))
            }
            Err(error) => Err(error),
        }
    }
}
