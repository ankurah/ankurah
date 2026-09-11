use crate::internal::prelude::*;
use ankql::ast::Parsed;
use std::sync::Arc;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

mod erased;
mod inner;
mod schema_resolver;

pub(crate) use erased::DynContextInner;
pub(crate) use inner::{ContextAuth, ContextInner};
pub(crate) use schema_resolver::SchemaResolver;

/// A local scope for reads and writes backed by a live credential source.
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
#[derive(Clone)]
pub struct Context(pub(crate) Arc<dyn DynContextInner + Send + Sync + 'static>);

// This whole impl is conditionalized by the wasm feature flag
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Context {
    #[wasm_bindgen(js_name = "node_id")]
    pub fn js_node_id(&self) -> proto::EntityId { self.0.node_id() }
}

// Generic methods cannot cross the wasm_bindgen boundary; they live in this
// plain impl and remain host-and-wasm callable from Rust.
impl Context {
    /// Register `M` and return its durable model id. Repeated calls are no-ops.
    pub async fn register_model<M: crate::model::Model>(&self) -> Result<proto::ModelId, crate::schema::registration::RegistrationError> {
        self.0.schema_resolver().ensure_registered(M::descriptor()).await.map(|(model, _epoch)| model)
    }
}

// This impl may or may not have the wasm_bindgen attribute but the functions will always be defined
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", uniffi::export)]
impl Context {
    /// Begin a transaction.
    pub fn begin(&self) -> Transaction { Transaction::new(self.0.clone()) }
}

impl Context {
    /// Type-erased query and transaction initiation context
    pub fn new<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: Node<SE, PA>,
        sessions: impl Into<crate::session::SessionSet<PA::ContextData>>,
    ) -> Self {
        let sessions = sessions.into();
        // Attach the source to the node's registry: a live edge keeping
        // it the continuous superset of every session backing a context
        // (a no-op when the source IS the registry).
        node.sessions.attach(&sessions);
        Self(Arc::new(ContextInner { node: NodeHandle::Strong(node), auth: ContextAuth::Sessions(sessions) }))
    }

    /// A context that does NOT keep the node alive, for node-owned machinery
    /// (the catalog projection) whose strong context would cycle.
    pub(crate) fn new_weak<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: &Node<SE, PA>,
        sessions: impl Into<crate::session::SessionSet<PA::ContextData>>,
    ) -> Self {
        let sessions = sessions.into();
        node.sessions.attach(&sessions);
        Self(Arc::new(ContextInner { node: NodeHandle::Weak(node.weak()), auth: ContextAuth::Sessions(sessions) }))
    }

    pub fn node_id(&self) -> proto::EntityId { self.0.node_id() }

    pub async fn get<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        use crate::model::Model;
        self.0.schema_resolver().ensure_registered(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(&R::collection(), id, false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, allowing a local result when no durable peer is connected.
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        use crate::model::Model;
        self.0.schema_resolver().ensure_registered(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(&R::collection(), id, true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<Vec<R>, RetrievalError> {
        let args: MatchArgs<Parsed> = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        self.0.schema_resolver().ensure_registered(R::Model::descriptor()).await?;
        let collection_id = R::Model::collection();
        let args = MatchArgs {
            selection: self.0.schema_resolver().resolve_selection_with_descriptor(R::Model::descriptor(), args.selection)?,
            cached: args.cached,
        };

        let entities = self.0.fetch_entities(&collection_id, args).await?;

        Ok(entities.into_iter().map(|e| R::from_entity(e)).collect())
    }

    pub async fn fetch_one<R: View + Clone + 'static>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<Option<R>, RetrievalError> {
        let views = self.fetch::<R>(args).await?;
        Ok(views.into_iter().next())
    }
    /// Subscribe to a typed selection, rejecting unknown fields immediately.
    /// When registration is needed, initialization failures appear in the query's
    /// error signal and are returned by `wait_initialized`.
    pub fn query<R>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        let args: MatchArgs<Parsed> = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        Ok(self.0.clone().query(Some(R::Model::descriptor()), R::Model::collection(), args)?.map::<R>())
    }

    /// Subscribe to changes in entities matching a selection and wait for initialization
    pub async fn query_wait<R>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        let livequery = self.query::<R>(args)?;
        livequery.wait_initialized().await?;
        Ok(livequery)
    }

    /// Open a storage collection for tests, bypassing context policy checks.
    #[cfg(feature = "test-helpers")]
    pub async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.0.collection(id).await
    }
}
