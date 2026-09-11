mod model;
pub(crate) mod register;
pub mod resolver;
pub use model::*;
use resolver::ModelResolutionError;

use std::collections::HashMap;
use std::sync::OnceLock;

use ankql::ast::{Parsed, Predicate, Resolved, Selection};
use ankurah_proto::{self as proto, CollectionId, EntityId, PropertyId};
use ankurah_signals::signal::{Calculated, Get};
use futures::FutureExt;

use crate::{
    context::Context,
    error::{NodeDropped, RetrievalError},
    livequery::LiveQuery,
    model::View,
    node::{MatchArgs, WeakNode},
    policy::PolicyAgent,
    session::SessionSet,
    storage::StorageEngine,
    value::ValueType,
};

const CACHED_TRUE: MatchArgs<Parsed> =
    MatchArgs { selection: Selection { predicate: Predicate::True, order_by: None, limit: None }, cached: true };

#[derive(Clone, Copy)]
enum NameSlot {
    Unique(EntityId),
    Ambiguous(EntityId, EntityId),
}

/// Local projection failures, excluding transient peer errors that leave cached rows usable.
fn local_read_failure(error: &RetrievalError) -> bool {
    match error {
        RetrievalError::StorageError(_)
        | RetrievalError::DeserializationError(_)
        | RetrievalError::DecodeError(_)
        | RetrievalError::InvalidState(_)
        | RetrievalError::FailedUpdate(_)
        | RetrievalError::StateError(_)
        | RetrievalError::PropertyError(_)
        | RetrievalError::EventNotFound(_) => true,
        RetrievalError::Anyhow(error) => error.downcast_ref::<RetrievalError>().is_some_and(local_read_failure),
        _ => false,
    }
}

struct CatalogIndex {
    models_by_label: HashMap<String, (EntityId, SysModelRow)>,
    models_by_id: HashMap<EntityId, SysModelRow>,
    properties: HashMap<EntityId, SysPropertyRow>,
    memberships: HashMap<(EntityId, EntityId), (EntityId, SysModelPropertyRow)>,
    names: HashMap<EntityId, HashMap<String, NameSlot>>,
}

#[derive(Default)]
pub struct CatalogManager {
    index: OnceLock<Calculated<CatalogIndex>>,
    pub(crate) allocator: tokio::sync::Mutex<()>,
}

fn rows<R: View + Clone + 'static>(query: &LiveQuery<R>) -> Vec<(EntityId, R::Model)> {
    // Resultset changes precede readiness; the query notification may follow it.
    ankurah_signals::CurrentObserver::track(&query.resultset());
    if let Some(error) = query.error().get().filter(|error| !query.resultset().is_loaded() || local_read_failure(error)) {
        tracing::warn!("cannot read {} catalog rows: {error}", R::collection());
        return Vec::new();
    }
    query
        .get()
        .into_iter()
        .filter_map(|row| match row.to_model() {
            Ok(model) => Some((row.id(), model)),
            Err(error) => {
                tracing::warn!("skipping unreadable {} row {}: {error}", R::collection(), row.id());
                None
            }
        })
        .collect()
}

impl CatalogManager {
    /// Resolve the complete declaration, registering locally or through a durable peer when needed.
    /// Registration authority is checked only when local resolution cannot satisfy the declaration.
    pub(crate) async fn resolve_or_register<SE, PA, R>(
        &self,
        node: &crate::node::Node<SE, PA>,
        registrant: &mut R,
        auth: register::RegistrationAuth<PA::ContextData>,
    ) -> Result<(), crate::schema::registration::RegistrationError>
    where
        SE: crate::storage::StorageEngine + Send + Sync + 'static,
        PA: crate::policy::PolicyAgent + Send + Sync + 'static,
        R: register::Registrant,
    {
        register::resolve_or_register(self, node, registrant, auth).await
    }

    /// Populate the registrant using only the local catalog; never register it.
    /// Returns [`RetrievalError::UnboundDeclaration`] when the complete declaration
    /// cannot be bound and catalog synchronization or registration is needed.
    pub(crate) fn resolve_local<R: register::Registrant>(
        &self,
        registrant: &mut R,
    ) -> Result<(), crate::schema::registration::RegistrationError> {
        register::resolve_local(self, registrant)
    }

    pub fn resolve_selection(
        &self,
        collection: &CollectionId,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        let model = self
            .model_id_for(collection.as_str())?
            .ok_or_else(|| RetrievalError::Other(format!("collection '{collection}' is not a registered model")))?;
        resolver::resolve_selection(&model, self, selection).map_err(Into::into)
    }

    /// Initialize the index and wait for all three catalog queries' durable answers.
    pub(crate) async fn start<SE, PA>(&self, node: WeakNode<SE, PA>) -> Result<(), RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node = node.upgrade().ok_or(NodeDropped)?;
        let ctx = Context::new_weak(&node, SessionSet::new());
        let models = ctx.query::<SysModelRowView>(CACHED_TRUE)?;
        let properties = ctx.query::<SysPropertyRowView>(CACHED_TRUE)?;
        let memberships = ctx.query::<SysModelPropertyRowView>(CACHED_TRUE)?;
        let ready = futures::future::try_join_all([
            models.wait_durable_answered().boxed(),
            properties.wait_durable_answered().boxed(),
            memberships.wait_durable_answered().boxed(),
        ]);
        let index = {
            let models = models.clone();
            let memberships = memberships.clone();
            let properties = properties.clone();
            Calculated::new(move || {
                let mut models_by_label = HashMap::new();
                let mut models_by_id = HashMap::new();
                for (id, model) in rows(&models) {
                    models_by_label.entry(model.label.clone()).or_insert((id, model.clone()));
                    models_by_id.entry(id).or_insert(model);
                }

                let properties: HashMap<_, _> = rows(&properties).into_iter().collect();
                let memberships: HashMap<_, _> =
                    rows(&memberships).into_iter().map(|(id, row)| ((row.model, row.property), (id, row))).collect();
                let mut names: HashMap<EntityId, HashMap<String, NameSlot>> = HashMap::new();
                for (_, row) in memberships.values() {
                    let Some(property) = properties.get(&row.property) else { continue };
                    match names.entry(row.model).or_default().entry(property.name.clone()) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(NameSlot::Unique(row.property));
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            if let NameSlot::Unique(first) = *slot.get() {
                                if first != row.property {
                                    slot.insert(NameSlot::Ambiguous(first, row.property));
                                }
                            }
                        }
                    }
                }
                CatalogIndex { models_by_label, models_by_id, properties, memberships, names }
            })
        };

        self.index.set(index).map_err(|_| RetrievalError::Other("catalog already initialized".into()))?;
        drop(node);
        ready.await?;
        Ok(())
    }

    pub fn property_id(&self, model: &proto::ModelId, name: &str) -> Result<Option<PropertyId>, RetrievalError> {
        self.try_resolve(model, name)
    }

    pub(crate) fn try_resolve(&self, model: &proto::ModelId, name: &str) -> Result<Option<PropertyId>, RetrievalError> {
        let model = match model {
            proto::ModelId::EntityId(model) => model,
            proto::ModelId::System(system) => {
                return Ok(super::resolver::resolve_system_property(*system, name).map(PropertyId::System));
            }
        };
        let Some(index) = self.index.get() else { return Ok(None) };
        index.peek_with(|index| match index.names.get(model).and_then(|names| names.get(name)) {
            None => Ok(None),
            Some(NameSlot::Unique(id)) => Ok(Some(PropertyId::EntityId(*id))),
            Some(NameSlot::Ambiguous(first, second)) => Err(RetrievalError::Other(format!(
                "property '{name}' in model {model} is ambiguous across durable identities {first} and {second}"
            ))),
        })
    }

    fn registered_value_type(&self, model: &proto::ModelId, property: &PropertyId) -> Result<ValueType, ModelResolutionError> {
        let lookup_failed =
            |message: &str| ModelResolutionError::ValueTypeLookup { model: *model, property: *property, message: message.into() };
        match property {
            PropertyId::Id => Ok(ValueType::EntityId),
            PropertyId::System(system) => Ok(resolver::system_property_value_type(*system)),
            PropertyId::EntityId(id) => self
                .with_property_by_id(id, |property| {
                    ValueType::from_property_str(&property.value_type)
                        .ok_or_else(|| lookup_failed(&format!("unparseable registered type '{}'", property.value_type)))
                })?
                .ok_or_else(|| lookup_failed("no catalog definition"))?,
        }
    }

    fn with_property_by_id<R>(&self, id: &EntityId, f: impl FnOnce(&SysPropertyRow) -> R) -> Result<Option<R>, RetrievalError> {
        let Some(index) = self.index.get() else { return Ok(None) };
        Ok(index.peek_with(|index| index.properties.get(id).map(f)))
    }

    pub fn property_by_id(&self, id: &EntityId) -> Result<Option<SysPropertyRow>, RetrievalError> {
        self.with_property_by_id(id, Clone::clone)
    }

    pub fn property_by_name(&self, model: &EntityId, name: &str) -> Result<Option<(EntityId, SysPropertyRow)>, RetrievalError> {
        let Some(PropertyId::EntityId(id)) = self.try_resolve(&proto::ModelId::EntityId(*model), name)? else { return Ok(None) };
        Ok(self.property_by_id(&id)?.map(|row| (id, row)))
    }

    pub fn model_by_label(&self, label: &str) -> Result<Option<(EntityId, SysModelRow)>, RetrievalError> {
        let Some(index) = self.index.get() else { return Ok(None) };
        Ok(index.peek_with(|index| index.models_by_label.get(label).cloned()))
    }

    pub fn model_by_id(&self, id: &EntityId) -> Result<Option<SysModelRow>, RetrievalError> {
        let Some(index) = self.index.get() else { return Ok(None) };
        Ok(index.peek_with(|index| index.models_by_id.get(id).cloned()))
    }

    pub fn membership(&self, model: &EntityId, property: &EntityId) -> Result<Option<(EntityId, SysModelPropertyRow)>, RetrievalError> {
        let Some(index) = self.index.get() else { return Ok(None) };
        Ok(index.peek_with(|index| index.memberships.get(&(*model, *property)).cloned()))
    }

    pub fn model_id_for(&self, label: &str) -> Result<Option<proto::ModelId>, RetrievalError> {
        if let Some(system) = crate::schema::system_model_id(label) {
            return Ok(Some(system));
        }
        let Some(index) = self.index.get() else { return Ok(None) };
        Ok(index.peek_with(|index| index.models_by_label.get(label).map(|(id, _)| proto::ModelId::EntityId(*id))))
    }

    #[cfg(any(test, feature = "test-helpers"))]
    /// Count decoded catalog rows.
    pub fn counts(&self) -> (usize, usize, usize) {
        self.index
            .get()
            .map_or((0, 0, 0), |index| index.peek_with(|index| (index.models_by_id.len(), index.properties.len(), index.memberships.len())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::Model,
        node::Node,
        policy::{PermissiveAgent, DEFAULT_CONTEXT},
        storage::StorageCollection,
        test_utils::TestStorage,
    };
    use std::{sync::Arc, time::Duration};

    #[derive(ankurah_derive::Model, Debug)]
    #[model(base = "crate", no_ffi)]
    pub struct PublicationModel {
        pub title: String,
    }

    #[derive(ankurah_derive::Model, Debug)]
    #[model(base = "crate", no_ffi)]
    pub struct MissingPublicationModel {
        pub title: String,
    }

    #[tokio::test]
    async fn unreadable_catalog_rows_are_skipped() -> anyhow::Result<()> {
        use crate::{model::Mutable, property::backend::LWWBackend};
        use proto::{SystemModel, SystemProperty};

        for (table, field) in [
            (SystemModel::Model, SystemProperty::Name),
            (SystemModel::Model, SystemProperty::Label),
            (SystemModel::Property, SystemProperty::Backend),
            (SystemModel::Property, SystemProperty::Name),
            (SystemModel::ModelProperty, SystemProperty::Optional),
            (SystemModel::ModelProperty, SystemProperty::Model),
        ] {
            let storage = Arc::new(TestStorage::default());
            let mut node = Node::new_durable(storage.clone(), PermissiveAgent::new());
            node.system.create().await?;
            let context = node.context(DEFAULT_CONTEXT)?;
            context.register_model::<PublicationModel>().await?;
            let healthy = context.register_model::<MissingPublicationModel>().await?;
            let (model, _) = node.catalog.model_by_label(PublicationModel::descriptor().label)?.unwrap();
            let (property, _) = node.catalog.property_by_name(&model, "title")?.unwrap();
            let (membership, _) = node.catalog.membership(&model, &property)?.unwrap();
            let transaction = node.privileged_context().begin();
            let entity = match table {
                SystemModel::Model => transaction.get::<SysModelRow>(&model).await?.entity().clone(),
                SystemModel::Property => transaction.get::<SysPropertyRow>(&property).await?.entity().clone(),
                SystemModel::ModelProperty => transaction.get::<SysModelPropertyRow>(&membership).await?.entity().clone(),
                _ => unreachable!(),
            };
            entity.get_backend::<LWWBackend>()?.set(PropertyId::System(field), None);
            transaction.commit().await?;
            drop(entity);
            drop(context);

            for restored in [false, true] {
                if restored {
                    node = Node::new_durable(storage.clone(), PermissiveAgent::new());
                }
                tokio::time::timeout(Duration::from_secs(2), node.wait_ready()).await??;
                let missing = match table {
                    SystemModel::Model => node.catalog.model_by_id(&model)?.is_none(),
                    SystemModel::Property => node.catalog.property_by_id(&property)?.is_none(),
                    SystemModel::ModelProperty => node.catalog.membership(&model, &property)?.is_none(),
                    _ => unreachable!(),
                };
                assert!(missing, "{table:?}.{field:?}, restored={restored}");
                assert_eq!(node.catalog.model_id_for(MissingPublicationModel::descriptor().label)?, Some(healthy));
                assert_eq!(node.state().value(), crate::NodeState::Running);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn catalog_load_failure_fails_startup_and_pending_queries() -> anyhow::Result<()> {
        for (backend, buffer, expected) in [
            ("lww", vec![0xff], "LWW state buffer"),
            ("lww", vec![], "empty LWW state buffer"),
            ("unknown", vec![], "unknown backend"),
            ("yrs", vec![0xff], "Update failed"),
        ] {
            let storage = Arc::new(TestStorage::default());
            let model = {
                let seed = Node::new_durable(storage.clone(), PermissiveAgent::new());
                seed.system.create().await?;
                seed.context(DEFAULT_CONTEXT)?.register_model::<PublicationModel>().await?;
                seed.catalog.model_by_label(PublicationModel::descriptor().label)?.unwrap().0
            };
            let table = storage.table(&SysModelRow::collection());
            let mut state = table.get_state(model).await?;
            state.payload.state.state_buffers.0.insert(backend.into(), buffer);
            table.set_state(state).await?;
            let (entered, entered_rx) = tokio::sync::oneshot::channel();
            let (release, release_rx) = tokio::sync::oneshot::channel();
            *table.hold_fetch.lock().unwrap() = Some((entered, release_rx));
            let node = Node::new_durable(storage, PermissiveAgent::new());
            node.system.wait_system_ready().await?;
            let query = node.context(DEFAULT_CONTEXT)?.query::<PublicationModelView>("true")?;
            entered_rx.await?;
            release.send(()).unwrap();

            let error = tokio::time::timeout(Duration::from_secs(2), node.wait_ready()).await?.unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            let error = tokio::time::timeout(Duration::from_secs(2), query.wait_initialized()).await?.unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            assert!(matches!(node.state().value(), crate::NodeState::Halted(crate::error::NodeHaltReason::CatalogLoad(_))));
        }
        Ok(())
    }

    #[tokio::test]
    async fn persisted_catalog_alone_does_not_complete_ephemeral_startup() -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let storage = Arc::new(TestStorage::default());
            let original = {
                let seed = Node::new_durable(storage.clone(), PermissiveAgent::new());
                seed.system.create().await?;
                seed.context(DEFAULT_CONTEXT)?.register_model::<PublicationModel>().await?
            };
            let node = Node::new(storage, PermissiveAgent::new());
            node.system.wait_system_ready().await?;
            while node.catalog.counts() != (1, 1, 1) {
                tokio::task::yield_now().await;
            }
            assert_eq!(node.catalog.model_id_for(PublicationModel::descriptor().label)?, Some(original));
            let mut ready = Box::pin(node.wait_ready());
            assert!(futures::poll!(&mut ready).is_pending());
            assert_eq!(node.state().value(), crate::NodeState::Startup);
            assert!(node.get_durable_peers().is_empty());

            let query = node.context(DEFAULT_CONTEXT)?.query::<PublicationModelView>(CACHED_TRUE)?;
            let mut initialized = Box::pin(query.wait_initialized());
            assert!(futures::poll!(&mut initialized).is_pending());
            assert!(query.selection().value().is_none());
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn ready_catalog_reuses_model_after_final_projection_notification() -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let storage = Arc::new(TestStorage::default());
            let original = {
                let seed = Node::new_durable(storage.clone(), PermissiveAgent::new());
                seed.system.create().await?;
                seed.context(DEFAULT_CONTEXT)?.register_model::<PublicationModel>().await?
            };
            let models = storage.table(&SysModelRow::collection());
            let (fetch_entered, fetch_observed) = tokio::sync::oneshot::channel();
            let (release_fetch, resume_fetch) = tokio::sync::oneshot::channel();
            *models.hold_fetch.lock().unwrap() = Some((fetch_entered, resume_fetch));
            let node = Node::new_durable(storage, PermissiveAgent::new());
            let (publication_observed, release_publication) = node.reactor.pause_next_publication(SysModelRow::collection());
            fetch_observed.await?;

            while node.catalog.counts() != (0, 1, 1) {
                tokio::task::yield_now().await;
            }
            let label = PublicationModel::descriptor().label;
            assert_eq!(node.catalog.model_id_for(label)?, None, "prime the index before the last projection fills");
            release_fetch.send(()).unwrap();
            publication_observed.await?;
            assert_eq!(node.catalog.model_id_for(label)?, Some(original));

            let mut ready = Box::pin(node.wait_ready());
            assert!(futures::poll!(&mut ready).is_pending());
            let context = node.context(DEFAULT_CONTEXT)?;
            let mut registration = Box::pin(context.register_model::<PublicationModel>());
            assert!(futures::poll!(&mut registration).is_pending());
            let selection = Selection { predicate: Predicate::True, order_by: None, limit: None };
            assert_eq!(models.fetch_states(&selection).await?.len(), 1);
            release_publication.send(()).unwrap();

            ready.await?;
            assert_eq!(registration.await?, original, "startup must not permit allocation from a stale index");
            assert_eq!(node.catalog.model_id_for(label)?, Some(original));
            assert_eq!(models.fetch_states(&selection).await?.len(), 1);
            Ok(())
        })
        .await?
    }
}
