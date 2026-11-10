//! Test seams and direct access to internal handlers. Feature-gated; never
//! compiled into a normal build.

use crate::internal::prelude::*;
use crate::schema::registration::RegistrationError;

pub use crate::node::handler::commit_transaction;

/// Bind `schema`'s cells from a hand-built definition under the node's
/// current epoch, bypassing registration and the catalog: deterministic
/// harnesses seed identical ids on every node this way.
pub fn seed_registered_schema<SE, PA>(
    node: &Node<SE, PA>,
    schema: &'static ModelStructDescriptor,
    model: &proto::RegisteredModel,
) -> Result<(), RegistrationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let epoch = node.system_epoch()?;
    crate::schema::catalog::register::accept(&mut schema.registrant(epoch), model)
}

/// Apply a fixture event as a direct extension of its parent clock, using the
/// same comparison and layer construction as entity application. No storage I/O.
pub fn apply_backend_event(backend: &dyn crate::property::backend::PropertyBackend, event: &proto::Event) -> Result<(), MutationError> {
    use futures::FutureExt;
    struct Incoming<'a>(&'a proto::Event);
    #[async_trait::async_trait]
    impl crate::retrieval::GetEvents for Incoming<'_> {
        async fn get_event(&self, id: &proto::EventId) -> Result<proto::Event, RetrievalError> {
            if *id == self.0.id() {
                Ok(self.0.clone())
            } else {
                Err(RetrievalError::EventNotFound(id.clone()))
            }
        }
        async fn event_stored(&self, _: &proto::EventId) -> Result<bool, RetrievalError> { Ok(false) }
    }
    async {
        let layers = crate::event_dag::EventLayers::prepare(
            &Incoming(event),
            &[event],
            &event.parent,
            &crate::node::event_admissibility::AdmittedOperations::new(),
        )
        .await?;
        for layer in layers.layers() {
            backend.apply_layer(layer)?;
        }
        Ok(())
    }
    .now_or_never()
    .expect("fixture comparison has no asynchronous I/O")
}

/// Commit a backend's staged fixture values under a deterministic genesis event.
/// Storage tests can use the returned event's clock when constructing a snapshot.
pub fn commit_backend<B: crate::property::backend::PropertyBackend>(backend: &B, seed: u8) -> Result<proto::Event, MutationError> {
    let operations = proto::CarriedOperations::encode(&proto::OperationSet::from_backends(std::collections::BTreeMap::from([(
        B::property_backend_name().to_owned(),
        backend.to_operations()?.unwrap_or_default(),
    )])));
    let minter = proto::Identity::Ed25519([seed; 32]);
    let nonce = [seed; 32];
    let entity_id = proto::EventId::from_genesis_parts(&minter, &nonce, 0, &operations).into();
    let event = proto::Event {
        entity_id,
        parent: proto::Clock::default(),
        body: proto::EventBody::Genesis { minter, nonce, timestamp: 0, operations },
    };
    apply_backend_event(backend, &event)?;
    Ok(event)
}
