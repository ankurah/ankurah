//! Test seams and direct access to internal handlers. Feature-gated; never
//! compiled into a normal build.

use crate::connector::{Handshake, PeerProvenance};
use crate::internal::prelude::*;
use crate::node::NodeKey;
use crate::schema::registration::RegistrationError;

pub use crate::node::inbound::handler::commit_transaction;

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
    let epoch = node.catalog_epoch()?;
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

/// Each of `operations` carried under `salt`, for a fixture whose ids must be the same in every
/// run. A node draws a salt for each operation it carries.
pub fn carried_under_salt(operations: &proto::OperationSet, salt: [u8; 16]) -> proto::CarriedOperations {
    proto::CarriedOperations(operations.iter().map(|operation| proto::CarriedOperation::encode_with_salt(operation, salt)).collect())
}

/// Commit a backend's staged fixture values under a deterministic genesis event.
/// Storage tests can use the returned event's clock when constructing a snapshot.
pub fn commit_backend<B: crate::property::backend::PropertyBackend>(backend: &B, seed: u8) -> Result<proto::Event, MutationError> {
    let operations = carried_under_salt(
        &proto::OperationSet::from_backends(std::collections::BTreeMap::from([(
            B::property_backend_name().to_owned(),
            backend.to_operations()?.unwrap_or_default(),
        )])),
        [seed; 16],
    );
    let nonce = [seed; 32];
    let entity_id = proto::EventId::from_genesis_parts(&nonce, 0, &operations).into();
    let event =
        proto::Event { entity_id, parent: proto::Clock::default(), body: proto::EventBody::Genesis { nonce, timestamp: 0, operations } };
    apply_backend_event(backend, &event)?;
    Ok(event)
}

/// Two sides of a fresh handshake, `node`'s, reached as `provenance` says, and `peer`'s, reached
/// the other way, each holding the other's opening, as two connected nodes hold them once both
/// openings have arrived.
pub fn opened_handshakes(node: proto::NodeId, provenance: PeerProvenance, peer: proto::NodeId) -> (Handshake, Handshake) {
    let peer_provenance = match provenance {
        PeerProvenance::Configured => PeerProvenance::Inbound,
        PeerProvenance::Inbound => PeerProvenance::Configured,
    };
    let (mut ours, mut theirs) = (Handshake::new(node, provenance), Handshake::new(peer, peer_provenance));
    // Cannot fail: each side receives one opening, and both offer the versions this build speaks.
    ours.receive_opening(theirs.opening()).expect("the openings share a version");
    theirs.receive_opening(ours.opening()).expect("the openings share a version");
    (ours, theirs)
}

/// `node`'s side of a fresh connection, reached as `provenance` says, to a peer that holds only
/// `key`, and the claims that peer's presence makes on it, for a test to change before `key`
/// signs them.
pub fn keyed_peer_claims(
    node: proto::NodeId,
    provenance: PeerProvenance,
    key: &NodeKey,
    durable: bool,
) -> (Handshake, proto::PresenceClaims) {
    let (ours, theirs) = opened_handshakes(node, provenance, key.node_id());
    // Cannot fail: the peer's side holds the node's opening.
    let claims = theirs.presence_claims(key.node_id(), durable).expect("the peer's side holds the node's opening");
    (ours, claims)
}
