use std::sync::Arc;

use ankql::ast::Predicate;
use ankurah_core::{
    node::NodeKey,
    schema::CatalogResolver,
    storage::{StorageEngine, StorageTransaction},
};
use ankurah_proto::{
    AttestationSet, Attested, Clock, EntityId, EntityState, Event, Identity, ModelId, OperationSet, PropertyId, SealedAttestation, State,
    Subject,
};

struct Labels;

#[async_trait::async_trait]
impl CatalogResolver for Labels {
    async fn get_model_label(&self, _: &ModelId) -> Option<String> { Some("attested".into()) }
    async fn get_property_label(&self, _: &PropertyId) -> Option<String> { Some("value".into()) }
}

fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([111; 32])) }

/// Attestations about `subject`: bytes that do not parse, no bytes at all, and a genuine
/// statement of a kind nobody defines, which opens but which no build can interpret.
fn attestations(subject: Subject) -> AttestationSet {
    let unknown_kind = NodeKey::from_seed([31; 32]).attest(subject, EntityId::from_bytes([32; 32]), b"read by no build".to_vec());
    AttestationSet(vec![SealedAttestation(b"not an attestation".to_vec()), SealedAttestation(Vec::new()), unknown_kind])
}

/// Store an event and its entity's state, each carrying [`attestations`], and return both as stored.
async fn write(engine: &impl StorageEngine, minter: u8) -> anyhow::Result<(Attested<Event>, Attested<EntityState>)> {
    let genesis = Event::genesis(Identity::Ed25519([minter; 32]), OperationSet::default());
    let state = EntityState {
        entity_id: genesis.entity_id,
        state: State { memberships: [model()].into(), head: Clock::singleton(&genesis), ..State::default() },
    };
    let event = Attested { attestations: attestations(Subject::event(&genesis)), payload: genesis };
    let state = Attested { attestations: attestations(Subject::state(&state)), payload: state };
    let mut transaction = engine.transaction();
    transaction.add_events(std::slice::from_ref(&event)).await?;
    transaction.set_state(&Clock::default(), &state).await?;
    transaction.commit().await?.committed()?;
    Ok((event, state))
}

/// Shared black-box case: an attestation's content never makes an event or a state unreadable,
/// and every read path returns each attestation with exactly the bytes it was stored with.
pub async fn check(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let labels: Arc<dyn CatalogResolver> = Arc::new(Labels);
    engine.set_catalog_resolver(Arc::downgrade(&labels));
    let (event, state) = write(engine, 1).await?;
    assert_eq!(engine.get_events(vec![event.payload.id()]).await?, vec![event.clone()]);
    assert_eq!(engine.dump_entity_events(event.payload.entity_id).await?, vec![event]);
    assert_eq!(engine.get_state(state.payload.entity_id).await?, state);
    assert_eq!(engine.fetch_states(&Predicate::MemberOf(model()).into()).await?, vec![state]);
    Ok(())
}
