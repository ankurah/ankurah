use ankurah_core::{
    node::NodeKey,
    storage::{StorageEngine, StorageTransaction},
};
use ankurah_proto::{AttestationSet, Attested, EntityId, Event, Identity, OperationSet, SealedAttestation, Subject};

/// Attestations about `subject`: bytes that do not parse, no bytes at all, and a genuine
/// statement of a kind nobody defines, which unseals but which no build can interpret.
fn attestations(subject: Subject) -> AttestationSet {
    let unknown_kind = NodeKey::from_seed([31; 32]).attest(subject, EntityId::from_bytes([32; 32]), b"read by no build".to_vec());
    AttestationSet(vec![SealedAttestation(b"not an attestation".to_vec()), SealedAttestation(Vec::new()), unknown_kind])
}

/// Shared black-box case: an attestation's content never makes an event unreadable, and every
/// read path returns each of an event's attestations with exactly the bytes it was stored with.
pub async fn check(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let genesis = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
    let event = Attested { attestations: attestations(Subject::event(&genesis)), payload: genesis };
    let mut transaction = engine.transaction();
    transaction.add_events(std::slice::from_ref(&event)).await?;
    transaction.commit().await?.committed()?;

    assert_eq!(engine.get_events(vec![event.payload.id()]).await?, vec![event.clone()]);
    assert_eq!(engine.dump_entity_events(event.payload.entity_id).await?, vec![event]);
    Ok(())
}
