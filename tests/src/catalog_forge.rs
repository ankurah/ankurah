//! Deterministic catalog rows for harnesses that forge events directly.

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::schema::{MODEL_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID, PROPERTY_COLLECTION_ID};
use ankurah::core::storage::StorageEngine;
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested, ModelId, PropertyId, SystemModel, SystemProperty};
use std::collections::BTreeMap;

pub struct ForgedCatalog {
    pub model: proto::EntityId,
    pub properties: Vec<proto::EntityId>,
    pub memberships: Vec<proto::EntityId>,
    pub rows: Vec<(proto::CollectionId, Attested<proto::EntityState>, Attested<proto::Event>)>,
}

pub fn forge_catalog(label: &str, display: &str, properties: &[(&str, &str, &str)], namespace: &[u8]) -> ForgedCatalog {
    let (model, model_row) = row(
        namespace,
        MODEL_COLLECTION_ID,
        0,
        ModelId::System(SystemModel::Model),
        vec![
            (SystemProperty::Label, Some(Value::String(label.to_owned()))),
            (SystemProperty::Name, Some(Value::String(display.to_owned()))),
        ],
    );
    let mut forged = ForgedCatalog {
        model,
        properties: Vec::with_capacity(properties.len()),
        memberships: Vec::with_capacity(properties.len()),
        rows: vec![model_row],
    };
    for (index, (name, backend, value_type)) in properties.iter().enumerate() {
        let (property, property_row) = row(
            namespace,
            PROPERTY_COLLECTION_ID,
            1 + 2 * index as u64,
            ModelId::System(SystemModel::Property),
            vec![
                (SystemProperty::Name, Some(Value::String((*name).to_owned()))),
                (SystemProperty::Backend, Some(Value::String((*backend).to_owned()))),
                (SystemProperty::ValueType, Some(Value::String((*value_type).to_owned()))),
                (SystemProperty::MintedFor, Some(Value::EntityId(model))),
                (SystemProperty::TargetModel, None),
            ],
        );
        forged.rows.push(property_row);
        forged.properties.push(property);
        let (membership, membership_row) = row(
            namespace,
            MODEL_PROPERTY_COLLECTION_ID,
            2 + 2 * index as u64,
            ModelId::System(SystemModel::ModelProperty),
            vec![
                (SystemProperty::Model, Some(Value::EntityId(model))),
                (SystemProperty::Property, Some(Value::EntityId(property))),
                (SystemProperty::Optional, Some(Value::Bool(false))),
            ],
        );
        forged.rows.push(membership_row);
        forged.memberships.push(membership);
    }
    forged
}

pub async fn plant<SE>(engine: &SE, forged: &ForgedCatalog) -> anyhow::Result<()>
where SE: StorageEngine + Send + Sync + 'static {
    for (collection, state, event) in &forged.rows {
        let storage = engine.collection(collection).await?;
        storage.add_event(event).await?;
        storage.set_state(state.clone()).await?;
    }
    Ok(())
}

fn row(
    namespace: &[u8],
    collection: &str,
    counter: u64,
    membership: ModelId,
    values: Vec<(SystemProperty, Option<Value>)>,
) -> (proto::EntityId, (proto::CollectionId, Attested<proto::EntityState>, Attested<proto::Event>)) {
    let staged = LWWBackend::new();
    for (property, value) in &values {
        staged.set(PropertyId::System(*property), value.clone());
    }
    let lww = staged.to_operations().expect("staged LWW values extract").expect("a written LWW backend yields operations");
    let mut operations = proto::OperationSet::from_backends(BTreeMap::from([("lww".to_owned(), lww.clone())]));
    operations.push(proto::Operation::Membership(proto::Membership::Add(membership.clone())));

    let system = Some(proto::EntityId::from_bytes([0x5A; 32]));
    let nonce = content_nonce(counter, &[b"catalog-row", namespace, collection.as_bytes()]);
    let author = proto::AuthorId::Unknown;
    let timestamp = 0u64;
    let event_id = proto::EventId::from_genesis_parts(&system, &nonce, timestamp, &author, &operations);
    let entity_id: proto::EntityId = event_id.clone().into();
    let event = proto::Event {
        collection: proto::CollectionId::fixed_name(collection),
        entity_id,
        parent: proto::Clock::default(),
        body: proto::EventBody::Genesis { system, nonce, timestamp, author, operations },
    };

    let stored = LWWBackend::new();
    stored.apply_operations_with_event(&lww, event_id.clone()).expect("forged catalog ops apply");
    let buffer = stored.to_state_buffer().expect("forged catalog state serializes");
    let state = proto::State {
        state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), buffer)])),
        memberships: std::collections::BTreeSet::from([membership]),
        head: event_id.into(),
    };
    let entity_state = proto::EntityState { entity_id, collection: proto::CollectionId::fixed_name(collection), state };
    (entity_id, (proto::CollectionId::fixed_name(collection), entity_state.into(), Attested::opt(event, None)))
}

fn content_nonce(counter: u64, parts: &[&[u8]]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"ankurah.catalog-forge.nonce.v0");
    hasher.update(counter.to_be_bytes());
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}
