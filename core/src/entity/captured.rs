use super::TemporaryEntity;
use crate::{error::RetrievalError, selection::filter::Filterable, value::Value};
use ankurah_proto::{EntityState, ModelId, PropertyId};
use std::{collections::BTreeMap, ops::Deref, sync::OnceLock};

/// One immutable entity version. The compact lookup shares its lifetime with
/// the serialized state; decoded property backends are never retained here.
#[derive(Debug)]
pub(crate) struct CapturedEntity {
    data: EntityState,
    lookup: OnceLock<Result<Lookup, RetrievalError>>,
}

#[derive(Debug)]
struct Lookup {
    values: BTreeMap<PropertyId, Value>,
    bytes: usize,
}

impl Lookup {
    fn new(values: impl IntoIterator<Item = (PropertyId, Option<Value>)>) -> Self {
        let mut first = BTreeMap::new();
        for (property, value) in values {
            // TemporaryEntity searches backends in name order for the first
            // Some value. A later backend or None must not replace that value.
            if let Some(value) = value {
                first.entry(property).or_insert(value);
            }
        }
        let bytes =
            first.values().fold(map_bytes::<PropertyId, Value>(first.len()), |bytes, value| bytes.saturating_add(value_heap_bytes(value)));
        Self { values: first, bytes }
    }
}

// Conservative retained-heap estimate, including spare map/array capacity.
// Serialized JSON length is not a proxy for its much larger in-memory values.
fn map_bytes<K, V>(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    len.max(11).saturating_mul(std::mem::size_of::<(K, V)>() + 16).saturating_mul(3)
}
fn value_heap_bytes(value: &Value) -> usize {
    match value {
        Value::String(value) => value.capacity(),
        Value::Object(value) | Value::Binary(value) => value.capacity(),
        Value::Json(value) => json_heap_bytes(value),
        _ => 0,
    }
}
fn json_heap_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => {
            values.iter().fold(values.capacity().saturating_mul(std::mem::size_of::<serde_json::Value>()), |bytes, value| {
                bytes.saturating_add(json_heap_bytes(value))
            })
        }
        serde_json::Value::Object(values) => {
            values.iter().fold(map_bytes::<String, serde_json::Value>(values.len()), |bytes, (key, value)| {
                bytes.saturating_add(key.capacity()).saturating_add(json_heap_bytes(value))
            })
        }
        _ => 0,
    }
}

impl CapturedEntity {
    /// Serialized input must validate every backend, even for a true or ID-only
    /// predicate. Delay validation until its first read, then cache the result.
    pub(crate) fn new(data: EntityState) -> Self { Self { data, lookup: OnceLock::new() } }

    /// Only the entity module may supply values from already validated backends.
    /// Its caller holds the same state read lock while serializing and reading values.
    pub(super) fn from_validated(data: EntityState, values: impl IntoIterator<Item = (PropertyId, Option<Value>)>) -> Self {
        Self { data, lookup: OnceLock::from(Ok(Lookup::new(values))) }
    }

    pub(crate) fn read(&self) -> Result<CapturedRead<'_>, RetrievalError> {
        let lookup = self.lookup.get_or_init(|| {
            let entity = TemporaryEntity::new(self.entity_id, &self.state)?;
            Ok(Lookup::new(entity.values()))
        });
        match lookup {
            Ok(lookup) => Ok(CapturedRead { entity: self, lookup }),
            Err(error) => Err(error.clone()),
        }
    }

    pub(crate) fn lookup_bytes(&self) -> usize {
        self.lookup.get().and_then(|lookup| lookup.as_ref().ok()).map_or(0, |lookup| lookup.bytes)
    }

    pub(crate) fn to_entity_state(&self) -> EntityState { self.data.clone() }
}

impl Deref for CapturedEntity {
    type Target = EntityState;
    fn deref(&self) -> &EntityState { &self.data }
}

// In-memory sharing and cached lookup values add nothing to the wire format.
impl serde::Serialize for CapturedEntity {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { self.data.serialize(serializer) }
}

pub(crate) struct CapturedRead<'a> {
    entity: &'a CapturedEntity,
    lookup: &'a Lookup,
}
impl Filterable for CapturedRead<'_> {
    fn value(&self, property: &PropertyId) -> Option<Value> {
        if *property == PropertyId::Id {
            Some(Value::EntityId(self.entity.entity_id))
        } else {
            self.lookup.values.get(property).cloned()
        }
    }
    fn is_member_of(&self, model: &ModelId) -> Result<bool, crate::selection::filter::Error> {
        Ok(self.entity.state.memberships.contains(model))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        entity::state::EntityState as LiveState,
        property::backend::{LWWBackend, PropertyBackend, YrsBackend},
        selection::filter::{evaluate_predicate, ValueLookup},
    };
    use ankurah_core_types::PropertyPath;
    use ankurah_proto::{Clock, EntityId, State, StateBuffers, SystemProperty};

    #[test]
    fn published_and_storage_captures_keep_the_same_frozen_lookup() -> anyhow::Result<()> {
        let state = LiveState::empty();
        let lww = state.get_backend::<LWWBackend>()?;
        let yrs = state.get_backend::<YrsBackend>()?;
        let first = PropertyId::System(SystemProperty::Label);
        let fallback = PropertyId::System(SystemProperty::Name);
        let absent = PropertyId::EntityId(EntityId::from_bytes([9; 32]));
        let json = PropertyId::EntityId(EntityId::from_bytes([8; 32]));
        lww.set(first, Some(Value::String("first backend".into())));
        lww.set(fallback, None);
        lww.set(absent, None);
        lww.set(json, Some(Value::Json(serde_json::json!({"nested": {"value": 7}}))));
        let event = crate::test_helpers::commit_backend(&*lww, 1)?;
        yrs.insert(&first, 0, "later backend")?;
        yrs.insert(&fallback, 0, "fallback value")?;
        let model = ModelId::EntityId(EntityId::from_bytes([3; 32]));
        state.add_membership(model);
        state.set_head(Clock::singleton(&event));
        let published = state.capture(event.entity_id)?;
        assert!(published.lookup.get().is_some(), "publication supplies values without decoding its own serialized state");
        let stored = CapturedEntity::new(published.to_entity_state());
        assert!(stored.lookup.get().is_none());
        let oracle = TemporaryEntity::new(event.entity_id, &published.state)?;
        let expected = [
            (first, Some(Value::String("first backend".into()))),
            (fallback, Some(Value::String("fallback value".into()))),
            (absent, None),
            (PropertyId::Id, Some(Value::EntityId(event.entity_id))),
        ];
        for captured in [&published, &stored] {
            let read = captured.read()?;
            for (property, value) in &expected {
                assert_eq!(read.value(property), *value);
                assert_eq!(read.value(property), oracle.value(property));
            }
            assert!(Filterable::is_member_of(&read, &model)?);
            assert!(!Filterable::is_member_of(&read, &ModelId::EntityId(EntityId::from_bytes([4; 32])))?);
            let path = PropertyPath::registered(EntityId::from_bytes([8; 32]), "json", vec!["nested".into(), "value".into()]);
            assert_eq!(read.value_at(&path), Some(Value::I64(7)));
            assert!(std::ptr::eq(read.lookup, captured.read()?.lookup), "every read reuses the compact lookup");
            assert!(captured.lookup_bytes() > 0);
        }
        lww.set(first, Some(Value::String("new version".into())));
        let changed = crate::test_helpers::commit_backend(&*lww, 2)?;
        state.set_head(Clock::singleton(&changed));
        let current = state.capture(event.entity_id)?;
        assert_eq!(current.read()?.value(&first), Some(Value::String("new version".into())));
        for captured in [&published, &stored] {
            assert_eq!(captured.state.head, Clock::singleton(&event));
            assert_eq!(captured.read()?.value(&first), expected[0].1);
            assert_eq!(TemporaryEntity::new(event.entity_id, &captured.state)?.value(&first), expected[0].1);
        }
        assert_eq!(bincode::serialize(&published)?, bincode::serialize(&published.to_entity_state())?);
        Ok(())
    }

    #[test]
    fn yrs_replacements_keep_published_values_and_serialized_state_in_agreement() -> anyhow::Result<()> {
        for (before, after) in [("08", "20"), ("12", "06")] {
            let state = LiveState::empty();
            let backend = state.get_backend::<YrsBackend>()?;
            let property = PropertyId::System(SystemProperty::Label);
            backend.insert(&property, 0, before)?;
            let initial = crate::test_helpers::commit_backend(&*backend, 1)?;
            state.set_head(Clock::singleton(&initial));
            let frozen = state.capture(initial.entity_id)?;
            backend.delete(&property, 0, before.len() as u32)?;
            backend.insert(&property, 0, after)?;
            let changed = crate::test_helpers::commit_backend(&*backend, 2)?;
            state.set_head(Clock::singleton(&changed));
            let current = state.capture(initial.entity_id)?;
            let decoded = TemporaryEntity::new(initial.entity_id, &current.state)?;
            assert_eq!(current.read()?.value(&property), Some(Value::String(after.into())));
            assert_eq!(current.read()?.value(&property), decoded.value(&property));
            assert_eq!(frozen.read()?.value(&property), Some(Value::String(before.into())));
            assert_eq!(frozen.state.head, Clock::singleton(&initial));
        }
        Ok(())
    }

    #[test]
    fn retained_lookup_accounts_for_json_capacity_not_only_serialized_bytes() {
        let mut values = Vec::with_capacity(8192);
        values.resize(4096, serde_json::json!(0));
        let allocated = values.capacity() * std::mem::size_of::<serde_json::Value>();
        let json = serde_json::Value::Array(values);
        let serialized = serde_json::to_vec(&json).unwrap().len();
        let lookup = Lookup::new([(PropertyId::System(SystemProperty::Label), Some(Value::Json(json)))]);
        assert!(lookup.bytes >= allocated);
        assert!(lookup.bytes > serialized * 8, "small JSON primitives occupy much more memory than wire bytes");
    }

    #[test]
    fn serialized_captures_validate_unused_backends_once_and_keep_errors() -> anyhow::Result<()> {
        let id = EntityId::from_bytes([1; 32]);
        let lww = LWWBackend::new();
        lww.set(PropertyId::System(SystemProperty::Label), Some(Value::String("valid".into())));
        crate::test_helpers::commit_backend(&lww, 1)?;
        for (name, buffer) in [("yrs", vec![255]), ("unknown", vec![]), ("lww", vec![255])] {
            let capture = CapturedEntity::new(EntityState {
                entity_id: id,
                state: State {
                    state_buffers: StateBuffers([("lww".into(), lww.to_state_buffer()?), (name.into(), buffer)].into()),
                    ..Default::default()
                },
            });
            let error = capture.read().err().expect("unused malformed backend must still fail");
            let result = capture.lookup.get().unwrap();
            assert_eq!(capture.read().err().unwrap().to_string(), error.to_string());
            assert!(std::ptr::eq(result, capture.lookup.get().unwrap()), "the failure is cached too");
            assert!(capture.read().map(|read| evaluate_predicate(&read, &ankql::ast::Predicate::True)).is_err());
        }
        Ok(())
    }
}
