//! Versioned logical dumps shared by the native storage engines.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use ankurah_core::{
    property::backend::backend_from_string,
    storage::{StorageCollection, StorageDump, StorageDumpItem},
    value::{Value, ValueType},
};
use ankurah_proto::{
    Attestation, AttestationSet, Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId, Operation, OperationSet, State,
    StateBuffers,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{pin_mut, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

pub const DUMP_FORMAT: &str = "ankurah-portable-dump";
pub const DUMP_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DumpSummary {
    pub states: u64,
    pub events: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum DumpError {
    #[error("dump I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("dump JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("storage operation failed: {0}")]
    Storage(String),
    #[error("dump record {line} is invalid: {message}")]
    InvalidRecord { line: u64, message: String },
    #[error("dump is incomplete: no end record")]
    MissingEnd,
    #[error("dump checksum mismatch: expected {expected}, calculated {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("target already contains Ankurah data; load only accepts an empty target")]
    TargetNotEmpty,
    #[error("dump output already exists: {0}")]
    OutputExists(PathBuf),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum DumpRecord {
    Header {
        format: String,
        format_version: u32,
        producer_version: String,
        created_at_unix_seconds: u64,
        source_engine: String,
    },
    Event {
        id: String,
        collection: String,
        entity_id: String,
        operations: BTreeMap<String, Vec<String>>,
        parent: Vec<String>,
        attestations: Vec<String>,
    },
    State {
        collection: String,
        entity_id: String,
        state_buffers: BTreeMap<String, String>,
        head: Vec<String>,
        attestations: Vec<String>,
    },
    End {
        states: u64,
        events: u64,
        sha256: String,
    },
}

pub async fn dump<E: StorageDump>(engine: &E, source_engine: &str, destination: impl AsRef<Path>) -> Result<DumpSummary, DumpError> {
    validate_source_engine(source_engine, 0)?;
    let destination = destination.as_ref();
    if destination.exists() {
        return Err(DumpError::OutputExists(destination.to_path_buf()));
    }

    let parent = destination.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent)?;
    let summary = {
        let mut writer = BufWriter::new(temporary.as_file_mut());
        let summary = dump_to_writer(engine, source_engine, &mut writer).await?;
        writer.flush()?;
        summary
    };
    temporary.as_file().sync_all()?;
    let (validated, _) = inspect_dump(temporary.reopen()?)?;
    if validated != summary {
        return Err(invalid(0, format!("generated dump summary changed during validation: wrote {summary:?}, read {validated:?}")));
    }
    let _persisted = temporary.persist_noclobber(destination).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            DumpError::OutputExists(destination.to_path_buf())
        } else {
            DumpError::Io(error.error)
        }
    })?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(summary)
}

pub async fn load<E: StorageDump>(engine: &E, source: impl AsRef<Path>) -> Result<DumpSummary, DumpError> {
    let prepared = PreparedDump::open(source.as_ref())?;
    let target = engine.dump().await.map_err(storage_error)?;
    pin_mut!(target);
    if let Some(item) = target.next().await {
        item.map_err(storage_error)?;
        return Err(DumpError::TargetNotEmpty);
    }

    let mut collections = HashMap::<CollectionId, std::sync::Arc<dyn StorageCollection>>::new();
    for id in &prepared.collections {
        let collection = engine.collection(id).await.map_err(|error| DumpError::Storage(error.to_string()))?;
        collections.insert(id.clone(), collection);
    }

    let mut reader = prepared.reader()?;
    while let Some(record) = reader.next_record()? {
        let line = reader.line_number;
        if !matches!(&record, DumpRecord::Event { .. }) {
            continue;
        }
        let event = record.into_event(line)?;
        let collection_id = event.payload.collection.clone();
        let collection = collections
            .get(&collection_id)
            .ok_or_else(|| invalid(line, format!("validated dump did not prepare event collection {collection_id}")))?
            .clone();
        collection.add_event(&event).await.map_err(|error| DumpError::Storage(error.to_string()))?;
    }

    let mut reader = prepared.reader()?;
    while let Some(record) = reader.next_record()? {
        let line = reader.line_number;
        if !matches!(&record, DumpRecord::State { .. }) {
            continue;
        }
        let state = record.into_state(line)?;
        let collection_id = state.payload.collection.clone();
        let collection = collections
            .get(&collection_id)
            .ok_or_else(|| invalid(line, format!("validated dump did not prepare state collection {collection_id}")))?
            .clone();
        collection.set_state(state).await.map_err(|error| DumpError::Storage(error.to_string()))?;
    }

    Ok(prepared.summary)
}

pub fn validate(source: impl AsRef<Path>) -> Result<DumpSummary, DumpError> { Ok(PreparedDump::open(source.as_ref())?.summary) }

async fn dump_to_writer<E: StorageDump, W: Write>(engine: &E, source_engine: &str, writer: &mut W) -> Result<DumpSummary, DumpError> {
    let created_at_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|error| invalid(0, error.to_string()))?.as_secs();
    let mut checksum = Sha256::new();
    write_hashed_record(
        writer,
        &mut checksum,
        &DumpRecord::Header {
            format: DUMP_FORMAT.to_owned(),
            format_version: DUMP_FORMAT_VERSION,
            producer_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at_unix_seconds,
            source_engine: source_engine.to_owned(),
        },
    )?;

    let mut summary = DumpSummary { states: 0, events: 0 };
    let items = engine.dump().await.map_err(storage_error)?;
    pin_mut!(items);
    let mut saw_state = false;
    while let Some(item) = items.next().await {
        match item.map_err(storage_error)? {
            StorageDumpItem::Event(event) => {
                if saw_state {
                    return Err(invalid(0, "storage emitted an event after its first state"));
                }
                write_hashed_record(writer, &mut checksum, &DumpRecord::from_event(event)?)?;
                summary.events += 1;
            }
            StorageDumpItem::State(state) => {
                saw_state = true;
                write_hashed_record(writer, &mut checksum, &DumpRecord::from_state(state)?)?;
                summary.states += 1;
            }
        }
    }

    let sha256 = digest_hex(checksum.finalize().as_slice());
    write_record(writer, &DumpRecord::End { states: summary.states, events: summary.events, sha256 })?;
    Ok(summary)
}

impl DumpRecord {
    fn from_event(event: Attested<Event>) -> Result<Self, DumpError> {
        let id = event.payload.id().to_base64();
        let mut operations = BTreeMap::new();
        for (backend, diffs) in event.payload.operations.0 {
            backend_from_string(&backend, None).map_err(|error| invalid(0, error.to_string()))?;
            operations.insert(backend, diffs.into_iter().map(|operation| URL_SAFE_NO_PAD.encode(operation.diff)).collect());
        }
        Ok(Self::Event {
            id,
            collection: event.payload.collection.to_string(),
            entity_id: event.payload.entity_id.to_base64(),
            operations,
            parent: event.payload.parent.to_strings(),
            attestations: encode_attestations(event.attestations),
        })
    }

    fn from_state(state: Attested<EntityState>) -> Result<Self, DumpError> {
        let mut state_buffers = BTreeMap::new();
        for (backend, buffer) in state.payload.state.state_buffers.0 {
            backend_from_string(&backend, Some(&buffer)).map_err(|error| invalid(0, error.to_string()))?;
            state_buffers.insert(backend, URL_SAFE_NO_PAD.encode(buffer));
        }
        Ok(Self::State {
            collection: state.payload.collection.to_string(),
            entity_id: state.payload.entity_id.to_base64(),
            state_buffers,
            head: state.payload.state.head.to_strings(),
            attestations: encode_attestations(state.attestations),
        })
    }

    fn into_event(self, line: u64) -> Result<Attested<Event>, DumpError> {
        let Self::Event { id, collection, entity_id, operations, parent, attestations } = self else {
            return Err(invalid(line, "record is not an event"));
        };
        let declared_id = EventId::from_base64(id).map_err(|error| invalid(line, error.to_string()))?;
        let mut decoded_operations = BTreeMap::new();
        for (backend, diffs) in operations {
            let property_backend = backend_from_string(&backend, None).map_err(|error| invalid(line, error.to_string()))?;
            let diffs =
                diffs.into_iter().map(|diff| decode_bytes(&diff, line).map(|diff| Operation { diff })).collect::<Result<Vec<_>, _>>()?;
            property_backend.apply_operations(&diffs).map_err(|error| invalid(line, error.to_string()))?;
            decoded_operations.insert(backend, diffs);
        }
        let event = Attested {
            payload: Event {
                collection: CollectionId::from(collection),
                entity_id: EntityId::from_base64(entity_id).map_err(|error| invalid(line, error.to_string()))?,
                operations: OperationSet(decoded_operations),
                parent: decode_clock(parent, line)?,
            },
            attestations: decode_attestations(attestations, line)?,
        };
        if event.payload.id() != declared_id {
            return Err(invalid(line, format!("event id {declared_id} does not match its payload")));
        }
        Ok(event)
    }

    fn into_state(self, line: u64) -> Result<Attested<EntityState>, DumpError> {
        let Self::State { collection, entity_id, state_buffers, head, attestations } = self else {
            return Err(invalid(line, "record is not a state"));
        };
        let mut decoded_buffers = BTreeMap::new();
        for (backend, encoded) in state_buffers {
            let buffer = decode_bytes(&encoded, line)?;
            backend_from_string(&backend, Some(&buffer)).map_err(|error| invalid(line, error.to_string()))?;
            decoded_buffers.insert(backend, buffer);
        }
        Ok(Attested {
            payload: EntityState {
                collection: CollectionId::from(collection),
                entity_id: EntityId::from_base64(entity_id).map_err(|error| invalid(line, error.to_string()))?,
                state: State { state_buffers: StateBuffers(decoded_buffers), head: decode_clock(head, line)? },
            },
            attestations: decode_attestations(attestations, line)?,
        })
    }
}

fn encode_attestations(attestations: AttestationSet) -> Vec<String> {
    attestations.0.into_iter().map(|attestation| URL_SAFE_NO_PAD.encode(attestation.0)).collect()
}

fn decode_attestations(attestations: Vec<String>, line: u64) -> Result<AttestationSet, DumpError> {
    Ok(AttestationSet(
        attestations.into_iter().map(|attestation| decode_bytes(&attestation, line).map(Attestation)).collect::<Result<Vec<_>, _>>()?,
    ))
}

fn decode_bytes(encoded: &str, line: u64) -> Result<Vec<u8>, DumpError> {
    URL_SAFE_NO_PAD.decode(encoded).map_err(|error| invalid(line, error.to_string()))
}

fn decode_clock(encoded: Vec<String>, line: u64) -> Result<Clock, DumpError> {
    let clock = Clock::from_strings(encoded.clone()).map_err(|error| invalid(line, error.to_string()))?;
    if clock.to_strings() != encoded {
        return Err(invalid(line, "clock entries must be sorted and deduplicated"));
    }
    Ok(clock)
}

fn write_hashed_record<W: Write>(writer: &mut W, checksum: &mut Sha256, record: &DumpRecord) -> Result<(), DumpError> {
    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    checksum.update(&line);
    writer.write_all(&line)?;
    Ok(())
}

fn write_record<W: Write>(writer: &mut W, record: &DumpRecord) -> Result<(), DumpError> {
    serde_json::to_writer(&mut *writer, record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn digest_hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

struct PreparedDump {
    spool: NamedTempFile,
    summary: DumpSummary,
    collections: Vec<CollectionId>,
}

impl PreparedDump {
    fn open(source: &Path) -> Result<Self, DumpError> {
        let mut source = File::open(source)?;
        let mut spool = NamedTempFile::new()?;
        std::io::copy(&mut source, &mut spool)?;
        spool.as_file_mut().flush()?;
        let (summary, collections) = inspect_dump(spool.reopen()?)?;
        Ok(Self { spool, summary, collections })
    }

    fn reader(&self) -> Result<DumpReader<BufReader<File>>, DumpError> { Ok(DumpReader::new(BufReader::new(self.spool.reopen()?))) }
}

fn inspect_dump(file: File) -> Result<(DumpSummary, Vec<CollectionId>), DumpError> {
    let mut reader = DumpReader::new(BufReader::new(file));
    while reader.next_record()?.is_some() {}
    let summary = reader.summary.expect("reader only ends after a validated end record");
    let collections = reader.validator.collections.iter().cloned().collect();
    Ok((summary, collections))
}

struct DumpReader<R> {
    reader: R,
    checksum: Sha256,
    validator: DumpValidator,
    line_number: u64,
    summary: Option<DumpSummary>,
}

impl<R: BufRead> DumpReader<R> {
    fn new(reader: R) -> Self {
        Self { reader, checksum: Sha256::new(), validator: DumpValidator::default(), line_number: 0, summary: None }
    }

    fn next_record(&mut self) -> Result<Option<DumpRecord>, DumpError> {
        if self.summary.is_some() {
            return Ok(None);
        }
        let mut line = Vec::new();
        if self.reader.read_until(b'\n', &mut line)? == 0 {
            return Err(DumpError::MissingEnd);
        }
        self.line_number += 1;
        let record: DumpRecord = serde_json::from_slice(&line).map_err(|error| invalid(self.line_number, error.to_string()))?;
        if let DumpRecord::End { states, events, sha256 } = &record {
            let actual = digest_hex(self.checksum.clone().finalize().as_slice());
            if *sha256 != actual {
                return Err(DumpError::ChecksumMismatch { expected: sha256.clone(), actual });
            }
            let summary = DumpSummary { states: *states, events: *events };
            self.validator.finish(summary, self.line_number)?;
            let mut trailing = [0];
            if self.reader.read(&mut trailing)? != 0 {
                return Err(invalid(self.line_number + 1, "records follow the end record"));
            }
            self.summary = Some(summary);
            return Ok(None);
        }
        self.checksum.update(&line);
        self.validator.accept(&record, self.line_number)?;
        Ok(Some(record))
    }
}

#[derive(Default)]
struct DumpValidator {
    saw_header: bool,
    collections: BTreeSet<CollectionId>,
    states: HashMap<(CollectionId, EntityId), BTreeSet<EventId>>,
    events: HashMap<EventId, (CollectionId, EntityId)>,
    entity_events: HashMap<(CollectionId, EntityId), BTreeSet<EventId>>,
    entity_parents: HashMap<(CollectionId, EntityId), BTreeSet<EventId>>,
    entity_collections: HashMap<EntityId, CollectionId>,
    materialized_types: HashMap<(CollectionId, String), MaterializedType>,
}

impl DumpValidator {
    fn accept(&mut self, record: &DumpRecord, line: u64) -> Result<(), DumpError> {
        if !self.saw_header {
            let DumpRecord::Header { format, format_version, source_engine, .. } = record else {
                return Err(invalid(line, "the first record must be a header"));
            };
            if format != DUMP_FORMAT {
                return Err(invalid(line, format!("unsupported dump format {format:?}")));
            }
            if *format_version != DUMP_FORMAT_VERSION {
                return Err(invalid(line, format!("unsupported dump format version {format_version}")));
            }
            validate_source_engine(source_engine, line)?;
            self.saw_header = true;
            return Ok(());
        }

        match record {
            DumpRecord::Header { .. } => Err(invalid(line, "duplicate header")),
            DumpRecord::Event { .. } => {
                let event = record.clone().into_event(line)?;
                self.check_membership(&event.payload.collection, event.payload.entity_id, line)?;
                let id = event.payload.id();
                if self.events.insert(id.clone(), (event.payload.collection.clone(), event.payload.entity_id)).is_some() {
                    return Err(invalid(line, format!("duplicate event {id}")));
                }
                let entity_key = (event.payload.collection.clone(), event.payload.entity_id);
                self.entity_events.entry(entity_key.clone()).or_default().insert(id);
                self.entity_parents.entry(entity_key).or_default().extend(event.payload.parent.iter().cloned());
                Ok(())
            }
            DumpRecord::State { .. } => {
                let state = record.clone().into_state(line)?;
                self.check_membership(&state.payload.collection, state.payload.entity_id, line)?;
                let key = (state.payload.collection.clone(), state.payload.entity_id);
                if self.states.insert(key, state.payload.state.head.iter().cloned().collect()).is_some() {
                    return Err(invalid(line, format!("duplicate state {}/{}", state.payload.collection, state.payload.entity_id)));
                }
                self.validate_materialization(&state, line)?;
                Ok(())
            }
            DumpRecord::End { .. } => unreachable!("end records are handled by DumpReader"),
        }
    }

    fn validate_materialization(&mut self, state: &Attested<EntityState>, line: u64) -> Result<(), DumpError> {
        let mut state_properties = HashSet::new();
        for (backend_name, buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(backend_name, Some(buffer)).map_err(|error| invalid(line, error.to_string()))?;
            for (property, value) in backend.property_values() {
                validate_portable_property(&property, line)?;
                if !state_properties.insert(property.clone()) {
                    return Err(invalid(
                        line,
                        format!(
                            "state {}/{} materializes property {property:?} from more than one backend",
                            state.payload.collection, state.payload.entity_id
                        ),
                    ));
                }
                let Some(value) = value else {
                    continue;
                };
                let observed = MaterializedType::of(&value);
                let key = (state.payload.collection.clone(), property.clone());
                if let Some(expected) = self.materialized_types.insert(key, observed) {
                    if expected != observed {
                        return Err(invalid(
                            line,
                            format!(
                                "collection {} materializes property {property:?} as both {expected:?} and {observed:?}",
                                state.payload.collection
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn check_membership(&mut self, collection: &CollectionId, entity: EntityId, line: u64) -> Result<(), DumpError> {
        validate_portable_collection(collection, line)?;
        self.collections.insert(collection.clone());
        if let Some(previous) = self.entity_collections.insert(entity, collection.clone()) {
            if previous != *collection {
                return Err(invalid(line, format!("entity {entity} appears in both {previous} and {collection}")));
            }
        }
        Ok(())
    }

    fn finish(&self, summary: DumpSummary, line: u64) -> Result<(), DumpError> {
        if !self.saw_header {
            return Err(invalid(line, "dump has no header"));
        }
        let actual = DumpSummary { states: self.states.len() as u64, events: self.events.len() as u64 };
        if summary != actual {
            return Err(invalid(line, format!("end counts do not match records: declared {summary:?}, observed {actual:?}")));
        }
        for collection in &self.collections {
            let event_table = CollectionId::from(format!("{collection}_event"));
            if self.collections.contains(&event_table) {
                return Err(invalid(line, format!("collection {event_table} collides with the event table for collection {collection}")));
            }
        }
        for ((collection, entity), references) in self.states.iter().chain(self.entity_parents.iter()) {
            for event in references {
                let Some((stored_collection, stored_entity)) = self.events.get(event) else {
                    return Err(invalid(line, format!("{collection}/{entity} refers to missing event {event}")));
                };
                if stored_collection != collection || stored_entity != entity {
                    return Err(invalid(
                        line,
                        format!("{collection}/{entity} refers to event {event} owned by {stored_collection}/{stored_entity}"),
                    ));
                }
            }
        }
        for (entity, events) in &self.entity_events {
            let Some(state_head) = self.states.get(entity) else {
                return Err(invalid(line, format!("{}/{} has events but no state row", entity.0, entity.1)));
            };
            let parents = self.entity_parents.get(entity).cloned().unwrap_or_default();
            let frontier = events.difference(&parents).cloned().collect::<BTreeSet<_>>();
            if state_head != &frontier {
                return Err(invalid(
                    line,
                    format!(
                        "{}/{} state head does not match its event DAG frontier: state={state_head:?}, events={frontier:?}",
                        entity.0, entity.1
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializedType {
    Varchar,
    SmallInt,
    Integer,
    BigInt,
    DoublePrecision,
    Boolean,
    Bytea,
    Json,
}

impl MaterializedType {
    fn of(value: &Value) -> Self {
        match ValueType::of(value) {
            ValueType::String | ValueType::EntityId => Self::Varchar,
            ValueType::I16 => Self::SmallInt,
            ValueType::I32 => Self::Integer,
            ValueType::I64 => Self::BigInt,
            ValueType::F64 => Self::DoublePrecision,
            ValueType::Bool => Self::Boolean,
            ValueType::Object | ValueType::Binary => Self::Bytea,
            ValueType::Json => Self::Json,
        }
    }
}

fn storage_error(error: ankurah_core::error::RetrievalError) -> DumpError { DumpError::Storage(error.to_string()) }

fn invalid(line: u64, message: impl Into<String>) -> DumpError { DumpError::InvalidRecord { line, message: message.into() } }

fn validate_source_engine(source_engine: &str, line: u64) -> Result<(), DumpError> {
    if matches!(source_engine, "postgres" | "sqlite" | "sled") {
        Ok(())
    } else {
        Err(invalid(line, format!("unsupported source engine {source_engine:?}")))
    }
}

fn validate_portable_collection(id: &CollectionId, line: u64) -> Result<(), DumpError> {
    let name = id.as_str();
    if name.is_empty() || name.len() > 57 {
        return Err(invalid(line, format!("collection name must contain 1 to 57 UTF-8 bytes: {name:?}")));
    }
    if !name.chars().all(|character| character.is_alphanumeric() || matches!(character, '_' | '.' | ':')) {
        return Err(invalid(line, format!("collection name contains non-portable characters: {name:?}")));
    }
    Ok(())
}

fn validate_portable_property(name: &str, line: u64) -> Result<(), DumpError> {
    const RESERVED: &[&str] = &["id", "state_buffer", "head", "attestations"];
    if name.is_empty() || name.len() > 63 {
        return Err(invalid(line, format!("property name must contain 1 to 63 UTF-8 bytes: {name:?}")));
    }
    if RESERVED.contains(&name) {
        return Err(invalid(line, format!("property name is reserved by native storage: {name:?}")));
    }
    if !name.chars().all(|character| character.is_alphanumeric() || matches!(character, '_' | '.' | ':')) {
        return Err(invalid(line, format!("property name contains non-portable characters: {name:?}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_validation_rejects_invalid_operation_bytes() {
        let event = Attested::opt(
            Event {
                collection: CollectionId::from("album"),
                entity_id: EntityId::new(),
                operations: OperationSet(BTreeMap::from([("lww".to_owned(), vec![Operation { diff: vec![0xff] }])])),
                parent: Clock::default(),
            },
            None,
        );
        let record = DumpRecord::from_event(event).expect("encode event DTO");
        assert!(matches!(record.into_event(2), Err(DumpError::InvalidRecord { line: 2, .. })));
    }

    #[test]
    fn clock_encoding_must_be_canonical() {
        let low = EventId::from_bytes([1; 32]).to_base64();
        let high = EventId::from_bytes([2; 32]).to_base64();
        assert!(matches!(decode_clock(vec![high, low], 3), Err(DumpError::InvalidRecord { line: 3, .. })));
    }
}
