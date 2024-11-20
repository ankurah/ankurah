use ankurah_core::property::YrsString;
struct Album {
    pub name: String,
}

impl ankurah_core::model::Model for Album {
    type Record = AlbumRecord;
    type ScopedRecord = AlbumScopedRecord;
    fn bucket_name() -> &'static str {
        "album"
    }
    fn new_scoped_record(id: ankurah_core::model::ID, model: &Self) -> Self::ScopedRecord {
        use ankurah_core::property::InitializeWith;
        let backends = ankurah_core::property::Backends::new();
        let field_name = YrsString::initialize_with(&backends, "name", &model.name);
        AlbumScopedRecord {
            id: id,
            backends: backends,
            name: field_name,
        }
    }
}
#[derive(Debug)]
pub struct AlbumRecord {
    scoped: AlbumScopedRecord,
}
impl ankurah_core::model::Record for AlbumRecord {
    type Model = Album;
    type ScopedRecord = AlbumScopedRecord;
    fn id(&self) -> ankurah_core::model::ID {
        use ankurah_core::model::ScopedRecord;
        self.scoped.id()
    }
    fn to_model(&self) -> Self::Model {
        Album { name: self.name() }
    }
}
impl AlbumRecord {
    pub fn new(node: &std::sync::Arc<ankurah_core::Node>, model: &Album) -> Self {
        let next_id = node.next_id();
        Self {
            scoped: <Album as ankurah_core::Model>::new_scoped_record(next_id, model),
        }
    }
    pub fn edit<'rec, 'trx: 'rec>(
        &self,
        trx: &'trx ankurah_core::transaction::Transaction,
    ) -> Result<&'rec AlbumScopedRecord, ankurah_core::error::RetrievalError> {
        use ankurah_core::model::Record;
        trx.edit::<Album>(self.id())
    }
    pub fn name(&self) -> <YrsString as ankurah_core::property::ProjectedValue>::Projected {
        let active = self.scoped.name();
        <YrsString as ankurah_core::property::ProjectedValue>::projected(&active)
    }
}
#[derive(Debug)]
pub struct AlbumScopedRecord {
    id: ankurah_core::model::ID,
    backends: ankurah_core::property::Backends,
    pub name: YrsString,
}
impl ankurah_core::model::ScopedRecord for AlbumScopedRecord {
    fn as_dyn_any(&self) -> &dyn std::any::Any {
        self as &dyn std::any::Any
    }
    fn as_arc_dyn_any(
        self: std::sync::Arc<Self>,
    ) -> std::sync::Arc<dyn std::any::Any + std::marker::Send + std::marker::Sync> {
        self as std::sync::Arc<dyn std::any::Any + std::marker::Send + std::marker::Sync>
    }
    fn from_backends(id: ankurah_core::ID, backends: ankurah_core::property::Backends) -> Self {
        let field_name = YrsString::from_backends("name", &backends);
        Self {
            id: id,
            backends: backends,
            name: field_name,
        }
    }
    fn id(&self) -> ankurah_core::ID {
        self.id
    }
    fn bucket_name(&self) -> &'static str {
        "album"
    }
    fn record_state(&self) -> ankurah_core::storage::RecordState {
        ankurah_core::storage::RecordState::from_backends(&self.backends)
    }
    fn from_record_state(
        id: ankurah_core::model::ID,
        record_state: &ankurah_core::storage::RecordState,
    ) -> Result<Self, ankurah_core::error::RetrievalError>
    where
        Self: Sized,
    {
        let backends = ankurah_core::property::Backends::from_state_buffers(&record_state)?;
        Ok(Self::from_backends(id, backends))
    }
    fn get_record_event(&self) -> Option<ankurah_core::property::backend::RecordEvent> {
        use ankurah_core::property::backend::PropertyBackend;
        let mut record_event =
            ankurah_core::property::backend::RecordEvent::new(self.id(), self.bucket_name());
        record_event.extend(
            ankurah_core::property::backend::YrsBackend::property_backend_name(),
            self.backends.yrs.to_operations(),
        );
        if record_event.is_empty() {
            None
        } else {
            Some(record_event)
        }
    }
}
impl AlbumScopedRecord {
    pub fn name(&self) -> &YrsString {
        &self.name
    }
}
impl<'a> Into<ankurah_core::ID> for &'a AlbumRecord {
    fn into(self) -> ankurah_core::ID {
        ankurah_core::model::Record::id(self)
    }
}
impl<'a> Into<ankurah_core::ID> for &'a AlbumScopedRecord {
    fn into(self) -> ankurah_core::ID {
        ankurah_core::model::ScopedRecord::id(self)
    }
}
