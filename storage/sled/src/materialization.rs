use ankql::selection::filter::Filterable;
use ankurah_proto::{CollectionId, EntityId};

// Lightweight filterable over materialized values
pub(crate) struct MatEntity {
    pub(crate) id: EntityId,
    pub(crate) collection: CollectionId,
    pub(crate) map: std::collections::BTreeMap<String, ankurah_core::property::PropertyValue>,
}
impl Filterable for MatEntity {
    fn collection(&self) -> &str { self.collection.as_str() }
    fn value(&self, name: &str) -> Option<String> {
        if name == "id" {
            return Some(self.id.to_base64());
        }
        self.map.get(name).and_then(|v| match v {
            ankurah_core::property::PropertyValue::String(s) => Some(s.clone()),
            ankurah_core::property::PropertyValue::I16(x) => Some(x.to_string()),
            ankurah_core::property::PropertyValue::I32(x) => Some(x.to_string()),
            ankurah_core::property::PropertyValue::I64(x) => Some(x.to_string()),
            ankurah_core::property::PropertyValue::Bool(b) => Some(b.to_string()),
            ankurah_core::property::PropertyValue::Object(bytes) | ankurah_core::property::PropertyValue::Binary(bytes) => {
                Some(String::from_utf8_lossy(bytes).to_string())
            }
        })
    }
}
