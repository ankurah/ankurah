use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CollectionId(String);

impl CollectionId {
    // Most collection ids will be entities, but system collection(s) will have a fixed name
    pub fn fixed_name(name: &str) -> Self { CollectionId(name.to_string()) }
}

impl From<&str> for CollectionId {
    fn from(val: &str) -> Self { CollectionId(val.to_string()) }
}
impl PartialEq<str> for CollectionId {
    fn eq(&self, other: &str) -> bool { self.0 == other }
}

impl From<CollectionId> for String {
    fn from(collection_id: CollectionId) -> Self { collection_id.0 }
}
impl AsRef<str> for CollectionId {
    fn as_ref(&self) -> &str { &self.0 }
}

impl CollectionId {
    pub fn as_str(&self) -> &str { &self.0 }
}

impl std::fmt::Display for CollectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}
