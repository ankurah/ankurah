use super::Model;
use crate::property::Ref;

/// A model exported to JavaScript with a concrete reference wrapper.
pub trait WasmModel: Model {
    type RefWrapper: From<Ref<Self>> + Into<Ref<Self>>;
}
