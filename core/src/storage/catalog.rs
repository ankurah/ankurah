use ankurah_proto::{ModelId, PropertyId};

/// Optional labels for engine-owned physical names; never register definitions.
#[async_trait::async_trait]
pub trait CatalogResolver: Send + Sync {
    /// Best-effort model label; None after a bounded wait if unavailable. Engines persist their own fallback name.
    async fn get_model_label(&self, model: &ModelId) -> Option<String>;

    /// Best-effort property label; None after a bounded wait if unavailable. Engines persist their own fallback name.
    async fn get_property_label(&self, property: &PropertyId) -> Option<String>;
}
