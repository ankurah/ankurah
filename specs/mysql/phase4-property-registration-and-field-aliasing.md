# Property Registration and Field Aliasing

Related: [ankurah/ankurah#85 — Property registration](https://github.com/ankurah/ankurah/issues/85)

## Problem

Currently, property names in ankurah are the lowercased Rust field name, passed as a plain `String` (see `core/src/property/mod.rs`: `pub type PropertyName = String`). The derive macro generates these from field names:

```rust
// derive/src/model/model.rs:38
#active_field_types_turbofish::initialize_with(&entity, #active_field_name_strs.into(), &self.#active_field_names);
```

And `active_field_name_strs()` returns `field.ident.to_string().to_lowercase()`.

This creates two problems:

1. **Renaming a Rust field breaks existing data.** The state buffer is a `BTreeMap<PropertyName, ...>` keyed by the string name. Rename `total_amount` to `amount` and all existing state buffers become unreadable for that property.

2. **The Rust field name must match the storage column name.** For existing MySQL tables where the column is `customer_id` but the Rust field should be `customer: Ref<Customer>`, there's no way to express the mapping.

Both problems are solved by decoupling the Rust field name from the property identity.

## Property Registration (Issue #85)

Properties become entities in the `_ankurah_property` system collection. Each property has:

- An **EntityId** (stable, immutable identity)
- A **collection** it belongs to
- A **name** (the current string name — may change over time via rename)
- A **type identifier** (backend name + value type, e.g., `lww:string`, `lww:ref`, `yrs:string`)

Property entities are upserted on first use based on `(collection, name, type)`. Once a property has an EntityId, that ID is the stable identity — the name can change without breaking existing data.

### Registration Flow

1. When a Model is first used (entity created or read), the derive-generated code registers its properties with the system.
2. Registration is an upsert: if a property with the same `(collection, name, type)` already exists, return its EntityId. If not, create it.
3. The property EntityId is cached locally after first lookup (via a subscription to the `_ankurah_property` collection, per issue #85 acceptance criteria).
4. State buffers and operations can transition from string-keyed to EntityId-keyed. (Migration strategy TBD — likely support both during a transition period.)

### Impact on State Serialization

Currently, `StateBuffers` is `BTreeMap<String, Vec<u8>>` where keys are backend names ("lww", "yrs"), and within each backend's state buffer, properties are keyed by `PropertyName` (String).

With property registration, the internal keying changes to use property EntityIds. This is a serialization format change that requires a migration path for existing data. The details of this migration are outside the scope of this spec but must be considered.

## Field Aliasing via Model Macro

The derive macro gains a `#[model(property = "...")]` attribute that decouples the Rust field name from the property identity in storage.

### Syntax

Two distinct attributes on `#[model(...)]`:

- **`name = "..."`** — Specifies the property name in storage. This is the column name in the database, the key in state buffers, and the name used for property registration. Use this when the Rust field name differs from the database column name.

- **`id = "..."`** — Specifies the property EntityId directly. This binds the field to an already-registered property by its stable identity. Use this for field renames: the EntityId is immutable even if the property's name changes.

These are mutually exclusive. If neither is specified, the lowercased Rust field name is used as the property name (current behavior).

```rust
#[derive(Model)]
#[model(table = "customer_order")]
pub struct CustomerOrder {
    pub order_num: String,
    pub status: i32,

    // Rust field is "customer", DB column is "customer_id"
    #[model(name = "customer_id")]
    pub customer: Ref<Customer>,

    // Rust field is "ship_address", DB column is "ship_to_addr_id"
    #[model(name = "ship_to_addr_id")]
    pub ship_address: Ref<CustomerAddress>,

    // No annotation — property name is "total_amount" (from field name)
    pub total_amount: String,

    // Field was renamed from "old_field" — bind to the original property by EntityId
    // so existing data is still accessible
    #[model(id = "01HXYZ...")]
    pub renamed_field: String,
}
```

### How `name` Works

When `#[model(name = "...")]` is present, the specified name replaces the field name for all storage operations:
- `InitializeWith::initialize_with(entity, property_name, value)` uses the specified name.
- `FromEntity` calls in View/Mutable use the specified name for property lookup.
- Property registration upserts using the specified name (not the Rust field name).
- Materialized column creation uses the specified name as the column name.

### How `id` Works

When `#[model(id = "...")]` is present, the field is bound directly to a registered property by EntityId:
- The property EntityId is used as the key in state buffers (once property-ID-keyed serialization is implemented).
- Property registration matches by EntityId rather than by name.
- The property's current name in the registry is used for materialized column names.
- This is the stable binding that survives both field renames and property name changes.

### When Neither Is Specified

Behavior is unchanged from today: the lowercased Rust field name is used as the property name. This name is used for property registration (upsert by collection + name + type).

### Derive Macro Changes

`ModelDescription` needs to:

1. **Parse `#[model(name = "...")]` and `#[model(id = "...")]` attributes.** The current `get_model_flag()` only supports bare idents (`#[model(ephemeral)]`). Extend attribute parsing to support `Meta::NameValue` pairs:

    ```rust
    fn get_model_attr(attrs: &[syn::Attribute], key: &str) -> Option<String> {
        // Parse #[model(key = "value")] and return the value string
    }
    ```

    Validate at compile time that `name` and `id` are mutually exclusive on the same field.

2. **Produce the effective property identifier per field.** New method:

    ```rust
    pub enum PropertyBinding {
        /// Default: property name derived from field name
        FieldName(String),
        /// Explicit name override: #[model(name = "...")]
        Name(String),
        /// Explicit property EntityId: #[model(id = "...")]
        Id(String),
    }

    pub fn property_bindings(&self) -> Vec<PropertyBinding> {
        self.active_fields.iter().map(|f| {
            if let Some(id) = get_model_attr(&f.attrs, "id") {
                PropertyBinding::Id(id)
            } else if let Some(name) = get_model_attr(&f.attrs, "name") {
                PropertyBinding::Name(name)
            } else {
                PropertyBinding::FieldName(
                    f.ident.as_ref().unwrap().to_string().to_lowercase()
                )
            }
        }).collect()
    }
    ```

3. **Use property bindings in code generation.** `model_impl()`, `view_impl()`, `mutable_impl()` all use the effective property name/id from the binding instead of the Rust field name for `InitializeWith`, `FromEntity`, property registration, and materialized column naming.

    For `PropertyBinding::FieldName` and `PropertyBinding::Name`, the string is used directly as the property name (same codegen as today, just with a different string).

    For `PropertyBinding::Id`, the generated code looks up the property by EntityId in the registry to get the current name for materialized column purposes, and uses the EntityId as the stable key in state buffers.

### Table Name Override

While we're extending the attribute parser, also support `#[model(table = "...")]` on the struct for overriding the collection/table name:

```rust
#[derive(Model)]
#[model(table = "customer_order")]
pub struct CustomerOrder { ... }
```

Currently `collection_str()` returns `self.name.to_string().to_lowercase()` which produces `customerorder`. The `table` attribute overrides this for cases where the actual table name differs.

## Interaction with Materialized Columns

Materialized columns (auto-created by the storage engine for query pushdown) use the property name as the column name. With field aliasing:

- A field `customer: Ref<Customer>` with `#[model(property = "customer_id")]` materializes as column `customer_id`.
- An ankql query `customer_id = 42` pushes down to `WHERE customer_id = 42` in SQL.
- The Rust code uses `order.customer()` — the Rust field name is ergonomic, the storage name matches the existing schema.

## Interaction with CDC (Phase 2)

The binlog listener sees MySQL column names, not Rust field names. With field aliasing, the column name in MySQL matches the property name in the property registry. The CDC listener uses the property name (= column name) when constructing synthetic LWW operations.

Without field aliasing, the CDC listener would need a separate mapping from column names to property names. With it, the mapping is built into the model definition and available through the property registry.

## Migration Path for Field Renames

### Using `name` (simple case)

1. Developer renames the Rust field from `total_amount` to `amount`.
2. Developer adds `#[model(name = "total_amount")]` to preserve the storage binding.
3. The property name in storage remains `total_amount`. No data migration needed.
4. The Rust API uses `.amount()` — ergonomic naming independent of storage.

### Using `id` (robust case)

1. Property `total_amount` was previously registered with EntityId `01HXYZ...`.
2. Developer renames the Rust field from `total_amount` to `amount`.
3. Developer adds `#[model(id = "01HXYZ...")]` to bind to the property by stable identity.
4. Existing state buffers (keyed by property EntityId) continue to work.
5. The property's name in the registry can also be updated from `total_amount` to `amount` if desired — this is a metadata change, not a data migration.

The `name` approach covers the common case (Rust field name differs from DB column name). The `id` approach is more robust — it survives both field renames and property name changes, because the EntityId is the immutable identity.
