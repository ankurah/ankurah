# Property Registration Specification

## Problem Statement

Empty strings in Yrs are encoded as missing properties. After serialization/deserialization round-trip, `txn.get_text("field")` returns `None` for a field that was set to `""`. This makes it impossible to distinguish between:
1. A property that was never set
2. A property that was explicitly set to empty string

See: https://github.com/ankurah/ankurah/issues/175

## Root Cause

Yrs CRDTs track operations, not state. Inserting an empty string (`text.insert(0, "")`) creates no CRDT operations, so after serialization there's no record the field ever existed.

## Solution: Property Registration

Implement a property registration system where Models and Properties are represented as Entities in the system collection. This provides:

1. **Schema awareness** - Know which properties *should* exist for a model
2. **Empty string fix** - If a registered required property is missing from backend, return empty/default instead of error
3. **Type casting foundation** - Enable proper type information for queries and migrations
4. **Language agnostic** - Support future TypeScript and other language bindings
5. **Property renames** - Change property name without losing data (entity ID is stable)

## Design Principles

1. **Models are Entities** - Registered in `_ankurah_system` collection
2. **Properties are Entities** - Linked to their Model via `Ref<Model>`
3. **Backend keying by Entity ID** - Backends store/retrieve using property entity IDs, not string names
4. **Language agnostic type descriptors** - No Rust-specific type info in property metadata
5. **Model ≠ Collection** - Model defines schema, Collection is storage (divergence intended)

## Related Issues

- https://github.com/ankurah/ankurah/issues/85 (Property registration)
- https://github.com/ankurah/ankurah/issues/175 (Empty string missing property)
