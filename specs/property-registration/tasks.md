# Property Registration Tasks

## Phase 1: Foundation

- [ ] **Expand sys::Item enum** - Add Model and Property variants to `proto/src/sys.rs`
- [ ] **Define BackendKind enum** - Language-agnostic backend identifiers
- [ ] **Define ValueType enum** - Language-agnostic primitive types for LWW
- [ ] **Update SystemManager** - Add methods for Model/Property entity CRUD

## Phase 2: Backend Refactoring

- [ ] **Refactor YrsBackend** - Accept property entity IDs instead of string names
- [ ] **Refactor LWWBackend** - Accept property entity IDs instead of string names
- [ ] **Internal ID mapping** - Implement map table for compacting entity IDs (optional optimization)
- [ ] **LWW native type registry** - Register value types with serialization logic

## Phase 3: Registration Flow

- [ ] **Model registration** - Ensure Model entity exists on first access
- [ ] **Property registration** - Ensure Property entities exist for all fields
- [ ] **Entity ID caching** - Cache property entity IDs for runtime efficiency
- [ ] **Derive macro updates** - Generate registration code from field metadata

## Phase 4: Read/Write Path Updates

- [ ] **Property lookup** - Look up Property entity on property access
- [ ] **Empty string fix** - Return default for missing required properties
- [ ] **Optional handling** - Return None for missing optional properties
- [ ] **Unknown property error** - New PropertyError variant for unregistered properties

## Phase 5: Ref<T> Support

- [ ] **target_model field** - Add to Property entity for ref types
- [ ] **Bootstrapping solution** - Handle circular Model references
- [ ] **Ref registration** - Register target model relationship

## Future / TBD

- [ ] **Migration tooling** - Upgrade existing data to new schema
- [ ] **Schema validation** - Runtime checks against registered schema
- [ ] **TypeScript bindings** - Generate TS types from property metadata
- [ ] **yrs_map backend** - Collaborative map support
- [ ] **yrs_array backend** - Collaborative array support

## Notes

Tasks will be added/refined as implementation progresses. Each task may spawn subtasks.
