#Ankurah Design goals:

# Schema / UX:

- Define schema using "Model" structs, which define the data types for a collection of entities
- An ActiveRecord style interface with type-specific methods for each value
- Typescript/Javascript bindings allow these Model definitions to be used client or serverside
- Macros to created and query entities in the collection

# Observability

- Utilize a "signal" style pattern to allow for observability of changes to entities, collections, values
- Derivative signals can also be created which filter, combine, and transform those changes
- React bindings are for this are a key consideration
- Leptos and other rust web frameworks should also work, but are lower priority initially

# Storage and state management

- Multiple backing stores including
  - Sled KV Store (initial)
  - Posgres
  - TiKV
  - Others
- "Event Sourced" / operation based / Audit trail
  - All operations have a unique ID, and a list of precursor operations, and are immutable (ish. Discuss CRDT compaction, GDPR)
  - "Present" state of an entity is maintained per node, which includes the "head" of the operation tree directly, to determine if a node has the latest version of an entity
- Operation IDs will initially be ulids to allow for distributed ID generation (and lexocographical ordering)
  Discuss: How can this be modified to provide non-adversarial, and perhaps also adversarial cryptographic collision/attack resistance?
- The id of an entity should be the initial operation id that created it, to provide the genesis operation for that entity.
  Discuss: How should we index operations? Would it be (entity ulid + operation ulid?)
