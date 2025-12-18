## Unified Navigation: Structured Properties, Refs, and Relation-Entities

### Motivation

AnkQL currently supports simple property comparisons (`name = ?`, `collection.property = ?`) but lacks the ability to:

1. Navigate across entity references (`artist.name` from an Album)
2. Query nested structured data (`licensing.territory = ?` inside a JSON field)
3. Traverse inbound references (find Artists by their Albums)
4. Support future relation-entities (hypergraph edges with their own properties)

This spec unifies these capabilities under a single path-oriented query surface that compiles to a schema-resolved representation.

### Scope

**In scope:**
- WHERE-clause semantics and compilation/resolution
- Path navigation syntax (`field.subfield.property`)
- Traversal filters (`[predicate]`) for multi-valued traversals
- Inbound navigation (`^Model.field`)
- Endpoint role selection (`->role`) for relation-entities
- Schema-driven resolution to PropertyId-based steps

**Out of scope:**
- Selection/projection (SELECT)
- Indexing strategies
- Wire protocol changes
- Relation-entity storage design

---

### Terminology

- **View**: The query's implicit root model context (e.g., `AlbumView`).
- **Property**: A field on a model schema, identified by a stable `PropertyId`.
- **Structured property**: A recursively-typed schema tree for inline data (JSON-like, not necessarily JSON syntax).
- **Ref**: A single pointer to another entity (`Ref<T>`).
- **MultiRef**: A collection of pointers (`Vec<Ref<T>>`).
- **Relation-entity**: An entity representing a relationship with endpoints and its own properties.
- **Step**: One navigation hop in a path.

---

### Surface Syntax

#### Path Navigation (baseline)

```
<path> <op> <expr>

path := path_step ('.' path_step)*
path_step := ident traversal_filter? role_select?
```

Examples:
- `name = ?` — scalar property
- `artist.name = ?` — ref traversal from Album to Artist
- `licensing.territory = ?` — structured property navigation

#### Inbound Traversal

Navigate the inverse of a reference field defined on another model:

```
inbound_step := '^' ModelName '.' ident traversal_filter?
```

Examples:
- `ctx.fetch::<ArtistView>("^Album.artist[name = ?]", "OK Computer")` — Artists with an album named "OK Computer"

Semantics:
- `^Album.artist` selects Album entities whose `artist` field points to the root entity
- Always multi-valued (returns a set)
- Filters `[...]` constrain the inbound set (ANY semantics)

#### Traversal Filters

Attach predicates to multi-valued path steps:

```
traversal_filter := '[' <predicate> ']'
```

Examples:
- `genre[name = "Rock"].name = ?` — any related genre with name "Rock"
- `^Album.artist[year >= ?].name = ?` — inbound albums from a given year

Rules:
- Valid on **multi-valued** traversals (`Vec<Ref<T>>`, inbound steps, relation-entity sets)
- Invalid on **single-valued** traversals (`Ref<T>`)

#### Endpoint Role Selection (future)

For relation-entities with multiple endpoints:

```
role_select := '->' ident
```

Example:
- `contribution[instrument = "guitar"]->person.name = ?`

#### Design Note: No `[:Type]` Selector

An earlier draft considered a separate `[:Type]` syntax for selecting relation-entity types:
```
album.contribution[:ProducerCredit][role = "executive"]->person.name = ?
```

**Decision**: This is **not included**. Instead, filter on a `kind`/`type` field:
```
album.contribution[kind = "ProducerCredit" AND role = "executive"]->person.name = ?
```

This keeps the language uniform — only predicates, no special type selectors.

---

### Schema Model

#### Entity Schema

Each View describes fields:
- `Scalar` — string/int/bool/etc
- `Structured` — recursive JSON-like tree
- `Ref(TargetModel)` — single entity reference
- `Vec<Ref(TargetModel)>` — multiple entity references
- `Relation(RelationView)` — (future) explicit relation-entity traversal

#### Structured Property Schema

Recursive tree of nodes:
- `Struct { fields: Map<String, StructuredNode> }`
- `List { element: StructuredNode }`
- `Scalar(Type)`
- `Any` — optional escape hatch

Each node addressable by `StructuredPropertyId` (parent PropertyId + child key).

#### Property Identifiers

- Top-level fields: `PropertyId`
- Structured nodes: `StructuredPropertyId`
- IDs are stable across builds given identical schema

---

### Resolution Rules

Given a root View schema:

1. Parse WHERE string into AST
2. For each path:
   - First step: resolve against root View fields, or parse as inbound step (`^Model.field`)
   - Subsequent steps:
     - `Structured`: resolve via structured schema
     - `Ref`/`MultiRef`: resolve against target model's fields
     - `Inbound`: context becomes the inbound source model
     - `Relation`: resolve via endpoint role selection or inference
3. Attach traversal filters only to multi-valued steps

#### Errors

- Traversal filter on single-valued step: invalid
- Role selection on non-relation step: invalid
- Inbound step must reference a ref-like field (`Ref<T>`, `Vec<Ref<T>>`, or relation endpoint)

---

### Semantics

#### Identity Boundaries

- **Structured traversal**: stays within the same entity (identity-less)
- **Ref traversal**: crosses to another entity
- **Relation-entity traversal**: binds an intermediate entity with its own properties

#### Multi-valued Traversal Semantics

`Vec<Ref<T>>` and relation-entity sets use **existential semantics** by default:

```
genre.name = "Rock"
```

Means: **any** related genre has name == "Rock".

Further constraints use `[...]` on the traversal step (no `any(...)`/`all(...)` wrappers).

#### Recheck Requirement

When traversal or structured filters serve as prefilters, the engine must recheck against source-of-truth storage to avoid false positives (especially with future indexing).

#### Null Handling

Null vs missing in structured properties: treated equivalently (both behave as NULL/absent).

---

### Defaults Locked In

| Aspect | Default |
|--------|---------|
| Multi-valued semantics | Existential (ANY) |
| Traversal constraints | Via `[...]` on step |
| Null vs missing | Equivalent |
| Type casting | Existing casting system |

---

### Remaining Open Items

1. **Inbound step ergonomics**: Always require `^Model.field`, or allow shorthand when unambiguous?
2. **Binary relation-entity inference**: When to infer target role vs require `->role`?
3. **Ref/MultiRef type syntax**: How do users declare `Ref<Artist>` in Rust models?

