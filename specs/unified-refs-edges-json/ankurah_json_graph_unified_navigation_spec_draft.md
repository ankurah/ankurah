# Unified Navigation for Structured Properties, Refs, and Relation-Entities

## Goal

Provide a single, readable path-oriented query surface (AnkQL WHERE) that treats:

- inline structured properties ("Json")
- `Ref<T>` links
- future relation-entities (edge entities; hypergraph-capable)

as **navigable steps** with optional **relation constraints**, compiling to a single MIR representation for planning and execution.

This spec focuses on WHERE-clause semantics and compilation/resolution. Selection/projection is out of scope.

---

## Terminology

- **View**: the query’s implicit root model context (e.g., `ArtistView`).
- **Property**: a field on a model schema, identified by a stable `PropertyId`.
- **Structured property**: a recursively-typed schema tree for inline data ("Json" semantics, not necessarily JSON syntax).
- **Ref**: a single pointer to another entity (`Ref<T>`).
- **Relation-entity**: an entity that represents a relationship, with endpoints (two or more) and its own properties.
- **Step**: one navigation hop in a path.

---

## Requirements

1. **Path-first syntax**: dot-path navigation is the default.
2. **Optional relation constraints on a step**: filter the relation between source and target without switching to Cypher pattern verbosity.
3. **Schema-driven resolution**: query strings resolve to `PropertyId`-based steps via the active View schema.
4. **Hypergraph-ready**: relation-entities may connect N endpoints; traversal can select an endpoint role.
5. **Backwards compatible**: existing AnkQL with `?` substitution continues to work.

---

## Surface Syntax (WHERE)

### A. Path navigation (existing / baseline)

```
<path> <op> <expr>

path := path_step ('.' path_step)*
path_step := ident traversal_filter? role_select?
```

A **path step** is one hop in the navigation chain. Whether a step is scalar/structured/ref/relation is determined during schema resolution.

Examples:

- `name = ?`
- `artist.name = ?`
- `licensing.something = ?`

### B. Inbound traversal (reverse navigation)

Inbound traversal allows following the inverse of a reference-like field defined on another model, without requiring a back-reference field.

Syntax:

```
inbound_step := '^' ModelName '.' ident traversal_filter? role_select?
```

Examples (shown in `ctx.fetch` style):

- Fetch **albums** by artist name (outbound): `ctx.fetch::<AlbumView>("artist.name = ?", "Radiohead")`
- Fetch **artists** that have an album named OK Computer (inbound): `ctx.fetch::<ArtistView>("^Album.artist[name = ?]", "OK Computer")`

Semantics:

- `^Album.artist` means: from the current root entity, select the set of `Album` entities whose `artist` points to the root entity.
- Inbound traversal is **always multi-valued** (a set), even if the underlying field is `Ref<T>`.
- Filters `[...]` on an inbound step constrain the inbound set (ANY semantics).

### C. Step-local traversal filter

Attach a filter to a path step using square brackets:

```
traversal_filter := '[' <predicate> ']'
```

Examples:

- `genre[name = "Rock"].name = ?` (ANY semantics; constrains the chosen related targets)
- `^Album.artist[year >= ?].name = ?` (constrains inbound albums)
- `album.contribution[instrument = "guitar" AND kind = "session"]->person.name = ?` (future; relation-entity filter)

Meaning:

- On **multi-valued traversals** (e.g., `Vec<Ref<T>>`, inbound steps, relation-entity sets), `[...]` constrains which related targets/relations participate.
- On **single-valued traversals** (`Ref<T>`), `[...]` is not applicable.

### D. Relation-entity predicates (no type selector)

There is no special "relation type" selector syntax. Relation-entities are just entities.

- If a traversal step resolves to a **relation-entity set**, constrain it with a normal traversal filter `[...]` on that step.
- If you need a "type", model it as a regular field on the relation-entity (e.g., `kind`, `type`, `label`) and filter it like any other property.

Example:

- `album.contribution[kind = "producer" AND instrument = "guitar"]->person.name = ?`

This keeps the language uniform: **only predicates**, no special-cased edge labels.

### E. Endpoint role selection (hypergraph)

When traversing a relation-entity with multiple endpoints, select the endpoint role explicitly:

```
role_select := '->' ident
```

Example:

- `album.contribution[instrument = "guitar"]->person.name = ?`

Meaning:

- `->person` selects which endpoint ref on the relation-entity you traverse to.
- For binary relation-entities, the target role may be inferred when unambiguous; otherwise `->role` is required.

```
<path> <op> <expr>

path := ident ('.' ident)*
```

Examples:

- `name = ?`
- `album.artist.name = ?`
- `track.licensing.something = ?`

### B. Step-local traversal filter

Attach a filter to a path step using square brackets:

```
path_step := ident traversal_filter?
traversal_filter := '[' <predicate> ']'
```

Examples:

- `genre[name = "Rock"].name = ?` (ANY semantics; constrains the chosen elements)
- `album.artist.name = ?` (single ref; no filter)
- `album.contribution[instrument = "guitar"]->person.name = ?` (future; relation-entity filter)

Meaning:

- On **multi-valued traversals** (e.g., `Vec<Ref<T>>`, relation-entity sets), `[...]` constrains which related targets/relations participate.
- On **single-valued traversals** (`Ref<T>`), `[...]` is not applicable (a single pointer is not a set).

### C. Optional relation type selector (future)

Allow selecting a specific relation-entity type/view when a field may traverse through different relation-entities (or when you want to be explicit):

```
relation_type := '[:' Ident ']'
path_step := ident relation_type? traversal_filter?
```

Example:

- `album.contribution[role = "producer"]->person.name = ?`

Meaning:

- \`\` selects the **relation-entity view/type** used for the traversal.
- The subsequent `[...]` filters that relation-entity instance set.

Notes:

- If the schema makes the relation-entity type unambiguous, `[:Type]` may be omitted.
- This is distinct from `[...]` (which is a predicate filter).

---

## Notes on "steps"

A "step" is one hop in a path (a dot-separated segment). Steps are an internal compilation unit: parsing yields steps as identifiers plus optional modifiers (`[...]`, `[:Type]`, `->role`), and resolution turns them into property-id-based MIR steps.

---

## Schema Model

### 1) Entity schema

Each View has a schema describing fields:

- `Scalar` (string/int/bool/etc)
- `Structured` (recursive)
- `Ref(TargetModel)`
- `Vec<Ref(TargetModel)>`
- `Relation(RelationView)` (future; explicit relation-entity traversal)

### 2) Structured property schema (recursive)

Define structured properties as a tree of nodes:

- `Struct { fields: Map<String, StructuredNode> }`
- `List { element: StructuredNode }`
- `Scalar(Type)`
- `Any` (optional escape hatch)

Each node is addressable by a **StructuredPropertyId** (or `PropertyId` + child index), enabling resolution without string parsing at execution time.

Resolution is recursive:

- `track.licensing.something` resolves `licensing` as `Structured`, then `something` as a child node.

### 3) Property identifiers

- Top-level entity fields have `PropertyId`.
- Structured nodes have `StructuredPropertyId` (derived from their parent’s id + stable child key).
- IDs must be stable across builds given identical schema; evolution rules defined separately.

---

## AST vs MIR (Phased Plan)

You can absolutely keep moving fast without locking yourself into a corner.

### Reality check: AST mutability is already a thing

The current AST is already transformed post-parse (placeholders → literals via `populate`). So “AST is immutable” is not a constraint.

### Key tension

- You want **low churn now** (iterate quickly).
- You also want a place to put **resolved information** (PropertyId / StructuredPropertyId, inbound metadata, future edge-entity routing) and possibly **compile-time ID injection** later.

### Recommendation: two-phase approach

#### Phase 1 (now): Enriched AST (a.k.a. Resolved AST)

Keep a single tree type, but extend it so identifiers can carry:

- the **syntactic shape** (names, model names, modifiers)
- optional **resolved IDs** (filled in by a resolution pass)

This avoids introducing a whole new MIR right away while still giving the planner/executor a stable, schema-resolved representation.

Concrete shape:

- Add a path-capable identifier form (new `Identifier` variant or new `Expr` variant).
- Within that form, each step stores:
  - `name: String`
  - `type_sel: Option<String>` (future `[:Type]`)
  - `filter: Option<Predicate>` (`[...]`)
  - `role: Option<String>` (`->role`)
  - `resolved: Option<ResolvedStepIds>` (filled by resolver)

Inbound step stores:

- `source_model: String`
- `field: String`
- plus the same modifiers and `resolved` slot.

`ResolvedStepIds` (example):

- `property_id: PropertyId`
- `structured_id: Option<StructuredPropertyId>`
- `target_model_id: Option<ModelId>`
- `step_kind: StepKindResolved` (Scalar/Structured/Ref/MultiRef/Inbound/Relation)

This preserves fast iteration and makes compile-time ID injection straightforward later: macros can pre-fill `resolved`.

Serde note:

- Keep both names and ids; if ids are absent, resolve at runtime.
- If ids are present but stale, resolver can either reject or re-resolve depending on a versioning policy (out of scope here).

#### Phase 2 (later): MIR

Introduce a dedicated MIR once:

- you have multiple backends/planners,
- you start doing more serious rewrites/optimizations,
- or you want the query format to be stable/portable across schema evolution.

At that point, lowering becomes: `AST (syntax) → Resolved AST (ids attached) → MIR (normalized ops)`.

### Why this is a good compromise

- Minimal churn now: you mostly extend parsing + add a resolve pass.
- You still get the benefits of “resolved representation” today.
- You keep the door open for a future MIR without painting the AST into a semantic corner.

---

## Resolution Rules

Given an implicit root View schema:

1. Parse the WHERE string into AST.
2. For each `Path`:
   - Resolve the first step against root View fields, **or** treat it as an inbound step if it matches `^Model.field`.
   - For each subsequent step:
     - If current step is `Structured`, resolve via structured schema.
     - If current step is `Ref`/`MultiRef`, resolve against target model’s fields.
     - If current step is `Inbound`, the current context becomes the inbound source model (e.g., `Album`) and subsequent identifiers resolve on that model.
     - If current step is `Relation`, resolve against endpoint role selection or inferred target.
3. If a step includes `[...]`, attach it as a traversal filter **only if the step resolves to a multi-valued traversal** (`MultiRef`, `Inbound`, `Relation`).

Errors:

- Applying a traversal filter `[...]` to a **single-valued** step (`Ref<T>`) is invalid.
- Applying `->role` to a non-relation step is invalid.
- Inbound steps must specify `^Model.field` and the field must be ref-like (e.g., `Ref<T>`, `Vec<Ref<T>>`, or relation endpoints).

---

## Semantics

### 1) Structured vs Ref vs Relation-Entity

- Structured traversal stays within the same entity row/value; identity-less.
- Ref traversal crosses identity boundary to another entity.
- Relation-entity traversal binds an intermediate relation entity with its own properties and endpoints.

### 2) Multi-valued traversals

`Vec<Ref<T>>` and relation-entity sets produce a **set** of targets. Comparisons against fields under a multi-valued traversal use **existential semantics** by default:

- `genre.name = "Rock"` means **any** related genre has name == "Rock".

Further constraints on the traversal set should be expressed using `[...]` on the traversal step (rather than `any(...)`/`all(...)` wrappers).

### 3) Recheck / correctness

When traversal filters or structured filters are used as prefilters, the engine must recheck candidates against source-of-truth storage to avoid false positives (especially with future indexing).

### 3) Recheck / correctness

When relation filters or structured filters are used as prefilters, the engine must recheck candidates against source-of-truth storage to avoid false positives (especially with future indexing).

---

## Examples

### Current world (no relation-entities)

Models:

```rust
#[derive(Model)]
pub struct Artist { name: String }

#[derive(Model)]
pub struct Album {
  artist: Ref<Artist>,
  genre: Vec<Ref<Genre>>,
  name: String,
  year: String,
}

#[derive(Model)]
pub struct Track {
  name: String,
  length: u32,
  licensing: Json,
}
```

Queries:

- `name = ?`
- `artist.name = ?` (from AlbumView)
- `licensing.something = ?` (from TrackView)

### Future: relation-entity backing for artist link

```ankql
artist[role = "primary"].name = ?
```

or with explicit role selection:

```ankql
contribution[role = "primary"]->artist.name = ?
```

### Hypergraph-ish relation

If `Contribution` connects endpoints `album`, `person`, `instrument`:

```ankql
contribution[instrument = "guitar"]->person.name = ?
```

---

## Implementation Plan (Agent Checklist)

1. **Parser**
   - Extend path step grammar to accept optional traversal filter `[...]` and optional `->role`.
   - (No special relation-type selector; relation-entities are filtered via `[...]`.)
   - Preserve `?` placeholders.
2. **Schema descriptors**
   - Add recursive structured schema metadata for `Json` fields (manual or generated).
   - Introduce `StructuredPropertyId` addressing.
3. **Resolver**
   - Convert identifiers to property ids by walking entity schema and structured schema.
   - Attach relation filter only on relation-capable steps.
4. **MIR**
   - Define step variants and normalized predicate forms.
   - Lower AST to MIR with resolved ids.
5. **Evaluator (baseline)**
   - Implement execution semantics without indexes: navigate refs, iterate multi-refs, evaluate structured fields.
   - Existential semantics for multi-valued traversals.
6. **Forward-compatibility stubs**
   - Recognize relation syntax but return a clear error until relation-entities exist.
   - Ensure MIR has `StepRelation` shape even if executor rejects it today.

---

## Defaults Locked In

- **Multi-valued traversal semantics**: existential (**ANY**) by default; no `any(...)`/`all(...)` wrappers.
- **Further traversal constraints**: expressed via `[...]` on the traversal step.
- **Null vs missing in structured properties**: treated equivalently for querying by default (both behave as NULL/absent).
- **Type casting/coercion**: use the existing casting system.

## Remaining Open Items

- Exact ergonomics for inbound steps: require `^Model.field` always, or allow a shorthand when unambiguous.
- Binary relation-entity role inference rule details (when to infer vs require `->role`).

