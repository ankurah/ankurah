# Canonical Value Types and Casting

## One type per property

Every registered property has a **canonical value type** -- `"string"`,
`"i32"`, `"i64"`, `"f64"`, `"bool"`, `"entityid"`, `"json"`, and so on --
fixed when the property is first allocated in the catalog and never changed
by registration afterward. Every value emitted by a compliant commit under
that property is stored *in* that type, regardless of what type the writing
binary was compiled with. Schema-blind ingress can preserve legacy or
malformed off-type residue; the defensive read boundaries below contain it.

The reason is collation. Storage engines materialize property values into
columns and build ordered indexes over them; the reactor matches live-query
predicates against entities through a byte-collated index. None of that
works over a mixed-type population: `"10"` sorts before `"2"` as a string
and after it as a number, and an equality lookup keyed on one type's bytes
misses a value stored in another's. Casting values at comparison time cannot
cure a mixed population that has already been stored. So compliant writers
guarantee one property, one type, while readers contain any off-type residue
at the boundaries described below.

## Registration is the type authority

A binary declares each property's type (via the `Property::VALUE_TYPE`
associated const, emitted by `#[derive(Model)]`). At registration, that
declaration is checked against the canonical type -- and the check is a
**compatibility question, never a mutation**:

- If the types match, the registration is the ordinary no-op upsert.
- If they differ but are **mutually castable** -- a cast exists in both
  directions per `Value::cast_to` (`core-types/src/value.rs`), e.g. `i64`
  against a canonical `i32`, or `string` against a canonical `i64` -- the
  binary is admitted and operates through casts. A warning is logged; the
  canonical type does not move.
- If they are not mutually castable (`binary` against `string`, any backend
  change), registration fails loudly with `RegistrationError::NonCastable`,
  before the binary can write anything.

Two consequences follow. **Changing a field's type in your model struct does
not change the stored type.** The canonical type does not follow your code;
a struct-level retype either operates through casts or is refused at
registration. Actually changing a canonical type is reserved for a future
explicit migration mechanism -- a deliberate catalog operation, never a
side effect of editing a struct. Mutations and predicate-query paths register
before they write or resolve fields, so an incompatible binary is refused at
registration. A direct get by entity id is deliberately different: it can
load existing state without registering, and its typed getter performs only
the per-value projection described below.

The one type check that necessarily stays at write time is per-value: a
compatible type *pair* does not guarantee every *value* fits (five billion
does not fit a canonical `i32`; `"abc"` does not parse into one). Those
fail the writing transaction's commit with `PropertyError::NonCastable`.

## Where casts happen

The cast discipline is a small, closed set of boundaries. Typed application
paths otherwise read and write canonically-typed values without repeated
checks; schema-blind ingress remains the explicit exception described below.

**Writes canonicalize at commit.** When a transaction commits, each staged
value is resolved from its field name to its property id and cast to the
canonical type *before the event is generated* -- so events, state buffers,
storage engines, and downstream indexes all receive one consistent type. A
value the canonical type cannot represent fails this commit, at the writer.

**Getters cast back out.** A typed view accessor (`film.year()` returning
`i64`) casts the stored canonical value to the compiled type on read. A
per-value failure -- including a legacy or ill-typed payload that reached
the entity some other way -- surfaces as `PropertyError::NonCastable`,
never a silently fabricated default.

**Query literals canonicalize at resolution.** When a predicate is resolved
against the catalog (at fetch, live-query, and subscription origins), each
comparison literal opposite a resolved property reference is cast to that
property's canonical type. `year = 2020` written by an `i64`-compiled
binary collates as an `i32` against a canonical-`i32` property. A literal
the canonical type cannot represent fails the query loudly at its origin.
(JSON subfield comparisons like `data.age > 5` are the exception: the
catalog does not describe subfield types, so they keep JSON semantics.)

**Every execution consumer re-checks.** Origin resolution is early validation,
not proof attached to the AST. Each consumer re-casts a comparison literal or
index bound immediately before it enters that consumer's comparison domain.
The reactor casts both watcher thresholds and changed entity values to the
registered property type at its comparison-index boundary. If a changed stored
value cannot be cast, the index conservatively wakes every watcher for that
property and exact predicate evaluation decides membership; the malformed
value is never allowed to cause a false negative.

Storage engines choose the authoritative execution type appropriate to their
physical representation. PostgreSQL uses the actual materialized column type.
SQLite prefers the catalog's logical type because SQLite affinity loses
distinctions such as Boolean versus integer and JSON versus binary, falling
back to materialized affinity when no resolver is available. Sled and
IndexedDB have no separate physical type registry, so they use the catalog's
canonical type for boundary casts and index collation.

**Sort keys type themselves at the execution boundary.** A resolved ORDER BY
item carries the property's stable id beside its display path, so rebuilding a
relay subscription after a rename keeps the same property and collation. Each
storage engine resolves the physical field through its own durable map and
uses the execution-type authority described above. The reactor uses the
catalog's canonical type; only standalone/test reactors without catalog
metadata fall back to a sampled value and then string collation. (Before
canonical types existed, collation was hardcoded to strings, which orders `10`
before `2`.)

**Indexes collate in exactly one domain.** The reactor comparison index casts
thresholds when watchers are registered and changed values when it selects
candidate watchers. Storage-engine index encoding (`encode_component_typed`
in `core/src/indexing/encoding.rs`) casts each component to the index's
expected type before encoding; only a failed cast is a type-mismatch error, so
an index never contains mixed-type bytes.

Property identity and physical addressing are separate from type lookup. A
resolved `PropertyId` reaches an engine-owned durable map: PostgreSQL and
SQLite map it to a column, IndexedDB maps it to a JSON object key, and sled
maps it to a numeric slot. The catalog supplies registered names and logical
types; it never resolves a storage column or materialization address.

## What deliberately does not happen

There are **no type-pair gates on the read or query path**. Operations that
require catalog resolution register first; a direct get by entity id does
not. Neither path re-runs registration compatibility during evaluation.
Predicate evaluation and policy inspection have no type-error surface at
all, while a typed getter can still return the per-value `NonCastable` error
below. Data sitting under a *different* property id than the one being read
is simply not that field: it never substitutes, and reads of the requested
property see absence, not an error.

The two `NonCastable` errors are the entire failure vocabulary:

| Error | When | Meaning |
|---|---|---|
| `RegistrationError::NonCastable` | registration | the declared type pair has no mutual cast against the canonical type |
| `PropertyError::NonCastable` | a write's commit, or a typed read | this particular value does not fit (overflow, unparseable) |

See [Property Backends](property-backends.md) for the storage layer these
types flow through, and [Storage Engine Layer](storage-engines.md) for how
engines materialize and index the canonically-typed values.
