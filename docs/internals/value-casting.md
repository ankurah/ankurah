# Canonical Value Types and Casting

## One type per property

Every registered property has a **canonical value type** -- `"string"`,
`"i32"`, `"i64"`, `"f64"`, `"bool"`, `"entityid"`, `"json"`, and so on --
fixed when the property is first allocated in the catalog and never changed
by registration afterward. Every value stored under that property is stored
*in* that type, regardless of what type the writing binary was compiled
with.

The reason is collation. Storage engines materialize property values into
columns and build ordered indexes over them; the reactor matches live-query
predicates against entities through a byte-collated index. None of that
works over a mixed-type population: `"10"` sorts before `"2"` as a string
and after it as a number, and an equality lookup keyed on one type's bytes
misses a value stored in another's. Casting values at comparison time cannot
cure a mixed population that has already been stored. So the system
guarantees the population is never mixed: one property, one type, enforced
at the boundaries described below.

## Registration is the type authority

A binary declares each property's type (via the `Property::VALUE_TYPE`
associated const, emitted by `#[derive(Model)]`). At registration, that
declaration is checked against the canonical type -- and the check is a
**compatibility question, never a mutation**:

- If the types match, the registration is the ordinary no-op upsert.
- If they differ but are **mutually castable** -- a cast exists in both
  directions per `Value::cast_to` (`core/src/value/cast.rs`), e.g. `i64`
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
side effect of editing a struct. And because `trx.create` and first-use
reads both register before any data flows, an incompatible binary is
refused **at registration time, not at write time**.

The one type check that necessarily stays at write time is per-value: a
compatible type *pair* does not guarantee every *value* fits (five billion
does not fit a canonical `i32`; `"abc"` does not parse into one). Those
fail the writing transaction's commit with `PropertyError::NonCastable`.

## Where casts happen

The cast discipline is a small, closed set of boundaries. Everything else
reads and writes canonically-typed values without checking anything.

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

**Evaluation reads defensively.** Predicate evaluation and the reactor's
watcher-index extraction read entity values through a canonicalizing
boundary: an off-type value is cast to canonical, and a value that cannot
be cast evaluates as NULL with a warning -- a junk payload never errors a
query or matches anything, it is simply absent to predicates. (Ingest is
deliberately schema-blind -- applying remote events never blocks on the
catalog -- so a misbehaving peer can land an ill-typed payload; this
boundary is what contains it.)

**Sort keys type themselves from the canonical type.** ORDER BY key
parts -- in the storage planner's index scans and in the reactor's ordered
live queries alike -- resolve their column through the catalog to the
canonical value type, so numerics sort numerically. (Before canonical
types existed, sort collation was hardcoded to strings, which orders
`10` before `2`.) A column the catalog cannot name falls back to string
collation.

**Indexes collate in exactly one domain.** The reactor's comparison index
converts predicate literals to `Value` at insertion so both sides of every
lookup collate under the same byte encoding. Storage-engine index encoding
(`encode_component_typed` in `core/src/indexing/encoding.rs`) casts each
component to the index's expected type before encoding; only a failed cast
is a type-mismatch error, so an index never contains mixed-type bytes.

## What deliberately does not happen

There are **no type gates on the read or query path**. By the time a read
or query runs, registration has already answered the type-pair question,
so nothing re-checks it -- predicate evaluation and policy inspection have
no type-error surface at all. Data sitting under a *different* property id
than the one being read is simply not that field: it never substitutes,
and reads of the requested property see absence, not an error.

The two `NonCastable` errors are the entire failure vocabulary:

| Error | When | Meaning |
|---|---|---|
| `RegistrationError::NonCastable` | registration | the declared type pair has no mutual cast against the canonical type |
| `PropertyError::NonCastable` | a write's commit, or a typed read | this particular value does not fit (overflow, unparseable) |

See [Property Backends](property-backends.md) for the storage layer these
types flow through, and [Storage Engine Layer](storage-engines.md) for how
engines materialize and index the canonically-typed values.
