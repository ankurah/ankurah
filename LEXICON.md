# Lexicon

The words Ankurah uses to describe itself. Contributions, including pull
request descriptions, use these terms rather than inventing new ones for ideas
that already have a name here. See [AI_POLICY.md](./AI_POLICY.md).

The canonical, maintained version lives in the Ankurah book:
https://ankurah.org/glossary.html. If this file and the book ever disagree, the
book wins.

## Core terms

The nouns you will reach for most. These match the README.

- **Component**: an entity-membership set with an associated property schema.
  An entity can have multiple components. Existing APIs still call this `Model`
  and identify it with `ModelId`; the code rename is deferred.
- **Model struct**: a concrete, typed representation of a component's properties
  for one entity.
- **Entity**: one canonical identity and state, with a dynamic schema and a set
  of component memberships.
- **View**: a read-only, typed representation of an entity.
- **Mutable**: a mutable, typed representation of an entity's state.
- **Event**: an atomic change, used for synchronization and as an audit trail.

## Concurrency terms

For the vocabulary of how changes merge (events, clocks, heads, meets, and the
merge semantics), the book is the source of truth:

- https://ankurah.org/concurrency/index.html
- https://ankurah.org/internals/event-dag.html
