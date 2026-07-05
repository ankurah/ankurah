# Tasks

Companion to plan.md (authority: rfc.md, ratified rev 3). Groups are in
landing order; each checkbox is intended to be a commit or a small commit
train, red-to-green where a test can pin the change.

## 1. Phase 0: #294 protocol version (separate PR off main)

DONE: PR #306; decision record posted on #294.

- [x] `PROTOCOL_VERSION: u32 = 1` in proto; `protocol_version: u32`
      appended as the last field of `Presence`; compatibility check
      isolated in one function (equality for now).
- [x] `Message::PresenceRejected { expected: u32, received: u32 }`
      variant.
- [x] Fallible `register_peer`: version mismatch (including implied
      version 0) refuses with `PresenceRejected` sent best-effort, then
      teardown via the existing deregister path; all four connectors
      (websocket-server, websocket-client, websocket-client-wasm,
      local-process) propagate the refusal and close instead of
      dropping the frame.
- [x] Presence decode failure before establishment closes the
      connection with an actionable log (version-0 peer guidance)
      instead of leaving it open.
- [x] Tests: same-version connect; mismatch refused in both directions
      (rejection observed); hand-crafted version-0 Presence bytes
      (old-shape mirror enum) refused without panic; existing
      integration suite green.
- [x] Close out #294: record refuse-semantics decision and the deferred
      policy-hook question on the issue.

## 2. Phase A foundation: derivation, frozen encoder, create-with-id

- [x] `proto/src/schema_id.rs`: model/property/membership id derivation
      (domain tags v1, u64-LE length prefixes, SHA-256 first 16 bytes);
      zero-model-id standalone scope constant; golden-vector tests.
- [x] Ulid audit: verify nothing interprets EntityId timestamp bits
      (fact-checked once already: no readers); record in PR
      description; regression test that from_bytes round-trips
      hash-derived ids.
- [x] `core/src/schema/genesis.rs`: frozen genesis encoder for the
      three catalog kinds (pinned LWWDiff v1 name-keyed shape, scalar
      Values, empty parent clock); golden byte vectors independent of
      lww.rs.
- [x] `validate_catalog_genesis`: recompute entity id from payload and
      event id from re-encoding; tamper tests (field value, field set,
      operation count).
- [x] Create-with-derived-id: RESOLVED without new plumbing. The
      executor rides `get_retrieve_or_create` +
      `commit_remote_transaction` (the receive path), which already
      materializes entities under given ids; the phantom guard only
      constrains client transactions, which registration does not use.

## 3. Catalog collections, protection, registration operation

- [x] Collection constants `_ankurah_model`, `_ankurah_property`,
      `_ankurah_model_property`; PROTECTED_COLLECTIONS extended to all
      four system collections.
- [x] `_ankurah_` prefix reservation in CollectionSet::get (user paths
      refused, system callers allowed); test.
- [ ] Catalog entity accessors in the SysRoot raw-entity style (system
      models; never derive(Model)).
- [x] proto descriptors (ModelDescriptor, PropertyDescriptor,
      MembershipDescriptor) + `NodeRequestBody::RegisterSchema`.
- [x] Durable-side executor: derive ids, catalog lookups, anchor-reuse
      refusal, explicit-id verification, frozen genesis + LWW follow-up
      events, PolicyAgent::check_event on every event, persist + relay;
      Success/Error response.
- [x] Receiver-side protection: CommitTransaction events targeting
      protected collections refused outright (local and remote paths);
      tests.
- [x] Tests: registration from descriptors alone on a schema-less
      server; idempotent re-issue (heads unchanged); anchor rename then
      reuse-refusal; explicit-id sharing with per-contract optionality;
      not-found and retype-mismatch hard-fails; ephemeral refusal.
- [ ] Relay-side self-certification: validate_catalog_genesis on
      catalog events arriving via subscription (with group 4).
- [ ] Multi-durable propagation: CommitTransaction is refused for
      catalog collections by design, so durable-durable catalog
      transport is the registration operation re-issued or the
      subscription relay; decide and implement with group 4.
- [ ] Policy-denial test (needs a denying PolicyAgent fixture).
- [ ] Client lifecycle: ensure-registration on first mutating use;
      derive+cache on read paths; `ctx.register::<M>()`; offline queue
      drained on durable-peer connect (needs group 8 descriptors).

## 4. Catalog subscription and map

- [x] CatalogMap: id -> definition maps for all three kinds; collection
      -> model; model -> membership set; per-collection display-name
      index + global name index (sibling gate feed); resolve/lookup API.
- [x] Warm + incremental updates: durable nodes subscribe FIRST
      (fetch-free reactor queries) then merge the storage scan, so a
      mid-warm registration is never missed; ephemeral nodes stand up
      three relay-backed LiveQueries on first context_async (deviation:
      sync context() cannot spawn the subscription without perturbing
      reactor timing; ensure_subscribed is public for explicit use).
- [x] `wait_catalog_ready` gate; `hard_reset` flushes the catalog map
      via a reset hook installed on SystemManager; tests for both
      (tests/tests/catalog_map.rs, 6 tests incl. rename re-indexing and
      ephemeral live updates).
- [ ] StorageEngine::list_collections overrides for postgres, sqlite,
      and IndexedDB (default is empty, so a RESTARTING durable node on
      those engines currently warms cold and relies on live updates
      only; sled is covered). Small, engine-local; fits Phase C or
      earlier.

## 5. LWW v2 / state 0xA2

- [x] Wire shapes: LWW_DIFF_VERSION_2, LWW_STATE_VERSION_2 (0xA2);
      by_id + residue two-map payloads; round-trip tests.
- [x] In-memory PropertyKey::{Id, Name} over the ValueEntry lifecycle;
      SchemaBinding (bind_schema migration) and WireMode per instance;
      default NameKeyedV1 byte-identical to today, so catalog/system
      collections are pinned by default. DORMANT until integration.
- [x] Read fallback mechanics: 0xA1 and pre-0.9 buffers decode to Name
      keys, bind_schema migrates known names to ids, rewrite-on-save
      emits 0xA2 with residue preserved; v1 diffs apply the same way.
- [ ] Integration flip: entity assembly attaches SchemaBinding sourced
      from (local compiled schema, catalog map) and sets IdKeyedV2 for
      user collections; catalog/system stay NameKeyedV1 (with groups
      6-8).
- [x] Unknown-id v2 payloads apply and persist opaquely (catalog lag);
      unprojectable until a binding knows the id.
- [x] Compatibility tests: 0xA3/v3 refused with the shipped refusal arm
      (the same arm a 0.9 binary refuses 0xA2 with); default-mode
      byte-compatibility pinned; residue preserved through rewrite.
- [ ] PROTOCOL_VERSION -> 2 lands with this epoch (one bump covering
      LWW v2, Identifier AST, RegisterSchema).

## 6. ankql Identifier and resolution

- [ ] `Identifier { property, name, subpath }` AST node; PathExpr stays
      the parse form.
- [ ] Resolution pass (local compiled schema, then catalog map);
      UnknownProperty fail-closed naming collection and property;
      wait_catalog_ready deferral for schema-less consumers; absorbs
      the TypeResolver pass at its four call sites.
- [ ] Fetch/SubscribeQuery carry resolved Selections; receiver-side
      pass-through for unknown ids (until #274).
- [ ] Engines consume Identifier.name for columns; assume_null /
      referenced_columns keyed consistently via Identifier.
- [ ] Unify missing-property semantics (filter error / reactor
      unwrap_or(false) / SQL assume_null) under the one rule.
- [ ] Tests: resolve via schema, via catalog only, UnknownProperty on
      neither; subpath preservation; cross-node rename scenario
      resolves both display names to one property id.

## 7. Read-path rules (#175 fix)

- [ ] Rule ladder with membership-sourced optionality
      (optional-on-ambiguity; contract in play); View getters keep
      compiled optionality.
- [ ] Cross-contract sibling gate -> PropertyError::TypeSkew naming
      both ids; test with retype lineages from two contracts.
- [ ] Type defaults for required-absent scalars; entityid exception
      stays Missing.
- [ ] Zero-op creation events; un-ignore
      tests/tests/yrs_backend.rs:303-332; integration: create with
      empty string, reload, read back "".

## 8. Derive macro, attributes, lifecycle glue

- [ ] ModelDescriptor/FieldSchema emission per the normative mapping
      table; ephemeral fields excluded; descriptor snapshot test
      covering every table row.
- [ ] `#[property(anchor = "...")]`; anchor chains keep the original
      anchor; anchor-reuse refusal test (retired name re-minted ->
      refusal demanding an anchor).
- [ ] `#[model(id = "...")]` / `#[property(id = "...")]`: compile-time
      id parse; registration-time verification (backend/value_type
      match, cold-start hard fail); shared-property membership minting;
      test: shared property read from two contracts with differing
      optional.
- [ ] `_ankurah_` collection prefix -> derive-time compile error
      (trybuild or equivalent).
- [ ] Registration triggers in context paths (mutating auto-assert,
      read-path cache-only, explicit register).

## 9. Cross-cutting and pre-PR

- [ ] Error variants: UnknownProperty, TypeSkew, CatalogGenesisError,
      registration refusals.
- [ ] Nomenclature pass over new code/docs (#305: Model = contract
      definition entity, Collection = storage table).
- [ ] Opus adversarial review: frozen encoder, v2/v1 fallback,
      registration convergence.
- [ ] Validation gate: cargo test -p ankurah-core (lib), ankurah-tests,
      jwt-auth; cargo check -p ankurah-core --features wasm; cargo fmt
      --all; taplo fmt if Cargo.toml changed.
- [ ] Progress note on #289; PR when reviewable.

## Phase C (tasks written when Phase A stabilizes)

- sled property_config rekey; postgres/sqlite catalog-bound columns,
  rename DDL, collision suffixes; IndexedDB re-materialization; seam
  cleanliness per RFC 4a.
