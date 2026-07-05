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
- [x] Relay-side self-certification: validate_catalog_genesis runs in
      NodeApplier::validate_and_stage (the shared funnel for subscription
      updates AND fetch/subscribe delta bridges) for every genesis
      targeting a catalog collection; forged and payload-swapped geneses
      are refused before staging (tests/tests/catalog_genesis_relay.rs).
- [ ] Multi-durable propagation: CommitTransaction is refused for
      catalog collections by design, so durable-durable catalog
      transport is the registration operation re-issued or the
      subscription relay; decide and implement with group 4.
- [ ] Policy-denial test (needs a denying PolicyAgent fixture).
- [x] Client lifecycle: DONE with group 8 (see group 8 trigger entry).

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
- [x] Integration flip: Node::bind_entity attaches the catalog-built
      SchemaBinding and IdKeyedV2 mode at every user-entity assembly
      path (create, get, fetch, remote commit, subscription/delta
      apply); catalog/system stay NameKeyedV1; 0xA2 state entries carry
      display-name hints so unbound engine parsers keep materializing
      through the A-to-C window (plan decision 13); incoming v1
      name-keyed writes migrate onto id keys at apply so mixed-version
      writes compete in one LWW election (tests/tests/epoch_flip.rs).
- [x] Unknown-id v2 payloads apply and persist opaquely (catalog lag);
      unprojectable until a binding knows the id.
- [x] Compatibility tests: 0xA3/v3 refused with the shipped refusal arm
      (the same arm a 0.9 binary refuses 0xA2 with); default-mode
      byte-compatibility pinned; residue preserved through rewrite.
- [x] PROTOCOL_VERSION -> 2 landed (one bump covering LWW v2/0xA2,
      resolved Identifier selections, RegisterSchema).

## 6. ankql Identifier and resolution

- [x] `Identifier { property, name, subpath }` AST node (property is a
      raw Ulid; ankql cannot dep on proto); PathExpr stays the parse
      form; every Expr match site across ankql/core/storage handles
      Identifier with Path-equivalent semantics; assume_null keys on
      the resolved name for subpaths BY DESIGN (the first-vs-last-step
      fix). NOTE for the resolution slice: Identifier evaluation
      currently shares Path's legacy collection-qualifier branch; a
      post-resolution Identifier should skip it (name == collection
      edge).
- [x] Resolution pass (CatalogManager::resolve_selection): binds
      steps[0] via the catalog, UnknownProperty fail-closed naming
      collection and property, id pseudo-property passthrough, legacy
      collection-qualifier normalization, idempotent on resolved input,
      rename follows the property id (tests/tests/resolution.rs).
      DELIBERATELY UNWIRED from the query paths: fail-closed resolution
      flips on with the client registration lifecycle + protocol v2
      epoch (no interim state, rev 3). wait_catalog_ready deferral and
      TypeResolver absorption land with that flip.
- [x] Fetch/SubscribeQuery carry resolved Selections (resolution runs
      at the four origin sites with wait_catalog_ready deferral; sync
      sites resolve in their async continuation); receiver-side
      pass-through for unknown ids (until #274).
- [x] Engines consume Identifier.name for columns; assume_null /
      referenced_columns keyed consistently via Identifier.
- [ ] Unify missing-property semantics (filter error / reactor
      unwrap_or(false) / SQL assume_null) under the one rule.
- [x] Tests: resolve via schema, via catalog only, UnknownProperty on
      neither; subpath preservation; rename resolves to one property id
      (resolution.rs); end-to-end fail-closed fetch + overlay
      resolution (epoch_flip.rs).

## 7. Read-path rules (#175 fix)

- [x] Rule ladder: View getters keep compiled optionality
      (Property::absent_default keyed on the projected type, so
      Option<T> short-circuits ahead of the required default);
      predicate evaluation of resolved identifiers treats absent as
      NULL (IsNull matches, comparisons false), unifying the three
      historical behaviors. Predicate-level membership-sourced
      required-defaults are deliberately OUT of Phase A (plan decision
      15).
- [x] Cross-contract sibling gate (LWWBackend::get_checked) ->
      PropertyError::TypeSkew naming both ids, from the View getter AND
      filter evaluation; the lenient foreign-id-by-hint fallback is
      REMOVED per the cross-root ruling (2026-07-05: different roots
      are different systems; transplants fail visible); hints are
      engine-projection only (tests/tests/read_rules.rs).
- [x] Type defaults for required-absent scalars ("", 0, 0.0, false,
      empty binary, Json null); entityid/Ref and custom Property types
      keep Missing (no fabricable default).
- [x] Zero-op creation events; un-ignored
      tests/tests/yrs_backend.rs (test_sequential_text_operations);
      integration: create with empty string, reload, read back ""
      (landed earlier with the #175 commit; boxes consolidated here).

## 8. Derive macro, attributes, lifecycle glue

- [x] ModelSchema/FieldSchema emission per the normative mapping table
      (errata recorded in rfc.md section 4: Option<String> is
      LWW-backed in shipped code, and custom derive(Property) types map
      to (lww, string)); ephemeral fields excluded; descriptor test
      covers every table row; Model trait gains schema().
- [x] `#[property(anchor = "...")]` parsed and emitted into FieldSchema
      (executor-side anchor-reuse refusal already tested in group 3).
- [x] `#[model(id = "...")]` / `#[property(id = "...")]`: compile-time
      base64/16-byte validation; carried on the schema and into
      descriptors (registration-time verification already tested in
      group 3).
- [x] `_ankurah_` collection prefix -> derive-time compile error
      (trybuild fixtures; the runtime commit_local_trx guard stays as
      defense-in-depth).
- [x] registration_request(): ModelSchema -> RegisterSchema descriptor
      vectors; end-to-end test registers a derived model's schema and
      resolves it through the catalog map.
- [x] Registration triggers in context paths: trx.create/get::<M>
      auto-assert (best-effort; policy gates schema definition, not
      data writes); sync edit caches, and COMMIT closes the edit-only
      gap by ensure-registering touched unensured collections; read
      paths overlay the compiled schema without durable writes;
      strict ctx.register::<M>(); offline queue drains on durable-peer
      connect; hard_reset clears the latch and queue
      (tests/tests/registration_lifecycle.rs, 6 tests).

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
- Stored-form transform layer (maintainer direction, 2026-07-05):
  design a storage-boundary translation that lets state buffers be
  STORED with cheap engine-local property ids (offsets/u32s) and
  transformed to full property EntityIds at the wire/memory boundary,
  mirroring the injected encryptor/decryptor seam the future E2EE phase
  needs for values. This subsumes and removes Phase A's 0xA2
  display-name hints AND shrinks the stored 16-byte ids. Events are
  exempt by nature (hashed identity: full ids forever). Design it once
  with the E2EE transform seam in view.
