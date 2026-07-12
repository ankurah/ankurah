# Tasks

Companion to plan.md (authority: rfc.md, ratified rev 3; AMENDED to rev 4
on 2026-07-06 -- the identity plane pivoted from derivation to
durable-allocated ids). Groups are in landing order; each checkbox is
intended to be a commit or a small commit train, red-to-green where a
test can pin the change.

REV 4 PIVOT NOTE: groups 2 and 3 below record the rev 3 build as it
happened; items marked [REMOVED rev 4] were subsequently DELETED by the
pivot (schema_id derivation, the frozen genesis encoder,
self-certification, the anchor apparatus, the offline queue). Group 12
records the pivot work itself.

## 1. Phase 0: #294 protocol negotiation (folded into PR #307)

DONE in the metadata epoch. The standalone PR #306 did not merge; its
handshake work was folded into #307 and the final released epoch is protocol
v3.

- [x] `protocol_version: u32` appended as the last field of `Presence`;
      compatibility isolated in one equality check. The initial handshake
      step used version 1; the final folded metadata epoch releases as v3.
- [x] `Message::PresenceRejected { expected: u32, received: u32 }`
      variant.
- [x] Fallible `register_peer`: a decoded version mismatch refuses with
      `PresenceRejected` sent best-effort, then teardown via the existing
      deregister path; all four connectors (websocket-server,
      websocket-client, websocket-client-wasm, local-process) propagate the
      refusal and close instead of dropping the frame. An implied version-0
      handshake cannot decode as the current `Presence`, so it logs actionable
      guidance and closes without a rejection payload.
- [x] Presence decode failure before establishment closes the
      connection with an actionable log (version-0 peer guidance)
      instead of leaving it open.
- [x] Tests: same-version connect; decoded mismatch refused in both directions
      (rejection observed); hand-crafted version-0 Presence bytes (old-shape
      mirror enum) classified and closed without panic; existing integration
      suite green.
- [x] Close out #294: record refuse-semantics decision and the deferred
      policy-hook question on the issue.

## 2. Phase A foundation: derivation, frozen encoder, create-with-id

- [x] [REMOVED rev 4] `proto/src/schema_id.rs`: schema-specific id derivation
      + golden vectors + standalone scope. Deleted with the pivot; the
      executor now allocates by emitting an ordinary content-hashed genesis.
- [x] [REMOVED rev 4] ULID audit: superseded by 32-byte content-hash EntityIds.
- [x] [REMOVED rev 4] `core/src/schema/genesis.rs`: frozen genesis
      encoder + golden byte vectors. Deleted; creation events are
      ordinary and carry the full definition state.
- [x] [REMOVED rev 4] `validate_catalog_genesis` + tamper tests.
      Deleted; single-allocator authority replaces self-certification.
- [x] Create-with-known-id: RESOLVED without new plumbing. The
      executor rides `get_retrieve_or_create` +
      `commit_remote_transaction` (the receive path), which already
      materializes entities under given ids (rev 4: freshly allocated
      ones); the phantom guard only constrains client transactions,
      which registration does not use.

## 3. Catalog collections, protection, registration operation

- [x] Collection constants `_ankurah_model`, `_ankurah_property`,
      `_ankurah_model_property`; the central protected-collection classifier
      covers all four system collections.
- [x] `_ankurah_` prefix reservation in CollectionSet::get (user paths
      refused, system callers allowed); test.
- [x] Catalog entities use raw parsers in `core/src/schema/catalog.rs` and
      creation/follow-up writers in `core/src/schema/registration.rs` (never
      derive(Model)).
- [x] proto descriptors (ModelDescriptor, PropertyDescriptor,
      MembershipDescriptor) + `NodeRequestBody::RegisterSchema`.
- [x] Durable-side executor (REWORKED by rev 4, group 12): upsert by
      lookup key under the allocator mutex, content-hashed genesis on miss,
      rename-hint pre-pass, explicit-id verification, ordinary creation
      + difference-only follow-up events, check_schema_registration on
      the resolved plan, PolicyAgent::check_event on every event,
      persist + relay, synchronous map upsert, SchemaRegistered
      response.
- [x] Receiver-side protection: CommitTransaction events targeting
      protected collections refused outright (local and remote paths);
      tests.
- [x] Tests (REWORKED by rev 4): registration from descriptors alone on
      a schema-less server; upsert idempotence (same ids, zero events);
      renamed_from lineage moves + guard + stale-writer fork; castable
      retypes reuse one identity and return the canonical type; incompatible
      declarations fail with zero writes; explicit-id sharing with
      per-contract optionality; ephemeral refusal; policy-verb denial.
- [x] [REMOVED rev 4] Relay-side self-certification in
      NodeApplier::validate_and_stage and its
      tests/tests/catalog_genesis_relay.rs suite. Deleted; relayed
      catalog events are policy-trusted allocator output behind the
      structural write ban.
- [x] Multi-durable scope disposition: PR #307 requires registrations in one
      system to route to a single allocator. Allocator discovery/consensus for
      multi-durable deployments is explicitly deferred to #309; it is not an
      unfinished metadata-epoch task.
- [x] Policy-denial test (`schema_registration.rs`: resolved-plan denial
      proves zero writes).
- [x] Client lifecycle: DONE with group 8 (see group 8 trigger entry).

## 4. Catalog subscription and map

- [x] CatalogMap: id -> definition maps for all three kinds; collection
      -> model; model -> membership set; per-collection display-name
      index + global name index (was the sibling-gate feed; the gate is
      removed, decision 31 -- the index now backs the by-name allocator
      lookup); resolve/lookup API.
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
- [x] `StorageEngine::list_collections` overrides for PostgreSQL, SQLite,
      IndexedDB, and sled. Restarting durable nodes can eagerly discover
      catalog collections; allocator storage checks remain the identity
      backstop on any map miss.

## 5. LWW v2 / state 0xA2

- [x] Wire shapes: LWW_DIFF_VERSION_2 and LWW_STATE_VERSION_2 (0xA2)
      carry one tagged `PropertyKey::{Id, Name}` map; round-trip tests.
- [x] Every property backend uses the same PropertyKey lifecycle. Backends
      are PropertyKey-keyed stores with no schema binding, display-name hint,
      or per-instance wire mode.
- [x] Read fallback mechanics: 0xA1 and pre-0.9 buffers decode to Name
      keys; an absent resolved Id may read that legacy Name residue; the
      next save emits the current 0xA2 tagged form.
- [x] Integration flip: Node assembly stamps a live catalog resolver;
      commit resolves ordinary staged LWW Name keys to Id and canonicalizes
      both those and already-Id explicit bindings; Yrs resolves ordinary roots
      at edit time and uses literal ids for explicit bindings. Registered user
      writes are id-keyed, while catalog/system fields stay Name-keyed.
- [x] Unknown-id v2 payloads apply and persist opaquely (catalog lag);
      unprojectable until a binding knows the id.
- [x] Compatibility tests: unknown state versions are refused; legacy
      buffers decode; Name residue is preserved without substituting for an
      authoritative Id entry.
- [x] PROTOCOL_VERSION -> 3 landed, covering LWW v2/0xA2, resolved
      predicate Identifiers and ORDER BY property identities, RegisterSchema,
      and model-id wire envelopes.

## 6. ankql Identifier and resolution

- [x] `Identifier { property, name, subpath }` AST node (property is a
      raw 32-byte id; ankql cannot dep on proto); PathExpr stays the parse
      form; every Expr match site across ankql/core/storage handles
      Identifier with the dedicated resolved evaluator; assume_null keys on
      the resolved name for subpaths BY DESIGN (the first-vs-last-step fix).
      Legacy collection-qualifier normalization applies only before
      resolution; Identifier evaluation never reinterprets its name.
- [x] Resolution pass (CatalogManager::resolve_selection): binds
      steps[0] via the catalog, UnknownProperty fail-closed naming
      collection and property, id pseudo-property passthrough, legacy
      collection-qualifier normalization, idempotent on resolved input,
      rename follows the property id (tests/tests/resolution.rs). Wired at
      every fetch, LiveQuery, and relay origin with catalog-ready deferral;
      the resolved selection stored locally is the same selection forwarded
      remotely and later activated by the reactor.
- [x] ORDER BY items retain the resolved property id beside their display
      path; re-resolution refreshes a renamed display name without changing
      identity, so relay re-upsert and result-set rebuild keep the same sort
      property and canonical collation.
- [x] Fetch/SubscribeQuery carry resolved Selections (resolution runs
      at the four origin sites with wait_catalog_ready deferral; sync
      sites resolve in their async continuation); receiver-side
      pass-through for unknown ids (until #274).
- [x] Engines consume Identifier.name for columns; assume_null /
      referenced_columns keyed consistently via Identifier.
- [x] Missing-property semantics unified: resolution fails unknown names;
      resolved absence or uncastable payload is NULL in predicate/policy
      evaluation; typed View getters apply their own optional/default rules.
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
- [x] Read-time sibling and foreign-data gates, backend-specific checked
      getters, display-name hints, and `TypeSkew` are removed. Generic
      `property::read_resolved` performs Id-then-legacy-Name dispatch;
      typed getters report per-value `NonCastable`, while predicates map
      absent or uncastable values to NULL (tests/tests/read_rules.rs).
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
- [x] `#[property(renamed_from = "...")]` parsed and emitted into
      FieldSchema (rev 4; replaced the anchor attribute; executor-side
      hint semantics tested in group 3's reworked suite).
- [x] `#[model(id = "...")]` / `#[property(id = "...")]`: compile-time
      base64/32-byte validation; carried on the schema and into
      descriptors; property ids also flow through generated Model/View/Mutable
      accessors and ensured-schema predicate/ORDER BY aliases, with
      registration-time verification already tested in group 3.
- [x] `_ankurah_` collection prefix -> derive-time compile error
      (trybuild fixtures; the runtime commit_local_trx guard stays as
      defense-in-depth).
- [x] registration_request(): ModelSchema -> RegisterSchema descriptor
      vectors; end-to-end test registers a derived model's schema and
      resolves it through the catalog map.
- [x] Registration triggers in context paths: trx.create and mutating get
      ensure registration; sync edit records the exact schema on its
      transaction, and COMMIT closes the edit-only gap by ensure-registering
      only the shapes that transaction used. Predicate reads resolve through
      registration when necessary; direct id gets cache the binary-known
      schema and project whatever state already exists. Strict
      ctx.register::<M>(), the never-registered-offline create/commit error,
      hard_reset clearing the latch and map, and generation-fenced durable
      catalog resume after replacement-root readiness are covered
      (tests/tests/registration_lifecycle.rs and tests/tests/system.rs).

## 9. Cross-cutting and pre-PR

- [x] Error variants: `UnknownProperty`; per-value
      `PropertyError::NonCastable`; registration refusals including
      `RegistrationError::NonCastable` (including an incompatible explicit
      property binding), explicit-id absence, PolicyDenied, and NoDurablePeer.
      `TypeSkew`, CatalogGenesisError, and AnchorReuse are deleted.
- [ ] Nomenclature pass over new code/docs (#305: Model = contract
      definition entity, Collection = storage table).
- [x] Adversarial review (rev 3 scope: frozen encoder, v2/v1 fallback,
      registration convergence) completed pre-pivot; rev 4's identity
      plane gets its own external re-review post-push.
- [ ] Validation gate: cargo test -p ankurah-core (lib), ankurah-tests,
      ankurah-derive, ankql, jwt-auth; cargo check -p ankurah-core
      --features wasm; cargo fmt --all; taplo fmt if Cargo.toml changed.
- [x] Progress note on #289; PR when reviewable.

## 12. Rev 4 pivot: durable-allocated identity plane (2026-07-06)

DONE in one train on this branch (ratification trail on #289):

- [x] DELETE proto/src/schema_id.rs, core/src/schema/genesis.rs, the
      NodeApplier self-cert block, catalog_genesis_relay.rs, the anchor
      apparatus (attribute parsing, FieldSchema.anchor/.anchored,
      PropertyDescriptor.anchor/.anchored, AnchorReuse), and the offline
      pending_registrations queue + drain_pending.
- [x] proto: PropertyDescriptor gains renamed_from, target_model ->
      target_collection; PropertyRef::Anchor -> Name;
      NodeResponseBody::SchemaRegistered + Registered{Model,Property,
      Membership}.
- [x] Executor rework: upsert by lookup key under the allocator mutex;
      content-hashed genesis on miss; rename-hint pre-pass (guarded); ordinary
      full-state creation events; difference-only head-parented
      follow-ups; target_collection resolution (stub model on miss);
      synchronous map upsert before mutex release; RegisteredDefs
      return.
- [x] PolicyAgent::check_schema_registration (default-allow) +
      RegistrationPlan/PlannedMembership/PlannedUpdate; called on the
      resolved plan before any event is emitted.
- [x] Client lifecycle: ensure_registered consumes SchemaRegistered
      into the map and retains the exact returned binding;
      strict never-registered-offline error at create/commit (an unavailable
      reassert proceeds only for an exact, fully compatible bound shape);
      TContext::ensure_registered returns Result and commit_local_trx enforces
      transaction-scoped schema provenance.
- [x] derive: renamed_from attribute replaces anchor; explicit-id
      binding unchanged.
- [x] Tests reworked: upsert idempotence, rename-hint application +
      guard + stale-writer fork, canonical-type compatibility and refusal,
      policy-verb denial, strict offline, response-fed maps, fail-closed
      pre-registration reads; golden-vector and self-cert suites
      deleted.

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
  display-name hints AND replaces full 32-byte ids with compact local ids.
  Events are
  exempt by nature (hashed identity: full ids forever). Design it once
  with the E2EE transform seam in view.
- CatalogManager split (pre-PR architectural review, 2026-07-05): the
  manager bundles the passive catalog projection (map + warm/subscribe +
  readiness) with the registration-lifecycle coordinator (ensured latch,
  compiled-schema records, the allocator mutex). Split into CatalogMap +
  RegistrationCoordinator before Phase C piles the transform layer onto
  the same type. Internal only; no API/wire impact.
- Resolution pass decoupling (same review): resolve.rs bolts AST
  resolution onto CatalogManager<SE, PA> though it needs only
  name->id + readiness; extract a narrow NameResolver trait so the
  pass is unit-testable without a manager. Internal only.
- System-transaction refactor of the registration executor (maintainer
  nod, 2026-07-05; SIMPLIFIED by rev 4, tracked as #313): replace the
  hand-built creation/follow-up events in
  core/src/schema/registration.rs (creation/follow_up) with the
  ordinary Entity/Transaction machinery, via an internal
  system-transaction capability: a constructor reachable only by
  system code, permitted on the protected catalog collections,
  committing through the check_event-gated commit_remote_transaction
  pipeline. Rev 4 removed the hard part: there is no frozen genesis to
  inject (creation events are ordinary), so the whole executor can ride
  the live backend. Entity::generate_commit_event head-parents
  automatically, making the genesis-parenting bug class structurally
  impossible. Behavior-preserving refactor: the current executor is
  correct and test-pinned (upsert idempotence, chained renames,
  optional flips, hint guard), so this can land any time after Phase A.
- Cast-at-read retype evolution (maintainer direction, 2026-07-05;
  the coercion half of #303): a sanctioned retype should not rewrite
  data. value_type stays an identity input (collision safety, #85), so
  a retype still mints a NEW property id, but the new property entity
  records a lineage edge (e.g. `succeeds: old_property_id`) REQUIRING
  the old value_type be castable to the new one; reads that miss the
  new id follow the edge and CAST the old backend value at query time
  (the RFC 5.4 sibling gate, but with recorded permission: today's
  TypeSkew is exactly this situation without the edge).
  (SUPERSEDED 2026-07-10, decisions 30/31 and the #303 reframe: identity
  never forks on retype and TypeSkew is deleted; the future migration
  operates on the single property's CANONICAL value_type as a deliberate
  catalog operation, never a code-driven fork-with-edge.) Optional lazy
  rewrite-on-save migrates values opportunistically, like the v1->v2
  key migration. Design needs: normative cast rules (cross-node
  determinism; value/cast.rs is the seed), fallible-cast policy
  (string->i64 fails per-value: fail-visible), predicate pushdown
  (engines must cast or post-filter), and interaction with membership
  tombstones (#303's other half).
