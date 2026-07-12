# RFC: Identity and Attestation Substrate

Status: ACCEPTED (rulings 2026-07-11; genesis shape refined to eager freeze
same date), implementation in same PR.
Scope: node identity, entity identity, attestation envelope, PolicyAgent
admission surface. This is RFC-1 of a pair; RFC-2 (multi-durable sync and
peer-to-peer reads) builds on it and is deliberately excluded here.
Related: specs/concurrency/threat-model.md (claims C4-01, C4-02, C4-06,
C4-15, C4-16, C4-20; gaps G-0, G-1, G-2, G-8), issue #274 (validated
ingress), issue #271 (history lifecycle), specs/concurrency/phase-2.md
(phase 3 parking items 3 and 7).

## Motivation

Ankurah's trust today is connection-scoped. A node id is a random ULID, the
`Presence` handshake is a single unsigned self-assertion including the
`durable` flag (proto/src/peering.rs), and authorization rides per-request
bearer tokens validated by `PolicyAgent::check_request`. This is coherent in
a star topology where the durable node is simultaneously the channel
counterparty and the authority on every event.

Two roadmap directions break that coincidence:

1. **Multiple durable nodes** (phase-2.md phase 3 parking item 3): durable
   node B must be able to accept durable node A's admissions without
   re-deriving them, which requires A's admission verdict to be a portable,
   verifiable artifact rather than a fact about A's channel.
2. **Peer-to-peer reads** (phase-2.md parking item 7, LONG_TERM_ASPIRATION):
   an ephemeral peer serving events to another ephemeral peer cannot vouch
   for them; the admission verdict must travel with the event and verify
   offline, after the admitting connection is gone.

Both need the same substrate: real cryptographic node identity, a signed
admission attestation whose verification is uniform across nodes and
PolicyAgent implementations, and entity identity that does not depend on any
single node's definitive storage. The threat model already names the holes
this fills: self-asserted peer class (C4-16 / G-8), opaque unverifiable
attestations (C4-20), the unvalidated mid-BFS fetch (C4-15 / G-1), and the
durable-only creation-uniqueness guard (C4-06).

## Design rulings (recorded 2026-07-11)

These were decided in design review and are not re-opened by this document:

- **R1. Signing scope.** Admission and state attestations are signed by
  **node identity**. Events themselves, including genesis events, are NOT
  signed; they remain content-addressed only. Author signatures (user-keyed
  or node-keyed authorship on events) are parked; see Non-goals.
- **R2. Key custody.** The core `Node` owns the keypair and performs all
  signing (presence, attestation envelopes). The PolicyAgent supplies only
  admission decisions and claims. Rationale: envelope verification must be
  agent-independent for cross-node interop; key handling stays out of every
  PolicyAgent implementation.
- **R3. Entity id scheme.** `EntityId` becomes the full 32-byte content hash
  of the entity's genesis event (EntityId = genesis EventId). No truncation,
  no ULID hybrid. Rationale: truncation converts creation uniqueness from a
  cryptographic invariant into an economic one (a 128-bit id has a 2^64
  birthday bound, mining-ASIC scale in 2026) and forces retention of
  twin-detection and eviction machinery. Survey grounding: git (SHA-1 to
  SHA-256 migration after real collisions), Matrix room v3 (federation
  forced content-hash event ids), IPFS, Nostr, Scuttlebutt, Hypercore, Tor
  v3 (v2's 80-bit truncated ids deprecated ecosystem-wide) all converged on
  32 bytes.
- **R4. Genesis preimage contents.** The genesis binds (system id, nonce,
  timestamp, initial operations) and carries **no creator field and no
  collection**. Creator: an
  unauthenticated creator claim is forgeable and a signed one requires the
  parked author-signature machinery; attribution instead lives in the
  admitting node's attestation claims, where it is already authenticated by
  the admission channel and signed by the attester. Collection: collection
  is being excised from identity (entities will exist independent of
  collection, which becomes a storage-organizational concept; the existing
  code comment on `EventId::from_parts` in proto/src/data.rs already records
  this direction). Timestamp: creator-supplied, advisory (same trust level
  as ULID timestamps today); adds entropy and preserves a creation-time
  signal for storage locality.
- **R5. Interop assumption.** Attestation *claims* interop assumes a system
  is homogeneous in its PolicyAgent (all nodes of a deployment run the same
  agent and share the claims vocabulary). The *envelope* (who attested what,
  verifiably) is agent-independent and is alone sufficient for the core
  admission-trust decision; claims are extension space, not load-bearing for
  it.
- **R6. Creation cost and id availability.** Entity creation adds no extra
  event: the genesis IS the single creation event and carries the entity's
  initial operations. The entity id remains available at `create()` return;
  genesis content freezes when `create()` returns (eager freeze). The
  operation-free "birth certificate" genesis was rejected for its per-entity
  event overhead; rejected shapes are recorded in II.2.1.

## Part I: Node identity

### I.1 Keys and NodeId

Every node holds an ed25519 keypair. The public (verifying) key IS the node
identity:

```rust
// proto
pub struct NodeId([u8; 32]);      // ed25519 verifying key bytes
pub struct Signature([u8; 64]);   // ed25519 signature
```

`NodeId` replaces the current use of `EntityId` for node identity everywhere
(Presence, register_peer and the peer maps, `PeerSender::recipient_node_id`,
`NodeMessage::UnsubscribeQuery { from }`, subscription bookkeeping). This
completes the node-identity third of the id-type split flagged by the TODO
in proto/src/id.rs; request/query correlation ids move to a dedicated
ULID-backed `RequestId`/`QueryId` (mechanical), and `EntityId` is left
meaning only entities (Part II).

Key custody per R2: `Node` construction accepts an optional keypair;
if absent, a fresh one is generated (appropriate for ephemeral nodes, whose
identity may be per-session). Durable deployments pass a persisted key; key
storage is the embedder's concern and a filesystem helper is provided.
Key rotation is out of scope (Non-goals); a rotated durable key is a
membership change, which is RFC-2 territory.

Alignment with the iroh connector: an iroh `EndpointId` is an ed25519
verifying key. When the iroh transport is used, the ankurah `NodeId` and the
iroh endpoint identity are the same key, so the QUIC handshake itself proves
possession. Other transports (websocket) rely on the signed presence below
plus transport security.

### I.2 Signed presence

`Presence` becomes a signed claim:

```rust
pub struct Presence {
    pub node_id: NodeId,
    pub durable: bool,
    pub system_root: Option<Attested<EntityState>>,
    pub timestamp: u64,          // unix ms, freshness signal
    pub signature: Signature,    // by node_id's key, over PRESENCE_TAG || bincode(claims)
    pub protocol_version: u32,
}
```

The signature covers a `PresenceClaims` projection (node_id, durable, the
system root entity id if present, timestamp, protocol version) under the domain tag
`b"ankurah.presence.v0"`. `register_peer` (core/src/node.rs) verifies the
signature before inserting the peer; an invalid signature rejects the
connection. This makes node identity unforgeable at the handshake
(closes the identity half of C4-16 / G-8).

The `durable` flag is verified, not trusted: a peer enters `durable_peers`
only if its NodeId is a recognized durable of the system. In this RFC's
scope, recognition means: the NodeId recorded as the founding durable in the
system root entity (I.3). Additional durables require the membership
mechanism, which is RFC-2. Until then a deployment has exactly one
recognized durable, which matches the current effective topology.

### I.3 System root binding and the join boundary (TOFU)

The system root entity gains a property recording the founder's NodeId,
written at system creation by the founding durable node. Ephemeral nodes
that have joined a system verify durable claims and attestation attesters
against it.

Honest boundary, stated explicitly: an ephemeral node's FIRST join is
trust-on-first-use. It learns the system root from the presence of the peer
it first connects to; it has no prior anchor to verify that root against.
This is exactly today's `join_system` trust posture, now named. After join,
the root is pinned and all subsequent durable claims and attester
recognitions verify against it. Out-of-band root pinning (configuring the
expected system id, which is now a hash and therefore pinnable) is available
to embedders that want to close the first-contact window; deeper mechanisms
are out of scope.

## Part II: Self-certifying entity identity

### II.1 EntityId = genesis EventId

`EntityId` becomes `[u8; 32]`, defined as the EventId of the entity's
genesis event. There is no separate allocation step and no randomness in the
id beyond what the genesis preimage carries.

The four bootstrap model ids for the system and catalog collections are the
single explicit exception. Those models cannot be ordinary catalog entities
without a self-description cycle, so their ids are domain-hashed virtual
model identities under `ankurah.well-known-model.v0`. They have the same
256-bit collision bound as entity ids, are reserved from user creation, and
exist only for bootstrap routing. Introducing a separate `ModelId` sum type
would make this distinction structural, but is deferred because it would
widen this RFC without changing the trust result.

### II.2 Event body split

`Event` currently carries `operations` for all events and signals creation
by an empty parent clock, with a standing TODO in proto/src/data.rs
("figure out how we actually want to signify entity creation"). This RFC
resolves that TODO:

```rust
pub struct Event {
    pub collection: CollectionId,   // envelope attribution, NOT identity (C4-02)
    pub entity_id: EntityId,        // for genesis: equals the derived id (verified)
    pub parent: Clock,              // empty iff genesis
    pub body: EventBody,
}

pub enum EventBody {
    Genesis {
        system: Option<EntityId>,   // None ONLY for the system root entity itself
        nonce: [u8; 16],            // creator-random
        timestamp: u64,             // unix ms, advisory
        operations: OperationSet,   // the entity's initial property values
    },
    Update {
        operations: OperationSet,
    },
}
```

The genesis is the single creation event and carries the entity's initial
operations, frozen when `create()` returns (eager freeze, R6).
Mechanically: `trx.create(&model)` already applies the model's initial
values to the property backends inside the call (`initialize_new_entity`,
core/src/transaction.rs); the creation path extracts them right there via
the same `to_operations()` machinery the commit path uses, assembles the
genesis, derives the entity id from it, and constructs the entity under
that id. Consequences:

- One creation event per entity; today's event count is unchanged.
- The entity id is available at `create()` return, preserving the current
  API property and intra-transaction references to created entities.
- The model snapshot passed to `create()` IS the genesis. Mutating the
  created entity later in the same transaction produces an ordinary Update
  event, exactly as the same mutation would after commit.
- A create retry that reuses the nonce (and payload) is idempotent: same
  genesis, same id, deduplicated by content addressing (C4-05). Distinct
  create calls draw distinct nonces and are distinct entities even under
  identical payloads.
- `is_entity_create()` becomes a match on `EventBody::Genesis` instead of
  the parent-emptiness hack. The invariant "parent empty iff genesis" is
  enforced at validation.
- Create-path surgery: today the id is allocated before the values are
  applied; the order inverts (apply values, extract, derive id, construct
  the entity under the derived id). Localized to the create path.
- Post-create mutations within a transaction fold into AT MOST ONE Update
  event per entity per commit, exactly as today: `.set` calls accumulate in
  the property backends and are extracted once at commit. Eager freeze
  moves only the CREATION event's extraction point; per-set calls never
  mint events.
- Mutual references require the back-reference field to be representable
  as absent at creation (an Option, a default, or an edge entity): the
  genesis of the first-created entity truthfully lacks the ref, which
  arrives in its single update event. A model with two REQUIRED mutual ref
  fields has a struct-level chicken-and-egg problem under any design (the
  second id cannot exist when the first struct is built); under eager
  freeze a placeholder would be enshrined in the genesis forever, so
  required mutual refs are explicitly discouraged in favor of optional
  fields or edge entities.

### II.2.1 Rejected genesis shapes (recorded so they are not re-litigated)

1. **Operation-free genesis**: a "birth certificate" event binding only
   (system, nonce, timestamp), with initial values riding a first Update
   event in the same transaction. Same properties, simpler create-path
   mechanics, but one extra event per entity, one extra hop on every walk
   to root, and a permanent event-count change. Rejected 2026-07-11 (R6).
2. **Partial binding**: entity id = hash of (system, nonce, timestamp)
   only, with the initial operations riding UNBOUND in the same creation
   event. Rejected as unsound, in both variants. If the entity id doubles
   as the genesis event id, two creation events differing only in
   operations share one event id, breaking event content-addressing itself
   (C4-01: staging, the accumulator, and dedup all key on `event.id()`).
   If instead the entity id is split from a full-content genesis event id
   to preserve C4-01, twins become free: any authorized writer reuses its
   own triple with different operations and obtains two structurally valid
   genesis events claiming one entity id; creation uniqueness reverts to
   authority-enforced first-writer-wins, the multi-durable genesis race
   returns, and the coordination/eviction machinery ruling R3 was purchased
   to delete comes back. The entity id also stops naming its genesis,
   forfeiting the #271 simplification (II.4). General rule, recorded: the
   entity id must be a commitment to the FULL distinguishing content of the
   genesis; any creator-controlled field left outside the commitment is a
   free twin channel.
3. **Commit-time ids**: genesis operations frozen at commit, no id before
   commit (whether breaking `entity.id()` silently or making it fallible
   until first commit). Breaks the id-at-create API property (the id is
   allocated as the first act of `create()` today, and the transaction's
   entity tracking, the returned handle, and intra-transaction references
   all key on it), infects the derive-generated model API, bindings, and
   reactive keying with a fallible id, and forces deferred-reference
   machinery for same-transaction refs, under which mutually referencing
   entities created in one transaction make ids circularly dependent with
   no resolution order; eager freeze resolves the same cycle naturally
   because the back-reference rides an ordinary Update event.
4. **Accountable equivocation**: partial binding plus creator signatures,
   with twins detected and punished after the fact. Requires the author
   signature machinery parked by R1, and is reactive where R3's scheme is
   structural: divergence has already happened by the time twins meet.

### II.3 Identity derivation (domain-tagged)

```
genesis id = SHA-256( "ankurah.genesis.v0" || bincode(system, nonce, timestamp, operations) )
update  id = SHA-256( "ankurah.event.v0"   || bincode(entity_id, operations, parent) )
entity  id = genesis id
```

Notes:
- Domain tags separate the two preimage shapes and version the scheme.
- The genesis preimage excludes `entity_id` (it is the output), excludes
  `collection` (R4; consistent with C4-02, whose exclusion this RFC
  re-dispositions from gap to by-design), and excludes `parent` (always
  empty for genesis; the tag plus body shape carries that fact).
- `system: None` versus `Some(id)` yields distinct preimages via the Option
  encoding, so the root genesis needs no second tag. Binding the system id
  into every non-root genesis gives the one-id-one-system invariant a
  cryptographic backstop: an entity id cannot be replayed into a foreign
  system, because verification recomputes the hash against the local
  system's id.
- Update events keep today's preimage fields (entity_id, operations,
  parent), gaining only the tag. The existing exclusion of collection is
  unchanged.

### II.4 What this buys (claims impact)

- **Creation uniqueness is structural on every node class.** A "different
  genesis for an existing id" is unrepresentable: a different genesis is a
  different id, hence a different entity. The C4-06 creation guard in
  core/src/entity.rs stops depending on `storage_is_definitive()` for
  finality; the check becomes: a genesis event is admissible for entity E
  iff `event.id() == E`. Ephemeral nodes get the same verdict with no
  durable round trip. C4-06's trust tier upgrades from "Byzantine-safe on
  durable / trusted-peer-plus-BFS on ephemeral" to Byzantine-safe
  everywhere.
- **The multi-durable genesis race dissolves.** Two durables can admit the
  same genesis (idempotent) but never different geneses for one id, with no
  coordination. The considered-and-discarded alternative was a home-durable
  genesis rule (rendezvous-hash each entity id to an owning durable that
  alone finalizes creation); it is unnecessary under R3 and is recorded here
  only so it is not re-invented.
- **Disjoint-lineage machinery is retained as defense in depth.** BFS
  comparison, grounded Disjoint verdicts (C4-08), and the creation guard's
  rejection path all remain: they still catch envelope lies (an event
  shipped under a mismatched entity_id envelope) and remain the mechanism
  for divergence handling generally. What changes is that the guard's
  definitive-storage dependency and its durable/ephemeral asymmetry go away.
- **#271 simplification.** The sealed-prefix checkpoint's "genesis
  attestation binding the seal to the creation event id" is partially
  discharged by construction: the entity id IS the genesis event id, so any
  artifact naming the entity id already binds the genesis. The seal still
  needs its own attestation for the folded state, but identity survival
  under pruning no longer needs a separate genesis-binding artifact.

### II.5 Migration surface (breaking, pre-1.0)

This is a breaking change to stored data and wire format. Posture: no
in-place migration tooling in this PR; pre-existing development deployments
reset. This matches the existing dev posture for root mismatch. The PR
description must state this prominently.

Mechanical surfaces (implementation checklist):
- proto: EntityId internals (Ulid to [u8;32]), serde impls (bincode fixed
  array; human-readable base64 lengthens from 22 to 43 chars), Display /
  short forms, TryFrom/FromStr, wasm and uniffi binding methods.
- ankql: `ast::Literal::EntityId(Ulid)` migrates to the 32-byte
  representation; predicate construction in core (`From<EntityId> for Expr`).
- Storage engines: key width for entity and event keys, index definitions,
  any code relying on ULID lexicographic time-ordering for locality (verify;
  the genesis timestamp is the replacement signal where an engine wants a
  creation-time index; sqlite reads stay strictly typed).
- `EntityId::new()` disappears for entities (ids are derived, not
  allocated). Test fixtures that need entities go through create paths or a
  test helper constructing genesis events.
- Node-identity call sites move to NodeId; request/query correlation ids
  move to RequestId/QueryId (ULID-backed, format unchanged).

## Part III: Attestation envelope and the PolicyAgent split

### III.1 Envelope

`proto::Attestation` stops being opaque bytes and becomes a signed,
structured envelope. `Attested<T>` and `AttestationSet` keep their shape;
their element type changes.

```rust
pub struct Attestation {
    pub attester: NodeId,
    pub body: AttestationBody,
    pub signature: Signature,   // over ATTEST_TAG || bincode(body)
}

pub enum AttestationBody {
    EventAdmitted  { event: EventId, model: EntityId, claims: Vec<u8> },
    StateAttested  {
        entity: EntityId,
        model: EntityId,
        head: Clock,
        state_digest: [u8; 32],
        claims: Vec<u8>,
    },
}
```

- The signed bytes are `b"ankurah.attestation.v0" || bincode(body)`.
  Verification is a pure function of (envelope, expected attester set):
  signature valid under `attester`, and `attester` recognized (I.2/I.3).
  No connection context involved; this is what makes the artifact portable
  (multi-durable acceptance, peer-served reads, storage and replay).
- `EventAdmitted` binds both the EventId and the model envelope used for
  admission. The EventId pins the identity-bearing content (C4-01); model is
  deliberately excluded from event identity because entities can be modeled
  independently, so binding it in the attestation prevents an admitted event
  from being relabeled under another model. Events themselves stay unsigned
  per R1.
- `StateAttested.state_digest` is SHA-256 over
  `b"ankurah.state.v0" || bincode(EntityState)`. Entity, model, and head are
  repeated in the typed body for routing and inspection; the digest binds the
  complete state buffers. Signing only `(entity, head)` would permit state
  substitution and is explicitly insufficient.
- `claims` is agent-defined and opaque to core (R5). The envelope alone
  ("recognized durable D admitted event E") is the load-bearing fact.
  Attribution of the submitting user context, policy epoch, and similar
  belong inside claims.

### III.2 PolicyAgent trait changes

The mechanical/semantic split (core does crypto, agent does policy):

1. **Admission (produce side).** `check_event` returns an admission decision
   instead of a raw attestation:

   ```rust
   pub enum Admission {
       Attest { claims: Vec<u8> },   // core mints + signs an EventAdmitted envelope
       Allow,                        // admit without attestation (current permissive behavior)
   }
   fn check_event(...) -> Result<Admission, AccessDenied>;
   ```

   Core wraps `Attest` into an envelope signed with the node key and
   attaches it to the event. `attest_state` gets the analogous treatment
   (`StateAttested`). PermissiveAgent returns `Allow`, preserving its
   current no-attestation behavior.

2. **Validation (consume side).** Core verifies envelopes BEFORE the agent
   hook runs: for each attestation on an incoming event/state, check the body
   variant and subject match (event id plus model, or complete state digest),
   signature, and attester recognition. Invalid or transplanted envelopes are
   stripped (and counted, for observability) rather than passed through. The agent hooks
   (`validate_received_event`, `validate_received_state`) then receive the
   payload with its VERIFIED attestation set and decide sufficiency:
   PermissiveAgent accepts anything (including zero attestations); a strict
   agent can require a recognized-durable admission envelope. The agent
   never re-implements signature verification.

3. **Unchanged.** `check_request`/`sign_request` (connection-scoped request
   auth, e.g. JWT bearer), the read/write/collection gates, and
   `validate_causal_assertion` (still zero call sites; G-9 status
   unchanged) are untouched by this RFC.

jwt-auth conformance: `JwtAgent` continues to return `Allow`-equivalent
admissions initially (its cross-node event validation is documented as
future work in specs/jwt-auth/spec.md); this RFC gives it the seam to
upgrade without trait churn later.

### III.3 Validated ingress interaction (#274) and the G-1 partial closure

Issue #274 owns the single-ingress `ValidatedEvent` seam and lists "new
attestation formats" as a non-goal; this RFC amends that boundary by
supplying the envelope that #274's seam will verify. Division of labor:

- **This RFC:** the envelope format, core verification functions, the
  PolicyAgent split, and a minimal closure of the sharpest gap instance:
  `CachedEventGetter::get_event` (core/src/retrieval.rs, the mid-BFS remote
  fetch, C4-15 / G-1 / #244) gains (a) response filtering to the requested
  ids with recomputed content hashes, and (b) the same
  `validate_received_event` gate the application arms already run, before
  any `add_event`.
- **#274 (unchanged mandate):** the structural refactor making every arm
  feed one seam producing `ValidatedEvent`, size limits (#246/#247, G-3),
  rate limiting (274-C, G-4), and clock-validation-as-ingress-check (V5
  residual from C4-03).

The G-1 closure here is deliberately the minimal correct patch at the
existing seam, not a preemption of #274's structure.

## Threat-model re-dispositions (edited in this PR)

specs/concurrency/threat-model.md is updated as follows; where this list
conflicts with the current text, this list governs:

- **C4-01** (content-hash identity): preimage text updated for domain tags
  and the EventBody split; the claim itself is unchanged and strengthened
  (genesis preimages are now also domain-separated).
- **C4-02 / G-2** (collection excluded from identity): re-dispositioned from
  open gap to BY DESIGN, citing the entity-collection decoupling direction
  (collection becomes storage-organizational; model binding recorded
  eventfully later). The residual envelope-attribution concern (policy must
  key on envelope-supplied collection consciously) remains noted and stays
  with #274's ingress checks.
- **C4-06** (creation uniqueness): trust tier upgraded to Byzantine-safe on
  all node classes; enforcing seam becomes the structural
  `event.id() == entity_id` genesis check; the definitive-storage dependency
  and the durable/ephemeral asymmetry text are retired. The
  multi-durable genesis race is recorded as dissolved.
- **C4-15 / G-1** (unvalidated BFS fetch): status moves from open gap to
  PARTIAL: id-filtering and policy validation land at the seam; the
  structural single-ingress conversion remains with #274. The red-ignored
  test arm `bfs_fetched_events_are_policy_validated` un-ignores.
- **C4-16 / G-8** (self-asserted peer class): status moves to enforced for
  identity (signed presence) and PARTIAL for the durable flag (verified
  against the system-root founder; multi-durable membership is RFC-2). The
  TOFU join boundary is documented as the honest residual.
- **C4-20** (authorship/authorization not structural): updated to
  distinguish the now-structural attestation envelope (verification is core,
  agent-independent) from authorship, which remains parked per R1. The
  "Attestation is opaque bytes" enforcing-seam text is retired.
- **G-0** (PermissiveAgent nil): wording updated: envelope verification is
  now agent-independent, so forged attestations are stripped even under
  PermissiveAgent; admission sufficiency under PermissiveAgent remains
  permissive by design.
- **Section 4 attestation map**: rows updated for the envelope and the new
  produce/consume split; the C4-15 row's "no attestation is consulted at
  all" text is retired.

## Non-goals (parked, with owners)

- **Author signatures on events** (user-keyed or node-keyed authorship,
  offline peer-to-peer WRITES, durable-forgery resistance,
  non-repudiation): parked. The envelope's `claims` field carries
  attested-attribution in the meantime. Owner: future RFC when offline
  writes or non-repudiation are pulled forward.
- **Multi-durable membership changes, durable-to-durable sync, fan-out and
  ack semantics, peer-to-peer read serving**: RFC-2.
- **Revocation epochs / policy-change fencing** (admission verdicts under
  racing policy changes; "revoke now" semantics): RFC-2, flagged there as
  the genuinely hard residual.
- **Key rotation**: a durable key rotation is a membership change (RFC-2).
- **Presence channel binding** (replay hardening of signed presence within
  a MITM'd unauthenticated transport): noted; websocket deployments rely on
  TLS, iroh deployments get possession proof from the handshake. Revisit if
  a transport without either appears.
- **#274's structural seam, size limits, rate limits**: unchanged mandate,
  not absorbed here.

## Implementation plan (this PR)

1. proto: NodeId/Signature/RequestId types, EntityId re-typing, EventBody
   split, domain-tagged derivation, payload-binding Attestation envelope,
   signed Presence including protocol version.
   ed25519-dalek v2 (pure Rust, wasm-compatible), rand for nonces; sha2
   already in tree.
2. core: Node keypair custody, presence sign/verify at register_peer,
   durable recognition against the system-root founder record, Admission
   enum + envelope minting at commit paths, core-side envelope verification
   ahead of the agent hooks, entity-id derivation in the create path
   (eager freeze: extraction at create() return, construction-order
   inversion), creation-guard simplification, G-1 seam patch.
3. Sweep: ankql literal, storage engines, wasm/uniffi bindings, examples,
   tests (fixtures move to create-path helpers).
4. specs/concurrency/threat-model.md re-dispositions per the list above;
   un-ignore the C4-15 arm; adversarial arms for: forged presence signature,
   unrecognized-durable claim, forged attestation envelope, genesis id
   mismatch, cross-system genesis replay.
5. Gates: fmt/taplo/clippy, full workspace tests, then PR (plain language,
   no internal shorthand; scope and migration posture stated).
