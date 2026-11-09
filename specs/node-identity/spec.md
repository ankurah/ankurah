# Node identity, attestation, and the end of the system root

Status: design of record for the branch `feat/node-identity-attestation`, written on
2026-11-08 from Daniel's rulings of the same day. Base: origin/main at 03349a58. The
explainer page that records the reasoning, the verified facts and the retraction audit is
"Node Identity and the System Root" (kept at ~/ak/ankurah-explainers, outside the
repository); this file is the part an implementer builds from.

The branch is one pass with no line limit. It is reviewed whole, then re-cut into stacked
pull requests. Factor prudently: a concern gets a module named for its job, behavior sits
beside the state it governs, and nothing is written for the branch's own history.

## 1. Intentions

Every choice below is checked against all seven together.

1. **One graph, held in parts, owned by no one.** Nothing in the data may require two nodes to
   agree on a bounded dataset before they exchange an entity. The system root is such a
   requirement today and keeps independently created stores apart forever.
2. **An entity means the same thing everywhere it is held.** Its identity is its own creation
   content; copying, merging or relaying it changes nothing about it.
3. **A node proves who it is and is trusted for what it was granted, never for what it
   claims.** Every node has a key that others verify. Durable standing and the relay of
   commits follow from how a peer was reached and what policy allows, not from a flag.
4. **Facts about where data came from outlive the connection that carried them.** Who minted an
   entity, who committed an event, which durable node admitted it or served a state: signed
   by a cryptographic identity and checkable later. The minter sits inside the entity's id so
   no relay can drop or re-attribute it.
5. **Core knows nodes and nothing else about who.** Application, tenant, system, author and
   owner are components that applications declare or adopt from the Ankurah Commons. Core
   records a key and knows nothing of what a key denotes.
6. **Development stays convenient without shaping production.** A node in a client role, which
   the application defines, discards stale data after a development reseed because the
   application decides so from its own data; core offers only the reset primitive.
7. **The id derivation changes once.** Entity and event ids are recomputed under the new
   derivation; existing development stores are discarded. Migration tooling is not part of
   this branch.

## 2. The change in one view

- Node identity is an ed25519 key held by core. Presence is a signed answer to a
  per-connection challenge. Durable trust comes from provenance through a default method on
  the `PolicyAgent`.
- The minter of a genesis is a cryptographic `Identity`, a node's key by default. `system`
  and `author` leave both id preimages. `AuthorId` leaves the crate.
- The system root, its creation, adoption, handshake comparison and halt-on-mismatch leave
  core. `create()` needs nothing but the node's key. A public reset primitive remains.
- `Membership::Remove` is appended so an entity can leave a component.
- Attestations become structured, signed envelopes. Every node attests the events it
  commits; a durable node attests the states it serves and, when policy says so, the events
  it admits. Core verifies and strips envelopes before any policy hook sees them.
- Ankurah ships the Commons publisher's public key as the default trusted publisher, through a
  build-time included file and an overridable `PolicyAgent` method.

## 3. Identity and keys

### 3.1 Types (proto)

```rust
// proto/src/identity.rs
pub enum Identity { Ed25519([u8; 32]) }   // a verifying key; other schemes append at the end
pub enum Signature { Ed25519([u8; 64]) }
pub struct NodeId(Identity);              // the peer layer's name for an identity that is a node
```

- Verification is strict (`verify_strict`); a weak key and a non-canonical key encoding
  verify as false. Re-encode the decoded point and compare to refuse non-canonical bytes.
- Display and human-readable serde use unpadded URL-safe base64 of the 32 bytes, as
  `EntityId` does; binary serde writes the variant index then the bytes. The variant index is
  part of every genesis id, so variants append and never reorder; pin the encoded form of each
  variant by test, as `author.rs` did for `AuthorId`.
- `NodeId` derefs or converts to `Identity`; the peer maps, `Presence`, request and response
  `from` and `to`, updates, `UnsubscribeQuery.from`, `PeerSender::recipient_node_id` and the
  `PolicyAgent` hooks' `received_from_node` all take `NodeId`. `EntityId::random()` keeps only
  its test use; its TODO is updated.
- wasm and uniffi bindings expose `Identity` and `NodeId` as base64 strings where `EntityId`
  is exposed today.

### 3.2 Node key custody (core)

```rust
// core/src/node/key.rs
pub struct NodeKey(ed25519_dalek::SigningKey);
impl NodeKey {
    pub fn generate() -> Self;
    pub fn from_seed(seed: [u8; 32]) -> Self;
    pub fn to_seed(&self) -> [u8; 32];
    pub fn identity(&self) -> proto::Identity;
    pub fn node_id(&self) -> proto::NodeId;
    pub fn sign(&self, message: &[u8]) -> proto::Signature;
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_or_create(path) -> io::Result<Self>;   // exactly 32 seed bytes; staged file, exclusive create, 0600, link into place
}
```

- The node owns its key privately; nothing serializes or prints it; `Debug` prints the public
  identity. Policy agents and transports never see it.
- Construction takes an optional key. Absent, a fresh key is drawn. A durable node built
  without a key logs a notice that its identity will not survive a restart.
- Where the seed is stored is the embedder's choice; the helper writes and reads a seed file.
  Daniel's answer on this point (8 November) is taken as applying to the Commons publisher
  key in 3.3; if he meant node seeds, this section changes and nothing else does.

### 3.3 Trusted publishers

The Commons publisher's public key ships with Ankurah as the default trusted publisher, so a
node can later adopt the component and property definitions the Commons publishes.

- A file `core/src/policy/trusted_publishers.txt`, one base64 `Identity` per line, comments
  with `#`, included at build time with `include_str!` and parsed once. It ships empty until
  the Commons key exists; parsing failures are a build-time test failure, not a runtime one.
- `PolicyAgent::trusted_publishers(&self) -> Vec<proto::Identity>` returns the shipped set by
  default; an application overrides it with its own.
- Nothing in this branch consumes the set beyond a test that the default reads the file and an
  override replaces it. Adoption of published definitions is a later branch.

## 4. The handshake and peer trust

Port this from the `feat/node-identity` working tree (its diff and four new files are
exported beside the brief that dispatched this work), minus everything about the system root.

```rust
// proto/src/peering.rs
pub const PRESENCE_TAG: &[u8] = b"ankurah.presence.v1";
pub struct HandshakeChallenge { issuer: NodeId, nonce: [u8; 32] }
pub struct Presence {
    pub node_id: NodeId,
    pub durable: bool,
    pub challenge: HandshakeChallenge,   // the receiver's challenge this presence answers
    pub timestamp: u64,                  // advisory; nothing checks it
    pub signature: Signature,            // node_id's signature over PRESENCE_TAG || bincode(PresenceClaims)
    pub protocol_version: u32,
}
pub struct PresenceClaims { node_id, durable, challenge, timestamp, protocol_version }

// proto/src/message.rs
pub enum Message { Presence(Presence), PeerMessage(NodeMessage), PresenceRejected(PresenceRejection), Challenge(HandshakeChallenge) }
```

- Each side sends a fresh challenge first. `register_peer` consumes a core-issued, single-use
  challenge capability, checks the challenge's issuer and value, the signature and the
  declared sender, refuses a presence replayed from another connection and a reflected
  self-connection, then registers. The protocol-version check stays first.
- Connectors gain the challenge round trip: websocket client, wasm client, websocket server,
  local-process, and the test harnesses. The server's opening challenge send, its presence
  send, its rejection send and its pre-admission readiness wait all run under the handshake
  deadline. The wasm client dispatches no peer frame before the server is authenticated.
- Trust by provenance, Daniel's ruling of 5 October 2026:

```rust
pub enum PeerProvenance { Configured, Inbound }     // Introduced and Discovered arrive with the transports that produce them
pub struct PeerTrust { pub durable: bool }
// PolicyAgent, default method
fn peer_trust(&self, peer: &NodeId, provenance: PeerProvenance, claimed_durable: bool) -> PeerTrust {
    match provenance { Configured => PeerTrust { durable: claimed_durable }, Inbound => PeerTrust { durable: false } }
}
```

  Every `register_peer` call site states the provenance: dialing connectors say `Configured`,
  the server says `Inbound`, local-process says `Configured` for the dialing side and
  `Inbound` for the accepting side. The relay and durable routing read the trust, never the
  claim; an inbound `durable: true` is logged at info level and otherwise ignored. Every
  request to a durable-selected peer re-checks trust at send time with no await between
  lookup, check and send; registration and deregistration of one `NodeId` are serialized.
- Frames after the handshake stay unsigned; the docs say so plainly: an authenticated peer can
  spoof another peer's `from` on a request until a later cut.

## 5. Events and identity derivation

```rust
// proto/src/data.rs
pub const GENESIS_TAG: &[u8] = b"org.ankurah.genesis.v1";
pub const EVENT_TAG: &[u8]   = b"org.ankurah.event.v1";
pub enum EventBody {
    Genesis { minter: Identity, nonce: [u8; 32], timestamp: u64, operations: OperationSet },
    Update  { nonce: [u8; 32], timestamp: u64, operations: OperationSet },
}
genesis id = SHA-256( GENESIS_TAG || bincode(minter) || bincode(nonce) || bincode(timestamp) || bincode(operations) )
update id  = SHA-256( EVENT_TAG || bincode(entity_id) || bincode(nonce) || bincode(timestamp) || bincode(operations) || bincode(parent) )
entity id  = genesis id
impl Event {
    pub fn genesis(minter: Identity, operations: OperationSet) -> Self;
    pub fn update(entity_id: EntityId, parent: Clock, operations: OperationSet) -> Self;
    pub fn minter(&self) -> Option<Identity>;   // Some for a genesis
}
```

- `system` and `author` leave; `proto/src/author.rs` is deleted; the root's special case of
  `system: None` no longer exists because no root is minted. `validate_structure` keeps its
  two rules: parent empty if and only if genesis, and a genesis's id equals its entity id.
  The preimage tests are rewritten for v1: recipes against an independently serialized tuple,
  the two shapes never colliding, tag bytes pinned, the minter inside the id.
- `Transaction::create` mints under the node's identity (`Event::genesis(node.identity(),
  ..)`); `LocalTrxEntity::new` takes the minter instead of the system and the author. Nothing
  gates creation on readiness beyond catalog registration of the model. Minting under another
  identity the process holds is done with `Event::genesis` and `Attestation::sign` directly
  (a publishing tool); a `Context` opened under another identity is not part of this branch.
- The minter appears in the genesis only. An update's authorship rides the committing node's
  attestation (section 8).
- The simulation harness (`tests/src/sim`) seeds one deterministic `Identity` per simulated
  node from the simulation seed and mints under it; `sim_system_id` and its comment go.
- The LWW tie-break, generations, clocks and the comparison are untouched.

## 6. The system leaves core

What leaves, by symbol:

- proto: `sys::Item::SysRoot` (the item is JSON-encoded in a property, so the variant can go);
  `Presence.system_root`.
- core/src/system.rs: `root`, `root()`, `root_id()`, `create()`, `adopt_system()`,
  `store_root`, the root arm of `load_system_catalog`, `allow_system_replacement`,
  `set_allow_system_replacement`. What stays: the `_ankurah_system` items that are not the
  root, readiness from the catalog load, the halt state, and the wipe (renamed and made
  public, below). Keep the type where callers need it; renaming it is for the re-cut.
- core/src/error.rs: `NodeHaltReason::SystemReplacement`, `MutationError::SystemNotReady`,
  `RequestError::SystemNotReady`, `RegistrationError::SystemNotReady`; `ForeignEntity` and
  `SystemEpoch` stay (the epoch is per node and unrelated to the root).
- core/src/connector.rs: `PeerConnectionError::{SystemMismatch, MissingSystem, InvalidSystem,
  SystemReset}`.
- core/src/node.rs: the root comparison and adoption in `register_peer`; `system_root` in
  `presence()`; `set_allow_system_replacement`.
- core/src/context/inner.rs `system_id()`; core/src/transaction/local.rs reading it.
- README, GETTING_STARTED, examples and tests: `node.system.create().await` calls go; readiness
  waits use `Node::wait_ready` or the context constructors.

What is added:

- `Node::reset_local_storage(&self) -> Result<()>`: halts the node with
  `NodeHaltReason::LocalReset`, then wipes storage through `StorageEngine::delete_all`,
  finishing even if the caller drops the future (today's `hard_reset`, made public and freed
  from the replacement path). Issue #477's contract stands: the host discards the halted node
  and its contexts and live queries, and builds a fresh node on the wiped store.
- A book section (docs/internals) on node identity, peer trust and the reset primitive, and
  the note that resetting a client after a development reseed is the application's decision
  over its own data.
- specs/concurrency/threat-model.md: the self-asserted peer class gap closes (identity by
  signed presence, durable standing by provenance); the authorship claim is updated for
  committed attestations; the system-root rows are retired.

## 7. Membership removal

```rust
pub enum Membership { Add(ModelId), Remove(ModelId) }   // appended
```

- A genesis may carry `Add` only; a `Remove` in a genesis is inadmissible
  (`check_genesis_membership`'s sibling). An update may add or remove. Removing the last
  membership is allowed: the entity remains addressable by id, typed access reports
  `MissingComponent`, and `MemberOf` queries no longer match it.
- `LocalTrxEntity::remove_membership(model)` beside `add_membership`; state application
  removes the membership; every storage engine updates its membership index on removal; live
  queries observe the change as a predicate removal.
- Tests: add then remove in one transaction; remove in a later update; removal observed by a
  live query on each engine the suite covers.

## 8. Attestation

```rust
// proto/src/auth.rs
pub const ATTESTATION_TAG: &[u8]  = b"ankurah.attestation.v1";
pub const STATE_DIGEST_TAG: &[u8] = b"ankurah.state.v1";
pub struct Attestation { pub attester: Identity, pub body: AttestationBody, pub signature: Signature }
pub enum AttestationBody {
    Event { event: EventId, role: AttesterRole, claims: Vec<u8> },
    State { entity: EntityId, head: Clock, state_digest: [u8; 32], claims: Vec<u8> },
}
pub enum AttesterRole { Committed, Admitted }
// signature over ATTESTATION_TAG || bincode(body)
// state_digest = SHA-256( STATE_DIGEST_TAG || bincode(EntityState) )
impl Attestation {
    pub fn sign(key: &NodeKey, body: AttestationBody) -> Self;     // core-side constructor; proto exposes the verify half
    pub fn verify(&self) -> bool;                                    // signature under attester
    pub fn covers_event(&self, event: &Event) -> bool;               // subject match
    pub fn covers_state(&self, state: &EntityState) -> bool;
}
```

`Attested<T>` and `AttestationSet` keep their shape; `Attestation(Vec<u8>)` is gone.

Production:

- Every node attaches a `Committed` attestation, signed with its own key, to every event it
  commits, in the local commit path and in `commit_remote_transaction` for the events the
  committing node minted. The claims of a committed attestation are empty in this branch.
- A durable node attaches a `State` attestation, signed with its own key, to every state it
  serves (fetch responses, subscription deltas, direct gets). `PolicyAgent::attest_state`
  becomes `fn attest_state(&self, node, state) -> Option<Vec<u8>>`, returning the claims to
  carry; `None` carries empty claims. Ephemeral nodes attach no state attestation.
- `PolicyAgent::check_write_event` returns `Result<Admission, AccessDenied>` with
  `pub enum Admission { Attest { claims: Vec<u8> }, Allow }`. On `Attest`, core signs an
  `Admitted` attestation for the event. `PermissiveAgent` answers `Allow`; `JwtAgent` answers
  `Allow` in this branch.

Verification, at every ingress (the applier's event and state arms, `commit_remote_transaction`,
the peer event fetch in retrieval):

- For each attestation: signature valid under `attester`; subject matches the carried payload
  (the event's recomputed id, or the state's entity, head and digest). An attestation that
  fails either is stripped and counted under a tracing warning. The payload then reaches
  `validate_received_event` or `validate_received_state` with its verified set, and the agent
  decides sufficiency. `PermissiveAgent` accepts anything, including an empty set.
- Recognition of an attester as a trusted durable peer or a trusted publisher is the agent's
  decision inside those hooks, reading the node's peer trust and `trusted_publishers`.
- Storage keeps attestations beside events and states as today; every stored set is empty on
  main, so the encoding change carries no data.

## 9. Protocol version and stored formats

`PROTOCOL_VERSION` stays 1; the one bump happens at the 0.10.0 release. Every change above
breaks peers and stores from before the branch; existing development stores are discarded,
as with every pre-freeze change. No migration code is built here.

## 10. Out of scope

Signed post-handshake frames; key rotation; the durable membership protocol; adoption of
published definitions and anything else Commons-side; catalog convergence for components
registered independently; a `Context` opened under a non-node identity; migration tooling in
ak-cli; iroh. Each is named in the explainer page with its owner.

## 11. Acceptance

Beyond the suites staying green, these tests exist and state their scenario:

- Encoded forms of `Identity` and `Signature` variants pinned; a weak key and a non-canonical
  key refused; a forged presence signature, a presence replayed on a second connection and a
  reflected self-connection refused; an inbound peer claiming durable receives no relayed
  commit and is no fetch source while a dialed durable peer is both; a durable node keeps its
  `NodeId` across a restart with its seed and an ephemeral node gets a fresh one.
- The v1 preimages against independently serialized tuples; the two shapes never collide; the
  minter is inside the genesis id; a genesis under one minter and the same content under
  another are different entities; `create()` succeeds on a node that has never connected.
- No root: a durable node and an ephemeral node connect and exchange entities with no root
  anywhere; two independently created durable stores connect and exchange when configured to
  trust each other; `reset_local_storage` halts and wipes and a fresh node starts clean.
- Membership removal as in section 7.
- Attestations: a committed attestation on every locally committed event, verified by the
  receiving peer; a state attestation on every served state from a durable node; a tampered
  attestation and a transplanted attestation are stripped and counted; `Admission::Attest`
  yields an admitted attestation and `Allow` yields none; the trusted-publishers default reads
  the shipped file and an override replaces it.

## 12. Assumptions recorded for the morning review

- Daniel's "in a file that gets included at build time" (8 November, on the question of where
  a node's seed lives) is taken to mean the Commons publisher's public key (3.3). Node seeds
  keep the embedder-chosen file of the October cut (3.2).
- The shipped trusted-publishers file is empty until the Commons key exists.
- State attestations are unconditional on durable nodes; committed attestations are
  unconditional on every node; admitted attestations follow the agent's `Admission`.
