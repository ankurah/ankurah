# Node identity, event identity and attestation

Design of record for how a node is identified, how an entity and an event get their ids, how
two nodes open a connection and decide what to trust, how a node attests, and how behaviour
attaches to a node from outside core. `specs/protocol/bytes.md` is the authority for every
byte named here; where the two files differ on a byte, that file governs.

## 1. Intentions

Every choice below is checked against all eight together.

1. **One graph, held in parts, owned by no one.** Nothing in the data may require two nodes to
   agree on a bounded dataset before they exchange an entity. A root entity that every id
   hashes and every handshake compares is such a requirement: it keeps independently created
   stores apart for good. Core has none (section 9).
2. **An entity means the same thing everywhere it is held.** Its identity is its own creation
   content; copying, merging or relaying it changes nothing about it.
3. **A node proves who it is and is trusted for what it was granted, never for what it
   claims.** Every node has a key that others verify. Durable standing and the relay of
   commits follow from how a peer was reached and what policy allows, not from a flag.
4. **Facts about where data came from can outlive the connection that carried them.** Who
   committed an event, which durable node admitted it or served a state: an application that
   wants these has a node sign them with its key, and anyone can check them later. The minter
   sits inside the entity's id so no relay can drop or re-attribute it; whether that claim
   must be proven by a signature is the application's choice.
5. **Core knows nodes and nothing else about who.** Application, tenant, system, author and
   owner are components that applications declare or adopt from the Ankurah Commons. Core
   records a key and knows nothing of what a key denotes.
6. **Development stays convenient without shaping production.** A node in a client role, which
   the application defines, discards stale data after a development reseed because the
   application decides so from its own data. Core offers the reset primitive and the place to
   attach such a decision (a node plugin, section 10); it holds no application concept itself.
7. **Ids are derived one way from 0.10 on.** The derivation in section 6 is the one that
   freezes. Stores from before it are discarded; converting one is a tool's job outside core.
8. **A toolkit that enables sharing and does not insist on it.** Ankurah is what an
   application is built from. Core offers abilities; what an application adopts, attests,
   checks or shares with other applications is its own choice. The Ankurah Commons is what
   makes sharing possible, for the applications that want it.

## 2. What goes into the frozen protocol, and two rules

The wire and stored formats freeze at 0.10. After the freeze a 0.10 node is the older peer in
every compatibility question, so most of what follows is about what it can hold or refuse
cleanly without understanding it.

Something belongs in the protocol before the freeze when it lands in bytes that a 0.10 node
hashes, signs, stores or exchanges, and leaving it for later would force a second break.
Everything else waits (section 12). Every extension point that 0.10 leaves unused is written
to by a test.

Two rules steer the rest:

- **A node refuses an event holding an operation it cannot read**, whether the operation is of
  a kind the node does not know or its bytes were erased. It refuses with a typed error for
  that entity and does not store the event unapplied, because a stored but unapplied event
  would leave the node's state stale without anyone knowing. A sender withholds what a peer
  has not announced it understands. An attestation the node cannot read is different: it
  changes no state, so the node keeps and relays it untouched.
- **Bytes that an id or a signature covers are carried, stored and relayed exactly as their
  author produced them.** No engine, backend or relay re-encodes an operation's bytes or an
  attestation's bytes.

## 3. Identity and keys

### 3.1 Types (proto)

```rust
// proto/src/identity.rs
pub enum Identity { Ed25519([u8; 32]) }   // a verifying key; other schemes append at the end
pub enum Signature { Ed25519([u8; 64]) }
pub struct NodeId(Identity);              // the peer layer's name for an identity that is a node
```

- The variant index of an `Identity` is part of every genesis id, so variants append and never
  reorder. A test pins the encoded form of each variant.
- Verification is strict. `specs/protocol/bytes.md` states the exact acceptance rule (canonical
  encodings only, a small-order key or `R` refused, the equation without the cofactor) with
  vectors of signatures a reader must refuse.
- In text an identity or a signature names its scheme: `ed25519:` followed by unpadded
  URL-safe base64. `Display`, `FromStr`, human-readable serde and the wasm and uniffi surfaces
  all use that form.

### 3.2 The node's key (core)

For: a node signs its presence and its attestations with a key nobody else holds, and a
durable node is the same node after a restart.

- `NodeKey` (`core/src/node/identity/key.rs`) holds the signing key. The node owns it
  privately; nothing prints it, and `Debug` prints the public identity. Policy agents, plugins
  and transports never see it. What a holder of a `NodeKey` can do with it is build a node,
  read its identity, take its seed to keep it (`to_seed`), seal a statement (`NodeKey::attest`,
  section 7) and sign a presence (`sign_presence`).
- A node built without a key draws a fresh one. A durable node built without a key logs a
  notice that its identity will not survive a restart.
- Where the 32-byte seed is kept is the embedder's choice. `NodeKey::load_or_create(path)`
  reads or creates a seed file (owner-only permissions, created exclusively and linked into
  place) for embedders that want a file.

## 4. Opening a connection

For: each side learns which node it is talking to, on this connection and no other, and the
two find out cleanly which protocol version they share and what each understands.

```rust
// proto/src/message.rs
pub enum Message {
    Open(Opening),                       // 0: the first frame each side sends
    NoCommonVersion(ProtocolVersions),   // 1: the answer to an opening that shares no version
    Presence(Presence),                  // 2
    PeerMessage(NodeMessage),            // 3
    Refused(String),                     // 4: the accepting side will not register this peer
}

// proto/src/peering.rs
pub struct ProtocolVersions { pub lowest: u32, pub highest: u32 }
pub struct Opening { pub versions: ProtocolVersions, pub challenge: HandshakeChallenge }
pub struct HandshakeChallenge { issuer: NodeId, nonce: [u8; 32] }
pub struct Presence { pub claims: PresenceClaims, pub signature: Signature }
pub struct PresenceClaims {
    pub node_id: NodeId,
    pub durable: bool,
    pub challenge: HandshakeChallenge,        // the receiver's challenge this presence answers
    pub timestamp: u64,                       // advisory; nothing checks it
    pub negotiation: VersionNegotiation,
    pub features: Vec<u32>,
    pub announcements: BTreeMap<String, Vec<u8>>,
}
pub struct VersionNegotiation { pub offered: ProtocolVersions, pub peer_offered: ProtocolVersions, pub chosen: u32 }
```

- **The opening.** Each side's first frame is `Message::Open`. Its first twelve bytes are the
  variant index, then the lowest and the highest protocol version the sender speaks. Those
  twelve bytes keep their position and meaning in every later version, so any two builds can
  read each other's range. A fresh challenge follows them. The two sides use the highest
  version both speak; with none in common the receiver answers with its own range
  (`NoCommonVersion`) and closes. In 0.10 both ends of the range are 1.
- **The order.** The side that dialed sends its presence first, answering the challenge in the
  accepting side's opening. The accepting side registers the dialer, which authenticates it,
  and only then builds and sends its own presence; if it will not register the dialer it
  sends `Message::Refused` with a short reason and closes. The dialing side registers the
  accepting side on its presence, and only then reports itself connected. So an accepting
  node signs nothing, and runs no plugin's `announce`, for a dialer it has not authenticated,
  and nobody can ask a server for a presence to use elsewhere. Core keeps this order for
  every connector: a handshake is begun with how the peer was reached
  (`Node::begin_handshake`), `Inbound` being the accepting side; `Node::presence` refuses an
  accepting handshake whose dialer has not registered; and one handshake registers one peer,
  once.
- **What a receiver checks** (`core::connector::Handshake::authenticate`), each failure a
  typed `PeerConnectionError`: the presence answers the challenge this node issued on this
  connection; its signature verifies under the node id it names; the challenge in the peer's
  own opening was issued by that same node; the peer is not this node; and the claims repeat
  the range each side offered and the version chosen, so a party between the two cannot lower
  the choice unnoticed. A handshake that has registered its peer refuses another presence.
  The signature covers the claims as `specs/protocol/bytes.md` writes them out.
- **Features.** `features` holds numbers that core assigns to things a later build may add. A
  node ignores a number it does not know. 0.10 assigns none.
- **Announcements.** `announcements` holds bytes that node plugins contribute, each under its
  plugin's name (section 10). A node ignores a name it does not know. The keys are names and
  not definition ids: no definition exists to name yet, and a presence is never stored, so
  changing the key type later is an ordinary version step.
- **One deadline.** Everything a server does before it has answered the dialer runs inside
  one handshake deadline: reading the dialer's opening and presence, registering the dialer
  with every plugin's hook, and signing its own presence with every plugin's announcement. A
  connection not established by then closes, so neither a dialer that never presents nor a
  slow plugin holds it open. The local-process connector, which joins two nodes in one
  process, sets no deadline.
- **Closing without a frame.** An accepting node that has itself halted, or whose deadline
  passed, closes without `Refused`. A receiver closes a connection that sends a frame of a
  variant its chosen version does not define.

What the handshake does not give. A node that dials an address sends its presence before it
knows which node answers there. Whoever answers can pass that presence to one other accepting
node and be registered there under the dialer's id, and can sit between the two and alter
frames, because frames after the handshake are not signed. A dialer chooses its addresses and
the transport protects them; letting a dialer name the node it expects at an address needs
nothing on the wire and is later work (section 12). An agent that grants standing to a node
id on an inbound connection relies on that node dialing only addresses it trusts.

## 5. Peer trust and registrations

For: durable standing is granted by the node that relies on it, a connection speaks only for
the node that authenticated on it, and an old connection closing cannot disturb a newer one.

```rust
pub enum PeerProvenance { Configured, Inbound }
pub struct PeerTrust { pub durable: bool }
// PolicyAgent, default method
fn peer_trust(&self, peer: &NodeId, provenance: PeerProvenance, claimed_durable: bool) -> PeerTrust {
    match provenance { Configured => PeerTrust { durable: claimed_durable }, Inbound => PeerTrust { durable: false } }
}
```

- **Trust by provenance.** Every handshake states, when it begins, how the peer was reached,
  and the registration takes it from the handshake. A connector that dialed an address the
  embedder configured says `Configured`; a server says `Inbound`;
  the local-process connector says `Configured` for the dialing side and `Inbound` for the
  accepting side. The relay of commits and the choice of a durable peer to fetch from read the
  trust and never the claim. An inbound peer that claims to be durable is logged and otherwise
  treated as ephemeral. A verified presence proves which key the peer holds and nothing about
  what that key should be trusted for; that is the agent's decision.
- **A registration belongs to one connection.** `Node::register_peer` returns a
  `ConnectionRegistration`. Removing it removes that registration only, so a connection that
  closes late cannot remove a newer registration of the same node id. Connectors hold the
  registration, not a node id.
- **A frame is accepted only from the connection it claims.** A connector passes its
  registration with each frame, and the node refuses a frame whose `from` is not that
  registration's node. The refusal is logged and the connection stays up. This is why frames
  need no signature for a node to know which peer sent one; it does not let a third party
  check a frame later, which is what attestations are for (section 7).
- **A request runs on the connection it arrived on.** Its answer goes out on that connection,
  and a subscription it installs is held by that connection, also when the same node
  registers again on a newer connection while the request is being checked.
- Every request to a peer chosen as durable checks its trust again at send time, with no
  await between the lookup, the check and the send.

## 6. Events and their ids

For: an entity's id is its creation content and nothing else, a node can recompute an event's
id without being able to read its operations, an operation's content can later be erased
without changing any id, and no id depends on what a serializer crate happens to emit.

```rust
// proto/src/event.rs, proto/src/operation.rs
pub enum EventBody {
    Genesis { minter: Identity, nonce: [u8; 32], timestamp: u64, operations: CarriedOperations },
    Update  { nonce: [u8; 32], timestamp: u64, operations: CarriedOperations },
}
pub enum CarriedOperation { Bytes { salt: [u8; 16], bytes: Vec<u8> }, Erased([u8; 32]) }
pub enum UnreadableOperation { UnknownKind { indices: Vec<u32> }, Malformed(String), Erased }

// operation digest = SHA-256( tag(operation) || salt || bytes )   for Bytes
//                  = the 32 bytes carried                         for Erased
// genesis id = SHA-256( tag(genesis) || minter || nonce || timestamp || count || digest_1 .. digest_n )
// update id  = SHA-256( tag(update)  || entity_id || nonce || timestamp || count || digest_1 .. digest_n || parent )
// entity id  = genesis id
```

- **Domain tags.** One family, `ankurah.<name>.v1`, declared together in
  `proto/src/domain_tag.rs`: `genesis`, `update`, `operation`, `presence`, `attestation`,
  `state`. A tag enters a hashed or signed string as one length byte followed by its bytes.
  Tests pin each tag's bytes and that no tag's encoded form is a prefix of another's. A new
  derivation is a new tag, never a new meaning for an existing one.
- **The minter.** A genesis names its minter as an `Identity`: the creating node's key by
  default, a publisher's key for a published definition. The minter is inside the id, so the
  same content under another minter is another entity. An update carries no minter. Core does
  not check that a genesis carries a signature by its minter; whether the claim must be proven
  is the application's choice, made with attestations.
- **Carried operations.** An event carries each operation as the bytes its author produced, or
  as only their digest once the bytes have been erased. Building an event encodes each
  operation once. Reading one yields a typed `UnreadableOperation` for bytes of a kind this
  build does not know, for bytes that do not parse, and for an erased operation; a node that
  meets any of the three while taking in an event refuses that event (section 2). Nothing in
  0.10 erases. A test builds an event with an erased operation and shows its id unchanged.
  Each operation is carried with 16 random bytes of salt, which its digest covers and which are
  erased with its bytes. Without the salt, anyone holding the digest of an erased short value, a
  flag or a name from a list, could confirm a guess by hashing it.
- **The parent clock** of an update is inside its id, so a `Clock` decodes strictly from the
  wire and from a store: entries out of order, repeated, or with a zero generation are refused
  with a typed error.
- **Chosen parts.** `Event::genesis` and `Event::update` draw a nonce, read the clock for the
  timestamp and draw each operation's salt. `Event::genesis_from_parts` and
  `Event::update_from_parts` take every part from the caller, the salts included: they take the
  operations already carried, each under the salt the caller chose. They are for a migration
  tool or a publisher that must arrive at the same id twice.
- **The encoding.** Every hashed or signed string, and the encoding of each type inside it, is
  written out byte by byte in `specs/protocol/bytes.md`, with literal hex vectors. The encoding
  is what bincode 1.3.3's default produces (fixed-width little-endian integers, four-byte
  variant indices, eight-byte lengths); the document is the authority and the crate, pinned to
  exactly that version, is one implementation of it. `proto/src/encoding.rs` is the only place
  in proto that calls the serializer. Tests compare each string against hex whose expected
  side never calls a serializer.
- **Variant order.** Tests pin the index of every variant of every enum on the wire or in a
  hashed string (`Message`, `NodeMessage`, the request, response and update bodies, `Value`,
  `EventBody`, `Operation`, `Membership`, `Identity`, `Signature`). `Membership` has `Add`
  alone; index 1 is left free and a test shows that an operation using it is refused as
  unreadable.

## 7. Attestation

For: a node can sign a statement about an event or a state when its application asks, and
anyone holding the statement can check it later, on any node, without the connection that
carried it. Core makes no statement on its own and checks none. An application chooses both
through its policy agent, and later through a plugin most applications would share.

```rust
// proto/src/attestation.rs
/// One attestation as it travels and is stored. A node keeps and relays these bytes whether
/// or not it can read them.
pub struct SealedAttestation(pub Vec<u8>);
pub struct AttestationSet(pub Vec<SealedAttestation>);

/// What a sealed attestation holds: the statement's exact bytes and a signature over them.
struct Sealed { statement: Vec<u8>, signature: Signature }
/// The statement, read from those bytes.
pub struct Statement { pub attester: Identity, pub subject: Subject, pub kind: EntityId, pub payload: Vec<u8> }
pub enum Subject { Event(EventId), State { entity: EntityId, head: Clock, state_digest: [u8; 32] } }
// signature over: tag(attestation) || statement bytes, exactly as carried
```

- **The kind** is the entity id of the definition that says what the statement means and how
  to read `payload`. Core defines no kinds, because core makes no statements. A type is
  admitted by whoever consumes it: an application or a plugin that reads a kind is what gives
  it meaning on that node.
- **What the signature covers.** The attester, the subject, the kind and the payload are all
  inside the signed bytes. A signature is checked against the carried bytes, never against a
  re-encoding.
- **Making one.** `PolicyAgent::check_write_event` returns
  `Result<Option<AttestationRequest>, AccessDenied>`, and the agent's hook for a served state
  (`request_state_attestation`) returns `Option<AttestationRequest>`, where
  `AttestationRequest { kind: EntityId, payload: Vec<u8> }`. When an agent returns one, the
  node that ran the hook signs a statement about that event or state with its own key and
  attaches it. Core attaches nothing otherwise. An event that core commits on its own
  authority never reaches the agent's hook: the catalog definitions that registration writes
  are such events, so they carry no attestation, and a definition is tied to its minter only
  through its id.
- **Reading one.** Core does not verify or strip on arrival. `validate_received_event` and
  `validate_received_state` receive the set as it came. An agent unseals the attestations it
  cares about (`SealedAttestation::unseal` yields the statement only if the bytes parse and
  the signature verifies under the attester) and checks that the subject names the event or
  state in hand (`Subject::names_event`, `Subject::names_state`).
- **Keeping them.** An event's attestations are stored with the event, as separate elements so
  one unreadable attestation never makes an event unreadable, and relayed with it. A state's
  attestations are not stored: a statement about a state names one head, so a durable node
  attaches its own agent's statement when it serves a state, and every engine stores a state
  without any. Keeping a received one would mean deciding when it stops naming the stored
  state.

## 8. Requests, responses and updates

For: a 0.10 node answers what it cannot read instead of going silent, so a later build can
add a request or an update without breaking a connection to an older peer.

- A request, a response and an update each carry their body as length-prefixed bytes
  (`proto::CarriedBody<T>`), so the frame, its ids and its addresses always decode.
- A request whose body this build cannot read is answered with
  `NodeResponseBody::Unsupported`; an update likewise in its acknowledgement
  (`NodeUpdateAckBody::Unsupported`).
- A requester that receives `Unsupported` gets `RequestError::UnsupportedByPeer`; a response
  body the requester cannot read fails that request with `RequestError::UnreadableResponse`.

## 9. The node's run state, and reset

Core has no system root, no built-in model for one, and no readiness that waits on one. A
node is ready to create an entity as soon as its model is registered in the catalog;
`Transaction::create` mints under the node's identity and needs nothing from any peer.

`NodeLifecycle` (`core/src/node/lifecycle.rs`) owns what is left: the node's run state, halt,
and reset. `Node` exposes it through its own methods (`state`, `check_ready`, `wait_ready`,
`reset`).

`Node::reset` is the reset primitive. Its contract:

- It halts the node with `NodeHaltReason::Reset`, then wipes storage through
  `StorageEngine::delete_all`.
- The wipe is started before anything is awaited, so a caller that stops waiting cannot leave
  a halted node with an unwiped store.
- Every storage commit is counted from when it begins until it returns or its caller drops
  it, and none begins once the node has halted. The wipe waits until none is counted, so no
  commit still awaited lands in a wiped store. A commit that an engine keeps running after
  its caller dropped it is the engine's to keep out of a store wiped since
  (`StorageTransaction::commit`).
- A call made while or after another reset waits for that one and does not wipe again. A
  failed wipe is retried by the next call, and a failure nobody is waiting for is logged.
- The host then discards the halted node with its contexts and live queries, and builds a
  fresh node on the wiped store.

When to reset is never core's decision. Section 10.1 is the one case that ships.

## 10. Node plugins

For: behaviour that only some applications want attaches to a node from outside core, several
to one node, and sees every peer registration before anything is exchanged with that peer.
The policy agent cannot carry such behaviour: a node has one agent, and the agent's subject is
access.

```rust
// core/src/node/peers/plugin.rs
#[async_trait]
pub trait NodePlugin<SE, PA>: Send + Sync + 'static {
    /// Names this plugin's entry in a presence.
    fn name(&self) -> &'static str;
    /// Bytes this node announces to every peer under `name`, inside its signed presence.
    async fn announce(&self, node: &Node<SE, PA>) -> Result<Option<Vec<u8>>, AnnouncementFailure> { Ok(None) }
    /// Asked once per registration, after the presence verifies and trust is decided and
    /// before the peer is registered. `Err` refuses the peer.
    async fn register_peer(&self, node: &Node<SE, PA>, peer: &PeerRegistration<'_>) -> Result<(), PeerRefusal> { Ok(()) }
}
pub struct PeerRefusal { pub plugin: &'static str, pub reason: String }
```

- **Attached at construction**, so a plugin sees every registration of its node:
  `Node::builder(engine, agent)` with `durable()`, `key(..)`, `plug(..)` and `build()`.
  `Node::new` and `Node::new_durable` are the shorthands for a node with no plugin. Two plugins
  under one name fail `build()` with a typed error.
- **Announcing.** `Node::presence` gathers each plugin's announcement, in the order plugged,
  under its name, inside the signed claims. A plugin that cannot make its announcement fails
  the presence with a typed `PeerConnectionError`; nothing is swallowed.
- **Deciding.** `Node::register_peer` asks each plugin, in the order plugged, after the
  handshake authenticates and `peer_trust` is decided, and before anything is inserted. The
  first refusal returns `PeerConnectionError::Refused(PeerRefusal)` and leaves nothing
  registered. No lock is held across a hook. A plugin may call `Node::reset` from the hook and
  then refuse. A hook can be dropped at any await: when its node halts or the connector shuts
  down, when the connection closes, or when an accepting connector's handshake deadline
  passes; the peer is then not registered.
- **A plugin decides from what the handshake carries.** No connector reads a peer's messages
  while `register_peer` is awaited, so a plugin cannot ask the peer anything from this hook;
  what it needs to know about the peer travels in the peer's announcement. A hook that
  converses with a peer would need a registered but unconfirmed peer state in the node and in
  every connector, and waits for a plugin that needs it.
- **Each hook on the trait has a consumer in the tree.** A further hook arrives with the plugin
  that needs it.

### 10.1 Reset after a reseed

For: during development a server's store is thrown away and seeded again, and a client still
holding the old seeding's data must not go on mixing it with the new.

`extensions/system-reset` is a plugin over a model `M` that the application designates as its
System model (any model of its own; the Commons will publish one later). The application's
server holds one `M` entity per seeding.

- The plugin announces the ids of the `M` entities its node holds.
- In `register_peer`, for a peer trusted as durable: when the peer announced some, the node
  holds some, and the two sets share none, the plugin refuses the peer, after resetting the
  node if the application chose `OnReseed::Reset` and without resetting under
  `OnReseed::Refuse`.
- Either side holding none is no verdict, so a first connection, a peer that is behind and a
  peer of the same seeding all register normally. An announcement the plugin cannot read is
  also no verdict: it accepts and logs.
- An ephemeral node fetches the announced entities it lacks once the peer is registered, so
  a first connection leaves it holding its System entity. A durable node reads only its own
  storage and fetches nothing.

## 11. Versions and stores

- `PROTOCOL_VERSION` is 1 and is the only version 0.10 offers or accepts.
- Each storage engine records a store format version of its own (`STORE_FORMAT_VERSION`, 1 in
  each), separate from the protocol version, and refuses a store of another format with
  `StoreFormatError`, which names both versions. That makes a version byte per stored record
  unnecessary for now.
- No engine converts a store. A store from before 0.10 is discarded.

## 12. Left for later, each with what keeps it possible

| Later work | What 0.10 fixes so it needs no break |
| --- | --- |
| Erasing an operation's content | The per-operation digest in every event id, and the salt inside it |
| New operation kinds | Carried operations and the refusal rule of section 2 |
| Membership removal | A free `Membership` index. Its design must settle the shape of stored state before the freeze |
| A signed tree root in place of a signature per served state | A new statement kind |
| Key rotation, delegation, threshold identities | New statement kinds, and `Identity` or `Signature` variants announced as features |
| Checking that a published definition comes from its publisher: a list of trusted publishers, and a publisher's statement about what it minted | The minter's identity inside every creation id (section 6) and the attestation of section 7. Nothing in a stored or exchanged byte changes |
| Signed frames after the handshake; new requests | Features and carried bodies |
| Replacing the serializer crate | The byte specification |
| Merging an attestation that arrives after its event was stored | Attestations stored as separate elements |
| A plugin hook that converses with a peer | Nothing on the wire; a node and connector change |
| Naming the node a dialer expects at an address | Nothing on the wire; a connector option |

## 13. Out of scope

The durable membership protocol; adopting a definition minted elsewhere by its id, and the
rest of the Commons; catalog convergence for models registered independently; a `Context`
opened under an identity other than the node's; store conversion and migration tooling; a
shipped attester and verifier plugin.

## 14. Required tests

Beyond the suites staying green, these exist and each states its scenario:

- **Identity.** The encoded and text forms of each `Identity` and `Signature` variant; the
  refusal vectors of `specs/protocol/bytes.md`; a second `Signature` variant refused as
  unreadable; a durable node keeps its `NodeId` across a restart with its seed and a node
  without one gets a fresh id.
- **Opening a connection.** An opening offering a wider range settles on 1; one sharing no
  version is answered with `NoCommonVersion`, and its error names both ranges; a forged
  presence signature, a presence replayed on a second connection, a reflected self-connection,
  a relayed challenge and an altered offer are each refused with their own error; a stranger
  that presents another node's challenge to an accepting node receives no presence from it; an
  accepting node presents only once it has registered the dialer, and a second registration on
  one handshake is refused; a registration not decided within the handshake deadline closes the
  connection; unknown feature numbers and announcement names are ignored, and a frame of an
  unknown kind closes the connection; a refused dialer reports the refusal and its reason,
  never reports itself connected, and keeps backing off.
- **Trust and registrations.** An inbound peer claiming durable receives no relayed commit and
  is no fetch source, while a dialed durable peer is both; an old connection's registration,
  removed late, leaves a newer one in place; a frame whose `from` is another node is refused
  and the connection stays up; a subscription whose peer registers again while its request is
  checked stands on the connection it arrived on.
- **Events.** Each hashed string against literal hex; the tags' bytes and that none prefixes
  another; the minter inside the genesis id; the same parts give the same id; two carryings of
  one operation have different digests and one salt gives one digest; an operation's digest is
  not the hash of its tag and bytes alone; an erased operation leaves the id unchanged and makes
  the event refused on arrival; an operation of an unknown kind is refused with its typed error
  and nothing is stored; a clock out of order is refused at decode.
- **Attestation.** An agent that requests one gets a statement signed by the node, on an event
  and on a served state, and an agent that requests none gets none; an attestation of a kind
  nobody knows, and one whose bytes do not parse, are stored and relayed unchanged by a node
  between two others; a statement moved to another event fails its subject check; a changed
  payload fails its signature; every engine returns an event's attestations as stored and
  stores a state without any.
- **Requests.** A request with an unknown body kind receives `Unsupported`, and its requester
  gets the typed error.
- **Reset.** `Node::reset` halts and wipes, and a fresh node starts clean; a commit in flight
  when a reset starts does not write into the wiped store; a second reset waits for the first
  and does not wipe again; a reset whose caller stopped waiting still wipes; a failed wipe is
  retried.
- **No root.** A durable node and an ephemeral node connect and exchange entities; two
  independently created durable stores connect and exchange when configured to trust each
  other; `create()` succeeds on a node that has never connected.
- **Plugins.** An announcement travels in the signed presence and a changed one fails the
  signature; a refusing plugin makes `register_peer` return `Refused` with nothing registered,
  on every connector; plugins are asked in the order plugged; a plugin that cannot announce
  fails the presence.
- **Reset after a reseed**, through the websocket connectors and the local-process connector:
  a client that held one seeding's System entity meets a server seeded again, resets by the
  plugin's own decision, and the host's fresh node connects and holds the new entity; the same
  client against the same seeding, against a server announcing nothing, against an
  announcement it cannot read, and on its first connection registers normally; a node
  configured to refuse does so without resetting.
- **Stores.** Each engine refuses a store of another format with `StoreFormatError`.
