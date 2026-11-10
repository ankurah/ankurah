# Node Identity, Peer Trust and Attestation

Every node has a key that other nodes verify. That key is how a node proves
who it is when it connects, how the entities it creates name their minter,
and how a statement an application has a node make about an event or a state,
such as "I committed this event" or "I held this state", stays checkable after
the connection that carried it is gone. Core records keys and nothing about
what a key denotes: an application, a tenant or an owner is a component the
application declares.

## Identities and Node Keys

An `Identity` is a public verifying key; its variant names the signature
scheme, and today the only scheme is ed25519. A `NodeId` is the peer layer's
name for an identity that is a node, a type of its own so a publisher's key
cannot be passed where a peer is meant. Both print, parse and serialize in
human-readable formats with their scheme named, `ed25519:` followed by the
unpadded URL-safe base64 of the key bytes, and a string naming no scheme or an
unknown one does not parse; binary formats write the variant index, then the
bytes. A signature's text form names its scheme the same way.
Verification is strict: a weak key, a key in a non-canonical encoding and a
non-canonical signature all verify as false.

A node holds its signing key (`NodeKey`) privately and does all of its own
signing; policy agents and transports never see it, and its `Debug` prints
only the public identity. A node built without a key draws a fresh one, so
its identity lasts only as long as the process; a durable node built that way
logs a notice saying so. A deployment that wants a durable node to keep its
identity gives it a key through `Node::builder(..).durable().key(..)`, and
`NodeKey::load_or_create` keeps the 32-byte seed in a file the embedder
chooses.

## The Handshake

When a connection opens, each side first sends an opening: the range of
protocol versions it speaks and a fresh `HandshakeChallenge`. An opening's
first twelve bytes, its frame's variant index and then the lowest and the
highest version, keep their place and meaning in every protocol version, so a
node reads them before anything else and learns whether it can talk to a
build newer or older than itself. The two sides use the highest version both
speak. A node that speaks none of the versions its peer offers answers with
the versions it does speak and closes, without reading the rest of the peer's
opening.

The dialing side then presents: it sends a signed `Presence` answering the
accepting side's challenge, carrying its `NodeId`, whether it claims to be
durable, the challenge it answers, an advisory timestamp, the versions it
offered, the versions it saw its peer offer and the version it chose, the
feature numbers it has, and the bytes each of its
[plugins](node-architecture.md#node-plugins) announces under the plugin's
name, all signed under the tag `ankurah.presence.v1`. The accepting side
registers the dialer on that presence, and only then signs its own presence,
answering the dialer's challenge, and sends it. The dialing side registers the
accepting side on that presence, and only then counts itself connected. So an
accepting node signs nothing, and runs no plugin's announcement, for a dialer
it has not authenticated and registered. An accepting node that will not
register the dialer, or cannot sign its own presence, sends `Refused` with a
reason a person can read in place of its presence, and closes; a websocket
client reports that as a refused connection state carrying the reason, and
keeps backing off before it dials again.

A node ignores a feature number it does not know, and an announcement under a
name none of its plugins has; each of its plugins receives the announcement
under its own name. This build assigns no feature numbers. Connectors hold each
connection's side of the handshake in a `Handshake`, and `register_peer`
checks a presence against it: it accepts the presence only if it answers the
single-use challenge this node issued for that connection, its signature
verifies under the key it names, the peer's opening carried a challenge the
peer issued itself, it does not name this node itself, and it repeats the
offers this node sent and saw and the version this node chose. Openings carry
no signature, so a party between the two nodes could change the offers to
lower the version chosen; the peer then signs offers that differ from this
node's, and its presence is refused. A presence copied from another connection
answers a different challenge and is refused. Nothing in the handshake
compares datasets: two nodes that share no history connect as readily as two
that share all of it.

What the handshake does not give: a node that dials an address sends its
presence before it knows who answers there, so whoever answers can use that
presence at one other accepting node, and can sit between the two and alter
frames, which are not signed. A dialer chooses its addresses; letting it name
the node it expects at an address needs nothing on the wire and is later
work, as is signing frames after the handshake. A registered peer cannot pass
its frames off as another peer's: a node handles a frame only on the
connection of the peer the frame names as its sender.

## Peer Trust

Whether a peer counts as durable follows from how it was reached, not from
its claim. Each connector states a `PeerProvenance`: a node that dialed the
peer from its own configuration registers it as `Configured`, and a node that
accepted the connection registers it as `Inbound`. `PolicyAgent::peer_trust`
turns the provenance and the claim into a `PeerTrust`. The default trusts a
durable claim only from a configured peer; a policy agent may override it,
for example to trust peers listed by key. Only a peer trusted as durable
becomes a relay target for local commits and a source for durable reads; an
inbound peer's durable claim is logged and otherwise ignored.

A node id's latest registration decides its trust, and registrations of one
id apply one at a time. A node that dials an address presents to whoever
answers there, so an id trusted as durable can be registered again over a
connection that is not. Durable routing for
that id is dropped at that moment, and every request to a peer chosen because
it is durable re-checks, as it is sent, that the peer's current connection is
trusted as durable; a request that fails the check is refused, so neither its
credentials nor its reply cross the new connection.

## Who Minted an Entity

A genesis names the `Identity` that minted it, and the minter is inside the
entity's id:

```text
operation digest = SHA-256(tag(operation) || the operation's bytes)
genesis id = SHA-256(tag(genesis) || minter || nonce || timestamp || count || digest_1 .. digest_n)
update id  = SHA-256(tag(update) || entity_id || nonce || timestamp || count || digest_1 .. digest_n || parent)
```

Every string Ankurah hashes or signs opens with a domain tag naming what the
string is for: `ankurah.genesis.v1`, `ankurah.update.v1`,
`ankurah.operation.v1`, `ankurah.presence.v1`, `ankurah.attestation.v1` or
`ankurah.state.v1`, written as one length byte followed by the tag's bytes, so
that a string made for one purpose can never be read as another's. The
repository's `specs/protocol/bytes.md` writes out every such string, and the
encoding of each value inside it, byte by byte, with test vectors.

`Transaction::create` mints under the node's own identity, so creating an
entity needs nothing but the node's key: a node that has never connected to
anyone can mint. Because the minter is hashed into the id, no relay can drop
or re-attribute it; the same content minted under another identity is a
different entity. An update names no minter; an application that wants to
record who committed an update has the committing node attest it.

## Attestations

An attestation is a statement one identity signs about one event or one entity
state, which anyone who later holds it can check without the connection that
carried it. A `Statement` names its attester, its subject (an event by its id,
or a state by its entity, its head and a digest of the whole state: SHA-256
over `ankurah.state.v1` followed by the state), its kind and its payload. The
kind is the entity id of the definition that says what the statement means and
how to read the payload; core defines no kinds. A `SealedAttestation` is the
statement's exact bytes and the attester's signature over
`ankurah.attestation.v1` followed by those bytes.

A node makes an attestation only when its application's policy agent asks for
one, and seals it with the node's own key:

- when `check_write_event` returns an `AttestationRequest`, the node that ran
  the check attaches a statement about the event with the request's kind and
  payload. The check runs on a local commit and on a durable node admitting
  another node's commit;
- when `PolicyAgent::request_state_attestation` returns one for a state a durable node
  serves, in fetch responses, subscription deltas and direct gets, the node
  attaches a statement about that state. Ephemeral nodes attach none.

An agent that returns `None` has its node sign nothing. `PermissiveAgent` and
`JwtAgent` return `None` from both hooks.

Core checks no attestation. It stores and relays the attestations on an event
exactly as they came, including ones it cannot read, and
`validate_received_event` and `validate_received_state` receive them
unchanged. A received state's attestations go no further than
`validate_received_state`: a node stores every state without attestations, and
a durable node serves a state with only the one its own agent requests. An
agent that cares unseals one with `SealedAttestation::unseal`, which
yields the statement only if the bytes decode and the signature verifies under
the statement's attester, and then checks that the subject names the event or
state in hand (`Subject::names_event`, `Subject::names_state`). Whether an
attestation suffices, and whether its attester is a trusted durable peer or a
trusted publisher, is the agent's decision.

`PolicyAgent::trusted_publishers` names the identities whose published
component and property definitions a node may adopt. By default it returns
the list Ankurah ships in `core/src/policy/trusted_publishers.txt`, which is
empty until the Commons publisher's key exists; an application overrides it
with its own.

## Resetting a Node

`Node::reset` halts the node with `NodeHaltReason::Reset` and then wipes its
storage through `StorageEngine::delete_all`; the wipe finishes even if the
caller stops waiting. The host then discards the halted node with its contexts
and live queries, and builds a fresh node on the wiped store. Wipes of one node
run one at a time, so a host that finds its node halted by a reset it did not
await, such as a plugin's, calls `Node::reset` itself to wait for that wipe
before it builds the fresh node.

Core never decides when to reset. A node in a client role that should discard
stale data after a development reseed does so because the application, from
its own data, decides it should. The role is the application's, and a
production node that moves between durable nodes has no reason to reset at
all.

### Resetting After a Development Reseed

The `ankurah-system-reset` crate makes that decision as a node plugin. The
application designates one of its models as its System model, its server
creates one entity of that model for each seeding of its data (the example
server creates one when it starts on an empty store), and the server and its
clients plug `SystemReset` over that model. The plugin announces, under the
name `ankurah-system-reset`, the ids of the System entities its node holds.
When a peer trusted as durable announces some, the node holds some, and the
two share none, the peer holds another seeding:
the plugin refuses the peer, after resetting the node if the application built
the plugin with `OnReseed::Reset` (`OnReseed::Refuse` only refuses). Either
side holding none is no verdict, and so is an announcement the plugin cannot
read: the peer registers and the plugin logs a warning. An ephemeral node that
registers a durable peer comes to hold the System entities the peer announced,
so its first connection leaves it holding the seeding later connections are
compared with.

A websocket client stops when its node halts, so after a reset it reports
`NodeHalted(Reset)` rather than the plugin's refusal; the host reacts to that
halt as described above. The example bindings do so at start-up: they reset
the halted node to wait for its wipe and start a fresh one on the store.
