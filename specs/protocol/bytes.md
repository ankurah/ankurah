# The Bytes Ankurah Hashes and Signs

This document is the authority for every byte string Ankurah hashes or signs,
and for the encoding of each value inside those strings. The `ankurah-proto`
crate is an implementation of it, and the vectors in section 5 are pinned by
tests beside the types they cover.

The presence string changes in a later pass of the 0.10 work
(specs/node-identity/spec.md section 13); this document changes with it.

## 1. Encoding rules

These are the rules bincode 1.3.3 follows in its default free functions
(`bincode::serialize`), written out here so that no string depends on that
crate.

| Value | Encoding |
|---|---|
| `u8`, `u32`, `u64` | 1, 4 or 8 bytes, little-endian |
| `bool` | one byte: `00` for false, `01` for true |
| fixed-size byte array, `[u8; n]` | the n bytes, with no length |
| byte string, `Vec<u8>` | the byte count as a `u64`, then the bytes |
| sequence, `Vec<T>` | the element count as a `u64`, then each element |
| `String` | the UTF-8 byte count as a `u64`, then the UTF-8 bytes |
| map, set | the entry count as a `u64`, then each entry in ascending key order: the key, then the value (a set has no value) |
| struct, tuple | each field in declaration order, with nothing between fields |
| enum | the variant index as a `u32`, then the variant's fields |

Variant indices count from 0 in declaration order. A variant is only ever
appended; an index never changes meaning.

## 2. Domain tags

Every hashed or signed string opens with a domain tag naming what the string
is for. A tag is `ankurah.<name>.v1` and enters the string as one length byte
followed by the tag's ASCII bytes. No encoded tag is a prefix of another. A new
derivation takes a new tag; an existing tag never takes a new meaning.

| Name | Tag | Encoded, hex |
|---|---|---|
| genesis | `ankurah.genesis.v1` | `12616e6b757261682e67656e657369732e7631` |
| event | `ankurah.event.v1` | `10616e6b757261682e6576656e742e7631` |
| operation | `ankurah.operation.v1` | `14616e6b757261682e6f7065726174696f6e2e7631` |
| presence | `ankurah.presence.v1` | `13616e6b757261682e70726573656e63652e7631` |
| attestation | `ankurah.attestation.v1` | `16616e6b757261682e6174746573746174696f6e2e7631` |
| state | `ankurah.state.v1` | `10616e6b757261682e73746174652e7631` |

Below, `tag(name)` stands for the encoded form in the third column.

## 3. Types

### 3.1 Identities and ids

| Type | Encoding | Length |
|---|---|---|
| `Identity` | enum; variant 0 `Ed25519`: the 32-byte compressed ed25519 verifying key | 36 |
| `NodeId` | the `Identity` it names | 36 |
| `Signature` | enum; variant 0 `Ed25519`: the 64-byte ed25519 signature | 68 |
| `EntityId` | its 32 bytes | 32 |
| `EventId` | its 32 bytes | 32 |

A `Signature` is never inside a hashed or signed string; it travels beside the
string it signs.

### 3.2 `Clock`

The entry count as a `u64`, then each entry in ascending order of its event id
(compared as bytes): the generation as a `u32`, then the `EventId`. No two
entries share an event id, and no generation is 0.

### 3.3 `ModelId` and `PropertyId`

`ModelId` is an enum:

| Index | Variant | Fields |
|---|---|---|
| 0 | `EntityId` | `EntityId` |
| 1 | `System` | `SystemModel` |

`SystemModel` is an enum without fields: 0 `Model`, 1 `Property`, 2
`ModelProperty`.

`ModelId` values order by variant index, then by their field: entity ids as
bytes, system models by index. That order sorts a set of them.

`PropertyId` is an enum:

| Index | Variant | Fields |
|---|---|---|
| 0 | `Id` | none |
| 1 | `EntityId` | `EntityId` |
| 2 | `System` | `SystemProperty` |

`SystemProperty` is an enum without fields: 0 `Label`, 1 `Name`, 2
`MintedFor`, 3 `Backend`, 4 `ValueType`, 5 `TargetModel`, 6 `Model`, 7
`Property`, 8 `Optional`.

### 3.4 Operations

`Operation` is an enum:

| Index | Variant | Fields |
|---|---|---|
| 0 | `Backend` | `backend: String`, `operations: Vec<BackendOperation>` |
| 1 | `Membership` | `Membership` |

`BackendOperation` is a struct with one field, `diff`, a byte string that only
the named property backend interprets.

`Membership` is an enum: 0 `Add`, with one `ModelId` field. Index 1 is
reserved for the variant that removes a membership.

An event carries each operation as a `CarriedOperation`, an enum:

| Index | Variant | Fields |
|---|---|---|
| 0 | `Bytes` | byte string: the encoding of one `Operation`, exactly as its author produced it |
| 1 | `Erased` | `[u8; 32]`: the digest of bytes that were erased |

The bytes of a `Bytes` operation hold one encoded `Operation` and nothing
after it. A node cannot read an operation whose bytes it cannot decode (an
unknown variant index, a malformed encoding, or bytes left over) or whose
bytes were erased; it refuses an event holding one. Bytes are carried, stored
and relayed exactly as their author produced them, never re-encoded.

### 3.5 `EntityState`

A struct: `entity_id: EntityId`, then `state: State`. `State` is a struct:
`state_buffers`, a map from `String` (ordered by its UTF-8 bytes) to byte
string; `memberships`, a set of `ModelId` (ordered as in 3.3); `head: Clock`.

### 3.6 Presence claims

`HandshakeChallenge` is a struct: `issuer: NodeId`, `nonce: [u8; 32]`.

`PresenceClaims` is a struct: `node_id: NodeId`, `durable: bool`, `challenge:
HandshakeChallenge`, `timestamp: u64`, `protocol_version: u32`.

### 3.7 Attestations

`Subject` is an enum naming what a statement is about:

| Index | Variant | Fields |
|---|---|---|
| 0 | `Event` | `EventId` |
| 1 | `State` | `entity: EntityId`, `head: Clock`, `state_digest: [u8; 32]` (the state digest of 4.4) |

`Statement` is a struct: `attester: Identity`, `subject: Subject`, `kind:
EntityId`, `payload` (byte string). `kind` is the entity id of the definition
that says what the statement means and how to read `payload`; core defines no
kinds.

A sealed attestation is a byte string holding a struct: `statement`, a byte
string holding the encoding of one `Statement` exactly as its attester produced
it, then `signature: Signature`. An `AttestationSet` is a sequence of sealed
attestations, each a byte string, so a node reads a set whose attestations it
cannot read, and keeps and relays each attestation's bytes unchanged.

A sealed attestation opens only if its bytes hold that struct and nothing after
it, the statement bytes hold one `Statement` and nothing after it, and the
signature verifies under the statement's attester over the signed bytes of 4.6,
taken from the statement bytes as carried. A statement whose bytes decode but
would encode differently, such as a head whose entries are out of order, is
checked against the bytes it carries, never against a re-encoding.

## 4. The strings

`||` joins byte strings. Every hash is SHA-256.

### 4.1 Operation digest

```text
digest = SHA-256( tag(operation) || bytes )
```

for a `Bytes` operation, where `bytes` are the carried bytes without their
length. An `Erased` operation's digest is the 32 bytes it carries.

### 4.2 Genesis id

```text
genesis id = SHA-256( tag(genesis) || minter || nonce || timestamp || count || digest_1 || ... || digest_n )
```

`minter` is an `Identity`, `nonce` 32 bytes, `timestamp` a `u64` (unix
milliseconds, advisory), `count` the number of operations as a `u64`, and
`digest_i` the digest of the i-th operation in the order the event carries
them. An entity's id is the id of its genesis.

### 4.3 Update id

```text
update id = SHA-256( tag(event) || entity_id || nonce || timestamp || count || digest_1 || ... || digest_n || parent )
```

`entity_id` is the `EntityId` of the entity the update extends and `parent` the
update's parent `Clock`; the other fields are as in 4.2.

### 4.4 State digest

```text
state digest = SHA-256( tag(state) || EntityState )
```

### 4.5 Presence signature

```text
signed bytes = tag(presence) || PresenceClaims
```

The signature is the ed25519 signature of `node_id`'s key over the signed
bytes.

### 4.6 Attestation signature

```text
signed bytes = tag(attestation) || statement
```

where `statement` is the statement bytes a sealed attestation carries, without
their length. The signature is the ed25519 signature of the attester's key over
the signed bytes.

### 4.7 Verification

A signature verifies only under strict ed25519 verification: a key that is not
the canonical encoding of its point, a weak key, and a non-canonical signature
all verify as false.

## 5. Vectors

Each vector gives its inputs, then the expected bytes or hash in hex. `[b; n]`
is n bytes of value b.

Operations:

| Operation | Bytes | Digest |
|---|---|---|
| `Membership(Add(System(Model)))` | `01000000000000000100000000000000` | `76ef9eee987e3dc3a09adbf20c51740e948216e9a8f79bdd1994deffedf5fd98` |
| `Backend { backend: "lww", operations: [{ diff: 010203 }] }` | `0000000003000000000000006c777701000000000000000300000000000000010203` | `75307fbd9932f311bc6799c89a2051231dc6e731410b47a0096484352f219db5` |

Event ids, with minter `Ed25519([0x11; 32])`, nonce `[0x22; 32]`, timestamp
`1720000000000`, and for the update the entity id `[0x33; 32]` and the parent
clock with one entry, generation 1 and event id `[0x44; 32]`. "Membership" and
"backend" are the operations above; "erased membership" is the membership
operation carried as `Erased` with its digest.

| Operations | Genesis id | Update id |
|---|---|---|
| none | `133fdae5349217e57d706cfb395b259cc94e718562a71779dfd1f0270d8bb649` | `ea0c549736a30f74f83fc27165938ccd17ac57c5b955fe4a1347f74b32372aa8` |
| membership | `9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a` | `e865a554005226a6dc65c5a8dc039f7d40cd2e658833e741bb6d3a149c0dfe7c` |
| membership, backend | `0395ecaf7e93f57b19cf2098a8161c6c0c72b182ff1f5dfc8a0525359cd927a3` | `76c6835c83eb2aba276df5ad184fca20075a513a9140db37cc00e43e6cb63623` |
| erased membership | `9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a` | `e865a554005226a6dc65c5a8dc039f7d40cd2e658833e741bb6d3a149c0dfe7c` |

State digest of the entity `[0x55; 32]` whose state buffers map `"lww"` to
`010203`, whose memberships are `{ System(Model) }`, and whose head has one
entry, generation 1 and event id `[0x66; 32]`:

```text
05e2003ec33f79b0e5b1aec10579c2c75fde70fd91971765028855fb8509221e
```

Presence signed bytes for node `Ed25519([0x99; 32])`, durable, answering the
challenge issued by `Ed25519([0x77; 32])` with nonce `[0x88; 32]`, timestamp
`1720000000000`, protocol version 1:

```text
13616e6b757261682e70726573656e63652e7631
00000000 9999999999999999999999999999999999999999999999999999999999999999
01
00000000 7777777777777777777777777777777777777777777777777777777777777777
8888888888888888888888888888888888888888888888888888888888888888
0030fd7790010000
01000000
```

Attestation signed bytes for the statement by `Ed25519([0x11; 32])` about
`Event([0xaa; 32])`, of kind `[0xee; 32]`, with payload `07`:

```text
16616e6b757261682e6174746573746174696f6e2e7631
00000000 1111111111111111111111111111111111111111111111111111111111111111
00000000 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
0100000000000000 07
```

and for the statement by the same attester, of the same kind, about `State {
entity: [0xbb; 32], head: one entry, generation 2 and event id [0xcc; 32],
state_digest: [0xdd; 32] }`, with an empty payload:

```text
16616e6b757261682e6174746573746174696f6e2e7631
00000000 1111111111111111111111111111111111111111111111111111111111111111
01000000 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
0100000000000000 02000000 cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
0000000000000000
```

The first statement sealed with the signature `Ed25519([0x99; 64])`, which
shows the layout and does not verify: the statement's length (113 bytes), the
statement, then the signature.

```text
7100000000000000
00000000 1111111111111111111111111111111111111111111111111111111111111111
00000000 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
0100000000000000 07
00000000 99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999
```

The line breaks and spaces in the last four vectors separate fields; the
string is the bytes alone.
