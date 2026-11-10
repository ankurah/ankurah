# The Bytes Ankurah Hashes and Signs

This document is the authority for every byte string Ankurah hashes or signs,
for the encoding of each value inside those strings, and for the frames that
open a connection. The `ankurah-proto` crate is an implementation of it, and
the vectors in sections 5 and 6 are pinned by tests beside the types they
cover.

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

A reader refuses a clock whose entries are out of order (3.2). A reader of any
other map or set accepts its entries in any order, holds them in ascending key
order and keeps the last of a repeated key. The state digest (4.4) and the
presence signature (4.5) are taken over the reader's own encoding of what it
holds, never over the bytes it received.

## 2. Domain tags

Every hashed or signed string opens with a domain tag naming what the string
is for. A tag is `ankurah.<name>.v1` and enters the string as one length byte
followed by the tag's ASCII bytes. No encoded tag is a prefix of another. A new
derivation takes a new tag; an existing tag never takes a new meaning.

| Name | Tag | Encoded, hex |
|---|---|---|
| genesis | `ankurah.genesis.v1` | `12616e6b757261682e67656e657369732e7631` |
| update | `ankurah.update.v1` | `11616e6b757261682e7570646174652e7631` |
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

A reader refuses a clock with a generation of 0, or whose entries are not in
strictly ascending order of event id, which refuses an event id given twice as
well. An update's id covers its parent clock (4.3), so a clock has exactly one
encoding.

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

`ProtocolVersions` is a struct: `lowest: u32`, `highest: u32`. It names every
protocol version from the lowest to the highest; one whose lowest exceeds its
highest names none.

`VersionNegotiation` is a struct: `offered: ProtocolVersions`, the versions the
signer offered in its opening frame (section 6); `peer_offered:
ProtocolVersions`, the versions it saw the receiver offer; `chosen: u32`, the
version it chose, the highest both include.

`PresenceClaims` is a struct: `node_id: NodeId`, `durable: bool`, `challenge:
HandshakeChallenge` (the receiver's), `timestamp: u64` (unix milliseconds,
advisory), `negotiation: VersionNegotiation`, `features`, a sequence of `u32`,
and `announcements`, a map from `String` to byte string. Each feature is a
number core assigns to an ability a later build adds; each announcement is what
one of the signer's plugins announces, under the plugin's name. A receiver
ignores a feature number and an announcement name it does not know.

A presence is a struct: `claims: PresenceClaims`, then `signature: Signature`,
the signature of 4.5. It travels as variant 2 of the `Message` enum of section
6.

A receiver accepts a presence only if its claims repeat the receiver's own
negotiation from the other side: `offered` is what the receiver saw the signer
offer, `peer_offered` is what the receiver offered, and `chosen` is the version
the receiver chose. Opening frames carry no signature, so a party between the
two sides could change one; the presence signed after it then names other
offers and is refused.

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

A sealed attestation unseals only if its bytes hold that struct and nothing after
it, the statement bytes hold one `Statement` and nothing after it, and the
signature verifies under the statement's attester over the signed bytes of 4.6,
taken from the statement bytes as carried. A statement whose head is not in
ascending order does not decode (3.2), so a statement that decodes encodes back
to the bytes it was read from.

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
update id = SHA-256( tag(update) || entity_id || nonce || timestamp || count || digest_1 || ... || digest_n || parent )
```

`entity_id` is the `EntityId` of the entity the update extends and `parent` the
update's parent `Clock`; the other fields are as in 4.2.

### 4.4 State digest

```text
state digest = SHA-256( tag(state) || EntityState )
```

`EntityState` is the state as the digesting node holds it, in its own encoding;
a digest is never taken over state bytes as carried.

### 4.5 Presence signature

```text
signed bytes = tag(presence) || PresenceClaims
```

The signature is the ed25519 signature of `node_id`'s key over the signed
bytes. A receiver checks it over its own encoding of the claims it decoded, not
over the bytes it received: claims whose announcements the signer wrote out of
key order, or with a name repeated, decode to claims that encode otherwise, and
their signature fails.

### 4.6 Attestation signature

```text
signed bytes = tag(attestation) || statement
```

where `statement` is the statement bytes a sealed attestation carries, without
their length. The signature is the ed25519 signature of the attester's key over
the signed bytes.

### 4.7 Verification

An ed25519 signature is 64 bytes: the encoding of a point `R`, then a
little-endian integer `S`. It verifies under the key `A`, the identity's 32
bytes, over the signed bytes `M` only if all of these hold:

- `S` is less than the group order L = 2^252 + 27742317777372353535851937790883648493;
- `A` and `R` each encode a point on the curve, and each is that point's own
  encoding: its y coordinate below p = 2^255 - 19, and its sign bit clear when
  x is 0;
- neither `A` nor `R` is of small order, that is, neither is a point P with
  [8]P the identity;
- [S]B = R + [k]A without the cofactor, where B is the base point and k is
  SHA-512(R || A || M), read as a little-endian integer, mod L.

This is what ed25519-dalek 2's `verify_strict` checks, with the key's encoding
checked as well. It refuses signatures that RFC 8032's cofactored check and
ZIP-215 accept, so an implementation must apply these rules exactly for two
nodes to agree on whether a presence or an attestation verifies. Section 5
gives vectors for these rules.

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
| none | `133fdae5349217e57d706cfb395b259cc94e718562a71779dfd1f0270d8bb649` | `036ff81f4ba2285f3ccd38e0ad3232855a466bb6c0b9d6f0fd254a202180364e` |
| membership | `9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a` | `c3f6d6bab2d1336547d778567c51962fb0fa95e04e35cd9bd45057687339c5c8` |
| membership, backend | `0395ecaf7e93f57b19cf2098a8161c6c0c72b182ff1f5dfc8a0525359cd927a3` | `342304016d7452fae948903da6bb928e9b69b3c460d1bdb49d69fbcead6e1b43` |
| erased membership | `9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a` | `c3f6d6bab2d1336547d778567c51962fb0fa95e04e35cd9bd45057687339c5c8` |

State digest of the entity `[0x55; 32]` whose state buffers map `"lww"` to
`010203` and `"yrs"` to `0405`, whose memberships are `{ EntityId([0x77; 32]),
System(Model) }`, and whose head has one entry, generation 1 and event id
`[0x66; 32]`:

```text
ff38c3da54c1ee5405e4ef560f987e2eda6b611009a1875e3d136b0ab1fbf3da
```

Presence signed bytes for node `Ed25519([0x99; 32])`, durable, answering the
challenge issued by `Ed25519([0x77; 32])` with nonce `[0x88; 32]`, timestamp
`1720000000000`, having offered versions 1 to 3 and seen versions 1 to 1
offered, and chosen version 1, naming feature 7 and announcing `0102` under
`"p"`:

```text
13616e6b757261682e70726573656e63652e7631
00000000 9999999999999999999999999999999999999999999999999999999999999999
01
00000000 7777777777777777777777777777777777777777777777777777777777777777
8888888888888888888888888888888888888888888888888888888888888888
0030fd7790010000
01000000 03000000 01000000 01000000 01000000
0100000000000000 07000000
0100000000000000 0100000000000000 70 0200000000000000 0102
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

Signatures over the signed bytes `6d657373616765` (the ASCII of `message`),
under the rules of 4.7. The base point B encodes as `58` followed by 31 bytes
of `66`; as a key it has the secret scalar 1, so its signatures can be written
down. Each signature refused below holds the equation of 4.7 and breaks one
other rule, except the last, which holds only the cofactored equation
[8][S]B = [8]R + [8][k]A.

| Key | Signature | Verifies | Why |
|---|---|---|---|
| `5866666666666666666666666666666666666666666666666666666666666666` | `5866666666666666666666666666666666666666666666666666666666666666f4a5bddca212b9849c7b53f96034edaf51addd8a2dc7cd06e20c117e5546b204` | yes | R = B and S = 1 + k |
| `0100000000000000000000000000000000000000000000000000000000000000` | `58666666666666666666666666666666666666666666666666666666666666660100000000000000000000000000000000000000000000000000000000000000` | no | the key is the identity point, of small order |
| `5866666666666666666666666666666666666666666666666666666666666666` | `0100000000000000000000000000000000000000000000000000000000000000ce3838e9daae0d280fb00ea0aaeb78b58b6deadcfff78187974fedb20ac4d80f` | no | R is the identity point, of small order |
| `5866666666666666666666666666666666666666666666666666666666666666` | `5866666666666666666666666666666666666666666666666666666666666666e179b339bd75cbdc72184b9c3f2eccc451addd8a2dc7cd06e20c117e5546b214` | no | S is the first signature's S plus L |
| `5866666666666666666666666666666666666666666666666666666666666666` | `98519eadf35b995233b51b5cd23e9cc5a28b639b5a4af0ec903cb960d81b78192827bea5c0e0ad7153f2599106042da3a250ea9c45bacdca959d700a7d53f30b` | no | R is B plus the point `c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a` of order 8 |

The key `f0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f` verifies
no signature: its y is 3 + p, a second encoding of the point whose y is 3. No
signature that holds the equation under it can be written down without that
point's secret scalar. Nor can one for an `R` that breaks only the encoding
rule, since the only points with a second encoding whose secret scalar is known
are of small order.

## 6. The frames that open a connection

Everything two nodes exchange is framed as a `Message`, an enum. The first
frame each side of a connection sends is an opening. Two of the enum's
variants keep their index and their first twelve bytes in every protocol
version, so a node can tell whether it shares a version with its peer before
it reads anything else:

| Index | Variant | Fields |
|---|---|---|
| 0 | `Open` | `versions: ProtocolVersions` (the versions the sender speaks), then `challenge: HandshakeChallenge` |
| 1 | `NoCommonVersion` | `ProtocolVersions` (the versions the sender speaks) |

The first twelve bytes of either frame are the variant index, then the lowest
and the highest version, each a `u32`. Variant 2 is the presence of 3.6, and
variant 3 carries every message after the handshake. A request, a response or
an update there carries its body as a byte string, so the message decodes even
when its body is of a kind the receiver does not know.

A receiver reads the twelve bytes of the peer's first frame before anything
else in it:

- Index 0, an opening. When the two sides' versions share one, both sides use
  the highest version both include, and the receiver reads the rest of the
  frame. In version 1 the rest is the challenge and nothing after it. When they
  share none, the receiver does not read the rest: it answers with
  `NoCommonVersion` carrying its own versions, and closes.
- Index 1, a refusal: the peer speaks the versions it carries. The receiver
  closes.
- Any other index, or fewer than twelve bytes: the receiver closes.

A build that offers version 1 sends exactly the version-1 layout of the
opening, since a receiver that speaks only version 1 reads the rest of the
frame under it. Once both openings are read, each side answers the other's
challenge with its presence.

Vectors. The opening offering versions 1 to 3, with the challenge issued by
`Ed25519([0x77; 32])` with nonce `[0x88; 32]`:

```text
00000000 01000000 03000000
00000000 7777777777777777777777777777777777777777777777777777777777777777
8888888888888888888888888888888888888888888888888888888888888888
```

The refusal from a node that speaks versions 1 to 1:

```text
01000000 01000000 01000000
```

As in section 5, the line breaks and spaces separate fields.
