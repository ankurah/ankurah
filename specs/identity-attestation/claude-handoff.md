# Claude review handoff: identity substrate and iroh connector

Date: 2026-07-11

This packet is the operational handoff back to Claude after Codex took over the
rate-limited session. Review this before changing the implementation. The
accepted design remains in `specs/identity-attestation/spec.md`; this file
records what was implemented, how it was hardened, what was validated, and
what still needs scrutiny.

## Branch and PR stack

The work is intentionally stacked:

1. Base: `model-property-metadata` at `4911845c`.
2. Identity substrate: `worktree-identity-substrate`, draft PR
   <https://github.com/ankurah/ankurah/pull/359>.
3. Iroh connector: `iroh-connector`, PR
   <https://github.com/ankurah/ankurah/pull/341>, based on the identity branch.

The identity branch already contained these reviewed implementation commits
before the final reset/session-hardening tranche:

- `29f9191d` - `feat(core): implement identity and attestation substrate`
- `2ee3b5aa` - merge the final model/property metadata base
- `9df93029` - carry genesis proofs in streaming state updates
- `f8b3ec1c` - re-sign logical simulator delivery retries
- `de1990fa` - harden authenticated reset, generation, catalog, and transport
  boundaries (the final code commit before this packet)

The final hardening and this handoff are later commits on the same branch. Use
`git log --oneline 4911845c..worktree-identity-substrate` for the authoritative
tip and complete list.

The iroh branch previously merged the identity stack through `2ee3b5aa` in
`41b5c4bb`. It merged the final identity/handoff tip in `8b823b55`, then adapted
the Iroh connection lifecycle to the terminal core-close contract in
`5cb89ba4`. Use the live PR head as the review target; these stable commits are
the useful review landmarks.

## Scope actually delivered

### Accepted identity design

- `NodeId` is the complete Ed25519 verifying key.
- Core `Node` owns the signing key. Durable callers can provide a persisted
  key; ephemeral callers may generate one per process/session.
- Events, including genesis, are not author-signed. They remain
  content-addressed. Admission and state attestations are signed by the
  admitting node.
- `EntityId` is the full 32-byte genesis `EventId`. Genesis binds the system,
  nonce, timestamp, initial operations, and empty parent. It deliberately does
  not bind collection or a self-declared creator.
- Attestation envelope verification is core-owned and agent-independent. The
  claims payload remains PolicyAgent-defined.
- This is RFC 1 only. Durable membership changes, durable-to-durable sync,
  quorum/ack semantics, revocation epochs, and peer-to-peer read serving remain
  RFC 2 work.

### Protocol and crypto implementation

- Wire protocol is version 6 and intentionally incompatible with older peers.
- Presence uses a symmetric challenge exchange and signs the complete claims,
  including protocol version and the genesis-backed system-root proof.
- Established frames are signed, session-bound, sequenced, and replay-checked.
- Exact bincode framing is enforced. Trailing bytes, text frames, malformed
  frames, wrong sessions, spoofed declared senders, weak Ed25519 keys, and
  non-strict signatures fail closed.
- Full-width entity, event, model, property, request, and query identifiers were
  threaded through proto, core, storage, query literals, examples, bindings,
  simulation, and tests.
- State snapshots and streamed state updates carry or retrieve the genesis
  proof required to validate self-certifying entity identity.

### System-root and reset hardening

- The complete immutable system-root proof is stored and compared with CAS-like
  claim semantics. Startup recovery repairs only a matching claimed root.
- First-join root reservation is fail-closed. Unrelated sessions remain inert
  until the winning root is durable.
- A hard reset invalidates old Entity, View, Mutable, Context/Transaction,
  catalog, subscription, and peer-session state before deleting storage.
- Entity assembly and mutation use exact generation-token identity, not only a
  boolean, closing same-ID ABA against a new resident entity.
- Delayed catalog registration responses, predicate resolution, explicit
  registration, durable schema execution, and response folding are pinned to
  the issuing generation.
- Already-verified messages carry the authenticated peer, exact incoming
  session, and system-generation token through async dispatch. Durable writes
  re-check the token at bounded mutation seams rather than holding the reset
  gate across remote or PolicyAgent I/O.
- Nodes constructed from the exact same `Arc<StorageEngine>` share a
  process-local reset fence and monotonic epoch. This is not cross-process
  coordination and does not deduplicate separately opened engine handles.

### Transport lifecycle hardening

- `PeerSender::close` is required, idempotent, and nonblocking.
- Local-process, native WebSocket client, WebSocket server, and browser/WASM
  WebSocket sessions now terminate the exact physical session when core removes
  the peer.
- A local core reset is terminal for a client connector so it cannot reconnect
  to and immediately rejoin the old founder. Ordinary remote close/error still
  uses backoff and reconnect.
- Native client cleanup is cancellation-safe. Server cleanup drops both socket
  halves. The WASM dispatch queue is bounded and abortable, callbacks are
  disabled on disconnect, and queued browser events observe terminal core-close
  state before decoding so they cannot turn reset into `Error` plus reconnect.

### Iroh connector

- `connectors/iroh` supplies dialer, acceptor/router, bounded framing, presence
  handshake, exact-session deregistration, reconnect behavior, and real
  localhost Iroh integration tests.
- The connector now uses the Ankurah node signing key as the Iroh endpoint key,
  ALPN `ankurah/1`, the protocol-v6 challenge/presence flow, and signed peer
  frames. The old PR text describing unsigned `ankurah/0` presence is obsolete.
- Browser Iroh, peer discovery, mesh routing, peer-to-peer reads, and
  multi-durable routing are outside this PR.

## Breaking and operational behavior

- Existing wire peers cannot interoperate with protocol v6.
- Existing persisted entity/event IDs and old system-root rows are not accepted
  as the new identity format. This is a pre-1.0 breaking storage epoch, not a
  dual-read migration.
- Durable deployments must persist and restore the node signing seed. The
  example server now demonstrates the durable-key helper.
- Changing a durable key changes its `NodeId`; key rotation and durable
  membership are not implemented here.
- Signed attestation claims assume one PolicyAgent claims vocabulary per
  system. Core can still verify the envelope without interpreting claims.

## Adversarial review findings and disposition

The final audit found and the hardening tranche addressed:

- signature malleability/identity-point Ed25519 keys;
- permissive/trailing frame decoding and authenticated text frames;
- client close/EOF hot reconnect behavior;
- unbounded or surviving WASM dispatch after teardown;
- stale native client registrations after cancellation/drop;
- old strong handles and transactions surviving reset;
- same-ID resident ABA during async entity assembly/apply;
- delayed schema responses repopulating a new system;
- already-verified frames dispatching after reset/reconnect;
- logical peer removal without physical transport termination;
- two independently built nodes racing through one shared storage engine;
- stale root-join abort cleanup deleting a successor root;
- stale `SubscribeQuery` work mutating a replacement session.

Before approval, re-audit these particular seams in the final tip:

1. `core/src/collectionset.rs`, `core/src/system.rs`, and
   `core/src/entity.rs`: a shared-engine epoch change must invalidate sibling
   retained handles and routes, while rebinding only the winning manager.
2. `core/src/system.rs`: a losing first-root join on an initially empty shared
   engine must never delete the winner's claim/rows during abort cleanup.
3. `core/src/node.rs` and `core/src/peer_subscription/server.rs`: an old verified
   `SubscribeQuery` paused in async policy work must never install or update a
   replacement session's subscription.
4. `connectors/websocket-client-wasm/src/connection.rs`: queued `open` or
   `message` callbacks after core close must reach terminal `None`, never
   schedule reconnect.

If any of the first three is still present in the published diff, treat it as a
merge blocker and correct it before merging. Do not weaken the tests to accept
stale reads, stale routes, or post-reset catalog/subscription mutation.

Cutoff audit disposition at handoff time:

- Shared retained Entity/View/Context state, competing empty-engine first join,
  stale replacement-session `SubscribeQuery`, and queued WASM callback races
  were fixed in the final working tree.
- One residual P2 remains: advancing the process-shared storage generation
  invalidates a sibling Node's handles and verified frames, but only the Node
  that initiated reset runs its peer-reset hook. A quiet sibling Node can retain
  physically open `PeerState` transports, and a sibling request already waiting
  on its pending oneshot may wait forever because that sibling pending map is
  not actively drained. Claude should add shared-fence participant callbacks
  (or an equivalent broadcast) that close sibling sessions and wake their
  pending requests when the epoch advances, with a deterministic two-Node
  shared-engine regression. Treat this as the known correction to apply before
  merge unless the project deliberately accepts it and documents the degraded
  cancellation behavior.
- The stale `SubscribeQuery` fix does not yet have the strongest deterministic
  paused-`check_request` plus reset plus same-NodeId reconnect regression. Add
  that test while reviewing the implementation.
- `sibling_reset_immediately_invalidates_retained_handles_and_routes` records
  the desired retained-handle/route regression but is ignored at handoff: its
  setup repeatedly hits the pre-existing album descriptor/catalog race before
  reaching reset. Replace that setup with a catalog-independent resident
  entity (or fix the descriptor race), then un-ignore it. The shared-token
  implementation was reviewed directly, but this exact integration arm is not
  a green proof yet.

## Validation record

Green during implementation and hardening:

- `cargo test -p ankurah-proto` (25 tests at the earlier full run)
- `cargo test -p ankurah-core --features test-helpers` (242 passed, 1 ignored
  at the earlier full run)
- native WebSocket client/server checks
- WASM WebSocket client check for `wasm32-unknown-unknown`
- WebSocket connector tests and doctests
- `cargo test -p ankurah-tests --test protocol_version` (14/14 after transport
  reset tests)
- `cargo test -p ankurah-tests --test websocket` (4/4)
- `cargo test -p ankurah-tests --test registration_lifecycle` (10/10)
- focused core schema tests (5/5)
- `cargo test -p ankurah-connector-iroh --lib` (15/15 at `5cb89ba4`)
- earlier system, identity/attestation, local subscription, and inter-node
  suites before the final shared-engine changes
- `git diff --check` and owned-file formatting checks throughout the audit

The final publishing turn intentionally uses a compact gate because the user
requested an immediate wrap-up. Check the PR description and latest local/CI
results for the exact final commands. The broad native workspace command is
not an honest single gate in this repository because wasm-only and JWT feature
unification creates known unrelated combinations; use the focused package and
target matrix instead.

## Recommended Claude review sequence

1. Read `specs/identity-attestation/spec.md`, especially the six recorded
   design rulings and non-goals. Do not reopen those choices accidentally.
2. Diff `worktree-identity-substrate` against `model-property-metadata`, then
   review the four hot spots above before style or refactoring.
3. Run formatting, `git diff --check`, the focused core/proto checks, and these
   integration tests serially and with their default runner where applicable:

   ```text
   cargo test -p ankurah-tests --test system
   cargo test -p ankurah-tests --test system -- --test-threads=1
   cargo test -p ankurah-tests --test protocol_version
   cargo test -p ankurah-tests --test registration_lifecycle
   cargo test -p ankurah-tests --test identity_attestation
   cargo test -p ankurah-tests --test inter_node -- --test-threads=1
   cargo test -p ankurah-tests --test local_subscription -- --test-threads=1
   cargo test -p ankurah-tests --test websocket
   ```

4. Review PR 341 only after the identity branch. Confirm its final merge parent
   includes the complete identity tip and adapt every Iroh `PeerSender` to the
   terminal close/session-generation contract.
5. Apply corrections as small follow-up commits on the owning branch. Keep the
   iroh PR stacked on the identity branch until the substrate merges.
6. Start RFC 2 separately for multi-durable membership/sync, policy/revocation
   epochs, ack semantics, and peer-to-peer read serving. None of those features
   should be inferred from this substrate PR.

## Review standard

Passing tests are necessary but not sufficient here. For every reset or
reconnect path, reason about an operation paused immediately before each await,
then resumed after a different root or same-NodeId replacement session becomes
current. The required outcome is either completion wholly inside the old fenced
generation or a typed reset/closed error with no durable, resident, catalog,
reactor, or route side effect in the new generation.
