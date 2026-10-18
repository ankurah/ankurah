Flat prefix inventory benchmark

The original nine modes preserve the benchmark-only experiment at ae2fb892.
The added packed_production mode measures the adopted production decoder,
including validation, retained buffer ownership, traversal and output drop.
The current_owned baseline is frozen in legacy.rs and legacy_compact.rs, so it
still measures the pre-adoption code. Run from the workspace root:

  cargo bench -p ankurah-proto --bench prefix_inventory > results.json

RESULTS.txt and results.json preserve the original pre-adoption measurements.
ADOPTION.txt and adoption-results.json report the subsequent production change.

PREFIX_CHECK_ONLY=1 runs its correctness checks without timings.
PREFIX_SAMPLES (default 9) and PREFIX_BATCH_MS (default 10) tune sampling.

The input is one SpanContent::Heads envelope. Historical controls use the
pre-adoption compact initial inventory encoding; the plain control reproduces
those bytes exactly. The prototype changes its local version and key encoding:

  bounds, count, repeated(shared_prefix_length, suffix_length, suffix, EntityHead)

Each key inherits bytes from the preceding key; the first prefix length is zero.
Records sit consecutively in one byte buffer, so explicit next pointers and
nested allocations are unnecessary. Every inventory is independently decodable.
IDs, clocks and bounds retain their existing encodings. The prefix never uses
TreeSpanDigest.prefix and does not imply tree structure. Bounds remain absolute.
The historical experiment measures both encodings. The added production mode
selects the smaller key encoding and uses fixed-width generations when those
are smaller than varints. It uses the same codec for initial/follow-up inventories.

Comparisons

current_owned: the frozen pre-adoption production deserializer and owned output.
packed_production: the actual adopted deserializer and lending cursor output.
current_owned_control: the prototype parser on unchanged production bytes.
prefix_owned: the same prototype parser, reconstructing owned keys.
current_borrowed / prefix_borrowed: consume views over an existing byte slice.
current_retained / prefix_retained: also allocate/copy the wire buffer first.
current_validated_retained / prefix_validated_retained: copy, validate the
  complete codec payload, then traverse it again to consume; this conservatively
  measures rejecting malformed later rows before exposing any earlier rows.

The current-format borrowed control separates representation improvements from
compression. Prefix traversal uses one reusable key scratch buffer; IDs and
canonical clocks borrow from the input. That key view lasts only for the current
callback. Arbitrary persistent full-key slices and cheap random access are not
provided. A scratch buffer can grow more than once. Noncanonical clocks fall
back to production Clock decoding, preserving normalization and errors at the
cost of allocations. Retained modes include a buffer copy, not a claim that
existing transports already transfer ownership without copying.

Measurement

All modes visit/hash every key, entity ID and normalized clock tip, then drop
temporary output. Reported time is decode + consume + drop, not pure parse time
or query initialization latency. Hashing deliberately touches all decoded data.
Size is the entire SpanContent::Heads envelope; TreeSpec/request framing is not
included. IDs and clocks are not compressed by this experiment.

The global System allocator wrapper measures allocation/reallocation calls,
total requested bytes and peak live requested bytes. Input fixtures and the
already-existing wire buffer are outside that scope. Retained modes include
one additional wire-buffer allocation/copy; borrowed modes exclude it. These
are requested heap bytes, not RSS, allocator overhead or reserved source trees.
Counting runs separately from timings; the enabled-flag check remains during
timings. Each mode warms up, calibrates a batch, and rotates execution order over
repeated samples. Raw samples are emitted so medians need not hide variability.

Correctness checks precede measurements: all fields round-trip for both modes,
the plain control matches production bytes, and all consumption checksums agree.
Cases cover empty/single/ID-only keys, nearby integers, random binary keys,
shared and clustered strings, duplicates, wide keys, null/NUL values, clipped
bounds, and multi-tip clocks across generation varint widths. Checks also cover
all bound forms, repaired-envelope truncations, trailing bytes, unknown versions,
illegal prefix lengths, clock normalization/conflicts, excess rows and bounded
expanded key work. This is not a complete untrusted-input hardening review.

Limitations

This benchmark measures native sequential inventory consumption. No browser,
network, whole-query benchmark, encoder timing or source-inventory construction
is included in its timings. ADOPTION.txt also reports separate query regressions.
Historical controls measure both encodings even when prefix coding loses bytes.
The prototype bounds rows, blob bytes and expanded key work, but does not replace
production KeySpec, address-order, span-membership, or application validation.
The borrowed parsers are coupled to their documented binary layouts. The adopted
production codec changes the binary protocol and requires updated peers; JSON
remains readable keyed heads. No microbenchmark result establishes end-to-end
Fetch/livequery performance or makes recursive grouping necessary. Historical
follow-up fixed-width inventories are outside this codec comparison.
