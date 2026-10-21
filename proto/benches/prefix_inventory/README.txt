Packed inventory benchmark

This benchmark compares the adopted production decoder with the frozen owned
baseline from ae2fb892. Run from the workspace root:

  cargo bench -p ankurah-proto --bench prefix_inventory > results.json

PREFIX_CHECK_ONLY=1 runs exact-field and consumption checks without timings.
PREFIX_SAMPLES (default 9) and PREFIX_BATCH_MS (default 10) tune sampling.

Modes

current_owned: the frozen pre-adoption production deserializer and owned output,
  kept in legacy.rs and legacy_compact.rs.
packed_production: the actual adopted deserializer, including codec validation,
  retained buffer ownership, lending cursor traversal and output drop.

Each input is one SpanContent::Heads envelope. The production codec chooses the
smaller of plain and previous-key prefix encoding and uses fixed-width clock
generations when they are smaller than varints. Initial and follow-up inventories
use the same codec. Bounds remain absolute; key prefix sharing does not describe
tree structure. The lending cursor reconstructs one key in a reusable buffer;
that key view lasts only until the next advance.

Measurement

Both modes visit/hash every key, entity ID and normalized clock tip, then drop
temporary output. Reported time is decode + consume + drop, not pure parse time
or query initialization latency. Hashing deliberately touches all decoded data.
Size is the entire SpanContent::Heads envelope; TreeSpec/request framing is not
included.

The global System allocator wrapper measures allocation/reallocation calls,
total requested bytes and peak live requested bytes. Input fixtures and the
already-existing wire buffer are outside that scope. Packed production includes
its additional wire-buffer allocation/copy. These are requested heap bytes, not
RSS, allocator overhead or reserved source trees. Counting runs separately from
timings; the enabled-flag check remains during timings. Each mode warms up,
calibrates a batch, and rotates execution order over repeated samples. Raw samples
are emitted so medians need not hide variability.

Correctness checks precede measurements: both modes round-trip bounds and every
key, ID and normalized clock tip exactly, and their consumption checksums agree.
The unchanged 20 fixtures cover empty/single/ID-only keys, nearby integers, random
binary keys, shared and clustered strings, duplicates, wide keys, null/NUL values,
clipped bounds, and multi-tip clocks across generation varint widths. Production
malformed-input, normalization and expanded-work-limit tests live beside the
production codec in proto/src/state/inventory/tests.rs.

History

RESULTS.txt and results.json preserve the original nine-mode prototype study.
ADOPTION.txt and adoption-results.json report the subsequent production change.
Their source, fixture definitions and raw data are unchanged by this cleanup;
their experimental modes are no longer part of the maintained benchmark.

The complete ten-mode harness before cleanup is preserved at commit 0f00d025
and in the research archive:

  research_notes/ankurah-replication/reviews/2026-10-21-review-fixes/benchmark-archive/

The archive includes the exploratory codec, frozen baseline, measurement code,
reports, raw JSON, and a SHA256/provenance manifest. The eight experimental modes
separated representation costs from prefix compression during design selection.
That experiment is complete; the maintained comparison now measures production
against its original baseline. Historical prefix_bytes/choose_prefix fields are
omitted from new output because they described the removed prototype encoder.

Limitations

This benchmark measures native sequential inventory consumption. No browser,
network, whole-query benchmark, encoder timing or source-inventory construction
is included in its timings. ADOPTION.txt reports separate query regressions.
KeySpec, address-order and span-membership validation are not part of the timed
endpoint. The two-mode comparison does not isolate prefix compression from
ownership changes; consult the historical experiment for that distinction.
No microbenchmark result establishes end-to-end Fetch/livequery performance or
makes recursive grouping necessary. Historical follow-up fixed-width inventories
are outside this codec comparison. The adopted codec requires updated peers;
JSON remains readable keyed heads.
