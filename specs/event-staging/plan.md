## Implementation Plan (Deferred): Event Staging

1. Add staging storage per collection/entity (in-memory or engine-backed queue)
2. Update retrievers to read from staging+store
3. Define promotion rules tied to lineage attestation or verified bridges
4. Garbage collect stale staged events
5. Tests for promotion/GC and interaction with subscription apply
