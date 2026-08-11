# Design Goals

The maintained design-goals page lives in the Ankurah book:
https://ankurah.org/design-goals.html

Two notes for readers of historical copies of this file: an entity id is the
32-byte id of that entity's genesis event, so an identity cannot be chosen
ahead of the content it names, and every event id is a content hash over the
event body and its parent clock. Earlier drafts of this document described
coordination-free ULID entity ids and a ULID-based operation-id scheme;
neither reflects the implementation.
