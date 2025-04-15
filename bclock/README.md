What do we need:

- The ability to apply an event exactly once to an entity
- Comparability determination for:
  - Entities
  - Collections
  - Database (seed event)

Pending Decisions:

- [ ] Entity ids are random - Ulid/UUID
- [ ] Event ids are content based (hash of entity id + precursor events)
- [ ] beacon events should have a bloom filter containing those events since the last beacon ping (skipchain)

Questions:

- [ ] How do we apply bloom filters so as to gain efficiency without saturating the filters over time? (Entity level only? Multiple, generational bloom filters?)
- [ ] How do we determine when to reject a very late arriving event?

Beacon roadmap

- [ ] create and store a genesis event
- [ ] compare genesis events on startup
- [ ] maintain a single head Clock for each collection which is updated with each edit to the entities in the collection
