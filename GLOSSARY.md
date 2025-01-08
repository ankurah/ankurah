# Glossary

- **Model**: A struct that describes the fields and their types for a entity in a collection.
- **Collection**: A collection of entitys, with a name and a type that implements the Model trait.
- **Entity**: An discrete identity in a collection similar to a row in a database.
- **View**: A struct that represents the read-only view of an entity which is typed by the Model.
- **Mutable**: A struct that represents the mutable state of an entity which is typed by the Model.
- **Event**: A single event that may or may not be applied to an entity.
- **Storage Engine**: A means of storing and retrieving data which is generally durable (but not necessarily).
- **Storage Collection**: A collection of entities in a storage engine.
