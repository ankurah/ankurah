use super::*;
use crate::maps::{calls, Lease};
use ankurah_core::storage::pending_writes::PendingWrites;
use tokio_postgres::Client;

/// PostgreSQL values prepared for one atomic commit.
pub struct PostgresTransaction<'a> {
    engine: &'a Postgres,
    entities: Vec<EntityRow>,
    events: Vec<EventRow>,

    /// The connection its first map call leased from the maps' pool, with
    /// the native transaction begun on it, held to the transaction's end.
    lease: Option<Lease>,

    /// Its map writes yet to send, which go ahead of its next request or its
    /// `COMMIT`.
    pending: PendingWrites,

    /// Set by `poison`, and by each map call from its start until it returns
    /// `Ok`, so that a call that fails, or is dropped part way, leaves it set:
    /// every later call and the commit then fail.
    poisoned: bool,

    /// The greatest lock name taken so far, which each map call's names
    /// ascend past.
    last_lock: Option<LockName>,
}

struct EntityRow {
    entity_id: EntityId,
    expected_head: Clock,
    head: Clock,
    memberships: BTreeSet<ModelId>,
    state_buffers: Vec<u8>,
    attestations: Vec<Vec<u8>>,
    materializations: Vec<PreparedMaterialization>,
}

pub(super) struct EventRow {
    id: EventId,
    entity_id: EntityId,
    body: Vec<u8>,
    parent: Clock,
    attestations: Vec<u8>,
}

pub(super) fn encode_events(events: &[Attested<Event>]) -> Result<Vec<EventRow>, MutationError> {
    events
        .iter()
        .map(|event| {
            Ok(EventRow {
                id: event.payload.id(),
                entity_id: event.payload.entity_id,
                body: bincode::serialize(&event.payload.body)?,
                parent: event.payload.parent.clone(),
                attestations: bincode::serialize(&event.attestations)?,
            })
        })
        .collect()
}

pub(super) async fn insert_events(client: &(impl tokio_postgres::GenericClient + Sync), events: &[EventRow]) -> Result<(), MutationError> {
    for event in events {
        client
            .execute(
                &format!(
                    r#"INSERT INTO "{EVENT_TABLE}" ("id", "entity_id", "body", "parent", "attestations")
                VALUES ($1, $2, $3, $4, $5) ON CONFLICT ("id") DO NOTHING"#
                ),
                &[&event.id, &event.entity_id, &event.body, &event.parent, &event.attestations],
            )
            .await
            .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
    }
    Ok(())
}

impl<'a> PostgresTransaction<'a> {
    pub(super) fn new(engine: &'a Postgres) -> Self {
        Self {
            engine,
            entities: Vec::new(),
            events: Vec::new(),
            lease: None,
            pending: PendingWrites::default(),
            poisoned: false,
            last_lock: None,
        }
    }
}

#[async_trait]
impl StorageTransaction for PostgresTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        self.events.extend(encode_events(events)?);
        Ok(())
    }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        if self.lease.is_some() {
            return Err(MutationError::InvalidUpdate("state set after the transaction's first map call"));
        }
        let entity_id = state.payload.entity_id;
        let index = self.entities.iter().position(|write| write.entity_id == entity_id);
        if index.is_some_and(|index| self.entities[index].head != *expected_head) {
            return Err(MutationError::InvalidUpdate("state does not follow the preceding transaction write"));
        }
        let mut materializations = Vec::new();
        for model in &state.payload.state.memberships {
            let projection = self
                .engine
                .materialization(model)
                .await
                .map_err(|error| MutationError::General(error.to_string().into()))?
                .prepare_state(state)
                .await?;
            materializations.push(projection);
        }
        let mut write = EntityRow {
            entity_id,
            expected_head: expected_head.clone(),
            head: state.payload.state.head.clone(),
            memberships: state.payload.state.memberships.clone(),
            state_buffers: bincode::serialize(&state.payload.state.state_buffers)?,
            attestations: state.attestations.iter().map(bincode::serialize).collect::<Result<_, _>>()?,
            materializations,
        };
        if let Some(index) = index {
            write.expected_head = self.entities[index].expected_head.clone();
            self.entities[index] = write;
        } else {
            self.entities.push(write);
        }
        Ok(())
    }

    async fn commit(self) -> Result<StorageCommitOutcome, MutationError> {
        if self.poisoned {
            return Err(MutationError::TransactionFailed);
        }
        let Self { engine, entities, events, lease, pending, .. } = self;
        let holds_records = !entities.is_empty() || !events.is_empty();

        // The shared tables are ensured outside the commit's native
        // transaction: DDL inside it would land only with it, after the DDL
        // lock that keeps two sessions from creating one table at once was
        // released.
        let mut pooled;
        let native = match lease {
            Some(lease) => {
                if holds_records {
                    engine.ensure_shared_tables_aside().await?;
                }
                NativeTransaction::Held { lease, pending }
            }
            None if !holds_records => return Ok(StorageCommitOutcome::Committed(StorageCommitResult::default())),
            None => {
                pooled = engine.pool.get().await.map_err(|error| MutationError::General(Box::new(error)))?;
                engine.ensure_shared_tables(&pooled).await?;
                NativeTransaction::Begun(pooled.transaction().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?)
            }
        };

        let outcome = write_entity_rows(engine, &entities, &events, native.client()).await?;
        native.end(&outcome).await?;
        Ok(outcome)
    }

    async fn map_lock_and_read(&mut self, locks: &[LockName], reads: Vec<MapRead>) -> Result<Vec<MapRows>, MutationError> {
        transaction_call(
            &mut self.poisoned,
            &mut self.last_lock,
            locks,
            calls::lock_and_read(&mut self.lease, &mut self.pending, &self.engine.maps, locks, reads),
        )
        .await
    }

    async fn map_write(&mut self, writes: Vec<MapWrite>) -> Result<(), MutationError> {
        transaction_call(
            &mut self.poisoned,
            &mut self.last_lock,
            &[],
            calls::write(&mut self.lease, &mut self.pending, &self.engine.maps, writes),
        )
        .await
    }

    fn poison(&mut self) { self.poisoned = true; }
}

/// The native transaction a commit runs on and ends, which it owns one of
/// two ways.
enum NativeTransaction<'c> {
    /// Begun by the transaction's first map call, on the connection it has
    /// held since: its pending map writes go ahead of the `COMMIT`.
    Held { lease: Lease, pending: PendingWrites },

    /// Begun for this commit alone, on a connection of the engine's pool, as
    /// a transaction that makes no map call commits.
    Begun(tokio_postgres::Transaction<'c>),
}

impl NativeTransaction<'_> {
    /// Its connection, for the entity rows' statements.
    fn client(&self) -> &Client {
        match self {
            Self::Held { lease, .. } => &lease.connection().client,
            Self::Begun(transaction) => transaction.client(),
        }
    }

    /// End it: `COMMIT` for an outcome that committed, `ROLLBACK` for a
    /// conflict.
    async fn end(self, outcome: &StorageCommitOutcome) -> Result<(), MutationError> {
        let committed = matches!(outcome, StorageCommitOutcome::Committed(_));
        match self {
            Self::Held { lease, pending } if committed => calls::commit(lease, pending).await,
            Self::Held { lease, .. } => calls::roll_back(lease).await,
            Self::Begun(transaction) if committed => {
                transaction.commit().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))
            }
            Self::Begun(transaction) => transaction.rollback().await.map_err(|error| MutationError::UpdateFailed(Box::new(error))),
        }
    }
}

/// Write the entity rows and events through `transaction`, a native
/// transaction still open, once every entity's expected head has been
/// compared under its lock: the outcome to commit, or a conflict to roll
/// back.
async fn write_entity_rows(
    engine: &Postgres,
    entities: &[EntityRow],
    events: &[EventRow],
    transaction: &Client,
) -> Result<StorageCommitOutcome, MutationError> {
    let mut ordered: Vec<(usize, &EntityRow)> = entities.iter().enumerate().collect();
    ordered.sort_by_key(|(_, write)| write.entity_id);

    let mut observed = BTreeMap::new();
    let mut conflict = false;
    for (_, write) in &ordered {
        let entity_id = write.entity_id;
        // Unlike a row lock, this also serializes competing inserts for
        // an entity which does not yet have a canonical row.
        let lock_identity = entity_id.to_base64();
        transaction
            .execute("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))", &[&lock_identity])
            .await
            .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
        let current_row = transaction
            .query_opt(
                &format!(
                    r#"SELECT "id", "state_buffer", "head", "attestations"
                       FROM "{ENTITY_TABLE}" WHERE "id" = $1 FOR UPDATE"#
                ),
                &[&entity_id],
            )
            .await
            .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
        let current = if let Some(row) = current_row {
            let memberships = engine
                .associated_models(transaction, entity_id)
                .await
                .map_err(|error| MutationError::General(error.to_string().into()))?
                .into_iter()
                .collect();
            Some(state_from_row(&row, memberships).map_err(|error| MutationError::General(error.to_string().into()))?)
        } else {
            None
        };
        let current_head = current.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
        if current_head != write.expected_head {
            conflict = true;
        }
        observed.insert(entity_id, current);
    }

    if conflict {
        return Ok(StorageCommitOutcome::Conflict { observed });
    }

    for (_, write) in &ordered {
        let entity_id = write.entity_id;
        let target = &write.memberships;
        if observed[&entity_id].as_ref().is_some_and(|state| !state.payload.state.memberships.is_subset(target)) {
            return Err(MutationError::General(
                format!("canonical state for entity {entity_id} would remove durable memberships; membership removal is not supported")
                    .into(),
            ));
        }
        for model in target {
            let model_key = bincode::serialize(model)?;
            transaction
                .execute(
                    &format!(
                        r#"INSERT INTO "{ENTITY_MODEL_TABLE}" ("entity_id", "model_key")
                           VALUES ($1, $2) ON CONFLICT DO NOTHING"#
                    ),
                    &[&entity_id, &model_key],
                )
                .await
                .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
        }
    }

    insert_events(transaction, events).await?;
    let mut committed = Vec::with_capacity(entities.len());
    for (original_index, write) in ordered {
        let entity_id = write.entity_id;
        transaction
            .execute(
                &format!(
                    r#"INSERT INTO "{ENTITY_TABLE}" ("id", "state_buffer", "head", "attestations")
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT ("id") DO UPDATE SET
                           "state_buffer" = EXCLUDED."state_buffer",
                           "head" = EXCLUDED."head",
                           "attestations" = EXCLUDED."attestations""#
                ),
                &[&entity_id, &write.state_buffers, &write.head, &write.attestations],
            )
            .await
            .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;

        for projection in &write.materializations {
            projection.write(transaction).await?;
        }
        let canonical_changed = write.expected_head != write.head;
        committed.push((original_index, CommittedEntityWrite { entity_id, canonical_changed }));
    }

    committed.sort_by_key(|(index, _)| *index);
    let entities = committed.into_iter().map(|(_, result)| result).collect();
    Ok(StorageCommitOutcome::Committed(StorageCommitResult { entities }))
}
