use super::*;

/// SQLite values prepared before acquiring the write transaction.
pub struct SqliteNodeTransaction<'a> {
    engine: &'a SqliteNodeStorageEngine,
    entities: Vec<EntityRow>,
    events: Vec<EventRow>,
}

struct EntityRow {
    entity_id: EntityId,
    entity_key: String,
    expected_head: Clock,
    head: Clock,
    head_json: String,
    memberships: BTreeSet<ModelId>,
    state_buffers: Vec<u8>,
    attestations: Vec<u8>,
    materializations: Vec<PreparedMaterialization>,
}

pub(super) struct EventRow {
    id: String,
    entity_id: String,
    body: Vec<u8>,
    parent: String,
    attestations: Vec<u8>,
}

pub(super) fn encode_events(events: &[Attested<Event>]) -> Result<Vec<EventRow>, SqliteNodeError> {
    events
        .iter()
        .map(|event| {
            Ok(EventRow {
                id: event.payload.id().to_base64(),
                entity_id: event.payload.entity_id.to_base64(),
                body: bincode::serialize(&event.payload.body)?,
                parent: serde_json::to_string(&event.payload.parent)?,
                attestations: bincode::serialize(&event.attestations)?,
            })
        })
        .collect()
}

pub(super) fn insert_events(executor: &dyn SqliteExecutor, events: &[EventRow]) -> Result<(), SqliteNodeError> {
    for event in events {
        executor.execute(
            &format!(
                r#"INSERT INTO "{EVENT_TABLE}" ("id", "entity_id", "body", "parent", "attestations")
                VALUES (?, ?, ?, ?, ?) ON CONFLICT ("id") DO NOTHING"#
            ),
            &[
                SqliteValue::Text(event.id.clone()),
                SqliteValue::Text(event.entity_id.clone()),
                SqliteValue::Blob(event.body.clone()),
                SqliteValue::Text(event.parent.clone()),
                SqliteValue::Blob(event.attestations.clone()),
            ],
        )?;
    }
    Ok(())
}

impl<'a> SqliteNodeTransaction<'a> {
    pub(super) fn new(engine: &'a SqliteNodeStorageEngine) -> Self { Self { engine, entities: Vec::new(), events: Vec::new() } }
}

#[async_trait]
impl StorageTransaction for SqliteNodeTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> {
        self.events.extend(encode_events(events)?);
        Ok(())
    }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
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
            entity_key: entity_id.to_base64(),
            expected_head: expected_head.clone(),
            head: state.payload.state.head.clone(),
            head_json: serde_json::to_string(&state.payload.state.head).map_err(SqliteNodeError::from)?,
            memberships: state.payload.state.memberships.clone(),
            state_buffers: bincode::serialize(&state.payload.state.state_buffers)?,
            attestations: bincode::serialize(&state.attestations)?,
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
        if self.entities.is_empty() && self.events.is_empty() {
            return Ok(StorageCommitOutcome::Committed(StorageCommitResult::default()));
        }
        let Self { engine, entities, events, .. } = self;
        let conn = engine.pool.get().await.map_err(|error| MutationError::General(Box::new(SqliteNodeError::Pool(error.to_string()))))?;
        engine.ensure_shared_tables(&conn).await?;
        conn.with_executor(move |executor| {
            // BEGIN IMMEDIATE serializes writers before expectations
            // are read, including concurrent first inserts.
            let tx = executor.begin(Behavior::Immediate)?;
            let mut ordered: Vec<(usize, &EntityRow)> = entities.iter().enumerate().collect();
            ordered.sort_by_key(|(_, write)| write.entity_id);

            let mut observed = BTreeMap::new();
            let mut conflict = false;
            for (_, write) in &ordered {
                let entity_id = write.entity_id;
                let entity_key = entity_id.to_base64();
                let current_row = tx.query_row(
                    &format!(
                        r#"SELECT "id", "state_buffer", "head", "attestations"
                           FROM "{ENTITY_TABLE}" WHERE "id" = ?"#
                    ),
                    &[SqliteValue::Text(entity_key.clone())],
                )?;
                let current = current_row
                    .map(|row| {
                        let row = raw_state_row(&row)?;
                        let memberships = memberships_from_executor(&tx, &entity_key)?;
                        decode_state_row(row, memberships)
                    })
                    .transpose()?;
                let current_head = current.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
                if current_head != write.expected_head {
                    conflict = true;
                }
                observed.insert(entity_id, current);
            }
            if conflict {
                tx.rollback()?;
                return Ok(StorageCommitOutcome::Conflict { observed });
            }

            for (_, write) in &ordered {
                let entity_id = write.entity_id;
                let entity_key = entity_id.to_base64();
                let target = &write.memberships;
                if observed[&entity_id].as_ref().is_some_and(|state| !state.payload.state.memberships.is_subset(target)) {
                    return Err(SqliteNodeError::CorruptRecord(format!(
                        "canonical state for entity {entity_id} would remove durable memberships; membership removal is not supported"
                    )));
                }
                for model in target {
                    tx.execute(
                        &format!(
                            r#"INSERT OR IGNORE INTO "{ENTITY_MODEL_TABLE}" ("entity_id", "model_key")
                               VALUES (?, ?)"#
                        ),
                        &[SqliteValue::Text(entity_key.clone()), SqliteValue::Blob(bincode::serialize(model)?)],
                    )?;
                }
            }

            insert_events(&tx, &events)?;
            let mut committed = Vec::with_capacity(entities.len());
            for (original_index, write) in ordered {
                let entity_id = write.entity_id;
                tx.execute(
                    &format!(
                        r#"INSERT INTO "{ENTITY_TABLE}" ("id", "state_buffer", "head", "attestations")
                           VALUES (?, ?, ?, ?)
                           ON CONFLICT ("id") DO UPDATE SET
                               "state_buffer" = excluded."state_buffer",
                               "head" = excluded."head",
                               "attestations" = excluded."attestations""#
                    ),
                    &[
                        SqliteValue::Text(write.entity_key.clone()),
                        SqliteValue::Blob(write.state_buffers.clone()),
                        SqliteValue::Text(write.head_json.clone()),
                        SqliteValue::Blob(write.attestations.clone()),
                    ],
                )?;
                for projection in &write.materializations {
                    projection.write(&tx)?;
                }
                committed.push((original_index, CommittedEntityWrite { entity_id, canonical_changed: write.expected_head != write.head }));
            }
            committed.sort_by_key(|(index, _)| *index);
            let entities = committed.into_iter().map(|(_, result)| result).collect();
            tx.commit()?;
            Ok(StorageCommitOutcome::Committed(StorageCommitResult { entities }))
        })
        .await
        .map_err(MutationError::from)
    }
}
