use super::*;

/// PostgreSQL values prepared for one atomic commit.
pub struct PostgresTransaction<'a> {
    engine: &'a Postgres,
    entities: Vec<EntityRow>,
    events: Vec<EventRow>,
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
    events.iter().map(|event| Ok(EventRow {
        id: event.payload.id(),
        entity_id: event.payload.entity_id,
        body: bincode::serialize(&event.payload.body)?,
        parent: event.payload.parent.clone(),
        attestations: bincode::serialize(&event.attestations)?,
    })).collect()
}

pub(super) async fn insert_events(
    client: &(impl tokio_postgres::GenericClient + Sync),
    events: &[EventRow],
) -> Result<(), MutationError> {
    for event in events {
        client.execute(
            &format!(r#"INSERT INTO "{EVENT_TABLE}" ("id", "entity_id", "body", "parent", "attestations")
                VALUES ($1, $2, $3, $4, $5) ON CONFLICT ("id") DO NOTHING"#),
            &[&event.id, &event.entity_id, &event.body, &event.parent, &event.attestations],
        ).await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
    }
    Ok(())
}

impl<'a> PostgresTransaction<'a> {
    pub(super) fn new(engine: &'a Postgres) -> Self { Self { engine, entities: Vec::new(), events: Vec::new() } }
}

#[async_trait]
impl StorageTransaction for PostgresTransaction<'_> {
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
            let projection = self.engine.materialization(model).await
                .map_err(|error| MutationError::General(error.to_string().into()))?
                .prepare_state(state).await?;
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
        if self.entities.is_empty() && self.events.is_empty() {
            return Ok(StorageCommitOutcome::Committed(StorageCommitResult::default()));
        }
        let mut client = self.engine.pool.get().await.map_err(|error| MutationError::General(Box::new(error)))?;
        self.engine.ensure_shared_tables(&client).await?;
        let transaction = client.transaction().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;

        let mut ordered: Vec<(usize, &EntityRow)> = self.entities.iter().enumerate().collect();
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
                let memberships = self.engine
                    .associated_models(&transaction, entity_id)
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
            transaction.rollback().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
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

        insert_events(&transaction, &self.events).await?;
        let mut committed = Vec::with_capacity(self.entities.len());
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
                projection.write(&transaction).await?;
            }
            let canonical_changed = write.expected_head != write.head;
            committed.push((
                original_index,
                CommittedEntityWrite {
                    entity_id,
                    canonical_changed,
                },
            ));
        }

        committed.sort_by_key(|(index, _)| *index);
        let entities = committed.into_iter().map(|(_, result)| result).collect();
        transaction.commit().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
        Ok(StorageCommitOutcome::Committed(StorageCommitResult { entities }))
    }
}
