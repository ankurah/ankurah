pub fn exec_index_plan(
    &self,
    plan: Plan,
    limit: Option<u64>,
    prefix_guard_disabled: bool,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    match plan {
        Plan::EmptyScan => Ok(Vec::new()),
        Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill: _ } => {
            // Ensure index exists and is built
            let (index, _match_type) = self.database.index_manager.assure_index_exists(
                self.collection_id.as_str(),
                &index_spec,
                &self.database.db,
                &self.database.property_manager,
            )?;

            // Normalize and convert bounds to sled range keys
            let (canonical, eq_prefix_len, eq_prefix_values) = normalize(&bounds);
            let (start_full, end_full_opt, upper_open_ended, eq_prefix_bytes) =
                bounds_to_sled_range(&canonical, eq_prefix_len, &eq_prefix_values);

            let base_iter: Box<dyn Iterator<Item = Result<(sled::IVec, sled::IVec), sled::Error>>> = match &end_full_opt {
                Some(end_full) => Box::new(index.tree().range(start_full.clone()..end_full.clone())),
                None => Box::new(index.tree().range(start_full.clone()..)),
            };
            let iter: Box<dyn Iterator<Item = Result<(sled::IVec, sled::IVec), sled::Error>>> = match scan_direction {
                ankurah_storage_common::ScanDirection::Forward => base_iter,
                ankurah_storage_common::ScanDirection::Reverse => match &end_full_opt {
                    Some(end_full) => Box::new(index.tree().range(start_full.clone()..end_full.clone()).rev()),
                    None => Box::new(index.tree().range(start_full.clone()..).rev()),
                },
            };

            let mut results: Vec<Attested<EntityState>> = Vec::new();
            let mut count: u64 = 0;

            let mut use_prefix_guard = upper_open_ended && eq_prefix_len > 0;
            #[cfg(debug_assertions)]
            if prefix_guard_disabled {
                use_prefix_guard = false;
            }

            for kv in iter {
                let (k, _v) = kv.map_err(SledRetrievalError::StorageError)?;
                let key_bytes = k.as_ref();

                // Equality-prefix guard for open-ended scans
                if use_prefix_guard {
                    if key_bytes.len() < eq_prefix_bytes.len() || &key_bytes[..eq_prefix_bytes.len()] != eq_prefix_bytes.as_slice() {
                        break;
                    }
                }

                let eid = decode_entity_id_from_index_key(key_bytes)?;
                if let Some(ivec) = self.database.entities_tree.get(eid.to_bytes()).map_err(sled_error)? {
                    let sfrag: StateFragment = bincode::deserialize(ivec.as_ref())?;
                    let es = Attested::<EntityState>::from_parts(eid, self.collection_id.clone(), sfrag);
                    let entity = TemporaryEntity::new(es.payload.entity_id, self.collection_id.clone(), &es.payload.state)?;
                    if evaluate_predicate(&entity, &remaining_predicate)? {
                        results.push(es);
                        if let Some(l) = limit {
                            count += 1;
                            if count >= l {
                                break;
                            }
                        }
                    }
                }
            }

            Ok(results)
        }
    }
}

pub(crate) fn exec_fallback_scan(
    &self,
    predicate: ankql::ast::Predicate,
    order_by: Option<Vec<ankql::ast::OrderByItem>>,
    limit: Option<u32>,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    // TODO: Replace with new pipeline architecture
    // For now, use simple full scan - this will be replaced by the streaming pipeline
    let mut results: Vec<Attested<EntityState>> = Vec::new();
    let iter = self.tree.iter();
    for item in iter {
        let (key_bytes, value_bytes) = item.map_err(SledRetrievalError::StorageError)?;
        let id = EntityId::from_bytes(key_bytes.as_ref().try_into().map_err(RetrievalError::storage)?);
        let sfrag: StateFragment = bincode::deserialize(&value_bytes)?;
        let es = Attested::<EntityState>::from_parts(id, self.collection_id.clone(), sfrag);

        let entity = TemporaryEntity::new(id, self.collection_id.clone(), &es.payload.state)?;
        if evaluate_predicate(&entity, &predicate)? {
            results.push(es);
            // Apply limit during scan for efficiency
            if let Some(limit_val) = limit {
                if results.len() >= limit_val as usize {
                    break;
                }
            }
        }
    }

    // TODO: Replace with pipeline sorting logic
    // For now, always sort if ORDER BY is present
    if order_by.is_some() {
        ankurah_storage_common::sorting::apply_order_by_sort(&mut results, &order_by, &self.collection_id)?;
    }
    if let Some(limit_val) = limit {
        results.truncate(limit_val as usize);
    }
    Ok(results)
}
}

/// Create the appropriate iterator based on the strategy

fn entity_id_successor_bytes(id: &EntityId) -> Option<[u8; 16]> {
let mut bytes = id.to_bytes();
for i in (0..bytes.len()).rev() {
    if bytes[i] != 0xFF {
        bytes[i] = bytes[i].saturating_add(1);
        for j in (i + 1)..bytes.len() {
            bytes[j] = 0x00;
        }
        return Some(bytes);
    }
}
None
}

fn decode_entity_id_from_index_key(key: &[u8]) -> Result<EntityId, RetrievalError> {
if key.len() < 1 + 16 {
    return Err(RetrievalError::StorageError("index key too short".into()));
}
let eid_bytes: [u8; 16] =
    key[key.len() - 16..].try_into().map_err(|_| RetrievalError::StorageError("invalid entity id suffix".into()))?;
Ok(EntityId::from_bytes(eid_bytes))
}
