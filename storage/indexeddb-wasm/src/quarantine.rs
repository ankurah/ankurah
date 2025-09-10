/// Helper function for error conversion in query execution
fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
    res.map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
}

/// Execute ID-optimized query using primary key cursor
async fn execute_id_optimized_query(
    store: &web_sys::IdbObjectStore,
    selection: &ankql::ast::Selection,
    collection_id: &str,
    range: Option<web_sys::IdbKeyRange>,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    // Use primary key cursor for ID-based queries
    // TODO: Implement direction support when web_sys supports it
    let cursor_request = if let Some(range) = range {
        step(store.open_cursor_with_range(&range), "open id cursor with range")?
    } else {
        step(store.open_cursor(), "open id cursor")?
    };

    let mut output: Vec<Attested<EntityState>> = Vec::new();
    let mut count = 0;
    let limit = selection.limit.map(|l| l as usize);
    let mut stream = crate::cb_stream::CBStream::new(&cursor_request, "success", "error");

    while let Some(result) = stream.next().await {
        let cursor_result = step(result, "cursor error")?;
        if cursor_result.is_null() || cursor_result.is_undefined() {
            break;
        }

        let cursor: web_sys::IdbCursorWithValue = step(cursor_result.dyn_into(), "cast cursor")?;
        let entity = step(cursor.value(), "get cursor value")?;

        // Check collection matches (since we're using primary key cursor)
        let entity_collection: String = get_property(&entity, &COLLECTION_KEY)?
            .try_into()
            .map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("Failed to convert collection: {:?}", e).into()))?;
        if entity_collection != collection_id {
            step(cursor.continue_(), "advance cursor")?;
            continue;
        }

        // Parse entity and apply predicate
        if let Some(attested_state) = parse_and_filter_entity(&entity, collection_id, &selection.predicate)? {
            output.push(attested_state);
            count += 1;

            // Apply LIMIT early termination
            if let Some(limit_val) = limit {
                if count >= limit_val {
                    break;
                }
            }
        }

        step(cursor.continue_(), "advance cursor")?;
    }

    Ok(output)
}

/// Execute collection scan query using collection index
async fn execute_collection_scan_query(
    store: &web_sys::IdbObjectStore,
    selection: &ankql::ast::Selection,
    collection_id: &str,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    // Use collection index for non-ID queries
    let index = step(store.index("by_collection"), "get collection index")?;
    let key_range = step(web_sys::IdbKeyRange::only(&collection_id.into()), "create key range")?;
    let request = step(index.open_cursor_with_range(&key_range), "open cursor")?;

    let mut output: Vec<Attested<EntityState>> = Vec::new();
    let mut count = 0;
    let limit = selection.limit.map(|l| l as usize);
    let mut stream = crate::cb_stream::CBStream::new(&request, "success", "error");

    while let Some(result) = stream.next().await {
        let cursor_result = step(result, "cursor error")?;
        if cursor_result.is_null() || cursor_result.is_undefined() {
            break;
        }

        let cursor: web_sys::IdbCursorWithValue = step(cursor_result.dyn_into(), "cast cursor")?;
        let entity = step(cursor.value(), "get cursor value")?;

        // Parse entity and apply predicate
        if let Some(attested_state) = parse_and_filter_entity(&entity, collection_id, &selection.predicate)? {
            output.push(attested_state);
            count += 1;

            // Apply LIMIT early termination
            if let Some(limit_val) = limit {
                if count >= limit_val {
                    break;
                }
            }
        }

        step(cursor.continue_(), "advance cursor")?;
    }

    Ok(output)
}

/// Parse entity from IndexedDB and apply predicate filter
fn parse_and_filter_entity(
    entity: &JsValue,
    collection_id: &str,
    predicate: &ankql::ast::Predicate,
) -> Result<Option<Attested<EntityState>>, RetrievalError> {
    let id = get_property(entity, &ID_KEY)?.try_into()?;
    let entity_state = EntityState {
        collection: collection_id.into(),
        entity_id: id,
        state: State {
            state_buffers: get_property(entity, &STATE_BUFFER_KEY)?.try_into()?,
            head: get_property(entity, &HEAD_KEY)?.try_into()?,
        },
    };
    let attestations = get_property(entity, &ATTESTATIONS_KEY)?.try_into()?;
    let attested_state = Attested { payload: entity_state, attestations };

    // Create entity to evaluate predicate
    let temp_entity = TemporaryEntity::new(id, collection_id.into(), &attested_state.payload.state)?;

    // Apply predicate filter
    if evaluate_predicate(&temp_entity, predicate)? {
        Ok(Some(attested_state))
    } else {
        Ok(None)
    }
}

/// Query execution strategy
#[derive(Debug)]
enum QueryStrategy {
    /// Use primary key cursor with optional range and direction
    IdOptimized { range: Option<web_sys::IdbKeyRange>, direction: &'static str },
    /// Use collection index scan (fallback)
    CollectionScan,
}

/// Analyze selection to determine what indexes are needed
fn analyze_needed_indexes(selection: &ankql::ast::Selection) -> Vec<IndexSpec> {
    let mut needed_indexes = Vec::new();

    // Check ORDER BY clause for index needs
    if let Some(ref order_by) = selection.order_by {
        if let Some(order_item) = order_by.first() {
            if let ankql::ast::Identifier::Property(field_name) = &order_item.identifier {
                // Skip id field - it uses primary key cursor
                if field_name != "id" {
                    let direction = match order_item.direction {
                        ankql::ast::OrderDirection::Asc => IndexDirection::Asc,
                        ankql::ast::OrderDirection::Desc => IndexDirection::Desc,
                    };
                    needed_indexes.push(IndexSpec::new(field_name.clone(), direction));
                }
            }
        }
    }

    // Check WHERE clause for range queries that could benefit from indexes
    analyze_predicate_for_indexes(&selection.predicate, &mut needed_indexes);

    needed_indexes
}

/// Recursively analyze predicate for indexable field comparisons
fn analyze_predicate_for_indexes(predicate: &ankql::ast::Predicate, indexes: &mut Vec<IndexSpec>) {
    use ankql::ast::{ComparisonOperator, Expr, Identifier, Predicate};

    match predicate {
        Predicate::Comparison { left, operator, right } => {
            // Look for field comparisons that benefit from indexes
            if let (Expr::Identifier(Identifier::Property(field_name)), _) = (left.as_ref(), right.as_ref()) {
                // Skip id field - it uses primary key cursor
                if field_name != "id" {
                    match operator {
                        ComparisonOperator::GreaterThan
                        | ComparisonOperator::GreaterThanOrEqual
                        | ComparisonOperator::LessThan
                        | ComparisonOperator::LessThanOrEqual => {
                            // Range queries benefit from ascending index
                            let index_spec = IndexSpec::new(field_name.clone(), IndexDirection::Asc);
                            if !indexes.contains(&index_spec) {
                                indexes.push(index_spec);
                            }
                        }
                        ComparisonOperator::Equal => {
                            // Equality queries also benefit from index (either direction works)
                            let index_spec = IndexSpec::new(field_name.clone(), IndexDirection::Asc);
                            if !indexes.contains(&index_spec) {
                                indexes.push(index_spec);
                            }
                        }
                        _ => {} // Other operators don't typically need indexes
                    }
                }
            }
        }
        Predicate::And(left, right) | Predicate::Or(left, right) => {
            analyze_predicate_for_indexes(left, indexes);
            analyze_predicate_for_indexes(right, indexes);
        }
        Predicate::Not(inner) => {
            analyze_predicate_for_indexes(inner, indexes);
        }
        _ => {} // Other predicates don't need indexes
    }
}

/// Determine the optimal query strategy based on the selection
fn determine_query_strategy(selection: &ankql::ast::Selection) -> QueryStrategy {
    use ankql::ast::{Identifier, OrderDirection};

    // Check if we have ORDER BY id
    if let Some(ref order_by) = selection.order_by {
        if let Some(order_item) = order_by.first() {
            if let Identifier::Property(field_name) = &order_item.identifier {
                if field_name == "id" {
                    // ORDER BY id - use primary key cursor
                    let direction = match order_item.direction {
                        OrderDirection::Asc => "next",
                        OrderDirection::Desc => "prev",
                    };

                    // Check if we also have WHERE id constraints
                    let range = extract_id_range(&selection.predicate);

                    return QueryStrategy::IdOptimized { range, direction };
                }
            }
        }
    }

    // Check if we have WHERE id constraints without ORDER BY
    if let Some(range) = extract_id_range(&selection.predicate) {
        return QueryStrategy::IdOptimized {
            range: Some(range),
            direction: "next", // Default to ascending
        };
    }

    // Fallback to collection scan
    QueryStrategy::CollectionScan
}

/// Extract ID range constraints from predicate
fn extract_id_range(predicate: &ankql::ast::Predicate) -> Option<web_sys::IdbKeyRange> {
    use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};

    match predicate {
        Predicate::Comparison { left, operator, right } => {
            // Check if this is an ID comparison
            if let (Expr::Identifier(Identifier::Property(field)), Expr::Literal(Literal::String(value))) = (left.as_ref(), right.as_ref())
            {
                if field == "id" {
                    let js_value = JsValue::from_str(value);
                    return match operator {
                        ComparisonOperator::GreaterThan => web_sys::IdbKeyRange::lower_bound_with_open(&js_value, true).ok(),
                        ComparisonOperator::GreaterThanOrEqual => web_sys::IdbKeyRange::lower_bound_with_open(&js_value, false).ok(),
                        ComparisonOperator::LessThan => web_sys::IdbKeyRange::upper_bound_with_open(&js_value, true).ok(),
                        ComparisonOperator::LessThanOrEqual => web_sys::IdbKeyRange::upper_bound_with_open(&js_value, false).ok(),
                        ComparisonOperator::Equal => web_sys::IdbKeyRange::only(&js_value).ok(),
                        _ => None,
                    };
                }
            }
        }
        Predicate::And(left, right) => {
            // For AND, we could potentially combine ranges, but for now just take the first ID constraint
            if let Some(range) = extract_id_range(left) {
                return Some(range);
            }
            return extract_id_range(right);
        }
        _ => {}
    }

    None
}

/// Validate that selection doesn't access internal __ prefixed fields
fn validate_no_internal_field_access(selection: &ankql::ast::Selection) -> Result<(), RetrievalError> {
    use ankql::ast::Identifier;

    // Check predicate for internal field access
    validate_predicate_no_internal_fields(&selection.predicate)?;

    // Check ORDER BY for internal field access
    if let Some(ref order_by) = selection.order_by {
        for order_item in order_by {
            if let Identifier::Property(field_name) = &order_item.identifier {
                if field_name.starts_with("__") {
                    return Err(RetrievalError::StorageError(
                        anyhow::anyhow!("Access to internal field '{}' is not allowed", field_name).into(),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Recursively validate predicate doesn't access internal fields
fn validate_predicate_no_internal_fields(predicate: &ankql::ast::Predicate) -> Result<(), RetrievalError> {
    use ankql::ast::Predicate;

    match predicate {
        Predicate::Comparison { left, right, .. } => {
            validate_expr_no_internal_fields(left)?;
            validate_expr_no_internal_fields(right)?;
        }
        Predicate::And(left, right) | Predicate::Or(left, right) => {
            validate_predicate_no_internal_fields(left)?;
            validate_predicate_no_internal_fields(right)?;
        }
        Predicate::Not(inner) => {
            validate_predicate_no_internal_fields(inner)?;
        }
        Predicate::True | Predicate::False => {
            // No field access
        }
        Predicate::IsNull(expr) => {
            validate_expr_no_internal_fields(expr)?;
        }
        Predicate::Placeholder => {
            // No field access
        }
    }

    Ok(())
}

/// Validate expression doesn't access internal fields
fn validate_expr_no_internal_fields(expr: &ankql::ast::Expr) -> Result<(), RetrievalError> {
    use ankql::ast::{Expr, Identifier};

    match expr {
        Expr::Identifier(Identifier::Property(field_name)) => {
            if field_name.starts_with("__") {
                return Err(RetrievalError::StorageError(
                    anyhow::anyhow!("Access to internal field '{}' is not allowed", field_name).into(),
                ));
            }
        }
        _ => {
            // Other expressions (literals, etc.) don't access fields
        }
    }

    Ok(())
}

/// Ensure an index exists, creating it if necessary via version upgrade
async fn ensure_index_exists(db: &IdbDatabase, index_spec: &IndexSpec) -> Result<(), RetrievalError> {
    // Check if index already exists
    if index_exists(db, &index_spec.name)? {
        return Ok(());
    }

    tracing::info!("Creating index: {} for fields: {:?}", index_spec.name, index_spec.fields);

    // Wrap all IndexedDB operations in SendWrapper since they're not Send in WASM
    let db_name = db.name();
    let current_version = db.version() as u32;
    let new_version = current_version + 1;
    let index_spec_clone = index_spec.clone();

    // Close current database connection
    db.close();

    // Reopen with version upgrade using SendWrapper
    SendWrapper::new(async move { upgrade_database_with_index(&db_name, new_version, &index_spec_clone).await }).await?;

    Ok(())
}

/// Check if an index exists in the entities object store
fn index_exists(_db: &IdbDatabase, _index_name: &str) -> Result<bool, RetrievalError> {
    // We need to check this via a transaction since we can't directly inspect object store structure
    // For now, we'll assume indexes don't exist and always try to create them
    // TODO: Implement proper index existence checking via metadata store
    Ok(false)
}

/// Upgrade database to create a new index
async fn upgrade_database_with_index(db_name: &str, new_version: u32, index_spec: &IndexSpec) -> Result<(), RetrievalError> {
    // Wrap all IndexedDB operations in SendWrapper for WASM compatibility
    SendWrapper::new(async move {
        let window = web_sys::window().ok_or_else(|| RetrievalError::StorageError(anyhow::anyhow!("No window found").into()))?;

        let idb: web_sys::IdbFactory = window
            .indexed_db()
            .map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("IndexedDB error: {:?}", e).into()))?
            .ok_or_else(|| RetrievalError::StorageError(anyhow::anyhow!("IndexedDB not available").into()))?;

        let open_request: web_sys::IdbOpenDbRequest = idb
            .open_with_u32(db_name, new_version)
            .map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("Failed to open DB: {:?}", e).into()))?;

        let index_spec_clone = index_spec.clone();

        // Set up upgrade handler
        let onupgradeneeded = Closure::wrap(Box::new(move |event: web_sys::IdbVersionChangeEvent| {
            let target: web_sys::IdbRequest = event.target().unwrap().unchecked_into();
            let _db: web_sys::IdbDatabase = target.result().unwrap().unchecked_into();

            // During upgrade, get the upgrade transaction
            let open_request: web_sys::IdbOpenDbRequest = target.unchecked_into();
            let transaction = match open_request.transaction() {
                Some(tx) => tx,
                None => {
                    tracing::error!("Failed to get upgrade transaction");
                    return;
                }
            };

            let store = match transaction.object_store("entities") {
                Ok(store) => store,
                Err(e) => {
                    tracing::error!("Failed to get entities store during upgrade: {:?}", e);
                    return;
                }
            };

            // Create the index
            let key_path: js_sys::Array = js_sys::Array::new();
            for field in &index_spec_clone.fields {
                key_path.push(&JsValue::from_str(&field.name));
            }

            if let Err(e) = store.create_index_with_str_sequence(&index_spec_clone.name, &key_path) {
                tracing::error!("Failed to create index {}: {:?}", index_spec_clone.name, e);
            } else {
                tracing::info!("Successfully created index: {}", index_spec_clone.name);
            }
        }) as Box<dyn FnMut(_)>);

        open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));

        // Wait for the upgrade to complete
        let success_future = crate::cb_future::CBFuture::new(&open_request, "success", "error");
        success_future.await.map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("Database upgrade failed: {:?}", e).into()))?;

        // Keep the closure alive
        onupgradeneeded.forget();

        Ok(())
    })
    .await
}
