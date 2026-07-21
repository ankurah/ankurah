use crate::{agent::AgentState, config::PolicyConfig, model::JwtPolicy, JwtPolicyView};
use ankurah::proto::EntityId;
use ankurah::Context;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Watches a JSON policy config file on disk and transcribes changes into a `JwtPolicy` entity.
///
/// Uses filesystem notification (via `notify` crate) rather than polling. Watches the parent
/// directory to handle atomic saves (temp file + rename) used by most editors.
pub struct PolicyWatcher {
    /// The entity ID of the JwtPolicy entity managed by this watcher.
    entity_id: EntityId,
    /// Handle to the background watch task.
    watch_handle: JoinHandle<()>,
}

impl PolicyWatcher {
    /// Start watching a JSON policy config file.
    ///
    /// Reads the file, parses as `PolicyConfig`, creates or updates a `JwtPolicy` entity,
    /// then spawns a background task that watches for file changes using filesystem notifications.
    ///
    /// The `shared_state` handle (obtained via `JwtAgent::state_handle()`) is updated
    /// atomically whenever the file changes so that in-memory policy checks stay current.
    pub async fn start(path: impl AsRef<Path>, context: Context, shared_state: Arc<RwLock<AgentState>>) -> Result<Self, anyhow::Error> {
        let path = path.as_ref().to_path_buf();

        // Read and parse the initial config
        let json_str = tokio::fs::read_to_string(&path).await?;
        let parsed_config: PolicyConfig = serde_json::from_str(&json_str)?;

        // Update the shared in-memory config atomically
        {
            let mut guard = shared_state.write().unwrap_or_else(|e| e.into_inner());
            guard.config = parsed_config;
        }

        // Read public key PEM from shared state
        let (public_key_pem, trust_json) = extract_trust_material(&shared_state)?;

        // Upsert the JwtPolicy entity: reuse an existing one or create a new one
        let entity_id = upsert_entity(&context, &json_str, &public_key_pem, trust_json.as_deref()).await?;

        let watch_entity_id = entity_id.clone();
        let watch_handle = tokio::spawn(watch_loop(path, context, watch_entity_id, shared_state));

        Ok(Self { entity_id, watch_handle })
    }

    /// Returns the `EntityId` of the `JwtPolicy` entity managed by this watcher.
    pub fn entity_id(&self) -> &EntityId { &self.entity_id }

    /// Stop the watcher, aborting the background task.
    pub fn stop(self) { self.watch_handle.abort(); }

    /// Returns a reference to the background task handle.
    pub fn handle(&self) -> &JoinHandle<()> { &self.watch_handle }
}

impl Drop for PolicyWatcher {
    fn drop(&mut self) { self.watch_handle.abort(); }
}

fn extract_trust_material(shared_state: &Arc<RwLock<AgentState>>) -> Result<(String, Option<String>), anyhow::Error> {
    let guard = shared_state.read().unwrap_or_else(|e| e.into_inner());
    if let Some(descriptor) = &guard.issuer_trust {
        return Ok((String::new(), Some(serde_json::to_string(descriptor)?)));
    }
    Ok(match guard.keys.as_ref() {
        Some(keys) => (keys.public_key_pem()?, None),
        None => (String::new(), None),
    })
}

/// Query for an existing JwtPolicy entity and update it, or create a new one.
async fn upsert_entity(
    context: &Context,
    json_str: &str,
    public_key_pem: &str,
    trust_json: Option<&str>,
) -> Result<EntityId, anyhow::Error> {
    let existing: Vec<JwtPolicyView> = context.fetch("true").await?;

    if let Some(view) = existing.into_iter().next() {
        // Update the existing entity
        let trx = context.begin();
        let edit = view.edit(&trx)?;
        edit.config_json().set(&json_str.to_string())?;
        edit.public_key_pem().set(&public_key_pem.to_string())?;
        edit.trust_json().set(&trust_json.map(ToString::to_string))?;
        trx.commit().await?;
        Ok(view.id())
    } else {
        // Create a new entity
        let trx = context.begin();
        let entity = trx
            .create(&JwtPolicy {
                config_json: json_str.to_string(),
                public_key_pem: public_key_pem.to_string(),
                trust_json: trust_json.map(ToString::to_string),
            })
            .await?;
        let id: EntityId = entity.id();
        trx.commit().await?;
        Ok(id)
    }
}

async fn watch_loop(path: PathBuf, context: Context, entity_id: EntityId, shared_state: Arc<RwLock<AgentState>>) {
    let (tx, mut rx) = mpsc::channel::<notify::Result<Event>>(64);

    // Watch the parent directory to catch atomic saves (temp+rename)
    let watch_dir = path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();

    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = tx.blocking_send(res);
        },
        notify::Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            warn!("PolicyWatcher: failed to create fs watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
        warn!("PolicyWatcher: failed to watch {}: {}", watch_dir.display(), e);
        return;
    }

    info!("PolicyWatcher: watching {} for changes", path.display());

    loop {
        // Wait for any event
        match rx.recv().await {
            // We watch the parent directory so atomic file replacement works,
            // but SQLite and other siblings are active there too. Ignore their
            // notifications: publishing JwtPolicy writes SQLite, and treating
            // that write as another policy edit creates a self-sustaining loop.
            Some(Ok(event)) if event_targets_policy(&event, &path) => {}
            Some(Ok(_)) => continue,
            Some(Err(e)) => {
                warn!("PolicyWatcher: fs watcher error: {}", e);
                continue;
            }
            None => {
                // Channel closed, watcher dropped
                break;
            }
        }

        // 100ms debounce: sleep briefly then drain any queued events
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        while rx.try_recv().is_ok() {}

        // Check if our file actually changed by trying to read it
        let json_str = match tokio::fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(e) => {
                warn!("PolicyWatcher: failed to read {}: {}", path.display(), e);
                continue;
            }
        };

        // Validate that it parses as PolicyConfig
        let new_config = match serde_json::from_str::<PolicyConfig>(&json_str) {
            Ok(c) => c,
            Err(e) => {
                warn!("PolicyWatcher: invalid PolicyConfig in {}: {}", path.display(), e);
                continue;
            }
        };

        info!("PolicyWatcher: detected valid change in {}", path.display());

        // Update the shared in-memory config atomically
        let trust_material = {
            let mut guard = shared_state.write().unwrap_or_else(|e| e.into_inner());
            guard.config = new_config;
            if let Some(descriptor) = &guard.issuer_trust {
                serde_json::to_string(descriptor).map(|json| (String::new(), Some(json))).map_err(anyhow::Error::from)
            } else {
                match guard.keys.as_ref() {
                    Some(keys) => keys.public_key_pem().map(|pem| (pem, None)).map_err(anyhow::Error::from),
                    None => Ok((String::new(), None)),
                }
            }
        };
        let (public_key_pem, trust_json) = match trust_material {
            Ok(material) => material,
            Err(e) => {
                warn!("PolicyWatcher: failed to export trust material: {e}");
                continue;
            }
        };

        // Update the entity
        match update_entity(&context, &entity_id, &json_str, &public_key_pem, trust_json.as_deref()).await {
            Ok(()) => info!("PolicyWatcher: updated JwtPolicy entity {}", entity_id),
            Err(e) => warn!("PolicyWatcher: failed to update entity: {}", e),
        }
    }
}

fn event_targets_policy(event: &Event, policy_path: &Path) -> bool { event.paths.iter().any(|event_path| event_path == policy_path) }

async fn update_entity(
    context: &Context,
    entity_id: &EntityId,
    json_str: &str,
    public_key_pem: &str,
    trust_json: Option<&str>,
) -> Result<(), anyhow::Error> {
    let view: JwtPolicyView = context.get::<JwtPolicyView>(entity_id.clone()).await?;
    let trx = context.begin();
    let edit = view.edit(&trx)?;
    edit.config_json().set(&json_str.to_string())?;
    edit.public_key_pem().set(&public_key_pem.to_string())?;
    edit.trust_json().set(&trust_json.map(ToString::to_string))?;
    trx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::event_targets_policy;
    use notify::{Event, EventKind};
    use std::path::Path;

    #[test]
    fn directory_watcher_ignores_sibling_events() {
        let policy = Path::new("/data/policy.json");
        let sqlite_event = Event::new(EventKind::Any).add_path("/data/store.sqlite-wal".into());
        assert!(!event_targets_policy(&sqlite_event, policy));

        let policy_event = Event::new(EventKind::Any).add_path(policy.to_path_buf());
        assert!(event_targets_policy(&policy_event, policy));
    }
}
