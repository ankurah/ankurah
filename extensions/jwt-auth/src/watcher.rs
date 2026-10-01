use crate::JwtAgent;
use ankurah_core::{node::{Node, WeakNode}, storage::StorageEngine};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Watches a JSON policy file and applies changes through `JwtAgent::set_policy_from_file`.
///
/// Uses filesystem notification (via `notify` crate) rather than polling. Watches the parent
/// directory to handle atomic saves (temp file + rename) used by most editors.
pub struct PolicyWatcher {
    /// Handle to the background watch task.
    watch_handle: JoinHandle<()>,
}

impl PolicyWatcher {
    /// Publish subsequent file changes; initial policy installation remains explicit.
    pub fn start<SE: StorageEngine + Send + Sync + 'static>(path: impl AsRef<Path>, node: &Node<SE, JwtAgent>, agent: JwtAgent) -> Self {
        let path = path.as_ref().to_path_buf();
        let watch_handle = tokio::spawn(watch_loop(path, node.weak(), agent));
        Self { watch_handle }
    }

    /// Stop the watcher, aborting the background task.
    pub fn stop(self) { self.watch_handle.abort(); }

    /// Returns a reference to the background task handle.
    pub fn handle(&self) -> &JoinHandle<()> { &self.watch_handle }
}

impl Drop for PolicyWatcher {
    fn drop(&mut self) { self.watch_handle.abort(); }
}

async fn watch_loop<SE: StorageEngine + Send + Sync + 'static>(path: PathBuf, node: WeakNode<SE, JwtAgent>, agent: JwtAgent) {
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
            Some(Ok(_event)) => {}
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

        let Some(node) = node.upgrade() else { break };
        match agent.set_policy_from_file(&node, &path).await {
            Ok(()) => info!("PolicyWatcher: updated policy from {}", path.display()),
            Err(e) => warn!("PolicyWatcher: failed to update policy: {}", e),
        }
    }
}
