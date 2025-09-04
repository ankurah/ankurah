use ankurah::signals::Get;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_server::WebsocketServer;
use anyhow::Result;
use example_model::{FlagsView, Level as LogLevel, LogEntry, Payload};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::Level;

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Initialize storage engine
    let storage = SledStorageEngine::with_homedir_folder(".ankurah_example")?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());

    node.system.wait_loaded().await;
    if node.system.root().is_none() {
        node.system.create().await?;
    }

    // Clone node for log generation task
    let log_node = node.clone();

    // Start log generation task
    tokio::spawn(async move {
        if let Err(e) = log_generation_task(log_node).await {
            tracing::error!("Log generation task failed: {}", e);
        }
    });

    // Create and start the websocket server
    let mut server = WebsocketServer::new(node);
    server.run("0.0.0.0:9797").await?;

    Ok(())
}

async fn log_generation_task(node: Node<SledStorageEngine, PermissiveAgent>) -> Result<()> {
    let context = node.context_async(c).await;

    // Subscribe to flags changes
    let flags_query = context.query_wait::<FlagsView>("name = 'generate_logs'").await?;

    let mut interval = interval(Duration::from_millis(100)); // 10 logs per second
    let mut log_counter = 0u64;

    loop {
        interval.tick().await;

        // Check if log generation is enabled
        let should_generate = {
            let flags = flags_query.get();
            flags.iter().any(|flag| flag.value().unwrap_or(false))
        };

        if should_generate {
            log_counter += 1;

            // Generate a fake log entry
            let level = match log_counter % 10 {
                0..=6 => LogLevel::Info,
                7..=8 => LogLevel::Warn,
                9 => LogLevel::Error,
                _ => LogLevel::Debug,
            };

            let sources = ["auth-service", "api-gateway", "database", "cache", "worker"];
            let source = sources[(log_counter % sources.len() as u64) as usize];

            let message = match level {
                LogLevel::Info => format!("Processing request #{}", log_counter),
                LogLevel::Warn => format!("Slow query detected ({}ms)", 500 + (log_counter % 1000)),
                LogLevel::Error => format!("Connection timeout to external service"),
                LogLevel::Debug => format!("Cache hit for key: user_{}", log_counter % 100),
                LogLevel::Trace => format!("Trace message {}", log_counter),
            };

            let payload = if log_counter % 3 == 0 {
                Payload::Json(serde_json::json!({
                    "request_id": format!("req_{}", log_counter),
                    "duration_ms": log_counter % 1000,
                    "user_id": log_counter % 100
                }))
            } else {
                Payload::Text(format!("Additional context for log {}", log_counter))
            };

            // Create the log entry
            let trx = context.begin();
            match trx
                .create(&LogEntry {
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    level,
                    message,
                    source: source.to_string(),
                    node_id: node.id.to_base64(),
                    payload,
                })
                .await
            {
                Ok(_) => {
                    if let Err(e) = trx.commit().await {
                        tracing::error!("Failed to commit log entry: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to create log entry: {}", e);
                }
            }
        }
    }
}
