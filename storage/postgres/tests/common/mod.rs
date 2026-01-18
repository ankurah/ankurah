//! Common utilities for Postgres storage tests

use ankurah::Model;
use ankurah_storage_postgres::Postgres;
use anyhow::Result;
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tracing::Level;

// Re-export proto for tests that need Attested, Event, etc.
pub use ankurah::proto;

/// Album model used by multiple postgres tests
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

// Initialize tracing for tests
#[ctor::ctor]
fn init_tracing() {
    if let Ok(level) = std::env::var("LOG_LEVEL") {
        tracing_subscriber::fmt().with_max_level(Level::from_str(&level).unwrap()).with_test_writer().init();
    } else {
        tracing_subscriber::fmt().with_max_level(Level::INFO).with_test_writer().init();
    }
}

pub async fn create_postgres_container() -> Result<(ContainerAsync<postgres::Postgres>, Postgres)> {
    let container: ContainerAsync<postgres::Postgres> = postgres::Postgres::default()
        .with_db_name("ankurah")
        .with_user("postgres")
        .with_password("postgres")
        .with_init_sql(include_str!("../pg_init.sql").to_string().into_bytes())
        .start()
        .await
        .unwrap();

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let manager = PostgresConnectionManager::new_from_stringlike(
        format!("host={host} port={port} user=postgres password=postgres dbname=ankurah"),
        tokio_postgres::NoTls,
    )?;
    let pool = bb8::Pool::builder().build(manager).await?;

    let storage_engine = Postgres::new(pool)?;

    Ok((container, storage_engine))
}
