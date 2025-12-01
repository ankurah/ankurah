//! Common helpers for benchmarks.

pub mod postgres {
    use std::sync::Arc;

    use anyhow::Result;
    use testcontainers::Container;
    use testcontainers_modules::{postgres as tc_postgres, testcontainers::runners::SyncRunner};

    /// Creates a Postgres test container and returns the connection string.
    pub fn create_container() -> Result<(Container<tc_postgres::Postgres>, String)> {
        let container = tc_postgres::Postgres::default().with_db_name("ankurah").with_user("postgres").with_password("postgres").start()?;

        let host = container.get_host()?;
        let port = container.get_host_port_ipv4(5432)?;
        let connection_string = format!("host={host} port={port} user=postgres password=postgres dbname=ankurah");

        Ok((container, connection_string))
    }

    /// Clears all tables from the Postgres database using raw SQL.
    pub async fn clear_database(connection_string: &str) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls).await?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Drop all tables in the public schema
        client
            .batch_execute(
                "DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;",
            )
            .await?;

        Ok(())
    }

    /// Creates a Postgres storage engine from a connection string.
    pub async fn create_storage(connection_string: &str) -> Result<Arc<ankurah_storage_postgres::Postgres>> {
        use bb8_postgres::PostgresConnectionManager;

        let manager = PostgresConnectionManager::new_from_stringlike(connection_string, tokio_postgres::NoTls)?;
        let pool = bb8::Pool::builder().build(manager).await?;
        Ok(Arc::new(ankurah_storage_postgres::Postgres::new(pool)?))
    }
}
