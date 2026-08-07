use std::path::PathBuf;

use ankurah_cli::dump::{dump, load, validate, DumpSummary};
use ankurah_storage_postgres_0_9::Postgres;
use ankurah_storage_sled_0_9::SledStorageEngine;
use ankurah_storage_sqlite_0_9::SqliteStorageEngine;
use anyhow::Context as _;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ak", version, about = "Command-line tools for Ankurah")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Dump a stopped native store to a portable logical dump.
    Dump(TransferCommand),
    /// Load a portable logical dump into an empty, stopped native store.
    Load(TransferCommand),
}

#[derive(Args)]
struct TransferCommand {
    /// Confirm that every durable node using this store is stopped.
    #[arg(long)]
    all_durable_nodes_stopped: bool,

    /// Dump file to create or load.
    #[arg(short, long)]
    file: PathBuf,

    #[command(subcommand)]
    storage: Storage,
}

#[derive(Subcommand)]
enum Storage {
    /// A PostgreSQL database.
    Postgres {
        /// PostgreSQL connection string. Defaults to DATABASE_URL.
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
    /// A SQLite database file.
    Sqlite {
        #[arg(long)]
        path: PathBuf,
    },
    /// A Sled storage directory.
    Sled {
        #[arg(long)]
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let summary = match cli.command {
        Command::Dump(command) => run_dump(command).await?,
        Command::Load(command) => run_load(command).await?,
    };
    println!("{} events, {} states", summary.events, summary.states);
    Ok(())
}

async fn run_dump(command: TransferCommand) -> anyhow::Result<DumpSummary> {
    require_stopped(command.all_durable_nodes_stopped)?;
    match command.storage {
        Storage::Postgres { database_url } => {
            let engine = Postgres::open(&database_url).await.context("open PostgreSQL source")?;
            dump(&engine, "postgres", command.file).await.map_err(Into::into)
        }
        Storage::Sqlite { path } => {
            require_sqlite_source(&path)?;
            let engine = SqliteStorageEngine::open(path).await.context("open SQLite source")?;
            dump(&engine, "sqlite", command.file).await.map_err(Into::into)
        }
        Storage::Sled { path } => {
            require_sled_source(&path)?;
            let engine = SledStorageEngine::with_path(path).context("open Sled source")?;
            dump(&engine, "sled", command.file).await.map_err(Into::into)
        }
    }
}

async fn run_load(command: TransferCommand) -> anyhow::Result<DumpSummary> {
    require_stopped(command.all_durable_nodes_stopped)?;
    // Opening a SQLite or Sled engine can create its target on disk. Validate
    // before that happens so malformed input cannot touch the destination.
    validate(&command.file).context("validate dump before opening target")?;
    match command.storage {
        Storage::Postgres { database_url } => {
            let engine = Postgres::open(&database_url).await.context("open PostgreSQL target")?;
            load(&engine, command.file).await.map_err(Into::into)
        }
        Storage::Sqlite { path } => {
            let engine = SqliteStorageEngine::open(path).await.context("open SQLite target")?;
            load(&engine, command.file).await.map_err(Into::into)
        }
        Storage::Sled { path } => {
            let engine = SledStorageEngine::with_path(path).context("open Sled target")?;
            load(&engine, command.file).await.map_err(Into::into)
        }
    }
}

fn require_stopped(confirmed: bool) -> anyhow::Result<()> {
    anyhow::ensure!(confirmed, "refusing to continue: stop every durable node using this store, then pass --all-durable-nodes-stopped");
    Ok(())
}

fn require_sqlite_source(path: &std::path::Path) -> anyhow::Result<()> {
    let metadata = std::fs::metadata(path).with_context(|| format!("SQLite dump source does not exist: {}", path.display()))?;
    anyhow::ensure!(metadata.is_file(), "SQLite dump source is not a file: {}", path.display());
    Ok(())
}

fn require_sled_source(path: &std::path::Path) -> anyhow::Result<()> {
    let database = path.join("sled");
    let metadata = std::fs::metadata(&database)
        .with_context(|| format!("Sled dump source does not exist at {} (expected {})", path.display(), database.display()))?;
    anyhow::ensure!(metadata.is_dir(), "Sled dump source is not a database directory: {}", database.display());

    // Sled creates both files before returning from `open`. Checking its
    // directory alone would let a typo silently become a new, empty database.
    for marker in ["conf", "db"] {
        let marker = database.join(marker);
        let metadata =
            std::fs::metadata(&marker).with_context(|| format!("Sled dump source is missing database file: {}", marker.display()))?;
        anyhow::ensure!(metadata.is_file(), "Sled database marker is not a file: {}", marker.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_sources_must_already_exist() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        assert!(require_sqlite_source(&directory.path().join("missing.sqlite")).is_err());
        assert!(require_sled_source(&directory.path().join("missing-sled")).is_err());

        let sqlite = directory.path().join("source.sqlite");
        std::fs::File::create(&sqlite)?;
        require_sqlite_source(&sqlite)?;

        let fake_sled = directory.path().join("fake-sled");
        std::fs::create_dir_all(fake_sled.join("sled"))?;
        assert!(require_sled_source(&fake_sled).is_err());

        let sled = directory.path().join("source-sled");
        drop(SledStorageEngine::with_path(sled.clone())?);
        require_sled_source(&sled)?;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_load_does_not_create_a_native_target() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let dump = directory.path().join("invalid.akdump");
        std::fs::write(&dump, b"not a dump\n")?;
        let target = directory.path().join("target-sled");
        let command = TransferCommand { all_durable_nodes_stopped: true, file: dump, storage: Storage::Sled { path: target.clone() } };

        assert!(run_load(command).await.is_err());
        assert!(!target.exists());
        Ok(())
    }
}
