use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement, Value,
};

use crate::migration::{Migrator, MigratorTrait};

/// File stem of the initial migration, also its `seaql_migrations.version`.
const INIT_MIGRATION: &str = "m20260606_000001_init";

/// Tables that only exist on databases created before the SeaORM migration.
const LEGACY_SCHEMA_TABLES: [&str; 2] = ["api_keys", "_sqlx_migrations"];

pub async fn connect(database_url: &str) -> Result<DatabaseConnection> {
    let mut options = ConnectOptions::new(database_url.to_string());
    options
        .max_connections(16)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(10));
    Database::connect(options).await.context("connect postgres")
}

pub async fn migrate(db: &DatabaseConnection) -> Result<()> {
    Migrator::install(db).await.context("install migrations")?;
    baseline_legacy_schema(db)
        .await
        .context("baseline legacy schema")?;
    Migrator::up(db, None)
        .await
        .context("run database migrations")
}

/// Register the initial migration for databases that already carry its schema.
///
/// Databases created before the SeaORM migration existed (SQLx-managed ones and
/// hand-rolled ones alike) have no `seaql_migrations` bookkeeping, so applying
/// the init migration would rebuild tables that are already there.
async fn baseline_legacy_schema(db: &DatabaseConnection) -> Result<()> {
    if applied_migration_count(db).await? > 0 {
        return Ok(());
    }

    let mut legacy = false;
    for table in LEGACY_SCHEMA_TABLES {
        if table_exists(db, table).await? {
            legacy = true;
            break;
        }
    }
    if !legacy {
        return Ok(());
    }

    let statement = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO seaql_migrations (version, applied_at) VALUES ($1, $2) \
         ON CONFLICT (version) DO NOTHING",
        [
            Value::from(INIT_MIGRATION),
            Value::from(Utc::now().timestamp()),
        ],
    );
    db.execute_raw(statement)
        .await
        .context("register baseline migration")?;
    tracing::info!("baselined existing schema as migration {INIT_MIGRATION}");
    Ok(())
}

async fn applied_migration_count(db: &DatabaseConnection) -> Result<i64> {
    let statement = Statement::from_string(
        DbBackend::Postgres,
        "SELECT COUNT(*)::BIGINT AS count FROM seaql_migrations",
    );
    let row = db
        .query_one_raw(statement)
        .await
        .context("count applied migrations")?
        .context("count applied migrations returned no row")?;
    row.try_get::<i64>("", "count")
        .context("decode applied migration count")
}

async fn table_exists(db: &DatabaseConnection, table: &str) -> Result<bool> {
    let statement = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = $1) AS exists",
        [Value::from(table)],
    );
    let row = db
        .query_one_raw(statement)
        .await
        .with_context(|| format!("check table {table}"))?
        .with_context(|| format!("check table {table} returned no row"))?;
    row.try_get::<bool>("", "exists")
        .with_context(|| format!("decode table check for {table}"))
}
